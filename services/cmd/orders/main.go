// orders — заказы после выделения из монолита. Владеет своими данными: чужие
// сервисы в его таблицы не заглядывают, а он не заглядывает в чужие.
//
// Сервис написан вокруг двух вопросов m07: как записать в базу и отправить
// событие, не потеряв ни то ни другое, и что делать, когда длинный процесс
// сломался на середине.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/kipyatl0/backend-architecture-lab/services/internal/lab"
)

type config struct {
	// none — событие не отправляется вовсе; direct — запись и отправка двумя
	// отдельными действиями (двойная запись); outbox — событие ложится в
	// исходящий ящик той же транзакцией, что и заказ.
	WriteMode string `json:"write_mode"`
	// Куда публикует сам сервис в режиме direct.
	Target string `json:"target"`
	// Умереть сразу после фиксации транзакции и до отправки события. Ровно
	// эта щель между двумя действиями и есть предмет урока m07 l01.
	CrashAfterCommit bool `json:"crash_after_commit"`

	// Длинный процесс: списание, уведомление, назначение курьера.
	Saga bool `json:"saga"`
	// На каком шаге процесс ломается: payment | notify | courier.
	FailStep string `json:"fail_step"`
	// Выполнять ли компенсации после поломки.
	Compensate bool `json:"compensate"`

	// Откуда сервис читает: primary — из первичного узла, replica — из второй
	// копии. Переключатель сцены m08 l01: код чтения при этом не меняется.
	ReadFrom string `json:"read_from"`
	// Кэш карточки ресторана (m09 l04): сколько живёт значение, сколько стоит
	// поход в базу и ходит ли за всех один.
	CacheTTLMS   int  `json:"cache_ttl_ms"`
	CardMS       int  `json:"card_ms"`
	SingleFlight bool `json:"single_flight"`
	// Где лежит кэш (m10 l01): shared — общая быстрая память, видная всем
	// инстансам; local — память этого процесса, своя у каждого инстанса.
	CacheMode string `json:"cache_mode"`

	// Класть ли контекст трассировки в заголовки отправляемого сообщения
	// (m11 l01). По HTTP контекст едет сам; через брокер — только если
	// отправитель положил его туда руками. Ровно на этом решении и рвётся трейс.
	Propagate bool `json:"propagate"`

	// Проверяет ли сервис право сам (m13 l02). Выключено — сервис считает, что
	// раз запрос пришёл, значит его уже кто-то проверил; включено — читает
	// контекст пользователя, проверяет подпись под ним и отдаёт заказ только
	// его владельцу. Переключатель сцены, а не свойство сервиса.
	Authz bool `json:"authz"`
}

func defaults() config {
	return config{
		WriteMode: "none", Target: "both", CrashAfterCommit: false,
		Saga: false, FailStep: "", Compensate: true,
		ReadFrom: "primary", CacheTTLMS: 2000, CardMS: 200, SingleFlight: false,
		CacheMode: "shared", Propagate: false, Authz: false,
	}
}

type app struct {
	mu  sync.Mutex
	cfg config

	pool *pgxpool.Pool
	log  *slog.Logger
	rec  lab.Recorder
	// Трассировка и лог с меткой трейса (m11). Выключены, пока стенду не задан
	// приёмник: в профилях до m11 сервис ведёт себя в точности как раньше.
	tracer *lab.Tracer
	trail  *lab.Trail

	acquirer string
	courier  string
	notifier string
	amqpURL  string
	brokers  []string

	http *http.Client

	// Журнал саги: какие шаги прошли, какие компенсированы и что осталось
	// невозвратным. Это и есть то, что студент читает в выводе сцены.
	sagaLog []string
	seq     int

	// Путь чтения: вторая копия базы и кэш перед ней. Заведён отдельно —
	// путь записи от его появления не изменился (см. read.go).
	read *readSide
}

func main() {
	log := lab.Logger("orders")
	ctx := context.Background()

	pool, err := bootstrap(ctx, log)
	if err != nil {
		log.Error("база не готова", "err", err)
		os.Exit(1)
	}
	defer pool.Close()

	a := &app{
		cfg:      defaults(),
		pool:     pool,
		log:      log,
		tracer:   lab.StartTracer("orders"),
		trail:    lab.NewTrail("orders"),
		acquirer: lab.Env("LAB_ACQUIRER_URL", "http://acquirer:8090"),
		courier:  lab.Env("LAB_COURIER_URL", "http://courier:8060"),
		notifier: lab.Env("LAB_NOTIFIER_URL", "http://notifier:8070"),
		amqpURL:  lab.Env("LAB_AMQP_URL", "amqp://lab:lab@rabbitmq:5672/"),
		brokers:  strings.Split(lab.Env("LAB_KAFKA_BROKERS", "kafka:9092"), ","),
		http:     &http.Client{Timeout: 30 * time.Second},
		read:     newReadSide(),
	}

	mux := http.NewServeMux()
	lab.Health(mux, func() error { return pool.Ping(context.Background()) })
	mux.HandleFunc("GET /_lab/events", a.rec.Handler())
	mux.HandleFunc("POST /orders", a.handleCreate)
	mux.HandleFunc("POST /_lab/config", a.handleConfig)
	mux.HandleFunc("POST /_lab/prepare", a.handlePrepare)
	mux.HandleFunc("POST /_lab/emit", a.handleEmit)
	mux.HandleFunc("POST /_lab/bulk-cancel", a.handleBulkCancel)
	mux.HandleFunc("POST /_lab/erase", a.handleErase)
	mux.HandleFunc("POST /_lab/cdc-reset", a.handleCDCReset)
	mux.HandleFunc("GET /_lab/state", a.handleState)
	mux.HandleFunc("GET /orders/{id}", a.handleReadOrder)
	mux.HandleFunc("GET /restaurants/{name}", a.handleCard)
	mux.HandleFunc("GET /_lab/read-stats", a.handleReadStats)
	mux.HandleFunc("POST /_lab/read-reset", a.handleReadReset)
	mux.HandleFunc("GET /_lab/log", a.trail.Handler())

	addr := lab.Env("LAB_ADDR", ":8050")
	// Каждый ответ подписан именем инстанса: с m10 их больше одного, и вопрос
	// «кто именно ответил» становится предметом урока. Спан вокруг запроса —
	// снаружи от всего остального: он обязан охватывать всю работу целиком.
	if err := lab.Serve(addr, a.tracer.Middleware(lab.WithInstance(mux)), log, nil); err != nil {
		log.Error("сервис остановлен с ошибкой", "err", err)
	}
}

// bootstrap заводит сервису его собственную базу, если её ещё нет, и создаёт
// схему. Отдельная база, а не общая с монолитом, — не украшение: без неё
// «владеет своими данными» осталось бы словами, а журнал репликации в сцене 19
// показывал бы чужие таблицы вперемешку со своими.
func bootstrap(ctx context.Context, log *slog.Logger) (*pgxpool.Pool, error) {
	adminURL := lab.Env("LAB_BOOTSTRAP_URL", "")
	dbURL := lab.Env("LAB_DATABASE_URL", "")

	deadline := time.Now().Add(90 * time.Second)
	for {
		conn, err := pgx.Connect(ctx, adminURL)
		if err == nil {
			var exists bool
			err = conn.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'orders')`).Scan(&exists)
			if err == nil && !exists {
				// CREATE DATABASE не умеет IF NOT EXISTS и не живёт в
				// транзакции — гонку двух стартов гасим тем, что ошибку
				// «уже существует» считаем успехом.
				if _, cerr := conn.Exec(ctx, `CREATE DATABASE orders`); cerr != nil &&
					!strings.Contains(cerr.Error(), "already exists") {
					err = cerr
				}
			}
			conn.Close(ctx)
			if err == nil {
				break
			}
		}
		if time.Now().After(deadline) {
			return nil, err
		}
		time.Sleep(500 * time.Millisecond)
	}

	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		return nil, err
	}
	for {
		if err = pool.Ping(ctx); err == nil {
			break
		}
		if time.Now().After(deadline) {
			return nil, err
		}
		time.Sleep(500 * time.Millisecond)
	}

	schema := `
CREATE TABLE IF NOT EXISTS orders (
    id         BIGSERIAL PRIMARY KEY,
    client     TEXT   NOT NULL,
    restaurant TEXT   NOT NULL,
    amount     BIGINT NOT NULL,
    status     TEXT   NOT NULL,
    charge     TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- Исходящий ящик живёт в той же базе, что и заказы, — иначе он не попал бы
-- в ту же транзакцию, и весь смысл приёма исчез бы.
CREATE TABLE IF NOT EXISTS outbox (
    id         BIGSERIAL PRIMARY KEY,
    order_id   BIGINT NOT NULL,
    type       TEXT   NOT NULL,
    payload    JSONB  NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    sent_at    TIMESTAMPTZ
);`
	if _, err := pool.Exec(ctx, schema); err != nil {
		pool.Close()
		return nil, err
	}
	log.Info("своя база готова")
	return pool, nil
}

// ── создание заказа ─────────────────────────────────────────────────────────

func (a *app) handleCreate(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Client     string `json:"client"`
		Restaurant string `json:"restaurant"`
		Amount     int64  `json:"amount"`
	}
	if err := lab.ReadJSON(r, &req); err != nil {
		lab.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	cfg := a.config()
	ctx := r.Context()

	if cfg.Saga {
		a.runSaga(ctx, w, req.Client, req.Restaurant, req.Amount, cfg)
		return
	}

	event := lab.Msg{Type: "order.paid", Amount: req.Amount, Client: req.Client}

	var id int64
	switch cfg.WriteMode {
	case "outbox":
		// Заказ и событие — одна транзакция. Либо есть оба, либо нет ни того
		// ни другого: третьего состояния эта запись не допускает.
		err := pgx.BeginFunc(ctx, a.pool, func(tx pgx.Tx) error {
			if err := tx.QueryRow(ctx,
				`INSERT INTO orders (client, restaurant, amount, status) VALUES ($1,$2,$3,'paid') RETURNING id`,
				req.Client, req.Restaurant, req.Amount).Scan(&id); err != nil {
				return err
			}
			event.Order = id
			event.ID = fmt.Sprintf("evt_%d", id)
			payload, _ := json.Marshal(event)
			_, err := tx.Exec(ctx,
				`INSERT INTO outbox (order_id, type, payload) VALUES ($1,$2,$3)`,
				id, event.Type, payload)
			return err
		})
		if err != nil {
			lab.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		a.rec.Record("orders.committed", map[string]string{"order": strconv.FormatInt(id, 10)})

	default: // none и direct пишут заказ обычной вставкой
		_, dbSpan := a.tracer.Start(ctx, "запись заказа", lab.KindClient)
		err := a.pool.QueryRow(ctx,
			`INSERT INTO orders (client, restaurant, amount, status) VALUES ($1,$2,$3,'paid') RETURNING id`,
			req.Client, req.Restaurant, req.Amount).Scan(&id)
		dbSpan.End()
		if err != nil {
			lab.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		event.Order = id
		event.ID = fmt.Sprintf("evt_%d", id)
		a.rec.Record("orders.committed", map[string]string{"order": strconv.FormatInt(id, 10)})
	}
	lab.SpanOf(ctx).Set("order", strconv.FormatInt(id, 10))

	// Данные ресторана изменились, и инстанс, принявший запись, сбрасывает свою
	// карточку. Свою — и только свою: до памяти соседнего процесса ему не
	// дотянуться. С общим кэшем (m09) этой строки хватило бы на всех; с
	// локальным (m10 l01) остальные инстансы продолжают отвечать старое.
	a.read.localDrop("card:" + req.Restaurant)

	// Транзакция зафиксирована. Данные есть. События ещё нет — и вот здесь
	// процесс умирает: не «падает от ошибки», а просто исчезает.
	if cfg.CrashAfterCommit {
		a.rec.Record("orders.die", map[string]string{"order": strconv.FormatInt(id, 10)})
		a.log.Info("процесс умер после фиксации и до отправки события", "order", id)
		go func() {
			// Ответ клиенту должен успеть уйти, а сцена — успеть увидеть, что
			// процесс действительно исчез. Тридцати миллисекунд на второе не
			// хватало: сцена спрашивала «поднялся?» раньше, чем он умирал.
			time.Sleep(150 * time.Millisecond)
			os.Exit(1)
		}()
		lab.WriteJSON(w, http.StatusCreated, map[string]any{"order": id})
		return
	}

	if cfg.WriteMode == "direct" {
		if err := a.publish(ctx, cfg.Target, event); err != nil {
			a.log.Info("событие не отправлено", "order", id, "err", err.Error())
		} else {
			a.rec.Record("orders.published", map[string]string{"order": strconv.FormatInt(id, 10)})
		}
	}

	lab.WriteJSON(w, http.StatusCreated, map[string]any{"order": id})
}

// ── сага ────────────────────────────────────────────────────────────────────

// runSaga — длинный процесс из четырёх шагов. У каждого шага, кроме первого,
// есть компенсация; у уведомления её нет вовсе, и это не упущение стенда, а
// свойство предметной области: отправленное письмо не отзывается.
func (a *app) runSaga(ctx context.Context, w http.ResponseWriter,
	client, restaurant string, amount int64, cfg config) {

	var id int64
	if err := a.pool.QueryRow(ctx,
		`INSERT INTO orders (client, restaurant, amount, status) VALUES ($1,$2,$3,'created') RETURNING id`,
		client, restaurant, amount).Scan(&id); err != nil {
		lab.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	order := strconv.FormatInt(id, 10)
	lab.SpanOf(ctx).Set("order", order)
	a.step("заказ создан")
	a.rec.Record("saga.created", map[string]string{"order": order})

	done := []string{} // что уже сделано — компенсировать надо в обратном порядке
	var chargeID string

	fail := func(step, reason string) {
		a.step(fmt.Sprintf("шаг «%s» не прошёл: %s", step, reason))
		a.rec.Record("saga.failed", map[string]string{"order": order, "step": step})
		a.compensate(ctx, id, order, done, chargeID, cfg)
		lab.WriteJSON(w, http.StatusConflict, map[string]any{
			"order": id, "failed_step": step, "error": reason,
		})
	}

	// 1. Списание.
	if cfg.FailStep == "payment" {
		fail("списание", "банк отказал")
		return
	}
	var charge struct {
		Charge string `json:"charge"`
	}
	// Эквайринг за периметром: своих спанов он не заводит и заводить не обязан.
	// Значит, отрезок за него заводит вызывающий — иначе в дереве трейса на
	// месте внешнего вызова будет дыра.
	chargeCtx, chargeSpan := a.tracer.Start(ctx, "списание в эквайринге", lab.KindClient)
	err := a.postJSON(chargeCtx, a.acquirer+"/v1/charge", map[string]any{
		"order": id, "amount": amount, "idempotency_key": fmt.Sprintf("order-%d", id),
	}, &charge)
	chargeSpan.End()
	if err != nil {
		fail("списание", err.Error())
		return
	}
	chargeID = charge.Charge
	done = append(done, "charge")
	a.step(fmt.Sprintf("списание выполнено: %s", chargeID))
	a.rec.Record("saga.charged", map[string]string{"order": order, "charge": chargeID})
	_, _ = a.pool.Exec(ctx, `UPDATE orders SET status='paid', charge=$2 WHERE id=$1`, id, chargeID)

	// 2. Уведомление. Компенсации у него не будет: письмо уже у клиента.
	if cfg.FailStep == "notify" {
		fail("уведомление", "сервис уведомлений недоступен")
		return
	}
	if err := a.postJSON(ctx, a.notifier+"/notifications", map[string]any{
		"order": id, "body": "Заказ принят",
	}, nil); err != nil {
		fail("уведомление", err.Error())
		return
	}
	done = append(done, "notify")
	a.step("клиент уведомлён: «Заказ принят»")
	a.rec.Record("saga.notified", map[string]string{"order": order})

	// 3. Назначение курьера.
	if err := a.postJSON(ctx, a.courier+"/_lab/assign", map[string]any{"order": id}, nil); err != nil {
		fail("назначение курьера", err.Error())
		return
	}
	done = append(done, "assign")
	a.step("курьер назначен")
	a.rec.Record("saga.assigned", map[string]string{"order": order})

	_, _ = a.pool.Exec(ctx, `UPDATE orders SET status='assigned' WHERE id=$1`, id)
	// Строка лога с меткой трассировки: третий сигнал. Метку она получает не
	// от программиста, а из контекста запроса — тем и держится связь между
	// логом, трейсом и числом в метрике.
	a.trail.Write(ctx, "заказ оформлен", map[string]string{"заказ": order})
	lab.WriteJSON(w, http.StatusCreated, map[string]any{"order": id, "status": "assigned"})
}

// compensate идёт по сделанному в обратном порядке. Компенсация — обычное
// действие, а не откат: она сама может не получиться, и тогда разбираться
// придётся человеку.
func (a *app) compensate(ctx context.Context, id int64, order string,
	done []string, chargeID string, cfg config) {

	if !cfg.Compensate {
		a.step("компенсации выключены сценой: система осталась в промежуточном состоянии")
		return
	}

	for i := len(done) - 1; i >= 0; i-- {
		switch done[i] {
		case "assign":
			if err := a.postJSON(ctx, a.courier+"/_lab/unassign", map[string]any{"order": id}, nil); err != nil {
				a.step("компенсация «снять назначение» не прошла")
				a.manual(ctx, id, order, "снять назначение")
				continue
			}
			a.step("компенсация: назначение снято")
			a.rec.Record("saga.unassigned", map[string]string{"order": order})

		case "notify":
			// Компенсации нет и быть не может.
			a.step("уведомление отозвать нельзя: клиент его уже получил")
			a.rec.Record("saga.irrevocable", map[string]string{"order": order})

		case "charge":
			if err := a.postJSON(ctx, a.acquirer+"/v1/refund", map[string]any{
				"charge": chargeID, "order": id,
			}, nil); err != nil {
				a.step(fmt.Sprintf("компенсация «возврат %s» не прошла", chargeID))
				a.rec.Record("saga.refund_failed", map[string]string{"order": order, "charge": chargeID})
				a.manual(ctx, id, order, "возврат "+chargeID)
				continue
			}
			a.step(fmt.Sprintf("компенсация: возврат %s выполнен", chargeID))
			a.rec.Record("saga.refunded", map[string]string{"order": order, "charge": chargeID})
		}
	}

	_, _ = a.pool.Exec(ctx, `UPDATE orders SET status='cancelled' WHERE id=$1`, id)
	a.step("заказ отменён")
	a.rec.Record("saga.cancelled", map[string]string{"order": order})
}

// manual — последний рубеж: то, что не удалось компенсировать, уходит человеку.
// Молча забыть об этом нельзя: деньги списаны, а услуги не будет.
func (a *app) manual(ctx context.Context, id int64, order, what string) {
	conn, err := lab.DialAMQP(a.amqpURL)
	if err != nil {
		a.log.Info("очередь ручного разбора недоступна", "order", id)
		return
	}
	defer conn.Close()
	_ = conn.PublishTo(ctx, lab.QueueManu, lab.Msg{
		Type: "compensation.failed", Order: id, Reason: what,
		ID: fmt.Sprintf("manual_%d", id),
	})
	a.step(fmt.Sprintf("в очередь ручного разбора: %s", what))
	a.rec.Record("saga.manual", map[string]string{"order": order, "what": what})
}

func (a *app) step(text string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.seq++
	a.sagaLog = append(a.sagaLog, fmt.Sprintf("%d. %s", a.seq, text))
}

// ── отправка события ────────────────────────────────────────────────────────

func (a *app) publish(ctx context.Context, target string, e lab.Msg) error {
	// Отправка события — своя работа со своей длительностью, и в дереве трейса
	// она обязана быть отдельным отрезком: иначе «где ушло время» останется
	// вопросом к сервису целиком.
	ctx, span := a.tracer.Start(ctx, "публикация "+e.Type, lab.KindProducer)
	defer span.End()
	span.Set("order", strconv.FormatInt(e.Order, 10))

	// Вот всё решение целиком. Положил контекст в заголовки — потребитель
	// продолжит трейс; не положил — начнёт свой, и связи не будет ни у кого.
	var headers map[string]string
	if a.config().Propagate {
		if tp := lab.Carrier(ctx); tp != "" {
			headers = map[string]string{"traceparent": tp}
			span.Set("propagated", "true")
		}
	}

	if target == "amqp" || target == "both" {
		conn, err := lab.DialAMQP(a.amqpURL)
		if err != nil {
			return err
		}
		err = conn.PublishWithHeaders(ctx, e.Type, e, headers)
		conn.Close()
		if err != nil {
			return err
		}
	}
	if target == "kafka" || target == "both" {
		k, err := lab.DialKafka(a.brokers)
		if err != nil {
			return err
		}
		err = k.ProduceWithHeaders(ctx, e, headers)
		k.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func (a *app) postJSON(ctx context.Context, url string, body, out any) error {
	payload, _ := json.Marshal(body)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(string(payload)))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	// Контекст трассировки уезжает заголовком в каждом исходящем вызове. Одна
	// строка — и соседний сервис оказывается в том же трейсе.
	lab.Inject(ctx, req.Header)
	resp, err := a.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		var e struct {
			Error string `json:"error"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&e)
		if e.Error == "" {
			e.Error = fmt.Sprintf("ответ %d", resp.StatusCode)
		}
		return fmt.Errorf("%s", e.Error)
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}

// ── управление сценой ───────────────────────────────────────────────────────

func (a *app) config() config {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.cfg
}

func (a *app) handleConfig(w http.ResponseWriter, r *http.Request) {
	var req struct {
		WriteMode        *string `json:"write_mode"`
		Target           *string `json:"target"`
		CrashAfterCommit *bool   `json:"crash_after_commit"`
		Saga             *bool   `json:"saga"`
		FailStep         *string `json:"fail_step"`
		Compensate       *bool   `json:"compensate"`
		ReadFrom         *string `json:"read_from"`
		CacheTTLMS       *int    `json:"cache_ttl_ms"`
		CardMS           *int    `json:"card_ms"`
		SingleFlight     *bool   `json:"single_flight"`
		CacheMode        *string `json:"cache_mode"`
		Propagate        *bool   `json:"propagate"`
		Authz            *bool   `json:"authz"`
		Reset            bool    `json:"reset"`
	}
	if err := lab.ReadJSON(r, &req); err != nil {
		lab.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	a.mu.Lock()
	if req.Reset {
		a.cfg = defaults()
		a.sagaLog, a.seq = nil, 0
		a.rec.Clear()
	}
	if req.WriteMode != nil {
		a.cfg.WriteMode = *req.WriteMode
	}
	if req.Target != nil {
		a.cfg.Target = *req.Target
	}
	if req.CrashAfterCommit != nil {
		a.cfg.CrashAfterCommit = *req.CrashAfterCommit
	}
	if req.Saga != nil {
		a.cfg.Saga = *req.Saga
	}
	if req.FailStep != nil {
		a.cfg.FailStep = *req.FailStep
	}
	if req.Compensate != nil {
		a.cfg.Compensate = *req.Compensate
	}
	if req.ReadFrom != nil {
		a.cfg.ReadFrom = *req.ReadFrom
	}
	if req.CacheTTLMS != nil {
		a.cfg.CacheTTLMS = *req.CacheTTLMS
	}
	if req.CardMS != nil {
		a.cfg.CardMS = *req.CardMS
	}
	if req.SingleFlight != nil {
		a.cfg.SingleFlight = *req.SingleFlight
	}
	if req.CacheMode != nil {
		a.cfg.CacheMode = *req.CacheMode
	}
	if req.Propagate != nil {
		a.cfg.Propagate = *req.Propagate
	}
	if req.Authz != nil {
		a.cfg.Authz = *req.Authz
	}
	cfg := a.cfg
	a.mu.Unlock()

	if req.Reset {
		a.trail.Clear()
		a.read.localClear()
		if _, err := a.pool.Exec(r.Context(), `TRUNCATE orders, outbox RESTART IDENTITY`); err != nil {
			lab.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
	}
	a.log.Info("сцена настроила заказы", "write_mode", cfg.WriteMode,
		"crash_after_commit", cfg.CrashAfterCommit, "saga", cfg.Saga, "fail_step", cfg.FailStep)
	lab.WriteJSON(w, http.StatusOK, cfg)
}

// handlePrepare задаёт номер первого заказа. Номера в тексте шага настоящие,
// и повторяются они только потому, что последовательность выставлена явно.
func (a *app) handlePrepare(w http.ResponseWriter, r *http.Request) {
	var req struct {
		FirstOrderID int64 `json:"first_order_id"`
	}
	if err := lab.ReadJSON(r, &req); err != nil {
		lab.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if req.FirstOrderID <= 0 {
		req.FirstOrderID = 7714
	}
	if _, err := a.pool.Exec(r.Context(),
		fmt.Sprintf(`ALTER SEQUENCE orders_id_seq RESTART WITH %d`, req.FirstOrderID)); err != nil {
		lab.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	lab.WriteJSON(w, http.StatusOK, map[string]any{"first_order_id": req.FirstOrderID})
}

// handleEmit отправляет названное сценой событие. Отправитель при этом
// остаётся настоящим — сервис заказов, а не исполнитель сцены: иначе в timeline
// пришлось бы писать неправду о том, кто это событие породил.
func (a *app) handleEmit(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Type   string `json:"type"`
		Order  int64  `json:"order"`
		Seq    int    `json:"seq"`
		WorkMS int    `json:"work_ms"`
		Target string `json:"target"`
	}
	if err := lab.ReadJSON(r, &req); err != nil {
		lab.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if req.Target == "" {
		req.Target = "kafka"
	}
	msg := lab.Msg{
		Type: req.Type, Order: req.Order, Seq: req.Seq, WorkMS: req.WorkMS,
		ID: fmt.Sprintf("evt_%d_%d", req.Order, req.Seq),
	}
	if err := a.publish(r.Context(), req.Target, msg); err != nil {
		lab.WriteJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
		return
	}
	a.rec.Record(fmt.Sprintf("orders.emit#%d", req.Seq),
		map[string]string{"order": strconv.FormatInt(req.Order, 10), "type": req.Type})
	lab.WriteJSON(w, http.StatusOK, msg)
}

// ── обслуживание данных руками: то, что делает не сервис, а человек ─────────
//
// Обе операции ниже — не «функции сервиса заказов», а то, что в жизни делает
// оператор или дежурный: отменить одной командой всё по закрывшемуся ресторану,
// удалить данные клиента по его просьбе. Приложение при этом не публикует
// ничего и не знает, что за его таблицами кто-то следит, — и ровно поэтому
// сцена 19 показывает на них цену захвата изменений.

func (a *app) handleBulkCancel(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Restaurant string `json:"restaurant"`
	}
	if err := lab.ReadJSON(r, &req); err != nil {
		lab.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	tag, err := a.pool.Exec(r.Context(),
		`UPDATE orders SET status='cancelled' WHERE restaurant=$1 AND status<>'cancelled'`, req.Restaurant)
	if err != nil {
		lab.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	n := tag.RowsAffected()
	a.rec.Record("orders.bulk", map[string]string{"rows": strconv.FormatInt(n, 10)})
	a.log.Info("массовая отмена одной командой", "restaurant", req.Restaurant, "rows", n)
	lab.WriteJSON(w, http.StatusOK, map[string]any{"rows": n})
}

func (a *app) handleErase(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Order int64 `json:"order"`
	}
	if err := lab.ReadJSON(r, &req); err != nil {
		lab.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	tag, err := a.pool.Exec(r.Context(), `DELETE FROM orders WHERE id=$1`, req.Order)
	if err != nil {
		lab.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	n := tag.RowsAffected()
	a.rec.Record("orders.erase", map[string]string{"order": strconv.FormatInt(req.Order, 10)})
	a.log.Info("строка удалена по просьбе клиента", "order", req.Order, "rows", n)
	lab.WriteJSON(w, http.StatusOK, map[string]any{"rows": n})
}

// handleCDCReset возвращает базу в состояние «за журналом никто не следит»:
// сносит слоты репликации прошлых прогонов и публикацию. Без этого второй
// запуск сцены 19 не сделал бы начального снимка — коннектор продолжил бы с
// того места, где остановился прошлый, — и упёрся бы в предел слотов.
func (a *app) handleCDCReset(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	dropped := []string{}

	// Слот, который ещё держит умирающий коннектор, отпускается не мгновенно.
	deadline := time.Now().Add(20 * time.Second)
	for {
		rows, err := a.pool.Query(ctx,
			`SELECT slot_name, active FROM pg_replication_slots WHERE slot_name LIKE 'lab_orders%'`)
		if err != nil {
			lab.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		type slot struct {
			name   string
			active bool
		}
		var slots []slot
		for rows.Next() {
			var s slot
			if err := rows.Scan(&s.name, &s.active); err == nil {
				slots = append(slots, s)
			}
		}
		rows.Close()

		busy := false
		for _, s := range slots {
			if s.active {
				busy = true
				continue
			}
			if _, err := a.pool.Exec(ctx, `SELECT pg_drop_replication_slot($1)`, s.name); err != nil {
				busy = true
				continue
			}
			dropped = append(dropped, s.name)
		}
		if !busy {
			break
		}
		if time.Now().After(deadline) {
			lab.WriteJSON(w, http.StatusConflict, map[string]any{
				"error": "слот репликации всё ещё занят: коннектор прошлого прогона жив",
			})
			return
		}
		time.Sleep(500 * time.Millisecond)
	}

	if _, err := a.pool.Exec(ctx, `DROP PUBLICATION IF EXISTS `+lab.CDCPublication); err != nil {
		lab.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	a.log.Info("журнал репликации возвращён в исходное состояние", "slots", dropped)
	lab.WriteJSON(w, http.StatusOK, map[string]any{"dropped": dropped})
}

func (a *app) handleState(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	type order struct {
		ID     int64  `json:"id"`
		Client string `json:"client"`
		Amount int64  `json:"amount"`
		Status string `json:"status"`
		Charge string `json:"charge"`
	}
	var orders []order
	rows, err := a.pool.Query(ctx,
		`SELECT id, client, amount, status, coalesce(charge,'') FROM orders ORDER BY id`)
	if err == nil {
		for rows.Next() {
			var o order
			if err := rows.Scan(&o.ID, &o.Client, &o.Amount, &o.Status, &o.Charge); err == nil {
				orders = append(orders, o)
			}
		}
		rows.Close()
	}

	type outboxRow struct {
		ID    int64  `json:"id"`
		Order int64  `json:"order"`
		Type  string `json:"type"`
		Sent  bool   `json:"sent"`
	}
	var outbox []outboxRow
	rows, err = a.pool.Query(ctx, `SELECT id, order_id, type, sent_at IS NOT NULL FROM outbox ORDER BY id`)
	if err == nil {
		for rows.Next() {
			var o outboxRow
			if err := rows.Scan(&o.ID, &o.Order, &o.Type, &o.Sent); err == nil {
				outbox = append(outbox, o)
			}
		}
		rows.Close()
	}

	a.mu.Lock()
	sagaLog := append([]string(nil), a.sagaLog...)
	a.mu.Unlock()

	lab.WriteJSON(w, http.StatusOK, map[string]any{
		"orders": orders, "outbox": outbox, "saga": sagaLog,
		"totals": map[string]any{"orders": len(orders), "outbox": len(outbox)},
	})
}
