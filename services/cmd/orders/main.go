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
}

func defaults() config {
	return config{
		WriteMode: "none", Target: "both", CrashAfterCommit: false,
		Saga: false, FailStep: "", Compensate: true,
	}
}

type app struct {
	mu  sync.Mutex
	cfg config

	pool *pgxpool.Pool
	log  *slog.Logger
	rec  lab.Recorder

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
		acquirer: lab.Env("LAB_ACQUIRER_URL", "http://acquirer:8090"),
		courier:  lab.Env("LAB_COURIER_URL", "http://courier:8060"),
		notifier: lab.Env("LAB_NOTIFIER_URL", "http://notifier:8070"),
		amqpURL:  lab.Env("LAB_AMQP_URL", "amqp://lab:lab@rabbitmq:5672/"),
		brokers:  strings.Split(lab.Env("LAB_KAFKA_BROKERS", "kafka:9092"), ","),
		http:     &http.Client{Timeout: 30 * time.Second},
	}

	mux := http.NewServeMux()
	lab.Health(mux, func() error { return pool.Ping(context.Background()) })
	mux.HandleFunc("GET /_lab/events", a.rec.Handler())
	mux.HandleFunc("POST /orders", a.handleCreate)
	mux.HandleFunc("POST /_lab/config", a.handleConfig)
	mux.HandleFunc("POST /_lab/prepare", a.handlePrepare)
	mux.HandleFunc("POST /_lab/emit", a.handleEmit)
	mux.HandleFunc("GET /_lab/state", a.handleState)

	addr := lab.Env("LAB_ADDR", ":8050")
	if err := lab.Serve(addr, mux, log, nil); err != nil {
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
		if err := a.pool.QueryRow(ctx,
			`INSERT INTO orders (client, restaurant, amount, status) VALUES ($1,$2,$3,'paid') RETURNING id`,
			req.Client, req.Restaurant, req.Amount).Scan(&id); err != nil {
			lab.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		event.Order = id
		event.ID = fmt.Sprintf("evt_%d", id)
		a.rec.Record("orders.committed", map[string]string{"order": strconv.FormatInt(id, 10)})
	}

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
	if err := a.postJSON(ctx, a.acquirer+"/v1/charge", map[string]any{
		"order": id, "amount": amount, "idempotency_key": fmt.Sprintf("order-%d", id),
	}, &charge); err != nil {
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
	if target == "amqp" || target == "both" {
		conn, err := lab.DialAMQP(a.amqpURL)
		if err != nil {
			return err
		}
		err = conn.Publish(ctx, e.Type, e)
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
		err = k.Produce(ctx, e)
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
	cfg := a.cfg
	a.mu.Unlock()

	if req.Reset {
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
