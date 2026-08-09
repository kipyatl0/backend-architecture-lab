// monolith — служба доставки одним процессом. Заказы, оплата, назначение
// курьера и уведомление лежат в одной транзакции: ровно та система, с которой
// курс начинается и об которую ломается в m01.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/kipyatl0/backend-architecture-lab/services/internal/lab"
)

// Курьеры службы. Назначение — по номеру заказа, а не по счётчику: так оно
// остаётся детерминированным даже когда два заказа идут одновременно.
var couriers = []string{"Айгуль", "Пётр", "Никита"}

const schema = `
CREATE SEQUENCE IF NOT EXISTS order_id_seq START 7714;

CREATE TABLE IF NOT EXISTS orders (
	id          bigint PRIMARY KEY DEFAULT nextval('order_id_seq'),
	client      text        NOT NULL,
	restaurant  text        NOT NULL,
	amount      bigint      NOT NULL,
	status      text        NOT NULL,
	created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS payments (
	id         bigserial   PRIMARY KEY,
	order_id   bigint      NOT NULL,
	amount     bigint      NOT NULL,
	status     text        NOT NULL,
	charge_id  text,
	created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS courier_assignments (
	id         bigserial   PRIMARY KEY,
	order_id   bigint      NOT NULL,
	courier    text        NOT NULL,
	created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS notifications (
	id         bigserial   PRIMARY KEY,
	order_id   bigint      NOT NULL,
	channel    text        NOT NULL,
	body       text        NOT NULL,
	created_at timestamptz NOT NULL DEFAULT now()
);
`

type app struct {
	pool     *pgxpool.Pool
	rec      lab.Recorder
	log      *slog.Logger
	acquirer string
	client   *http.Client
	attempts atomic.Int64

	// Управляемая сценой часть службы: пул, очередь, поведение уведомлений,
	// повторы и предохранитель. См. control.go.
	ctl *control
	// Пусто в профиле mono (модуль уведомлений живёт в этом же процессе) и
	// заполнено в профиле split (модуль уехал за сеть).
	notifier     string
	notifyClient *http.Client
}

// querier — то общее, что есть у пула и у транзакции. Модуль уведомлений в
// профиле mono пишет строку в ТОЙ ЖЕ транзакции, что и заказ: так эта система
// была написана, и курс на этом стоит.
type querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

func main() {
	log := lab.Logger("monolith")

	ctx := context.Background()
	pool, err := connect(ctx, lab.Env("LAB_DATABASE_URL", ""), log)
	if err != nil {
		log.Error("база недоступна", "err", err)
		return
	}
	defer pool.Close()

	if _, err := pool.Exec(ctx, schema); err != nil {
		log.Error("схема не создана", "err", err)
		return
	}

	a := &app{
		pool:     pool,
		log:      log,
		acquirer: lab.Env("LAB_ACQUIRER_URL", "http://acquirer:8090"),
		client:   &http.Client{Timeout: lab.EnvDuration("LAB_ACQUIRER_TIMEOUT", 120*time.Second)},
		ctl:      newControl(),
		notifier: lab.Env("LAB_NOTIFIER_URL", ""),
		// Таймаут вызова уведомлений задаёт сцена, а не клиент: в курсе
		// таймаут — часть контракта, и трогать его должен тот, кто ставит опыт.
		notifyClient: &http.Client{},
	}
	if a.notifier == "" {
		log.Info("уведомления — модуль этого же процесса")
	} else {
		log.Info("уведомления — отдельный сервис за сетью", "url", a.notifier)
	}

	mux := http.NewServeMux()
	lab.Health(mux, func() error {
		if a.ctl.draining.Load() {
			return fmt.Errorf("останавливаемся")
		}
		return pool.Ping(context.Background())
	})
	mux.HandleFunc("POST /orders", a.handleCreateOrder)
	mux.HandleFunc("GET /orders/{id}", a.handleGetOrder)
	mux.HandleFunc("GET /catalog", a.handleCatalog)
	mux.HandleFunc("GET /_lab/events", a.rec.Handler())
	mux.HandleFunc("POST /_lab/prepare", a.handlePrepare)
	mux.HandleFunc("GET /_lab/state", a.handleState)
	mux.HandleFunc("POST /_lab/config", a.handleLabConfig)
	mux.HandleFunc("GET /_lab/metrics", a.handleLabMetrics)
	mux.HandleFunc("GET /_lab/buckets", a.handleLabBuckets)
	mux.HandleFunc("POST /_lab/shutdown", a.handleLabShutdown)

	if err := lab.Serve(lab.Env("LAB_ADDR", ":8080"), mux, log, nil); err != nil {
		log.Error("сервис остановлен с ошибкой", "err", err)
	}
}

func connect(ctx context.Context, url string, log *slog.Logger) (*pgxpool.Pool, error) {
	var last error
	for i := 0; i < 60; i++ {
		pool, err := pgxpool.New(ctx, url)
		if err == nil {
			if err = pool.Ping(ctx); err == nil {
				return pool, nil
			}
			pool.Close()
		}
		last = err
		if i == 0 {
			log.Info("ждём базу")
		}
		time.Sleep(time.Second)
	}
	return nil, last
}

// ── заказ ───────────────────────────────────────────────────────────────────

type createOrderRequest struct {
	Client         string `json:"client"`
	Restaurant     string `json:"restaurant"`
	Amount         int64  `json:"amount"`
	IdempotencyKey string `json:"idempotency_key"`
}

type orderResult struct {
	Order   int64  `json:"order"`
	Status  string `json:"status"`
	Charge  string `json:"charge"`
	Courier string `json:"courier"`
}

func (a *app) handleCreateOrder(w http.ResponseWriter, r *http.Request) {
	var req createOrderRequest
	if err := lab.ReadJSON(r, &req); err != nil {
		lab.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	attempt := a.attempts.Add(1)

	// Заказы и каталог делят один пул обработчиков — как и было в одном
	// процессе. Пока пул не ограничен (сцены m01–m04), это ничего не меняет;
	// в m05 именно отсюда берётся каскад: медленная зависимость заказа
	// съедает обработчиков, и падает каталог, который ни в чём не виноват.
	release, ok := a.ctl.gate.Acquire(r.Context())
	if !ok {
		w.Header().Set("Retry-After", "1")
		lab.WriteJSON(w, http.StatusServiceUnavailable,
			map[string]string{"error": "очередь заполнена", "code": "overloaded"})
		return
	}
	defer release()

	// Клиент отвалился по таймауту — а работа продолжается: соединение
	// закрыто, но заказ уже создан и платёж уже ушёл в эквайринг. Ровно это
	// делает первую попытку живой и оплачивает заказ дважды.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), 5*time.Minute)
	defer cancel()

	res, err := a.createOrder(ctx, req, attempt)
	if err != nil {
		a.log.Error("заказ не создан", "attempt", attempt, "err", err)
		lab.WriteJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
		return
	}

	// Есть ли кому отдать ответ? Контекст исходного запроса отменяется, когда
	// клиент закрыл соединение, — это наблюдение, а не догадка.
	if r.Context().Err() != nil {
		a.rec.Record(fmt.Sprintf("orders.lost#%d", attempt), map[string]string{
			"order": strconv.FormatInt(res.Order, 10), "status": res.Status,
		})
		a.log.Info("ответ некому отдать: клиент закрыл соединение",
			"attempt", attempt, "order", res.Order)
		return
	}
	a.rec.Record(fmt.Sprintf("orders.reply#%d", attempt), map[string]string{
		"order": strconv.FormatInt(res.Order, 10), "status": res.Status,
	})
	lab.WriteJSON(w, http.StatusCreated, res)
}

// createOrder — весь бизнес-процесс в одной транзакции: так его написали
// когда сервис был один, и так он работает до m03.
func (a *app) createOrder(ctx context.Context, req createOrderRequest, attempt int64) (orderResult, error) {
	var res orderResult

	tx, err := a.pool.Begin(ctx)
	if err != nil {
		return res, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var orderID int64
	err = tx.QueryRow(ctx,
		`INSERT INTO orders (client, restaurant, amount, status) VALUES ($1,$2,$3,'created') RETURNING id`,
		req.Client, req.Restaurant, req.Amount).Scan(&orderID)
	if err != nil {
		return res, fmt.Errorf("orders insert: %w", err)
	}

	var paymentID int64
	err = tx.QueryRow(ctx,
		`INSERT INTO payments (order_id, amount, status) VALUES ($1,$2,'pending') RETURNING id`,
		orderID, req.Amount).Scan(&paymentID)
	if err != nil {
		return res, fmt.Errorf("payments insert: %w", err)
	}

	order := strconv.FormatInt(orderID, 10)
	amount := strconv.FormatInt(req.Amount, 10)

	// модуль orders зовёт модуль payments — вызов внутри одного процесса
	a.rec.Record(fmt.Sprintf("orders.call.payments#%d", attempt),
		map[string]string{"order": order, "amount": amount})
	a.log.Info("оплата заказа", "attempt", attempt, "order", orderID, "amount", req.Amount)

	chargeID, err := a.charge(ctx, orderID, req.Amount, req.IdempotencyKey, attempt)
	if err != nil {
		return res, err
	}

	if _, err = tx.Exec(ctx, `UPDATE payments SET status='charged', charge_id=$1 WHERE id=$2`,
		chargeID, paymentID); err != nil {
		return res, fmt.Errorf("payments update: %w", err)
	}
	if _, err = tx.Exec(ctx, `UPDATE orders SET status='paid' WHERE id=$1`, orderID); err != nil {
		return res, fmt.Errorf("orders update: %w", err)
	}

	courier := couriers[int(orderID)%len(couriers)]
	if _, err = tx.Exec(ctx, `INSERT INTO courier_assignments (order_id, courier) VALUES ($1,$2)`,
		orderID, courier); err != nil {
		return res, fmt.Errorf("courier insert: %w", err)
	}
	// Модуль уведомлений. В профиле mono он здесь же, в этой же транзакции;
	// в профиле split — за сетью, и у того же вызова появляется третий исход.
	body := fmt.Sprintf("Заказ %d оплачен, курьер %s уже едет", orderID, courier)
	if _, err = a.notify(ctx, tx, orderID, body, attempt); err != nil {
		if !a.ctl.knobs().Degrade {
			return res, fmt.Errorf("уведомления: %w", err)
		}
		// Деградация: заказ принимается без необязательной части. Это
		// решение вызывающего, а не случайность, и оно попадает в журнал.
		a.rec.Record(fmt.Sprintf("orders.degrade#%d", attempt),
			map[string]string{"order": order, "reason": err.Error()})
		a.log.Info("уведомление не отправлено, заказ принят без него",
			"order", orderID, "err", err)
	}

	if err = tx.Commit(ctx); err != nil {
		return res, fmt.Errorf("commit: %w", err)
	}

	a.rec.Record(fmt.Sprintf("orders.recv.payments#%d", attempt),
		map[string]string{"order": order, "status": "paid", "courier": courier, "charge": chargeID})
	a.log.Info("заказ оплачен", "attempt", attempt, "order", orderID,
		"charge", chargeID, "courier", courier)

	return orderResult{Order: orderID, Status: "paid", Charge: chargeID, Courier: courier}, nil
}

// charge — модуль payments зовёт внешний эквайринг.
func (a *app) charge(ctx context.Context, orderID, amount int64, key string, attempt int64) (string, error) {
	payload, _ := json.Marshal(map[string]any{
		"order": orderID, "amount": amount, "idempotency_key": key,
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.acquirer+"/v1/charge",
		bytes.NewReader(payload))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")

	a.rec.Record(fmt.Sprintf("payments.wait.acquirer#%d", attempt), map[string]string{
		"order": strconv.FormatInt(orderID, 10), "amount": strconv.FormatInt(amount, 10),
	})

	resp, err := a.client.Do(req)
	if err != nil {
		a.rec.Record(fmt.Sprintf("payments.fail.acquirer#%d", attempt),
			map[string]string{"error": err.Error()})
		return "", fmt.Errorf("эквайринг недоступен: %w", err)
	}
	defer resp.Body.Close()

	var body struct {
		Charge string `json:"charge"`
		Status string `json:"status"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&body)

	if resp.StatusCode != http.StatusOK {
		a.rec.Record(fmt.Sprintf("payments.fail.acquirer#%d", attempt), map[string]string{
			"code": strconv.Itoa(resp.StatusCode), "status": body.Status,
		})
		return "", fmt.Errorf("эквайринг ответил %d", resp.StatusCode)
	}

	a.rec.Record(fmt.Sprintf("payments.recv.acquirer#%d", attempt), map[string]string{
		"charge": body.Charge, "code": "200 OK",
	})
	return body.Charge, nil
}

func (a *app) handleGetOrder(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		lab.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "bad id"})
		return
	}
	var res orderResult
	err = a.pool.QueryRow(r.Context(),
		`SELECT o.id, o.status, COALESCE(p.charge_id,''), COALESCE(c.courier,'')
		   FROM orders o
		   LEFT JOIN payments p ON p.order_id = o.id
		   LEFT JOIN courier_assignments c ON c.order_id = o.id
		  WHERE o.id = $1`, id).Scan(&res.Order, &res.Status, &res.Charge, &res.Courier)
	if err != nil {
		lab.WriteJSON(w, http.StatusNotFound, map[string]string{"error": "заказ не найден"})
		return
	}
	lab.WriteJSON(w, http.StatusOK, res)
}

// ── управление сценой ───────────────────────────────────────────────────────

func (a *app) handlePrepare(w http.ResponseWriter, r *http.Request) {
	var req struct {
		FirstOrderID int64 `json:"first_order_id"`
	}
	if err := lab.ReadJSON(r, &req); err != nil {
		lab.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if req.FirstOrderID == 0 {
		req.FirstOrderID = 7714
	}

	ctx := r.Context()
	if _, err := a.pool.Exec(ctx,
		`TRUNCATE orders, payments, courier_assignments, notifications RESTART IDENTITY`); err != nil {
		lab.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if _, err := a.pool.Exec(ctx,
		fmt.Sprintf(`ALTER SEQUENCE order_id_seq RESTART WITH %d`, req.FirstOrderID)); err != nil {
		lab.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	a.rec.Clear()
	a.attempts.Store(0)
	a.log.Info("состояние сброшено", "first_order_id", req.FirstOrderID)
	lab.WriteJSON(w, http.StatusOK, map[string]any{"first_order_id": req.FirstOrderID})
}

type stateOrder struct {
	ID         int64  `json:"id"`
	Client     string `json:"client"`
	Restaurant string `json:"restaurant"`
	Amount     int64  `json:"amount"`
	Status     string `json:"status"`
}

type statePayment struct {
	OrderID int64  `json:"order"`
	Amount  int64  `json:"amount"`
	Status  string `json:"status"`
	Charge  string `json:"charge"`
}

type stateRow struct {
	OrderID int64  `json:"order"`
	Text    string `json:"text"`
}

func (a *app) handleState(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	state := map[string]any{}

	orders := []stateOrder{}
	rows, err := a.pool.Query(ctx,
		`SELECT id, client, restaurant, amount, status FROM orders ORDER BY id`)
	if err != nil {
		lab.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	for rows.Next() {
		var o stateOrder
		if err := rows.Scan(&o.ID, &o.Client, &o.Restaurant, &o.Amount, &o.Status); err != nil {
			lab.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		orders = append(orders, o)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		lab.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	payments := []statePayment{}
	prows, err := a.pool.Query(ctx,
		`SELECT order_id, amount, status, COALESCE(charge_id,'') FROM payments ORDER BY id`)
	if err != nil {
		lab.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	for prows.Next() {
		var p statePayment
		if err := prows.Scan(&p.OrderID, &p.Amount, &p.Status, &p.Charge); err != nil {
			lab.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		payments = append(payments, p)
	}
	prows.Close()

	assignments := []stateRow{}
	arows, err := a.pool.Query(ctx, `SELECT order_id, courier FROM courier_assignments ORDER BY id`)
	if err != nil {
		lab.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	for arows.Next() {
		var row stateRow
		if err := arows.Scan(&row.OrderID, &row.Text); err != nil {
			lab.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		assignments = append(assignments, row)
	}
	arows.Close()

	notifications := []stateRow{}
	nrows, err := a.pool.Query(ctx, `SELECT order_id, body FROM notifications ORDER BY id`)
	if err != nil {
		lab.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	for nrows.Next() {
		var row stateRow
		if err := nrows.Scan(&row.OrderID, &row.Text); err != nil {
			lab.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		notifications = append(notifications, row)
	}
	nrows.Close()

	var chargedTotal int64
	charged := 0
	for _, p := range payments {
		if p.Status == "charged" {
			charged++
			chargedTotal += p.Amount
		}
	}

	state["orders"] = orders
	state["payments"] = payments
	state["assignments"] = assignments
	state["notifications"] = notifications
	state["totals"] = map[string]any{
		"orders":        len(orders),
		"payments":      len(payments),
		"charged":       charged,
		"charged_total": chargedTotal,
		"assignments":   len(assignments),
		"notifications": len(notifications),
	}
	lab.WriteJSON(w, http.StatusOK, state)
}
