package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/kipyatl0/backend-architecture-lab/services/internal/lab"
)

// Сцены профиля broker. Общее у всех: отправитель один и тот же — сервис
// заказов, а меняется только то, как сообщение доставляется и когда
// получатель считает его обработанным.

// ── общая обвязка ───────────────────────────────────────────────────────────

// useBroker переводит сцену на сервисы профиля broker: наблюдения собираются
// с них, а не с монолита, которого в этих сценах нет в кадре.
func (r *Run) useBroker(withRelay bool) {
	r.Sources = []string{r.OrdersURL, r.CourierURL}
	if withRelay {
		r.Sources = append(r.Sources, r.RelayURL)
	}
}

func (r *Run) configureOrders(body map[string]any) error {
	return r.postJSON(r.OrdersURL+"/_lab/config", body, nil)
}

func (r *Run) configureCourier(body map[string]any) error {
	return r.postJSON(r.CourierURL+"/_lab/config", body, nil)
}

func (r *Run) configureRelay(body map[string]any) error {
	return r.postJSON(r.RelayURL+"/_lab/config", body, nil)
}

func (r *Run) prepareOrders(first int64) error {
	return r.postJSON(r.OrdersURL+"/_lab/prepare", map[string]any{"first_order_id": first}, nil)
}

// createOrders прогоняет через сервис заказов N заказов подряд и возвращает
// их номера. Клиент здесь — сама сцена: она играет ту же роль, что играла в
// первом уроке курса.
func (r *Run) createOrders(n int, client, restaurant string, amount int64) ([]int64, error) {
	c := &http.Client{Timeout: 30 * time.Second}
	var ids []int64
	for i := 0; i < n; i++ {
		body, _ := json.Marshal(map[string]any{
			"client": client, "restaurant": restaurant, "amount": amount,
		})
		var out struct {
			Order int64 `json:"order"`
		}
		resp, err := c.Post(r.OrdersURL+"/orders", "application/json", bytesReader(body))
		if err != nil {
			return ids, fmt.Errorf("заказ %d не создан: %w", i+1, err)
		}
		_ = json.NewDecoder(resp.Body).Decode(&out)
		resp.Body.Close()
		if resp.StatusCode >= 300 {
			return ids, fmt.Errorf("заказ %d отклонён: код %d", i+1, resp.StatusCode)
		}
		ids = append(ids, out.Order)
	}
	return ids, nil
}

// courierState — то, что курьеры видят у себя. Единственный честный источник
// ответа на вопрос «сколько раз это сообщение обработали».
type courierState struct {
	Assignments []struct {
		Order   int64  `json:"order"`
		Courier string `json:"courier"`
		Times   int    `json:"times"`
	} `json:"assignments"`
	Statuses     map[string]string `json:"statuses"`
	Sequence     []string          `json:"sequence"`
	Processed    int64             `json:"processed"`
	Duplicates   int64             `json:"duplicates"`
	Redelivered  int64             `json:"redelivered"`
	DeadLettered int64             `json:"dead_lettered"`
	Failed       int64             `json:"failed"`
}

func (r *Run) courierState() (courierState, error) {
	var s courierState
	err := r.getJSON(r.CourierURL+"/_lab/state", &s)
	return s, err
}

// waitState ждёт, пока состояние курьеров удовлетворит условию. Ждать по
// состоянию, а не по часам, — единственный способ не превратить сцену в
// лотерею на медленной машине.
func (r *Run) waitState(ctx context.Context, ok func(courierState) bool, limit time.Duration) (courierState, error) {
	deadline := time.Now().Add(limit)
	var last courierState
	for {
		s, err := r.courierState()
		if err == nil {
			last = s
			if ok(s) {
				return s, nil
			}
		}
		if time.Now().After(deadline) {
			return last, nil // расхождение поймает сверка со сценарием
		}
		select {
		case <-ctx.Done():
			return last, ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// waitProcessed ждёт, пока курьеры обработают ожидаемое число сообщений.
// Ждать по состоянию, а не по часам, — единственный способ не превратить
// сцену в лотерею на медленной машине.
func (r *Run) waitProcessed(ctx context.Context, want int64, limit time.Duration) (courierState, error) {
	deadline := time.Now().Add(limit)
	var last courierState
	for {
		s, err := r.courierState()
		if err == nil {
			last = s
			if s.Processed >= want {
				return s, nil
			}
		}
		if time.Now().After(deadline) {
			return last, nil // расхождение поймает сверка со сценарием
		}
		select {
		case <-ctx.Done():
			return last, ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// ── сцена 13 — очередь и лог на одном потоке (m06 l02) ──────────────────────
//
// Один и тот же поток из пяти событий проходит сначала через очередь, потом
// через лог. Разница видна ровно в одном месте: что достанется тому, кто
// пришёл читать вторым.
func scene13(ctx context.Context, r *Run) error {
	cfg := r.Script.Config
	// Наблюдатель здесь — сама сцена. Сервисы в этой сцене ничего не
	// рассказывают: предмет не в том, что делает каждый из них, а в том, что
	// достаётся второму читателю.
	r.Sources = nil

	if err := r.configureCourier(map[string]any{"reset": true, "source": "none"}); err != nil {
		return fmt.Errorf("курьеры не настроены — поднят ли профиль broker? %w", err)
	}
	if err := r.configureOrders(map[string]any{
		"reset": true, "write_mode": "direct", "target": "amqp",
	}); err != nil {
		return fmt.Errorf("заказы не настроены: %w", err)
	}
	if err := r.prepareOrders(cfg.FirstOrderID); err != nil {
		return err
	}

	// Топология и лог пересоздаются целиком: сцена начинается с пустого
	// брокера, иначе число сообщений в выводе зависело бы от прошлых прогонов.
	conn, err := lab.DialAMQPWait(ctx, r.AMQPURL, 30*time.Second)
	if err != nil {
		return fmt.Errorf("очередь недоступна: %w", err)
	}
	defer conn.Close()
	if err := conn.Declare(lab.Topology{}); err != nil {
		return fmt.Errorf("топология не объявлена: %w", err)
	}
	if err := lab.WaitKafka(ctx, r.Brokers, 60*time.Second); err != nil {
		return err
	}
	if err := lab.RecreateTopic(ctx, r.Brokers, lab.TopicParts); err != nil {
		return fmt.Errorf("лог не пересоздан: %w", err)
	}

	total := cfg.Messages
	if total == 0 {
		total = 5
	}
	r.Set("total", strconv.Itoa(total))
	r.Set("first_order", strconv.FormatInt(cfg.FirstOrderID, 10))
	r.Set("last_order", strconv.FormatInt(cfg.FirstOrderID+int64(total)-1, 10))
	r.Set("parts", strconv.Itoa(lab.TopicParts))

	r.T0 = time.Now()

	// ── модель очереди ──────────────────────────────────────────────────────

	r.Record("queue.published", nil)
	if _, err := r.createOrders(total, cfg.Client, cfg.Restaurant, cfg.Amount); err != nil {
		return err
	}

	// Курьеры подписываются и забирают всё, подтверждая каждое сообщение.
	if err := r.configureCourier(map[string]any{"source": "amqp", "ack": "after"}); err != nil {
		return err
	}
	state, err := r.waitProcessed(ctx, int64(total), 30*time.Second)
	if err != nil {
		return err
	}
	r.Record("queue.consumed", map[string]string{"n": strconv.FormatInt(state.Processed, 10)})
	r.Set("queue_consumed", strconv.FormatInt(state.Processed, 10))

	// Потребителя отключаем: иначе он же и съест то, что мы сейчас пойдём
	// искать в очереди.
	if err := r.configureCourier(map[string]any{"source": "none"}); err != nil {
		return err
	}

	depth, err := conn.Depth(lab.QueueCour)
	if err != nil {
		return err
	}
	r.Record("queue.depth", map[string]string{"n": strconv.Itoa(depth)})
	r.Set("queue_left", strconv.Itoa(depth))

	// Второй читатель приходит к той же очереди — и не находит ничего.
	rest, err := conn.Drain(lab.QueueCour)
	if err != nil {
		return err
	}
	r.Record("queue.audit", map[string]string{"n": strconv.Itoa(len(rest))})
	r.Set("queue_audit", strconv.Itoa(len(rest)))

	// ── модель лога ─────────────────────────────────────────────────────────

	if err := r.configureOrders(map[string]any{"write_mode": "direct", "target": "kafka"}); err != nil {
		return err
	}
	if err := r.prepareOrders(cfg.FirstOrderID + int64(total)); err != nil {
		return err
	}

	r.SleepUntil(cfg.SecondPhaseAtS)
	r.Record("log.published", nil)
	if _, err := r.createOrders(total, cfg.Client, cfg.Restaurant, cfg.Amount); err != nil {
		return err
	}

	if err := r.configureCourier(map[string]any{
		"reset": true, "source": "kafka", "group": "courier", "from_start": true, "ack": "after",
	}); err != nil {
		return err
	}
	state, err = r.waitProcessed(ctx, int64(total), 30*time.Second)
	if err != nil {
		return err
	}
	r.Record("log.consumed", map[string]string{"n": strconv.FormatInt(state.Processed, 10)})
	r.Set("log_consumed", strconv.FormatInt(state.Processed, 10))

	if err := r.configureCourier(map[string]any{"source": "none"}); err != nil {
		return err
	}

	// Новая группа читает тот же лог с начала. Записи никуда не делись:
	// курсор у неё свой, и чужое чтение его не двигало.
	audit, err := lab.ReadAll(ctx, r.Brokers, "audit", 3*time.Second)
	if err != nil {
		return err
	}
	r.Record("log.audit", map[string]string{"n": strconv.Itoa(len(audit))})
	r.Set("log_audit", strconv.Itoa(len(audit)))

	end, err := lab.EndOffsets(ctx, r.Brokers)
	if err != nil {
		return err
	}
	r.Set("log_left", strconv.FormatInt(end, 10))
	return r.collect()
}

// ── сцена 14 — одно сообщение обработано дважды (m06 l03) ───────────────────
//
// Ничего не сломалось: брокер работает как обещал, потребитель написан как
// обычно. Просто процесс умер между обработкой и подтверждением — и брокер,
// который об обработке не знал, выдал сообщение снова.
func scene14(ctx context.Context, r *Run) error {
	cfg := r.Script.Config
	// Здесь наблюдатель — сам потребитель: предмет сцены в том, что с ним
	// произошло, а не в том, что увидела снаружи сцена.
	r.Sources = []string{r.CourierURL}

	if err := r.configureCourier(map[string]any{"reset": true, "source": "none"}); err != nil {
		return fmt.Errorf("курьеры не настроены — поднят ли профиль broker? %w", err)
	}
	if err := r.configureOrders(map[string]any{
		"reset": true, "write_mode": "direct", "target": "amqp",
	}); err != nil {
		return err
	}
	if err := r.prepareOrders(cfg.FirstOrderID); err != nil {
		return err
	}

	conn, err := lab.DialAMQPWait(ctx, r.AMQPURL, 30*time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()
	if err := conn.Declare(lab.Topology{}); err != nil {
		return err
	}

	total := cfg.Messages
	if total == 0 {
		total = 3
	}
	dieAfter := cfg.DieAfter
	if dieAfter == 0 {
		dieAfter = 2
	}
	r.Set("total", strconv.Itoa(total))
	r.Set("first_order", strconv.FormatInt(cfg.FirstOrderID, 10))
	r.Set("last_order", strconv.FormatInt(cfg.FirstOrderID+int64(total)-1, 10))
	r.Set("dup_order", strconv.FormatInt(cfg.FirstOrderID+int64(dieAfter)-1, 10))

	r.T0 = time.Now()
	r.Record("queue.published", nil)
	if _, err := r.createOrders(total, cfg.Client, cfg.Restaurant, cfg.Amount); err != nil {
		return err
	}

	// Потребитель обычный: подтверждает после обработки. Ровно так его и
	// написали бы, и ровно поэтому сцена показывает не ошибку, а норму.
	if err := r.configureCourier(map[string]any{
		"source": "amqp", "ack": "after", "prefetch": 1,
		"die_after": dieAfter, "restart_ms": cfg.RestartMS(),
	}); err != nil {
		return err
	}

	// Ждём, пока обработок станет больше, чем сообщений: это и есть повтор.
	state, err := r.waitState(ctx, func(s courierState) bool {
		return s.Processed >= int64(total+1)
	}, 40*time.Second)
	if err != nil {
		return err
	}
	if err := r.configureCourier(map[string]any{"source": "none"}); err != nil {
		return err
	}

	dup := 0
	for _, a := range state.Assignments {
		if a.Times > 1 {
			dup++
		}
	}
	r.Set("processed", strconv.FormatInt(state.Processed, 10))
	r.Set("assignments", strconv.Itoa(len(state.Assignments)))
	r.Set("dup_count", strconv.Itoa(dup))
	r.Set("redelivered", strconv.FormatInt(state.Redelivered, 10))
	return r.collect()
}

// ── сцена 15 — отравленное сообщение (m06 l04) ──────────────────────────────
//
// Два прогона одного и того же потока. Разница ровно одна: у очереди во втором
// есть предел повторных доставок. Всё остальное — и поток, и потребитель, и
// неудачное сообщение — не меняется.
func scene15(ctx context.Context, r *Run) error {
	cfg := r.Script.Config
	r.Sources = nil

	conn, err := lab.DialAMQPWait(ctx, r.AMQPURL, 30*time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()

	total := cfg.Messages
	if total == 0 {
		total = 3
	}
	poison := cfg.FirstOrderID + int64(cfg.PoisonOffset)
	limit := cfg.DeliveryLimit
	if limit == 0 {
		limit = 3
	}

	r.Set("total", strconv.Itoa(total))
	r.Set("poison", strconv.FormatInt(poison, 10))
	r.Set("limit", strconv.Itoa(limit))
	r.Set("first_order", strconv.FormatInt(cfg.FirstOrderID, 10))
	r.Set("last_order", strconv.FormatInt(cfg.FirstOrderID+int64(total)-1, 10))

	// ── прогон 1: предела повторных доставок нет ────────────────────────────

	phase := func(first int64, topology lab.Topology, poisonOrder int64,
		wait time.Duration) (courierState, int, int, error) {

		if err := r.configureCourier(map[string]any{"reset": true, "source": "none"}); err != nil {
			return courierState{}, 0, 0, err
		}
		if err := conn.Declare(topology); err != nil {
			return courierState{}, 0, 0, err
		}
		if err := r.configureOrders(map[string]any{
			"reset": true, "write_mode": "direct", "target": "amqp",
		}); err != nil {
			return courierState{}, 0, 0, err
		}
		if err := r.prepareOrders(first); err != nil {
			return courierState{}, 0, 0, err
		}
		if _, err := r.createOrders(total, cfg.Client, cfg.Restaurant, cfg.Amount); err != nil {
			return courierState{}, 0, 0, err
		}
		if err := r.configureCourier(map[string]any{
			"source": "amqp", "ack": "after", "prefetch": 1, "poison_order": poisonOrder,
		}); err != nil {
			return courierState{}, 0, 0, err
		}
		// Ждём фиксированное окно: предмет наблюдения — что успело
		// произойти за это время, а не «когда всё кончится». В первом
		// прогоне оно не кончится никогда.
		select {
		case <-ctx.Done():
			return courierState{}, 0, 0, ctx.Err()
		case <-time.After(wait):
		}
		state, err := r.courierState()
		if err != nil {
			return courierState{}, 0, 0, err
		}
		if err := r.configureCourier(map[string]any{"source": "none"}); err != nil {
			return courierState{}, 0, 0, err
		}
		depth, err := conn.Depth(lab.QueueCour)
		if err != nil {
			return courierState{}, 0, 0, err
		}
		dead, err := conn.Drain(lab.QueueDead)
		if err != nil {
			return courierState{}, 0, 0, err
		}
		return state, depth, len(dead), nil
	}

	window := time.Duration(cfg.WindowMS()) * time.Millisecond

	r.T0 = time.Now()
	r.Record("noLimit.published", nil)

	state, _, dead, err := phase(cfg.FirstOrderID, lab.Topology{DeliveryLimit: -1}, poison, window)
	if err != nil {
		return err
	}
	r.Record("noLimit.stuck", nil)
	r.Set("a_done", strconv.FormatInt(state.Processed, 10))
	r.Set("a_failed", strconv.FormatInt(state.Failed, 10))
	r.Set("a_left", strconv.Itoa(total-int(state.Processed)))
	r.Set("a_dead", strconv.Itoa(dead))
	r.Measure("no_limit", "done", float64(state.Processed))
	r.Measure("no_limit", "left", float64(total)-float64(state.Processed))
	r.Measure("no_limit", "dead", float64(dead))

	// ── прогон 2: у очереди есть предел повторных доставок ──────────────────

	r.SleepUntil(cfg.SecondPhaseAtS)
	r.Record("limit.published", nil)

	second := cfg.FirstOrderID + int64(total)
	state, _, dead, err = phase(second, lab.Topology{DeliveryLimit: limit}, poison+int64(total), window)
	if err != nil {
		return err
	}
	r.Record("limit.dead", nil)
	r.Set("b_done", strconv.FormatInt(state.Processed, 10))
	r.Set("b_failed", strconv.FormatInt(state.Failed, 10))
	r.Set("b_left", strconv.Itoa(total-int(state.Processed)))
	r.Set("b_dead", strconv.Itoa(dead))
	r.Measure("with_limit", "done", float64(state.Processed))
	r.Measure("with_limit", "left", float64(total)-float64(state.Processed))
	r.Measure("with_limit", "dead", float64(dead))
	r.Set("b_poison", strconv.FormatInt(poison+int64(total), 10))
	r.Set("second_first", strconv.FormatInt(second, 10))
	r.Set("second_last", strconv.FormatInt(second+int64(total)-1, 10))
	return r.collect()
}

// ── сцена 16 — события применены не в том порядке (m06 l04) ─────────────────
//
// Брокер обещает порядок внутри партиции и обещание держит: все три события
// заказа лежат в одной партиции подряд. Ломает порядок не он, а потребитель —
// в тот момент, когда берётся за несколько сообщений сразу.
func scene16(ctx context.Context, r *Run) error {
	cfg := r.Script.Config
	r.Sources = nil

	if err := lab.WaitKafka(ctx, r.Brokers, 60*time.Second); err != nil {
		return err
	}

	order := cfg.FirstOrderID
	// Три события одного заказа. Время обработки убывает — и это не подгонка
	// под результат: подтверждение оплаты дороже отметки об отмене почти в
	// любой системе.
	events := []struct {
		Type   string
		Seq    int
		WorkMS int
		Short  string
	}{
		{"order.paid", 1, 300, "paid"},
		{"order.assigned", 2, 100, "assigned"},
		{"order.cancelled", 3, 10, "cancelled"},
	}

	var natural []string
	for _, e := range events {
		natural = append(natural, e.Short)
	}
	r.Set("order", strconv.FormatInt(order, 10))
	r.Set("natural", strings.Join(natural, " → "))
	r.Set("parts", strconv.Itoa(lab.TopicParts))

	// pass прогоняет один и тот же лог через потребителя с заданным числом
	// обработчиков и возвращает порядок, в котором события были применены.
	pass := func(workers int, group string) ([]string, string, error) {
		if err := r.configureCourier(map[string]any{"reset": true, "source": "none"}); err != nil {
			return nil, "", err
		}
		if err := lab.RecreateTopic(ctx, r.Brokers, lab.TopicParts); err != nil {
			return nil, "", err
		}
		if err := r.configureOrders(map[string]any{"reset": true, "write_mode": "none"}); err != nil {
			return nil, "", err
		}
		for _, e := range events {
			if err := r.postJSON(r.OrdersURL+"/_lab/emit", map[string]any{
				"type": e.Type, "order": order, "seq": e.Seq,
				"work_ms": e.WorkMS, "target": "kafka",
			}, nil); err != nil {
				return nil, "", err
			}
		}
		if err := r.configureCourier(map[string]any{
			"source": "kafka", "group": group, "from_start": true,
			"ack": "after", "workers": workers,
		}); err != nil {
			return nil, "", err
		}
		state, err := r.waitState(ctx, func(s courierState) bool {
			return s.Processed >= int64(len(events))
		}, 40*time.Second)
		if err != nil {
			return nil, "", err
		}
		if err := r.configureCourier(map[string]any{"source": "none"}); err != nil {
			return nil, "", err
		}

		var applied []string
		for _, s := range state.Sequence {
			parts := strings.SplitN(s, ":", 2)
			if len(parts) == 2 {
				applied = append(applied, parts[1])
			}
		}
		final := ""
		if len(applied) > 0 {
			final = applied[len(applied)-1]
		}
		return applied, final, nil
	}

	r.T0 = time.Now()

	applied, final, err := pass(1, "courier-one")
	if err != nil {
		return err
	}
	r.Set("one_applied", strings.Join(applied, " → "))
	r.Set("one_final", final)
	r.Measure("one", "correct", boolToFloat(final == events[len(events)-1].Short))

	applied, final, err = pass(len(events)+1, "courier-many")
	if err != nil {
		return err
	}
	r.Set("many_applied", strings.Join(applied, " → "))
	r.Set("many_final", final)
	r.Set("workers", strconv.Itoa(len(events)+1))
	r.Measure("many", "correct", boolToFloat(final == events[len(events)-1].Short))
	return nil
}

func boolToFloat(v bool) float64 {
	if v {
		return 1
	}
	return 0
}

// ordersState — то, что сервис заказов видит у себя: свои заказы и свой
// исходящий ящик. Чужих данных здесь нет и быть не может.
type ordersState struct {
	Orders []struct {
		ID     int64  `json:"id"`
		Status string `json:"status"`
		Charge string `json:"charge"`
	} `json:"orders"`
	Outbox []struct {
		ID    int64  `json:"id"`
		Order int64  `json:"order"`
		Type  string `json:"type"`
		Sent  bool   `json:"sent"`
	} `json:"outbox"`
	Saga []string `json:"saga"`
}

func (r *Run) ordersState() (ordersState, error) {
	var s ordersState
	err := r.getJSON(r.OrdersURL+"/_lab/state", &s)
	return s, err
}

// waitOrdersDown ждёт, пока сервис заказов действительно исчезнет. Без этого
// шага сцена спрашивает «поднялся?» у процесса, который ещё не умирал, и
// печатает подъём раньше смерти.
func (r *Run) waitOrdersDown(ctx context.Context, limit time.Duration) error {
	deadline := time.Now().Add(limit)
	for {
		if err := r.getJSON(r.OrdersURL+"/_lab/state", nil); err != nil {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("сервис заказов не умер за %s — сцена не сыграна", limit)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// waitOrdersUp ждёт, пока сервис заказов поднимется после смерти. Платформа
// поднимает его сама — сцена только дожидается, как дождался бы дежурный.
func (r *Run) waitOrdersUp(ctx context.Context, limit time.Duration) error {
	deadline := time.Now().Add(limit)
	for {
		if err := r.getJSON(r.OrdersURL+"/_lab/state", nil); err == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("сервис заказов не поднялся за %s", limit)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// ── сцена 17 — двойная запись (m07 l01) ─────────────────────────────────────
//
// Запись в базу и отправка события — два разных действия с двумя разными
// исходами. Между ними есть щель, и однажды процесс умирает именно в ней.
// Никакой ошибки при этом не возникает: клиент получил ответ, база
// зафиксирована, событие не ушло, и никто об этом не узнал.
func scene17(ctx context.Context, r *Run) error {
	cfg := r.Script.Config
	r.Sources = nil

	if err := r.configureCourier(map[string]any{"reset": true, "source": "none"}); err != nil {
		return fmt.Errorf("курьеры не настроены — поднят ли профиль broker? %w", err)
	}
	if err := r.configureRelay(map[string]any{"reset": true, "enabled": false}); err != nil {
		return fmt.Errorf("отправитель ящика не настроен: %w", err)
	}
	conn, err := lab.DialAMQPWait(ctx, r.AMQPURL, 30*time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()
	if err := conn.Declare(lab.Topology{}); err != nil {
		return err
	}
	if err := r.configureOrders(map[string]any{
		"reset": true, "write_mode": "direct", "target": "amqp",
	}); err != nil {
		return err
	}
	if err := r.prepareOrders(cfg.FirstOrderID); err != nil {
		return err
	}

	total := cfg.Messages
	if total == 0 {
		total = 3
	}
	last := cfg.FirstOrderID + int64(total) - 1
	r.Set("total", strconv.Itoa(total))
	r.Set("first_order", strconv.FormatInt(cfg.FirstOrderID, 10))
	r.Set("last_order", strconv.FormatInt(last, 10))
	r.Set("good", strconv.Itoa(total-1))
	r.Set("amount", strconv.FormatInt(cfg.Amount, 10))

	// Потребитель работает всё это время: он и покажет, что до него дошло.
	if err := r.configureCourier(map[string]any{"source": "amqp", "ack": "after"}); err != nil {
		return err
	}

	r.T0 = time.Now()

	// Обычные заказы: запись прошла, событие ушло, курьер назначен.
	r.Record("orders.normal", nil)
	if _, err := r.createOrders(total-1, cfg.Client, cfg.Restaurant, cfg.Amount); err != nil {
		return err
	}
	if _, err := r.waitProcessed(ctx, int64(total-1), 20*time.Second); err != nil {
		return err
	}
	r.Record("courier.normal", nil)

	// Тот же самый заказ, но процесс умирает между фиксацией и отправкой.
	r.SleepUntil(cfg.SecondPhaseAtS)
	if err := r.configureOrders(map[string]any{"crash_after_commit": true}); err != nil {
		return err
	}
	r.Record("client.last", nil)
	if _, err := r.createOrders(1, cfg.Client, cfg.Restaurant, cfg.Amount); err != nil {
		return err
	}
	if err := r.waitOrdersDown(ctx, 15*time.Second); err != nil {
		return err
	}
	r.Record("orders.die", nil)

	if err := r.waitOrdersUp(ctx, 60*time.Second); err != nil {
		return err
	}
	r.Record("orders.up", nil)

	// Даём потребителю столько же времени, сколько было у нормальных заказов:
	// если бы событие ушло, оно бы уже дошло.
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(time.Duration(cfg.WindowMS()) * time.Millisecond):
	}

	state, err := r.courierState()
	if err != nil {
		return err
	}
	if err := r.configureCourier(map[string]any{"source": "none"}); err != nil {
		return err
	}
	orders, err := r.ordersState()
	if err != nil {
		return err
	}

	r.Record("silence", nil)
	r.Set("in_db", strconv.Itoa(len(orders.Orders)))
	r.Set("delivered", strconv.FormatInt(state.Processed, 10))
	r.Set("assigned", strconv.Itoa(len(state.Assignments)))
	return r.collect()
}

// ── сцена 18 — исходящий ящик под тем же отказом (m07 l02) ──────────────────
//
// Тот же самый отказ, что и в сцене 17, и тот же самый момент смерти. Разница
// одна: событие ложится в базу той же транзакцией, что и заказ. Оно переживает
// смерть процесса — и приходит дважды, потому что «минимум один раз» никуда
// не делось.
func scene18(ctx context.Context, r *Run) error {
	cfg := r.Script.Config
	r.Sources = nil

	if err := r.configureCourier(map[string]any{"reset": true, "source": "none"}); err != nil {
		return fmt.Errorf("курьеры не настроены — поднят ли профиль broker? %w", err)
	}
	conn, err := lab.DialAMQPWait(ctx, r.AMQPURL, 30*time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()
	if err := conn.Declare(lab.Topology{}); err != nil {
		return err
	}
	// Ящик пишется той же транзакцией, что и заказ: либо есть оба, либо нет
	// ни того ни другого.
	if err := r.configureOrders(map[string]any{"reset": true, "write_mode": "outbox"}); err != nil {
		return err
	}
	if err := r.prepareOrders(cfg.FirstOrderID); err != nil {
		return err
	}
	if err := r.configureRelay(map[string]any{
		"reset": true, "enabled": true, "target": "amqp", "die_after_publish": false,
	}); err != nil {
		return fmt.Errorf("отправитель ящика не настроен: %w", err)
	}
	if err := r.configureCourier(map[string]any{"source": "amqp", "ack": "after"}); err != nil {
		return err
	}

	total := cfg.Messages
	if total == 0 {
		total = 3
	}
	last := cfg.FirstOrderID + int64(total) - 1
	r.Set("total", strconv.Itoa(total))
	r.Set("first_order", strconv.FormatInt(cfg.FirstOrderID, 10))
	r.Set("last_order", strconv.FormatInt(last, 10))
	r.Set("good", strconv.Itoa(total-1))
	r.Set("amount", strconv.FormatInt(cfg.Amount, 10))

	r.T0 = time.Now()

	r.Record("orders.normal", nil)
	if _, err := r.createOrders(total-1, cfg.Client, cfg.Restaurant, cfg.Amount); err != nil {
		return err
	}
	if _, err := r.waitProcessed(ctx, int64(total-1), 25*time.Second); err != nil {
		return err
	}
	r.Record("courier.normal", nil)

	// Тот же отказ, что и в прошлой сцене, плюс второй: отправитель ящика
	// умирает между публикацией и отметкой.
	r.SleepUntil(cfg.SecondPhaseAtS)
	if err := r.configureRelay(map[string]any{"die_after_publish": true}); err != nil {
		return err
	}
	if err := r.configureOrders(map[string]any{"crash_after_commit": true}); err != nil {
		return err
	}
	r.Record("client.last", nil)
	if _, err := r.createOrders(1, cfg.Client, cfg.Restaurant, cfg.Amount); err != nil {
		return err
	}
	if err := r.waitOrdersDown(ctx, 15*time.Second); err != nil {
		return err
	}
	r.Record("orders.die", nil)

	// Событие пережило смерть владельца данных: оно лежит в его же базе, и
	// отправитель ящика — отдельный процесс, которому эта смерть не помешала.
	// Ждём доставку раньше, чем подъём: она и правда случается раньше.
	state, err := r.waitState(ctx, func(s courierState) bool {
		return s.Processed >= int64(total)
	}, 40*time.Second)
	if err != nil {
		return err
	}
	r.Record("relay.delivered", nil)

	// А теперь — цена приёма: отметка не пережила смерть отправителя, и то же
	// событие уходит второй раз.
	state, err = r.waitState(ctx, func(s courierState) bool {
		return s.Processed >= int64(total+1)
	}, 40*time.Second)
	if err != nil {
		return err
	}
	r.Record("relay.again", nil)

	if err := r.waitOrdersUp(ctx, 60*time.Second); err != nil {
		return err
	}
	r.Record("orders.up", nil)

	if err := r.configureCourier(map[string]any{"source": "none"}); err != nil {
		return err
	}
	orders, err := r.ordersState()
	if err != nil {
		return err
	}
	dup := 0
	for _, a := range state.Assignments {
		if a.Times > 1 {
			dup++
		}
	}

	r.Set("in_db", strconv.Itoa(len(orders.Orders)))
	r.Set("in_outbox", strconv.Itoa(len(orders.Outbox)))
	r.Set("delivered", strconv.FormatInt(state.Processed, 10))
	r.Set("assigned", strconv.Itoa(len(state.Assignments)))
	r.Set("dup_count", strconv.Itoa(dup))
	return r.collect()
}

// runSagaScene — общая часть сцен 20 и 21. Отличаются они ровно одним: удаётся
// ли компенсация. Всё остальное — тот же процесс, сломавшийся на том же шаге.
func (r *Run) runSagaScene(ctx context.Context, refundMode string, manual bool) error {
	cfg := r.Script.Config
	// Наблюдатель — сервис заказов: он и есть оркестратор процесса, и журнал
	// шагов ведёт он.
	r.Sources = []string{r.OrdersURL}

	if err := r.postJSON(r.AcquirerURL+"/_lab/config", map[string]any{
		"delay_ms": 0, "mode": "ok", "idempotent": true,
		"refund_mode": refundMode, "reset": true,
	}, nil); err != nil {
		return err
	}
	if err := r.postJSON(r.NotifierURL+"/_lab/config", map[string]any{
		"mode": "ok", "strict": false, "reset": true,
	}, nil); err != nil {
		return err
	}
	conn, err := lab.DialAMQPWait(ctx, r.AMQPURL, 30*time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()
	if err := conn.Declare(lab.Topology{ManualReview: manual}); err != nil {
		return err
	}
	if err := r.configureRelay(map[string]any{"reset": true, "enabled": false}); err != nil {
		return err
	}
	// Курьеры отказывают на прямом шаге: свободных курьеров нет.
	if err := r.configureCourier(map[string]any{
		"reset": true, "source": "none", "assign_fail": true,
	}); err != nil {
		return err
	}
	if err := r.configureOrders(map[string]any{
		"reset": true, "write_mode": "none", "saga": true, "compensate": true,
	}); err != nil {
		return err
	}
	if err := r.prepareOrders(cfg.FirstOrderID); err != nil {
		return err
	}

	order := strconv.FormatInt(cfg.FirstOrderID, 10)
	r.Set("order", order)
	r.Set("amount", strconv.FormatInt(cfg.Amount, 10))
	// Идентификатор списания приходит из журнала провайдера, а не выдумывается:
	// в очередь ручного разбора уходит именно он.
	r.Set("charge", "ch_1")

	r.T0 = time.Now()
	r.Record("client.call", nil)
	if _, err := r.createOrders(1, cfg.Client, cfg.Restaurant, cfg.Amount); err == nil {
		return fmt.Errorf("процесс должен был сломаться на назначении курьера, но прошёл целиком")
	}
	r.Record("client.recv", nil)

	state, err := r.ordersState()
	if err != nil {
		return err
	}
	status := ""
	for _, o := range state.Orders {
		if strconv.FormatInt(o.ID, 10) == order {
			status = o.Status
		}
	}
	var ledger struct {
		Charged  int `json:"charged"`
		Refunded int `json:"refunded"`
	}
	if err := r.getJSON(r.AcquirerURL+"/_lab/ledger", &ledger); err != nil {
		return err
	}
	var notif struct {
		Received int64 `json:"received"`
	}
	if err := r.getJSON(r.NotifierURL+"/_lab/state", &notif); err != nil {
		return err
	}

	r.Set("status", status)
	r.Set("charges", strconv.Itoa(ledger.Charged))
	r.Set("refunds", strconv.Itoa(ledger.Refunded))
	r.Set("notifications", strconv.FormatInt(notif.Received, 10))

	if manual {
		pending, err := conn.Drain(lab.QueueManu)
		if err != nil {
			return err
		}
		r.Set("manual", strconv.Itoa(len(pending)))
	}
	return r.collect()
}

// ── сцена 20 — сага: шаг упал, компенсации пошли (m07 l04) ──────────────────
//
// Общей транзакции у четырёх участников нет и быть не может. Значит, отменять
// сделанное приходится по одному действию за раз — и в обратном порядке.
func scene20(ctx context.Context, r *Run) error {
	return r.runSagaScene(ctx, "ok", false)
}

// ── сцена 21 — компенсация тоже не прошла (m07 l05) ─────────────────────────
//
// Компенсация — обычный вызов обычного сервиса, и отказывает она ровно так же,
// как прямой шаг. Разница в том, что дальше отменять уже нечего.
func scene21(ctx context.Context, r *Run) error {
	return r.runSagaScene(ctx, "error", true)
}
