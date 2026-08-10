package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
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
