// courier — назначение курьеров. С m06 это второй подписчик того же события,
// и именно он превращает отправку в веерную: отправитель по-прежнему не знает,
// сколько у него получателей.
//
// Весь сервис написан вокруг одного вопроса урока — когда потребитель отмечает
// сообщение обработанным. Поэтому подтверждение здесь всегда ручное, а момент
// подтверждения задаёт сцена.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/kipyatl0/backend-architecture-lab/services/internal/lab"
)

// Курьеры и правило назначения — канон стенда: назначение по номеру заказа,
// а не по счётчику. Счётчик разъехался бы, как только два заказа пошли
// одновременно, и сцена перестала бы повторяться.
var couriers = []string{"Айгуль", "Пётр", "Никита"}

func courierFor(order int64) string { return couriers[order%3] }

type config struct {
	// Откуда читаем: none — не читаем вовсе, amqp — модель очереди,
	// kafka — модель лога.
	Source string `json:"source"`
	Group  string `json:"group"`
	// Читать лог с начала или только новое. Для очереди смысла не имеет:
	// в ней «начала» нет, есть только то, что ещё не забрали.
	FromStart bool `json:"from_start"`

	// after — подтверждаем после обработки, before — до неё. Ровно эти два
	// значения и есть «что теряется» против «что дублируется».
	Ack string `json:"ack"`
	// Сколько сообщений потребитель берёт на себя вперёд.
	Prefetch int `json:"prefetch"`
	// Сколько сообщений обрабатывается одновременно. Больше одного — порядок
	// внутри партиции перестаёт существовать, сколько бы его ни обещал брокер.
	Workers int `json:"workers"`
	WorkMS  int `json:"work_ms"`

	// Умереть после N-го обработанного сообщения, не подтвердив его. Не
	// «сломаться» — именно умереть в самый обычный момент.
	DieAfter   int `json:"die_after"`
	RestartMS  int `json:"restart_ms"`
	Idempotent bool `json:"idempotent"`
	// Заказ, обработка которого не удаётся никогда. Одно такое сообщение и
	// останавливает поток, пока у очереди нет предела повторных доставок.
	PoisonOrder int64 `json:"poison_order"`

	// Синхронное назначение (сага m07) отказывает.
	AssignFail bool `json:"assign_fail"`
	// Отмена назначения (компенсация) отказывает.
	CompensateFail bool `json:"compensate_fail"`
}

func defaults() config {
	return config{
		Source: "none", Group: "courier", FromStart: true,
		Ack: "after", Prefetch: 1, Workers: 1, WorkMS: 0,
		DieAfter: 0, RestartMS: 1000, Idempotent: false, PoisonOrder: 0,
		AssignFail: false, CompensateFail: false,
	}
}

type assignment struct {
	Order   int64  `json:"order"`
	Courier string `json:"courier"`
	// Сколько раз это назначение было записано. Двойка здесь — не ошибка
	// стенда, а ровно то, что показывает сцена про повторную доставку.
	Times int `json:"times"`
}

type app struct {
	mu  sync.Mutex
	cfg config

	// Назначения и порядок их появления: сцена про порядок сверяет не только
	// итог, но и последовательность, в которой он собрался.
	assign map[int64]*assignment
	status map[int64]string
	order  []string

	seen map[string]bool // идентификаторы сообщений — для идемпотентного потребителя

	processed    atomic.Int64
	duplicates   atomic.Int64
	redelivered  atomic.Int64
	deadLettered atomic.Int64
	failed       atomic.Int64

	rec lab.Recorder

	// Управление подпиской: сцена включает и выключает потребителя целиком.
	cancel context.CancelFunc
	done   chan struct{}
}

func main() {
	log := lab.Logger("courier")
	a := &app{
		assign: map[int64]*assignment{},
		status: map[int64]string{},
		seen:   map[string]bool{},
	}
	a.setConfig(defaults())

	amqpURL := lab.Env("LAB_AMQP_URL", "amqp://lab:lab@rabbitmq:5672/")
	brokers := strings.Split(lab.Env("LAB_KAFKA_BROKERS", "kafka:9092"), ",")

	mux := http.NewServeMux()
	lab.Health(mux, func() error { return nil })
	mux.HandleFunc("GET /_lab/events", a.rec.Handler())

	// ── синхронное назначение: участник саги в m07 ─────────────────────────
	// Тот же сервис, но вызванный напрямую. Курс показывает на нём разницу
	// между «попросил и жду» и «сообщил и пошёл дальше».
	mux.HandleFunc("POST /_lab/assign", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Order int64 `json:"order"`
		}
		if err := lab.ReadJSON(r, &req); err != nil {
			lab.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		if a.config().AssignFail {
			a.failed.Add(1)
			log.Info("назначение не удалось", "order", req.Order)
			lab.WriteJSON(w, http.StatusServiceUnavailable,
				map[string]string{"code": "no_courier", "error": "свободных курьеров нет"})
			return
		}
		name := a.apply(req.Order, "assigned")
		log.Info("курьер назначен", "order", req.Order, "courier", name)
		lab.WriteJSON(w, http.StatusOK, map[string]any{"order": req.Order, "courier": name})
	})

	// Компенсация: снять назначение. Она такой же шаг, как и прямой, и точно
	// так же умеет не получиться — на этом стоит сцена 21.
	mux.HandleFunc("POST /_lab/unassign", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Order int64 `json:"order"`
		}
		if err := lab.ReadJSON(r, &req); err != nil {
			lab.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		if a.config().CompensateFail {
			a.failed.Add(1)
			log.Info("компенсация не удалась", "order", req.Order)
			lab.WriteJSON(w, http.StatusServiceUnavailable,
				map[string]string{"code": "unassign_failed", "error": "назначение снять не удалось"})
			return
		}
		a.mu.Lock()
		delete(a.assign, req.Order)
		a.status[req.Order] = "cancelled"
		a.order = append(a.order, fmt.Sprintf("%d:unassigned", req.Order))
		a.mu.Unlock()
		log.Info("назначение снято", "order", req.Order)
		lab.WriteJSON(w, http.StatusOK, map[string]any{"order": req.Order})
	})

	// ── управление сценой ─────────────────────────────────────────────────

	mux.HandleFunc("POST /_lab/config", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Source         *string `json:"source"`
			Group          *string `json:"group"`
			FromStart      *bool   `json:"from_start"`
			Ack            *string `json:"ack"`
			Prefetch       *int    `json:"prefetch"`
			Workers        *int    `json:"workers"`
			WorkMS         *int    `json:"work_ms"`
			DieAfter       *int    `json:"die_after"`
			RestartMS      *int    `json:"restart_ms"`
			Idempotent     *bool   `json:"idempotent"`
			PoisonOrder    *int64  `json:"poison_order"`
			AssignFail     *bool   `json:"assign_fail"`
			CompensateFail *bool   `json:"compensate_fail"`
			Reset          bool    `json:"reset"`
		}
		if err := lab.ReadJSON(r, &req); err != nil {
			lab.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}

		// Подписку всегда останавливаем до перенастройки: потребитель с
		// прежними правилами, доедающий очередь во время новой сцены, — это
		// именно то, из-за чего вывод перестал бы повторяться.
		a.stopConsumer()

		a.mu.Lock()
		if req.Reset {
			a.cfg = defaults()
			a.assign = map[int64]*assignment{}
			a.status = map[int64]string{}
			a.seen = map[string]bool{}
			a.order = nil
			a.processed.Store(0)
			a.duplicates.Store(0)
			a.redelivered.Store(0)
			a.deadLettered.Store(0)
			a.failed.Store(0)
			a.rec.Clear()
		}
		setStr(&a.cfg.Source, req.Source)
		setStr(&a.cfg.Group, req.Group)
		setStr(&a.cfg.Ack, req.Ack)
		setInt(&a.cfg.Prefetch, req.Prefetch)
		setInt(&a.cfg.Workers, req.Workers)
		setInt(&a.cfg.WorkMS, req.WorkMS)
		setInt(&a.cfg.DieAfter, req.DieAfter)
		setInt(&a.cfg.RestartMS, req.RestartMS)
		setBool(&a.cfg.FromStart, req.FromStart)
		setBool(&a.cfg.Idempotent, req.Idempotent)
		setBool(&a.cfg.AssignFail, req.AssignFail)
		setBool(&a.cfg.CompensateFail, req.CompensateFail)
		if req.PoisonOrder != nil {
			a.cfg.PoisonOrder = *req.PoisonOrder
		}
		cfg := a.cfg
		a.mu.Unlock()

		if cfg.Source == "amqp" || cfg.Source == "kafka" {
			a.startConsumer(cfg, amqpURL, brokers, log)
		}
		log.Info("сцена настроила курьеров", "source", cfg.Source, "ack", cfg.Ack,
			"workers", cfg.Workers, "prefetch", cfg.Prefetch, "die_after", cfg.DieAfter)
		lab.WriteJSON(w, http.StatusOK, cfg)
	})

	mux.HandleFunc("GET /_lab/state", func(w http.ResponseWriter, r *http.Request) {
		a.mu.Lock()
		out := make([]assignment, 0, len(a.assign))
		for _, v := range a.assign {
			out = append(out, *v)
		}
		statuses := map[string]string{}
		for k, v := range a.status {
			statuses[fmt.Sprint(k)] = v
		}
		seq := append([]string(nil), a.order...)
		a.mu.Unlock()
		sort.Slice(out, func(i, j int) bool { return out[i].Order < out[j].Order })

		lab.WriteJSON(w, http.StatusOK, map[string]any{
			"assignments":   out,
			"statuses":      statuses,
			"sequence":      seq,
			"processed":     a.processed.Load(),
			"duplicates":    a.duplicates.Load(),
			"redelivered":   a.redelivered.Load(),
			"dead_lettered": a.deadLettered.Load(),
			"failed":        a.failed.Load(),
		})
	})

	addr := lab.Env("LAB_ADDR", ":8060")
	if err := lab.Serve(addr, mux, log, a.stopConsumer); err != nil {
		log.Error("сервис остановлен с ошибкой", "err", err)
	}
}

// ── состояние ───────────────────────────────────────────────────────────────

func (a *app) config() config {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.cfg
}

func (a *app) setConfig(c config) {
	a.mu.Lock()
	a.cfg = c
	a.mu.Unlock()
}

// apply записывает назначение. Повторная запись не «чинится» молча: счётчик
// times растёт, и именно он показывает студенту цену повторной доставки.
func (a *app) apply(order int64, status string) string {
	name := courierFor(order)
	a.mu.Lock()
	defer a.mu.Unlock()
	if cur, ok := a.assign[order]; ok {
		cur.Times++
	} else {
		a.assign[order] = &assignment{Order: order, Courier: name, Times: 1}
	}
	a.status[order] = status
	a.order = append(a.order, fmt.Sprintf("%d:%s", order, status))
	return name
}

func setStr(dst *string, src *string) {
	if src != nil {
		*dst = *src
	}
}
func setInt(dst *int, src *int) {
	if src != nil {
		*dst = *src
	}
}
func setBool(dst *bool, src *bool) {
	if src != nil {
		*dst = *src
	}
}

// ── подписка ────────────────────────────────────────────────────────────────

func (a *app) stopConsumer() {
	a.mu.Lock()
	cancel, done := a.cancel, a.done
	a.cancel, a.done = nil, nil
	a.mu.Unlock()
	if cancel == nil {
		return
	}
	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
	}
}

func (a *app) startConsumer(cfg config, amqpURL string, brokers []string, log *slog.Logger) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	a.mu.Lock()
	a.cancel, a.done = cancel, done
	a.mu.Unlock()

	go func() {
		defer close(done)
		if cfg.Source == "amqp" {
			a.consumeAMQP(ctx, cfg, amqpURL, log)
			return
		}
		a.consumeKafka(ctx, cfg, brokers, log)
	}()
}

// consumeAMQP — модель очереди. Смерть потребителя здесь моделируется честно:
// соединение закрывается, неподтверждённое сообщение возвращается в очередь,
// а через restart_ms потребитель подключается заново.
func (a *app) consumeAMQP(ctx context.Context, cfg config, url string, log *slog.Logger) {
	handled := 0
	for ctx.Err() == nil {
		conn, err := lab.DialAMQPWait(ctx, url, 20*time.Second)
		if err != nil {
			return
		}
		deliveries, err := conn.Consume(lab.QueueCour, "courier", cfg.Prefetch)
		if err != nil {
			conn.Close()
			return
		}

		died := a.pumpAMQP(ctx, cfg, deliveries, &handled, log)
		conn.Close()
		if !died || ctx.Err() != nil {
			return
		}
		a.rec.Record("courier.restart", nil)
		log.Info("потребитель поднялся заново")
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Duration(cfg.RestartMS) * time.Millisecond):
		}
	}
}

// pumpAMQP возвращает true, если потребитель «умер» и его надо поднять заново.
func (a *app) pumpAMQP(ctx context.Context, cfg config, deliveries <-chan amqp.Delivery,
	handled *int, log *slog.Logger) bool {

	sem := make(chan struct{}, max(1, cfg.Workers))
	var wg sync.WaitGroup
	defer wg.Wait()

	for {
		select {
		case <-ctx.Done():
			return false
		case d, ok := <-deliveries:
			if !ok {
				return false
			}
			if d.Redelivered {
				a.redelivered.Add(1)
			}
			*handled++
			// Смерть до подтверждения. Сообщение уже обработано — но брокер
			// об этом не знает и выдаст его снова: это и есть «минимум один раз».
			if cfg.DieAfter > 0 && *handled == cfg.DieAfter {
				a.handle(cfg, d.Body, d.MessageId, log)
				a.rec.Record("courier.die", map[string]string{"handled": fmt.Sprint(*handled)})
				log.Info("потребитель умер, не подтвердив сообщение", "handled", *handled)
				return true
			}

			if cfg.Ack == "before" {
				// Подтверждение до обработки: если процесс умрёт сейчас,
				// сообщение потеряно навсегда — брокер считает его сделанным.
				_ = d.Ack(false)
			}

			wg.Add(1)
			sem <- struct{}{}
			go func(d amqp.Delivery) {
				defer wg.Done()
				defer func() { <-sem }()

				ok := a.handle(cfg, d.Body, d.MessageId, log)
				if cfg.Ack == "before" {
					return
				}
				if !ok {
					// Отравленное сообщение: возвращаем в очередь. Пока у
					// очереди нет предела повторных доставок, оно вернётся
					// в голову и встанет там намертво.
					_ = d.Nack(false, true)
					return
				}
				_ = d.Ack(false)
			}(d)
		}
	}
}

// consumeKafka — модель лога. Смещение двигается только явно: пока курсор не
// сдвинут, перезапуск читает те же самые записи заново.
func (a *app) consumeKafka(ctx context.Context, cfg config, brokers []string, log *slog.Logger) {
	handled := 0
	for ctx.Err() == nil {
		k, err := lab.DialKafkaGroup(brokers, cfg.Group, cfg.FromStart)
		if err != nil {
			return
		}
		died := a.pumpKafka(ctx, cfg, k, &handled, log)
		k.Close()
		if !died || ctx.Err() != nil {
			return
		}
		a.rec.Record("courier.restart", nil)
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Duration(cfg.RestartMS) * time.Millisecond):
		}
	}
}

func (a *app) pumpKafka(ctx context.Context, cfg config, k *lab.Kafka, handled *int, log *slog.Logger) bool {
	cl := k.Client()
	for {
		if ctx.Err() != nil {
			return false
		}
		pollCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		fetches := cl.PollFetches(pollCtx)
		cancel()
		if fetches.IsClientClosed() {
			return false
		}

		var records []*kgo.Record
		fetches.EachRecord(func(r *kgo.Record) { records = append(records, r) })
		if len(records) == 0 {
			if ctx.Err() != nil {
				return false
			}
			continue
		}

		// Записи одной партиции обрабатываются столькими обработчиками,
		// сколько велела сцена. Порядок гарантирован брокером — и перестаёт
		// существовать ровно здесь, в потребителе.
		sem := make(chan struct{}, max(1, cfg.Workers))
		var wg sync.WaitGroup
		for _, r := range records {
			*handled++
			if cfg.DieAfter > 0 && *handled == cfg.DieAfter {
				wg.Wait()
				a.handle(cfg, r.Value, string(r.Key), log)
				a.rec.Record("courier.die", map[string]string{"handled": fmt.Sprint(*handled)})
				log.Info("потребитель умер, не сдвинув смещение", "handled", *handled)
				return true
			}
			wg.Add(1)
			sem <- struct{}{}
			go func(r *kgo.Record) {
				defer wg.Done()
				defer func() { <-sem }()
				a.handle(cfg, r.Value, string(r.Key), log)
			}(r)
		}
		wg.Wait()

		// Смещение сдвигается только после обработки всей выборки.
		if cfg.Ack != "before" {
			if err := cl.CommitRecords(ctx, records...); err != nil && ctx.Err() == nil {
				log.Info("смещение не сдвинуто", "err", err.Error())
			}
		}
	}
}

// handle — собственно обработка. Возвращает false, если сообщение обработать
// не удалось.
func (a *app) handle(cfg config, body []byte, msgID string, log *slog.Logger) bool {
	e, err := lab.ParseMsg(body)
	if err != nil {
		a.failed.Add(1)
		return false
	}
	if cfg.PoisonOrder != 0 && e.Order == cfg.PoisonOrder {
		a.failed.Add(1)
		a.rec.Record("courier.poison", map[string]string{"order": e.Key()})
		log.Info("сообщение обработать не удалось", "order", e.Order)
		return false
	}

	if cfg.Idempotent {
		id := e.ID
		if id == "" {
			id = msgID
		}
		a.mu.Lock()
		dup := a.seen[id]
		a.seen[id] = true
		a.mu.Unlock()
		if dup {
			a.duplicates.Add(1)
			a.rec.Record("courier.skip", map[string]string{"order": e.Key()})
			log.Info("сообщение уже обработано, пропускаем", "order", e.Order, "id", id)
			return true
		}
	}

	if cfg.WorkMS > 0 {
		time.Sleep(time.Duration(cfg.WorkMS) * time.Millisecond)
	}

	status := "assigned"
	switch e.Type {
	case "order.cancelled":
		status = "cancelled"
	case "order.paid":
		status = "assigned"
	}
	name := a.apply(e.Order, status)
	a.processed.Add(1)
	a.rec.Record(fmt.Sprintf("courier.handled#%d", a.processed.Load()),
		map[string]string{"order": e.Key(), "courier": name, "type": e.Type})
	log.Info("событие обработано", "order", e.Order, "type", e.Type, "courier", name)
	return true
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
