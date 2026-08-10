// outbox-relay — отправитель исходящего ящика: читает таблицу, публикует,
// отмечает отправленное.
//
// Это отдельный процесс, а не горутина внутри заказов, ровно по одной причине:
// сцена m07 убивает отправителя между публикацией и отметкой. Внутри чужого
// процесса такую смерть не показать — умер бы и владелец данных.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/kipyatl0/backend-architecture-lab/services/internal/lab"
)

type config struct {
	Enabled    bool `json:"enabled"`
	IntervalMS int  `json:"interval_ms"`
	// Умереть сразу после публикации, не отметив строку отправленной. Строка
	// останется неотмеченной, и поднявшийся отправитель опубликует её снова:
	// исходящий ящик обещает «минимум один раз», а не «ровно один раз».
	DieAfterPublish bool `json:"die_after_publish"`
	// Куда публиковать: amqp, kafka или both.
	Target string `json:"target"`
}

func defaults() config {
	return config{Enabled: true, IntervalMS: 200, DieAfterPublish: false, Target: "both"}
}

type app struct {
	mu  sync.Mutex
	cfg config

	published atomic.Int64
	marked    atomic.Int64

	rec lab.Recorder
}

func main() {
	log := lab.Logger("outbox-relay")
	a := &app{cfg: defaults()}

	ctx := context.Background()
	pool, err := waitForDB(ctx, lab.Env("LAB_DATABASE_URL", ""), log)
	if err != nil {
		log.Error("база недоступна", "err", err)
		os.Exit(1)
	}
	defer pool.Close()

	amqpURL := lab.Env("LAB_AMQP_URL", "amqp://lab:lab@rabbitmq:5672/")
	brokers := strings.Split(lab.Env("LAB_KAFKA_BROKERS", "kafka:9092"), ",")

	loopCtx, stopLoop := context.WithCancel(ctx)
	defer stopLoop()
	go a.loop(loopCtx, pool, amqpURL, brokers, log)

	mux := http.NewServeMux()
	lab.Health(mux, func() error { return pool.Ping(context.Background()) })
	mux.HandleFunc("GET /_lab/events", a.rec.Handler())

	mux.HandleFunc("POST /_lab/config", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Enabled         *bool   `json:"enabled"`
			IntervalMS      *int    `json:"interval_ms"`
			DieAfterPublish *bool   `json:"die_after_publish"`
			Target          *string `json:"target"`
			Reset           bool    `json:"reset"`
		}
		if err := lab.ReadJSON(r, &req); err != nil {
			lab.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		a.mu.Lock()
		if req.Reset {
			a.cfg = defaults()
			a.published.Store(0)
			a.marked.Store(0)
			a.rec.Clear()
		}
		if req.Enabled != nil {
			a.cfg.Enabled = *req.Enabled
		}
		if req.IntervalMS != nil {
			a.cfg.IntervalMS = *req.IntervalMS
		}
		if req.DieAfterPublish != nil {
			a.cfg.DieAfterPublish = *req.DieAfterPublish
		}
		if req.Target != nil {
			a.cfg.Target = *req.Target
		}
		cfg := a.cfg
		a.mu.Unlock()
		log.Info("сцена настроила отправителя", "enabled", cfg.Enabled,
			"die_after_publish", cfg.DieAfterPublish, "target", cfg.Target)
		lab.WriteJSON(w, http.StatusOK, cfg)
	})

	mux.HandleFunc("GET /_lab/state", func(w http.ResponseWriter, r *http.Request) {
		var pending, sent int
		row := pool.QueryRow(r.Context(),
			`SELECT count(*) FILTER (WHERE sent_at IS NULL), count(*) FILTER (WHERE sent_at IS NOT NULL) FROM outbox`)
		_ = row.Scan(&pending, &sent)
		lab.WriteJSON(w, http.StatusOK, map[string]any{
			"published": a.published.Load(),
			"marked":    a.marked.Load(),
			"pending":   pending,
			"sent":      sent,
		})
	})

	addr := lab.Env("LAB_ADDR", ":8040")
	if err := lab.Serve(addr, mux, log, stopLoop); err != nil {
		log.Error("сервис остановлен с ошибкой", "err", err)
	}
}

// waitForDB — база поднимается вместе с сервисом, и первые секунды её нет.
// Ждать её здесь дешевле, чем объяснять студенту, почему стенд зависит от
// порядка старта контейнеров.
func waitForDB(ctx context.Context, url string, log *slog.Logger) (*pgxpool.Pool, error) {
	deadline := time.Now().Add(60 * time.Second)
	for {
		pool, err := pgxpool.New(ctx, url)
		if err == nil {
			if err = pool.Ping(ctx); err == nil {
				// Таблицу заводит владелец данных — заказы. Отправитель её
				// только читает: чужую схему он не создаёт даже себе на пользу.
				var exists bool
				_ = pool.QueryRow(ctx,
					`SELECT to_regclass('public.outbox') IS NOT NULL`).Scan(&exists)
				if exists {
					return pool, nil
				}
				err = errNoOutbox
			}
			pool.Close()
		}
		if time.Now().After(deadline) {
			return nil, err
		}
		time.Sleep(500 * time.Millisecond)
	}
}

var errNoOutbox = errors.New("таблицы outbox ещё нет — её заводит сервис заказов")

// loop — весь отправитель целиком: выбрать неотправленное, опубликовать,
// отметить. Три шага, и между вторым и третьим живёт вся сцена 18.
func (a *app) loop(ctx context.Context, pool *pgxpool.Pool, amqpURL string,
	brokers []string, log *slog.Logger) {

	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Duration(a.interval()) * time.Millisecond):
		}

		a.mu.Lock()
		cfg := a.cfg
		a.mu.Unlock()
		if !cfg.Enabled {
			continue
		}

		rows, err := pool.Query(ctx,
			`SELECT id, payload FROM outbox WHERE sent_at IS NULL ORDER BY id LIMIT 10`)
		if err != nil {
			continue
		}
		type item struct {
			id      int64
			payload []byte
		}
		var batch []item
		for rows.Next() {
			var it item
			if err := rows.Scan(&it.id, &it.payload); err == nil {
				batch = append(batch, it)
			}
		}
		rows.Close()
		if len(batch) == 0 {
			continue
		}

		for _, it := range batch {
			var e lab.Msg
			if err := json.Unmarshal(it.payload, &e); err != nil {
				continue
			}
			if err := a.publish(ctx, cfg, amqpURL, brokers, e); err != nil {
				log.Info("опубликовать не удалось", "row", it.id, "err", err.Error())
				break
			}
			a.published.Add(1)
			a.rec.Record("relay.published", map[string]string{"order": e.Key(), "row": itoa(it.id)})
			log.Info("событие опубликовано", "row", it.id, "order", e.Order)

			// Публикация прошла, отметки ещё нет. Процесс, умерший здесь,
			// оставляет строку неотмеченной — и поднявшийся отправитель
			// опубликует её ещё раз.
			if cfg.DieAfterPublish {
				a.rec.Record("relay.die", map[string]string{"row": itoa(it.id)})
				log.Info("отправитель умер между публикацией и отметкой", "row", it.id)
				os.Exit(1)
			}

			if _, err := pool.Exec(ctx, `UPDATE outbox SET sent_at = now() WHERE id = $1`, it.id); err != nil {
				log.Info("отметить не удалось", "row", it.id, "err", err.Error())
				continue
			}
			a.marked.Add(1)
		}
	}
}

func (a *app) interval() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.cfg.IntervalMS <= 0 {
		return 200
	}
	return a.cfg.IntervalMS
}

func (a *app) publish(ctx context.Context, cfg config, amqpURL string,
	brokers []string, e lab.Msg) error {

	if cfg.Target == "amqp" || cfg.Target == "both" {
		conn, err := lab.DialAMQP(amqpURL)
		if err != nil {
			return err
		}
		err = conn.Publish(ctx, e.Type, e)
		conn.Close()
		if err != nil {
			return err
		}
	}
	if cfg.Target == "kafka" || cfg.Target == "both" {
		k, err := lab.DialKafka(brokers)
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

func itoa(v int64) string { return strconv.FormatInt(v, 10) }
