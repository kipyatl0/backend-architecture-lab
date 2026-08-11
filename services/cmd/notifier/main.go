// notifier — уведомления, вынесенные из монолита в отдельный сервис. Тот же
// самый модуль, что был вызовом внутри процесса: код тот же, граница другая.
// Своими данными владеет сам — монолит в них не заглядывает.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kipyatl0/backend-architecture-lab/services/internal/lab"
)

type config struct {
	// ok — отвечает; slow — отвечает медленно; hang — не отвечает никогда;
	// down — отвечает отказом; grey — проверка здоровья зелёная, работа стоит.
	Mode    string `json:"mode"`
	DelayMS int    `json:"delay_ms"`
	// Строгий потребитель падает на неизвестном поле. Нестрогий его переживает —
	// и это требование к потребителю, а не к отправителю.
	Strict bool `json:"strict"`

	// Внешний провайдер пуш-уведомлений — третий уровень цепочки. Своего
	// контейнера у него нет: он и в жизни чужой, за периметром.
	ProviderMode    string `json:"provider_mode"` // ok | down
	ProviderRetries int    `json:"provider_retries"`
	// Сколько провайдер думает над обращением и на каждом каком по счёту он
	// думает долго (m11 l04). Не «сервис стал медленным», а «часть обращений
	// уходит в долгую» — ровно так деградация и выглядит в жизни: среднее
	// спокойно, хвост распределения уехал.
	ProviderMS   int `json:"provider_ms"`
	SlowEveryN   int `json:"slow_every_n"`
	SlowProvider int `json:"slow_provider_ms"`
}

type notification struct {
	ID    string `json:"id"`
	Order int64  `json:"order"`
	Body  string `json:"body"`
}

type app struct {
	mu   sync.Mutex
	cfg  config
	seq  int64
	sent []notification

	received atomic.Int64
	rejected atomic.Int64
	hanging  atomic.Int64
	// Сколько раз сервис постучался к внешнему провайдеру. Это и есть нижний
	// этаж амплификации повторов.
	providerAttempts atomic.Int64
	// Порядковый номер обращения к провайдеру: по нему сцена m11 решает, какое
	// из них уйдёт в долгую.
	calls atomic.Int64

	tracer *lab.Tracer
	trail  *lab.Trail
}

// callProvider — обращение к внешнему провайдеру со своими повторами.
// Каждая попытка считается: именно из этого счётчика собирается «двадцать
// семь запросов из одного пользовательского».
func (a *app) callProvider(ctx context.Context, cfg config) error {
	// Отрезок за внешнего участника заводит вызывающий: провайдер за периметром
	// и о трассировке ничего не знает. Без этого спана в дереве трейса на месте
	// самой долгой работы была бы пустота.
	_, span := a.tracer.Start(ctx, "провайдер уведомлений", lab.KindClient)
	defer span.End()

	n := a.calls.Add(1)
	took := cfg.ProviderMS
	slow := cfg.SlowEveryN > 0 && n%int64(cfg.SlowEveryN) == 0
	if slow {
		took = cfg.SlowProvider
		span.Set("медленно", "да")
	}
	if took > 0 {
		time.Sleep(time.Duration(took) * time.Millisecond)
	}

	tries := cfg.ProviderRetries + 1
	var err error
	for i := 0; i < tries; i++ {
		a.providerAttempts.Add(1)
		if cfg.ProviderMode == "down" {
			err = errProviderDown
			continue
		}
		if slow {
			a.trail.Write(ctx, "провайдер уведомлений отвечал медленно", map[string]string{
				"мс": strconv.Itoa(took), "обращение": strconv.FormatInt(n, 10),
			})
		}
		return nil
	}
	return err
}

var errProviderDown = errors.New("провайдер уведомлений недоступен")

func main() {
	log := lab.Logger("notifier")
	a := &app{
		cfg:    config{Mode: "ok", DelayMS: 0, Strict: true, ProviderMode: "ok", ProviderRetries: 0},
		tracer: lab.StartTracer("notifier"),
		trail:  lab.NewTrail("notifier"),
	}

	mux := http.NewServeMux()

	// Живость и готовность — разные вопросы. Серый отказ ровно в том и
	// состоит, что оба ответа зелёные, а работа не идёт.
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		lab.WriteJSON(w, http.StatusOK, map[string]string{"status": "alive"})
	})
	mux.HandleFunc("GET /ready", func(w http.ResponseWriter, r *http.Request) {
		a.mu.Lock()
		mode := a.cfg.Mode
		a.mu.Unlock()
		if mode == "down" {
			lab.WriteJSON(w, http.StatusServiceUnavailable,
				map[string]string{"status": "not ready", "reason": "провайдер уведомлений недоступен"})
			return
		}
		lab.WriteJSON(w, http.StatusOK, map[string]string{"status": "ready"})
	})

	mux.HandleFunc("POST /notifications", func(w http.ResponseWriter, r *http.Request) {
		a.mu.Lock()
		cfg := a.cfg
		a.mu.Unlock()

		switch cfg.Mode {
		case "hang", "grey":
			a.hanging.Add(1)
			defer a.hanging.Add(-1)
			<-r.Context().Done() // ответа не будет никогда
			return
		case "down":
			a.rejected.Add(1)
			lab.WriteJSON(w, http.StatusServiceUnavailable,
				map[string]string{"error": "провайдер уведомлений недоступен", "code": "unavailable"})
			return
		case "slow":
			t := time.NewTimer(time.Duration(cfg.DelayMS) * time.Millisecond)
			defer t.Stop()
			select {
			case <-t.C:
			case <-r.Context().Done():
				return
			}
		}

		// Разбор тела — и есть контракт. Строгий потребитель отвергает
		// сообщение с неизвестным полем; нестрогий его переживает.
		var req struct {
			Order int64  `json:"order"`
			Body  string `json:"body"`
		}
		dec := json.NewDecoder(r.Body)
		if cfg.Strict {
			dec.DisallowUnknownFields()
		}
		if err := dec.Decode(&req); err != nil {
			a.rejected.Add(1)
			log.Info("сообщение не разобрано", "err", err.Error(), "strict", cfg.Strict)
			lab.WriteJSON(w, http.StatusBadRequest, map[string]any{
				"code": "schema_mismatch", "error": err.Error(),
			})
			return
		}
		if req.Body == "" {
			a.rejected.Add(1)
			log.Info("обязательное поле body отсутствует", "order", req.Order)
			lab.WriteJSON(w, http.StatusBadRequest, map[string]any{
				"code": "field_missing", "error": "поле body обязательно",
			})
			return
		}

		// Дальше — внешний провайдер. Со своими повторами: каждый участник
		// цепочки считает, что уж он-то повторяет разумно.
		if err := a.callProvider(r.Context(), cfg); err != nil {
			a.rejected.Add(1)
			log.Info("провайдер не принял уведомление", "order", req.Order, "err", err.Error())
			lab.WriteJSON(w, http.StatusBadGateway, map[string]any{
				"code": "provider_unavailable", "error": err.Error(),
			})
			return
		}

		a.mu.Lock()
		a.seq++
		id := "n_" + strconv.FormatInt(a.seq, 10)
		a.sent = append(a.sent, notification{ID: id, Order: req.Order, Body: req.Body})
		a.mu.Unlock()
		a.received.Add(1)

		log.Info("уведомление отправлено", "order", req.Order, "id", id)
		lab.WriteJSON(w, http.StatusOK, map[string]any{"id": id, "order": req.Order})
	})

	// ── управление сценой ───────────────────────────────────────────────────

	mux.HandleFunc("POST /_lab/config", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Mode            *string `json:"mode"`
			DelayMS         *int    `json:"delay_ms"`
			Strict          *bool   `json:"strict"`
			ProviderMode    *string `json:"provider_mode"`
			ProviderRetries *int    `json:"provider_retries"`
			ProviderMS      *int    `json:"provider_ms"`
			SlowEveryN      *int    `json:"slow_every_n"`
			SlowProvider    *int    `json:"slow_provider_ms"`
			Reset           bool    `json:"reset"`
		}
		if err := lab.ReadJSON(r, &req); err != nil {
			lab.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		a.mu.Lock()
		if req.Mode != nil {
			a.cfg.Mode = *req.Mode
		}
		if req.DelayMS != nil {
			a.cfg.DelayMS = *req.DelayMS
		}
		if req.Strict != nil {
			a.cfg.Strict = *req.Strict
		}
		if req.ProviderMode != nil {
			a.cfg.ProviderMode = *req.ProviderMode
		}
		if req.ProviderRetries != nil {
			a.cfg.ProviderRetries = *req.ProviderRetries
		}
		if req.ProviderMS != nil {
			a.cfg.ProviderMS = *req.ProviderMS
		}
		if req.SlowEveryN != nil {
			a.cfg.SlowEveryN = *req.SlowEveryN
		}
		if req.SlowProvider != nil {
			a.cfg.SlowProvider = *req.SlowProvider
		}
		if req.Reset {
			a.seq, a.sent = 0, nil
			a.received.Store(0)
			a.rejected.Store(0)
			a.providerAttempts.Store(0)
			a.calls.Store(0)
			a.trail.Clear()
		}
		cfg := a.cfg
		a.mu.Unlock()
		log.Info("сцена настроила уведомления", "mode", cfg.Mode, "delay_ms", cfg.DelayMS,
			"strict", cfg.Strict, "reset", req.Reset)
		lab.WriteJSON(w, http.StatusOK, cfg)
	})

	mux.HandleFunc("GET /_lab/state", func(w http.ResponseWriter, r *http.Request) {
		a.mu.Lock()
		defer a.mu.Unlock()
		out := make([]notification, len(a.sent))
		copy(out, a.sent)
		lab.WriteJSON(w, http.StatusOK, map[string]any{
			"notifications":     out,
			"received":          a.received.Load(),
			"rejected":          a.rejected.Load(),
			"hanging":           a.hanging.Load(),
			"provider_attempts": a.providerAttempts.Load(),
		})
	})

	mux.HandleFunc("GET /_lab/log", a.trail.Handler())

	addr := lab.Env("LAB_ADDR", ":8070")
	if err := lab.Serve(addr, a.tracer.Middleware(mux), log, nil); err != nil {
		log.Error("сервис остановлен с ошибкой", "err", err)
	}
}
