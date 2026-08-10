// acquirer — внешний эквайринг. Всё его поведение задаётся сценой: задержка,
// исход, дедупликация по ключу идемпотентности. Случайности внутри нет —
// студент сверяет свой вывод с текстом шага.
package main

import (
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/kipyatl0/backend-architecture-lab/services/internal/lab"
)

type config struct {
	// Сколько эквайринг «думает» перед ответом.
	DelayMS int `json:"delay_ms"`
	// ok — списание проходит; decline — отказ банка; error — 500.
	Mode string `json:"mode"`
	// Умеет ли провайдер отбрасывать повтор по ключу идемпотентности.
	// В сцене 1 — нет: ключа ему всё равно никто не передаёт.
	Idempotent bool `json:"idempotent"`
	// Возврат — такой же шаг, как и списание, и точно так же умеет не
	// получиться. На этом стоит сцена 21: компенсация тоже не прошла.
	RefundMode string `json:"refund_mode"`
}

type charge struct {
	ID             string    `json:"id"`
	Order          int64     `json:"order"`
	Amount         int64     `json:"amount"`
	IdempotencyKey string    `json:"idempotency_key,omitempty"`
	ReceivedAt     time.Time `json:"received_at"`
	AnsweredAt     time.Time `json:"answered_at"`
	Outcome        string    `json:"outcome"`
}

type refund struct {
	Charge string    `json:"charge"`
	Order  int64     `json:"order"`
	At     time.Time `json:"at"`
}

type app struct {
	mu      sync.Mutex
	cfg     config
	seq     int64
	ledger  []charge
	refunds []refund
	byKey   map[string]string
}

func main() {
	log := lab.Logger("acquirer")
	a := &app{
		cfg:   config{DelayMS: 0, Mode: "ok", Idempotent: false, RefundMode: "ok"},
		byKey: map[string]string{},
	}

	mux := http.NewServeMux()
	lab.Health(mux, func() error { return nil })

	mux.HandleFunc("POST /v1/charge", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Order          int64  `json:"order"`
			Amount         int64  `json:"amount"`
			IdempotencyKey string `json:"idempotency_key"`
		}
		if err := lab.ReadJSON(r, &req); err != nil {
			lab.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "bad request"})
			return
		}

		a.mu.Lock()
		cfg := a.cfg
		if cfg.Idempotent && req.IdempotencyKey != "" {
			if id, ok := a.byKey[req.IdempotencyKey]; ok {
				a.mu.Unlock()
				log.Info("повтор по ключу идемпотентности отброшен", "order", req.Order, "charge", id)
				lab.WriteJSON(w, http.StatusOK, map[string]any{"charge": id, "status": "ok", "duplicate": true})
				return
			}
		}
		a.seq++
		id := fmt.Sprintf("ch_%d", a.seq)
		if req.IdempotencyKey != "" {
			a.byKey[req.IdempotencyKey] = id
		}
		idx := len(a.ledger)
		a.ledger = append(a.ledger, charge{
			ID: id, Order: req.Order, Amount: req.Amount,
			IdempotencyKey: req.IdempotencyKey, ReceivedAt: time.Now().UTC(),
		})
		a.mu.Unlock()

		log.Info("запрос на списание принят", "order", req.Order, "amount", req.Amount,
			"charge", id, "delay_ms", cfg.DelayMS)

		// Задержка провайдера: ровно столько, сколько велела сцена.
		if cfg.DelayMS > 0 {
			t := time.NewTimer(time.Duration(cfg.DelayMS) * time.Millisecond)
			defer t.Stop()
			select {
			case <-t.C:
			case <-r.Context().Done():
				a.finish(idx, "abandoned")
				log.Info("вызывающий не дождался ответа", "order", req.Order, "charge", id)
				return
			}
		}

		switch cfg.Mode {
		case "decline":
			a.finish(idx, "declined")
			log.Info("банк отказал", "order", req.Order, "charge", id)
			lab.WriteJSON(w, http.StatusPaymentRequired, map[string]any{"charge": id, "status": "declined"})
		case "error":
			a.finish(idx, "error")
			log.Info("сбой на стороне провайдера", "order", req.Order, "charge", id)
			lab.WriteJSON(w, http.StatusInternalServerError, map[string]any{"error": "provider error"})
		default:
			a.finish(idx, "charged")
			log.Info("списание выполнено", "order", req.Order, "amount", req.Amount, "charge", id)
			lab.WriteJSON(w, http.StatusOK, map[string]any{"charge": id, "status": "ok",
				"order": req.Order, "amount": req.Amount})
		}
	})

	// Возврат списания — компенсирующее действие саги. Отдельная операция, а
	// не «откат»: у внешнего провайдера транзакции нет и быть не может.
	mux.HandleFunc("POST /v1/refund", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Charge string `json:"charge"`
			Order  int64  `json:"order"`
		}
		if err := lab.ReadJSON(r, &req); err != nil {
			lab.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "bad request"})
			return
		}
		a.mu.Lock()
		mode := a.cfg.RefundMode
		a.mu.Unlock()

		if mode != "ok" {
			log.Info("возврат не прошёл", "order", req.Order, "charge", req.Charge)
			lab.WriteJSON(w, http.StatusServiceUnavailable,
				map[string]any{"code": "refund_failed", "error": "возврат не выполнен"})
			return
		}
		a.mu.Lock()
		a.refunds = append(a.refunds, refund{Charge: req.Charge, Order: req.Order, At: time.Now().UTC()})
		a.mu.Unlock()
		log.Info("возврат выполнен", "order", req.Order, "charge", req.Charge)
		lab.WriteJSON(w, http.StatusOK, map[string]any{"charge": req.Charge, "status": "refunded"})
	})

	// ── управление сценой ───────────────────────────────────────────────────

	mux.HandleFunc("POST /_lab/config", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			DelayMS    *int    `json:"delay_ms"`
			Mode       *string `json:"mode"`
			Idempotent *bool   `json:"idempotent"`
			RefundMode *string `json:"refund_mode"`
			Reset      bool    `json:"reset"`
		}
		if err := lab.ReadJSON(r, &req); err != nil {
			lab.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		a.mu.Lock()
		if req.DelayMS != nil {
			a.cfg.DelayMS = *req.DelayMS
		}
		if req.Mode != nil {
			a.cfg.Mode = *req.Mode
		}
		if req.Idempotent != nil {
			a.cfg.Idempotent = *req.Idempotent
		}
		if req.RefundMode != nil {
			a.cfg.RefundMode = *req.RefundMode
		}
		if req.Reset {
			a.seq = 0
			a.ledger = nil
			a.refunds = nil
			a.byKey = map[string]string{}
			a.cfg.RefundMode = "ok"
		}
		cfg := a.cfg
		a.mu.Unlock()
		log.Info("сцена настроила эквайринг", "delay_ms", cfg.DelayMS, "mode", cfg.Mode,
			"idempotent", cfg.Idempotent, "reset", req.Reset)
		lab.WriteJSON(w, http.StatusOK, cfg)
	})

	mux.HandleFunc("GET /_lab/ledger", func(w http.ResponseWriter, r *http.Request) {
		a.mu.Lock()
		defer a.mu.Unlock()
		var total int64
		charged := 0
		out := make([]charge, len(a.ledger))
		copy(out, a.ledger)
		for _, c := range out {
			if c.Outcome == "charged" {
				charged++
				total += c.Amount
			}
		}
		refunds := make([]refund, len(a.refunds))
		copy(refunds, a.refunds)
		lab.WriteJSON(w, http.StatusOK, map[string]any{
			"charges": out, "charged": charged, "charged_total": total,
			"refunds": refunds, "refunded": len(refunds),
		})
	})

	addr := lab.Env("LAB_ADDR", ":8090")
	if err := lab.Serve(addr, mux, log, nil); err != nil {
		log.Error("сервис остановлен с ошибкой", "err", err)
	}
}

func (a *app) finish(idx int, outcome string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if idx < len(a.ledger) {
		a.ledger[idx].AnsweredAt = time.Now().UTC()
		a.ledger[idx].Outcome = outcome
	}
}
