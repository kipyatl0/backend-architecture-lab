package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"
)

// Сцены профиля split: тот же код службы, но модуль уведомлений уехал за сеть.

// ── сцена 5 — уведомления вынесены в сервис (m03 l05) ───────────────────────
//
// Вызов остался тем же вызовом. Изменилось одно: у него появился исход, которого
// у вызова внутри процесса не бывает, — «неизвестно». И этого хватило, чтобы
// платёж прошёл, а заказа не осталось.
func scene05(ctx context.Context, r *Run) error {
	cfg := r.Script.Config

	if err := r.postJSON(r.AcquirerURL+"/_lab/config", map[string]any{
		"delay_ms": 0, "mode": "ok", "idempotent": false, "reset": true,
	}, nil); err != nil {
		return fmt.Errorf("эквайринг не настроен: %w", err)
	}
	if err := r.configureNotifier(map[string]any{
		"mode": "ok", "strict": false, "reset": true,
	}); err != nil {
		return fmt.Errorf("уведомления не настроены — поднят ли профиль split? %w", err)
	}
	if err := r.configure(map[string]any{
		"reset": true, "notify_timeout_ms": cfg.NotifyTimeoutMS,
	}); err != nil {
		return err
	}
	if err := r.postJSON(r.MonolithURL+"/_lab/prepare", map[string]any{
		"first_order_id": cfg.FirstOrderID,
	}, nil); err != nil {
		return err
	}

	r.Set("first_order", strconv.FormatInt(cfg.FirstOrderID, 10))
	r.Set("second_order", strconv.FormatInt(cfg.FirstOrderID+1, 10))
	r.Set("amount", strconv.FormatInt(cfg.Amount, 10))
	r.Set("notify_timeout", ms(cfg.NotifyTimeoutMS))

	client := &http.Client{Timeout: 30 * time.Second}
	order, _ := json.Marshal(map[string]any{
		"client": cfg.Client, "restaurant": cfg.Restaurant, "amount": cfg.Amount,
	})

	r.T0 = time.Now()

	// Заказ 1 — всё как раньше, только вызов ушёл по сети.
	r.Record("client.call.orders#1", nil)
	code, err := doOnce(client, http.MethodPost, r.MonolithURL+"/orders", order)
	if err != nil || code != http.StatusCreated {
		return fmt.Errorf("первый заказ должен был пройти: код %d, ошибка %v", code, err)
	}
	r.Record("client.recv#1", nil)

	// Сервис уведомлений перестаёт отвечать. Не падает — именно перестаёт
	// отвечать: это другой отказ, и различить их вызывающий не может.
	if err := r.configureNotifier(map[string]any{"mode": "hang"}); err != nil {
		return err
	}
	r.Record("notifier.hang", nil)

	r.SleepUntil(cfg.RetryAtS)
	r.Record("client.call.orders#2", nil)
	code, err = doOnce(client, http.MethodPost, r.MonolithURL+"/orders", order)
	if err != nil {
		return fmt.Errorf("второй заказ: ожидался отказ службы, а не обрыв клиента: %v", err)
	}
	if code == http.StatusCreated {
		return fmt.Errorf("второй заказ прошёл — проверь режим уведомлений в сценарии")
	}
	r.Record("client.fail#2", map[string]string{"code": strconv.Itoa(code)})

	if err := r.collect(); err != nil {
		return err
	}

	// Итог берётся из трёх независимых источников: база службы, журнал
	// эквайринга и состояние сервиса уведомлений.
	var state struct {
		Totals struct {
			Orders int `json:"orders"`
		} `json:"totals"`
	}
	if err := r.getJSON(r.MonolithURL+"/_lab/state", &state); err != nil {
		return err
	}
	var ledger struct {
		Charged      int   `json:"charged"`
		ChargedTotal int64 `json:"charged_total"`
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

	r.Set("orders", strconv.Itoa(state.Totals.Orders))
	r.Set("charges", strconv.Itoa(ledger.Charged))
	r.Set("charged_total", strconv.FormatInt(ledger.ChargedTotal, 10))
	r.Set("notifications", strconv.FormatInt(notif.Received, 10))
	return nil
}

// ── сцена 6 — старый потребитель, новый отправитель (m04 l05) ───────────────
//
// Четыре прогона одного и того же заказа. Меняются две вещи: что отправляет
// служба и насколько придирчиво разбирает сообщение потребитель.
func scene06(ctx context.Context, r *Run) error {
	cfg := r.Script.Config

	if err := r.postJSON(r.AcquirerURL+"/_lab/config", map[string]any{
		"delay_ms": 0, "mode": "ok", "idempotent": false, "reset": true,
	}, nil); err != nil {
		return err
	}

	client := &http.Client{Timeout: 30 * time.Second}
	order, _ := json.Marshal(map[string]any{
		"client": cfg.Client, "restaurant": cfg.Restaurant, "amount": cfg.Amount,
	})

	passes := []struct {
		key    string
		schema string
		strict bool
	}{
		{"pass#1", "v1", true},
		{"pass#2", "v1+extra", true},
		{"pass#3", "v1+extra", false},
		{"pass#4", "v2", false},
	}

	accepted := 0
	for _, p := range passes {
		if err := r.configureNotifier(map[string]any{
			"mode": "ok", "strict": p.strict, "reset": true,
		}); err != nil {
			return fmt.Errorf("уведомления не настроены — поднят ли профиль split? %w", err)
		}
		if err := r.configure(map[string]any{
			"reset": true, "notify_schema": p.schema, "notify_timeout_ms": 5000,
		}); err != nil {
			return err
		}
		if err := r.postJSON(r.MonolithURL+"/_lab/prepare", map[string]any{
			"first_order_id": cfg.FirstOrderID,
		}, nil); err != nil {
			return err
		}

		code, err := doOnce(client, http.MethodPost, r.MonolithURL+"/orders", order)
		if err != nil {
			return fmt.Errorf("%s: служба не ответила: %w", p.key, err)
		}

		var notif struct {
			Received int64 `json:"received"`
			Rejected int64 `json:"rejected"`
		}
		if err := r.getJSON(r.NotifierURL+"/_lab/state", &notif); err != nil {
			return err
		}

		r.Measure(p.key, "order_code", float64(code))
		r.Measure(p.key, "received", float64(notif.Received))
		r.Measure(p.key, "rejected", float64(notif.Rejected))
		if code == http.StatusCreated {
			accepted++
		}
	}

	r.Set("accepted", strconv.Itoa(accepted))
	r.Set("total", strconv.Itoa(len(passes)))
	return nil
}
