package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"
)

// Сцены профиля mono, кроме первой: нагрузка до потолка, форма распределения
// времён ответа и общий отказ одного процесса.

// ── сцена 2 — нагрузка ступенями до потолка (m02 l03) ───────────────────────
//
// Обработчик каталога занимает ровно столько, сколько велела сцена, а
// обработчиков ровно столько, сколько задано, — поэтому у службы есть
// вычислимый потолок, и излом задержки происходит там, где ему положено, а не
// там, где сегодня занят ноутбук.
func scene02(ctx context.Context, r *Run) error {
	cfg := r.Script.Config
	capacity := float64(cfg.Workers) * 1000.0 / float64(cfg.CatalogMS)

	r.Set("workers", strconv.Itoa(cfg.Workers))
	r.Set("service_ms", strconv.Itoa(cfg.CatalogMS))
	r.Set("capacity", strconv.FormatFloat(capacity, 'f', 0, 64))
	r.Set("step_seconds", strconv.FormatFloat(cfg.StepSeconds, 'f', 0, 64))

	if err := r.configure(map[string]any{
		"reset": true, "catalog_ms": cfg.CatalogMS, "workers": cfg.Workers, "queue": cfg.Queue,
	}); err != nil {
		return fmt.Errorf("служба не настроена: %w", err)
	}

	// Прогрев: первые соединения и разогрев пула не должны попасть в первую же
	// ступень и выдать излом там, где его нет.
	r.load(ctx, http.MethodGet, r.MonolithURL+"/catalog", nil, 20, 2*time.Second, 10*time.Second)

	step := time.Duration(cfg.StepSeconds * float64(time.Second))
	for i, rps := range cfg.Steps {
		if _, err := r.metrics(true); err != nil {
			return err
		}
		st := r.load(ctx, http.MethodGet, r.MonolithURL+"/catalog", nil, rps, step, 10*time.Second)
		m, err := r.metrics(false)
		if err != nil {
			return err
		}

		key := fmt.Sprintf("step#%d", i+1)
		r.Measure(key, "util", float64(rps)/capacity*100)
		r.Measure(key, "p50_ms", st.p(0.50))
		r.Measure(key, "p99_ms", st.p(0.99))
		r.Measure(key, "queue_max", float64(m.Gate.MaxWaiting))
		r.Measure(key, "failed", float64(st.Failed))

		// Пауза между ступенями: очередь предыдущей ступени должна разойтись,
		// иначе следующая начинается не с нуля и излом смазывается.
		if i < len(cfg.Steps)-1 {
			time.Sleep(2 * time.Second)
		}
	}
	return nil
}

// ── сцена 3 — гистограмма ответа под нагрузкой (m02 l04) ────────────────────
//
// Каждый N-й запрос обслуживается заметно дольше остальных: пауза сборщика,
// промах кэша, сосед по железу — источник неважен, важна форма распределения.
func scene03(ctx context.Context, r *Run) error {
	cfg := r.Script.Config

	r.Set("rps", strconv.Itoa(cfg.RPS))
	r.Set("seconds", strconv.FormatFloat(cfg.Seconds, 'f', 0, 64))
	r.Set("service_ms", strconv.Itoa(cfg.CatalogMS))
	r.Set("slow_every", strconv.Itoa(cfg.SlowEveryN))
	r.Set("slow_ms", strconv.Itoa(cfg.SlowMS))
	r.Set("slow_share", strconv.FormatFloat(100/float64(cfg.SlowEveryN), 'f', 0, 64))

	if err := r.configure(map[string]any{
		"reset": true, "catalog_ms": cfg.CatalogMS, "workers": cfg.Workers, "queue": 0,
		"slow_every_n": cfg.SlowEveryN, "slow_ms": cfg.SlowMS,
	}); err != nil {
		return fmt.Errorf("служба не настроена: %w", err)
	}

	r.load(ctx, http.MethodGet, r.MonolithURL+"/catalog", nil, 20, 2*time.Second, 10*time.Second)
	if _, err := r.metrics(true); err != nil {
		return err
	}

	st := r.load(ctx, http.MethodGet, r.MonolithURL+"/catalog", nil,
		cfg.RPS, time.Duration(cfg.Seconds*float64(time.Second)), 10*time.Second)

	// Распределение считает служба: у неё есть все замеры, а не выборка.
	var hist struct {
		Bounds []float64 `json:"bounds"`
		Counts []int64   `json:"counts"`
	}
	url := r.MonolithURL + "/_lab/buckets?bounds=" + joinFloats(cfg.Buckets)
	if err := r.getJSON(url, &hist); err != nil {
		return err
	}
	for i, c := range hist.Counts {
		r.Measure(fmt.Sprintf("bucket#%d", i+1), "count", float64(c))
	}

	m, err := r.metrics(false)
	if err != nil {
		return err
	}
	r.Measure("summary", "p50_ms", m.Latency.P50MS)
	r.Measure("summary", "p95_ms", m.Latency.P95MS)
	r.Measure("summary", "p99_ms", m.Latency.P99MS)
	r.Measure("summary", "avg_ms", m.Latency.AvgMS)
	r.Measure("summary", "count", float64(m.Latency.Count))
	r.Set("total", strconv.FormatInt(m.Latency.Count, 10))
	_ = st
	return nil
}

func joinFloats(v []float64) string {
	out := ""
	for i, f := range v {
		if i > 0 {
			out += ","
		}
		out += strconv.FormatFloat(f, 'f', -1, 64)
	}
	return out
}

// ── сцена 4 — общий отказ монолита (m03 l01) ────────────────────────────────
//
// Ошибка в модуле уведомлений уносит процесс целиком. Каталог ресторанов не
// имеет к уведомлениям никакого отношения — и перестаёт отвечать вместе с ними.
func scene04(ctx context.Context, r *Run) error {
	cfg := r.Script.Config

	if err := r.postJSON(r.AcquirerURL+"/_lab/config", map[string]any{
		"delay_ms": 0, "mode": "ok", "idempotent": false, "reset": true,
	}, nil); err != nil {
		return fmt.Errorf("эквайринг не настроен: %w", err)
	}
	if err := r.configure(map[string]any{"reset": true}); err != nil {
		return fmt.Errorf("служба не настроена: %w", err)
	}
	if err := r.postJSON(r.MonolithURL+"/_lab/prepare", map[string]any{
		"first_order_id": cfg.FirstOrderID,
	}, nil); err != nil {
		return fmt.Errorf("состояние службы не сброшено: %w", err)
	}
	r.Set("first_order", strconv.FormatInt(cfg.FirstOrderID, 10))
	r.Set("amount", strconv.FormatInt(cfg.Amount, 10))

	client := &http.Client{Timeout: 5 * time.Second}
	order, _ := json.Marshal(map[string]any{
		"client": cfg.Client, "restaurant": cfg.Restaurant, "amount": cfg.Amount,
	})

	r.T0 = time.Now()

	// 1. Оба сценария работают.
	if code, err := doOnce(client, http.MethodGet, r.MonolithURL+"/catalog", nil); err != nil || code != 200 {
		return fmt.Errorf("каталог не отвечает до сцены: код %d, ошибка %v", code, err)
	}
	r.Record("catalog.ok#1", nil)

	if code, err := doOnce(client, http.MethodPost, r.MonolithURL+"/orders", order); err != nil || code != 201 {
		return fmt.Errorf("заказ не создался до сцены: код %d, ошибка %v", code, err)
	}
	r.Record("orders.ok#1", map[string]string{"order": strconv.FormatInt(cfg.FirstOrderID, 10)})

	// 2. Модуль уведомлений ломается — на следующем же заказе.
	if err := r.configure(map[string]any{"notify_mode": "crash"}); err != nil {
		return err
	}
	r.Record("bug.armed", nil)

	r.SleepUntil(2.0)
	_, _ = doOnce(client, http.MethodPost, r.MonolithURL+"/orders", order)
	r.Record("orders.crash", map[string]string{
		"order": strconv.FormatInt(cfg.FirstOrderID+1, 10),
	})

	// 3. Процесса больше нет. Каталог, который к уведомлениям не имеет
	//    отношения, перестал отвечать вместе с ними.
	deadline := time.Now().Add(20 * time.Second)
	down := false
	for time.Now().Before(deadline) {
		if _, err := doOnce(client, http.MethodGet, r.MonolithURL+"/catalog", nil); err != nil {
			down = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !down {
		return fmt.Errorf("служба не упала — проверь режим модуля уведомлений в сценарии")
	}
	r.Record("catalog.down", nil)
	r.Record("orders.down", nil)

	// 4. Платформа поднимает процесс обратно. Простой измеряется, а не
	//    пересказывается: сцена ждёт готовности и печатает, сколько это заняло.
	back := time.Now()
	up := false
	for time.Now().Before(back.Add(90 * time.Second)) {
		if code, err := doOnce(client, http.MethodGet, r.MonolithURL+"/catalog", nil); err == nil && code == 200 {
			up = true
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if !up {
		return fmt.Errorf("служба не поднялась обратно за 90 с")
	}
	r.Record("catalog.back", nil)

	// Состояние: заказ, на котором процесс умер, не сохранился — транзакция
	// не была зафиксирована.
	var state struct {
		Totals struct {
			Orders int `json:"orders"`
		} `json:"totals"`
	}
	if err := r.getJSON(r.MonolithURL+"/_lab/state", &state); err != nil {
		return err
	}
	r.Set("orders_left", strconv.Itoa(state.Totals.Orders))

	r.collected = r.own.Snapshot()
	return nil
}
