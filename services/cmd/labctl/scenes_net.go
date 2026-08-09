package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"
)

// Сцены профиля net: между службой и уведомлениями стоит посредник, которому
// сцена портит сеть, и защиты, которые курс вводит после каждой поломки.

func (r *Run) orderBody() []byte {
	cfg := r.Script.Config
	b, _ := json.Marshal(map[string]any{
		"client": cfg.Client, "restaurant": cfg.Restaurant, "amount": cfg.Amount,
	})
	return b
}

// notifierState — то, что сервис уведомлений знает о себе сам.
type notifierState struct {
	Received         int64 `json:"received"`
	Rejected         int64 `json:"rejected"`
	Hanging          int64 `json:"hanging"`
	ProviderAttempts int64 `json:"provider_attempts"`
}

func (r *Run) notifierState() (notifierState, error) {
	var s notifierState
	err := r.getJSON(r.NotifierURL+"/_lab/state", &s)
	return s, err
}

// ── сцена 7 — задержка, разрыв, обрыв на полуслове (m05 l01) ────────────────
func scene07(ctx context.Context, r *Run) error {
	cfg := r.Script.Config
	if err := r.prepareProxy(); err != nil {
		return err
	}
	r.Set("notify_timeout", ms(cfg.NotifyTimeoutMS))
	r.Set("latency", ms(cfg.LatencyMS))

	if err := r.postJSON(r.AcquirerURL+"/_lab/config", map[string]any{
		"delay_ms": 0, "mode": "ok", "idempotent": false, "reset": true,
	}, nil); err != nil {
		return err
	}

	passes := []struct {
		key   string
		toxic string
		kind  string
		strm  string
		attrs map[string]any
	}{
		{"pass#1", "slow", "latency", "downstream", map[string]any{"latency": cfg.LatencyMS, "jitter": 0}},
		{"pass#2", "cut", "reset_peer", "upstream", map[string]any{"timeout": 0}},
		{"pass#3", "half", "limit_data", "downstream", map[string]any{"bytes": cfg.CutAfterB}},
	}

	client := &http.Client{Timeout: 30 * time.Second}
	for _, p := range passes {
		if err := r.configureNotifier(map[string]any{
			"mode": "ok", "strict": false, "provider_mode": "ok", "reset": true,
		}); err != nil {
			return err
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
		if err := r.breakNetwork(p.toxic, p.kind, p.strm, p.attrs); err != nil {
			return fmt.Errorf("%s: поломка сети не навесилась: %w", p.key, err)
		}

		start := time.Now()
		code, err := doOnce(client, http.MethodPost, r.MonolithURL+"/orders", r.orderBody())
		elapsed := time.Since(start)
		if err != nil {
			code = 0
		}
		_ = r.healNetwork(p.toxic)

		st, sErr := r.notifierState()
		if sErr != nil {
			return sErr
		}
		r.Measure(p.key, "order_code", float64(code))
		r.Measure(p.key, "elapsed_ms", float64(elapsed.Milliseconds()))
		r.Measure(p.key, "arrived", float64(st.Received+st.Rejected))
	}
	return nil
}

// ── сцена 8 — серый отказ (m05 l01) ─────────────────────────────────────────
func scene08(ctx context.Context, r *Run) error {
	cfg := r.Script.Config
	if err := r.prepareProxy(); err != nil {
		return err
	}
	r.Set("notify_timeout", ms(cfg.NotifyTimeoutMS))

	if err := r.postJSON(r.AcquirerURL+"/_lab/config", map[string]any{
		"delay_ms": 0, "mode": "ok", "idempotent": false, "reset": true,
	}, nil); err != nil {
		return err
	}
	if err := r.configureNotifier(map[string]any{
		"mode": "grey", "strict": false, "provider_mode": "ok", "reset": true,
	}); err != nil {
		return err
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

	client := &http.Client{Timeout: 30 * time.Second}
	probe := func(key, method, url string, body []byte) error {
		start := time.Now()
		code, err := doOnce(client, method, url, body)
		if err != nil {
			code = 0
		}
		r.Measure(key, "code", float64(code))
		r.Measure(key, "elapsed_ms", float64(time.Since(start).Milliseconds()))
		return nil
	}

	if err := probe("health#1", http.MethodGet, r.NotifierURL+"/health", nil); err != nil {
		return err
	}
	if err := probe("ready#1", http.MethodGet, r.NotifierURL+"/ready", nil); err != nil {
		return err
	}
	if err := probe("order#1", http.MethodPost, r.MonolithURL+"/orders", r.orderBody()); err != nil {
		return err
	}
	if err := probe("health#2", http.MethodGet, r.NotifierURL+"/health", nil); err != nil {
		return err
	}
	if err := probe("ready#2", http.MethodGet, r.NotifierURL+"/ready", nil); err != nil {
		return err
	}

	st, err := r.notifierState()
	if err != nil {
		return err
	}
	r.Measure("hanging", "count", float64(st.Hanging))
	r.Set("hanging", strconv.FormatInt(st.Hanging, 10))
	return nil
}

// ── сцена 9 — повтор на трёх уровнях (m05 l03) ──────────────────────────────
func scene09(ctx context.Context, r *Run) error {
	cfg := r.Script.Config
	if err := r.prepareProxy(); err != nil {
		return err
	}
	if err := r.postJSON(r.AcquirerURL+"/_lab/config", map[string]any{
		"delay_ms": 0, "mode": "ok", "idempotent": false, "reset": true,
	}, nil); err != nil {
		return err
	}
	// Провайдер уведомлений лежит. Все три участника цепочки повторяют — и
	// каждый считает, что уж он-то повторяет разумно.
	if err := r.configureNotifier(map[string]any{
		"mode": "ok", "strict": false, "reset": true,
		"provider_mode": "down", "provider_retries": cfg.ProviderRetries,
	}); err != nil {
		return err
	}
	if err := r.configure(map[string]any{
		"reset": true, "notify_timeout_ms": cfg.NotifyTimeoutMS,
		"notify_retries": cfg.NotifyRetries, "notify_retry_ms": cfg.NotifyRetryMS,
	}); err != nil {
		return err
	}
	if err := r.postJSON(r.MonolithURL+"/_lab/prepare", map[string]any{
		"first_order_id": cfg.FirstOrderID,
	}, nil); err != nil {
		return err
	}

	tries := cfg.ClientRetries + 1
	r.Set("client_tries", strconv.Itoa(tries))
	r.Set("service_tries", strconv.Itoa(cfg.NotifyRetries+1))
	r.Set("notifier_tries", strconv.Itoa(cfg.ProviderRetries+1))

	client := &http.Client{Timeout: 60 * time.Second}
	for i := 0; i < tries; i++ {
		if _, err := doOnce(client, http.MethodPost, r.MonolithURL+"/orders", r.orderBody()); err != nil {
			return fmt.Errorf("попытка %d: служба не ответила: %w", i+1, err)
		}
	}

	m, err := r.metrics(false)
	if err != nil {
		return err
	}
	st, err := r.notifierState()
	if err != nil {
		return err
	}

	r.Measure("level#1", "out", float64(tries))
	r.Measure("level#2", "out", float64(m.NotifyCalls))
	r.Measure("level#3", "out", float64(st.ProviderAttempts))
	r.Set("total", strconv.FormatInt(st.ProviderAttempts, 10))
	return nil
}

// ── сцена 10 — каскад с предохранителем и без (m05 l04) ─────────────────────
func scene10(ctx context.Context, r *Run) error {
	cfg := r.Script.Config
	if err := r.prepareProxy(); err != nil {
		return err
	}
	if err := r.postJSON(r.AcquirerURL+"/_lab/config", map[string]any{
		"delay_ms": 0, "mode": "ok", "idempotent": false, "reset": true,
	}, nil); err != nil {
		return err
	}
	r.Set("workers", strconv.Itoa(cfg.Workers))
	r.Set("notify_ms", ms(cfg.NotifyMS))
	r.Set("notify_timeout", ms(cfg.NotifyTimeoutMS))
	r.Set("rps", strconv.Itoa(cfg.RPS))
	r.Set("seconds", strconv.FormatFloat(cfg.Seconds, 'f', 0, 64))

	passes := []struct {
		key     string
		breaker bool
		degrade bool
	}{
		{"without", false, false},
		{"with", true, true},
	}

	for _, p := range passes {
		// Зависимость отвечает медленнее, чем её готовы ждать: вызов
		// заканчивается таймаутом, а обработчик всё это время занят.
		if err := r.configureNotifier(map[string]any{
			"mode": "slow", "delay_ms": cfg.NotifyMS, "strict": false,
			"provider_mode": "ok", "reset": true,
		}); err != nil {
			return err
		}
		if err := r.configure(map[string]any{
			"reset": true, "workers": cfg.Workers, "queue": 0,
			"catalog_ms": cfg.CatalogMS, "notify_timeout_ms": cfg.NotifyTimeoutMS,
			"breaker": p.breaker, "breaker_fails": cfg.BreakerFails,
			"breaker_open_ms": cfg.BreakerOpenMS, "degrade": p.degrade,
		}); err != nil {
			return err
		}
		if err := r.postJSON(r.MonolithURL+"/_lab/prepare", map[string]any{
			"first_order_id": cfg.FirstOrderID,
		}, nil); err != nil {
			return err
		}

		dur := time.Duration(cfg.Seconds * float64(time.Second))
		var orders, catalog loadStats
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			orders = r.load(ctx, http.MethodPost, r.MonolithURL+"/orders", r.orderBody(),
				cfg.RPS, dur, 10*time.Second)
		}()
		go func() {
			defer wg.Done()
			// Каталог — исправный сценарий, который ни в чём не виноват.
			catalog = r.load(ctx, http.MethodGet, r.MonolithURL+"/catalog", nil,
				5, dur, 10*time.Second)
		}()
		wg.Wait()

		m, err := r.metrics(false)
		if err != nil {
			return err
		}
		r.Measure(p.key, "orders_ok", float64(orders.OK))
		r.Measure(p.key, "catalog_p99", catalog.p(0.99))
		r.Measure(p.key, "catalog_ok", float64(catalog.OK))
		r.Measure(p.key, "busy_max", float64(m.Gate.MaxBusy))
		r.Measure(p.key, "short", float64(m.BreakerShort))

		time.Sleep(2 * time.Second) // хвост предыдущего прогона не должен течь в следующий
	}
	return nil
}

// ── сцена 11 — очередь без границы под перегрузкой (m05 l05) ────────────────
func scene11(ctx context.Context, r *Run) error {
	cfg := r.Script.Config
	capacity := float64(cfg.Workers) * 1000.0 / float64(cfg.CatalogMS)
	r.Set("workers", strconv.Itoa(cfg.Workers))
	r.Set("service_ms", strconv.Itoa(cfg.CatalogMS))
	r.Set("capacity", strconv.FormatFloat(capacity, 'f', 0, 64))
	r.Set("rps", strconv.Itoa(cfg.RPS))
	r.Set("queue_limit", strconv.Itoa(cfg.Queue))
	r.Set("seconds", strconv.FormatFloat(cfg.Seconds, 'f', 0, 64))

	dur := time.Duration(cfg.Seconds * float64(time.Second))

	run := func(prefix string, queue int) (loadStats, error) {
		if err := r.configure(map[string]any{
			"reset": true, "catalog_ms": cfg.CatalogMS, "workers": cfg.Workers, "queue": queue,
		}); err != nil {
			return loadStats{}, err
		}

		// Снимки во время подачи: важен не итог, а то, как растёт долг.
		done := make(chan struct{})
		var mu sync.Mutex
		samples := map[float64]serverMetrics{}
		begin := time.Now()
		go func() {
			defer close(done)
			// Отметки отсчитываются от начала подачи, а не друг от друга:
			// иначе третий замер придётся на восемнадцатую секунду, когда
			// нагрузка давно кончилась и очередь уже разошлась.
			for _, at := range cfg.Slices {
				wait := time.Until(begin.Add(time.Duration(at * float64(time.Second))))
				if wait > 0 {
					select {
					case <-time.After(wait):
					case <-ctx.Done():
						return
					}
				}
				m, err := r.metrics(false)
				if err != nil {
					continue
				}
				mu.Lock()
				samples[at] = m
				mu.Unlock()
			}
		}()

		st := r.load(ctx, http.MethodGet, r.MonolithURL+"/catalog", nil, cfg.RPS, dur, 60*time.Second)
		<-done

		mu.Lock()
		defer mu.Unlock()
		for _, at := range cfg.Slices {
			m, ok := samples[at]
			if !ok {
				continue
			}
			key := fmt.Sprintf("%s#%d", prefix, int(at))
			r.Measure(key, "waiting", float64(m.Gate.Waiting))
			r.Measure(key, "heap_mb", m.HeapMB)
			r.Measure(key, "goroutines", float64(m.Goroutines))
		}
		return st, nil
	}

	free, err := run("free", 0)
	if err != nil {
		return err
	}
	r.Measure("free.total", "p99_ms", free.p(0.99))
	r.Measure("free.total", "ok", float64(free.OK))
	r.Measure("free.total", "rejected", float64(free.Rejected))

	time.Sleep(3 * time.Second)

	bound, err := run("bound", cfg.Queue)
	if err != nil {
		return err
	}
	r.Measure("bound.total", "p99_ms", bound.p(0.99))
	r.Measure("bound.total", "ok", float64(bound.OK))
	r.Measure("bound.total", "rejected", float64(bound.Rejected))
	return nil
}

// ── сцена 12 — остановка сервиса под нагрузкой (m05 l06) ────────────────────
func scene12(ctx context.Context, r *Run) error {
	cfg := r.Script.Config
	r.Set("rps", strconv.Itoa(cfg.RPS))
	r.Set("service_ms", strconv.Itoa(cfg.CatalogMS))

	client := &http.Client{Timeout: 30 * time.Second}
	waitUp := func() error {
		deadline := time.Now().Add(90 * time.Second)
		for time.Now().Before(deadline) {
			if code, err := doOnce(client, http.MethodGet, r.MonolithURL+"/catalog", nil); err == nil && code == 200 {
				return nil
			}
			time.Sleep(200 * time.Millisecond)
		}
		return fmt.Errorf("служба не поднялась обратно за 90 с")
	}

	for _, mode := range []string{"graceful", "abrupt"} {
		if err := waitUp(); err != nil {
			return err
		}
		if err := r.configure(map[string]any{
			"reset": true, "catalog_ms": cfg.CatalogMS, "workers": cfg.Workers, "queue": 0,
		}); err != nil {
			return err
		}

		dur := time.Duration(cfg.Seconds * float64(time.Second))
		var st loadStats
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			st = r.load(ctx, http.MethodGet, r.MonolithURL+"/catalog", nil,
				cfg.RPS, dur, 20*time.Second)
		}()

		// Останавливаем в середине подачи: важно, что в этот момент внутри
		// службы есть принятая, но не доделанная работа.
		time.Sleep(dur / 2)
		_ = r.postJSON(r.MonolithURL+"/_lab/shutdown", map[string]any{"mode": mode}, nil)
		wg.Wait()

		r.Measure(mode, "ok", float64(st.OK))
		r.Measure(mode, "rejected", float64(st.Rejected))
		r.Measure(mode, "failed", float64(st.Failed))
		r.Measure(mode, "sent", float64(st.Sent))
		r.Measure(mode, "refused", float64(st.Refused))
	}
	return waitUp()
}
