package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/kipyatl0/backend-architecture-lab/services/internal/lab"
)

// Поведение службы задаётся сценой целиком: время обработки, размер пула,
// граница очереди, поведение уведомлений, повторы и предохранитель. Ничего
// случайного внутри нет — студент сверяет свой вывод с текстом шага.
type knobs struct {
	// Каталог ресторанов — дешёвое чтение, на котором курс показывает
	// очередь. Обработчик занимает ровно столько, сколько сказано: иначе
	// излом на утилизации утонул бы в шуме ноутбука.
	CatalogMS  int `json:"catalog_ms"`
	SlowEveryN int `json:"slow_every_n"`
	SlowMS     int `json:"slow_ms"`
	Workers    int `json:"workers"`
	Queue      int `json:"queue"`

	// Модуль уведомлений: ok — работает, slow — отвечает медленно,
	// hang — не отвечает никогда, down — отказ, crash — уносит процесс.
	NotifyMode      string `json:"notify_mode"`
	NotifyMS        int    `json:"notify_ms"`
	NotifyTimeoutMS int    `json:"notify_timeout_ms"`
	NotifyRetries   int    `json:"notify_retries"`
	NotifyRetryMS   int    `json:"notify_retry_ms"`
	// Какую версию сообщения отправляет служба: v1 — как договаривались,
	// v1+extra — добавлено новое поле, v2 — поле переименовано.
	NotifySchema string `json:"notify_schema"`

	// Предохранитель на вызове уведомлений.
	Breaker       bool `json:"breaker"`
	BreakerFails  int  `json:"breaker_fails"`
	BreakerOpenMS int  `json:"breaker_open_ms"`

	// Необязательная часть ответа: при деградации заказ принимается без неё.
	Degrade bool `json:"degrade"`
}

func defaultKnobs() knobs {
	return knobs{
		CatalogMS: 0, SlowEveryN: 0, SlowMS: 0, Workers: 0, Queue: 0,
		NotifyMode: "ok", NotifyMS: 0, NotifyTimeoutMS: 2000,
		NotifyRetries: 0, NotifyRetryMS: 0, NotifySchema: "v1",
		Breaker: false, BreakerFails: 5, BreakerOpenMS: 5000,
		Degrade: false,
	}
}

// control — вся управляемая сценой часть службы в одном месте.
type control struct {
	mu sync.RWMutex
	k  knobs

	gate *lab.Gate
	lat  lab.Latency

	seq atomic.Int64 // номер запроса к каталогу: «каждый N-й долгий» без случайности

	// предохранитель
	bmu       sync.Mutex
	bFails    int
	bOpenTill time.Time
	bShort    atomic.Int64 // сколько вызовов отсечено открытым предохранителем

	notifyCalls atomic.Int64 // сколько раз служба реально позвала уведомления

	draining atomic.Bool
}

func newControl() *control {
	c := &control{k: defaultKnobs()}
	c.gate = lab.NewGate(0, 0)
	return c
}

func (c *control) knobs() knobs {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.k
}

// ── каталог: сценарий чтения, на котором живут нагрузочные сцены ────────────

var restaurants = []string{
	"Пекарня «Батон»", "Плов-хаус", "Суши на углу",
	"Кофейня «Пар»", "Бургерная «Гриль»",
}

func (a *app) handleCatalog(w http.ResponseWriter, r *http.Request) {
	if a.ctl.draining.Load() {
		// Остановка началась: новую работу не принимаем, и говорим об этом
		// честным кодом, а не молчанием.
		w.Header().Set("Connection", "close")
		lab.WriteJSON(w, http.StatusServiceUnavailable,
			map[string]string{"error": "служба останавливается", "code": "shutting_down"})
		return
	}

	start := time.Now()
	release, ok := a.ctl.gate.Acquire(r.Context())
	if !ok {
		a.ctl.lat.Rejected()
		w.Header().Set("Retry-After", "1")
		lab.WriteJSON(w, http.StatusServiceUnavailable,
			map[string]string{"error": "очередь заполнена", "code": "overloaded"})
		return
	}

	// Обработчик занят ровно столько, сколько велела сцена, — считая от
	// момента, когда он взялся за запрос. Всё, что после (запись ответа,
	// журнал), делается уже освободившимся: иначе настоящая ёмкость службы
	// оказывается ниже расчётной, и потолок в шапке сцены становится враньём.
	taken := time.Now()
	d := time.Duration(a.ctl.knobs().CatalogMS) * time.Millisecond
	if k := a.ctl.knobs(); k.SlowEveryN > 0 && a.ctl.seq.Add(1)%int64(k.SlowEveryN) == 0 {
		d += time.Duration(k.SlowMS) * time.Millisecond
	}
	if d > 0 {
		t := time.NewTimer(time.Until(taken.Add(d)))
		defer t.Stop()
		select {
		case <-t.C:
		case <-r.Context().Done():
			release()
			a.ctl.lat.Failed()
			return
		}
	}
	release()

	a.ctl.lat.Observe(time.Since(start))
	lab.WriteJSON(w, http.StatusOK, map[string]any{"restaurants": restaurants})
}

// ── модуль уведомлений ──────────────────────────────────────────────────────

// notify — вызов модуля уведомлений. В профиле mono модуль живёт в этом же
// процессе и просто пишет строку; в профиле split он за сетью, и у того же
// вызова появляется третий исход. Повторы и предохранитель навешаны здесь же:
// курс показывает их на одном и том же вызове.
func (a *app) notify(ctx context.Context, q querier, orderID int64, body string, attempt int64) (string, error) {
	k := a.ctl.knobs()
	order := strconv.FormatInt(orderID, 10)

	if k.NotifyMode == "crash" {
		// Ошибка в редком модуле, до которой никому не было дела. Паника в
		// горутине не ловится обработчиком: она уносит процесс целиком.
		go func() {
			time.Sleep(50 * time.Millisecond)
			var broken *string
			_ = *broken // nil-разыменование: процесс умирает здесь
		}()
		time.Sleep(300 * time.Millisecond)
		return "", fmt.Errorf("модуль уведомлений сломан")
	}

	// В timeline попадает обмен, а не всякий вызов. Пока уведомления — модуль
	// этого же процесса, вызывать их и вызывать функцию — одно и то же, и
	// строки в ленте у этого нет. Она появляется ровно тогда, когда появляется
	// сеть: в этом и состоит содержание сцены 5.
	wire := a.notifier != ""
	rec := func(key string, f map[string]string) {
		if wire {
			a.rec.Record(key, f)
		}
	}

	if k.Breaker && a.ctl.breakerOpen() {
		a.ctl.bShort.Add(1)
		rec(fmt.Sprintf("orders.short.notifier#%d", attempt), map[string]string{"order": order})
		return "", errBreakerOpen
	}

	attempts := k.NotifyRetries + 1
	var last error
	for i := 1; i <= attempts; i++ {
		if i > 1 && k.NotifyRetryMS > 0 {
			select {
			case <-time.After(time.Duration(k.NotifyRetryMS) * time.Millisecond):
			case <-ctx.Done():
				return "", ctx.Err()
			}
		}
		a.ctl.notifyCalls.Add(1)
		rec(fmt.Sprintf("orders.call.notifier#%d.%d", attempt, i),
			map[string]string{"order": order, "try": strconv.Itoa(i)})

		id, err := a.notifyOnce(ctx, q, orderID, body, k)
		if err == nil {
			a.ctl.breakerSuccess()
			rec(fmt.Sprintf("orders.recv.notifier#%d.%d", attempt, i),
				map[string]string{"order": order, "notification": id})
			return id, nil
		}
		last = err
		rec(fmt.Sprintf("orders.fail.notifier#%d.%d", attempt, i),
			map[string]string{"order": order, "error": err.Error()})
	}
	a.ctl.breakerFailure(k)
	return "", last
}

var errBreakerOpen = fmt.Errorf("предохранитель разомкнут: вызов не отправлен")

func (a *app) notifyOnce(ctx context.Context, q querier, orderID int64, body string, k knobs) (string, error) {
	switch k.NotifyMode {
	case "hang":
		<-ctx.Done()
		return "", ctx.Err()
	case "down":
		return "", fmt.Errorf("модуль уведомлений недоступен")
	case "slow":
		select {
		case <-time.After(time.Duration(k.NotifyMS) * time.Millisecond):
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}

	if a.notifier == "" { // профиль mono: модуль в этом же процессе
		var id int64
		err := q.QueryRow(ctx,
			`INSERT INTO notifications (order_id, channel, body) VALUES ($1,'push',$2) RETURNING id`,
			orderID, body).Scan(&id)
		if err != nil {
			return "", err
		}
		return "n_" + strconv.FormatInt(id, 10), nil
	}

	// профиль split: тот же вызов, но по сети
	msg := map[string]any{"order": orderID, "body": body}
	switch k.NotifySchema {
	case "v1+extra":
		// Совместимое изменение: поле добавлено. Пережить его — обязанность
		// потребителя, и сцена показывает, кто эту обязанность не выполнил.
		msg["channel"] = "push"
	case "v2":
		// Несовместимое: то же самое поле называется иначе.
		delete(msg, "body")
		msg["text"] = body
	}
	payload, _ := json.Marshal(msg)
	cctx, cancel := context.WithTimeout(ctx, time.Duration(k.NotifyTimeoutMS)*time.Millisecond)
	defer cancel()
	req, err := http.NewRequestWithContext(cctx, http.MethodPost, a.notifier+"/notifications",
		bytes.NewReader(payload))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := a.notifyClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	var out struct {
		ID string `json:"id"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&out)
	if resp.StatusCode >= 300 {
		return "", fmt.Errorf("уведомления ответили %d", resp.StatusCode)
	}
	return out.ID, nil
}

// ── предохранитель ──────────────────────────────────────────────────────────

func (c *control) breakerOpen() bool {
	c.bmu.Lock()
	defer c.bmu.Unlock()
	return time.Now().Before(c.bOpenTill)
}

func (c *control) breakerSuccess() {
	c.bmu.Lock()
	defer c.bmu.Unlock()
	c.bFails = 0
}

func (c *control) breakerFailure(k knobs) {
	if !k.Breaker {
		return
	}
	c.bmu.Lock()
	defer c.bmu.Unlock()
	c.bFails++
	if c.bFails >= k.BreakerFails {
		c.bOpenTill = time.Now().Add(time.Duration(k.BreakerOpenMS) * time.Millisecond)
	}
}

// ── управление сценой ───────────────────────────────────────────────────────

func (a *app) handleLabConfig(w http.ResponseWriter, r *http.Request) {
	var req struct {
		CatalogMS       *int    `json:"catalog_ms"`
		SlowEveryN      *int    `json:"slow_every_n"`
		SlowMS          *int    `json:"slow_ms"`
		Workers         *int    `json:"workers"`
		Queue           *int    `json:"queue"`
		NotifyMode      *string `json:"notify_mode"`
		NotifyMS        *int    `json:"notify_ms"`
		NotifyTimeoutMS *int    `json:"notify_timeout_ms"`
		NotifyRetries   *int    `json:"notify_retries"`
		NotifyRetryMS   *int    `json:"notify_retry_ms"`
		NotifySchema    *string `json:"notify_schema"`
		Breaker         *bool   `json:"breaker"`
		BreakerFails    *int    `json:"breaker_fails"`
		BreakerOpenMS   *int    `json:"breaker_open_ms"`
		Degrade         *bool   `json:"degrade"`
		Reset           bool    `json:"reset"`
	}
	if err := lab.ReadJSON(r, &req); err != nil {
		lab.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	c := a.ctl
	c.mu.Lock()
	if req.Reset {
		c.k = defaultKnobs()
	}
	set := func(dst *int, src *int) {
		if src != nil {
			*dst = *src
		}
	}
	set(&c.k.CatalogMS, req.CatalogMS)
	set(&c.k.SlowEveryN, req.SlowEveryN)
	set(&c.k.SlowMS, req.SlowMS)
	set(&c.k.Workers, req.Workers)
	set(&c.k.Queue, req.Queue)
	set(&c.k.NotifyMS, req.NotifyMS)
	set(&c.k.NotifyTimeoutMS, req.NotifyTimeoutMS)
	set(&c.k.NotifyRetries, req.NotifyRetries)
	set(&c.k.NotifyRetryMS, req.NotifyRetryMS)
	set(&c.k.BreakerFails, req.BreakerFails)
	set(&c.k.BreakerOpenMS, req.BreakerOpenMS)
	if req.NotifyMode != nil {
		c.k.NotifyMode = *req.NotifyMode
	}
	if req.NotifySchema != nil {
		c.k.NotifySchema = *req.NotifySchema
	}
	if req.Breaker != nil {
		c.k.Breaker = *req.Breaker
	}
	if req.Degrade != nil {
		c.k.Degrade = *req.Degrade
	}
	k := c.k
	c.mu.Unlock()

	c.gate.Configure(k.Workers, k.Queue)
	c.lat.Reset()
	c.seq.Store(0)
	c.bShort.Store(0)
	c.notifyCalls.Store(0)
	c.bmu.Lock()
	c.bFails, c.bOpenTill = 0, time.Time{}
	c.bmu.Unlock()

	a.log.Info("сцена настроила службу", "workers", k.Workers, "queue", k.Queue,
		"catalog_ms", k.CatalogMS, "notify_mode", k.NotifyMode, "breaker", k.Breaker)
	lab.WriteJSON(w, http.StatusOK, k)
}

func (a *app) handleLabMetrics(w http.ResponseWriter, r *http.Request) {
	if r.URL.Query().Get("reset") == "1" {
		a.ctl.lat.Reset()
		a.ctl.gate.ResetPeaks()
	}
	// Память процесса — наблюдаемая величина, а не метафора: очередь без
	// границы копит запросы, и каждый ждущий запрос занимает место.
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)

	lab.WriteJSON(w, http.StatusOK, map[string]any{
		"latency":       a.ctl.lat.State(),
		"gate":          a.ctl.gate.State(),
		"notify_calls":  a.ctl.notifyCalls.Load(),
		"breaker_short": a.ctl.bShort.Load(),
		"breaker_open":  a.ctl.breakerOpen(),
		"heap_mb":       float64(mem.HeapAlloc) / (1024 * 1024),
		"goroutines":    runtime.NumGoroutine(),
	})
}

func (a *app) handleLabBuckets(w http.ResponseWriter, r *http.Request) {
	var bounds []float64
	if s := r.URL.Query().Get("bounds"); s != "" {
		for _, part := range splitCommas(s) {
			v, err := strconv.ParseFloat(part, 64)
			if err != nil {
				lab.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "плохие границы"})
				return
			}
			bounds = append(bounds, v)
		}
	}
	lab.WriteJSON(w, http.StatusOK, map[string]any{
		"bounds": bounds, "counts": a.ctl.lat.Buckets(bounds),
	})
}

func splitCommas(s string) []string {
	out := []string{}
	cur := ""
	for _, r := range s {
		if r == ',' {
			if cur != "" {
				out = append(out, cur)
			}
			cur = ""
			continue
		}
		cur += string(r)
	}
	if cur != "" {
		out = append(out, cur)
	}
	return out
}

// handleLabShutdown останавливает службу так, как велела сцена. Разница между
// двумя способами и есть предмет урока про корректную остановку.
func (a *app) handleLabShutdown(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Mode string `json:"mode"` // graceful | abrupt
	}
	if err := lab.ReadJSON(r, &req); err != nil {
		lab.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	mode := req.Mode
	if mode == "" {
		mode = "graceful"
	}
	lab.WriteJSON(w, http.StatusOK, map[string]string{"mode": mode})

	go func() {
		time.Sleep(50 * time.Millisecond)
		if mode == "abrupt" {
			a.log.Info("грубая остановка: процесс уходит немедленно")
			os.Exit(1)
		}
		// Корректная остановка — это четыре шага, и первый из них не «выйти».
		// Сначала перестать принимать новое и сказать об этом платформе
		// (готовность краснеет, новые запросы получают 503), потом отдать
		// сигнал самому себе: дальше работает обычный обработчик SIGTERM,
		// который закрывает простаивающие соединения и дожидается активных.
		a.log.Info("корректная остановка: перестаём принимать новое")
		a.ctl.draining.Store(true)
		time.Sleep(300 * time.Millisecond) // платформе нужно успеть увидеть /ready
		if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
			a.log.Error("сигнал не отправлен", "err", err)
			os.Exit(0)
		}
	}()
}
