package main

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Проверка гипотез (m12) идёт на профиле net — том же, где живут защиты,
// которые она и проверяет. Нового в стенд не приезжает ничего, и это не
// экономия, а содержание: проверка добавляет метод, а не машинерию. Отдельный
// профиль отрезал бы эти сцены от предохранителя, деградации и повторов —
// профили стенда не совмещаются.

// ── сцена 35 — гипотеза, поломка, наблюдение, вывод (m12 l03) ────────────────
//
// Три блока — три опыта над одной системой. В первом гипотеза подтвердилась,
// во втором рассыпалась, в третьем предмет опыта — сам опыт: что именно
// сломано и чем это немедленно прекратить.
func scene35(ctx context.Context, r *Run) error {
	cfg := r.Script.Config
	if err := r.prepareProxy(); err != nil {
		return err
	}
	if err := r.postJSON(r.AcquirerURL+"/_lab/config", map[string]any{
		"delay_ms": 0, "mode": "ok", "idempotent": false, "reset": true,
	}, nil); err != nil {
		return fmt.Errorf("эквайринг не настроен: %w", err)
	}

	r.Set("latency", ms(cfg.LatencyMS))
	r.Set("notify_timeout", ms(cfg.NotifyTimeoutMS))
	r.Set("workers", strconv.Itoa(cfg.Workers))
	r.Set("rps", strconv.Itoa(cfg.RPS))
	r.Set("seconds", strconv.FormatFloat(cfg.Seconds, 'f', 0, 64))
	r.Set("breaker_fails", strconv.Itoa(cfg.BreakerFails))
	r.Set("breaker_open", ms(cfg.BreakerOpenMS))
	r.Set("cut_bytes", strconv.Itoa(cfg.CutAfterB))
	r.Set("client_tries", strconv.Itoa(cfg.ClientRetries+1))
	r.Set("service_tries", strconv.Itoa(cfg.NotifyRetries+1))
	r.Set("dupes", strconv.Itoa((cfg.ClientRetries+1)*(cfg.NotifyRetries+1)))

	if err := r.hypothesisSurvives(ctx, cfg); err != nil {
		return err
	}
	if err := r.hypothesisFails(cfg); err != nil {
		return err
	}
	return r.blastRadius(cfg)
}

// hypothesisSurvives — блок 1. Медлит СЕТЬ, а не сервис: уведомления здоровы и
// отвечают мгновенно, задержка живёт на проводе. Это другой уровень поломки,
// чем «сервис стал медленным», и вносится он тоже иначе — снаружи, не трогая
// настроек сервиса.
func (r *Run) hypothesisSurvives(ctx context.Context, cfg ScriptConfig) error {
	if err := r.armDefences(cfg, true); err != nil {
		return err
	}
	if err := r.breakNetwork("slow", "latency", "downstream",
		map[string]any{"latency": cfg.LatencyMS, "jitter": 0}); err != nil {
		return fmt.Errorf("задержка сети не навесилась: %w", err)
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
		// Каталог ресторанов к уведомлениям не обращается: он и есть та часть
		// системы, о которой гипотеза утверждает «не пострадает».
		catalog = r.load(ctx, http.MethodGet, r.MonolithURL+"/catalog", nil,
			5, dur, 10*time.Second)
	}()
	wg.Wait()
	_ = r.healNetwork("slow")

	m, err := r.metrics(false)
	if err != nil {
		return err
	}
	r.Measure("h1.orders", "ok", float64(orders.OK))
	r.Measure("h1.orders", "sent", float64(orders.Sent))
	r.Measure("h1.catalog", "ok", float64(catalog.OK))
	r.Measure("h1.catalog", "sent", float64(catalog.Sent))
	r.Measure("h1.tail", "p99_ms", catalog.p(0.99))
	r.Measure("h1.short", "n", float64(m.BreakerShort))

	time.Sleep(2 * time.Second) // хвост блока не должен течь в следующий опыт
	return nil
}

// hypothesisFails — блок 2. Та же система, другой отказ: ответ обрывается на
// полуслове. Вызывающий считает вызов неудачным и честно повторяет — а работа
// на той стороне каждый раз сделана.
func (r *Run) hypothesisFails(cfg ScriptConfig) error {
	if err := r.configureNotifier(map[string]any{
		"mode": "ok", "strict": false, "provider_mode": "ok", "reset": true,
	}); err != nil {
		return err
	}
	if err := r.configure(map[string]any{
		"reset": true, "workers": 0, "queue": 0,
		"notify_timeout_ms": cfg.NotifyTimeoutMS,
		"notify_retries":    cfg.NotifyRetries, "notify_retry_ms": cfg.NotifyRetryMS,
		"breaker": false, "degrade": false,
	}); err != nil {
		return err
	}
	if err := r.postJSON(r.MonolithURL+"/_lab/prepare", map[string]any{
		"first_order_id": cfg.FirstOrderID,
	}, nil); err != nil {
		return err
	}
	if err := r.breakNetwork("half", "limit_data", "downstream",
		map[string]any{"bytes": cfg.CutAfterB}); err != nil {
		return fmt.Errorf("обрыв ответа не навесился: %w", err)
	}

	// Клиент подаёт заказ столько раз, сколько велит сцена, и каждый раз честно
	// считает вызов провалившимся: другого признака у него нет.
	client := &http.Client{Timeout: 30 * time.Second}
	tries := cfg.ClientRetries + 1
	clientErrors := 0
	for i := 0; i < tries; i++ {
		code, err := doOnce(client, http.MethodPost, r.MonolithURL+"/orders", r.orderBody())
		if err != nil || code >= 300 {
			clientErrors++
		}
	}
	_ = r.healNetwork("half")

	m, err := r.metrics(false)
	if err != nil {
		return err
	}
	st, err := r.notifierState()
	if err != nil {
		return err
	}
	var state struct {
		Totals struct {
			Orders int `json:"orders"`
		} `json:"totals"`
	}
	if err := r.getJSON(r.MonolithURL+"/_lab/state", &state); err != nil {
		return err
	}

	r.Measure("h2.calls", "n", float64(m.NotifyCalls))
	r.Measure("h2.seen", "n", float64(clientErrors))
	r.Measure("h2.delivered", "n", float64(st.Received))
	r.Measure("h2.orders", "n", float64(state.Totals.Orders))
	return nil
}

// blastRadius — блок 3. Предмет опыта здесь сам опыт: радиус выбран тремя
// ручками поломки, а прекращается он её снятием. Наблюдаемое число — сколько
// система приходит в себя после того, как сеть починили.
func (r *Run) blastRadius(cfg ScriptConfig) error {
	if err := r.armDefences(cfg, false); err != nil {
		return err
	}
	if err := r.breakNetwork("slow", "latency", "downstream",
		map[string]any{"latency": cfg.LatencyMS, "jitter": 0}); err != nil {
		return err
	}

	client := &http.Client{Timeout: 30 * time.Second}
	order := func() { _, _ = doOnce(client, http.MethodPost, r.MonolithURL+"/orders", r.orderBody()) }

	// Ждём, пока предохранитель разомкнётся: до этого момента снимать поломку
	// нечестно — прекращать надо то, что уже случилось.
	opened := false
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		order()
		m, err := r.metrics(false)
		if err == nil && m.BreakerOpen {
			opened = true
			break
		}
	}
	if !opened {
		return fmt.Errorf("предохранитель не разомкнулся за 30 с — проверь настройки сцены")
	}

	// Кнопка отмены: одна команда, и порча снята. Состояние стенда при этом не
	// трогается вовсе — прекращение опыта не должно быть вторым опытом.
	if err := r.healNetwork("slow"); err != nil {
		return err
	}
	healed := time.Now()
	before, err := r.notifierState()
	if err != nil {
		return err
	}

	recovered := -1.0
	for time.Since(healed) < 30*time.Second {
		order()
		st, err := r.notifierState()
		if err == nil && st.Received > before.Received {
			recovered = time.Since(healed).Seconds()
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if recovered < 0 {
		return fmt.Errorf("уведомления не пошли снова за 30 с после снятия поломки")
	}
	r.Measure("h3.back", "s", recovered)
	return nil
}

// armDefences ставит службу в то состояние, в котором её застаёт опыт: пул,
// предохранитель, деградация. Оба блока с задержкой сети настраивают её
// одинаково — иначе они сравнивали бы не поломки, а конфигурации. Пул и каталог
// нужны только тому блоку, который подаёт поток: в остальных ограничивать
// параллелизм нечем и незачем.
func (r *Run) armDefences(cfg ScriptConfig, underLoad bool) error {
	if err := r.configureNotifier(map[string]any{
		"mode": "ok", "strict": false, "provider_mode": "ok", "reset": true,
	}); err != nil {
		return err
	}
	workers, catalogMS := 0, 0
	if underLoad {
		workers, catalogMS = cfg.Workers, cfg.CatalogMS
	}
	if err := r.configure(map[string]any{
		"reset": true, "workers": workers, "queue": 0, "catalog_ms": catalogMS,
		"notify_timeout_ms": cfg.NotifyTimeoutMS, "notify_retries": 0,
		"breaker": true, "breaker_fails": cfg.BreakerFails,
		"breaker_open_ms": cfg.BreakerOpenMS, "degrade": true,
	}); err != nil {
		return err
	}
	return r.postJSON(r.MonolithURL+"/_lab/prepare", map[string]any{
		"first_order_id": cfg.FirstOrderID,
	}, nil)
}

// ── сцена 36 — нагрузочный профиль до точки излома (m12 l04) ─────────────────
//
// Тест здесь отвечает не на «выдержит ли», а на «что ломается первым и на каком
// числе». Поэтому у сцены есть объявленное обещание, и каждая ступень сверяется
// с ним, а не с ощущением «много».
func scene36(ctx context.Context, r *Run) error {
	cfg := r.Script.Config
	capacity := float64(cfg.Workers) * 1000.0 / float64(cfg.CatalogMS)

	r.Set("workers", strconv.Itoa(cfg.Workers))
	r.Set("service_ms", strconv.Itoa(cfg.CatalogMS))
	r.Set("capacity", strconv.FormatFloat(capacity, 'f', 0, 64))
	r.Set("step_seconds", strconv.FormatFloat(cfg.StepSeconds, 'f', 0, 64))
	r.Set("queue_limit", strconv.Itoa(cfg.Queue))
	r.Set("slo_p99", strconv.Itoa(cfg.SLOP99MS))
	// Десятичная запятая, а не точка: вывод сцены копируется в русский текст шага.
	r.Set("slo_errors", strings.Replace(
		strconv.FormatFloat(cfg.SLOErrorPct, 'f', -1, 64), ".", ",", 1))
	r.Set("rps", strconv.Itoa(cfg.RPS))
	r.Set("seconds", strconv.FormatFloat(cfg.Seconds, 'f', 0, 64))
	r.Set("clients", strconv.Itoa(cfg.ClosedClients))

	if err := r.steps(ctx, cfg, capacity); err != nil {
		return err
	}
	return r.twoModels(ctx, cfg)
}

// steps — блок 1. Ступени сравниваются не друг с другом, а с обещанием: столбец
// вердикта и есть весь смысл нагрузочного теста как проверки гипотезы.
func (r *Run) steps(ctx context.Context, cfg ScriptConfig, capacity float64) error {
	if err := r.configure(map[string]any{
		"reset": true, "catalog_ms": cfg.CatalogMS,
		"workers": cfg.Workers, "queue": cfg.Queue,
	}); err != nil {
		return fmt.Errorf("служба не настроена: %w", err)
	}
	// Прогрев в замер не попадает — по той же причине, по которой его
	// выбрасывают в настоящем тесте: первые соединения меряют не службу.
	r.load(ctx, http.MethodGet, r.MonolithURL+"/catalog", nil, 20, 2*time.Second, 10*time.Second)

	step := time.Duration(cfg.StepSeconds * float64(time.Second))
	for i, rps := range cfg.Steps {
		if _, err := r.metrics(true); err != nil {
			return err
		}
		st := r.load(ctx, http.MethodGet, r.MonolithURL+"/catalog", nil, rps, step, 30*time.Second)

		key := fmt.Sprintf("step#%d", i+1)
		r.Measure(key, "util", float64(rps)/capacity*100)
		r.Measure(key, "p99_ms", st.p(0.99))
		r.Measure(key, "rejected", float64(st.Rejected))
		r.Measure(key, "error_pct", st.errorPct())

		if i < len(cfg.Steps)-1 {
			time.Sleep(3 * time.Second) // очередь ступени должна разойтись до следующей
		}
	}
	return nil
}

// twoModels — блок 2. Одна служба, один номинальный поток, две модели подачи —
// и два противоположных вердикта. Разница не в настройке теста, а в том, какой
// вопрос он задаёт.
func (r *Run) twoModels(ctx context.Context, cfg ScriptConfig) error {
	dur := time.Duration(cfg.Seconds * float64(time.Second))

	// Граница очереди здесь снята: предмет блока — не отказ на границе, а то,
	// что в закрытой модели очереди не возникает вовсе.
	arm := func() error {
		if err := r.configure(map[string]any{
			"reset": true, "catalog_ms": cfg.CatalogMS, "workers": cfg.Workers, "queue": 0,
		}); err != nil {
			return err
		}
		_, err := r.metrics(true)
		return err
	}

	if err := arm(); err != nil {
		return err
	}
	start := time.Now()
	open := r.load(ctx, http.MethodGet, r.MonolithURL+"/catalog", nil,
		cfg.RPS, dur, 60*time.Second)
	openTook := time.Since(start)
	openGate, err := r.metrics(false)
	if err != nil {
		return err
	}

	time.Sleep(3 * time.Second)

	if err := arm(); err != nil {
		return err
	}
	start = time.Now()
	closed := r.loadClosed(ctx, http.MethodGet, r.MonolithURL+"/catalog", nil,
		cfg.ClosedClients, dur, 60*time.Second)
	closedTook := time.Since(start)
	closedGate, err := r.metrics(false)
	if err != nil {
		return err
	}

	// Пропускную способность считаем по всему времени, включая разбор долга:
	// в открытой модели заявки продолжают обслуживаться и после того, как
	// подача кончилась, и не учесть это значило бы приписать службе чужие rps.
	r.Measure("open", "rps_out", float64(open.OK)/openTook.Seconds())
	r.Measure("open", "p99_ms", open.p(0.99))
	r.Measure("open", "queue_max", float64(openGate.Gate.MaxWaiting))
	r.Measure("closed", "rps_out", float64(closed.OK)/closedTook.Seconds())
	r.Measure("closed", "p99_ms", closed.p(0.99))
	r.Measure("closed", "queue_max", float64(closedGate.Gate.MaxWaiting))
	return nil
}
