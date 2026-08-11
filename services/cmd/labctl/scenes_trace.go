package main

// Профиль trace (m11). Наблюдаемость перестаёт быть «посмотрим логи»: у запроса
// появляется сквозной контекст, у работы — дерево отрезков, у инцидента —
// порядок разбора. Обе сцены профиля построены на одном и том же приёмнике
// трейсов и на одном и том же вопросе: что именно из трёх сигналов отвечает на
// какой вопрос.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/kipyatl0/backend-architecture-lab/services/internal/lab"
)

// traceServices — у кого спрашивать трейсы. Один и тот же трейс приёмник
// отдаёт по запросу о любом своём участнике; спрашиваем всех троих, потому что
// в первом прогоне участники расходятся по разным трейсам.
var traceServices = []string{"orders", "courier", "notifier"}

// ── сцена 33 — трейс, порвавшийся о брокер (m11 l01) ────────────────────────
//
// Один и тот же путь заказа проходится дважды. Разница между прогонами — одна
// строка в отправителе: положить контекст в заголовки сообщения или не класть.
// По HTTP контекст едет сам, и это скрывает проблему до первого брокера.
func scene33(ctx context.Context, r *Run) error {
	cfg := r.Script.Config
	// Наблюдает сцена и приёмник трейсов; своих событий сервисы здесь не
	// рассказывают — предмет сцены в том, что собралось у приёмника.
	r.Sources = nil

	// Задержки провайдера здесь сняты явно, а не по умолчанию: сцена 34 их
	// выставляет, и порядок запуска сцен не должен решать, что покажет эта.
	if err := r.configureNotifier(map[string]any{
		"reset": true, "mode": "ok", "provider_ms": 0, "slow_every_n": 0,
	}); err != nil {
		return fmt.Errorf("уведомления не настроены — поднят ли профиль trace? %w", err)
	}
	if err := r.configureCourier(map[string]any{"reset": true, "source": "none"}); err != nil {
		return fmt.Errorf("курьеры не настроены: %w", err)
	}
	if err := r.configureOrders(map[string]any{
		"reset": true, "write_mode": "direct", "target": "kafka",
	}); err != nil {
		return fmt.Errorf("заказы не настроены: %w", err)
	}
	if err := r.prepareOrders(cfg.FirstOrderID); err != nil {
		return err
	}
	if err := lab.WaitKafka(ctx, r.Brokers, 60*time.Second); err != nil {
		return err
	}
	if err := lab.RecreateTopic(ctx, r.Brokers, lab.TopicParts); err != nil {
		return fmt.Errorf("лог не пересоздан: %w", err)
	}
	// Курьеры читают лог и после назначения зовут уведомления обычным вызовом:
	// третий участник нужен, чтобы стало видно, что разрыв случился именно на
	// брокере, а дальше по HTTP всё склеилось само.
	if err := r.configureCourier(map[string]any{
		"source": "kafka", "ack": "after", "notify": true,
	}); err != nil {
		return err
	}

	r.T0 = time.Now()
	done := int64(0)

	for i, propagate := range []bool{false, true} {
		run := i + 1
		since := time.Now()
		if err := r.configureOrders(map[string]any{"propagate": propagate}); err != nil {
			return err
		}
		ids, err := r.createOrders(1, cfg.Client, cfg.Restaurant, cfg.Amount)
		if err != nil {
			return err
		}
		done++
		if _, err := r.waitProcessed(ctx, done, 30*time.Second); err != nil {
			return err
		}

		traces, err := r.waitForSpans(ctx, traceServices, since, 7, 25*time.Second)
		if err != nil {
			return err
		}

		spans := 0
		row := 0
		for n, t := range traces {
			for _, s := range t.Spans {
				spans++
				row++
				key := fmt.Sprintf("r%d_%d", run, row)
				r.Set(key+"_span", indent(s.Depth, s.Name))
				r.Set(key+"_service", s.Service)
				r.Set(key+"_parent", parentName(t, s))
				r.Set(key+"_trace", strconv.Itoa(n+1))
			}
		}
		// Числа трейсов и спанов — единственное, что сцена сверяет: они и есть
		// её утверждение. Всё остальное в таблице печатается из наблюдения.
		r.Measure(fmt.Sprintf("r%d", run), "trees", float64(len(traces)))
		r.Measure(fmt.Sprintf("r%d", run), "spans", float64(spans))
		r.Set(fmt.Sprintf("r%d_trees", run), strconv.Itoa(len(traces)))
		r.Set(fmt.Sprintf("r%d_spans", run), strconv.Itoa(spans))
		r.Set(fmt.Sprintf("r%d_order", run), strconv.FormatInt(ids[0], 10))
		if len(traces) > 0 {
			r.Set(fmt.Sprintf("r%d_ids", run), traceIDs(traces))
		}
	}

	if err := r.configureCourier(map[string]any{"source": "none"}); err != nil {
		return err
	}
	return nil
}

// parentName — чьим продолжением спан является, по имени. Для корня — прочерк:
// именно он и означает «этот отрезок работы ни с чем не связан».
func parentName(t traceTree, s traceSpan) string {
	if s.Parent == "" {
		return "—"
	}
	for _, p := range t.Spans {
		if p.SpanID == s.Parent {
			return p.Name
		}
	}
	return "—"
}

func traceIDs(traces []traceTree) string {
	out := ""
	for i, t := range traces {
		if i > 0 {
			out += ", "
		}
		out += fmt.Sprintf("%d — %s", i+1, t.ID)
	}
	return out
}

// ── сцена 34 — деградация: от симптома к причине (m11 l04) ──────────────────
//
// Инцидент разбирается в одном и том же порядке, и порядок этот не про
// инструменты, а про вопросы. Метрика отвечает «что и насколько», трейс —
// «где именно», лог — «что там случилось». Ни один из трёх не отвечает на
// чужой вопрос, и пропуск любого превращает разбор в угадывание.
func scene34(ctx context.Context, r *Run) error {
	cfg := r.Script.Config
	r.Sources = nil

	total := cfg.Clients
	if total == 0 {
		total = 60
	}
	slowEvery := cfg.SlowEveryN
	if slowEvery == 0 {
		slowEvery = 20
	}
	slowMS := cfg.SlowMS
	if slowMS == 0 {
		slowMS = 2300
	}

	// Внешние участники стоят времени и в обычном режиме: эквайринг отвечает
	// за десятки миллисекунд, провайдер уведомлений — за единицы. Без этого
	// дерево трейса состояло бы из одного долгого спана и четырёх нулей, и
	// сравнивать доли было бы не с чем.
	if err := r.postJSON(r.AcquirerURL+"/_lab/config", map[string]any{
		"reset": true, "delay_ms": cfg.AcquirerDelayMS, "mode": "ok",
	}, nil); err != nil {
		return fmt.Errorf("эквайринг не настроен: %w", err)
	}
	if err := r.configureNotifier(map[string]any{
		"reset": true, "mode": "ok", "strict": true,
		"provider_ms": cfg.NotifyMS, "slow_every_n": slowEvery, "slow_provider_ms": slowMS,
	}); err != nil {
		return fmt.Errorf("уведомления не настроены — поднят ли профиль trace? %w", err)
	}
	if err := r.configureCourier(map[string]any{"reset": true, "source": "none"}); err != nil {
		return err
	}
	if err := r.configureOrders(map[string]any{
		"reset": true, "write_mode": "none", "saga": true, "compensate": true,
	}); err != nil {
		return err
	}
	if err := r.prepareOrders(cfg.FirstOrderID); err != nil {
		return err
	}

	client := &http.Client{Timeout: 60 * time.Second}
	r.T0 = time.Now()
	since := time.Now()

	// ── симптом: числа службы за прогон ─────────────────────────────────────
	// Считает их здесь сама сцена, потому что клиент — это она. В жизни те же
	// числа отдаёт сама служба, и вопрос «чьи это цифры» стоит задавать всегда:
	// у клиента в них входит сеть, у службы — нет.
	var writes, reads []time.Duration
	var ids []int64
	writeErrs, readErrs := 0, 0

	for i := 0; i < total; i++ {
		body, _ := json.Marshal(map[string]any{
			"client": cfg.Client, "restaurant": cfg.Restaurant, "amount": cfg.Amount,
		})
		start := time.Now()
		resp, err := client.Post(r.OrdersURL+"/orders", "application/json", bytesReader(body))
		if err != nil {
			writeErrs++
			continue
		}
		var out struct {
			Order int64 `json:"order"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&out)
		resp.Body.Close()
		writes = append(writes, time.Since(start))
		if resp.StatusCode >= 300 {
			writeErrs++
			continue
		}
		ids = append(ids, out.Order)
	}
	for _, id := range ids {
		start := time.Now()
		resp, err := client.Get(r.OrdersURL + "/orders/" + strconv.FormatInt(id, 10))
		if err != nil {
			readErrs++
			continue
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		reads = append(reads, time.Since(start))
		if resp.StatusCode >= 300 {
			readErrs++
		}
	}

	r.Set("total", strconv.Itoa(total))
	r.Set("writes", strconv.Itoa(len(writes)))
	r.Set("reads", strconv.Itoa(len(reads)))
	r.Set("write_errors", strconv.Itoa(writeErrs))
	r.Set("read_errors", strconv.Itoa(readErrs))
	r.Set("slow_every", strconv.Itoa(slowEvery))
	r.Set("slow", strconv.Itoa(len(writes)/slowEvery))

	for _, m := range []struct {
		key  string
		obs  []time.Duration
		errs int
	}{
		{"write", writes, writeErrs},
		{"read", reads, readErrs},
	} {
		r.Measure(m.key, "requests", float64(len(m.obs)))
		r.Measure(m.key, "errors", float64(m.errs))
		r.Measure(m.key, "p50", percentile(m.obs, 0.50).Seconds())
		r.Measure(m.key, "p95", percentile(m.obs, 0.95).Seconds())
		r.Measure(m.key, "p99", percentile(m.obs, 0.99).Seconds())
	}

	// ── трейс: где именно ушло время ────────────────────────────────────────
	// Метрика назвала эндпойнт и величину, но не назвала место. Место называет
	// дерево одного конкретного запроса — любого из тех, что попали в хвост.
	//
	// Пять отрезков на каждое оформление плюс по одному на каждое чтение: пока
	// приёмник не собрал столько, дерево разбираемого запроса может быть
	// неполным, и разбор пошёл бы по половине картинки.
	want := len(writes)*5 + len(reads)
	traces, err := r.waitForSpans(ctx, traceServices, since, want, 40*time.Second)
	if err != nil {
		return err
	}
	// Порог — половина заданной задержки провайдера: всё, что дольше, пришло
	// из хвоста распределения, и годится в пример одинаково. Берём первый
	// такой, а не самый долгий: «самый долгий» из трёх почти одинаковых
	// выбирался бы дрожанием машины, и вывод сцены перестал бы повторяться.
	slowest, ok := firstSlowTrace(traces, "POST /orders",
		time.Duration(slowMS/2)*time.Millisecond)
	if !ok {
		return fmt.Errorf("в приёмнике нет ни одного долгого трейса запроса POST /orders")
	}
	root := slowest.Root()
	r.Set("trace_id", slowest.ID)
	r.Set("trace_spans", strconv.Itoa(len(slowest.Spans)))
	r.Set("trace_services", fmt.Sprintf("%d", len(slowest.Services())))

	for i, s := range slowest.Spans {
		key := fmt.Sprintf("t%d", i+1)
		r.Set(key+"_span", indent(s.Depth, s.Name))
		r.Set(key+"_service", s.Service)
		r.Set(key+"_ms", human(s.Duration))
	}
	// Виновник — самый долгий спан без своих детей: время, которое не ушло
	// дальше по дереву, и есть время, потраченное здесь.
	guilty := leafMax(slowest)
	r.Set("guilty", guilty.Name)
	r.Set("guilty_service", guilty.Service)
	r.Measure("trace", "total", root.Duration.Seconds())
	r.Measure("trace", "guilty", guilty.Duration.Seconds())
	r.Measure("trace", "spans", float64(len(slowest.Spans)))

	// ── лог: что там случилось ──────────────────────────────────────────────
	// Строки выбраны не глазами и не по времени, а по метке трассировки: у
	// этого запроса она одна на все три сервиса, и по ней лог сходится с
	// деревом без единого предположения.
	lines, err := r.logLines(slowest.ID)
	if err != nil {
		return err
	}
	for i, l := range lines {
		key := fmt.Sprintf("l%d", i+1)
		r.Set(key+"_service", l.Service)
		r.Set(key+"_text", logText(l))
	}
	r.Measure("log", "lines", float64(len(lines)))
	return nil
}

// logLines собирает строки лога всех сервисов, помеченные этим трейсом.
func (r *Run) logLines(traceID string) ([]lab.LogLine, error) {
	var out []lab.LogLine
	for _, base := range []string{r.OrdersURL, r.NotifierURL} {
		var body struct {
			Lines []lab.LogLine `json:"lines"`
		}
		if err := r.getJSON(base+"/_lab/log?trace="+traceID, &body); err != nil {
			return nil, err
		}
		out = append(out, body.Lines...)
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].At.Before(out[j].At) })
	return out, nil
}

// logText — строка лога так, как её читает человек: сообщение и его поля.
func logText(l lab.LogLine) string {
	keys := make([]string, 0, len(l.Fields))
	for k := range l.Fields {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := l.Message
	for _, k := range keys {
		out += fmt.Sprintf(" · %s=%s", k, l.Fields[k])
	}
	return out
}

// firstSlowTrace — первый по времени трейс с названным корнем, чей корень
// длился дольше порога. Разбор начинается с примера из хвоста распределения:
// именно про хвост спрашивает метрика, и любой его представитель показывает
// одно и то же.
func firstSlowTrace(traces []traceTree, root string, min time.Duration) (traceTree, bool) {
	for _, t := range traces {
		if len(t.Spans) == 0 || t.Spans[0].Name != root {
			continue
		}
		if t.Spans[0].Duration >= min {
			return t, true
		}
	}
	return traceTree{}, false
}

// leafMax — самый долгий отрезок, внутри которого нет других. Время родителя
// складывается из времени детей плюс своего, и «где ушло» — вопрос именно
// к листу дерева.
func leafMax(t traceTree) traceSpan {
	kids := map[string]bool{}
	for _, s := range t.Spans {
		if s.Parent != "" {
			kids[s.Parent] = true
		}
	}
	var best traceSpan
	for _, s := range t.Spans {
		if kids[s.SpanID] {
			continue
		}
		if s.Duration > best.Duration {
			best = s
		}
	}
	return best
}

// percentile — значение, ниже которого лежит заданная доля наблюдений. Считаем
// по порядковой статистике, без сглаживания: на шестидесяти запросах любое
// сглаживание врало бы больше, чем округление.
func percentile(d []time.Duration, q float64) time.Duration {
	if len(d) == 0 {
		return 0
	}
	sorted := append([]time.Duration(nil), d...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	idx := int(q*float64(len(sorted)) + 0.9999) // потолок
	if idx < 1 {
		idx = 1
	}
	if idx > len(sorted) {
		idx = len(sorted)
	}
	return sorted[idx-1]
}

// human — длительность так, как её показывают инструменты: миллисекунды до
// секунды, дальше секунды с одним знаком.
func human(d time.Duration) string {
	if d < time.Second {
		return fmt.Sprintf("%d мс", d.Milliseconds())
	}
	return fmt.Sprintf("%.2f с", d.Seconds())
}
