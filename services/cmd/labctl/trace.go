package main

// Чтение собранных трейсов (профиль trace, m11). Сцена спрашивает приёмник тем
// же запросом, каким его спрашивает интерфейс на 16686, и печатает то же самое
// дерево — чтобы «посмотри глазами» и «вот вывод сцены» не расходились.

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"
)

// traceSpan — отрезок работы: кто, что, сколько и чьим продолжением.
type traceSpan struct {
	TraceID  string
	SpanID   string
	Parent   string
	Name     string
	Service  string
	Start    time.Time
	Duration time.Duration
	Depth    int
}

// traceTree — трейс целиком, спаны уже разложены обходом в глубину: так же,
// как их показывает интерфейс, и так же, как их печатает сцена.
type traceTree struct {
	ID    string
	Start time.Time
	Spans []traceSpan
}

func (t traceTree) Root() traceSpan {
	if len(t.Spans) == 0 {
		return traceSpan{}
	}
	return t.Spans[0]
}

// Services — какие сервисы участвовали, в порядке первого появления.
func (t traceTree) Services() []string {
	var out []string
	seen := map[string]bool{}
	for _, s := range t.Spans {
		if !seen[s.Service] {
			seen[s.Service] = true
			out = append(out, s.Service)
		}
	}
	return out
}

// ── запрос к приёмнику ──────────────────────────────────────────────────────

// jaegerTrace — ответ приёмника. Разбираем только то, что печатаем: имя, кто
// наблюдал, начало, длительность и ссылка на родителя.
type jaegerTrace struct {
	TraceID string `json:"traceID"`
	Spans   []struct {
		TraceID       string `json:"traceID"`
		SpanID        string `json:"spanID"`
		OperationName string `json:"operationName"`
		References    []struct {
			RefType string `json:"refType"`
			SpanID  string `json:"spanID"`
		} `json:"references"`
		StartTime int64  `json:"startTime"` // микросекунды с эпохи
		Duration  int64  `json:"duration"`  // микросекунды
		ProcessID string `json:"processID"`
	} `json:"spans"`
	Processes map[string]struct {
		ServiceName string `json:"serviceName"`
	} `json:"processes"`
}

// fetchTraces забирает трейсы, начавшиеся не раньше since, у каждого из
// названных сервисов. Один трейс может прийти несколько раз — по разу от
// каждого своего участника; поэтому собираем по идентификатору.
func (r *Run) fetchTraces(services []string, since time.Time) ([]traceTree, error) {
	byID := map[string]traceTree{}
	for _, svc := range services {
		var body struct {
			Data []jaegerTrace `json:"data"`
		}
		url := fmt.Sprintf("%s/api/traces?service=%s&lookback=1h&limit=1000", r.JaegerURL, svc)
		if err := r.getJSON(url, &body); err != nil {
			return nil, fmt.Errorf("приёмник трейсов не ответил (поднят ли профиль trace?): %w", err)
		}
		for _, t := range body.Data {
			tree, ok := buildTree(t, since)
			if !ok {
				continue
			}
			byID[tree.ID] = tree
		}
	}

	out := make([]traceTree, 0, len(byID))
	for _, t := range byID {
		out = append(out, t)
	}
	// Порядок трейсов — по времени начала: он и есть порядок, в котором работа
	// случилась, и в нём же её читает студент.
	sort.SliceStable(out, func(i, j int) bool { return out[i].Start.Before(out[j].Start) })
	return out, nil
}

// buildTree раскладывает плоский список спанов деревом. Трейсы старше since
// отбрасываются целиком: приёмник помнит и прошлые прогоны, а сцена печатает
// только свой.
func buildTree(t jaegerTrace, since time.Time) (traceTree, bool) {
	type node struct {
		span traceSpan
		kids []string
	}
	nodes := map[string]*node{}
	var order []string
	start := time.Time{}

	for _, s := range t.Spans {
		at := time.UnixMicro(s.StartTime)
		if at.Before(since) {
			return traceTree{}, false
		}
		if start.IsZero() || at.Before(start) {
			start = at
		}
		parent := ""
		for _, ref := range s.References {
			if ref.RefType == "CHILD_OF" {
				parent = ref.SpanID
			}
		}
		nodes[s.SpanID] = &node{span: traceSpan{
			TraceID:  t.TraceID,
			SpanID:   s.SpanID,
			Parent:   parent,
			Name:     s.OperationName,
			Service:  t.Processes[s.ProcessID].ServiceName,
			Start:    at,
			Duration: time.Duration(s.Duration) * time.Microsecond,
		}}
		order = append(order, s.SpanID)
	}
	if len(nodes) == 0 {
		return traceTree{}, false
	}

	var roots []string
	for _, id := range order {
		n := nodes[id]
		if p, ok := nodes[n.span.Parent]; ok && n.span.Parent != "" {
			p.kids = append(p.kids, id)
			continue
		}
		roots = append(roots, id)
	}

	byStart := func(ids []string) {
		sort.SliceStable(ids, func(i, j int) bool {
			return nodes[ids[i]].span.Start.Before(nodes[ids[j]].span.Start)
		})
	}
	byStart(roots)

	tree := traceTree{ID: t.TraceID, Start: start}
	var walk func(id string, depth int)
	walk = func(id string, depth int) {
		n := nodes[id]
		n.span.Depth = depth
		tree.Spans = append(tree.Spans, n.span)
		byStart(n.kids)
		for _, k := range n.kids {
			walk(k, depth+1)
		}
	}
	for _, id := range roots {
		walk(id, 0)
	}
	return tree, true
}

// waitForSpans ждёт, пока приёмник соберёт ожидаемое число спанов. Спаны
// уходят пачками и приезжают не мгновенно: спросить сразу после прогона
// значило бы увидеть половину дерева.
func (r *Run) waitForSpans(ctx context.Context, services []string, since time.Time,
	want int, limit time.Duration) ([]traceTree, error) {

	deadline := time.Now().Add(limit)
	var last []traceTree
	for {
		traces, err := r.fetchTraces(services, since)
		if err == nil {
			last = traces
			total := 0
			for _, t := range traces {
				total += len(t.Spans)
			}
			if total >= want {
				// Ещё полсекунды на опоздавших: приехавшее «ровно столько»
				// иногда оказывается «столько же, но не тех».
				time.Sleep(500 * time.Millisecond)
				return r.fetchTraces(services, since)
			}
		}
		if time.Now().After(deadline) {
			return last, nil
		}
		select {
		case <-ctx.Done():
			return last, ctx.Err()
		case <-time.After(300 * time.Millisecond):
		}
	}
}

// indent — отступ спана в печатаемом дереве. Два пробела на уровень: дерево
// читается вложенностью, а не рамками.
func indent(depth int, name string) string {
	return strings.Repeat("  ", depth) + name
}
