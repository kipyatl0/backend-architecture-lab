package lab

import (
	"context"
	"sort"
	"sync"
	"time"
)

// Gate — ограничение параллелизма с очередью перед ним. Ровно та конструкция,
// о которой курс говорит начиная с закона Литтла: одновременно работают
// `workers` запросов, остальные ждут, и ожидание — это и есть очередь.
//
// Очередь бывает с границей и без: `queue` = 0 означает «без границы», и это
// не абстракция, а самый частый способ превратить перегрузку в отказ.
type Gate struct {
	mu      sync.Mutex
	workers int
	queue   int

	sem     chan struct{}
	waiting int
	busy    int

	maxWaiting int
	maxBusy    int
}

func NewGate(workers, queue int) *Gate {
	g := &Gate{}
	g.Configure(workers, queue)
	return g
}

// Configure перенастраивает пул между сценами. Переконфигурация допустима
// только на покое: сцена всегда настраивает стенд до подачи нагрузки.
func (g *Gate) Configure(workers, queue int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.workers, g.queue = workers, queue
	if workers > 0 {
		g.sem = make(chan struct{}, workers)
	} else {
		g.sem = nil
	}
	g.waiting, g.busy, g.maxWaiting, g.maxBusy = 0, 0, 0, 0
}

// Acquire занимает обработчика. Второе значение — false, если запрос отвергнут
// на границе очереди: это не отказ системы, а её сознательное поведение за
// потолком ёмкости.
func (g *Gate) Acquire(ctx context.Context) (func(), bool) {
	g.mu.Lock()
	sem := g.sem
	if sem == nil { // без ограничения параллелизма — очереди тоже нет
		g.busy++
		if g.busy > g.maxBusy {
			g.maxBusy = g.busy
		}
		g.mu.Unlock()
		return func() {
			g.mu.Lock()
			g.busy--
			g.mu.Unlock()
		}, true
	}
	if g.queue > 0 && g.waiting >= g.queue {
		g.mu.Unlock()
		return nil, false
	}
	g.waiting++
	if g.waiting > g.maxWaiting {
		g.maxWaiting = g.waiting
	}
	g.mu.Unlock()

	select {
	case sem <- struct{}{}:
	case <-ctx.Done():
		g.mu.Lock()
		g.waiting--
		g.mu.Unlock()
		return nil, false
	}

	g.mu.Lock()
	g.waiting--
	g.busy++
	if g.busy > g.maxBusy {
		g.maxBusy = g.busy
	}
	g.mu.Unlock()

	return func() {
		g.mu.Lock()
		g.busy--
		g.mu.Unlock()
		<-sem
	}, true
}

type GateState struct {
	Workers    int `json:"workers"`
	Queue      int `json:"queue"`
	Busy       int `json:"busy"`
	Waiting    int `json:"waiting"`
	MaxBusy    int `json:"max_busy"`
	MaxWaiting int `json:"max_waiting"`
}

func (g *Gate) State() GateState {
	g.mu.Lock()
	defer g.mu.Unlock()
	return GateState{
		Workers: g.workers, Queue: g.queue,
		Busy: g.busy, Waiting: g.waiting,
		MaxBusy: g.maxBusy, MaxWaiting: g.maxWaiting,
	}
}

func (g *Gate) ResetPeaks() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.maxWaiting, g.maxBusy = g.waiting, g.busy
}

// ── замеры ──────────────────────────────────────────────────────────────────

// Latency — журнал времён ответа. Перцентили считаются по всем замерам, а не
// по скользящему окну: сцена короткая, а точность здесь важнее памяти.
type Latency struct {
	mu       sync.Mutex
	samples  []time.Duration
	ok       int64
	rejected int64
	failed   int64
}

func (l *Latency) Observe(d time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.samples = append(l.samples, d)
	l.ok++
}

func (l *Latency) Rejected() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.rejected++
}

func (l *Latency) Failed() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.failed++
}

func (l *Latency) Reset() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.samples, l.ok, l.rejected, l.failed = nil, 0, 0, 0
}

type LatencyState struct {
	Count    int64   `json:"count"`
	Rejected int64   `json:"rejected"`
	Failed   int64   `json:"failed"`
	P50MS    float64 `json:"p50_ms"`
	P95MS    float64 `json:"p95_ms"`
	P99MS    float64 `json:"p99_ms"`
	MaxMS    float64 `json:"max_ms"`
	AvgMS    float64 `json:"avg_ms"`
}

func (l *Latency) State() LatencyState {
	l.mu.Lock()
	s := make([]time.Duration, len(l.samples))
	copy(s, l.samples)
	st := LatencyState{Count: l.ok, Rejected: l.rejected, Failed: l.failed}
	l.mu.Unlock()

	if len(s) == 0 {
		return st
	}
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	var total time.Duration
	for _, d := range s {
		total += d
	}
	st.AvgMS = ms(total / time.Duration(len(s)))
	st.P50MS = ms(quantile(s, 0.50))
	st.P95MS = ms(quantile(s, 0.95))
	st.P99MS = ms(quantile(s, 0.99))
	st.MaxMS = ms(s[len(s)-1])
	return st
}

// Buckets — распределение времён ответа по границам (в миллисекундах).
// Нужно там, где курс показывает не число, а форму: правый край гистограммы.
func (l *Latency) Buckets(bounds []float64) []int64 {
	l.mu.Lock()
	s := make([]time.Duration, len(l.samples))
	copy(s, l.samples)
	l.mu.Unlock()

	out := make([]int64, len(bounds)+1)
	for _, d := range s {
		v := ms(d)
		i := sort.SearchFloat64s(bounds, v)
		out[i]++
	}
	return out
}

func quantile(sorted []time.Duration, q float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	i := int(q * float64(len(sorted)))
	if i >= len(sorted) {
		i = len(sorted) - 1
	}
	return sorted[i]
}

func ms(d time.Duration) float64 {
	return float64(d.Microseconds()) / 1000.0
}
