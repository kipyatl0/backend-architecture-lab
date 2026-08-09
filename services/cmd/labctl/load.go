package main

import (
	"context"
	"errors"
	"io"
	"math/rand/v2"
	"net/http"
	"sort"
	"sync"
	"syscall"
	"time"
)

// Нагрузка подаётся отсюда: отдельного генератора-контейнера в стенде нет.
// Одна сцена — одна команда, и лишний сервис в кадре студенту ничего не
// объясняет.
//
// Модель подачи — открытая: заявки приходят по расписанию и не ждут, пока
// освободится предыдущая. Это принципиально. В закрытой модели (N клиентов
// по кругу) очередь не растёт никогда, и закон Литтла показать нечем.

type loadStats struct {
	Sent     int
	OK       int
	Rejected int // служба честно отказала: 503
	Failed   int // соединение оборвалось на полуслове или клиент не дождался
	Refused  int // службы на том конце уже нет: соединение не приняли вовсе
	Lat      []time.Duration
}

// Разница между Failed и Refused — это разница между «работу могли начать и
// потеряли» и «работу не приняли». Для клиента это два разных мира, и сцена
// про остановку сервиса держится ровно на ней.

func (s loadStats) p(q float64) float64 {
	if len(s.Lat) == 0 {
		return 0
	}
	c := make([]time.Duration, len(s.Lat))
	copy(c, s.Lat)
	sort.Slice(c, func(i, j int) bool { return c[i] < c[j] })
	i := int(q * float64(len(c)))
	if i >= len(c) {
		i = len(c) - 1
	}
	return float64(c[i].Microseconds()) / 1000
}

func (s loadStats) maxMS() float64 {
	var m time.Duration
	for _, d := range s.Lat {
		if d > m {
			m = d
		}
	}
	return float64(m.Microseconds()) / 1000
}

// Зерно генератора потока. Промежутки между заявками случайны по
// распределению, но последовательность у всех одна и та же: два прогона на
// одной машине дают почти одинаковые числа, и вывод можно сверять с текстом.
const arrivalSeed = 20260809

// load подаёт на url поток в rps запросов в течение dur и возвращает то, что
// увидел клиент. Клиентский таймаут — часть опыта: без него «медленно» и
// «не ответил» неразличимы.
//
// Промежутки между заявками — экспоненциальные, а не равные. Это не украшение:
// при идеально равномерном потоке и постоянном времени обработки очередь не
// возникает вообще до самого потолка, и излом на утилизации показать нечем.
// Очередь на 80% рождается из разброса, а клиенты между собой не сговариваются.
func (r *Run) load(ctx context.Context, method, url string, body []byte, rps int, dur, timeout time.Duration) loadStats {
	client := &http.Client{Timeout: timeout, Transport: loadTransport()}
	src := rand.New(rand.NewPCG(arrivalSeed, arrivalSeed+1))
	mean := float64(time.Second) / float64(rps)

	var (
		mu    sync.Mutex
		stats loadStats
		wg    sync.WaitGroup
	)

	// Расписание считается от начала ступени, а не «сколько-то после
	// предыдущей заявки»: иначе накладные расходы цикла копятся, и заданные
	// 60 rps превращаются в 54.
	begin := time.Now()
	next := begin

loop:
	for {
		next = next.Add(time.Duration(src.ExpFloat64() * mean))
		if next.Sub(begin) > dur {
			break
		}
		if d := time.Until(next); d > 0 {
			select {
			case <-ctx.Done():
				break loop
			case <-time.After(d):
			}
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			start := time.Now()
			code, err := doOnce(client, method, url, body)
			took := time.Since(start)

			mu.Lock()
			defer mu.Unlock()
			stats.Sent++
			switch {
			case err != nil && errors.Is(err, syscall.ECONNREFUSED):
				stats.Refused++
			case err != nil:
				stats.Failed++
			case code == http.StatusServiceUnavailable:
				stats.Rejected++
				stats.Lat = append(stats.Lat, took)
			case code >= 200 && code < 400:
				stats.OK++
				stats.Lat = append(stats.Lat, took)
			default:
				stats.Failed++
			}
		}()
	}

	// Ждём тех, кто ещё в полёте: их судьба и есть половина ответа на вопрос
	// «что происходит за потолком ёмкости».
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(timeout + 5*time.Second):
	}

	mu.Lock()
	defer mu.Unlock()
	out := stats
	out.Lat = append([]time.Duration(nil), stats.Lat...)
	return out
}

// Соединения переиспользуются: иначе установка нового соединения под нагрузкой
// сама даёт десятки миллисекунд и подмешивается в хвост, который сцена
// показывает. Предмет сцены — служба, а не разогрев пула клиента.
func loadTransport() *http.Transport {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.MaxIdleConns = 1024
	t.MaxIdleConnsPerHost = 1024
	t.MaxConnsPerHost = 0
	t.IdleConnTimeout = 90 * time.Second
	return t
}

func doOnce(c *http.Client, method, url string, body []byte) (int, error) {
	var rdr io.Reader
	if body != nil {
		rdr = bytesReader(body)
	}
	req, err := http.NewRequest(method, url, rdr)
	if err != nil {
		return 0, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode, nil
}

// ── метрики службы ──────────────────────────────────────────────────────────

type serverMetrics struct {
	Latency struct {
		Count    int64   `json:"count"`
		Rejected int64   `json:"rejected"`
		Failed   int64   `json:"failed"`
		P50MS    float64 `json:"p50_ms"`
		P95MS    float64 `json:"p95_ms"`
		P99MS    float64 `json:"p99_ms"`
		MaxMS    float64 `json:"max_ms"`
		AvgMS    float64 `json:"avg_ms"`
	} `json:"latency"`
	Gate struct {
		Workers    int `json:"workers"`
		Queue      int `json:"queue"`
		Busy       int `json:"busy"`
		Waiting    int `json:"waiting"`
		MaxBusy    int `json:"max_busy"`
		MaxWaiting int `json:"max_waiting"`
	} `json:"gate"`
	NotifyCalls  int64   `json:"notify_calls"`
	BreakerShort int64   `json:"breaker_short"`
	BreakerOpen  bool    `json:"breaker_open"`
	HeapMB       float64 `json:"heap_mb"`
	Goroutines   int     `json:"goroutines"`
}

func (r *Run) metrics(reset bool) (serverMetrics, error) {
	var m serverMetrics
	url := r.MonolithURL + "/_lab/metrics"
	if reset {
		url += "?reset=1"
	}
	err := r.getJSON(url, &m)
	return m, err
}

// configure — единственный способ настроить службу под сцену.
func (r *Run) configure(body map[string]any) error {
	return r.postJSON(r.MonolithURL+"/_lab/config", body, nil)
}

func (r *Run) configureNotifier(body map[string]any) error {
	return r.postJSON(r.NotifierURL+"/_lab/config", body, nil)
}
