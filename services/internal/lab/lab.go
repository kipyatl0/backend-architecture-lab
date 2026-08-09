// Package lab — общее для всех сервисов стенда: запись наблюдаемых событий,
// структурный лог, HTTP-обвязка и корректная остановка по сигналу.
package lab

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// Event — факт, который сервис наблюдал у себя. Текст строки timeline берётся
// НЕ отсюда, а из сценария: сервис сообщает, что произошло и когда, сценарий
// решает, как это назвать. Fields подставляются в шаблон строки сценария.
type Event struct {
	Key    string            `json:"key"`
	At     time.Time         `json:"at"`
	Fields map[string]string `json:"fields,omitempty"`
}

// Recorder — журнал событий сцены. Живёт в памяти и обнуляется подготовкой
// сцены: стенд не копит наблюдения между прогонами.
type Recorder struct {
	mu     sync.Mutex
	events []Event
}

func (r *Recorder) Record(key string, fields map[string]string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, Event{Key: key, At: time.Now().UTC(), Fields: fields})
}

func (r *Recorder) Snapshot() []Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Event, len(r.events))
	copy(out, r.events)
	return out
}

func (r *Recorder) Clear() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = nil
}

// Handler отдаёт снимок журнала: GET /_lab/events.
func (r *Recorder) Handler() http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		WriteJSON(w, http.StatusOK, map[string]any{"events": r.Snapshot()})
	}
}

// ── HTTP ────────────────────────────────────────────────────────────────────

func WriteJSON(w http.ResponseWriter, code int, body any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(body)
}

func ReadJSON(r *http.Request, dst any) error {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	err := dec.Decode(dst)
	if errors.Is(err, io.EOF) { // пустое тело — законный «ничего не меняем»
		return nil
	}
	return err
}

// Serve поднимает HTTP-сервер и держит его до SIGTERM/SIGINT, после чего
// закрывается корректно: принятая работа доводится до конца.
// Требование стенда — все сервисы останавливаются по сигналу одинаково.
func Serve(addr string, mux http.Handler, log *slog.Logger, onShutdown func()) error {
	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		// Ответ сцены может готовиться десятки секунд — это и есть предмет
		// курса, поэтому запись ответа временем не ограничена.
		WriteTimeout: 0,
		IdleTimeout:  120 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	errc := make(chan error, 1)
	go func() {
		log.Info("сервис запущен", "addr", addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errc <- err
		}
	}()

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
		log.Info("получен сигнал остановки, завершаем принятую работу")
		shutCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		err := srv.Shutdown(shutCtx)
		if onShutdown != nil {
			onShutdown()
		}
		log.Info("сервис остановлен")
		return err
	}
}

// Logger — структурный лог в общем для стенда формате: JSON в stdout.
func Logger(service string) *slog.Logger {
	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})
	return slog.New(h).With("service", service)
}

func Env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func EnvDuration(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return def
	}
	return d
}

// Health вешает раздельные /health и /ready: живость процесса и готовность
// обслуживать запросы — разные вопросы, и курс на этом настаивает.
func Health(mux *http.ServeMux, ready func() error) {
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		WriteJSON(w, http.StatusOK, map[string]string{"status": "alive"})
	})
	mux.HandleFunc("GET /ready", func(w http.ResponseWriter, r *http.Request) {
		if err := ready(); err != nil {
			WriteJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "not ready", "reason": err.Error()})
			return
		}
		WriteJSON(w, http.StatusOK, map[string]string{"status": "ready"})
	})
}
