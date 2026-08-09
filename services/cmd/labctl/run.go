package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/kipyatl0/backend-architecture-lab/services/internal/lab"
)

// Run — один прогон сцены. Держит t0, собственные наблюдения (сторона
// клиента) и то, что сообщили сервисы, а в конце сверяет наблюдение со
// сценарием и печатает timeline.
type Run struct {
	Scene  Scene
	Script Script
	T0     time.Time

	MonolithURL string
	AcquirerURL string

	ctl       *http.Client
	own       lab.Recorder
	collected []lab.Event
	fields    map[string]string
}

func newRun(scene Scene, script Script) *Run {
	return &Run{
		Scene:       scene,
		Script:      script,
		MonolithURL: lab.Env("LAB_MONOLITH_URL", "http://monolith:8080"),
		AcquirerURL: lab.Env("LAB_ACQUIRER_URL", "http://acquirer:8090"),
		// Управляющий клиент — не участник сцены: его таймаут щедрый,
		// иначе он сам стал бы источником отказа.
		ctl:    &http.Client{Timeout: 30 * time.Second},
		fields: map[string]string{},
	}
}

func (r *Run) Record(key string, fields map[string]string) { r.own.Record(key, fields) }

func (r *Run) Set(key, value string) { r.fields[key] = value }

// SleepUntil ждёт до заданного смещения от начала сцены. Именно так сцена
// удерживает расписание: повтор клиента случается не «когда получится», а
// в свою секунду.
func (r *Run) SleepUntil(offset float64) {
	target := r.T0.Add(time.Duration(offset * float64(time.Second)))
	if d := time.Until(target); d > 0 {
		time.Sleep(d)
	}
}

func (r *Run) postJSON(url string, body, out any) error {
	payload, err := json.Marshal(body)
	if err != nil {
		return err
	}
	resp, err := r.ctl.Post(url, "application/json", bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("%s: %w", url, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return fmt.Errorf("%s ответил %d: %s", url, resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	if out != nil {
		return json.Unmarshal(raw, out)
	}
	return nil
}

func (r *Run) getJSON(url string, out any) error {
	resp, err := r.ctl.Get(url)
	if err != nil {
		return fmt.Errorf("%s: %w", url, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return fmt.Errorf("%s ответил %d", url, resp.StatusCode)
	}
	if out != nil {
		return json.Unmarshal(raw, out)
	}
	return nil
}

func (r *Run) serviceEvents() ([]lab.Event, error) {
	var body struct {
		Events []lab.Event `json:"events"`
	}
	if err := r.getJSON(r.MonolithURL+"/_lab/events", &body); err != nil {
		return nil, err
	}
	return body.Events, nil
}

func (r *Run) collect() error {
	svc, err := r.serviceEvents()
	if err != nil {
		return err
	}
	r.collected = append(r.own.Snapshot(), svc...)
	return nil
}

// waitForScript ждёт, пока серверная сторона доиграет свою часть. Клиент к
// этому моменту уже ушёл — но работа-то продолжается, и именно её надо
// дождаться, чтобы увидеть цену.
func (r *Run) waitForScript(ctx context.Context) error {
	last := 0.0
	for _, l := range r.Script.Timeline {
		if l.At > last {
			last = l.At
		}
	}
	deadline := r.T0.Add(time.Duration((last + 25) * float64(time.Second)))

	for {
		if err := r.collect(); err != nil {
			return err
		}
		have := map[string]bool{}
		for _, e := range r.collected {
			have[e.Key] = true
		}
		missing := false
		for _, l := range r.Script.Timeline {
			if !have[l.Key] {
				missing = true
				break
			}
		}
		if !missing {
			return nil
		}
		if time.Now().After(deadline) {
			return nil // расхождение поймает сверка, а вывод всё равно напечатаем
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// report собирает вывод сцены и список расхождений со сценарием.
func (r *Run) report(explain bool) (string, []string) {
	events := map[string]lab.Event{}
	for _, e := range r.collected {
		if _, dup := events[e.Key]; !dup {
			events[e.Key] = e
		}
	}

	var problems []string
	var rows []row
	var chrono []string
	inScript := map[string]bool{}

	for _, l := range r.Script.Timeline {
		inScript[l.Key] = true
		ev, ok := events[l.Key]
		rows = append(rows, row{
			At:     l.At,
			From:   l.From,
			Arrow:  l.Arrow,
			To:     l.To,
			Detail: substitute(l.Detail, ev.Fields, r.fields, r.Script.Fields),
			Note:   substitute(l.Note, ev.Fields, r.fields, r.Script.Fields),
		})
		if !ok {
			problems = append(problems, fmt.Sprintf("событие %s не наблюдалось", l.Key))
			chrono = append(chrono, fmt.Sprintf("  %sсценарий %7.3f   наблюдение       —", pad(l.Key, 26), l.At))
			continue
		}
		obs := ev.At.Sub(r.T0).Seconds()
		delta := obs - l.At
		if delta < -l.Tol || delta > l.Tol {
			problems = append(problems, fmt.Sprintf(
				"%s: сценарий %.3f, наблюдение %.3f (допуск ±%.1f с)", l.Key, l.At, obs, l.Tol))
		}
		chrono = append(chrono, fmt.Sprintf("  %sсценарий %7.3f   наблюдение %7.3f   Δ %+.3f",
			pad(l.Key, 26), l.At, obs, delta))
	}
	for _, e := range r.collected {
		if !inScript[e.Key] {
			problems = append(problems, "вне сценария наблюдалось событие "+e.Key)
		}
	}

	var b strings.Builder
	determinism := "детерминирована"
	if !r.Scene.Deterministic {
		determinism = "НЕдетерминирована — сверяй класс исхода, а не числа"
	}
	fmt.Fprintf(&b, "Сцена %s · %s · профиль %s · %s\n",
		r.Scene.ID, r.Scene.Lesson, r.Scene.Profile, determinism)
	fmt.Fprintf(&b, "%s\n", r.Scene.Title)
	for _, h := range r.Script.Header {
		fmt.Fprintf(&b, "%s\n", substitute(h, r.fields, r.Script.Fields))
	}
	b.WriteString("\n")
	for _, line := range renderTimeline(rows) {
		b.WriteString(line + "\n")
	}
	b.WriteString(substitute(r.Script.Summary, r.fields, r.Script.Fields) + "\n")

	if explain {
		b.WriteString("\nЧТО СМОТРЕТЬ\n")
		for _, e := range r.Scene.Explain {
			b.WriteString("  · " + substitute(e, r.fields, r.Script.Fields) + "\n")
		}
		b.WriteString("\nХРОНОМЕТРАЖ — сверка наблюдения со сценарием (в текст шага не копируется)\n")
		for _, c := range chrono {
			b.WriteString(c + "\n")
		}
	}

	return b.String(), problems
}
