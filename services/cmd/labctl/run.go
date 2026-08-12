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

	MonolithURL  string
	AcquirerURL  string
	NotifierURL  string
	ToxiproxyURL string
	OrdersURL    string
	CourierURL   string
	RelayURL     string
	// Профиль scale: второй инстанс той же службы и две точки входа
	// балансировщика — по запросам и по соединениям.
	Orders2URL  string
	BalancerL7  string
	BalancerL4  string
	DebeziumURL string
	AMQPURL     string
	Brokers     []string
	// Профиль cluster: обе копии базы напрямую и управление узлами. Сцены m08
	// спрашивают не приложение, а сами узлы — «что у тебя лежит» и «жив ли ты».
	PrimaryDSN string
	ReplicaDSN string
	// Сцена 40: место, куда переезжают профили клиентов. Базы у нового места
	// заранее нет — её заводит оператор переезда, как и в жизни, поэтому
	// сцене нужен и адрес самой базы, и адрес соседней, через которую её
	// создают: к несуществующей базе не подключиться.
	OldPlaceDSN string
	NewPlaceDSN string
	RedisAddr   string
	// Профиль trace: приёмник трейсов. Сцена достаёт из него дерево тем же
	// запросом, каким его достаёт интерфейс.
	JaegerURL string
	// Профиль trust: периметр. Сцена ходит и через него, и к сервису напрямую —
	// разница между этими путями и есть предмет сцен m13.
	GatewayURL string
	Docker     *lab.Docker

	// С кого собираются наблюдения. До m06 сцену наблюдал один монолит; с
	// появлением второго и третьего сервиса источников становится несколько,
	// и сцена называет их сама.
	Sources []string

	ctl       *http.Client
	own       lab.Recorder
	collected []lab.Event
	fields    map[string]string
	// Замеры табличных сцен: строка таблицы → поле → наблюдение.
	measured map[string]map[string]float64
}

func newRun(scene Scene, script Script) *Run {
	r := &Run{
		Scene:        scene,
		Script:       script,
		MonolithURL:  lab.Env("LAB_MONOLITH_URL", "http://monolith:8080"),
		AcquirerURL:  lab.Env("LAB_ACQUIRER_URL", "http://acquirer:8090"),
		NotifierURL:  lab.Env("LAB_NOTIFIER_URL", "http://notifier:8070"),
		ToxiproxyURL: lab.Env("LAB_TOXIPROXY_URL", "http://toxiproxy:8474"),
		OrdersURL:    lab.Env("LAB_ORDERS_URL", "http://orders:8050"),
		CourierURL:   lab.Env("LAB_COURIER_URL", "http://courier:8060"),
		Orders2URL:   lab.Env("LAB_ORDERS2_URL", "http://orders-2:8050"),
		BalancerL7:   lab.Env("LAB_BALANCER_L7", "http://balancer:8080"),
		BalancerL4:   lab.Env("LAB_BALANCER_L4", "http://balancer:8081"),
		RelayURL:     lab.Env("LAB_RELAY_URL", "http://outbox-relay:8040"),
		DebeziumURL:  lab.Env("LAB_DEBEZIUM_URL", "http://debezium:8083"),
		AMQPURL:      lab.Env("LAB_AMQP_URL", "amqp://lab:lab@rabbitmq:5672/"),
		Brokers:      strings.Split(lab.Env("LAB_KAFKA_BROKERS", "kafka:9092"), ","),
		PrimaryDSN:   lab.Env("LAB_PRIMARY_DSN", "postgres://delivery:delivery@postgres:5432/orders?sslmode=disable"),
		ReplicaDSN:   lab.Env("LAB_REPLICA_DSN", "postgres://delivery:delivery@postgres-replica:5432/orders?sslmode=disable"),
		OldPlaceDSN:  lab.Env("LAB_OLDPLACE_DSN", "postgres://delivery:delivery@postgres:5432/delivery?sslmode=disable"),
		NewPlaceDSN:  lab.Env("LAB_NEWPLACE_DSN", "postgres://delivery:delivery@postgres:5432/clients?sslmode=disable"),
		RedisAddr:    lab.Env("LAB_REDIS_ADDR", "redis:6379"),
		JaegerURL:    lab.Env("LAB_JAEGER_URL", "http://jaeger:16686"),
		GatewayURL:   lab.Env("LAB_GATEWAY_URL", "http://gateway:8000"),
		Docker:       lab.NewDocker(lab.Env("LAB_COMPOSE_PROJECT", "backend-architecture-lab")),
		// Управляющий клиент — не участник сцены: его таймаут щедрый,
		// иначе он сам стал бы источником отказа.
		ctl:      &http.Client{Timeout: 60 * time.Second},
		fields:   map[string]string{},
		measured: map[string]map[string]float64{},
	}
	r.Sources = []string{r.MonolithURL}
	return r
}

func (r *Run) Record(key string, fields map[string]string) { r.own.Record(key, fields) }

func (r *Run) Set(key, value string) { r.fields[key] = value }

// Measure кладёт наблюдение под ключ строки таблицы. Печатается при этом
// сценарное значение — наблюдение только сверяется с ним.
func (r *Run) Measure(key, field string, v float64) {
	if r.measured[key] == nil {
		r.measured[key] = map[string]float64{}
	}
	r.measured[key][field] = v
}

func bytesReader(b []byte) *bytes.Reader { return bytes.NewReader(b) }

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
	var all []lab.Event
	for _, src := range r.Sources {
		var body struct {
			Events []lab.Event `json:"events"`
		}
		if err := r.getJSON(src+"/_lab/events", &body); err != nil {
			return nil, err
		}
		all = append(all, body.Events...)
	}
	return all, nil
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
	if r.Script.Render == "table" {
		return r.reportTable(explain)
	}
	return r.reportTimeline(explain)
}

// head — общая шапка любой сцены: чья она, что показывает и можно ли сверять
// числа буква в букву.
func (r *Run) head(b *strings.Builder) {
	determinism := "детерминирована"
	if !r.Scene.Deterministic {
		determinism = "НЕдетерминирована — сверяй класс исхода, а не числа"
	}
	fmt.Fprintf(b, "Сцена %s · %s · профиль %s · %s\n",
		r.Scene.ID, r.Scene.Lesson, r.Scene.Profile, determinism)
	fmt.Fprintf(b, "%s\n", r.Scene.Title)
	for _, h := range r.Script.Header {
		fmt.Fprintf(b, "%s\n", substitute(h, r.fields, r.Script.Fields))
	}
}

func (r *Run) tail(b *strings.Builder, explain bool, chrono []string) {
	if len(r.Script.Legend) > 0 {
		b.WriteString("\n")
		for _, l := range r.Script.Legend {
			b.WriteString(substitute(l, r.fields, r.Script.Fields) + "\n")
		}
	}
	if !explain {
		return
	}
	b.WriteString("\nЧТО СМОТРЕТЬ\n")
	for _, e := range r.Scene.Explain {
		b.WriteString("  · " + substitute(e, r.fields, r.Script.Fields) + "\n")
	}
	b.WriteString("\nСВЕРКА наблюдения со сценарием (в текст шага не копируется)\n")
	for _, c := range chrono {
		b.WriteString(c + "\n")
	}
}

func (r *Run) reportTable(explain bool) (string, []string) {
	tables := r.Script.Tables
	if len(tables) == 0 {
		tables = []Table{r.Script.Table}
	}

	var problems, chrono []string
	var b strings.Builder
	r.head(&b)
	for _, t := range tables {
		p, c := verifyTable(t, r.measured)
		problems = append(problems, p...)
		chrono = append(chrono, c...)

		b.WriteString("\n")
		if t.Title != "" {
			b.WriteString(substitute(t.Title, r.fields, r.Script.Fields) + "\n\n")
		}
		for _, line := range renderTable(t, r.fields, r.Script.Fields) {
			b.WriteString(line + "\n")
		}
	}
	if s := substitute(r.Script.Summary, r.fields, r.Script.Fields); s != "" {
		b.WriteString(s + "\n")
	}
	r.tail(&b, explain, chrono)
	return b.String(), problems
}

func (r *Run) reportTimeline(explain bool) (string, []string) {
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
			At: l.At,
			// Имена сторон тоже подставляются: в сценах кластера действующее
			// лицо — конкретный узел, и какой именно, известно только в
			// прогоне (лидера выбирает кластер, а не сценарий).
			From:   substitute(l.From, ev.Fields, r.fields, r.Script.Fields),
			Arrow:  l.Arrow,
			To:     substitute(l.To, ev.Fields, r.fields, r.Script.Fields),
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
	for _, k := range r.Script.Ignore {
		inScript[k] = true
	}
	for _, e := range r.collected {
		if !inScript[e.Key] {
			problems = append(problems, "вне сценария наблюдалось событие "+e.Key)
		}
	}

	var b strings.Builder
	r.head(&b)
	b.WriteString("\n")
	for _, line := range renderTimeline(rows) {
		b.WriteString(line + "\n")
	}
	b.WriteString(substitute(r.Script.Summary, r.fields, r.Script.Fields) + "\n")
	r.tail(&b, explain, chrono)

	return b.String(), problems
}
