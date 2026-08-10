package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/kipyatl0/backend-architecture-lab/services/internal/lab"
)

// Профиль cdc. Отличие от broker ровно одно: рядом с базой встаёт процесс,
// который читает её журнал репликации. Приложение при этом не меняется —
// и в этом весь предмет: поток событий появляется без единой строчки кода
// отправки, а вместе с ним появляется и счёт за такой способ его получить.

const cdcConnector = "orders-cdc" // префикс имени: имя уникально в каждом прогоне

// cdcRec — запись журнала изменений, разобранная до того, что показывает сцена.
// Полей у неё больше: здесь названы те, ради которых сцена существует.
type cdcRec struct {
	Op     string // r — снимок, c — вставка, u — обновление, d — удаление
	ID     int64
	Before map[string]any
	After  map[string]any
}

func parseCDC(raw lab.Raw) (cdcRec, bool) {
	var v struct {
		Op     string         `json:"op"`
		Before map[string]any `json:"before"`
		After  map[string]any `json:"after"`
	}
	if err := json.Unmarshal(raw.Value, &v); err != nil || v.Op == "" {
		return cdcRec{}, false
	}
	var k struct {
		ID int64 `json:"id"`
	}
	_ = json.Unmarshal(raw.Key, &k)
	return cdcRec{Op: v.Op, ID: k.ID, Before: v.Before, After: v.After}, true
}

// ── управление коннектором ──────────────────────────────────────────────────

func (r *Run) cdcConnectors() ([]string, error) {
	var names []string
	if err := r.getJSON(r.DebeziumURL+"/connectors", &names); err != nil {
		return nil, err
	}
	return names, nil
}

// cdcCleanup сносит коннекторы прошлых прогонов. Слот репликации они держат до
// самой смерти, а слотов у базы конечное число: без уборки сцена ломается на
// пятом запуске, и ломается непонятно.
func (r *Run) cdcCleanup(ctx context.Context) error {
	names, err := r.cdcConnectors()
	if err != nil {
		return fmt.Errorf("служба захвата изменений недоступна — поднят ли профиль cdc? %w", err)
	}
	for _, n := range names {
		req, err := http.NewRequestWithContext(ctx, http.MethodDelete, r.DebeziumURL+"/connectors/"+n, nil)
		if err != nil {
			return err
		}
		resp, err := r.ctl.Do(req)
		if err != nil {
			return err
		}
		resp.Body.Close()
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		names, err = r.cdcConnectors()
		if err == nil && len(names) == 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("коннекторы прошлого прогона не удалились: %v", names)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(300 * time.Millisecond):
		}
	}
}

// cdcRegister заводит коннектор. Вся настройка — про то, ОТКУДА читать и КУДА
// класть: чему быть событием, здесь не решает никто. Отсюда и берётся главное
// свойство приёма, и главная его цена.
func (r *Run) cdcRegister(ctx context.Context, name, slot string) error {
	cfg := map[string]any{
		"connector.class":    "io.debezium.connector.postgresql.PostgresConnector",
		"tasks.max":          "1",
		"database.hostname":  lab.Env("LAB_CDC_DB_HOST", "postgres"),
		"database.port":      lab.Env("LAB_CDC_DB_PORT", "5432"),
		"database.user":      lab.Env("LAB_CDC_DB_USER", "delivery"),
		"database.password":  lab.Env("LAB_CDC_DB_PASSWORD", "delivery"),
		"database.dbname":    lab.Env("LAB_CDC_DB_NAME", "orders"),
		"topic.prefix":       lab.CDCPrefix,
		"table.include.list": "public.orders",
		// Логическое декодирование встроено в PostgreSQL с 10-й версии:
		// отдельное расширение в базу ставить не нужно.
		"plugin.name":                 "pgoutput",
		"slot.name":                   slot,
		"publication.name":            lab.CDCPublication,
		"publication.autocreate.mode": "filtered",
		"snapshot.mode":               "initial",
		// Слот держит журнал за собой, пока жив: сцена обязана отпускать его.
		"slot.drop.on.stop":    "true",
		"tombstones.on.delete": "false",
		"time.precision.mode":  "connect",
		// Схема в каждом сообщении утроила бы вывод и ничего бы не добавила:
		// предмет сцены — содержание записи, а не способ её описать.
		"key.converter":                  "org.apache.kafka.connect.json.JsonConverter",
		"key.converter.schemas.enable":   "false",
		"value.converter":                "org.apache.kafka.connect.json.JsonConverter",
		"value.converter.schemas.enable": "false",
		// Автосоздание тем в стенде выключено (число партиций — предмет m06),
		// поэтому тему для потока изменений заводит сам коннектор.
		"topic.creation.default.replication.factor": "1",
		"topic.creation.default.partitions":         "1",
	}
	return r.postJSON(r.DebeziumURL+"/connectors", map[string]any{"name": name, "config": cfg}, nil)
}

func (r *Run) cdcWaitRunning(ctx context.Context, name string, limit time.Duration) error {
	deadline := time.Now().Add(limit)
	for {
		var st struct {
			Connector struct {
				State string `json:"state"`
			} `json:"connector"`
			Tasks []struct {
				State string `json:"state"`
				Trace string `json:"trace"`
			} `json:"tasks"`
		}
		err := r.getJSON(r.DebeziumURL+"/connectors/"+name+"/status", &st)
		if err == nil {
			for _, t := range st.Tasks {
				if t.State == "FAILED" {
					return fmt.Errorf("задача коннектора упала: %s", firstLine(t.Trace))
				}
			}
			if st.Connector.State == "RUNNING" && len(st.Tasks) > 0 && st.Tasks[0].State == "RUNNING" {
				return nil
			}
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("коннектор не запустился за %s", limit)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(300 * time.Millisecond):
		}
	}
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i > 0 {
		return s[:i]
	}
	return s
}

// ── чтение потока изменений ─────────────────────────────────────────────────

func (r *Run) cdcRead(ctx context.Context) ([]cdcRec, error) {
	raws, err := lab.ReadRaw(ctx, r.Brokers, lab.TopicCDC, 1500*time.Millisecond)
	if err != nil {
		return nil, err
	}
	var out []cdcRec
	for _, raw := range raws {
		if rec, ok := parseCDC(raw); ok {
			out = append(out, rec)
		}
	}
	return out, nil
}

// cdcWait ждёт, пока в теме окажется ожидаемое число записей. Ждать по
// содержимому, а не по часам: задержка захвата зависит от машины, а число
// записей — нет.
func (r *Run) cdcWait(ctx context.Context, want int, limit time.Duration) ([]cdcRec, error) {
	deadline := time.Now().Add(limit)
	var last []cdcRec
	for {
		recs, err := r.cdcRead(ctx)
		if err == nil {
			last = recs
			if len(recs) >= want {
				return recs, nil
			}
		}
		if time.Now().After(deadline) {
			return last, fmt.Errorf("в теме %s оказалось %d записей вместо %d",
				lab.TopicCDC, len(last), want)
		}
		select {
		case <-ctx.Done():
			return last, ctx.Err()
		case <-time.After(300 * time.Millisecond):
		}
	}
}

// ── как записи печатаются ───────────────────────────────────────────────────
//
// Служебные поля (номер в журнале, номер транзакции, метки времени) меняются
// каждый прогон — печатать их значит сделать вывод несверяемым. Они сокращены
// до «…», и сцена говорит об этом прямо. Всё остальное — как лежит в теме.

var cdcCols = []string{"id", "client", "restaurant", "amount", "status", "charge", "created_at"}

func cdcValue(col string, v any) string {
	if v == nil {
		return "null"
	}
	switch t := v.(type) {
	case string:
		if col == "created_at" {
			// Настоящее время создания меняется каждый прогон — его сокращаем.
			// А начало эпохи — не время, а пустышка, которой журнал заполнил
			// поле, о котором ничего не знает: её показать обязательно.
			if !strings.HasPrefix(t, "1970-01-01") {
				return "…"
			}
		}
		return `"` + t + `"`
	case float64:
		if t == math.Trunc(t) {
			return strconv.FormatInt(int64(t), 10)
		}
		return strconv.FormatFloat(t, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(t)
	default:
		b, _ := json.Marshal(t)
		return string(b)
	}
}

// cdcRow печатает строку таблицы так, как её видит получатель потока: столбцы
// в порядке таблицы, перенос — после третьего, чтобы строка влезала в экран.
func cdcRow(m map[string]any, indent string) string {
	if m == nil {
		return "null"
	}
	var head, tail []string
	for i, c := range cdcCols {
		v, ok := m[c]
		if !ok {
			continue
		}
		part := `"` + c + `": ` + cdcValue(c, v)
		if i < 3 {
			head = append(head, part)
		} else {
			tail = append(tail, part)
		}
	}
	if len(tail) == 0 {
		return "{" + strings.Join(head, ", ") + "}"
	}
	return "{" + strings.Join(head, ", ") + ",\n" + indent + strings.Join(tail, ", ") + "}"
}

// cdcShort — одна строка на запись: что за операция, с какой строкой и что
// в ней видно. Именно этот список отвечает на вопрос «а что вообще пришло».
func cdcShort(n int, rec cdcRec) string {
	// Что показывать про строку: статус, если он в записи есть, и честное
	// «только ключ», если нет. Второе — не украшение вывода, а то, что
	// приходит при удалении.
	side := func(m map[string]any) string {
		if m == nil {
			return "—"
		}
		if s, ok := m["status"].(string); ok && s != "" {
			return "status=" + s
		}
		return "только ключ id=" + strconv.FormatInt(rec.ID, 10)
	}
	before, after := side(rec.Before), side(rec.After)
	return fmt.Sprintf("  #%d  op=%s  id=%d  before: %s  after: %s",
		n, rec.Op, rec.ID, pad(before, 30), after)
}

// cdcFull печатает запись целиком — так, как она лежит в теме.
func cdcFull(rec cdcRec) string {
	return fmt.Sprintf("  {\"op\": %q,\n   \"before\": %s,\n   \"after\": %s,\n"+
		"   \"source\": {\"db\": \"orders\", \"schema\": \"public\", \"table\": \"orders\", \"lsn\": …, \"txId\": …},\n"+
		"   \"ts_ms\": …}",
		rec.Op, cdcRow(rec.Before, "              "), cdcRow(rec.After, "             "))
}

// ── сцена 19 — CDC: журнал базы как источник событий (m07 l03) ──────────────
//
// Сервис заказов в этой сцене не публикует ничего: ни прямой отправки, ни
// исходящего ящика. Поток событий всё равно появляется — его читает из журнала
// репликации отдельный процесс, о котором приложение не знает. Вместе с
// потоком появляется и счёт: события приходят в терминах таблицы, намерения в
// них нет, а схема базы становится публичным контрактом.
func scene19(ctx context.Context, r *Run) error {
	cfg := r.Script.Config
	// Наблюдатель — сервис заказов: массовую отмену и удаление строки делает
	// он, и в timeline они стоят с его стороны.
	r.Sources = []string{r.OrdersURL}

	// ── исходное состояние: за журналом никто не следит ─────────────────────

	if err := r.cdcCleanup(ctx); err != nil {
		return err
	}
	if err := r.postJSON(r.OrdersURL+"/_lab/cdc-reset", map[string]any{}, nil); err != nil {
		return fmt.Errorf("журнал репликации не сброшен: %w", err)
	}
	if err := lab.WaitKafka(ctx, r.Brokers, 60*time.Second); err != nil {
		return err
	}
	if err := lab.DropTopic(ctx, r.Brokers, lab.TopicCDC); err != nil {
		return err
	}
	// Пустые очередь и лог — единственный честный способ показать, что
	// приложение не отправило ни одного события: числа в ИТОГЕ измерены.
	conn, err := lab.DialAMQPWait(ctx, r.AMQPURL, 30*time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()
	if err := conn.Declare(lab.Topology{}); err != nil {
		return err
	}
	if err := lab.RecreateTopic(ctx, r.Brokers, lab.TopicParts); err != nil {
		return err
	}

	// Режим записи none: заказ пишется в базу, и на этом работа сервиса
	// заканчивается. Кода отправки в этом прогоне не исполняется ни строчки.
	if err := r.configureOrders(map[string]any{"reset": true, "write_mode": "none"}); err != nil {
		return fmt.Errorf("заказы не настроены — поднят ли профиль cdc? %w", err)
	}
	if err := r.prepareOrders(cfg.FirstOrderID); err != nil {
		return err
	}

	pre := cfg.PreOrders
	if pre == 0 {
		pre = 2
	}
	if _, err := r.createOrders(pre, cfg.Client, cfg.Restaurant, cfg.Amount); err != nil {
		return err
	}

	newOrder := cfg.FirstOrderID + int64(pre)
	r.Set("pre", strconv.Itoa(pre))
	r.Set("first_order", strconv.FormatInt(cfg.FirstOrderID, 10))
	r.Set("last_pre_order", strconv.FormatInt(cfg.FirstOrderID+int64(pre)-1, 10))
	r.Set("new_order", strconv.FormatInt(newOrder, 10))
	r.Set("restaurant", cfg.Restaurant)
	r.Set("amount", strconv.FormatInt(cfg.Amount, 10))
	r.Set("slot", lab.CDCSlot)
	r.Set("publication", lab.CDCPublication)
	r.Set("topic", lab.TopicCDC)

	// ── коннектор подключается к журналу ────────────────────────────────────

	r.T0 = time.Now()
	// Имя уникально в каждом прогоне: смещения удалённого коннектора остаются
	// в служебной теме, и коннектор с прежним именем продолжил бы с прошлого
	// места — начального снимка сцена не увидела бы.
	name := fmt.Sprintf("%s-%d", cdcConnector, r.T0.UnixNano())
	// А слот называется одинаково всегда: его имя стоит в выводе сцены, и
	// студент должен найти ровно его в pg_replication_slots. Прежний слот к
	// этому моменту уже снят уборкой.
	r.Record("cdc.connect", nil)
	if err := r.cdcRegister(ctx, name, lab.CDCSlot); err != nil {
		return fmt.Errorf("коннектор не создан: %w", err)
	}
	if err := r.cdcWaitRunning(ctx, name, 90*time.Second); err != nil {
		return err
	}

	// Начальный снимок: строки, которые появились до того, как за журналом
	// начали следить. В брокере такой истории не было бы вовсе.
	if _, err := r.cdcWait(ctx, pre, 90*time.Second); err != nil {
		return err
	}
	r.Record("cdc.snapshot", nil)

	// ── обычный заказ: приложение по-прежнему ничего не отправляет ──────────

	r.SleepUntil(cfg.SecondPhaseAtS)
	r.Record("client.create", nil)
	if _, err := r.createOrders(1, cfg.Client, cfg.Restaurant, cfg.Amount); err != nil {
		return err
	}
	if _, err := r.cdcWait(ctx, pre+1, 60*time.Second); err != nil {
		return err
	}
	r.Record("cdc.insert", nil)

	// ── массовая отмена одной командой ──────────────────────────────────────

	r.SleepUntil(cfg.BulkAtS)
	var bulk struct {
		Rows int `json:"rows"`
	}
	if err := r.postJSON(r.OrdersURL+"/_lab/bulk-cancel",
		map[string]any{"restaurant": cfg.Restaurant}, &bulk); err != nil {
		return err
	}
	if _, err := r.cdcWait(ctx, pre+1+bulk.Rows, 60*time.Second); err != nil {
		return err
	}
	r.Record("cdc.update", nil)
	r.Set("bulk_rows", strconv.Itoa(bulk.Rows))

	// ── удаление строки по просьбе клиента ──────────────────────────────────

	r.SleepUntil(cfg.DeleteAtS)
	if err := r.postJSON(r.OrdersURL+"/_lab/erase",
		map[string]any{"order": cfg.FirstOrderID}, nil); err != nil {
		return err
	}
	recs, err := r.cdcWait(ctx, pre+2+bulk.Rows, 60*time.Second)
	if err != nil {
		return err
	}
	r.Record("cdc.delete", nil)

	// ── что получилось ──────────────────────────────────────────────────────

	byOp := map[string]int{}
	var short []string
	var sampleUpdate, sampleDelete cdcRec
	var updateNo, deleteNo int
	for i, rec := range recs {
		byOp[rec.Op]++
		short = append(short, cdcShort(i+1, rec))
		if rec.Op == "u" && updateNo == 0 {
			sampleUpdate, updateNo = rec, i+1
		}
		if rec.Op == "d" && deleteNo == 0 {
			sampleDelete, deleteNo = rec, i+1
		}
	}

	depth, err := conn.Depth(lab.QueueCour)
	if err != nil {
		return err
	}
	end, err := lab.EndOffsets(ctx, r.Brokers)
	if err != nil {
		return err
	}
	state, err := r.ordersState()
	if err != nil {
		return err
	}

	r.Set("in_db", strconv.Itoa(len(state.Orders)))
	r.Set("queue_left", strconv.Itoa(depth))
	r.Set("log_left", strconv.FormatInt(end, 10))
	r.Set("cdc_total", strconv.Itoa(len(recs)))
	for _, op := range []string{"r", "c", "u", "d"} {
		r.Set("n_"+op, strconv.Itoa(byOp[op]))
	}
	r.Set("records", "Тема "+lab.TopicCDC+" целиком — по строке на запись "+
		"(op: r — снимок, c — вставка, u — обновление, d — удаление):\n\n"+
		strings.Join(short, "\n"))
	r.Set("sample", fmt.Sprintf(
		"Запись #%d целиком — та же строка после массовой отмены:\n\n%s",
		updateNo, cdcFull(sampleUpdate)))
	r.Set("erased", fmt.Sprintf(
		"Запись #%d — удаление:\n\n%s", deleteNo, cdcFull(sampleDelete)))
	return r.collect()
}
