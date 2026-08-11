package lab

// Трассировка и связанный с ней структурный лог — общая часть для всех сервисов
// стенда (профиль trace, m11).
//
// Своего клиента, а не библиотеки: у стенда одна зависимость — Docker, и все
// его клиенты (базы, брокера, быстрой памяти, Docker API) написаны здесь же
// минимальным объёмом. Приёмнику трейсов нужны ровно две вещи — заголовок
// W3C `traceparent` между процессами и POST со спанами в конце, — и обе
// умещаются в этот файл.
//
// Выключено, пока стенду не задан приёмник (LAB_OTLP_URL). В профилях m01–m10
// переменная пуста, и сервисы работают ровно как раньше: ни одного лишнего
// действия, ни одного лишнего байта в выводе сцен.

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Вид спана в терминах OTLP. Курс их называет по-русски, но в приёмник они
// уходят числами протокола, и менять их нельзя.
const (
	KindInternal = 1
	KindServer   = 2
	KindClient   = 3
	KindProducer = 4
	KindConsumer = 5
)

// SpanContext — то, что едет между процессами. Ровно два идентификатора:
// какому трейсу принадлежит работа и чьим продолжением она является.
type SpanContext struct {
	TraceID string // 32 шестнадцатеричных знака
	SpanID  string // 16 шестнадцатеричных знаков
}

func (c SpanContext) Valid() bool { return len(c.TraceID) == 32 && len(c.SpanID) == 16 }

// Traceparent — заголовок W3C: версия, трейс, спан, флаги. Формат общий для
// всех языков и всех приёмников, и именно поэтому контекст переживает границу
// процесса, написанного кем угодно на чём угодно.
func (c SpanContext) Traceparent() string {
	if !c.Valid() {
		return ""
	}
	return "00-" + c.TraceID + "-" + c.SpanID + "-01"
}

func ParseTraceparent(v string) (SpanContext, bool) {
	parts := strings.Split(strings.TrimSpace(v), "-")
	if len(parts) != 4 || parts[0] != "00" {
		return SpanContext{}, false
	}
	c := SpanContext{TraceID: strings.ToLower(parts[1]), SpanID: strings.ToLower(parts[2])}
	if !c.Valid() {
		return SpanContext{}, false
	}
	if _, err := hex.DecodeString(c.TraceID + c.SpanID); err != nil {
		return SpanContext{}, false
	}
	return c, true
}

type ctxKey struct{}

func ContextWith(ctx context.Context, c SpanContext) context.Context {
	return context.WithValue(ctx, ctxKey{}, c)
}

// ContextOf — контекст трассировки, лежащий в ctx. Пустой, если его туда никто
// не положил: ровно это и происходит с потребителем события, которому контекст
// не приехал в заголовках сообщения.
func ContextOf(ctx context.Context) SpanContext {
	c, _ := ctx.Value(ctxKey{}).(SpanContext)
	return c
}

// TraceIDOf — метка трассировки для строки лога. Пусто — значит эта работа ни
// к какому трейсу не привязана, и найти её по метке будет нельзя.
func TraceIDOf(ctx context.Context) string { return ContextOf(ctx).TraceID }

// Carrier — контекст строкой, пригодной для заголовка сообщения. Отправитель
// решает, класть ли её в сообщение; на этом решении и построена сцена 33.
func Carrier(ctx context.Context) string { return ContextOf(ctx).Traceparent() }

// WithCarrier — обратное действие на стороне получателя: контекст из заголовка
// сообщения. Заголовка нет — потребитель начнёт свой собственный трейс, и
// связи с тем, кто событие породил, не будет ни у кого.
func WithCarrier(ctx context.Context, traceparent string) context.Context {
	if c, ok := ParseTraceparent(traceparent); ok {
		return ContextWith(ctx, c)
	}
	return ctx
}

// ── спан ────────────────────────────────────────────────────────────────────

type Span struct {
	tr     *Tracer
	Ctx    SpanContext
	parent string
	name   string
	kind   int
	start  time.Time

	mu    sync.Mutex
	attrs map[string]string
	ended bool
}

// Rename меняет имя спана уже после начала. Нужно там, где путь запроса —
// служебный (`/_lab/assign`), а в дереве трейса студент должен видеть работу,
// а не адрес ручки стенда.
func (s *Span) Rename(name string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.name = name
	s.mu.Unlock()
}

func (s *Span) Set(key, value string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	if s.attrs == nil {
		s.attrs = map[string]string{}
	}
	s.attrs[key] = value
	s.mu.Unlock()
}

func (s *Span) End() {
	if s == nil || s.tr == nil {
		return
	}
	s.mu.Lock()
	if s.ended {
		s.mu.Unlock()
		return
	}
	s.ended = true
	rec := otlpSpan{
		TraceID:      s.Ctx.TraceID,
		SpanID:       s.Ctx.SpanID,
		ParentSpanID: s.parent,
		Name:         s.name,
		Kind:         s.kind,
		Start:        fmt.Sprintf("%d", s.start.UnixNano()),
		End:          fmt.Sprintf("%d", time.Now().UnixNano()),
	}
	for k, v := range s.attrs {
		rec.Attributes = append(rec.Attributes, otlpAttr{Key: k, Value: otlpValue{String: v}})
	}
	s.mu.Unlock()
	s.tr.enqueue(rec)
}

// ── трассировщик ────────────────────────────────────────────────────────────

type Tracer struct {
	service  string
	endpoint string
	client   *http.Client

	mu    sync.Mutex
	queue []otlpSpan
}

// StartTracer поднимает отправку спанов, если стенду задан приёмник. Пусто —
// возвращается выключенный трассировщик: все его вызовы стоят одну проверку
// указателя, и профили до m11 ведут себя в точности как раньше.
func StartTracer(service string) *Tracer {
	endpoint := Env("LAB_OTLP_URL", "")
	if endpoint == "" {
		return &Tracer{service: service}
	}
	t := &Tracer{
		service:  service,
		endpoint: endpoint,
		client:   &http.Client{Timeout: 5 * time.Second},
	}
	// Спаны уходят пачками: отдельный POST на каждый спан превратил бы
	// наблюдение в нагрузку, а сцена меряет службу, а не приёмник трейсов.
	go func() {
		for {
			time.Sleep(200 * time.Millisecond)
			t.Flush()
		}
	}()
	return t
}

func (t *Tracer) Enabled() bool { return t != nil && t.endpoint != "" }

// Start заводит спан — отрезок работы с именем, началом и концом. Родителя
// берёт из ctx: есть контекст — спан продолжает чужой трейс, нет — начинает
// свой собственный.
func (t *Tracer) Start(ctx context.Context, name string, kind int) (context.Context, *Span) {
	if !t.Enabled() {
		return ctx, nil
	}
	parent := ContextOf(ctx)
	s := &Span{tr: t, name: name, kind: kind, start: time.Now()}
	s.Ctx.SpanID = randomHex(8)
	if parent.Valid() {
		s.Ctx.TraceID = parent.TraceID
		s.parent = parent.SpanID
	} else {
		s.Ctx.TraceID = randomHex(16)
	}
	return ContextWith(ctx, s.Ctx), s
}

// Middleware — серверный спан вокруг каждого запроса. Он же вносит контекст
// вызывающего: по HTTP тот едет заголовком сам собой, и ни один сервис для
// этого ничего не делает. Ровно поэтому разрыв в сцене 33 случается не здесь,
// а на брокере.
func (t *Tracer) Middleware(h http.Handler) http.Handler {
	if !t.Enabled() {
		return h
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := WithCarrier(r.Context(), r.Header.Get("traceparent"))
		// Служебные ручки стенда и проверки здоровья своего трейса не заводят:
		// иначе половина собранного была бы опросом контейнеров каждые две
		// секунды. Но чужой трейс они продолжают — синхронный шаг саги ходит
		// именно в такую ручку, и в дереве он обязан быть.
		if internalPath(r.URL.Path) && !ContextOf(ctx).Valid() {
			h.ServeHTTP(w, r)
			return
		}
		ctx, span := t.Start(ctx, r.Method+" "+routeOf(r.URL.Path), KindServer)
		defer span.End()
		h.ServeHTTP(w, r.WithContext(WithSpan(ctx, span)))
	})
}

func internalPath(path string) bool {
	return strings.HasPrefix(path, "/_lab/") || path == "/health" || path == "/ready"
}

// routeOf сводит путь к маршруту: `/orders/7714` → `/orders/{id}`. Спан
// именуется маршрутом, а не адресом, — иначе у каждого заказа заводилось бы
// собственное имя операции, и любая сводка по ним рассыпалась бы в пыль.
func routeOf(path string) string {
	parts := strings.Split(path, "/")
	for i, p := range parts {
		if p == "" {
			continue
		}
		if strings.IndexFunc(p, func(r rune) bool { return r < '0' || r > '9' }) < 0 {
			parts[i] = "{id}"
		}
	}
	return strings.Join(parts, "/")
}

// Inject кладёт контекст в исходящий запрос. Одна строка на вызов — и вся
// разница между «трейс дошёл до соседнего сервиса» и «не дошёл».
func Inject(ctx context.Context, h http.Header) {
	if tp := Carrier(ctx); tp != "" {
		h.Set("traceparent", tp)
	}
}

// SpanOf достаёт спан текущей работы там, где его завёл не вызывающий код, а
// обвязка: обработчику нужно переименовать спан или дописать в него поле.
func SpanOf(ctx context.Context) *Span {
	s, _ := ctx.Value(spanKey{}).(*Span)
	return s
}

type spanKey struct{}

// WithSpan кладёт спан в ctx рядом с его контекстом. Отдельно от SpanContext:
// контекст едет между процессами, спан живёт только в этом.
func WithSpan(ctx context.Context, s *Span) context.Context {
	if s == nil {
		return ctx
	}
	return context.WithValue(ctx, spanKey{}, s)
}

func (t *Tracer) enqueue(s otlpSpan) {
	t.mu.Lock()
	t.queue = append(t.queue, s)
	n := len(t.queue)
	t.mu.Unlock()
	if n >= 128 {
		t.Flush()
	}
}

// Flush отправляет накопленное. Ошибку глотаем намеренно: приёмник трейсов —
// не участник сцены, и его недоступность не должна ронять службу. Это и есть
// нормальное свойство наблюдаемости: она пристроена сбоку.
func (t *Tracer) Flush() {
	if !t.Enabled() {
		return
	}
	t.mu.Lock()
	batch := t.queue
	t.queue = nil
	t.mu.Unlock()
	if len(batch) == 0 {
		return
	}

	payload := otlpPayload{ResourceSpans: []otlpResourceSpans{{
		Resource: otlpResource{Attributes: []otlpAttr{
			{Key: "service.name", Value: otlpValue{String: t.service}},
		}},
		ScopeSpans: []otlpScopeSpans{{Spans: batch}},
	}}}
	body, err := json.Marshal(payload)
	if err != nil {
		return
	}
	req, err := http.NewRequest(http.MethodPost, t.endpoint, bytes.NewReader(body))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := t.client.Do(req)
	if err != nil {
		return
	}
	resp.Body.Close()
}

func randomHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		// Практически недостижимо; но молчаливый нулевой идентификатор был бы
		// хуже — все спаны схлопнулись бы в один трейс.
		for i := range b {
			b[i] = byte(time.Now().UnixNano() >> (i % 8 * 8))
		}
	}
	return hex.EncodeToString(b)
}

// ── формат OTLP/JSON ────────────────────────────────────────────────────────
//
// Протокол приёма спанов. Идентификаторы в JSON-варианте — шестнадцатеричные
// строки, времена — наносекунды с эпохи строкой (в JSON нет 64-битного целого).

type otlpPayload struct {
	ResourceSpans []otlpResourceSpans `json:"resourceSpans"`
}

type otlpResourceSpans struct {
	Resource   otlpResource     `json:"resource"`
	ScopeSpans []otlpScopeSpans `json:"scopeSpans"`
}

type otlpResource struct {
	Attributes []otlpAttr `json:"attributes"`
}

type otlpScopeSpans struct {
	Spans []otlpSpan `json:"spans"`
}

type otlpSpan struct {
	TraceID      string     `json:"traceId"`
	SpanID       string     `json:"spanId"`
	ParentSpanID string     `json:"parentSpanId,omitempty"`
	Name         string     `json:"name"`
	Kind         int        `json:"kind"`
	Start        string     `json:"startTimeUnixNano"`
	End          string     `json:"endTimeUnixNano"`
	Attributes   []otlpAttr `json:"attributes,omitempty"`
}

type otlpAttr struct {
	Key   string    `json:"key"`
	Value otlpValue `json:"value"`
}

type otlpValue struct {
	String string `json:"stringValue"`
}

// ── третий сигнал: лог с меткой трассировки ─────────────────────────────────
//
// Метрики говорят, что стало плохо; трейс — где именно; лог — что там
// случилось. Связывает их всех одна метка, и она обязана попадать в строку
// лога сама, а не приписываться руками.

// LogLine — строка структурного лога, доступная сцене. Ровно то же, что уходит
// в stdout сервиса, плюс метка трассировки.
type LogLine struct {
	At      time.Time         `json:"at"`
	Service string            `json:"service"`
	Message string            `json:"message"`
	TraceID string            `json:"trace_id,omitempty"`
	Fields  map[string]string `json:"fields,omitempty"`
}

// Trail — последние строки лога сервиса. Кольцо, а не журнал: сцене нужен
// хвост, а не история, и расти в памяти ему незачем.
type Trail struct {
	service string
	mu      sync.Mutex
	lines   []LogLine
}

func NewTrail(service string) *Trail { return &Trail{service: service} }

const trailLimit = 500

func (t *Trail) Write(ctx context.Context, message string, fields map[string]string) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.lines = append(t.lines, LogLine{
		At: time.Now().UTC(), Service: t.service, Message: message,
		TraceID: TraceIDOf(ctx), Fields: fields,
	})
	if len(t.lines) > trailLimit {
		t.lines = t.lines[len(t.lines)-trailLimit:]
	}
}

func (t *Trail) Clear() {
	if t == nil {
		return
	}
	t.mu.Lock()
	t.lines = nil
	t.mu.Unlock()
}

// Handler отдаёт хвост лога: GET /_lab/log[?trace=<id>]. Отбор по метке — это
// и есть переход «из трейса в лог», который курс показывает в сцене 34.
func (t *Trail) Handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		want := r.URL.Query().Get("trace")
		t.mu.Lock()
		out := make([]LogLine, 0, len(t.lines))
		for _, l := range t.lines {
			if want != "" && l.TraceID != want {
				continue
			}
			out = append(out, l)
		}
		t.mu.Unlock()
		WriteJSON(w, http.StatusOK, map[string]any{"lines": out})
	}
}
