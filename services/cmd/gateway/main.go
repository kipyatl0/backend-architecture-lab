// gateway — периметр службы (m13). Единственная точка, через которую запрос
// снаружи попадает внутрь: здесь предъявляют токен, здесь принимают решение о
// праве и отсюда вниз по цепочке едет контекст пользователя.
//
// Эмитент токенов живёт в этом же процессе, отдельной ручкой (/oauth/…).
// Отдельного контейнера ради одной ручки в стенде нет, но участник это
// отдельный: шлюз ходит к нему обычным HTTP-запросом и платит за это временем
// — ровно так же, как платил бы за поход к эмитенту в чужой сети.
//
// Две модели проверки живут рядом и переключаются сценой:
//
//	local      — токен самодостаточный: подпись и срок проверяются на месте,
//	             никого не спрашивая. Отзыв при этом не виден вовсе.
//	introspect — шлюз спрашивает эмитента, действует ли токен, и держит ответ
//	             в кэше заданный срок. Отзыв виден — но не раньше, чем истечёт
//	             кэш.
package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kipyatl0/backend-architecture-lab/services/internal/lab"
)

type config struct {
	// local — проверка на месте; introspect — вопрос эмитенту.
	Check string `json:"check"`
	// Сколько живёт результат интроспекции. 0 — кэша нет вовсе, и тогда
	// каждый запрос стоит похода к эмитенту.
	CacheTTLMS int `json:"cache_ttl_ms"`
	// Сколько эмитент думает над ответом: цена интроспекции по задержке.
	IntrospectMS int `json:"introspect_ms"`
	// Срок жизни выдаваемого токена.
	TokenTTLMS int `json:"token_ttl_ms"`
}

func defaults() config {
	return config{Check: "local", CacheTTLMS: 0, IntrospectMS: 0, TokenTTLMS: 8000}
}

// claims — то, что эмитент утверждает о предъявителе. Больше сюда класть
// нечего: размер токена и устаревание того, что в нём лежит, — предмет урока,
// а не стенда.
type claims struct {
	Sub   string `json:"sub"`
	JTI   string `json:"jti"`
	ExpMS int64  `json:"exp_ms"`
}

// grant — то, что помнит о выданном токене сам эмитент. Отзыв живёт здесь и
// только здесь: у предъявителя на руках токен, который об отзыве не знает.
type grant struct {
	claims  claims
	revoked bool
}

type decision struct {
	sub    string
	active bool
	reason string
	until  time.Time // до какого момента ответ эмитента считается свежим
}

type app struct {
	mu    sync.Mutex
	cfg   config
	seq   int
	given map[string]*grant
	cache map[string]decision

	passed       atomic.Int64
	rejected     atomic.Int64
	introspected atomic.Int64
	fromCache    atomic.Int64
	noToken      atomic.Int64
	badSignature atomic.Int64
	expired      atomic.Int64
	revoked      atomic.Int64

	upstream string
	issuer   string
	key      []byte

	http   *http.Client
	log    *slog.Logger
	tracer *lab.Tracer
}

func main() {
	log := lab.Logger("gateway")
	a := &app{
		cfg:      defaults(),
		given:    map[string]*grant{},
		cache:    map[string]decision{},
		upstream: lab.Env("LAB_ORDERS_URL", "http://orders:8050"),
		// Куда шлюз ходит за интроспекцией. По умолчанию — к себе же: эмитент
		// живёт в этом контейнере. Появится отдельный — поменяется одна
		// переменная, и код проверки не изменится ни на строчку.
		issuer: lab.Env("LAB_ISSUER_URL", "http://127.0.0.1:8000"),
		key:    []byte(lab.Env("LAB_TOKEN_KEY", "lab-token-key")),
		http:   &http.Client{Timeout: 30 * time.Second},
		log:    log,
		tracer: lab.StartTracer("gateway"),
	}

	mux := http.NewServeMux()
	lab.Health(mux, func() error { return nil })

	// ── периметр ────────────────────────────────────────────────────────────
	mux.HandleFunc("GET /orders/{id}", a.handleProxy)

	// ── эмитент токенов ─────────────────────────────────────────────────────
	// Живёт в этом же процессе, но обращаются к нему по сети, как к чужому.
	mux.HandleFunc("POST /oauth/introspect", a.handleIntrospect)

	// ── управление сценой ───────────────────────────────────────────────────
	mux.HandleFunc("POST /_lab/config", a.handleConfig)
	mux.HandleFunc("POST /_lab/issue", a.handleIssue)
	mux.HandleFunc("POST /_lab/revoke", a.handleRevoke)
	mux.HandleFunc("GET /_lab/metrics", a.handleMetrics)

	addr := lab.Env("LAB_ADDR", ":8000")
	if err := lab.Serve(addr, a.tracer.Middleware(lab.WithInstance(mux)), log, nil); err != nil {
		log.Error("сервис остановлен с ошибкой", "err", err)
		os.Exit(1)
	}
}

// ── токен ───────────────────────────────────────────────────────────────────
//
// Токен собран из двух частей: утверждение и подпись под ним. Разбирать саму
// подпись курс не собирается — она нужна ровно затем, чтобы «проверяется на
// месте» отличалось от «спрашиваем у эмитента».

func (a *app) sign(payload []byte) string {
	m := hmac.New(sha256.New, a.key)
	m.Write(payload)
	return base64.RawURLEncoding.EncodeToString(payload) + "." +
		base64.RawURLEncoding.EncodeToString(m.Sum(nil))
}

// verify — вся проверка самодостаточного токена: подпись сошлась и срок не
// вышел. Ни одного обращения наружу, ни одного разделяемого состояния.
func (a *app) verify(token string) (claims, string, bool) {
	var c claims
	parts := strings.Split(token, ".")
	if len(parts) != 2 {
		return c, "bad_signature", false
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return c, "bad_signature", false
	}
	m := hmac.New(sha256.New, a.key)
	m.Write(payload)
	want := base64.RawURLEncoding.EncodeToString(m.Sum(nil))
	if !hmac.Equal([]byte(want), []byte(parts[1])) {
		return c, "bad_signature", false
	}
	if err := json.Unmarshal(payload, &c); err != nil {
		return c, "bad_signature", false
	}
	if time.Now().UnixMilli() > c.ExpMS {
		return c, "token_expired", false
	}
	return c, "", true
}

// ── проверка запроса на периметре ───────────────────────────────────────────

func (a *app) handleProxy(w http.ResponseWriter, r *http.Request) {
	token := bearer(r)
	if token == "" {
		a.noToken.Add(1)
		a.deny(w, "no_token", "токена нет")
		return
	}

	cfg := a.config()
	var (
		sub  string // чьё намерение выполняется
		how  string // как принято решение: local | issuer | cache
		ok   bool
		fail string // почему не пропустили
	)

	switch cfg.Check {
	case "introspect":
		var d decision
		d, how = a.ask(r, token, cfg)
		sub, ok, fail = d.sub, d.active, d.reason
	default: // local — самодостаточный токен
		how = "local"
		c, reason, good := a.verify(token)
		sub, ok, fail = c.Sub, good, reason
	}

	if !ok {
		switch fail {
		case "token_expired":
			a.expired.Add(1)
		case "token_revoked":
			a.revoked.Add(1)
		default:
			a.badSignature.Add(1)
		}
		w.Header().Set("X-Lab-Authz", how)
		a.deny(w, fail, denyText(fail))
		return
	}

	a.passed.Add(1)
	a.forward(w, r, sub, how)
}

// ask — интроспекция: действует ли этот токен прямо сейчас. Ответ эмитента
// кладётся в кэш на заданный срок, и всё, что случится с правами внутри этого
// срока, шлюз узнает только после того, как срок кончится.
func (a *app) ask(r *http.Request, token string, cfg config) (decision, string) {
	a.mu.Lock()
	d, hit := a.cache[token]
	if hit && time.Now().Before(d.until) {
		a.mu.Unlock()
		a.fromCache.Add(1)
		return d, "cache"
	}
	a.mu.Unlock()

	a.introspected.Add(1)
	d = a.introspect(r, token)
	if cfg.CacheTTLMS > 0 {
		d.until = time.Now().Add(time.Duration(cfg.CacheTTLMS) * time.Millisecond)
		a.mu.Lock()
		a.cache[token] = d
		a.mu.Unlock()
	}
	return d, "issuer"
}

// introspect — обычный запрос к эмитенту. Он стоит времени и требует, чтобы
// эмитент был жив: и то и другое — цена этой модели, и оба числа наблюдаемы.
func (a *app) introspect(r *http.Request, token string) decision {
	body, _ := json.Marshal(map[string]string{"token": token})
	req, err := http.NewRequestWithContext(r.Context(), http.MethodPost,
		a.issuer+"/oauth/introspect", strings.NewReader(string(body)))
	if err != nil {
		return decision{reason: "issuer_unreachable"}
	}
	req.Header.Set("Content-Type", "application/json")
	lab.Inject(r.Context(), req.Header)

	resp, err := a.http.Do(req)
	if err != nil {
		return decision{reason: "issuer_unreachable"}
	}
	defer resp.Body.Close()
	var out struct {
		Active bool   `json:"active"`
		Sub    string `json:"sub"`
		Reason string `json:"reason"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return decision{reason: "issuer_unreachable"}
	}
	return decision{sub: out.Sub, active: out.Active, reason: out.Reason}
}

// forward пропускает запрос дальше и кладёт в него контекст пользователя.
// Подпись под именем — не украшение: без неё сосед по сети выставит этот
// заголовок сам.
func (a *app) forward(w http.ResponseWriter, r *http.Request, user, how string) {
	req, err := http.NewRequestWithContext(r.Context(), r.Method, a.upstream+r.URL.Path, nil)
	if err != nil {
		lab.WriteJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
		return
	}
	req.Header.Set(lab.HeaderUser, user)
	req.Header.Set(lab.HeaderUserSig, lab.SignContext(user))
	lab.Inject(r.Context(), req.Header)

	resp, err := a.http.Do(req)
	if err != nil {
		lab.WriteJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
		return
	}
	defer resp.Body.Close()

	// Кто ответил на самом деле — часть наблюдения: за периметром стоит служба,
	// и её заголовки должны доехать до клиента как есть.
	for name, values := range resp.Header {
		if strings.HasPrefix(name, "X-Lab-") && len(values) > 0 {
			w.Header().Set(name, values[0])
		}
	}
	w.Header().Set("X-Lab-Authz", how)
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

func (a *app) deny(w http.ResponseWriter, code, text string) {
	a.rejected.Add(1)
	lab.WriteJSON(w, http.StatusUnauthorized, map[string]string{"code": code, "error": text})
}

func denyText(code string) string {
	switch code {
	case "token_expired":
		return "срок токена истёк"
	case "token_revoked":
		return "токен отозван"
	case "issuer_unreachable":
		return "эмитент не отвечает"
	default:
		return "подпись токена не сошлась"
	}
}

func bearer(r *http.Request) string {
	v := r.Header.Get("Authorization")
	if after, ok := strings.CutPrefix(v, "Bearer "); ok {
		return strings.TrimSpace(after)
	}
	return ""
}

// ── эмитент ─────────────────────────────────────────────────────────────────

func (a *app) handleIssue(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Subject string `json:"subject"`
		TTLMS   int    `json:"ttl_ms"`
	}
	if err := lab.ReadJSON(r, &req); err != nil {
		lab.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	a.mu.Lock()
	if req.TTLMS <= 0 {
		req.TTLMS = a.cfg.TokenTTLMS
	}
	a.seq++
	c := claims{
		Sub:   req.Subject,
		JTI:   "t_" + strconv.Itoa(a.seq),
		ExpMS: time.Now().Add(time.Duration(req.TTLMS) * time.Millisecond).UnixMilli(),
	}
	payload, _ := json.Marshal(c)
	token := a.sign(payload)
	a.given[token] = &grant{claims: c}
	a.mu.Unlock()

	lab.WriteJSON(w, http.StatusOK, map[string]any{
		"token": token, "jti": c.JTI, "subject": c.Sub, "ttl_ms": req.TTLMS,
	})
}

// handleRevoke отзывает права. Отзыв меняет состояние эмитента — и ничего
// больше: у предъявителя на руках остаётся тот же самый токен.
func (a *app) handleRevoke(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Token string `json:"token"`
	}
	if err := lab.ReadJSON(r, &req); err != nil {
		lab.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	g, ok := a.given[req.Token]
	if !ok {
		lab.WriteJSON(w, http.StatusNotFound, map[string]string{"error": "такого токена эмитент не выдавал"})
		return
	}
	g.revoked = true
	lab.WriteJSON(w, http.StatusOK, map[string]any{"jti": g.claims.JTI, "revoked": true})
}

func (a *app) handleIntrospect(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Token string `json:"token"`
	}
	if err := lab.ReadJSON(r, &req); err != nil {
		lab.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if d := a.config().IntrospectMS; d > 0 {
		time.Sleep(time.Duration(d) * time.Millisecond)
	}

	a.mu.Lock()
	g, known := a.given[req.Token]
	a.mu.Unlock()

	switch {
	case !known:
		lab.WriteJSON(w, http.StatusOK, map[string]any{"active": false, "reason": "unknown_token"})
	case g.revoked:
		lab.WriteJSON(w, http.StatusOK, map[string]any{
			"active": false, "sub": g.claims.Sub, "reason": "token_revoked"})
	case time.Now().UnixMilli() > g.claims.ExpMS:
		lab.WriteJSON(w, http.StatusOK, map[string]any{
			"active": false, "sub": g.claims.Sub, "reason": "token_expired"})
	default:
		lab.WriteJSON(w, http.StatusOK, map[string]any{
			"active": true, "sub": g.claims.Sub, "jti": g.claims.JTI})
	}
}

// ── управление сценой ───────────────────────────────────────────────────────

func (a *app) config() config {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.cfg
}

func (a *app) handleConfig(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Check        *string `json:"check"`
		CacheTTLMS   *int    `json:"cache_ttl_ms"`
		IntrospectMS *int    `json:"introspect_ms"`
		TokenTTLMS   *int    `json:"token_ttl_ms"`
		Reset        bool    `json:"reset"`
	}
	if err := lab.ReadJSON(r, &req); err != nil {
		lab.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	a.mu.Lock()
	if req.Reset {
		a.cfg = defaults()
		a.given = map[string]*grant{}
		a.cache = map[string]decision{}
		a.seq = 0
	}
	if req.Check != nil {
		a.cfg.Check = *req.Check
	}
	if req.CacheTTLMS != nil {
		a.cfg.CacheTTLMS = *req.CacheTTLMS
	}
	if req.IntrospectMS != nil {
		a.cfg.IntrospectMS = *req.IntrospectMS
	}
	if req.TokenTTLMS != nil {
		a.cfg.TokenTTLMS = *req.TokenTTLMS
	}
	cfg := a.cfg
	a.mu.Unlock()

	if req.Reset {
		a.resetCounters()
	}
	a.log.Info("сцена настроила периметр", "check", cfg.Check,
		"cache_ttl_ms", cfg.CacheTTLMS, "token_ttl_ms", cfg.TokenTTLMS, "reset", req.Reset)
	lab.WriteJSON(w, http.StatusOK, cfg)
}

func (a *app) resetCounters() {
	for _, c := range []*atomic.Int64{
		&a.passed, &a.rejected, &a.introspected, &a.fromCache,
		&a.noToken, &a.badSignature, &a.expired, &a.revoked,
	} {
		c.Store(0)
	}
}

// handleMetrics отдаёт то, что периметр видел у себя: сколько пропущено,
// сколько отвергнуто, сколько раз спрошено у эмитента и сколько взято из кэша.
func (a *app) handleMetrics(w http.ResponseWriter, r *http.Request) {
	cfg := a.config()
	out := map[string]any{
		"passed":       a.passed.Load(),
		"rejected":     a.rejected.Load(),
		"introspected": a.introspected.Load(),
		"from_cache":   a.fromCache.Load(),
		"reasons": map[string]int64{
			"no_token":      a.noToken.Load(),
			"bad_signature": a.badSignature.Load(),
			"token_expired": a.expired.Load(),
			"token_revoked": a.revoked.Load(),
		},
		"check":        cfg.Check,
		"cache_ttl_ms": cfg.CacheTTLMS,
	}
	if r.URL.Query().Get("reset") != "" {
		a.resetCounters()
	}
	lab.WriteJSON(w, http.StatusOK, out)
}
