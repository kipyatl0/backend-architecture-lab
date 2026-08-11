package main

// Профиль trust (m13). Периметр — единственное новое в кадре, и обе сцены
// профиля стоят на одном вопросе: где именно принимается решение о праве и что
// остаётся верным, когда запрос приходит не оттуда, откуда ждали.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/kipyatl0/backend-architecture-lab/services/internal/lab"
)

// ── общая обвязка ───────────────────────────────────────────────────────────

// gwAnswer — ответ, каким его увидел вызывающий: код, тело и две служебные
// пометки. Кто ответил на самом деле (`X-Lab-Instance`) и как решение принято
// на периметре (`X-Lab-Authz`) — заголовки, а не поля тела: тело принадлежит
// службе, и подмешивать в него наблюдение стенда нельзя.
type gwAnswer struct {
	Code     int
	Instance string
	Authz    string
	Body     struct {
		Order  int64  `json:"order"`
		Client string `json:"client"`
		Amount int64  `json:"amount"`
		Status string `json:"status"`
		Error  string `json:"error"`
		Code   string `json:"code"`
	}
}

// askOrder — один запрос за заказом. Куда идти (через периметр или мимо него),
// с каким токеном и не подделан ли контекст пользователя — решает вызывающая
// сторона, и в этом вся сцена 37.
func (r *Run) askOrder(c *http.Client, base string, id int64, token, forged string) (gwAnswer, error) {
	var a gwAnswer
	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/orders/%d", base, id), nil)
	if err != nil {
		return a, err
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	// Заголовок с именем пользователя выставить может кто угодно: он обычный.
	// Подписать его нечем — ключ есть только у периметра.
	if forged != "" {
		req.Header.Set(lab.HeaderUser, forged)
	}

	resp, err := c.Do(req)
	if err != nil {
		return a, fmt.Errorf("%s: %w", base, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	a.Code = resp.StatusCode
	a.Instance = resp.Header.Get("X-Lab-Instance")
	a.Authz = resp.Header.Get("X-Lab-Authz")
	_ = json.Unmarshal(raw, &a.Body)
	return a, nil
}

func (r *Run) configureGateway(body map[string]any) error {
	return r.postJSON(r.GatewayURL+"/_lab/config", body, nil)
}

// issueToken просит эмитента выдать токен. Эмитент живёт внутри контейнера
// периметра, но участник это отдельный: шлюз ходит к нему по сети.
func (r *Run) issueToken(subject string, ttlMS int) (string, error) {
	var out struct {
		Token string `json:"token"`
	}
	body := map[string]any{"subject": subject}
	if ttlMS > 0 {
		body["ttl_ms"] = ttlMS
	}
	if err := r.postJSON(r.GatewayURL+"/_lab/issue", body, &out); err != nil {
		return "", fmt.Errorf("эмитент не выдал токен: %w", err)
	}
	return out.Token, nil
}

// revokeToken отзывает права. Меняется при этом состояние эмитента — и только
// оно: у предъявителя на руках остаётся тот же самый токен.
func (r *Run) revokeToken(token string) error {
	return r.postJSON(r.GatewayURL+"/_lab/revoke", map[string]any{"token": token}, nil)
}

type gatewayMetrics struct {
	Passed       int64 `json:"passed"`
	Rejected     int64 `json:"rejected"`
	Introspected int64 `json:"introspected"`
	FromCache    int64 `json:"from_cache"`
	Reasons      struct {
		NoToken      int64 `json:"no_token"`
		BadSignature int64 `json:"bad_signature"`
		TokenExpired int64 `json:"token_expired"`
		TokenRevoked int64 `json:"token_revoked"`
	} `json:"reasons"`
}

func (r *Run) gatewayMetrics(reset bool) (gatewayMetrics, error) {
	var m gatewayMetrics
	url := r.GatewayURL + "/_lab/metrics"
	if reset {
		url += "?reset=1"
	}
	err := r.getJSON(url, &m)
	return m, err
}

// trustStats — то, что видел у себя сервис: сколько запросов к нему пришло и
// сколько из них пришло не через периметр. Вопрос «сколько прошло мимо» — к
// сервису, и ответ на него не зависит от того, проверяет ли он право.
type trustStats struct {
	Reads      int64 `json:"reads"`
	NoContext  int64 `json:"no_context"`
	Forged     int64 `json:"forged"`
	ViaGateway int64 `json:"via_gateway"`
	Denied     int64 `json:"denied"`
}

func (r *Run) trustStats() (trustStats, error) {
	var s trustStats
	err := r.getJSON(r.OrdersURL+"/_lab/read-stats", &s)
	return s, err
}

func (r *Run) resetReads() error {
	return r.postJSON(r.OrdersURL+"/_lab/read-reset", map[string]any{}, nil)
}

// ── сцена 37 — запрос без проверки прав изнутри сети (m13 l02) ───────────────
//
// Один и тот же запрос идёт двумя путями: через периметр и мимо него. Пока
// право проверяет только периметр, второй путь работает — и отдаёт чужие
// данные тому, кто просто оказался в той же сети. Второй прогон отличается
// одной настройкой сервиса: проверяет ли он право сам.
func scene37(ctx context.Context, r *Run) error {
	cfg := r.Script.Config
	// Наблюдает сцена: предмет — что увидел вызывающий снаружи и изнутри сети.
	r.Sources = nil

	stranger := cfg.Stranger
	if stranger == "" {
		stranger = "Марат"
	}

	if err := r.configureGateway(map[string]any{
		"reset": true, "check": "local", "token_ttl_ms": 120000,
	}); err != nil {
		return fmt.Errorf("периметр не настроен — поднят ли профиль trust? %w", err)
	}
	if err := r.configureOrders(map[string]any{
		"reset": true, "write_mode": "none", "authz": false,
	}); err != nil {
		return fmt.Errorf("сервис заказов не настроен: %w", err)
	}
	if err := r.prepareOrders(cfg.FirstOrderID); err != nil {
		return err
	}
	if err := r.resetReads(); err != nil {
		return err
	}

	// Три заказа двух клиентов: два у владельца, один у второго. Чужие данные
	// должны быть настоящими чужими данными, а не второй строкой того же лица.
	var ids []int64
	for _, owner := range []string{cfg.Client, stranger, cfg.Client} {
		got, err := r.createOrders(1, owner, cfg.Restaurant, cfg.Amount)
		if err != nil {
			return err
		}
		ids = append(ids, got[0])
	}
	ownerToken, err := r.issueToken(cfg.Client, 0)
	if err != nil {
		return err
	}
	strangerToken, err := r.issueToken(stranger, 0)
	if err != nil {
		return err
	}

	client := &http.Client{Timeout: 15 * time.Second}
	target := ids[0]

	r.Set("owner", cfg.Client)
	r.Set("stranger", stranger)
	r.Set("target", strconv.FormatInt(target, 10))
	r.Set("second", strconv.FormatInt(ids[1], 10))
	r.Set("third", strconv.FormatInt(ids[2], 10))

	// Сколько запросов пришло к сервису мимо периметра и сколько из них унесло
	// данные. Считает это сама сцена — по кодам, которые она увидела; сервис
	// подтверждает то же самое своими счётчиками.
	bypass, leak := [2]int{}, [2]int{}
	direct := func(run int, a gwAnswer) {
		bypass[run]++
		if a.Code == http.StatusOK {
			leak[run]++
		}
	}

	// Каждый обмен печатается из наблюдения: код ответа, кто ответил на самом
	// деле и что именно вернулось. Сверяется при этом код — то самое
	// утверждение, ради которого строка стоит в таблице.
	ask := func(key, base string, id int64, token, forged string) (gwAnswer, error) {
		a, err := r.askOrder(client, base, id, token, forged)
		if err != nil {
			return a, err
		}
		r.Set(key+"_code", strconv.Itoa(a.Code))
		r.Set(key+"_by", answeredBy(a))
		r.Set(key+"_body", answerBody(a))
		r.Measure(key, "code", float64(a.Code))
		return a, nil
	}

	// ── прогон 1: право проверяет только периметр ───────────────────────────
	if _, err := ask("r1.own", r.GatewayURL, target, ownerToken, ""); err != nil {
		return err
	}
	// Токен настоящий, предъявитель настоящий, заказ чужой. Периметр пропускает:
	// чей это заказ, он не знает и знать не может — этих данных у него нет.
	if _, err := ask("r1.foreign", r.GatewayURL, target, strangerToken, ""); err != nil {
		return err
	}
	// Сосед по сети приходит на периметр без токена и получает отказ.
	if _, err := ask("r1.no_token", r.GatewayURL, target, "", ""); err != nil {
		return err
	}
	// Тот же самый запрос, но прямо в службу. Сеть его пропустила: ничего
	// другого она не умеет.
	first, err := ask("r1.direct", r.OrdersURL, target, "", "")
	if err != nil {
		return err
	}
	direct(0, first)
	second, err := ask("r1.bulk", r.OrdersURL, ids[1], "", "")
	if err != nil {
		return err
	}
	direct(0, second)
	third, err := r.askOrder(client, r.OrdersURL, ids[2], "", "")
	if err != nil {
		return err
	}
	direct(0, third)
	r.Set("r1.bulk_body", fmt.Sprintf("заказы %d · %s и %d · %s",
		second.Body.Order, second.Body.Client, third.Body.Order, third.Body.Client))

	after1, err := r.trustStats()
	if err != nil {
		return err
	}
	r.Measure("bypassed", "run1", float64(after1.NoContext+after1.Forged))
	r.Measure("leaked", "run1", float64(leak[0]))
	r.Measure("denied", "run1", float64(after1.Denied))

	// ── прогон 2: сервис проверяет право сам ────────────────────────────────
	if err := r.configureOrders(map[string]any{"authz": true}); err != nil {
		return err
	}
	if _, err := ask("r2.own", r.GatewayURL, target, ownerToken, ""); err != nil {
		return err
	}
	if _, err := ask("r2.foreign", r.GatewayURL, target, strangerToken, ""); err != nil {
		return err
	}
	plain, err := ask("r2.direct", r.OrdersURL, target, "", "")
	if err != nil {
		return err
	}
	direct(1, plain)
	// Последняя попытка соседа: выставить заголовок с чужим именем самому.
	// Заголовок он выставит, подпись под ним — нет.
	forged, err := ask("r2.forged", r.OrdersURL, target, "", cfg.Client)
	if err != nil {
		return err
	}
	direct(1, forged)

	total, err := r.trustStats()
	if err != nil {
		return err
	}
	r.Measure("bypassed", "run2", float64(total.NoContext+total.Forged-after1.NoContext-after1.Forged))
	r.Measure("leaked", "run2", float64(leak[1]))
	r.Measure("denied", "run2", float64(total.Denied-after1.Denied))
	r.Set("leaked_n", strconv.Itoa(leak[0]))
	r.Set("bypass_n", strconv.Itoa(bypass[0]))
	r.Set("denied_n", strconv.FormatInt(total.Denied-after1.Denied, 10))
	return nil
}

// answeredBy — кто ответил по существу. Имя инстанса приходит заголовком, и
// периметр честно передаёт вниз чужой: 403 из-под шлюза выдал не шлюз.
func answeredBy(a gwAnswer) string {
	if a.Instance == "gateway" {
		return "шлюз"
	}
	return "сервис"
}

func answerBody(a gwAnswer) string {
	if a.Code == http.StatusOK {
		return fmt.Sprintf("заказ %d · %s · %d", a.Body.Order, a.Body.Client, a.Body.Amount)
	}
	return a.Body.Error
}

// ── сцена 38 — токен отозван, доступ работает (m13 l03) ─────────────────────
//
// Два прогона отличаются одной настройкой периметра: проверять токен на месте
// или спрашивать эмитента. Всё остальное — тот же клиент, тот же заказ, тот же
// отзыв в ту же секунду. Наблюдаемое — сколько времени после отзыва доступ ещё
// работал, и это число выводится из срока жизни токена и срока кэша.
func scene38(ctx context.Context, r *Run) error {
	cfg := r.Script.Config
	r.Sources = nil

	tokenTTL := cfg.TokenTTLMS
	if tokenTTL == 0 {
		tokenTTL = 8000
	}
	cacheTTL := cfg.CacheTTLMS
	if cacheTTL == 0 {
		cacheTTL = 3000
	}
	poll := cfg.PollMS
	if poll == 0 {
		poll = 400
	}
	revokeAt := cfg.RevokeAtS
	if revokeAt == 0 {
		revokeAt = 2.0
	}
	first := float64(poll) / 2000.0 // первый запрос — на полшага, чтобы срок и опрос не совпали

	if err := r.configureOrders(map[string]any{
		"reset": true, "write_mode": "none", "authz": true,
	}); err != nil {
		return fmt.Errorf("сервис заказов не настроен — поднят ли профиль trust? %w", err)
	}
	if err := r.prepareOrders(cfg.FirstOrderID); err != nil {
		return err
	}
	ids, err := r.createOrders(1, cfg.Client, cfg.Restaurant, cfg.Amount)
	if err != nil {
		return err
	}
	target := ids[0]

	r.Set("owner", cfg.Client)
	r.Set("target", strconv.FormatInt(target, 10))
	r.Set("token_ttl", seconds(tokenTTL))
	r.Set("cache_ttl", seconds(cacheTTL))
	r.Set("poll", seconds(poll))
	r.Set("revoke_at", commaFloat(revokeAt, 1))
	r.Set("introspect_ms", strconv.Itoa(cfg.IntrospectMS))

	client := &http.Client{Timeout: 15 * time.Second}

	// Один прогон: выдать токен, опрашивать заказ, отозвать права в назначенную
	// секунду и дождаться первого отказа.
	run := func(name, check string, cache int) error {
		if err := r.configureGateway(map[string]any{
			"reset": true, "check": check, "cache_ttl_ms": cache,
			"introspect_ms": cfg.IntrospectMS, "token_ttl_ms": tokenTTL,
		}); err != nil {
			return fmt.Errorf("периметр не настроен — поднят ли профиль trust? %w", err)
		}
		token, err := r.issueToken(cfg.Client, tokenTTL)
		if err != nil {
			return err
		}

		r.T0 = time.Now()
		var revokedAt time.Time
		passedAfter, requests := 0, 0
		done := false

		for k := 0; k < 200; k++ {
			at := first + float64(k)*float64(poll)/1000.0
			if revokedAt.IsZero() && at > revokeAt {
				r.SleepUntil(revokeAt)
				if err := r.revokeToken(token); err != nil {
					return err
				}
				revokedAt = time.Now()
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			r.SleepUntil(at)
			a, err := r.askOrder(client, r.GatewayURL, target, token, "")
			if err != nil {
				return err
			}
			requests++
			if a.Code == http.StatusOK {
				if !revokedAt.IsZero() {
					passedAfter++
				}
				continue
			}
			// Первый отказ и есть конец окна. Чем именно он объяснён, сцена не
			// придумывает: причину называет тот, кто отказал.
			if revokedAt.IsZero() {
				return fmt.Errorf("прогон «%s»: доступ прекратился до отзыва (%s) — проверь сроки сцены",
					name, endReason(a))
			}
			window := time.Since(revokedAt).Seconds()
			r.Measure("window", name, window)
			r.Measure("passed", name, float64(passedAfter))
			r.Measure("requests", name, float64(requests))
			r.Set(name+"_end", endReason(a))
			done = true
			break
		}
		if !done {
			return fmt.Errorf("прогон «%s»: доступ не прекратился за 200 запросов", name)
		}

		m, err := r.gatewayMetrics(false)
		if err != nil {
			return err
		}
		r.Measure("issuer", name, float64(m.Introspected))
		r.Measure("cache", name, float64(m.FromCache))
		return nil
	}

	// Самодостаточный токен: кэшу здесь взяться неоткуда — спрашивать некого.
	if err := run("local", "local", 0); err != nil {
		return err
	}
	// Интроспекция у эмитента: тот же токен, тот же отзыв, ответ живёт в кэше.
	return run("introspect", "introspect", cacheTTL)
}

// endReason — чем кончился доступ, словами того, кто отказал.
func endReason(a gwAnswer) string {
	switch a.Body.Code {
	case "token_expired":
		return "истёк срок жизни токена"
	case "token_revoked":
		return "эмитент ответил: токен отозван"
	case "no_token":
		return "токен не предъявлен"
	default:
		return a.Body.Error
	}
}

// seconds — миллисекунды словами вывода: «3,0 с». Десятичная запятая, потому
// что вывод копируется в русский текст шага.
func seconds(ms int) string {
	return commaFloat(float64(ms)/1000.0, 1) + " с"
}

func commaFloat(v float64, digits int) string {
	s := strconv.FormatFloat(v, 'f', digits, 64)
	return replaceDot(s)
}

func replaceDot(s string) string {
	out := []rune(s)
	for i, c := range out {
		if c == '.' {
			out[i] = ','
		}
	}
	return string(out)
}
