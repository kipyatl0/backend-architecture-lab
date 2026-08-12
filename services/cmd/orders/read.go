package main

// Путь чтения (m08 l01 и m09 l04). До сих пор сервис заказов только писал: все
// вопросы курса были про то, как запись доходит до второго сервиса. Здесь у
// него появляются две вещи, без которых чтение не обсудить, — вторая копия
// базы и кэш перед ней.
//
// Обе включаются сценой и по умолчанию выключены: сервис остаётся тем же
// самым, меняется только то, откуда он берёт ответ.

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/kipyatl0/backend-architecture-lab/services/internal/lab"
)

// readSide — всё, что сервис завёл ради чтения. Живёт рядом с app, а не внутри
// него, чтобы было видно: путь записи от этого не изменился ни на строчку.
type readSide struct {
	mu       sync.Mutex
	replica  *pgxpool.Pool
	cache    *lab.Redis
	inflight map[string]*flight
	// Кэш внутри процесса (m10 l01). Та же карточка ресторана, только лежит она
	// не в общей быстрой памяти, а в памяти этого инстанса — и потому у каждого
	// инстанса своя. Сбросить её снаружи нечем: ровно это и делает локальный
	// кэш спрятанным состоянием, а инстансы — не взаимозаменяемыми.
	local map[string]localEntry

	dbQueries atomic.Int64
	hits      atomic.Int64
	misses    atomic.Int64

	// Кто приходил за заказом (m13 l02). Считается всегда, а не только когда
	// сервис проверяет право: «сколько запросов пришло мимо периметра» — вопрос
	// к сервису, и ответ на него не должен зависеть от его настроек.
	reads      atomic.Int64
	noContext  atomic.Int64 // контекста пользователя нет вовсе: запрос пришёл не через периметр
	forged     atomic.Int64 // контекст есть, подпись под ним не сошлась
	viaGateway atomic.Int64 // контекст подтверждён подписью периметра
	denied     atomic.Int64 // право проверено сервисом и не подтвердилось
}

type localEntry struct {
	value string
	until time.Time
}

// flight — один поход в базу, который делят все, кто пришёл за тем же ключом.
// Классическая защита от давки: она не ускоряет промах, она делает его одним.
type flight struct {
	wg   sync.WaitGroup
	card string
	err  error
}

func newReadSide() *readSide {
	return &readSide{inflight: map[string]*flight{}, local: map[string]localEntry{}}
}

// ── локальный кэш инстанса ──────────────────────────────────────────────────

func (rs *readSide) localGet(key string) (string, bool) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	e, ok := rs.local[key]
	if !ok || time.Now().After(e.until) {
		return "", false
	}
	return e.value, true
}

func (rs *readSide) localSet(key, value string, ttl time.Duration) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.local[key] = localEntry{value: value, until: time.Now().Add(ttl)}
}

// localDrop сбрасывает ключ у ЭТОГО инстанса. Другие инстансы про запись не
// узнают: дотянуться до чужой памяти процессу нечем, и это не недоделка стенда,
// а свойство локального кэша.
func (rs *readSide) localDrop(key string) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	delete(rs.local, key)
}

func (rs *readSide) localClear() {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.local = map[string]localEntry{}
}

// replicaPool подключается ко второй копии базы лениво: в профилях m01–m07
// её рядом нет вовсе, и требовать её от сервиса было бы неправдой.
func (rs *readSide) replicaPool(ctx context.Context) (*pgxpool.Pool, error) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if rs.replica != nil {
		return rs.replica, nil
	}
	url := lab.Env("LAB_REPLICA_URL", "")
	if url == "" {
		return nil, fmt.Errorf("адрес второй копии базы не задан")
	}
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("вторая копия базы недоступна: %w", err)
	}
	rs.replica = pool
	return pool, nil
}

// resetReplica закрывает пул ко второй копии. Нужен после того, как сцена
// пересобрала реплику: старые соединения ведут в контейнер, которого больше
// нет, и первый же запрос после этого отвечал бы ошибкой связи, а не данными.
func (rs *readSide) resetReplica() {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if rs.replica != nil {
		rs.replica.Close()
		rs.replica = nil
	}
}

func (rs *readSide) redis() (*lab.Redis, error) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if rs.cache != nil {
		return rs.cache, nil
	}
	c, err := lab.DialRedis(lab.Env("LAB_REDIS_ADDR", "redis:6379"), 32)
	if err != nil {
		return nil, fmt.Errorf("кэш недоступен: %w", err)
	}
	rs.cache = c
	return c, nil
}

// ── чтение заказа: из первичного узла или из второй копии ───────────────────

func (a *app) handleReadOrder(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		lab.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "номер заказа не число"})
		return
	}
	cfg := a.config()

	// Кто пришёл. Контекст пользователя ставит периметр и подписывает его своим
	// ключом; выставить заголовок может кто угодно, кто дотянулся до сети, —
	// подписать не может никто, кроме периметра.
	user := r.Header.Get(lab.HeaderUser)
	trusted := lab.CheckContext(user, r.Header.Get(lab.HeaderUserSig))
	a.read.reads.Add(1)
	switch {
	case user == "":
		a.read.noContext.Add(1)
	case !trusted:
		a.read.forged.Add(1)
	default:
		a.read.viaGateway.Add(1)
	}

	// Сервис проверяет право сам — или не проверяет вовсе. Второе и есть та
	// самая мягкая начинка: «раз запрос дошёл, значит его уже проверили».
	if cfg.Authz && !trusted {
		a.read.denied.Add(1)
		reason := "контекста пользователя нет"
		if user != "" {
			reason = "подпись контекста не сошлась"
		}
		lab.WriteJSON(w, http.StatusUnauthorized, map[string]any{
			"order": id, "code": "no_context", "error": reason,
		})
		return
	}

	pool := a.pool
	source := "primary"
	if cfg.ReadFrom == "replica" {
		p, err := a.read.replicaPool(r.Context())
		if err != nil {
			lab.WriteJSON(w, http.StatusServiceUnavailable, map[string]string{"error": err.Error()})
			return
		}
		pool, source = p, "replica"
	}

	var (
		client, restaurant, status string
		amount                     int64
	)
	err = pool.QueryRow(r.Context(),
		`SELECT client, restaurant, amount, status FROM orders WHERE id = $1`, id).
		Scan(&client, &restaurant, &amount, &status)
	if err != nil {
		// Заказ, которого на этой копии ещё нет, — не ошибка сервиса, а ровно
		// то наблюдение, ради которого сцена существует.
		lab.WriteJSON(w, http.StatusNotFound, map[string]any{
			"order": id, "found": false, "source": source,
		})
		return
	}
	// Статус разбирается по перечню известных значений — как разбирает его
	// любой enum. Старая версия собрана до того, как в процессе появился новый
	// шаг, и значения packing для неё не существует: это не «неизвестное поле,
	// которое можно пропустить», а невозможное состояние.
	//
	// Отсюда и берётся точка невозврата (m14 l01): откатить код — минута,
	// а строки, которые новая версия успела написать, откатывать нечем.
	if cfg.Version != "v2" && !knownStatusV1(status) {
		lab.WriteJSON(w, http.StatusInternalServerError, map[string]any{
			"order": id, "code": "unknown_status",
			"error": "неизвестный статус заказа: " + status,
		})
		return
	}

	// Владелец заказа известен только здесь: у периметра этих данных нет и быть
	// не может. Поэтому решение «твой ли это заказ» принимается рядом с данными,
	// а не на границе сети.
	if cfg.Authz && client != user {
		a.read.denied.Add(1)
		lab.WriteJSON(w, http.StatusForbidden, map[string]any{
			"order": id, "code": "not_yours", "error": "заказ не ваш",
		})
		return
	}
	lab.WriteJSON(w, http.StatusOK, map[string]any{
		"order": id, "found": true, "source": source,
		"client": client, "restaurant": restaurant, "amount": amount, "status": status,
	})
}

// knownStatusV1 — статусы, которые знала версия, работавшая до изменения.
// Перечень закрытый, и в этом весь смысл: открытый перечень пережил бы новое
// значение молча, а закрытый на нём останавливается.
func knownStatusV1(s string) bool {
	switch s {
	case "created", "paid", "assigned", "cancelled":
		return true
	}
	return false
}

// ── карточка ресторана: дешёвое чтение через кэш ────────────────────────────

// card собирает карточку ресторана из базы. Запрос настоящий и стоит настоящих
// денег: сколько именно — задаёт сцена, как задаёт задержку эквайринга.
func (a *app) card(ctx context.Context, name string) (string, error) {
	cfg := a.config()
	a.read.dbQueries.Add(1)

	var orders int64
	var total int64
	// Задержка стоит отдельным множителем, а не условием в WHERE: в условии
	// она выполнялась бы на каждой строке, и «запрос стоит 200 мс» превратился
	// бы в «200 мс за строку» — число в шапке сцены оказалось бы враньём.
	err := a.pool.QueryRow(ctx, `
		SELECT c.orders, c.total
		  FROM (SELECT count(*) AS orders, coalesce(sum(amount), 0) AS total
		          FROM orders WHERE restaurant = $1) c,
		       pg_sleep($2)`,
		name, float64(cfg.CardMS)/1000.0).Scan(&orders, &total)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`{"restaurant":%q,"orders":%d,"total":%d}`, name, orders, total), nil
}

func (a *app) handleCard(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	cfg := a.config()
	rs := a.read
	key := "card:" + name

	// Где лежит значение — переключатель сцены, а не свойство службы. Общая
	// быстрая память (m09) видна всем инстансам сразу; память процесса (m10) —
	// только ему одному, и в этом вся разница.
	var (
		get func() (string, bool)
		put func(string)
	)
	if cfg.CacheMode == "local" {
		get = func() (string, bool) { return rs.localGet(key) }
		put = func(v string) { rs.localSet(key, v, time.Duration(cfg.CacheTTLMS)*time.Millisecond) }
	} else {
		cache, err := rs.redis()
		if err != nil {
			lab.WriteJSON(w, http.StatusServiceUnavailable, map[string]string{"error": err.Error()})
			return
		}
		get = func() (string, bool) {
			v, ok, err := cache.Get(key)
			return v, err == nil && ok
		}
		put = func(v string) {
			_ = cache.SetPX(key, v, time.Duration(cfg.CacheTTLMS)*time.Millisecond)
		}
	}

	if v, ok := get(); ok {
		rs.hits.Add(1)
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("X-Lab-Source", "cache")
		fmt.Fprint(w, v)
		return
	}
	rs.misses.Add(1)

	fill := func() (string, error) {
		v, err := a.card(r.Context(), name)
		if err != nil {
			return "", err
		}
		put(v)
		return v, nil
	}

	var (
		value string
		err   error
	)
	if !cfg.SingleFlight {
		// Без защиты каждый, кто пришёл на промах, идёт в базу сам. Именно
		// это и называется давкой: чем популярнее ключ, тем больше их придёт.
		value, err = fill()
	} else {
		value, err = rs.share(key, fill)
	}
	if err != nil {
		lab.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("X-Lab-Source", "db")
	fmt.Fprint(w, value)
}

// share — тот самый «один поход за всех»: первый пришедший идёт в базу,
// остальные ждут его результата.
func (rs *readSide) share(key string, fill func() (string, error)) (string, error) {
	rs.mu.Lock()
	if f, ok := rs.inflight[key]; ok {
		rs.mu.Unlock()
		f.wg.Wait()
		return f.card, f.err
	}
	f := &flight{}
	f.wg.Add(1)
	rs.inflight[key] = f
	rs.mu.Unlock()

	f.card, f.err = fill()

	rs.mu.Lock()
	delete(rs.inflight, key)
	rs.mu.Unlock()
	f.wg.Done()
	return f.card, f.err
}

// ── что сцена спрашивает у сервиса после прогона ────────────────────────────

func (a *app) handleReadStats(w http.ResponseWriter, r *http.Request) {
	rs := a.read
	lab.WriteJSON(w, http.StatusOK, map[string]any{
		"db_queries":  rs.dbQueries.Load(),
		"hits":        rs.hits.Load(),
		"misses":      rs.misses.Load(),
		"reads":       rs.reads.Load(),
		"no_context":  rs.noContext.Load(),
		"forged":      rs.forged.Load(),
		"via_gateway": rs.viaGateway.Load(),
		"denied":      rs.denied.Load(),
	})
}

// handleReadReset обнуляет счётчики и выбрасывает кэш: каждая сцена начинается
// с холодного кэша, иначе первое же число в выводе окажется чужим.
func (a *app) handleReadReset(w http.ResponseWriter, r *http.Request) {
	rs := a.read
	rs.dbQueries.Store(0)
	rs.hits.Store(0)
	rs.misses.Store(0)
	rs.reads.Store(0)
	rs.noContext.Store(0)
	rs.forged.Store(0)
	rs.viaGateway.Store(0)
	rs.denied.Store(0)
	rs.resetReplica()
	rs.localClear()
	if c, err := rs.redis(); err == nil {
		_ = c.FlushAll()
	}
	lab.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}
