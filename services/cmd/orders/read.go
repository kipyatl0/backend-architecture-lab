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

	dbQueries atomic.Int64
	hits      atomic.Int64
	misses    atomic.Int64
}

// flight — один поход в базу, который делят все, кто пришёл за тем же ключом.
// Классическая защита от давки: она не ускоряет промах, она делает его одним.
type flight struct {
	wg   sync.WaitGroup
	card string
	err  error
}

func newReadSide() *readSide {
	return &readSide{inflight: map[string]*flight{}}
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
	lab.WriteJSON(w, http.StatusOK, map[string]any{
		"order": id, "found": true, "source": source,
		"client": client, "restaurant": restaurant, "amount": amount, "status": status,
	})
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

	cache, err := rs.redis()
	if err != nil {
		lab.WriteJSON(w, http.StatusServiceUnavailable, map[string]string{"error": err.Error()})
		return
	}
	key := "card:" + name

	if v, ok, err := cache.Get(key); err == nil && ok {
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
		_ = cache.SetPX(key, v, time.Duration(cfg.CacheTTLMS)*time.Millisecond)
		return v, nil
	}

	var value string
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
		"db_queries": rs.dbQueries.Load(),
		"hits":       rs.hits.Load(),
		"misses":     rs.misses.Load(),
	})
}

// handleReadReset обнуляет счётчики и выбрасывает кэш: каждая сцена начинается
// с холодного кэша, иначе первое же число в выводе окажется чужим.
func (a *app) handleReadReset(w http.ResponseWriter, r *http.Request) {
	rs := a.read
	rs.dbQueries.Store(0)
	rs.hits.Store(0)
	rs.misses.Store(0)
	rs.resetReplica()
	if c, err := rs.redis(); err == nil {
		_ = c.FlushAll()
	}
	lab.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}
