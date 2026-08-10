package main

// Профиль cache (m09). Путь чтения: проекция, которую строит подписчик, кэш
// перед базой и аренда распределённой блокировки. Общее у всех трёх — данные,
// живущие не там, где владелец, и потому всегда немного из прошлого.

import (
	"context"
	"fmt"
	"math"
	"net/url"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/kipyatl0/backend-architecture-lab/services/internal/lab"
)

// ── сцена 27 — проекция отстаёт от записи (m09 l01) ─────────────────────────
//
// Владелец данных подтвердил запись. Читают её не у владельца, а из проекции,
// которую строит подписчик, — и между этими двумя моментами лежит окно, в
// котором система отвечает про заказ старую правду. Окно не константа: под
// потоком оно растёт вместе с очередью подписчика.
func scene27(ctx context.Context, r *Run) error {
	cfg := r.Script.Config
	r.Sources = []string{r.OrdersURL}

	if err := lab.WaitKafka(ctx, r.Brokers, 90*time.Second); err != nil {
		return err
	}
	if err := lab.RecreateTopicRF(ctx, r.Brokers, lab.Topic, 1, 1, nil); err != nil {
		return err
	}
	cache, err := lab.DialRedisWait(r.RedisAddr, 8, 60*time.Second)
	if err != nil {
		return err
	}
	defer cache.Close()
	if err := cache.FlushAll(); err != nil {
		return err
	}
	if err := r.configureOrders(map[string]any{
		"reset": true, "write_mode": "direct", "target": "kafka",
	}); err != nil {
		return fmt.Errorf("заказы не настроены — поднят ли профиль cache? %w", err)
	}
	if err := r.prepareOrders(cfg.FirstOrderID); err != nil {
		return err
	}

	projectMS := cfg.ProjectMS
	if projectMS == 0 {
		projectMS = 700
	}

	// ── подписчик, который строит проекцию ─────────────────────────────────
	// Он не владелец данных: он читает события и раскладывает их так, как
	// удобно читателю. Применение стоит времени — ровно оно и превращается в
	// окно рассогласования.
	consumer, err := lab.DialKafkaGroup(r.Brokers, "projection", true)
	if err != nil {
		return err
	}
	defer consumer.Close()

	var mu sync.Mutex
	applied := map[int64]time.Time{}
	done := make(chan struct{})
	pctx, stopConsumer := context.WithCancel(ctx)
	defer stopConsumer()

	go func() {
		defer close(done)
		for {
			fetches := consumer.Client().PollFetches(pctx)
			if fetches.IsClientClosed() || pctx.Err() != nil {
				return
			}
			var recs []*kgo.Record
			fetches.EachRecord(func(rec *kgo.Record) { recs = append(recs, rec) })
			for _, rec := range recs {
				msg, err := lab.ParseMsg(rec.Value)
				if err != nil {
					continue
				}
				// Применение события — работа, а не мгновение: пересчитать
				// витрину, сходить в другую таблицу, положить в кэш.
				time.Sleep(time.Duration(projectMS) * time.Millisecond)
				status := "paid"
				_ = cache.SetPX("read:order:"+strconv.FormatInt(msg.Order, 10), status, time.Hour)
				mu.Lock()
				applied[msg.Order] = time.Now()
				mu.Unlock()
			}
		}
	}()

	readProjection := func(order int64) bool {
		_, ok, err := cache.Get("read:order:" + strconv.FormatInt(order, 10))
		return err == nil && ok
	}
	waitApplied := func(order int64, limit time.Duration) (time.Time, bool) {
		deadline := time.Now().Add(limit)
		for {
			mu.Lock()
			at, ok := applied[order]
			mu.Unlock()
			if ok {
				return at, true
			}
			if time.Now().After(deadline) {
				return time.Time{}, false
			}
			time.Sleep(50 * time.Millisecond)
		}
	}

	r.T0 = time.Now()

	// ── одна запись на спокойной системе ───────────────────────────────────
	ids, err := r.createOrders(1, cfg.Client, cfg.Restaurant, cfg.Amount)
	if err != nil {
		return err
	}
	order := ids[0]
	written := time.Now()
	r.Record("client.create", map[string]string{"order": strconv.FormatInt(order, 10)})

	r.SleepUntil(0.3)
	r.Record("read.stale", map[string]string{
		"answer": map[bool]string{true: "заказ есть", false: "нет такого заказа"}[readProjection(order)],
	})

	at, ok := waitApplied(order, 30*time.Second)
	if !ok {
		return fmt.Errorf("проекция не получила заказ %d", order)
	}
	quiet := at.Sub(written).Seconds()
	r.Record("proj.apply", map[string]string{"order": strconv.FormatInt(order, 10)})
	r.Record("read.fresh", map[string]string{
		"answer": map[bool]string{true: "заказ есть", false: "нет такого заказа"}[readProjection(order)],
	})

	// ── тот же вопрос под потоком ──────────────────────────────────────────
	burst := cfg.Burst
	if burst == 0 {
		burst = 10
	}
	r.SleepUntil(cfg.SecondPhaseAtS)
	burstStart := time.Now()
	batch, err := r.createOrders(burst, cfg.Client, cfg.Restaurant, cfg.Amount)
	if err != nil {
		return err
	}
	r.Record("burst.write", map[string]string{"n": strconv.Itoa(burst)})

	last := batch[len(batch)-1]
	lastAt, ok := waitApplied(last, 120*time.Second)
	if !ok {
		return fmt.Errorf("проекция не догнала поток из %d заказов", burst)
	}
	loaded := lastAt.Sub(burstStart).Seconds()
	r.Record("burst.apply", map[string]string{
		"order": strconv.FormatInt(last, 10),
		// Округляем вниз до секунды: доли дрожат от прогона к прогону, а
		// предмет наблюдения — порядок величины, а не миллисекунды.
		"secs": fmt.Sprintf("%.0f", math.Floor(loaded)),
	})

	stopConsumer()
	<-done

	r.Set("order", strconv.FormatInt(order, 10))
	r.Set("last_order", strconv.FormatInt(last, 10))
	r.Set("burst", strconv.Itoa(burst))
	r.Set("project_ms", strconv.Itoa(projectMS))
	_ = quiet // на спокойной системе окно меньше секунды — числом его не печатаем
	r.Set("loaded", fmt.Sprintf("%.0f", math.Floor(loaded)))
	return r.collect()
}

// ── сцена 28 — давка на промахе (m09 l04) ───────────────────────────────────
//
// Кэш перед базой держит популярное чтение. Пока значение живо, база отдыхает;
// в момент, когда оно протухло, вся толпа читателей уходит в базу разом. Второй
// прогон отличается одной настройкой: за всех идёт один.
func scene28(ctx context.Context, r *Run) error {
	cfg := r.Script.Config
	r.Sources = []string{r.OrdersURL}

	clients := cfg.Clients
	if clients == 0 {
		clients = 100
	}
	cardMS := cfg.CardMS
	if cardMS == 0 {
		cardMS = 200
	}
	ttlMS := cfg.CacheTTLMS
	if ttlMS == 0 {
		ttlMS = 1500
	}

	if err := r.configureOrders(map[string]any{
		"reset": true, "write_mode": "none",
		"card_ms": cardMS, "cache_ttl_ms": ttlMS, "single_flight": false,
	}); err != nil {
		return fmt.Errorf("заказы не настроены — поднят ли профиль cache? %w", err)
	}
	if err := r.prepareOrders(cfg.FirstOrderID); err != nil {
		return err
	}
	if _, err := r.createOrders(5, cfg.Client, cfg.Restaurant, cfg.Amount); err != nil {
		return err
	}

	type phase struct {
		key    string
		label  string
		single bool
	}
	phases := []phase{
		{"herd", "без защиты", false},
		{"single", "один поход за всех", true},
	}

	r.T0 = time.Now()
	for _, ph := range phases {
		if err := r.configureOrders(map[string]any{"single_flight": ph.single}); err != nil {
			return err
		}
		// Каждый прогон начинается с холодного кэша и обнулённых счётчиков.
		if err := r.postJSON(r.OrdersURL+"/_lab/read-reset", map[string]any{}, nil); err != nil {
			return err
		}

		// Значение кладётся в кэш и живёт положенное время. Давка случается не
		// на пустом кэше, а на истёкшем: ключ был горячим — тем и опасен.
		if _, err := r.card(ctx, cfg.Restaurant); err != nil {
			return err
		}
		time.Sleep(time.Duration(ttlMS+200) * time.Millisecond)

		lat := make([]float64, clients)
		var wg sync.WaitGroup
		start := time.Now()
		for i := 0; i < clients; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				t := time.Now()
				_, _ = r.card(ctx, cfg.Restaurant)
				lat[i] = float64(time.Since(t).Milliseconds())
			}(i)
		}
		wg.Wait()
		wall := float64(time.Since(start).Milliseconds())

		var stats struct {
			DBQueries int64 `json:"db_queries"`
			Hits      int64 `json:"hits"`
			Misses    int64 `json:"misses"`
		}
		if err := r.getJSON(r.OrdersURL+"/_lab/read-stats", &stats); err != nil {
			return err
		}
		sort.Float64s(lat)
		p50 := lat[len(lat)/2]
		p99 := lat[(len(lat)*99)/100]

		// Первый поход — прогрев, он есть в обоих прогонах: считаем то, что
		// вызвала именно толпа.
		r.Measure(ph.key, "queries", float64(stats.DBQueries-1))
		r.Measure(ph.key, "misses", float64(stats.Misses-1))
		r.Measure(ph.key, "p50", p50)
		r.Measure(ph.key, "p99", p99)
		r.Measure(ph.key, "wall", wall)

		r.Set(ph.key+"_queries", strconv.FormatInt(stats.DBQueries-1, 10))
		r.Set(ph.key+"_misses", strconv.FormatInt(stats.Misses-1, 10))
		r.Set(ph.key+"_hits", strconv.FormatInt(stats.Hits, 10))
	}

	r.Set("clients", strconv.Itoa(clients))
	r.Set("card_ms", strconv.Itoa(cardMS))
	r.Set("ttl", fmt.Sprintf("%.1f", float64(ttlMS)/1000))
	r.Set("restaurant", cfg.Restaurant)
	return nil
}

// Имя ресторана в пути — с кавычками и кириллицей: экранируем, иначе запрос
// до сервиса не доедет.
func urlEscape(s string) string { return url.PathEscape(s) }

// card — один читатель: запрос карточки ресторана через сервис.
func (r *Run) card(ctx context.Context, name string) (string, error) {
	var out map[string]any
	url := r.OrdersURL + "/restaurants/" + urlEscape(name)
	if err := r.getJSON(url, &out); err != nil {
		return "", err
	}
	return fmt.Sprint(out["restaurant"]), nil
}

// ── сцена 29 — аренда истекла, держателей двое (m09 l04) ────────────────────
//
// Распределённая блокировка — это аренда с истечением, а не замок. Держатель,
// который задержался дольше аренды, об этом не узнаёт: у него нет способа
// заметить, что его уже сменили. Второй прогон добавляет ограждающий номер, и
// решение переезжает туда, где ему место, — в хранилище.
func scene29(ctx context.Context, r *Run) error {
	// Участники сцены — два работника, кэш и база: приложение в ней не занято.
	r.Sources = nil
	cfg := r.Script.Config

	leaseMS := cfg.LeaseMS
	if leaseMS == 0 {
		leaseMS = 2000
	}
	stallMS := cfg.StallMS
	if stallMS == 0 {
		stallMS = 5000
	}
	order := cfg.FirstOrderID

	cache, err := lab.DialRedisWait(r.RedisAddr, 8, 60*time.Second)
	if err != nil {
		return err
	}
	defer cache.Close()
	if err := cache.FlushAll(); err != nil {
		return err
	}

	db, err := pgConnect(ctx, r.PrimaryDSN, 60*time.Second)
	if err != nil {
		return err
	}
	defer db.Close(ctx)
	if _, err := db.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS lab_assignments (
			order_id BIGINT PRIMARY KEY,
			courier  TEXT   NOT NULL,
			fence    BIGINT NOT NULL DEFAULT 0
		)`); err != nil {
		return err
	}
	if _, err := db.Exec(ctx, `TRUNCATE lab_assignments`); err != nil {
		return err
	}

	key := fmt.Sprintf("lock:order:%d", order)
	lease := time.Duration(leaseMS) * time.Millisecond

	// assign без ограждения: кто последний написал, тот и прав.
	assign := func(courier string) error {
		_, err := db.Exec(ctx, `
			INSERT INTO lab_assignments (order_id, courier) VALUES ($1, $2)
			ON CONFLICT (order_id) DO UPDATE SET courier = excluded.courier`, order, courier)
		return err
	}
	// assignFenced: хранилище само отвергает запись с устаревшим номером.
	assignFenced := func(courier string, token int64) (bool, error) {
		tag, err := db.Exec(ctx, `
			INSERT INTO lab_assignments (order_id, courier, fence) VALUES ($1, $2, $3)
			ON CONFLICT (order_id) DO UPDATE SET courier = excluded.courier, fence = excluded.fence
			WHERE lab_assignments.fence < excluded.fence`, order, courier, token)
		if err != nil {
			return false, err
		}
		return tag.RowsAffected() > 0, nil
	}
	courierOf := func() string {
		var c string
		if err := db.QueryRow(ctx, `SELECT courier FROM lab_assignments WHERE order_id = $1`,
			order).Scan(&c); err != nil {
			return "—"
		}
		return c
	}

	r.T0 = time.Now()

	// ── прогон 1: аренда без ограждающего номера ───────────────────────────
	ok, err := cache.SetNX(key, "worker-1", lease)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("первый работник не смог взять аренду")
	}
	r.Record("lease.take1", map[string]string{"ttl": fmt.Sprintf("%.0f", lease.Seconds())})

	// Второй работник приходит, пока аренда жива, — и уходит ни с чем.
	r.SleepUntil(0.5)
	busy, err := cache.SetNX(key, "worker-2", lease)
	if err != nil {
		return err
	}
	if busy {
		return fmt.Errorf("аренда оказалась свободной, хотя её держат")
	}
	r.Record("lease.busy", nil)

	// Аренда истекает сама — первый работник об этом не знает.
	r.SleepUntil(float64(leaseMS)/1000 + 0.3)
	took, err := cache.SetNX(key, "worker-2", lease)
	if err != nil {
		return err
	}
	if !took {
		return fmt.Errorf("аренда не освободилась после истечения")
	}
	r.Record("lease.take2", nil)

	r.SleepUntil(float64(leaseMS)/1000 + 0.8)
	if err := assign("Пётр"); err != nil {
		return err
	}
	r.Record("write.two", map[string]string{"courier": "Пётр"})

	// Первый работник просыпается и дописывает своё — он всё ещё считает
	// себя держателем.
	r.SleepUntil(float64(stallMS) / 1000)
	if err := assign("Айгуль"); err != nil {
		return err
	}
	r.Record("write.one", map[string]string{"courier": "Айгуль"})
	plain := courierOf()

	// ── прогон 2: то же самое с ограждающим номером ────────────────────────
	if err := cache.FlushAll(); err != nil {
		return err
	}
	if _, err := db.Exec(ctx, `TRUNCATE lab_assignments`); err != nil {
		return err
	}
	base := float64(stallMS)/1000 + 2

	r.SleepUntil(base)
	if _, err := cache.SetNX(key, "worker-1", lease); err != nil {
		return err
	}
	token1, err := cache.Incr("fence:order:" + strconv.FormatInt(order, 10))
	if err != nil {
		return err
	}
	r.Record("fence.take1", map[string]string{"token": strconv.FormatInt(token1, 10)})

	r.SleepUntil(base + float64(leaseMS)/1000 + 0.3)
	if _, err := cache.SetNX(key, "worker-2", lease); err != nil {
		return err
	}
	token2, err := cache.Incr("fence:order:" + strconv.FormatInt(order, 10))
	if err != nil {
		return err
	}
	r.Record("fence.take2", map[string]string{"token": strconv.FormatInt(token2, 10)})

	r.SleepUntil(base + float64(leaseMS)/1000 + 0.8)
	if _, err := assignFenced("Пётр", token2); err != nil {
		return err
	}
	r.Record("fence.write2", map[string]string{
		"courier": "Пётр", "token": strconv.FormatInt(token2, 10),
	})

	r.SleepUntil(base + float64(stallMS)/1000)
	wrote, err := assignFenced("Айгуль", token1)
	if err != nil {
		return err
	}
	r.Record("fence.write1", map[string]string{
		"courier": "Айгуль", "token": strconv.FormatInt(token1, 10),
		"result": map[bool]string{true: "записано", false: "отвергнуто хранилищем: 0 строк"}[wrote],
	})
	fenced := courierOf()

	r.Set("order", strconv.FormatInt(order, 10))
	r.Set("lease", fmt.Sprintf("%.0f", lease.Seconds()))
	r.Set("stall", fmt.Sprintf("%.0f", float64(stallMS)/1000))
	r.Set("plain", plain)
	r.Set("fenced", fenced)
	r.Set("token1", strconv.FormatInt(token1, 10))
	r.Set("token2", strconv.FormatInt(token2, 10))
	return r.collect()
}
