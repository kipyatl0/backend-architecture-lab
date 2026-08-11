package main

// Профиль scale (m10). Инстансов службы стало больше одного — и с этого момента
// перестают быть бесплатными три вещи, которые внутри одного процесса ничего не
// стоили: «у службы одна память», «запрос попадёт куда надо» и «данных ровно
// столько, сколько влезает в один узел».

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"
)

// ── общее: один читатель и один инстанс, который ему ответил ─────────────────

// cardAnswer — ответ службы на запрос карточки ресторана вместе с тем, кто
// именно ответил. Второе — не часть контракта службы, а заголовок: тело ответа
// у всех инстансов обязано быть одинаковым, и в этом вся соль сцены 30.
type cardAnswer struct {
	Instance string
	Source   string // cache | db
	Orders   int64  `json:"orders"`
	Total    int64  `json:"total"`
}

func (r *Run) cardFrom(client *http.Client, base, name string) (cardAnswer, error) {
	var a cardAnswer
	resp, err := client.Get(base + "/restaurants/" + urlEscape(name))
	if err != nil {
		return a, fmt.Errorf("%s: %w", base, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return a, fmt.Errorf("%s ответил %d: %s", base, resp.StatusCode, string(raw))
	}
	if err := json.Unmarshal(raw, &a); err != nil {
		return a, err
	}
	a.Instance = resp.Header.Get("X-Lab-Instance")
	a.Source = resp.Header.Get("X-Lab-Source")
	return a, nil
}

// instances — оба инстанса службы напрямую, мимо балансировщика. Настройка
// сцены раздаётся им обоим: они одинаковые, и сцена не имеет права делать их
// разными — иначе она показывала бы не то, что обещает.
func (r *Run) instances() []string { return []string{r.OrdersURL, r.Orders2URL} }

// resetBalancer поднимает балансировщик заново. Круг он раздаёт от своего
// внутреннего счётчика, и счётчик этот помнит прошлую сцену: без перезапуска
// один и тот же прогон начинался бы то с первого инстанса, то со второго, и
// вывод сцены нельзя было бы сверить с текстом шага.
func (r *Run) resetBalancer(ctx context.Context) error {
	if err := r.Docker.Restart(ctx, "balancer"); err != nil {
		return err
	}
	return r.Docker.WaitHealthy(ctx, "balancer", 60*time.Second)
}

func (r *Run) configureBoth(body map[string]any) error {
	for _, u := range r.instances() {
		if err := r.postJSON(u+"/_lab/config", body, nil); err != nil {
			return fmt.Errorf("инстанс %s не настроен — поднят ли профиль scale? %w", u, err)
		}
	}
	return nil
}

// ── сцена 30 — локальный кэш двух инстансов (m10 l01) ───────────────────────
//
// Служба stateless ровно до тех пор, пока в процессе ничего не лежит. Кэш
// карточки лежит — и с этого момента инстансы перестают быть взаимозаменяемыми:
// один и тот же запрос получает два разных ответа в зависимости от того, кому
// его отдал балансировщик.
func scene30(ctx context.Context, r *Run) error {
	cfg := r.Script.Config
	// Наблюдения собирает сама сцена: предмет — что увидел клиент снаружи, а не
	// что сервис записал у себя.
	r.Sources = nil

	ttl := cfg.CacheTTLMS
	if ttl == 0 {
		ttl = 3000
	}
	cardMS := cfg.CardMS
	if cardMS == 0 {
		cardMS = 200
	}

	if err := r.resetBalancer(ctx); err != nil {
		return err
	}
	if err := r.configureBoth(map[string]any{
		"reset": true, "write_mode": "none",
		"cache_mode": "local", "cache_ttl_ms": ttl, "card_ms": cardMS,
	}); err != nil {
		return err
	}
	if err := r.prepareOrders(cfg.FirstOrderID); err != nil {
		return err
	}
	if _, err := r.createOrders(5, cfg.Client, cfg.Restaurant, cfg.Amount); err != nil {
		return err
	}

	// Клиент один и тот же во всех запросах и ходит только через балансировщик:
	// какой инстанс ему ответит, он не выбирает и знать не может.
	client := &http.Client{Timeout: 30 * time.Second}

	read := func(key string, at float64) (cardAnswer, error) {
		r.SleepUntil(at)
		a, err := r.cardFrom(client, r.BalancerL7, cfg.Restaurant)
		if err != nil {
			return a, err
		}
		r.Record(key, map[string]string{
			"instance": a.Instance,
			"source":   map[string]string{"cache": "из своего кэша", "db": "из базы"}[a.Source],
			"orders":   strconv.FormatInt(a.Orders, 10),
			"total":    strconv.FormatInt(a.Total, 10),
		})
		return a, nil
	}

	r.T0 = time.Now()

	// ── оба инстанса согревают свой кэш ────────────────────────────────────
	// Согревают в разные моменты — потому что запросы приходят в разные
	// моменты. Дальше вся сцена вытекает из этой разницы.
	first, err := read("warm.1", 0.0)
	if err != nil {
		return err
	}
	if _, err := read("warm.2", 1.2); err != nil {
		return err
	}

	// ── данные изменились ──────────────────────────────────────────────────
	// Три новых заказа. Запись принял один инстанс — тот, к которому обратился
	// клиент; он же сбросил свою карточку. Второй об этом не узнал.
	r.SleepUntil(2.0)
	if _, err := r.createOrders(3, cfg.Client, cfg.Restaurant, cfg.Amount); err != nil {
		return err
	}
	r.Record("write", map[string]string{"n": "3"})

	after := []struct {
		key string
		at  float64
	}{
		{"read.1", 2.6}, {"read.2", 2.9},
		{"read.3", 3.6}, {"read.4", 3.9},
		{"read.5", 4.8}, {"read.6", 5.2},
	}
	stale, fresh := 0, 0
	var newOrders int64
	for _, s := range after {
		a, err := read(s.key, s.at)
		if err != nil {
			return err
		}
		if a.Orders > first.Orders {
			fresh++
			newOrders = a.Orders
		} else {
			stale++
		}
	}

	r.Set("restaurant", cfg.Restaurant)
	r.Set("ttl", fmt.Sprintf("%.0f", float64(ttl)/1000))
	r.Set("old_orders", strconv.FormatInt(first.Orders, 10))
	r.Set("new_orders", strconv.FormatInt(newOrders, 10))
	r.Set("stale", strconv.Itoa(stale))
	r.Set("fresh", strconv.Itoa(fresh))
	r.Set("after", strconv.Itoa(len(after)))
	return r.collect()
}

// ── сцена 31 — балансировка при долгих соединениях (m10 l02) ────────────────
//
// Один и тот же клиент, одни и те же четыре соединения, одни и те же сорок
// запросов. Разница между прогонами — только в том, на чём балансировщик
// принимает решение: на запросе или на соединении. Из неё и получается перекос.
func scene31(ctx context.Context, r *Run) error {
	cfg := r.Script.Config
	r.Sources = nil

	conns := cfg.Conns
	if conns == 0 {
		conns = 4
	}
	heavy := cfg.HeavyReqs
	if heavy == 0 {
		heavy = 25
	}
	light := cfg.LightReqs
	if light == 0 {
		light = 5
	}

	if err := r.resetBalancer(ctx); err != nil {
		return err
	}
	if err := r.configureBoth(map[string]any{"reset": true, "write_mode": "none"}); err != nil {
		return err
	}
	if err := r.prepareOrders(cfg.FirstOrderID); err != nil {
		return err
	}
	ids, err := r.createOrders(1, cfg.Client, cfg.Restaurant, cfg.Amount)
	if err != nil {
		return err
	}
	order := ids[0]

	// Одно соединение — один транспорт с потолком в одно соединение к хосту.
	// Запросы по нему идут по очереди, поэтому TCP-соединение ровно одно и то
	// же от первого запроса до последнего: именно такое соединение и держат
	// вебсокет, пул к соседнему сервису и длинный опрос.
	dialPinned := func() *http.Client {
		return &http.Client{
			Timeout: 30 * time.Second,
			Transport: &http.Transport{
				MaxConnsPerHost:     1,
				MaxIdleConnsPerHost: 1,
				IdleConnTimeout:     5 * time.Minute,
			},
		}
	}

	ask := func(c *http.Client, base string) (string, error) {
		resp, err := c.Get(base + "/orders/" + strconv.FormatInt(order, 10))
		if err != nil {
			return "", err
		}
		// Тело дочитывается до конца: иначе соединение не вернётся в пул, и
		// «одно долгое соединение» превратится в сорок коротких.
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		return resp.Header.Get("X-Lab-Instance"), nil
	}

	// run — один прогон: открыть соединения по порядку, прогнать по ним поток,
	// посчитать, кому что досталось.
	type result struct {
		perInstance map[string]int
		perConn     []string // куда попало каждое соединение (по первому запросу)
		connReqs    []int
		total       int
	}
	run := func(base string) (result, error) {
		res := result{perInstance: map[string]int{}}
		clients := make([]*http.Client, conns)
		// Соединения открываются по одному и по порядку: балансировщик
		// раскладывает их по кругу, и порядок — часть наблюдения.
		for i := 0; i < conns; i++ {
			clients[i] = dialPinned()
			who, err := ask(clients[i], base)
			if err != nil {
				return res, err
			}
			res.perConn = append(res.perConn, who)
		}

		reqs := make([]int, conns)
		for i := range reqs {
			reqs[i] = light
		}
		reqs[0] = heavy
		res.connReqs = reqs

		var mu sync.Mutex
		var wg sync.WaitGroup
		var runErr error
		for i := 0; i < conns; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				for k := 0; k < reqs[i]; k++ {
					who, err := ask(clients[i], base)
					mu.Lock()
					if err != nil && runErr == nil {
						runErr = err
					}
					if err == nil {
						res.perInstance[who]++
						res.total++
					}
					mu.Unlock()
				}
			}(i)
		}
		wg.Wait()
		for _, c := range clients {
			c.CloseIdleConnections()
		}
		return res, runErr
	}

	phases := []struct {
		key  string
		base string
	}{
		{"l7", r.BalancerL7},
		{"l4", r.BalancerL4},
	}
	for _, ph := range phases {
		res, err := run(ph.base)
		if err != nil {
			return fmt.Errorf("прогон %s: %w", ph.key, err)
		}
		one, two := res.perInstance["orders-1"], res.perInstance["orders-2"]
		r.Measure(ph.key, "reqs1", float64(one))
		r.Measure(ph.key, "reqs2", float64(two))
		r.Set(ph.key+"_reqs1", strconv.Itoa(one))
		r.Set(ph.key+"_reqs2", strconv.Itoa(two))
		r.Set(ph.key+"_total", strconv.Itoa(res.total))
		for i, who := range res.perConn {
			r.Set(fmt.Sprintf("%s_conn%d", ph.key, i+1), who)
			r.Set(fmt.Sprintf("%s_conn%d_reqs", ph.key, i+1), strconv.Itoa(res.connReqs[i]))
		}
	}

	r.Set("conns", strconv.Itoa(conns))
	r.Set("heavy", strconv.Itoa(heavy))
	r.Set("light", strconv.Itoa(light))
	r.Set("total", strconv.Itoa(heavy+light*(conns-1)))
	return nil
}

// ── сцена 32 — горячий ключ шардирования (m10 l04) ──────────────────────────
//
// Шардирование делит данные, а не нагрузку. Пока ключи приходят примерно
// поровну, разница незаметна; как только один ключ забирает большую долю
// потока, его шард работает за всех — и добавление узлов этого не лечит,
// потому что один ключ не делится.
func scene32(ctx context.Context, r *Run) error {
	cfg := r.Script.Config
	r.Sources = nil

	shardSets := cfg.Shards
	if len(shardSets) == 0 {
		shardSets = []int{4, 8}
	}
	stream := cfg.Stream
	if stream == 0 {
		stream = 200
	}
	share := cfg.HotShare
	if share == 0 {
		share = 0.6
	}
	costMS := cfg.ShardMS
	if costMS == 0 {
		costMS = 15
	}
	catalog := cfg.Catalog
	if len(catalog) == 0 {
		return fmt.Errorf("в сценарии не задан каталог ресторанов")
	}
	hot := cfg.Restaurant

	// По соединению на шард и одно сверху: работники пишут одновременно, и
	// очередь к общему соединению исказила бы ровно то, что сцена измеряет.
	maxShards := 0
	for _, s := range shardSets {
		if s > maxShards {
			maxShards = s
		}
	}
	db, err := pgPool(ctx, r.PrimaryDSN, maxShards+1, 60*time.Second)
	if err != nil {
		return err
	}
	defer db.Close()
	if _, err := db.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS lab_shards (
			id         BIGSERIAL PRIMARY KEY,
			shards     INT    NOT NULL,
			shard      INT    NOT NULL,
			restaurant TEXT   NOT NULL
		)`); err != nil {
		return err
	}

	// Поток заказов задан, а не разыгран: каждый пятый-шестой-седьмой заказ
	// уходит в самый популярный ресторан, остальные по очереди в остальные.
	// Ровно так выглядит любой каталог с одним крупным партнёром.
	cold := make([]string, 0, len(catalog))
	for _, name := range catalog {
		if name != hot {
			cold = append(cold, name)
		}
	}
	hotEvery := int(share * 10) // 0,6 → 6 из каждых 10
	orders := make([]string, stream)
	coldSeen := 0
	for i := range orders {
		if i%10 < hotEvery {
			orders[i] = hot
			continue
		}
		orders[i] = cold[coldSeen%len(cold)]
		coldSeen++
	}

	r.T0 = time.Now()

	for _, shards := range shardSets {
		if _, err := db.Exec(ctx, `DELETE FROM lab_shards WHERE shards = $1`, shards); err != nil {
			return err
		}

		// Каждый шард — своя очередь и свой работник: у узла свои ресурсы, и
		// чужой работой он не помогает. Это и есть то, что измеряет сцена.
		queues := make([][]string, shards)
		for _, name := range orders {
			queues[shardOf(name, shards)] = append(queues[shardOf(name, shards)], name)
		}

		drained := make([]float64, shards)
		var wg sync.WaitGroup
		start := time.Now()
		var mu sync.Mutex
		for s := 0; s < shards; s++ {
			wg.Add(1)
			go func(s int) {
				defer wg.Done()
				for _, name := range queues[s] {
					// Запись стоит времени. Сколько именно — задаёт сцена, как
					// она задаёт задержку эквайринга: предмет здесь не скорость
					// базы, а то, что работа шарда делится только на него.
					time.Sleep(time.Duration(costMS) * time.Millisecond)
					if _, err := db.Exec(ctx,
						`INSERT INTO lab_shards (shards, shard, restaurant) VALUES ($1,$2,$3)`,
						shards, s, name); err != nil {
						return
					}
				}
				mu.Lock()
				drained[s] = time.Since(start).Seconds()
				mu.Unlock()
			}(s)
		}
		wg.Wait()
		wall := time.Since(start).Seconds()

		rows, err := db.Query(ctx, `
			SELECT shard, count(*), count(DISTINCT restaurant)
			  FROM lab_shards WHERE shards = $1 GROUP BY shard`, shards)
		if err != nil {
			return err
		}
		counts := map[int]int{}
		keys := map[int]int{}
		for rows.Next() {
			var s, n, k int
			if err := rows.Scan(&s, &n, &k); err != nil {
				rows.Close()
				return err
			}
			counts[s], keys[s] = n, k
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}

		busiest, quietest := 0, stream+1
		for s := 0; s < shards; s++ {
			key := fmt.Sprintf("s%d_%d", shards, s)
			n := counts[s]
			if n > busiest {
				busiest = n
			}
			if n < quietest {
				quietest = n
			}
			r.Set(key+"_keys", strconv.Itoa(keys[s]))
			r.Set(key+"_records", strconv.Itoa(n))
			r.Set(key+"_share", fmt.Sprintf("%.0f%%", 100*float64(n)/float64(stream)))
			// Числа записей наблюдаемые и печатаются как есть; времена —
			// сценарные, наблюдение только сверяется с ними по допуску.
			r.Measure(key, "records", float64(n))
			r.Measure(key, "drained", drained[s])
			r.Measure(key, "idle", wall-drained[s])
		}
		r.Set(fmt.Sprintf("s%d_busiest", shards), strconv.Itoa(busiest))
		r.Set(fmt.Sprintf("s%d_quietest", shards), strconv.Itoa(quietest))
	}

	if _, err := db.Exec(ctx, `DROP TABLE lab_shards`); err != nil {
		return err
	}

	r.Set("hot", hot)
	r.Set("stream", strconv.Itoa(stream))
	r.Set("cost", strconv.Itoa(costMS))
	r.Set("hot_share", fmt.Sprintf("%.0f%%", share*100))
	return nil
}

// shardOf — номер шарда по ключу. Обычное деление хэша с остатком: именно так
// выглядит шардирование до того, как в него добавляют консистентное
// хэширование, и именно на нём видно, что ключ не делится.
func shardOf(key string, shards int) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() % uint32(shards))
}
