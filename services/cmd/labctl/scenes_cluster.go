package main

// Профиль cluster (m08). Одна и та же служба, но данные её лежат больше чем на
// одном узле — и все допущения, которые до сих пор были бесплатны, кончаются:
// «записал — значит вижу», «подтвердили — значит навсегда», «главный один».
//
// Ломаются здесь не процессы приложения, а узлы: у брокера и у базы нет ручки
// «умри», и сцена останавливает их снаружи (см. internal/lab/docker.go).

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/kipyatl0/backend-architecture-lab/services/internal/lab"
)

const (
	svcReplica = "postgres-replica"
	svcPrimary = "postgres"
)

// ── общая обвязка ───────────────────────────────────────────────────────────

// rebuildReplica пересобирает вторую копию базы. Нужна не для красоты: сцена
// m08 l03 промотирует реплику в первичный узел, и вернуть её в строй можно
// только базовой копией с нуля. Данные реплики живут в памяти, поэтому
// перезапуск контейнера — это и есть пересборка.
func (r *Run) rebuildReplica(ctx context.Context) error {
	if err := r.Docker.Restart(ctx, svcReplica); err != nil {
		return fmt.Errorf("вторая копия базы не пересобрана: %w", err)
	}
	if err := r.Docker.WaitHealthy(ctx, svcReplica, 120*time.Second); err != nil {
		return err
	}
	// Ждём не запуска, а потока: копия, которая ещё не подключилась к
	// первичному узлу, показала бы сцене вчерашние данные.
	primary, err := pgConnect(ctx, r.PrimaryDSN, 60*time.Second)
	if err != nil {
		return err
	}
	defer primary.Close(ctx)

	deadline := time.Now().Add(90 * time.Second)
	for {
		ok, err := streaming(ctx, primary)
		if err == nil && ok {
			break
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("вторая копия базы не подключилась к первичному узлу")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
	// У сервиса заказов остались соединения к прежнему контейнеру копии —
	// их надо закрыть, иначе первое же чтение ответит ошибкой связи.
	return r.postJSON(r.OrdersURL+"/_lab/read-reset", map[string]any{}, nil)
}

// readOrder спрашивает у сервиса заказ. Ответ «нет такого заказа» — законное
// наблюдение сцены, а не ошибка: в этом вся m08 l01.
func (r *Run) readOrder(id int64) (bool, string, error) {
	resp, err := r.ctl.Get(fmt.Sprintf("%s/orders/%d", r.OrdersURL, id))
	if err != nil {
		return false, "", err
	}
	defer resp.Body.Close()
	var body struct {
		Found  bool   `json:"found"`
		Source string `json:"source"`
	}
	if err := decodeJSON(resp, &body); err != nil {
		return false, "", err
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNotFound {
		return false, "", fmt.Errorf("сервис ответил %d", resp.StatusCode)
	}
	return body.Found, body.Source, nil
}

// nodeNames переводит номера узлов в имена, которые студент видит в выводе.
func nodeNames(hosts map[int32]string, ids []int32) string {
	sorted := append([]int32(nil), ids...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	var out []string
	for _, id := range sorted {
		if h, ok := hosts[id]; ok {
			out = append(out, h)
		} else {
			out = append(out, "узел "+strconv.FormatInt(int64(id), 10))
		}
	}
	return strings.Join(out, ", ")
}

// clusterTopic пересоздаёт тему с заданным числом копий. Как и в m06: каждая
// сцена начинается с чистого лога — иначе число записей в выводе окажется
// суммой этого прогона и предыдущего.
func (r *Run) clusterTopic(ctx context.Context, rf, minISR int) error {
	if err := lab.RecreateTopicRF(ctx, r.Brokers, lab.Topic, 1, int16(rf),
		map[string]string{"min.insync.replicas": strconv.Itoa(minISR)}); err != nil {
		return err
	}
	// Копии кладём на узлы явно: иначе кластер выбирает их сам и каждый раз
	// по-своему, а в выводе сцены стоят имена узлов.
	replicas := make([]int32, 0, rf)
	for i := 1; i <= rf; i++ {
		replicas = append(replicas, int32(i))
	}
	return lab.PlaceReplicas(ctx, r.Brokers, lab.Topic, 0, replicas)
}

func paidEvent(order int64, amount int64, client string) lab.Msg {
	return lab.Msg{
		Type: "order.paid", Order: order, Amount: amount, Client: client, Seq: 1,
		ID: fmt.Sprintf("evt_%d", order),
	}
}

// ── сцена 22 — записал и не вижу (m08 l01) ──────────────────────────────────
//
// Чтение уехало на вторую копию — самый дешёвый способ разгрузить первичный
// узел, и самый частый. Вместе с разгрузкой приезжает окно, в котором система
// отвечает «такого заказа нет» про заказ, который сама же и подтвердила.
func scene22(ctx context.Context, r *Run) error {
	cfg := r.Script.Config
	r.Sources = []string{r.OrdersURL}

	if err := r.rebuildReplica(ctx); err != nil {
		return err
	}
	if err := r.configureOrders(map[string]any{
		"reset": true, "write_mode": "none", "read_from": "primary",
	}); err != nil {
		return fmt.Errorf("заказы не настроены — поднят ли профиль cluster? %w", err)
	}
	if err := r.prepareOrders(cfg.FirstOrderID); err != nil {
		return err
	}

	replica, err := pgConnect(ctx, r.ReplicaDSN, 60*time.Second)
	if err != nil {
		return err
	}
	defer replica.Close(ctx)
	primary, err := pgConnect(ctx, r.PrimaryDSN, 60*time.Second)
	if err != nil {
		return err
	}
	defer primary.Close(ctx)
	// Сцена начинается только тогда, когда копия догнала подготовку: иначе
	// первое же чтение упрётся в блокировку от TRUNCATE, а не в отставание.
	if err := waitCaughtUp(ctx, primary, replica, 60*time.Second); err != nil {
		return err
	}

	r.T0 = time.Now()

	// Заказ создаётся как обычно: запись всегда идёт в первичный узел.
	ids, err := r.createOrders(1, cfg.Client, cfg.Restaurant, cfg.Amount)
	if err != nil {
		return err
	}
	order := ids[0]
	r.Record("client.create", map[string]string{"order": strconv.FormatInt(order, 10)})

	// А чтение сервис берёт со второй копии.
	if err := r.configureOrders(map[string]any{"read_from": "replica"}); err != nil {
		return err
	}

	reads := cfg.ReadAtS
	if len(reads) == 0 {
		reads = []float64{0.5, 1.5, 3.0}
	}
	firstSeen := -1.0
	for i, at := range reads {
		r.SleepUntil(at)
		found, _, err := r.readOrder(order)
		if err != nil {
			return err
		}
		answer := "404 нет такого заказа"
		if found {
			answer = "200 OK: заказ на месте"
			if firstSeen < 0 {
				firstSeen = time.Since(r.T0).Seconds()
			}
		}
		lag, _ := replicaLag(ctx, replica)
		r.Record(fmt.Sprintf("read.%d", i+1), map[string]string{
			"answer": answer,
			// Отставание округляем вниз до секунды: доли секунды в этом числе
			// дрожат от прогона к прогону, а вывод сцены сверяется побуквенно.
			"lag": fmt.Sprintf("%.0f", math.Floor(lag)),
		})
	}

	// Тот же вопрос, заданный первичному узлу: разница не в коде сервиса, а
	// в том, к какой копии он подключён.
	r.SleepUntil(reads[len(reads)-1] + 1.5)
	if err := r.configureOrders(map[string]any{"read_from": "primary"}); err != nil {
		return err
	}
	second, err := r.createOrders(1, cfg.Client, "Плов-хаус", cfg.Amount)
	if err != nil {
		return err
	}
	r.Record("client.create2", map[string]string{"order": strconv.FormatInt(second[0], 10)})
	found, _, err := r.readOrder(second[0])
	if err != nil {
		return err
	}
	r.Record("read.primary", map[string]string{
		"answer": map[bool]string{true: "200 OK: заказ на месте", false: "404 нет такого заказа"}[found],
	})

	window := firstSeen
	if window < 0 {
		window = 0
	}
	r.Set("order", strconv.FormatInt(order, 10))
	r.Set("second_order", strconv.FormatInt(second[0], 10))
	r.Set("window", fmt.Sprintf("%.0f", math.Floor(window)))
	// Задержка применения задана профилем стенда, а не сценой: это свойство
	// второй копии, и в выводе оно названо отдельно от наблюдения.
	r.Set("delay", "2")
	r.Set("client", cfg.Client)
	r.Set("amount", strconv.FormatInt(cfg.Amount, 10))
	return r.collect()
}

// ── сцена 23 — копий стало мало (m08 l02) ───────────────────────────────────
//
// Тема живёт в двух копиях, и от них требуют быть в синхроне обеим. Один узел
// уходит — и кластер, оставаясь живым, отказывается принимать запись. Это не
// поломка: это выбор, сделанный настройкой, и сцена показывает его цену с двух
// сторон сразу.
func scene23(ctx context.Context, r *Run) error {
	// Сцена смотрит на кластер, а не на приложение: собственных наблюдений
	// сервисов в ней нет.
	r.Sources = nil
	cfg := r.Script.Config
	if err := lab.WaitKafka(ctx, r.Brokers, 90*time.Second); err != nil {
		return err
	}
	rf, minISR := cfg.RF, cfg.MinISR
	if rf == 0 {
		rf, minISR = 2, 2
	}
	if err := r.clusterTopic(ctx, rf, minISR); err != nil {
		return err
	}
	hosts, err := lab.BrokerHosts(ctx, r.Brokers)
	if err != nil {
		return err
	}
	states, err := lab.TopicState(ctx, r.Brokers, lab.Topic)
	if err != nil {
		return err
	}
	st := states[0]
	var follower int32 = -1
	for _, id := range st.Replicas {
		if id != st.Leader {
			follower = id
			break
		}
	}
	if follower < 0 {
		return fmt.Errorf("у партиции не нашлось второй копии: %v", st.Replicas)
	}
	r.Set("leader", hosts[st.Leader])
	r.Set("follower", hosts[follower])
	r.Set("replicas", nodeNames(hosts, st.Replicas))
	r.Set("min_isr", strconv.Itoa(minISR))
	r.Set("rf", strconv.Itoa(rf))

	all, err := lab.NewWriter(r.Brokers, -1)
	if err != nil {
		return err
	}
	defer all.Close()
	leaderOnly, err := lab.NewWriter(r.Brokers, 1)
	if err != nil {
		return err
	}
	defer leaderOnly.Close()
	// Знакомство с кластером — не часть сцены: греем оба отправителя до T0.
	if err := all.Warm(ctx); err != nil {
		return err
	}
	if err := leaderOnly.Warm(ctx); err != nil {
		return err
	}

	order := cfg.FirstOrderID
	r.T0 = time.Now()

	// Пока копий достаточно, запись с самым строгим требованием проходит.
	if err := all.Write(ctx, lab.Topic, 0, paidEvent(order, cfg.Amount, cfg.Client)); err != nil {
		return fmt.Errorf("первая запись не прошла: %w", err)
	}
	r.Record("write.ok1", map[string]string{"order": strconv.FormatInt(order, 10)})

	// Узел уходит штатно — так, как уходит выключенная машина.
	r.SleepUntil(2)
	if err := r.Docker.Stop(ctx, hosts[follower]); err != nil {
		return err
	}
	if err := r.Docker.WaitStopped(ctx, hosts[follower], 60*time.Second); err != nil {
		return err
	}
	r.Record("node.stop", nil)

	shrunk, err := lab.WaitISR(ctx, r.Brokers, lab.Topic, 0, 1, 60*time.Second)
	if err != nil {
		return err
	}
	r.Record("isr.shrink", map[string]string{"isr": nodeNames(hosts, shrunk.ISR)})

	// Требование «две копии из двух» больше не выполнимо — и кластер говорит
	// об этом прямо, а не принимает запись молча.
	err = all.Write(ctx, lab.Topic, 0, paidEvent(order+1, cfg.Amount, cfg.Client))
	if err == nil {
		return fmt.Errorf("запись с acks=all прошла, хотя синхронная копия одна")
	}
	r.Record("write.fail", map[string]string{
		"order": strconv.FormatInt(order+1, 10),
		"err":   lab.ErrName(err),
	})

	// Понизить требование можно в одну строчку. Именно так теряют данные.
	if err := leaderOnly.Write(ctx, lab.Topic, 0, paidEvent(order+2, cfg.Amount, cfg.Client)); err != nil {
		return fmt.Errorf("запись с acks=1 не прошла: %w", err)
	}
	r.Record("write.acks1", map[string]string{"order": strconv.FormatInt(order+2, 10)})

	// Узел возвращается.
	if err := r.Docker.Start(ctx, hosts[follower]); err != nil {
		return err
	}
	r.Record("node.start", nil)
	if err := r.Docker.WaitHealthy(ctx, hosts[follower], 180*time.Second); err != nil {
		return err
	}
	back, err := lab.WaitISR(ctx, r.Brokers, lab.Topic, 0, 2, 120*time.Second)
	if err != nil {
		return err
	}
	r.Record("isr.restore", map[string]string{"isr": nodeNames(hosts, back.ISR)})

	if err := all.Write(ctx, lab.Topic, 0, paidEvent(order+3, cfg.Amount, cfg.Client)); err != nil {
		return fmt.Errorf("запись после возвращения узла не прошла: %w", err)
	}
	r.Record("write.ok2", map[string]string{"order": strconv.FormatInt(order+3, 10)})

	total, err := lab.CountRecords(ctx, r.Brokers, lab.Topic)
	if err != nil {
		return err
	}
	r.Set("total", strconv.FormatInt(total, 10))
	r.Set("first_order", strconv.FormatInt(order, 10))
	r.Set("failed_order", strconv.FormatInt(order+1, 10))
	r.Set("acks1_order", strconv.FormatInt(order+2, 10))
	r.Set("last_order", strconv.FormatInt(order+3, 10))
	return r.collect()
}

// ── сцена 24 — лидер выбран из отставшей копии (m08 l02) ────────────────────
//
// Продолжение предыдущей сцены с другой стороны: там понизили требование и
// запись прошла, здесь видно, чем она была оплачена. Подтверждённые записи
// исчезают — не из-за поломки диска, а потому, что оператор выбрал
// доступность, когда единственная копия с данными не отвечала.
func scene24(ctx context.Context, r *Run) error {
	// Сцена смотрит на кластер, а не на приложение: собственных наблюдений
	// сервисов в ней нет.
	r.Sources = nil
	cfg := r.Script.Config
	if err := lab.WaitKafka(ctx, r.Brokers, 90*time.Second); err != nil {
		return err
	}
	// Одна синхронная копия из двух: «нам была важна доступность записи».
	if err := r.clusterTopic(ctx, 2, 1); err != nil {
		return err
	}
	hosts, err := lab.BrokerHosts(ctx, r.Brokers)
	if err != nil {
		return err
	}
	states, err := lab.TopicState(ctx, r.Brokers, lab.Topic)
	if err != nil {
		return err
	}
	st := states[0]
	leader := st.Leader
	var follower int32 = -1
	for _, id := range st.Replicas {
		if id != leader {
			follower = id
			break
		}
	}
	r.Set("leader", hosts[leader])
	r.Set("follower", hosts[follower])
	r.Set("replicas", nodeNames(hosts, st.Replicas))

	w, err := lab.NewWriter(r.Brokers, -1)
	if err != nil {
		return err
	}
	defer w.Close()
	if err := w.Warm(ctx); err != nil {
		return err
	}

	order := cfg.FirstOrderID
	safe := cfg.PreOrders // сколько записей увидели обе копии
	if safe == 0 {
		safe = 2
	}
	risky := cfg.Events // сколько записано, пока копия была одна
	if risky == 0 {
		risky = 3
	}

	r.T0 = time.Now()
	for i := 0; i < safe; i++ {
		if err := w.Write(ctx, lab.Topic, 0, paidEvent(order+int64(i), cfg.Amount, cfg.Client)); err != nil {
			return err
		}
	}
	r.Record("write.safe", map[string]string{"n": strconv.Itoa(safe)})

	// Вторая копия уходит. Синхронный набор ужимается до одного узла — и
	// требование «одна копия» по-прежнему выполняется.
	r.SleepUntil(2)
	if err := r.Docker.Stop(ctx, hosts[follower]); err != nil {
		return err
	}
	if err := r.Docker.WaitStopped(ctx, hosts[follower], 60*time.Second); err != nil {
		return err
	}
	shrunk, err := lab.WaitISR(ctx, r.Brokers, lab.Topic, 0, 1, 60*time.Second)
	if err != nil {
		return err
	}
	r.Record("node.stop", map[string]string{"isr": nodeNames(hosts, shrunk.ISR)})

	for i := 0; i < risky; i++ {
		e := paidEvent(order+int64(safe+i), cfg.Amount, cfg.Client)
		if err := w.Write(ctx, lab.Topic, 0, e); err != nil {
			return fmt.Errorf("запись при одной копии не прошла: %w", err)
		}
	}
	total, err := lab.CountRecords(ctx, r.Brokers, lab.Topic)
	if err != nil {
		return err
	}
	r.Record("write.risky", map[string]string{
		"n": strconv.Itoa(risky), "total": strconv.FormatInt(total, 10),
	})

	// Единственный узел с этими записями умирает без прощания.
	if err := r.Docker.Kill(ctx, hosts[leader]); err != nil {
		return err
	}
	if err := r.Docker.WaitStopped(ctx, hosts[leader], 60*time.Second); err != nil {
		return err
	}
	r.Record("leader.kill", nil)

	// Отставшая копия возвращается — но лидером её не назначают: кластер знает,
	// что она видела не всё.
	if err := r.Docker.Start(ctx, hosts[follower]); err != nil {
		return err
	}
	if err := r.Docker.WaitHealthy(ctx, hosts[follower], 180*time.Second); err != nil {
		return err
	}
	none, err := lab.WaitLeader(ctx, r.Brokers, lab.Topic, 0,
		func(p lab.PartState) bool { return p.Leader < 0 }, 90*time.Second)
	if err != nil {
		return err
	}
	r.Record("no.leader", map[string]string{"isr": nodeNames(hosts, none.ISR)})

	// Команда оператора: назначить лидером живую копию, какой бы она ни была.
	if err := lab.ElectUnclean(ctx, r.Brokers, lab.Topic, 0); err != nil {
		return err
	}
	elected, err := lab.WaitLeader(ctx, r.Brokers, lab.Topic, 0,
		func(p lab.PartState) bool { return p.Leader == follower }, 90*time.Second)
	if err != nil {
		return err
	}
	r.Record("elect.unclean", map[string]string{"leader": hosts[elected.Leader]})

	left, err := lab.CountRecords(ctx, r.Brokers, lab.Topic)
	if err != nil {
		return err
	}
	r.Record("read.after", map[string]string{"left": strconv.FormatInt(left, 10)})

	r.Set("safe", strconv.Itoa(safe))
	r.Set("risky", strconv.Itoa(risky))
	r.Set("acked", strconv.FormatInt(total, 10))
	r.Set("left", strconv.FormatInt(left, 10))
	r.Set("lost", strconv.FormatInt(total-left, 10))
	return r.collect()
}

// ── сцена 25 — разрыв сети пополам (m08 l03) ────────────────────────────────
//
// Две копии базы и сторож, который поднимает вторую, когда не видит первую.
// Ровно так выглядит самодельное переключение — и ровно так появляются два
// узла, каждый из которых уверен, что главный он.
func scene25(ctx context.Context, r *Run) error {
	cfg := r.Script.Config
	r.Sources = []string{r.OrdersURL}

	if err := r.rebuildReplica(ctx); err != nil {
		return err
	}
	if err := r.configureOrders(map[string]any{
		"reset": true, "write_mode": "none", "read_from": "primary",
	}); err != nil {
		return err
	}
	if err := r.prepareOrders(cfg.FirstOrderID); err != nil {
		return err
	}

	primary, err := pgConnect(ctx, r.PrimaryDSN, 60*time.Second)
	if err != nil {
		return err
	}
	defer primary.Close(ctx)
	replica, err := pgConnect(ctx, r.ReplicaDSN, 60*time.Second)
	if err != nil {
		return err
	}
	defer replica.Close(ctx)

	r.T0 = time.Now()

	ids, err := r.createOrders(1, cfg.Client, cfg.Restaurant, cfg.Amount)
	if err != nil {
		return err
	}
	order := ids[0]
	r.Record("client.create", map[string]string{"order": strconv.FormatInt(order, 10)})

	// Копия догоняет — до разрыва обе стороны видят одно и то же.
	deadline := time.Now().Add(30 * time.Second)
	for {
		_, ok, err := orderStatus(ctx, replica, order)
		if err == nil && ok {
			break
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("копия не получила заказ %d до разрыва", order)
		}
		time.Sleep(200 * time.Millisecond)
	}
	r.Record("replica.sync", map[string]string{"order": strconv.FormatInt(order, 10)})

	// ── сеть рвётся пополам ────────────────────────────────────────────────
	// Копия больше не может дотянуться до первичного узла, и первичный узел
	// не видит копии. Обе половины при этом живы и обслуживают своих клиентов.
	r.SleepUntil(cfg.SecondPhaseAtS)
	if _, err := replica.Exec(ctx,
		`ALTER SYSTEM SET primary_conninfo = 'host=10.255.255.1 port=5432 connect_timeout=1'`); err != nil {
		return err
	}
	if _, err := replica.Exec(ctx, `SELECT pg_reload_conf()`); err != nil {
		return err
	}
	if _, err := primary.Exec(ctx,
		`SELECT pg_terminate_backend(pid) FROM pg_stat_replication`); err != nil {
		return err
	}
	r.Record("net.split", nil)

	// ── сторож делает то, ради чего его поставили ──────────────────────────
	r.SleepUntil(cfg.BulkAtS)
	var promoted bool
	if err := replica.QueryRow(ctx, `SELECT pg_promote(true, 60)`).Scan(&promoted); err != nil {
		return err
	}
	if !promoted {
		return fmt.Errorf("копия не стала первичным узлом")
	}
	recovering, err := inRecovery(ctx, replica)
	if err != nil {
		return err
	}
	if recovering {
		return fmt.Errorf("копия осталась копией после promote")
	}
	r.Record("watchdog.promote", nil)

	// ── обе половины принимают запись ──────────────────────────────────────
	r.SleepUntil(cfg.DeleteAtS)
	if _, err := replica.Exec(ctx,
		`UPDATE orders SET status = 'cancelled' WHERE id = $1`, order); err != nil {
		return err
	}
	r.Record("client.b", map[string]string{"order": strconv.FormatInt(order, 10)})

	r.SleepUntil(cfg.DeleteAtS + 2)
	if _, err := primary.Exec(ctx,
		`UPDATE orders SET status = 'paid', charge = 'ch_1' WHERE id = $1`, order); err != nil {
		return err
	}
	r.Record("client.a", map[string]string{"order": strconv.FormatInt(order, 10)})

	// ── сеть чинят ─────────────────────────────────────────────────────────
	r.SleepUntil(cfg.DeleteAtS + 5)
	left, _, err := orderStatus(ctx, primary, order)
	if err != nil {
		return err
	}
	right, _, err := orderStatus(ctx, replica, order)
	if err != nil {
		return err
	}
	r.Record("net.heal", map[string]string{"left": left, "right": right})

	r.Set("order", strconv.FormatInt(order, 10))
	r.Set("left", left)
	r.Set("right", right)
	r.Set("client", cfg.Client)
	return r.collect()
}

// ── сцена 26 — часы двух узлов разъехались (m08 l04) ────────────────────────
//
// Два узла службы пишут историю одного заказа, и каждый ставит своё время.
// Часы одного отстали на несколько секунд — этого достаточно, чтобы
// упорядоченная по времени история читалась как «курьера назначили до оплаты».
func scene26(ctx context.Context, r *Run) error {
	// Сцена смотрит на кластер, а не на приложение: собственных наблюдений
	// сервисов в ней нет.
	r.Sources = nil
	cfg := r.Script.Config
	if err := lab.WaitKafka(ctx, r.Brokers, 90*time.Second); err != nil {
		return err
	}
	if err := r.clusterTopic(ctx, 3, 2); err != nil {
		return err
	}
	w, err := lab.NewWriter(r.Brokers, -1)
	if err != nil {
		return err
	}
	defer w.Close()
	if err := w.Warm(ctx); err != nil {
		return err
	}

	skew := time.Duration(cfg.SkewMS) * time.Millisecond
	if skew == 0 {
		skew = -3 * time.Second
	}
	order := cfg.FirstOrderID

	type step struct {
		key    string
		node   string
		typ    string
		seq    int
		at     float64
		skewed bool
	}
	steps := []step{
		{"node.paid", "node-a", "order.paid", 1, 0.0, false},
		{"node.assigned", "node-b", "order.assigned", 2, 0.6, true},
		{"node.cancelled", "node-a", "order.cancelled", 3, 1.2, false},
	}

	r.T0 = time.Now()
	stamps := map[string]time.Time{}
	for _, s := range steps {
		r.SleepUntil(s.at)
		e := lab.Msg{
			Type: s.typ, Order: order, Amount: cfg.Amount, Client: cfg.Client,
			Seq: s.seq, ID: fmt.Sprintf("evt_%d", order),
		}
		if s.typ == "order.assigned" {
			e.Courier = "Пётр"
		}
		if s.typ == "order.cancelled" {
			e.Reason = "клиент передумал"
		}
		// Время ставит тот, кто записывает. У второго узла оно отстаёт —
		// не потому, что он медленный, а потому, что у него такие часы.
		// Метка берётся от расписания сцены, а не от часов исполнителя: в
		// записи она окажется настоящей (её прочитает аналитик оттуда же),
		// но не будет дрожать на десятые доли от прогона к прогону.
		ts := r.T0.Add(time.Duration(s.at * float64(time.Second)))
		if s.skewed {
			ts = ts.Add(skew)
		}
		if err := w.WriteAt(ctx, lab.Topic, 0, e, ts); err != nil {
			return err
		}
		stamps[s.typ] = ts
		r.Record(s.key, map[string]string{
			"node": s.node, "type": s.typ, "ts": stamp(ts, r.T0),
		})
	}

	_ = stamps

	// Аналитик читает лог и раскладывает историю двумя способами.
	raws, err := lab.ReadRaw(ctx, r.Brokers, lab.Topic, 2*time.Second)
	if err != nil {
		return err
	}
	if len(raws) != len(steps) {
		return fmt.Errorf("в логе оказалось %d записей вместо %d", len(raws), len(steps))
	}
	r.Record("analyst.read", map[string]string{"n": strconv.Itoa(len(raws))})

	type rec struct {
		typ    string
		seq    int
		offset int64
		at     time.Time
	}
	var recs []rec
	for _, raw := range raws {
		m, err := lab.ParseMsg(raw.Value)
		if err != nil {
			return err
		}
		recs = append(recs, rec{typ: m.Type, seq: m.Seq, offset: raw.Offset, at: raw.At})
	}

	byTime := append([]rec(nil), recs...)
	sort.SliceStable(byTime, func(i, j int) bool { return byTime[i].at.Before(byTime[j].at) })
	byOffset := append([]rec(nil), recs...)
	sort.SliceStable(byOffset, func(i, j int) bool { return byOffset[i].offset < byOffset[j].offset })

	line := func(rs []rec, showTime bool) string {
		var out []string
		for _, x := range rs {
			if showTime {
				out = append(out, fmt.Sprintf("%s (%s)", x.typ, stamp(x.at, r.T0)))
			} else {
				out = append(out, fmt.Sprintf("%s (#%d)", x.typ, x.seq))
			}
		}
		return strings.Join(out, " → ")
	}

	r.Set("order", strconv.FormatInt(order, 10))
	r.Set("skew", fmt.Sprintf("%.0f", -skew.Seconds()))
	r.Set("by_time", line(byTime, true))
	r.Set("by_offset", line(byOffset, false))
	r.Set("wrong", strconv.FormatBool(byTime[0].seq != 1))
	return r.collect()
}

// stamp — время по часам того, кто его поставил, от начала сцены. Настенное
// время печатать нельзя: оно разное в каждом прогоне, а вывод сверяется буква
// в букву. Разъехавшиеся часы при этом видны ровно так же — по знаку.
func stamp(t, t0 time.Time) string {
	v := t.Sub(t0).Seconds()
	if math.Abs(v) < 0.05 { // «минус ноль» в выводе — мусор
		v = 0
	}
	return fmt.Sprintf("%+.1f с", v)
}

// decodeJSON — разбор ответа, у которого важен и код, и тело. Обычный getJSON
// на 404 возвращает ошибку, а здесь 404 — это и есть ответ сцены.
func decodeJSON(resp *http.Response, out any) error {
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if len(raw) == 0 {
		return nil
	}
	return json.Unmarshal(raw, out)
}
