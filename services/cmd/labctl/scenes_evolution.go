package main

// Сцены m14. Своего профиля у модуля нет и быть не должно: изменение системы
// не добавляет машинерии, оно добавляет порядок действий. Поэтому обе сцены
// стоят на уже введённых профилях — на тех, где нужные им стороны уже есть:
//
//	39 · scale  — инстансов больше одного, значит на время выката за одним
//	              адресом стоят обе версии сразу;
//	40 · broker — единственный профиль, где рядом стоят старое место (монолит)
//	              и новое (сервис заказов), а переезд возможен только между
//	              двумя местами.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"time"
)

// ── сцена 39 — две версии одновременно (m14 l01) ────────────────────────────
//
// Выкатка не мгновенна. Пока она идёт, за одним адресом стоят обе версии, и
// какая из них ответит на конкретный запрос, вызывающий не выбирает. Из этого
// вытекает всё остальное: порядок выката перестаёт быть вопросом вкуса, а
// откат перестаёт быть бесплатным.
//
// Изменение контракта одно на всю сцену: сумма заказа переезжает из отдельного
// поля в список позиций. Новая версия понимает оба вида — сначала расширяем,
// сужаем потом, — и заводит заказ новым шагом процесса.
func scene39(ctx context.Context, r *Run) error {
	cfg := r.Script.Config
	// Наблюдает сцена: предмет — что увидел вызывающий, а не что записал сервис.
	r.Sources = nil

	reqs := cfg.Requests
	if reqs == 0 {
		reqs = 8
	}
	if len(cfg.Items) == 0 {
		return fmt.Errorf("в сценарии не заданы позиции заказа")
	}

	// Круг балансировщик раздаёт от внутреннего счётчика, который помнит прошлую
	// сцену: без перезапуска половина запросов доставалась бы то одной версии,
	// то другой, и числа в таблице нельзя было бы сверить с текстом шага.
	if err := r.resetBalancer(ctx); err != nil {
		return err
	}
	// База у инстансов одна: разъезжаются здесь версии кода, а не хранилища.
	// Ровно поэтому дальше и получится точка невозврата.
	for _, u := range r.instances() {
		if err := r.configureInstance(u, map[string]any{
			"reset": true, "write_mode": "none", "version": "v1",
		}); err != nil {
			return fmt.Errorf("инстанс %s не настроен — поднят ли профиль scale? %w", u, err)
		}
	}
	if err := r.prepareOrders(cfg.FirstOrderID); err != nil {
		return err
	}

	client := &http.Client{Timeout: 30 * time.Second}

	// Один прогон: reqs одинаковых запросов подряд через балансировщик. Клиент
	// один и тот же, запрос один и тот же; кому он достанется — не его решение.
	wrongBad := 0
	pass := func(key string, newStyle bool) error {
		ok, bad := 0, 0
		for i := 0; i < reqs; i++ {
			code, err := r.placeOrder(client, r.BalancerL7, cfg, newStyle)
			if err != nil {
				return fmt.Errorf("%s: %w", key, err)
			}
			if code == http.StatusCreated {
				ok++
			} else {
				bad++
			}
		}
		r.Measure(key, "ok", float64(ok))
		r.Measure(key, "bad", float64(bad))
		r.Set(key+"_ok", strconv.Itoa(ok))
		r.Set(key+"_bad", strconv.Itoa(bad))
		if len(key) > 5 && key[:5] == "wrong" {
			wrongBad += bad
		}
		return nil
	}
	deploy := func(url, version string) error {
		return r.configureInstance(url, map[string]any{"version": version})
	}

	// ── порядок правильный: первым выкатывается тот, кто принимает ───────────
	if err := deploy(r.OrdersURL, "v2"); err != nil {
		return err
	}
	if err := pass("right.half", false); err != nil {
		return err
	}
	if err := deploy(r.Orders2URL, "v2"); err != nil {
		return err
	}
	if err := pass("right.full", false); err != nil {
		return err
	}
	// И только теперь переключается тот, кто шлёт.
	if err := pass("right.client", true); err != nil {
		return err
	}

	// ── тот же выкат наоборот: первым переключается тот, кто шлёт ────────────
	for _, u := range r.instances() {
		if err := deploy(u, "v1"); err != nil {
			return err
		}
	}
	if err := pass("wrong.client", true); err != nil {
		return err
	}
	if err := deploy(r.OrdersURL, "v2"); err != nil {
		return err
	}
	if err := pass("wrong.half", true); err != nil {
		return err
	}
	if err := deploy(r.Orders2URL, "v2"); err != nil {
		return err
	}
	if err := pass("wrong.full", true); err != nil {
		return err
	}

	// ── откат: код возвращается за минуту, данные не возвращаются вовсе ──────
	//
	// Заказы в базе созданы обеими версиями. Какие именно — вопрос не к сцене:
	// она спрашивает у самой службы, что у неё лежит.
	rows, err := r.ordersInStore(r.OrdersURL)
	if err != nil {
		return err
	}
	// В примеры берутся два соседних заказа: сначала первый, заведённый старой
	// версией, следом первый, заведённый новой. Соседние — чтобы в таблице было
	// видно, что разница между ними не во времени и не в клиенте, а только в
	// том, какой инстанс поймал запрос.
	var oldMade, newMade int64
	total, unreadable := 0, 0
	for _, o := range rows {
		total++
		if knownToV1(o.Status) {
			if oldMade == 0 {
				oldMade = o.ID
			}
			continue
		}
		unreadable++
		if oldMade != 0 && newMade == 0 {
			newMade = o.ID
		}
	}
	if oldMade == 0 || newMade == 0 {
		return fmt.Errorf("в базе нет заказов обеих версий: старой %d, новой %d", oldMade, newMade)
	}

	// Сначала читает новая версия — обе ещё выкачены.
	for _, key := range []struct {
		name string
		id   int64
	}{{"v2.old", oldMade}, {"v2.new", newMade}} {
		code, reason, err := r.fetchOrder(client, r.OrdersURL, key.id)
		if err != nil {
			return err
		}
		r.Set(key.name+"_answer", answerOf(code, reason))
		r.Measure(key.name, "code", float64(code))
	}
	// Откатываем обе: в базе при этом не меняется ничего.
	for _, u := range r.instances() {
		if err := deploy(u, "v1"); err != nil {
			return err
		}
	}
	for _, key := range []struct {
		name string
		id   int64
	}{{"v1.old", oldMade}, {"v1.new", newMade}} {
		code, reason, err := r.fetchOrder(client, r.OrdersURL, key.id)
		if err != nil {
			return err
		}
		r.Set(key.name+"_answer", answerOf(code, reason))
		r.Measure(key.name, "code", float64(code))
	}
	r.Measure("rollback", "unreadable", float64(unreadable))

	r.Set("requests", strconv.Itoa(reqs))
	r.Set("wrong.total_bad", strconv.Itoa(wrongBad))
	r.Set("wrong.total", strconv.Itoa(reqs*3))
	r.Set("amount", strconv.FormatInt(cfg.Amount, 10))
	r.Set("items", itemList(cfg.Items))
	r.Set("old_order", strconv.FormatInt(oldMade, 10))
	r.Set("new_order", strconv.FormatInt(newMade, 10))
	r.Set("total_orders", strconv.Itoa(total))
	r.Set("unreadable", strconv.Itoa(unreadable))
	r.Set("readable", strconv.Itoa(total-unreadable))
	return nil
}

// placeOrder — один заказ через балансировщик. Старый вид запроса называет
// сумму полем, новый складывает её из позиций; больше между ними ничего не
// отличается, и в этом весь смысл — изменение маленькое, а цена у него есть.
func (r *Run) placeOrder(c *http.Client, base string, cfg ScriptConfig, newStyle bool) (int, error) {
	body := map[string]any{"client": cfg.Client, "restaurant": cfg.Restaurant}
	if newStyle {
		list := make([]map[string]any, 0, len(cfg.Items))
		for _, it := range cfg.Items {
			list = append(list, map[string]any{"name": it.Name, "price": it.Price})
		}
		body["items"] = list
	} else {
		body["amount"] = cfg.Amount
	}
	payload, _ := json.Marshal(body)
	resp, err := c.Post(base+"/orders", "application/json", bytesReader(payload))
	if err != nil {
		return 0, fmt.Errorf("%s: %w", base, err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return resp.StatusCode, nil
}

// fetchOrder — чтение заказа вместе с причиной отказа, если он был. Причину
// называет тот, кто отказал: придумывать её за него сцена не имеет права.
func (r *Run) fetchOrder(c *http.Client, base string, id int64) (int, string, error) {
	resp, err := c.Get(fmt.Sprintf("%s/orders/%d", base, id))
	if err != nil {
		return 0, "", fmt.Errorf("%s: %w", base, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var body struct {
		Status string `json:"status"`
		Error  string `json:"error"`
	}
	_ = json.Unmarshal(raw, &body)
	if resp.StatusCode == http.StatusOK {
		return resp.StatusCode, "заказ отдан, статус " + body.Status, nil
	}
	return resp.StatusCode, body.Error, nil
}

func answerOf(code int, reason string) string {
	return strconv.Itoa(code) + " · " + reason
}

// knownToV1 — статусы, которые знала версия, работавшая до изменения. Тот же
// перечень, что у службы: сцена не угадывает её поведение, а повторяет его.
func knownToV1(status string) bool {
	switch status {
	case "created", "paid", "assigned", "cancelled":
		return true
	}
	return false
}

func itemList(items []Item) string {
	out := ""
	for i, it := range items {
		if i > 0 {
			out += ", "
		}
		out += fmt.Sprintf("%s %d", it.Name, it.Price)
	}
	return out
}

func (r *Run) configureInstance(url string, body map[string]any) error {
	return r.postJSON(url+"/_lab/config", body, nil)
}

// ── сцена 40 — переезд данных со сверкой (m14 l02) ──────────────────────────
//
// Данные переезжают из старого места в новое, пока система работает. Отсюда
// три обязательных части: двойная запись (новое не отстаёт от старого),
// переливка партиями (старое доезжает до нового) и сверка — единственное, чем
// доказывается, что копия равна истине.
//
// Расхождения сцена заводит намеренно и по одному на механизм: копия не доехала,
// строка изменилась после переливки, строка удалена после переливки. Все три
// настоящие, все три не видны ни в одном месте по отдельности.
func scene40(ctx context.Context, r *Run) error {
	cfg := r.Script.Config
	r.Sources = nil

	history := cfg.History
	if history == 0 {
		history = 24
	}
	batch := cfg.Batch
	if batch == 0 {
		batch = 8
	}
	flowA := cfg.FlowFirst
	if flowA == 0 {
		flowA = 4
	}
	flowB := cfg.FlowSecond
	if flowB == 0 {
		flowB = 2
	}
	lostAt := cfg.LostCopyAt
	if lostAt == 0 {
		lostAt = 3
	}
	if lostAt > flowA {
		return fmt.Errorf("потерянная копия назначена на заказ %d, а в потоке их %d", lostAt, flowA)
	}
	first := cfg.FirstOrderID
	if first == 0 {
		first = 7714
	}

	// Эквайринг и уведомления — фон этой сцены: заказы должны просто приниматься.
	if err := r.postJSON(r.AcquirerURL+"/_lab/config", map[string]any{
		"delay_ms": 0, "mode": "ok", "idempotent": false, "reset": true,
	}, nil); err != nil {
		return err
	}
	if err := r.configureNotifier(map[string]any{"mode": "ok", "strict": false, "reset": true}); err != nil {
		return fmt.Errorf("уведомления не настроены — поднят ли профиль broker? %w", err)
	}
	if err := r.configure(map[string]any{"reset": true}); err != nil {
		return fmt.Errorf("старое место не настроено — поднят ли профиль broker? %w", err)
	}
	if err := r.postJSON(r.MonolithURL+"/_lab/prepare",
		map[string]any{"first_order_id": first}, nil); err != nil {
		return err
	}
	// Новое место начинается пустым: переезд, который начался с непустого
	// нового места, сверять нечем.
	if err := r.configureOrders(map[string]any{
		"reset": true, "write_mode": "none", "version": "v1",
	}); err != nil {
		return fmt.Errorf("новое место не настроено: %w", err)
	}

	client := &http.Client{Timeout: 60 * time.Second}
	step := func(key string) error {
		old, err := r.placeCount(r.MonolithURL)
		if err != nil {
			return err
		}
		fresh, err := r.placeCount(r.OrdersURL)
		if err != nil {
			return err
		}
		r.Measure(key, "old", float64(old))
		r.Measure(key, "new", float64(fresh))
		r.Set(key+"_old", strconv.Itoa(old))
		r.Set(key+"_new", strconv.Itoa(fresh))
		r.Set(key+"_gap", strconv.Itoa(old-fresh))
		return nil
	}

	// ── 1. история: то, что накопилось до переезда ──────────────────────────
	if err := r.placeOrders(client, cfg, history); err != nil {
		return err
	}
	if err := step("history"); err != nil {
		return err
	}

	// ── 2. двойная запись включена, поток не останавливается ────────────────
	if err := r.configure(map[string]any{"mirror": true}); err != nil {
		return err
	}
	for i := 1; i <= flowA; i++ {
		if i == lostAt {
			// Копия этого заказа не доедет. Приём заказа от этого не изменится
			// ничем: вторая запись живёт вне транзакции первой.
			if err := r.configure(map[string]any{"mirror_drop_next": 1}); err != nil {
				return err
			}
		}
		if err := r.placeOrders(client, cfg, 1); err != nil {
			return err
		}
	}
	lost := first + int64(history+lostAt-1)
	if err := step("flow.a"); err != nil {
		return err
	}

	// ── 3. переливка партиями, между партиями поток идёт ────────────────────
	batches := [][]int64{}
	for i := 0; i < history; i += batch {
		var ids []int64
		for j := i; j < i+batch && j < history; j++ {
			ids = append(ids, first+int64(j))
		}
		batches = append(batches, ids)
	}

	if err := r.copyBatch(batches[0]); err != nil {
		return err
	}
	if err := step("batch.1"); err != nil {
		return err
	}

	// Дежурный отменяет заказ в старом месте. Двойная запись этого не видит:
	// она навешана на приём заказа, а тут заказ никто не принимает.
	changed := first + 2
	if err := r.postJSON(r.MonolithURL+"/_lab/cancel", map[string]any{"order": changed}, nil); err != nil {
		return err
	}
	if err := step("changed"); err != nil {
		return err
	}

	if err := r.placeOrders(client, cfg, flowB); err != nil {
		return err
	}
	if err := step("flow.b"); err != nil {
		return err
	}

	if len(batches) < 3 {
		return fmt.Errorf("партий переливки должно быть хотя бы три, вышло %d", len(batches))
	}
	if err := r.copyBatch(batches[1]); err != nil {
		return err
	}
	if err := step("batch.2"); err != nil {
		return err
	}

	// Клиент просит удалить свои данные. Строка исчезает из старого места —
	// и остаётся в новом, куда партия успела её перелить.
	erased := batches[1][0]
	if err := r.postJSON(r.MonolithURL+"/_lab/erase", map[string]any{"order": erased}, nil); err != nil {
		return err
	}
	if err := step("erased"); err != nil {
		return err
	}

	for _, ids := range batches[2:] {
		if err := r.copyBatch(ids); err != nil {
			return err
		}
	}
	if err := step("batch.3"); err != nil {
		return err
	}

	// ── 4. сверка ───────────────────────────────────────────────────────────
	d, same, err := r.reconcile()
	if err != nil {
		return err
	}
	r.Measure("same", "n", float64(same))
	r.Measure("only.old", "n", float64(len(d.onlyOld)))
	r.Measure("only.new", "n", float64(len(d.onlyNew)))
	r.Measure("mismatch", "n", float64(len(d.mismatch)))
	r.Set("same_n", strconv.Itoa(same))
	r.Set("only_old_n", strconv.Itoa(len(d.onlyOld)))
	r.Set("only_new_n", strconv.Itoa(len(d.onlyNew)))
	r.Set("mismatch_n", strconv.Itoa(len(d.mismatch)))
	r.Set("only_old_ids", idList(d.onlyOld))
	r.Set("only_new_ids", idList(d.onlyNew))
	r.Set("mismatch_ids", idList(d.mismatch))
	r.Set("checked", strconv.Itoa(same+len(d.onlyOld)+len(d.onlyNew)+len(d.mismatch)))
	r.Set("found", strconv.Itoa(len(d.onlyOld)+len(d.onlyNew)+len(d.mismatch)))
	r.Set("lost", strconv.FormatInt(lost, 10))
	r.Set("changed", strconv.FormatInt(changed, 10))
	r.Set("erased", strconv.FormatInt(erased, 10))

	// Расхождение по полям печатается тем, что в нём разошлось: «одна запись
	// отличается» — не наблюдение, а пересказ.
	if len(d.mismatch) > 0 {
		oldRows, err := r.storeMap(r.MonolithURL)
		if err != nil {
			return err
		}
		newRows, err := r.storeMap(r.OrdersURL)
		if err != nil {
			return err
		}
		id := d.mismatch[0]
		r.Set("mismatch_old", oldRows[id].Status)
		r.Set("mismatch_new", newRows[id].Status)
	}

	// ── 5. разрешение по заранее выбранному правилу ─────────────────────────
	// Правило выбирается до переезда, а не в момент, когда сверка что-то нашла:
	// пока владение не передано, истина — в старом месте.
	current, err := r.storeMap(r.MonolithURL)
	if err != nil {
		return err
	}
	for _, id := range append(append([]int64{}, d.onlyOld...), d.mismatch...) {
		if err := r.copyOne(current[id]); err != nil {
			return err
		}
	}
	for _, id := range d.onlyNew {
		if err := r.postJSON(r.OrdersURL+"/_lab/drop-copy", map[string]any{"order": id}, nil); err != nil {
			return err
		}
	}

	after, sameAfter, err := r.reconcile()
	if err != nil {
		return err
	}
	left := len(after.onlyOld) + len(after.onlyNew) + len(after.mismatch)
	r.Measure("after", "same", float64(sameAfter))
	r.Measure("after", "left", float64(left))
	r.Set("after_same", strconv.Itoa(sameAfter))
	r.Set("after_left", strconv.Itoa(left))

	// ── 6. перенос владения ─────────────────────────────────────────────────
	// Момент, ради которого всё и делалось: с этой секунды заказ заводит новое
	// место, и старое о нём не узнаёт.
	if err := r.configure(map[string]any{"mirror": false}); err != nil {
		return err
	}
	ids, err := r.createOrders(1, cfg.Client, cfg.Restaurant, cfg.Amount)
	if err != nil {
		return err
	}
	if err := step("owned"); err != nil {
		return err
	}
	r.Set("owned_order", strconv.FormatInt(ids[0], 10))
	r.Set("history", strconv.Itoa(history))
	r.Set("batch", strconv.Itoa(batch))
	r.Set("batches", strconv.Itoa(len(batches)))
	r.Set("flow_a", strconv.Itoa(flowA))
	r.Set("flow_b", strconv.Itoa(flowB))
	r.Set("first", strconv.FormatInt(first, 10))
	r.Set("batch1_from", strconv.FormatInt(batches[0][0], 10))
	r.Set("batch1_to", strconv.FormatInt(batches[0][len(batches[0])-1], 10))
	r.Set("batch2_from", strconv.FormatInt(batches[1][0], 10))
	r.Set("batch2_to", strconv.FormatInt(batches[1][len(batches[1])-1], 10))
	r.Set("batch3_from", strconv.FormatInt(batches[2][0], 10))
	r.Set("batch3_to", strconv.FormatInt(batches[2][len(batches[2])-1], 10))
	return nil
}

// storeRow — запись так, как её отдаёт хозяин места. Сверка сравнивает их
// целиком: расхождение по одному полю — такое же расхождение, как отсутствие
// строки, и ловится оно только сравнением всех полей сразу.
type storeRow struct {
	ID         int64  `json:"id"`
	Client     string `json:"client"`
	Restaurant string `json:"restaurant"`
	Amount     int64  `json:"amount"`
	Status     string `json:"status"`
}

// ordersInStore спрашивает у самого хозяина места, что у него лежит. В чужую
// базу сверка не заглядывает: у обоих мест есть свой ответ на этот вопрос, и
// брать его надо у них.
func (r *Run) ordersInStore(base string) ([]storeRow, error) {
	var state struct {
		Orders []storeRow `json:"orders"`
	}
	if err := r.getJSON(base+"/_lab/state", &state); err != nil {
		return nil, err
	}
	return state.Orders, nil
}

func (r *Run) storeMap(base string) (map[int64]storeRow, error) {
	rows, err := r.ordersInStore(base)
	if err != nil {
		return nil, err
	}
	out := make(map[int64]storeRow, len(rows))
	for _, o := range rows {
		out[o.ID] = o
	}
	return out, nil
}

func (r *Run) placeCount(base string) (int, error) {
	rows, err := r.ordersInStore(base)
	return len(rows), err
}

// placeOrders — n заказов через старое место. Клиент здесь сама сцена, и
// заказы настоящие: переезд идёт под потоком, а не на замершей системе.
func (r *Run) placeOrders(c *http.Client, cfg ScriptConfig, n int) error {
	body, _ := json.Marshal(map[string]any{
		"client": cfg.Client, "restaurant": cfg.Restaurant, "amount": cfg.Amount,
	})
	for i := 0; i < n; i++ {
		code, err := doOnce(c, http.MethodPost, r.MonolithURL+"/orders", body)
		if err != nil {
			return fmt.Errorf("заказ не принят старым местом: %w", err)
		}
		if code != http.StatusCreated {
			return fmt.Errorf("заказ не принят старым местом: код %d", code)
		}
	}
	return nil
}

// copyOne — одна строка в новое место. Той же ручкой, которой пользуется
// двойная запись: разница между переливкой и потоком не в способе записи.
func (r *Run) copyOne(row storeRow) error {
	return r.postJSON(r.OrdersURL+"/_lab/mirror", map[string]any{
		"id": row.ID, "client": row.Client, "restaurant": row.Restaurant,
		"amount": row.Amount, "status": row.Status,
	}, nil)
}

// copyBatch — одна партия переливки. Старое место читается в момент запуска
// партии, а не по снимку, снятому в начале переезда: между партиями система
// работает, и данные в старом месте меняются.
func (r *Run) copyBatch(ids []int64) error {
	src, err := r.storeMap(r.MonolithURL)
	if err != nil {
		return err
	}
	for _, id := range ids {
		row, ok := src[id]
		if !ok {
			return fmt.Errorf("партия ссылается на заказ %d, которого в старом месте нет", id)
		}
		if err := r.copyOne(row); err != nil {
			return err
		}
	}
	return nil
}

// migDiff — то, что нашла сверка. Три вида расхождения, и лечатся они
// по-разному: недостача доливается, лишнее удаляется, разошедшееся
// перезаписывается.
type migDiff struct {
	onlyOld  []int64
	onlyNew  []int64
	mismatch []int64
}

// reconcile сравнивает два места построчно. Сравнение по количеству строк —
// не сверка: количество может сойтись при трёх расхождениях сразу, и в этой
// сцене именно так и выходит.
func (r *Run) reconcile() (migDiff, int, error) {
	var d migDiff
	old, err := r.storeMap(r.MonolithURL)
	if err != nil {
		return d, 0, err
	}
	fresh, err := r.storeMap(r.OrdersURL)
	if err != nil {
		return d, 0, err
	}

	seen := map[int64]bool{}
	var ids []int64
	for id := range old {
		seen[id] = true
		ids = append(ids, id)
	}
	for id := range fresh {
		if !seen[id] {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	same := 0
	for _, id := range ids {
		o, inOld := old[id]
		n, inNew := fresh[id]
		switch {
		case !inNew:
			d.onlyOld = append(d.onlyOld, id)
		case !inOld:
			d.onlyNew = append(d.onlyNew, id)
		case o != n:
			d.mismatch = append(d.mismatch, id)
		default:
			same++
		}
	}
	return d, same, nil
}

func idList(ids []int64) string {
	if len(ids) == 0 {
		return "—"
	}
	out := ""
	for i, id := range ids {
		if i > 0 {
			out += ", "
		}
		out += strconv.FormatInt(id, 10)
	}
	return out
}
