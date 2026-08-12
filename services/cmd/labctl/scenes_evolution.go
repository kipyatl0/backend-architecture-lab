package main

// Сцены m14. Своего профиля у модуля нет и быть не должно: изменение системы
// не добавляет машинерии, оно добавляет порядок действий. Поэтому обе сцены
// стоят на уже введённых профилях — на тех, где нужные им стороны уже есть:
//
//	39 · scale  — инстансов больше одного, значит на время выката за одним
//	              адресом стоят обе версии сразу;
//	40 · broker — переезжает остаток монолита, а остаток есть только там, где
//	              заказы, курьеры и уведомления уже уехали в свои службы. Ни
//	              брокер, ни служба заказов в сцене не участвуют.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
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
// Переезжает то, что монолит не отдал никому: профили клиентов. Заказы,
// курьеров и уведомления к этому времени обслуживают выделенные службы, а
// клиент со своим адресом так и остался в старом месте — и остаток этот виден
// только там, где остальное уже уехало.
//
// Данные переезжают, пока система работает. Отсюда три обязательных части:
// двойная запись (новое не отстаёт от старого), переливка партиями (старое
// доезжает до нового) и сверка — единственное, чем доказывается, что копия
// равна истине.
//
// Расхождения сцена заводит намеренно и по одному на механизм: копия не
// доехала, строка изменилась после переливки, строка удалена после переливки.
// Все три настоящие, все три не видны ни в одном месте по отдельности.
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
		return fmt.Errorf("потерянная копия назначена на регистрацию %d, а в потоке их %d", lostAt, flowA)
	}
	first := cfg.FirstClientID
	if first == 0 {
		first = 4021
	}
	if len(cfg.Names) == 0 || len(cfg.Streets) == 0 {
		return fmt.Errorf("в сценарии не заданы имена и улицы: профиль клиента должен из чего-то состоять")
	}

	// Старое место — монолит. Профили в нём заводит регистрация, и на неё же
	// навешана двойная запись.
	if err := r.configure(map[string]any{"reset": true}); err != nil {
		return fmt.Errorf("старое место не настроено — поднят ли профиль broker? %w", err)
	}
	if err := r.postJSON(r.MonolithURL+"/_lab/prepare",
		map[string]any{"first_client_id": first}, nil); err != nil {
		return err
	}
	// Новое место заводит оператор переезда: базы у него до этой секунды нет
	// вовсе. Так это и происходит в жизни — хранилище нового владельца
	// появляется раньше, чем его код.
	newPlace, err := r.openNewPlace(ctx)
	if err != nil {
		return err
	}
	defer newPlace.Close(ctx)

	client := &http.Client{Timeout: 60 * time.Second}
	// seq — сколько регистраций сцена уже подала. Из него собираются поля
	// профиля: имя, телефон и адрес обязаны быть предсказуемы, иначе сверка
	// сравнивала бы случайные строки.
	seq := 0
	register := func(n int) error {
		for i := 0; i < n; i++ {
			body, _ := json.Marshal(profileOf(cfg, first+int64(seq), seq))
			code, err := doOnce(client, http.MethodPost, r.MonolithURL+"/clients", body)
			if err != nil {
				return fmt.Errorf("профиль не принят старым местом: %w", err)
			}
			if code != http.StatusCreated {
				return fmt.Errorf("профиль не принят старым местом: код %d", code)
			}
			seq++
		}
		return nil
	}
	step := func(key string) error {
		old, err := r.oldPlaceRows()
		if err != nil {
			return err
		}
		fresh, err := newPlaceRows(ctx, newPlace)
		if err != nil {
			return err
		}
		r.Measure(key, "old", float64(len(old)))
		r.Measure(key, "new", float64(len(fresh)))
		r.Set(key+"_old", strconv.Itoa(len(old)))
		r.Set(key+"_new", strconv.Itoa(len(fresh)))
		r.Set(key+"_gap", strconv.Itoa(len(old)-len(fresh)))
		return nil
	}

	// ── 1. история: то, что накопилось до переезда ──────────────────────────
	if err := register(history); err != nil {
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
			// Копия этой регистрации не доедет. Приём профиля от этого не
			// изменится ничем: вторая запись живёт вне транзакции первой.
			if err := r.configure(map[string]any{"mirror_drop_next": 1}); err != nil {
				return err
			}
		}
		if err := register(1); err != nil {
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
	if len(batches) < 3 {
		return fmt.Errorf("партий переливки должно быть хотя бы три, вышло %d", len(batches))
	}

	if err := r.copyBatch(ctx, newPlace, batches[0]); err != nil {
		return err
	}
	if err := step("batch.1"); err != nil {
		return err
	}

	// Поддержка исправляет адрес по звонку клиента. Двойная запись этого не
	// видит: она навешана на регистрацию, а тут никто не регистрируется.
	changed := first + 2
	address := cfg.FixedAddress
	if address == "" {
		address = "Садовая 12"
	}
	if err := r.postJSON(r.MonolithURL+"/_lab/edit-address",
		map[string]any{"client": changed, "address": address}, nil); err != nil {
		return err
	}
	if err := step("changed"); err != nil {
		return err
	}

	if err := register(flowB); err != nil {
		return err
	}
	if err := step("flow.b"); err != nil {
		return err
	}

	if err := r.copyBatch(ctx, newPlace, batches[1]); err != nil {
		return err
	}
	if err := step("batch.2"); err != nil {
		return err
	}

	// Клиент просит удалить свои данные. Строка исчезает из старого места —
	// и остаётся в новом, куда партия успела её перелить.
	erased := batches[1][0]
	if err := r.postJSON(r.MonolithURL+"/_lab/erase", map[string]any{"client": erased}, nil); err != nil {
		return err
	}
	if err := step("erased"); err != nil {
		return err
	}

	for _, ids := range batches[2:] {
		if err := r.copyBatch(ctx, newPlace, ids); err != nil {
			return err
		}
	}
	if err := step("batch.3"); err != nil {
		return err
	}

	// ── 4. сверка ───────────────────────────────────────────────────────────
	d, same, err := r.reconcile(ctx, newPlace)
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
		oldRows, err := r.oldPlaceMap()
		if err != nil {
			return err
		}
		newRows, err := newPlaceMap(ctx, newPlace)
		if err != nil {
			return err
		}
		id := d.mismatch[0]
		r.Set("mismatch_old", oldRows[id].Address)
		r.Set("mismatch_new", newRows[id].Address)
	}

	// ── 5. разрешение по заранее выбранному правилу ─────────────────────────
	// Правило выбирается до переезда, а не в момент, когда сверка что-то нашла:
	// пока владение не передано, истина — в старом месте.
	current, err := r.oldPlaceMap()
	if err != nil {
		return err
	}
	for _, id := range append(append([]int64{}, d.onlyOld...), d.mismatch...) {
		if err := copyOne(ctx, newPlace, current[id]); err != nil {
			return err
		}
	}
	for _, id := range d.onlyNew {
		if err := dropCopy(ctx, newPlace, id); err != nil {
			return err
		}
	}

	after, sameAfter, err := r.reconcile(ctx, newPlace)
	if err != nil {
		return err
	}
	left := len(after.onlyOld) + len(after.onlyNew) + len(after.mismatch)
	r.Measure("after", "same", float64(sameAfter))
	r.Measure("after", "left", float64(left))
	r.Set("after_same", strconv.Itoa(sameAfter))
	r.Set("after_left", strconv.Itoa(left))

	// ── 6. перенос владения ─────────────────────────────────────────────────
	// Момент, ради которого всё и делалось: с этой секунды профиль заводит
	// новое место, и старое о нём не узнаёт. Пишет его оператор — службы у
	// нового места ещё нет, и её роль он играет так же, как во всех сценах
	// курса роль клиента играет сама сцена.
	if err := r.configure(map[string]any{"mirror": false}); err != nil {
		return err
	}
	owned := first + int64(seq)
	if err := copyOne(ctx, newPlace, profileOf(cfg, owned, seq)); err != nil {
		return err
	}
	if err := step("owned"); err != nil {
		return err
	}
	r.Set("owned_client", strconv.FormatInt(owned, 10))
	r.Set("history", strconv.Itoa(history))
	r.Set("batch", strconv.Itoa(batch))
	r.Set("batches", strconv.Itoa(len(batches)))
	r.Set("flow_a", strconv.Itoa(flowA))
	r.Set("flow_b", strconv.Itoa(flowB))
	r.Set("first", strconv.FormatInt(first, 10))
	r.Set("address", address)
	r.Set("batch1_from", strconv.FormatInt(batches[0][0], 10))
	r.Set("batch1_to", strconv.FormatInt(batches[0][len(batches[0])-1], 10))
	r.Set("batch2_from", strconv.FormatInt(batches[1][0], 10))
	r.Set("batch2_to", strconv.FormatInt(batches[1][len(batches[1])-1], 10))
	r.Set("batch3_from", strconv.FormatInt(batches[2][0], 10))
	r.Set("batch3_to", strconv.FormatInt(batches[2][len(batches[2])-1], 10))
	return nil
}

// clientRow — профиль так, как его отдаёт хозяин места. Сверка сравнивает их
// целиком: расхождение по одному полю — такое же расхождение, как отсутствие
// строки, и ловится оно только сравнением всех полей сразу.
type clientRow struct {
	ID      int64  `json:"id"`
	Name    string `json:"name"`
	Phone   string `json:"phone"`
	Address string `json:"address"`
}

// profileOf — профиль n-го по счёту клиента. Поля выводятся из номера, и это
// не украшение: сверке нужно больше одного поля, иначе расхождение «строки
// есть в обоих местах, а внутри разное» негде показать.
func profileOf(cfg ScriptConfig, id int64, seq int) clientRow {
	return clientRow{
		ID:      id,
		Name:    cfg.Names[seq%len(cfg.Names)],
		Phone:   fmt.Sprintf("+7 900 555-%04d", id),
		Address: fmt.Sprintf("%s %d", cfg.Streets[seq%len(cfg.Streets)], seq%9+1),
	}
}

// oldPlaceRows спрашивает у самого хозяина места, что у него лежит. В чужую
// базу сверка не заглядывает: у старого места есть свой ответ на этот вопрос,
// и брать его надо у него.
func (r *Run) oldPlaceRows() ([]clientRow, error) {
	var state struct {
		Clients []clientRow `json:"clients"`
	}
	if err := r.getJSON(r.MonolithURL+"/_lab/state", &state); err != nil {
		return nil, err
	}
	return state.Clients, nil
}

func (r *Run) oldPlaceMap() (map[int64]clientRow, error) {
	rows, err := r.oldPlaceRows()
	if err != nil {
		return nil, err
	}
	return rowsByID(rows), nil
}

// openNewPlace заводит новое место. База, в которую переезжают, до переезда не
// существует: её создаёт оператор, подключившись к соседней. Таблица пустая —
// переезд, начавшийся с непустого нового места, сверять нечем.
func (r *Run) openNewPlace(ctx context.Context) (*pgx.Conn, error) {
	admin, err := pgConnect(ctx, r.OldPlaceDSN, 60*time.Second)
	if err != nil {
		return nil, fmt.Errorf("сервер базы не ответил — поднят ли профиль broker? %w", err)
	}
	var exists bool
	if err := admin.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'clients')`).Scan(&exists); err != nil {
		admin.Close(ctx)
		return nil, err
	}
	if !exists {
		if _, err := admin.Exec(ctx, `CREATE DATABASE clients`); err != nil {
			admin.Close(ctx)
			return nil, fmt.Errorf("новое место не создано: %w", err)
		}
	}
	admin.Close(ctx)

	conn, err := pgConnect(ctx, r.NewPlaceDSN, 60*time.Second)
	if err != nil {
		return nil, err
	}
	if _, err := conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS clients (
			id      bigint PRIMARY KEY,
			name    text NOT NULL,
			phone   text NOT NULL,
			address text NOT NULL
		)`); err != nil {
		conn.Close(ctx)
		return nil, err
	}
	if _, err := conn.Exec(ctx, `TRUNCATE clients`); err != nil {
		conn.Close(ctx)
		return nil, err
	}
	return conn, nil
}

func newPlaceRows(ctx context.Context, conn *pgx.Conn) ([]clientRow, error) {
	rows, err := conn.Query(ctx, `SELECT id, name, phone, address FROM clients ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []clientRow{}
	for rows.Next() {
		var c clientRow
		if err := rows.Scan(&c.ID, &c.Name, &c.Phone, &c.Address); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

func newPlaceMap(ctx context.Context, conn *pgx.Conn) (map[int64]clientRow, error) {
	rows, err := newPlaceRows(ctx, conn)
	if err != nil {
		return nil, err
	}
	return rowsByID(rows), nil
}

func rowsByID(rows []clientRow) map[int64]clientRow {
	out := make(map[int64]clientRow, len(rows))
	for _, c := range rows {
		out[c.ID] = c
	}
	return out
}

// copyOne — одна строка в новое место. Тем же способом, каким пишет двойная
// запись: разница между переливкой и потоком не в способе записи, а в том, кто
// и когда шлёт. Вставка с обновлением — партия может встретить строку, которую
// двойная запись уже принесла, и падать на этом нельзя.
func copyOne(ctx context.Context, conn *pgx.Conn, row clientRow) error {
	_, err := conn.Exec(ctx,
		`INSERT INTO clients (id, name, phone, address) VALUES ($1,$2,$3,$4)
		 ON CONFLICT (id) DO UPDATE SET name=$2, phone=$3, address=$4`,
		row.ID, row.Name, row.Phone, row.Address)
	return err
}

// dropCopy убирает из нового места строку, которой в старом уже нет. Это и есть
// разрешение расхождения третьего вида: правило «истина в старом месте»
// действует в обе стороны, а не только на недостачу.
func dropCopy(ctx context.Context, conn *pgx.Conn, id int64) error {
	_, err := conn.Exec(ctx, `DELETE FROM clients WHERE id=$1`, id)
	return err
}

// copyBatch — одна партия переливки. Старое место читается в момент запуска
// партии, а не по снимку, снятому в начале переезда: между партиями система
// работает, и данные в старом месте меняются.
func (r *Run) copyBatch(ctx context.Context, conn *pgx.Conn, ids []int64) error {
	src, err := r.oldPlaceMap()
	if err != nil {
		return err
	}
	for _, id := range ids {
		row, ok := src[id]
		if !ok {
			return fmt.Errorf("партия ссылается на профиль %d, которого в старом месте нет", id)
		}
		if err := copyOne(ctx, conn, row); err != nil {
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
func (r *Run) reconcile(ctx context.Context, conn *pgx.Conn) (migDiff, int, error) {
	var d migDiff
	old, err := r.oldPlaceMap()
	if err != nil {
		return d, 0, err
	}
	fresh, err := newPlaceMap(ctx, conn)
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

// storeRow — заказ так, как его отдаёт служба. Сцене 39 он нужен, чтобы
// спросить у самой службы, что у неё лежит после выката обеих версий.
type storeRow struct {
	ID         int64  `json:"id"`
	Client     string `json:"client"`
	Restaurant string `json:"restaurant"`
	Amount     int64  `json:"amount"`
	Status     string `json:"status"`
}

// ordersInStore спрашивает у самой службы, что у неё лежит: в её базу сцена
// не заглядывает — ответ на этот вопрос есть у хозяина.
func (r *Run) ordersInStore(base string) ([]storeRow, error) {
	var state struct {
		Orders []storeRow `json:"orders"`
	}
	if err := r.getJSON(base+"/_lab/state", &state); err != nil {
		return nil, err
	}
	return state.Orders, nil
}
