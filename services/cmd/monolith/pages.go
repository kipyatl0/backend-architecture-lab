package main

// Чтение списка заказов страницами — два способа, которые в спокойной системе
// неотличимы, а под потоком вставок расходятся. Нужны одному уроку m04 и одной
// сцене: пока список не меняется под читателем, спорить тут не о чем.

import (
	"context"
	"net/http"
	"strconv"

	"github.com/kipyatl0/backend-architecture-lab/services/internal/lab"
)

type pageItem struct {
	ID     int64  `json:"id"`
	Client string `json:"client"`
	Amount int64  `json:"amount"`
}

// handlePage отдаёт страницу списка заказов, отсортированного от новых к старым.
// Порядок именно такой, потому что таким его показывают в интерфейсе: новое
// сверху. Из этого и растёт вся разница между двумя способами листать.
func (a *app) handlePage(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit, _ := strconv.Atoi(q.Get("limit"))
	if limit <= 0 || limit > 100 {
		limit = 10
	}

	var (
		rows interface {
			Next() bool
			Scan(...any) error
			Close()
		}
		err error
	)

	if cur := q.Get("after"); cur != "" {
		// По курсору: «дай то, что старше вот этой записи». Ответ не зависит
		// от того, сколько записей появилось выше.
		after, cerr := strconv.ParseInt(cur, 10, 64)
		if cerr != nil {
			lab.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "плохой курсор"})
			return
		}
		rows, err = a.pool.Query(r.Context(),
			`SELECT id, client, amount FROM orders WHERE id < $1 ORDER BY id DESC LIMIT $2`,
			after, limit)
	} else if q.Get("mode") == "cursor" {
		rows, err = a.pool.Query(r.Context(),
			`SELECT id, client, amount FROM orders ORDER BY id DESC LIMIT $1`, limit)
	} else {
		// По смещению: «пропусти N и дай следующие». Что именно окажется на
		// позиции N, зависит от того, сколько записей добавилось выше.
		offset, _ := strconv.Atoi(q.Get("offset"))
		rows, err = a.pool.Query(r.Context(),
			`SELECT id, client, amount FROM orders ORDER BY id DESC LIMIT $1 OFFSET $2`,
			limit, offset)
	}
	if err != nil {
		lab.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	defer rows.Close()

	items := []pageItem{}
	for rows.Next() {
		var it pageItem
		if err := rows.Scan(&it.ID, &it.Client, &it.Amount); err == nil {
			items = append(items, it)
		}
	}
	next := int64(0)
	if len(items) > 0 {
		next = items[len(items)-1].ID
	}
	lab.WriteJSON(w, http.StatusOK, map[string]any{"items": items, "next": next})
}

// handleSeed добавляет в список заказы, не трогая ни оплату, ни уведомления.
// Сцене нужен поток вставок, а не полный жизненный цикл заказа: предмет урока —
// чтение списка, который меняется под читателем.
func (a *app) handleSeed(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Count      int    `json:"count"`
		Client     string `json:"client"`
		Restaurant string `json:"restaurant"`
		Amount     int64  `json:"amount"`
	}
	if err := lab.ReadJSON(r, &req); err != nil {
		lab.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if req.Count <= 0 {
		req.Count = 1
	}
	if req.Client == "" {
		req.Client = "Ирина"
	}
	if req.Restaurant == "" {
		req.Restaurant = "Пекарня «Батон»"
	}
	if req.Amount == 0 {
		req.Amount = 1890
	}

	ctx := context.Background()
	first, lastID := int64(0), int64(0)
	for i := 0; i < req.Count; i++ {
		var id int64
		if err := a.pool.QueryRow(ctx,
			`INSERT INTO orders (client, restaurant, amount, status) VALUES ($1,$2,$3,'paid') RETURNING id`,
			req.Client, req.Restaurant, req.Amount).Scan(&id); err != nil {
			lab.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		if first == 0 {
			first = id
		}
		lastID = id
	}
	lab.WriteJSON(w, http.StatusOK, map[string]any{"first": first, "last": lastID, "count": req.Count})
}
