package main

import (
	"context"
	"fmt"
	"strconv"
	"time"
)

// Сцена 41 — постраничная выдача под потоком вставок (m04 l03).
//
// Номер вне общего порядка каталога намеренно: сцена добавлена к уже
// написанному модулю, а перенумеровывать сцены 22–40 значило бы сломать ссылки
// в LAB.md и в будущих шагах ради одной строки в таблице.

type pageResp struct {
	Items []struct {
		ID int64 `json:"id"`
	} `json:"items"`
	Next int64 `json:"next"`
}

func scene41(ctx context.Context, r *Run) error {
	cfg := r.Script.Config
	r.Sources = nil

	total := cfg.Messages
	if total == 0 {
		total = 30
	}
	limit := cfg.Prefetch
	if limit == 0 {
		limit = 10
	}
	inserts := cfg.Consumers
	if inserts == 0 {
		inserts = 5
	}
	pages := total / limit

	r.Set("total", strconv.Itoa(total))
	r.Set("limit", strconv.Itoa(limit))
	r.Set("inserts", strconv.Itoa(inserts))
	r.Set("pages", strconv.Itoa(pages))
	r.Set("first_order", strconv.FormatInt(cfg.FirstOrderID, 10))
	r.Set("last_order", strconv.FormatInt(cfg.FirstOrderID+int64(total)-1, 10))

	// seed кладёт в список заказы, не трогая оплату и уведомления: предмет
	// сцены — чтение списка, а не жизненный цикл заказа.
	seed := func(n int) error {
		return r.postJSON(r.MonolithURL+"/_lab/seed", map[string]any{
			"count": n, "client": cfg.Client, "restaurant": cfg.Restaurant, "amount": cfg.Amount,
		}, nil)
	}

	// scroll листает список сверху вниз, между страницами добавляя новые
	// записи. Возвращает: сколько всего показано, сколько из них повторов и
	// сколько записей исходного списка не показано ни разу.
	scroll := func(cursor bool) (shown, dups, missed int, err error) {
		if err = r.postJSON(r.MonolithURL+"/_lab/config", map[string]any{"reset": true}, nil); err != nil {
			return
		}
		if err = r.postJSON(r.MonolithURL+"/_lab/prepare",
			map[string]any{"first_order_id": cfg.FirstOrderID}, nil); err != nil {
			return
		}
		if err = seed(total); err != nil {
			return
		}

		seen := map[int64]int{}
		next := int64(0)
		for p := 0; p < pages; p++ {
			if p > 0 {
				// Список меняется под читателем — ровно то, что происходит в
				// работающей системе, пока пользователь листает.
				if err = seed(inserts); err != nil {
					return
				}
			}
			url := fmt.Sprintf("%s/orders?limit=%d&offset=%d", r.MonolithURL, limit, p*limit)
			if cursor {
				url = fmt.Sprintf("%s/orders?limit=%d&mode=cursor", r.MonolithURL, limit)
				if p > 0 {
					url = fmt.Sprintf("%s/orders?limit=%d&after=%d", r.MonolithURL, limit, next)
				}
			}
			var resp pageResp
			if err = r.getJSON(url, &resp); err != nil {
				return
			}
			for _, it := range resp.Items {
				shown++
				if seen[it.ID] > 0 {
					dups++
				}
				seen[it.ID]++
			}
			next = resp.Next
		}

		// Не показано — из тех записей, что лежали в списке к началу листания.
		// Появившиеся во время листания в счёт не идут: их читатель и не
		// обещал увидеть.
		for id := cfg.FirstOrderID; id < cfg.FirstOrderID+int64(total); id++ {
			if seen[id] == 0 {
				missed++
			}
		}
		return
	}

	r.T0 = time.Now()

	shown, dups, missed, err := scroll(false)
	if err != nil {
		return err
	}
	r.Set("o_shown", strconv.Itoa(shown))
	r.Set("o_dups", strconv.Itoa(dups))
	r.Set("o_missed", strconv.Itoa(missed))
	r.Measure("offset", "shown", float64(shown))
	r.Measure("offset", "dups", float64(dups))
	r.Measure("offset", "missed", float64(missed))

	shown, dups, missed, err = scroll(true)
	if err != nil {
		return err
	}
	r.Set("c_shown", strconv.Itoa(shown))
	r.Set("c_dups", strconv.Itoa(dups))
	r.Set("c_missed", strconv.Itoa(missed))
	r.Measure("cursor", "shown", float64(shown))
	r.Measure("cursor", "dups", float64(dups))
	r.Measure("cursor", "missed", float64(missed))
	return nil
}
