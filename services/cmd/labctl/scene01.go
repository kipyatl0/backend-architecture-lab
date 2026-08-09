package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"time"
)

// Сцена 1 — m01 l04. Внешний провайдер отвечает 30 секунд, клиент ждать
// столько не готов и повторяет запрос. Монолит при этом не бросает начатое:
// обе попытки доходят до эквайринга, и заказ оплачивается дважды.
//
// Драйвер играет роль клиента и только её. Всё остальное — настоящая работа
// сервисов: заказы лежат в базе, списания — в журнале эквайринга.
func scene01(ctx context.Context, r *Run) error {
	cfg := r.Script.Config

	// Числа шапки и timeline берутся из конфигурации сцены, а не пишутся
	// в тексте дважды: «задержка 30s» ровно потому, что провайдеру велели 30s.
	r.Set("delay", ms(cfg.AcquirerDelayMS))
	r.Set("client_timeout", ms(cfg.ClientTimeoutMS))
	r.Set("first_order", strconv.FormatInt(cfg.FirstOrderID, 10))

	// 1. Подготовка: провайдеру задана задержка, состояние службы сброшено,
	//    следующий заказ получит номер из сценария, а не какой придётся.
	if err := r.postJSON(r.AcquirerURL+"/_lab/config", map[string]any{
		"delay_ms":   cfg.AcquirerDelayMS,
		"mode":       cfg.AcquirerMode,
		"idempotent": cfg.Idempotent,
		"reset":      true,
	}, nil); err != nil {
		return fmt.Errorf("эквайринг не настроен: %w", err)
	}
	if err := r.postJSON(r.MonolithURL+"/_lab/prepare", map[string]any{
		"first_order_id": cfg.FirstOrderID,
	}, nil); err != nil {
		return fmt.Errorf("состояние службы не сброшено: %w", err)
	}

	// Прогрев: соединения и пул к базе не должны попасть в хронометраж сцены.
	_ = r.getJSON(r.MonolithURL+"/health", nil)
	_ = r.getJSON(r.AcquirerURL+"/health", nil)

	// 2. Клиент. Ждёт ровно столько, сколько сказано в сценарии, — и это
	//    единственное, что он умеет: узнать исход он не может.
	client := &http.Client{Timeout: time.Duration(cfg.ClientTimeoutMS) * time.Millisecond}
	order, _ := json.Marshal(map[string]any{
		"client":     cfg.Client,
		"restaurant": cfg.Restaurant,
		"amount":     cfg.Amount,
	})

	r.T0 = time.Now()

	r.Record("client.call.orders#1", nil)
	if err := expectTimeout(client, r.MonolithURL+"/orders", order); err != nil {
		return fmt.Errorf("первая попытка: %w", err)
	}
	r.Record("client.timeout#1", nil)

	r.SleepUntil(cfg.RetryAtS)
	r.Record("client.call.orders#2", nil)
	if err := expectTimeout(client, r.MonolithURL+"/orders", order); err != nil {
		return fmt.Errorf("повтор: %w", err)
	}
	r.Record("client.timeout#2", nil)

	// 3. Клиент ушёл — работа продолжается. Ждём, пока обе попытки доиграют.
	if err := r.waitForScript(ctx); err != nil {
		return err
	}

	// 4. Итог берём не из сценария, а из системы: заказы из базы службы,
	//    списания — из журнала эквайринга.
	var state struct {
		Totals struct {
			Orders       int   `json:"orders"`
			Charged      int   `json:"charged"`
			ChargedTotal int64 `json:"charged_total"`
		} `json:"totals"`
	}
	if err := r.getJSON(r.MonolithURL+"/_lab/state", &state); err != nil {
		return err
	}
	var ledger struct {
		Charged      int   `json:"charged"`
		ChargedTotal int64 `json:"charged_total"`
	}
	if err := r.getJSON(r.AcquirerURL+"/_lab/ledger", &ledger); err != nil {
		return err
	}

	r.Set("orders", strconv.Itoa(state.Totals.Orders))
	r.Set("charges", strconv.Itoa(ledger.Charged))
	r.Set("charged_total", strconv.FormatInt(ledger.ChargedTotal, 10))
	r.Set("amount", strconv.FormatInt(cfg.Amount, 10))
	return nil
}

func ms(v int) string { return (time.Duration(v) * time.Millisecond).String() }

// expectTimeout — вызов от лица клиента, который ОБЯЗАН упереться в таймаут:
// для этой сцены таймаут не сбой, а её содержание. Любой другой исход
// означает, что сцена не воспроизвелась, и об этом надо сказать вслух.
func expectTimeout(c *http.Client, url string, body []byte) error {
	resp, err := c.Post(url, "application/json", bytes.NewReader(body))
	if err == nil {
		defer resp.Body.Close()
		return fmt.Errorf("клиент дождался ответа %d — проверь задержку эквайринга в сценарии",
			resp.StatusCode)
	}
	if !os.IsTimeout(err) {
		return fmt.Errorf("вместо таймаута клиент получил ошибку: %w", err)
	}
	return nil
}
