package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
)

// Реестр сцен — данные, а не код: `lab scenes`, `--explain` и сам прогон
// читают один и тот же файл, поэтому каталог сцен и стенд не разъезжаются.

type Scene struct {
	ID            string   `json:"id"`
	Profile       string   `json:"profile"`
	Lesson        string   `json:"lesson"`
	Title         string   `json:"title"`
	Shows         string   `json:"shows"`
	Deterministic bool     `json:"deterministic"`
	DurationS     int      `json:"duration_s"`
	Driver        string   `json:"driver"`
	Script        string   `json:"script"`
	Explain       []string `json:"explain"`
}

type Registry struct {
	Scenes []Scene `json:"scenes"`
}

// Script — что сцена печатает. Времена в timeline — сценарные: прогон
// проверяет, что наблюдение укладывается в допуск, и печатает сценарное
// значение. Иначе один и тот же стенд давал бы студенту и автору курса
// разные числа, и сверять свой вывод с текстом шага стало бы нельзя.
type Script struct {
	ID       string            `json:"id"`
	Config   ScriptConfig      `json:"config"`
	Fields   map[string]string `json:"fields"`
	Header   []string          `json:"header"`
	Timeline []Line            `json:"timeline"`
	Summary  string            `json:"summary"`
}

type ScriptConfig struct {
	AcquirerDelayMS int     `json:"acquirer_delay_ms"`
	AcquirerMode    string  `json:"acquirer_mode"`
	Idempotent      bool    `json:"idempotent"`
	Amount          int64   `json:"amount"`
	Client          string  `json:"client"`
	Restaurant      string  `json:"restaurant"`
	FirstOrderID    int64   `json:"first_order_id"`
	ClientTimeoutMS int     `json:"client_timeout_ms"`
	RetryAtS        float64 `json:"retry_at_s"`
}

type Line struct {
	Key    string  `json:"key"`
	At     float64 `json:"at"`
	Tol    float64 `json:"tol"`
	From   string  `json:"from"`
	Arrow  string  `json:"arrow"`
	To     string  `json:"to"`
	Detail string  `json:"detail"`
	Note   string  `json:"note,omitempty"`
}

func loadRegistry(dir string) (*Registry, error) {
	raw, err := os.ReadFile(filepath.Join(dir, "registry.json"))
	if err != nil {
		return nil, fmt.Errorf("реестр сцен не прочитан: %w", err)
	}
	var reg Registry
	if err := json.Unmarshal(raw, &reg); err != nil {
		return nil, fmt.Errorf("реестр сцен испорчен: %w", err)
	}
	sort.SliceStable(reg.Scenes, func(i, j int) bool {
		a, _ := strconv.Atoi(reg.Scenes[i].ID)
		b, _ := strconv.Atoi(reg.Scenes[j].ID)
		return a < b
	})
	return &reg, nil
}

func (r *Registry) find(id string) (Scene, bool) {
	for _, s := range r.Scenes {
		if s.ID == id {
			return s, true
		}
	}
	return Scene{}, false
}

func loadScript(dir string, s Scene) (Script, error) {
	var sc Script
	raw, err := os.ReadFile(filepath.Join(dir, s.Script))
	if err != nil {
		return sc, fmt.Errorf("сценарий сцены %s не прочитан: %w", s.ID, err)
	}
	if err := json.Unmarshal(raw, &sc); err != nil {
		return sc, fmt.Errorf("сценарий сцены %s испорчен: %w", s.ID, err)
	}
	sort.SliceStable(sc.Timeline, func(i, j int) bool { return sc.Timeline[i].At < sc.Timeline[j].At })
	for i := range sc.Timeline {
		if sc.Timeline[i].Tol == 0 {
			sc.Timeline[i].Tol = 1.5
		}
	}
	return sc, nil
}
