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
	ID     string            `json:"id"`
	Config ScriptConfig      `json:"config"`
	Fields map[string]string `json:"fields"`
	Header []string          `json:"header"`
	// Чем сцена печатает результат: timeline двух сторон обмена (по умолчанию)
	// или таблица замеров. Форма выбирается предметом: обмен читается лентой,
	// нагрузка — таблицей.
	Render   string `json:"render"`
	Timeline []Line `json:"timeline"`
	Table    Table  `json:"table"`
	// Наблюдения, которые сцена видит, но в ленту не печатает: обычно это
	// вторая сторона уже показанного обмена. Перечислять их обязательно —
	// иначе «вне сценария наблюдалось событие» перестанет ловить дрейф стенда.
	Ignore  []string `json:"ignore"`
	Summary string   `json:"summary"`
	Legend  []string `json:"legend"`
}

// ScriptConfig — все ручки, которыми сцена настраивает стенд. Числа сцены
// живут здесь и только здесь: в текст шага они попадают из эталонного вывода,
// а не переписываются руками.
type ScriptConfig struct {
	// эквайринг и заказ
	AcquirerDelayMS int     `json:"acquirer_delay_ms"`
	AcquirerMode    string  `json:"acquirer_mode"`
	Idempotent      bool    `json:"idempotent"`
	Amount          int64   `json:"amount"`
	Client          string  `json:"client"`
	Restaurant      string  `json:"restaurant"`
	FirstOrderID    int64   `json:"first_order_id"`
	ClientTimeoutMS int     `json:"client_timeout_ms"`
	RetryAtS        float64 `json:"retry_at_s"`

	// служба: обработчик, пул и очередь
	CatalogMS  int `json:"catalog_ms"`
	SlowEveryN int `json:"slow_every_n"`
	SlowMS     int `json:"slow_ms"`
	Workers    int `json:"workers"`
	Queue      int `json:"queue"`

	// нагрузка
	Steps       []int     `json:"steps"`
	StepSeconds float64   `json:"step_seconds"`
	RPS         int       `json:"rps"`
	Seconds     float64   `json:"seconds"`
	Buckets     []float64 `json:"buckets"`
	Slices      []float64 `json:"slices"`

	// уведомления и защиты
	NotifyMode      string `json:"notify_mode"`
	NotifyMS        int    `json:"notify_ms"`
	NotifyTimeoutMS int    `json:"notify_timeout_ms"`
	NotifyRetries   int    `json:"notify_retries"`
	NotifyRetryMS   int    `json:"notify_retry_ms"`
	ClientRetries   int    `json:"client_retries"`
	ProviderRetries int    `json:"provider_retries"`
	BreakerFails    int    `json:"breaker_fails"`
	BreakerOpenMS   int    `json:"breaker_open_ms"`

	// порча сети
	LatencyMS int `json:"latency_ms"`
	CutAfterB int `json:"cut_after_bytes"`

	// обмен сообщениями
	Messages       int     `json:"messages"`
	SecondPhaseAtS float64 `json:"second_phase_at_s"`
	Prefetch       int     `json:"prefetch"`
	Consumers      int     `json:"consumers"`
	DieAfter       int     `json:"die_after"`
	RestartMSValue int     `json:"restart_ms"`
	DeliveryLimit  int     `json:"delivery_limit"`
	PoisonOffset   int     `json:"poison_offset"`
	WindowMSValue  int     `json:"window_ms"`
	IdempotentCons bool    `json:"idempotent_consumer"`
	FailStep       string  `json:"fail_step"`
	WorkMS         int     `json:"work_ms"`

	// журнал репликации базы
	PreOrders int     `json:"pre_orders"` // строк, появившихся до того, как за журналом начали следить
	BulkAtS   float64 `json:"bulk_at_s"`
	DeleteAtS float64 `json:"delete_at_s"`
}

// Значения по умолчанию у ручек обмена — не «на всякий случай», а часть
// контракта: сцена задаёт только то, что для неё существенно, и в её сценарии
// не заводится строк, которые ничего не решают.

// RestartMS — через сколько умерший потребитель поднимается заново.
func (c ScriptConfig) RestartMS() int {
	if c.RestartMSValue <= 0 {
		return 800
	}
	return c.RestartMSValue
}

// WindowMS — окно наблюдения там, где предмет сцены есть «что успело
// произойти», а не «когда всё кончится». В сцене про отравленное сообщение
// без предела повторов оно не кончится никогда.
func (c ScriptConfig) WindowMS() int {
	if c.WindowMSValue <= 0 {
		return 4000
	}
	return c.WindowMSValue
}

// ── таблица замеров ─────────────────────────────────────────────────────────

type Table struct {
	Columns []Column `json:"columns"`
	Rows    []Row    `json:"rows"`
}

type Column struct {
	Title string `json:"title"`
	Right bool   `json:"right"`
}

// Row — строка таблицы. Cells печатаются как есть (сценарные значения),
// Checks сверяются с наблюдением: тот же контракт, что у timeline.
type Row struct {
	Key    string   `json:"key"`
	Cells  []string `json:"cells"`
	Checks []Check  `json:"checks"`
	Rule   bool     `json:"rule"`  // горизонтальная черта перед строкой
	Label  string   `json:"label"` // подпись блока вместо строки таблицы
}

type Check struct {
	Field  string   `json:"field"`
	Value  float64  `json:"value"`
	Tol    float64  `json:"tol"`
	TolPct float64  `json:"tol_pct"`
	Min    *float64 `json:"min"`
	Max    *float64 `json:"max"`
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
