package main

import (
	"fmt"
	"strings"
)

// Вторая форма вывода сцены — таблица замеров. Timeline читается там, где
// предмет есть обмен; там, где предмет есть число под нагрузкой, лента
// бесполезна: студенту нужен столбец, который растёт.
//
//	Нагрузка   Утилизация   В очереди   p50      p99
//	  20 rps          20%           0   41 мс    44 мс
//
// Правило то же, что у timeline: печатаются сценарные значения, наблюдение
// сверяется с ними по допуску. Иначе вывод студента и текст шага разошлись бы
// на первом же прогоне.

func renderTable(t Table, fields ...map[string]string) []string {
	// Заголовки столбцов тоже подставляются: число из конфигурации сцены
	// («не показано из 30») принадлежит шапке таблицы не меньше, чем ячейкам,
	// и дублировать его руками значит однажды забыть.
	titles := make([]string, len(t.Columns))
	widths := make([]int, len(t.Columns))
	for i, c := range t.Columns {
		titles[i] = substitute(c.Title, fields...)
		widths[i] = width(titles[i])
	}
	cells := make([][]string, len(t.Rows))
	for ri, r := range t.Rows {
		if r.Label != "" {
			continue
		}
		cells[ri] = make([]string, len(r.Cells))
		for ci, raw := range r.Cells {
			v := substitute(raw, fields...)
			cells[ri][ci] = v
			if ci < len(widths) && width(v) > widths[ci] {
				widths[ci] = width(v)
			}
		}
	}

	total := 0
	for _, w := range widths {
		total += w + 2
	}
	if total > 2 {
		total -= 2
	}

	out := []string{header(titles, t.Columns, widths)}
	for ri, r := range t.Rows {
		if r.Rule {
			out = append(out, strings.Repeat("─", total))
		}
		if r.Label != "" {
			out = append(out, substitute(r.Label, fields...))
			continue
		}
		var b strings.Builder
		for ci := range cells[ri] {
			if ci >= len(widths) {
				break
			}
			b.WriteString(cell(cells[ri][ci], widths[ci], t.Columns[ci].Right))
			if ci < len(widths)-1 {
				b.WriteString("  ")
			}
		}
		out = append(out, strings.TrimRight(b.String(), " "))
	}
	return out
}

func header(titles []string, cols []Column, widths []int) string {
	var b strings.Builder
	for i, c := range cols {
		b.WriteString(cell(titles[i], widths[i], c.Right))
		if i < len(cols)-1 {
			b.WriteString("  ")
		}
	}
	return strings.TrimRight(b.String(), " ")
}

func cell(s string, w int, right bool) string {
	gap := w - width(s)
	if gap < 0 {
		gap = 0
	}
	if right {
		return strings.Repeat(" ", gap) + s
	}
	return s + strings.Repeat(" ", gap)
}

// verifyTable сверяет наблюдения со сценарием и возвращает расхождения и
// строки сверки для `--explain`.
func verifyTable(t Table, measured map[string]map[string]float64) ([]string, []string) {
	var problems, chrono []string
	for _, r := range t.Rows {
		if len(r.Checks) == 0 {
			continue
		}
		obs, seen := measured[r.Key]
		if !seen {
			problems = append(problems, fmt.Sprintf("замер %s не сделан", r.Key))
			continue
		}
		for _, c := range r.Checks {
			got, ok := obs[c.Field]
			if !ok {
				problems = append(problems, fmt.Sprintf("%s: поле %s не измерено", r.Key, c.Field))
				continue
			}
			name := r.Key + "." + c.Field
			switch {
			case c.Min != nil && got < *c.Min:
				problems = append(problems, fmt.Sprintf(
					"%s: наблюдение %.1f, а сцена требует не меньше %.1f", name, got, *c.Min))
			case c.Max != nil && got > *c.Max:
				problems = append(problems, fmt.Sprintf(
					"%s: наблюдение %.1f, а сцена требует не больше %.1f", name, got, *c.Max))
			case c.Tol > 0 || c.TolPct > 0:
				tol := c.Tol
				if p := c.Value * c.TolPct / 100; p > tol {
					tol = p
				}
				if d := got - c.Value; d < -tol || d > tol {
					problems = append(problems, fmt.Sprintf(
						"%s: сценарий %.1f, наблюдение %.1f (допуск ±%.1f)", name, c.Value, got, tol))
				}
			}
			chrono = append(chrono, fmt.Sprintf("  %sсценарий %8.1f   наблюдение %8.1f",
				pad(name, 30), c.Value, got))
		}
	}
	return problems, chrono
}
