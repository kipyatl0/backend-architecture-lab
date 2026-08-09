package main

import (
	"fmt"
	"strings"
	"unicode/utf8"
)

// Формат вывода сцены — timeline двух сторон обмена. Курс с первого урока
// учит читать ровно его, поэтому формат здесь жёсткий:
//
//	t=0.000  client    → orders     POST /orders {idempotency_key: none}
//	t=0.005  payments  ⋯ acquirer   POST /v1/charge            [задержка 30s]
//
//   · слева от стрелки — тот, кто наблюдал событие, справа — вторая сторона;
//   · → вызов, ← ответ, ⋯ незавершённое ожидание, ✗ отказ или таймаут;
//   · время — от начала сцены, всегда три знака после запятой;
//   · последняя строка — итог сцены обычными словами;
//   · никаких ANSI-цветов: вывод копируется в текст шага как есть.

type row struct {
	At     float64
	From   string
	Arrow  string
	To     string
	Detail string
	Note   string
}

func renderTimeline(rows []row) []string {
	timeW, fromW, toW, detailW := 9, 9, 10, 0
	tokens := make([]string, len(rows))
	for i, r := range rows {
		tokens[i] = fmt.Sprintf("t=%.3f", r.At)
		if n := width(tokens[i]) + 1; n > timeW {
			timeW = n
		}
		if n := width(r.From); n > fromW {
			fromW = n
		}
		if n := width(r.To); n > toW {
			toW = n
		}
		if r.Note != "" {
			if n := width(r.Detail); n > detailW {
				detailW = n
			}
		}
	}
	if detailW > 0 && detailW < 27 {
		detailW = 27
	}

	out := make([]string, 0, len(rows))
	for i, r := range rows {
		var b strings.Builder
		b.WriteString(pad(tokens[i], timeW))
		b.WriteString(pad(r.From, fromW))
		b.WriteString(" ")
		b.WriteString(r.Arrow)
		b.WriteString(" ")
		b.WriteString(pad(r.To, toW))
		b.WriteString(" ")
		if r.Note != "" {
			b.WriteString(pad(r.Detail, detailW))
			b.WriteString(" ")
			b.WriteString(r.Note)
		} else {
			b.WriteString(r.Detail)
		}
		out = append(out, strings.TrimRight(b.String(), " "))
	}
	return out
}

// width считает знаки, а не байты: половина вывода — кириллица.
func width(s string) int { return utf8.RuneCountInString(s) }

func pad(s string, w int) string {
	if n := width(s); n < w {
		return s + strings.Repeat(" ", w-n)
	}
	return s + " "
}

// substitute подставляет {{поле}}. Числа и идентификаторы в строке timeline —
// настоящие: они пришли из базы и из журнала эквайринга, а не из сценария.
func substitute(tpl string, fields ...map[string]string) string {
	out := tpl
	for _, f := range fields {
		for k, v := range f {
			out = strings.ReplaceAll(out, "{{"+k+"}}", v)
		}
	}
	return out
}
