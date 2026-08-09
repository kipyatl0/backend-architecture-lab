#!/usr/bin/env bash
#
# Смоук стенда: каждая сцена, у которой есть эталонный вывод, прогоняется и
# сверяется с ним посимвольно. Расхождение — это сломанный шаг курса, а не
# мелочь: студент сверяет свой вывод с текстом шага.

set -euo pipefail

cd -- "$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/.."

failed=0
found=0

for ref in scenes/expected/scene-*.txt; do
  [ -e "$ref" ] || { echo "эталонных выводов нет — нечего сверять"; exit 1; }
  found=$((found + 1))

  id="$(basename "$ref" .txt)"; id="${id#scene-}"; id="$((10#$id))"

  echo "── сцена $id ──"
  actual="$(mktemp)"
  if ! ./lab scene "$id" > "$actual"; then
    echo "сцена $id завершилась с ошибкой"
    failed=$((failed + 1))
    rm -f "$actual"
    continue
  fi

  if diff -u "$ref" "$actual"; then
    echo "сцена $id совпала с эталоном"
  else
    echo "сцена $id РАЗОШЛАСЬ с эталоном ($ref)"
    failed=$((failed + 1))
  fi
  rm -f "$actual"
done

echo
if [ "$failed" -gt 0 ]; then
  echo "смоук провален: расхождений $failed из $found"
  exit 1
fi
echo "смоук пройден: сцен $found, все совпали с эталоном"
