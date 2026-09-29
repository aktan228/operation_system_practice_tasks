#!/usr/bin/env bash
set -euo pipefail

usage() {
    printf 'Использование: %s <каталог> <ERROR|WARN> [--top N]\n' "$0" >&2
    printf 'Пример: %s data ERROR --top 3\n' "$0" >&2
}

fail() {
    printf 'Ошибка: %s\n' "$1" >&2
    usage
    exit 1
}

if [[ $# -ne 2 && $# -ne 4 ]]; then
    fail "неверное число аргументов"
fi

dir=$1
level=$2
top=0

[[ -d "$dir" ]] || fail "каталог не существует: $dir"
[[ -r "$dir" && -x "$dir" ]] || fail "нет доступа к каталогу: $dir"

case "$level" in
    ERROR|WARN) ;;
    *) fail "уровень должен быть ERROR или WARN" ;;
esac

if [[ $# -eq 4 ]]; then
    [[ "$3" == "--top" ]] || fail "неизвестный параметр: $3"
    [[ "$4" =~ ^[1-9][0-9]*$ ]] || fail "N должно быть целым положительным числом"
    top=$4
fi

shopt -s nullglob dotglob

files=()
for file in "$dir"/*.log; do
    [[ -f "$file" ]] || continue
    [[ -r "$file" ]] || fail "нет доступа к файлу: $file"
    files+=("$file")
done

printf '%-12s %s\n' "Модуль" "Сообщений"

if [[ ${#files[@]} -eq 0 ]]; then
    printf 'В каталоге нет файлов .log.\n' >&2
    exit 0
fi

cat -- "${files[@]}" |
    awk -v level="$level" '
        $3 == level && $4 ~ /^module=./ {
            module = $4
            sub(/^module=/, "", module)
            count[module]++
        }
        END {
            for (module in count)
                printf "%s %d\n", module, count[module]
        }
    ' |
    LC_ALL=C sort -k2,2nr -k1,1 |
    awk -v top="$top" '
        top == 0 || NR <= top {
            printf "%-12s %d\n", $1, $2
        }
    '
