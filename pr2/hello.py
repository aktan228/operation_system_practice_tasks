#!/usr/bin/env python3
import sys

if len(sys.argv) != 2:
    print("Использование: python3 hello.py <файл>", file=sys.stderr)
    sys.exit(1)

try:
    with open(sys.argv[1], "rb") as file:
        content = file.read()
except OSError as error:
    print(f"Ошибка чтения файла: {error}", file=sys.stderr)
    sys.exit(1)

print(len(content))
