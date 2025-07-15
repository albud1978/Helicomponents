#!/bin/bash
# Автозагрузка environment variables для проекта
# Поиск .env в нескольких возможных локациях для универсальности

# Список возможных путей к .env файлу в порядке приоритета
POSSIBLE_PATHS=(
    "$(pwd)"                     # Текущая директория
    "$(dirname "$(pwd)")"        # Родительская директория
    "$HOME"                      # Домашняя директория
)

# Если установлена переменная CUBE_CONFIG_PATH, добавляем её в начало списка
if [ -n "$CUBE_CONFIG_PATH" ]; then
    POSSIBLE_PATHS=("$CUBE_CONFIG_PATH" "${POSSIBLE_PATHS[@]}")
fi

# Поиск .env файла
ENV_FILE=""
for path in "${POSSIBLE_PATHS[@]}"; do
    if [ -f "$path/.env" ]; then
        ENV_FILE="$path/.env"
        break
    fi
done

if [ -n "$ENV_FILE" ]; then
    export $(cat "$ENV_FILE" | grep -v '^#' | xargs)
    echo "✅ Environment variables загружены из $ENV_FILE"
    
    # Проверка наличия критически важной переменной
    if [ -n "$CLICKHOUSE_PASSWORD" ]; then
        echo "🔒 CLICKHOUSE_PASSWORD установлен"
    else
        echo "⚠️ CLICKHOUSE_PASSWORD не найден в $ENV_FILE"
    fi
else
    echo "❌ Файл .env не найден в проверенных директориях:"
    for path in "${POSSIBLE_PATHS[@]}"; do
        echo "   - $path"
    done
    echo "Создайте файл .env с необходимыми переменными или укажите путь через CUBE_CONFIG_PATH"
    exit 1
fi
