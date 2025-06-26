#!/bin/bash
# Автозагрузка environment variables для проекта

if [ -f .env ]; then
    export $(cat .env | grep -v '^#' | xargs)
    echo "✅ Environment variables загружены из .env"
    echo "🔒 CLICKHOUSE_PASSWORD установлен"
else
    echo "❌ Файл .env не найден"
    exit 1
fi
