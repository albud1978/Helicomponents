#!/bin/bash
# Пример установки environment variables для ClickHouse
# Скопируйте этот файл в env.sh и установите свои значения

# ClickHouse connection settings
export CLICKHOUSE_HOST="10.95.19.132"
export CLICKHOUSE_PORT="9000" 
export CLICKHOUSE_DATABASE="default"
export CLICKHOUSE_USER="default"
export CLICKHOUSE_PASSWORD="your_password_here"

echo "✅ Environment variables установлены"
echo "🔗 Host: $CLICKHOUSE_HOST:$CLICKHOUSE_PORT"
echo "🗄️  Database: $CLICKHOUSE_DATABASE"
echo "👤 User: $CLICKHOUSE_USER"

# Для использования запустите:
# source env.sh 