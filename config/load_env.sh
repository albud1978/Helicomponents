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

# Кэширование скомпилированных RTC ядер FLAME GPU
# Ускоряет повторные запуски симуляции (компиляция только при изменении кода)
# Определяем корень проекта относительно расположения этого скрипта
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
RTC_CACHE_DIR="$PROJECT_ROOT/.rtc_cache"
if [ ! -d "$RTC_CACHE_DIR" ]; then
    mkdir -p "$RTC_CACHE_DIR"
    echo "📁 Создана директория кэша RTC: $RTC_CACHE_DIR"
fi
export FLAMEGPU_RTC_EXPORT_CACHE_PATH="$RTC_CACHE_DIR"

# FLAMEGPU использует /tmp/flamegpu/jitifycache — создаём симлинк на персистентный кэш
if [ ! -L "/tmp/flamegpu/jitifycache" ]; then
    rm -rf /tmp/flamegpu/jitifycache 2>/dev/null
    mkdir -p /tmp/flamegpu
    ln -sf "$RTC_CACHE_DIR" /tmp/flamegpu/jitifycache
fi
echo "⚡ RTC кэш: $RTC_CACHE_DIR ($(ls "$RTC_CACHE_DIR" 2>/dev/null | wc -l) файлов)"

# CUDA configuration (универсальный — автоопределение)
# Приоритет: 1) conda с cuda-toolkit, 2) /usr/local/cuda, 3) системный nvcc
# Для машинно-специфичных настроек создайте load_env.local.sh (в gitignore)

if [ -f "$SCRIPT_DIR/load_env.local.sh" ]; then
    source "$SCRIPT_DIR/load_env.local.sh"
    echo "🔧 Локальные настройки загружены из load_env.local.sh"
elif [ -d "$HOME/miniconda3/targets/x86_64-linux/include" ]; then
    # Conda с CUDA toolkit (например, для RTX 5090 + CUDA 13)
    source "$HOME/miniconda3/etc/profile.d/conda.sh" 2>/dev/null
    if [ -d "$HOME/miniconda3/envs/cuda13" ]; then
        conda activate cuda13 2>/dev/null
        echo "🐍 Conda env: cuda13"
    else
        conda activate base 2>/dev/null
        echo "🐍 Conda env: base"
    fi
    export CUDA_PATH="$HOME/miniconda3/targets/x86_64-linux"
    export LD_LIBRARY_PATH="$HOME/miniconda3/lib:$LD_LIBRARY_PATH"
    echo "🚀 CUDA (conda): $CUDA_PATH"
elif [ -d "/usr/local/cuda" ]; then
    export CUDA_PATH="/usr/local/cuda"
    export LD_LIBRARY_PATH="$CUDA_PATH/lib64:$LD_LIBRARY_PATH"
    echo "🚀 CUDA (system): $CUDA_PATH"
else
    echo "⚠️ CUDA не найден. Установите CUDA Toolkit или укажите CUDA_PATH вручную"
fi
