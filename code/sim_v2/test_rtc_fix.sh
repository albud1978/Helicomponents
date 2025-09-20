#!/bin/bash
# Тестовый скрипт для проверки исправлений RTC компиляции

echo "========================================="
echo "Тест исправлений RTC компиляции в sim_v2"
echo "========================================="

# Включаем подробный вывод
export PYTHONUNBUFFERED=1

# Тест 1: 03_add_probe_mp5.py
echo ""
echo "Тест 1: 03_add_probe_mp5.py"
echo "-----------------------------------------"
python3 -u code/sim_v2/03_add_probe_mp5.py 2>&1 | grep -E "MP5Probe|ERROR|error|compilation"

# Тест 2: 04_add_status_246.py с малым количеством шагов
echo ""
echo "Тест 2: 04_add_status_246.py (5 шагов)"
echo "-----------------------------------------"
HL_V2_STEPS=5 python3 -u code/sim_v2/04_add_status_246.py 2>&1 | grep -E "Status246|ERROR|error|compilation"

# Тест 3: 99_debug_probe_mp5.py
echo ""
echo "Тест 3: 99_debug_probe_mp5.py"
echo "-----------------------------------------"
python3 -u code/sim_v2/99_debug_probe_mp5.py 2>&1 | grep -E "COMPILED|ERROR|error|compilation"

echo ""
echo "========================================="
echo "Тесты завершены"
echo "========================================="
echo ""
echo "Если все тесты прошли успешно, примените аналогичные изменения"
echo "в вашем файле 04_status246_orchestrator.py:"
echo ""
echo "1. Добавьте константу TOTAL_SIZE перед использованием getMacroProperty"
echo "2. Замените выражения в шаблонных параметрах на константу"
echo ""
echo "См. подробности в code/sim_v2/fix_rtc_compilation.md"