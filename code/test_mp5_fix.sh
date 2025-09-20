#!/bin/bash
# Тестовый скрипт для проверки исправлений конфликта MP5

echo "========================================="
echo "Тест исправлений конфликта MP5"
echo "========================================="

# Включаем подробный вывод для отладки
export PYTHONUNBUFFERED=1
export HL_JIT_LOG=1

# Тест 1: Малый объем данных (без MacroProperty)
echo ""
echo "Тест 1: status246-smoke-real --status246-days 5 (малый объем, без MacroProperty)"
echo "-----------------------------------------"
python3 -u code/sim_master.py --status246-smoke-real --status246-days 5 --seatbelts on 2>&1 | grep -E "MP5|BUILD|timing_ms|ERROR|error"

# Тест 2: Большой объем данных (с MacroProperty)
echo ""
echo "Тест 2: status246-smoke-real --status246-days 100 (большой объем, с MacroProperty)"
echo "-----------------------------------------"
# Форсируем включение MacroProperty даже для малых данных для тестирования
export HL_USE_MP5_MACRO=1
python3 -u code/sim_master.py --status246-smoke-real --status246-days 100 --seatbelts on 2>&1 | grep -E "MP5|BUILD|timing_ms|ERROR|error"

# Тест 3: mp5_probe с MacroProperty
echo ""
echo "Тест 3: mp5_probe с MacroProperty"
echo "-----------------------------------------"
export HL_USE_MP5_MACRO=1
python3 -u code/sim_master.py --mp5-probe --seatbelts on 2>&1 | grep -E "MP5|BUILD|probe|ERROR|error"

echo ""
echo "========================================="
echo "Тесты завершены"
echo "========================================="