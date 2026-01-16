# Отчёт валидации симуляции (MESSAGING)

**Дата отчёта:** 2026-01-16 18:11:02
**Датасет:** 2025-07-04
**Таблица:** sim_masterv2_v8

## Сводка

❌ **ВАЛИДАЦИЯ НЕ ПРОЙДЕНА**

| Проверка | Статус | Ошибки | Предупреждения |
|----------|--------|--------|----------------|
| Квоты ops vs target | ✅ | 0 | 538 |
| Матрица переходов | ✅ | 0 | 0 |
| Инкременты наработок | ❌ | 7 | 0 |

**Всего:** 7 ошибок, 538 предупреждений

## 1. Валидация квот

### MI8

| Категория | Дней | % |
|-----------|------|---|
| Точное соответствие | 0 | 0.0% |
| Отклонение ±1 | 0 | 0.0% |
| Недобор 2-3 | 0 | 0.0% |
| Критичный >3 | 0 | 0.0% |
| Избыток | 269 | 100.0% |

### MI17

| Категория | Дней | % |
|-----------|------|---|
| Точное соответствие | 0 | 0.0% |
| Отклонение ±1 | 0 | 0.0% |
| Недобор 2-3 | 0 | 0.0% |
| Критичный >3 | 0 | 0.0% |
| Избыток | 269 | 100.0% |

### Примеры предупреждений (ops > target)

Показано: 50 из 538

- Day 0: mi8 ops=67 > target=0 (delta=+67)
- Day 0: mi17 ops=87 > target=0 (delta=+87)
- Day 3: mi8 ops=68 > target=0 (delta=+68)
- Day 3: mi17 ops=88 > target=0 (delta=+88)
- Day 11: mi8 ops=68 > target=0 (delta=+68)
- Day 11: mi17 ops=88 > target=0 (delta=+88)
- Day 23: mi8 ops=68 > target=0 (delta=+68)
- Day 23: mi17 ops=88 > target=0 (delta=+88)
- Day 26: mi8 ops=68 > target=0 (delta=+68)
- Day 26: mi17 ops=88 > target=0 (delta=+88)
- Day 28: mi8 ops=64 > target=0 (delta=+64)
- Day 28: mi17 ops=88 > target=0 (delta=+88)
- Day 49: mi8 ops=64 > target=0 (delta=+64)
- Day 49: mi17 ops=88 > target=0 (delta=+88)
- Day 63: mi8 ops=64 > target=0 (delta=+64)
- Day 63: mi17 ops=88 > target=0 (delta=+88)
- Day 73: mi8 ops=64 > target=0 (delta=+64)
- Day 73: mi17 ops=88 > target=0 (delta=+88)
- Day 89: mi8 ops=65 > target=0 (delta=+65)
- Day 89: mi17 ops=89 > target=0 (delta=+89)
- Day 103: mi8 ops=65 > target=0 (delta=+65)
- Day 103: mi17 ops=89 > target=0 (delta=+89)
- Day 120: mi8 ops=68 > target=0 (delta=+68)
- Day 120: mi17 ops=88 > target=0 (delta=+88)
- Day 128: mi8 ops=68 > target=0 (delta=+68)
- Day 128: mi17 ops=88 > target=0 (delta=+88)
- Day 150: mi8 ops=68 > target=0 (delta=+68)
- Day 150: mi17 ops=89 > target=0 (delta=+89)
- Day 160: mi8 ops=68 > target=0 (delta=+68)
- Day 160: mi17 ops=89 > target=0 (delta=+89)
- Day 181: mi8 ops=42 > target=0 (delta=+42)
- Day 181: mi17 ops=88 > target=0 (delta=+88)
- Day 183: mi8 ops=42 > target=0 (delta=+42)
- Day 183: mi17 ops=88 > target=0 (delta=+88)
- Day 209: mi8 ops=42 > target=0 (delta=+42)
- Day 209: mi17 ops=88 > target=0 (delta=+88)
- Day 212: mi8 ops=43 > target=0 (delta=+43)
- Day 212: mi17 ops=88 > target=0 (delta=+88)
- Day 240: mi8 ops=42 > target=0 (delta=+42)
- Day 240: mi17 ops=90 > target=0 (delta=+90)
- Day 271: mi8 ops=44 > target=0 (delta=+44)
- Day 271: mi17 ops=95 > target=0 (delta=+95)
- Day 301: mi8 ops=46 > target=0 (delta=+46)
- Day 301: mi17 ops=101 > target=0 (delta=+101)
- Day 308: mi8 ops=46 > target=0 (delta=+46)
- Day 308: mi17 ops=101 > target=0 (delta=+101)
- Day 331: mi8 ops=46 > target=0 (delta=+46)
- Day 331: mi17 ops=101 > target=0 (delta=+101)
- Day 332: mi8 ops=47 > target=0 (delta=+47)
- Day 332: mi17 ops=104 > target=0 (delta=+104)

## 2. Валидация переходов

### Статистика переходов

| Переход | Всего | Mi-8 | Mi-17 |
|---------|-------|------|-------|

## 3. Валидация инкрементов

### Агрегированный налёт

| Тип | Бортов | Σ часов | Ср. на борт |
|-----|--------|---------|-------------|
| Mi-8 | 163 | 244,327 | 1,498.9 |
| Mi-17 | 152 | 723,757 | 4,761.6 |

### Ошибки инкрементов (примеры)

Показано: 7 из 7

- delta_sne > 0 вне operations (prev_state=): 279 интервалов
- delta_sne > 0 вне operations (prev_state=reserve): 6 интервалов
- idx=0: delta_sne вне ops = 1600262
- idx=1: delta_sne вне ops = 1619963
- idx=2: delta_sne вне ops = 1199280
- idx=3: delta_sne вне ops = 677061
- idx=4: delta_sne вне ops = 1362161
