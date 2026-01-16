# Отчёт валидации симуляции (MESSAGING)

**Дата отчёта:** 2026-01-16 18:25:10
**Датасет:** 2025-07-04
**Таблица:** sim_masterv2_v8

## Сводка

❌ **ВАЛИДАЦИЯ НЕ ПРОЙДЕНА**

| Проверка | Статус | Ошибки | Предупреждения |
|----------|--------|--------|----------------|
| Квоты ops vs target | ❌ | 24 | 2 |
| Матрица переходов | ✅ | 0 | 0 |
| Инкременты наработок | ❌ | 7 | 0 |

**Всего:** 31 ошибок, 2 предупреждений

## 1. Валидация квот

### MI8

| Категория | Дней | % |
|-----------|------|---|
| Точное соответствие | 265 | 98.5% |
| Предупреждения (<=180) | 1 | 0.4% |
| Ошибки (>180) | 3 | 1.1% |

### MI17

| Категория | Дней | % |
|-----------|------|---|
| Точное соответствие | 247 | 91.8% |
| Предупреждения (<=180) | 1 | 0.4% |
| Ошибки (>180) | 21 | 7.8% |

### Предупреждения по квотам (<=180 дней)

Показано: 2 из 2

- Day 0: mi8 ops=67 vs target=68 (delta=-1)
- Day 0: mi17 ops=87 vs target=88 (delta=-1)

### Ошибки по квотам (>180 дней)

Показано: 24 из 24

- Day 389: mi17 ops=103 vs target=104 (delta=-1)
- Day 577: mi17 ops=102 vs target=104 (delta=-2)
- Day 830: mi17 ops=114 vs target=115 (delta=-1)
- Day 1032: mi17 ops=121 vs target=123 (delta=-2)
- Day 1048: mi17 ops=122 vs target=123 (delta=-1)
- Day 1397: mi17 ops=126 vs target=127 (delta=-1)
- Day 1762: mi17 ops=126 vs target=127 (delta=-1)
- Day 1793: mi17 ops=127 vs target=129 (delta=-2)
- Day 1802: mi17 ops=128 vs target=129 (delta=-1)
- Day 1860: mi17 ops=126 vs target=127 (delta=-1)
- Day 1881: mi17 ops=126 vs target=127 (delta=-1)
- Day 1931: mi17 ops=126 vs target=127 (delta=-1)
- Day 1992: mi8 ops=46 vs target=47 (delta=-1)
- Day 2127: mi17 ops=126 vs target=127 (delta=-1)
- Day 2155: mi17 ops=126 vs target=127 (delta=-1)
- Day 2184: mi17 ops=128 vs target=129 (delta=-1)
- Day 2219: mi17 ops=126 vs target=127 (delta=-1)
- Day 2227: mi17 ops=126 vs target=127 (delta=-1)
- Day 2284: mi17 ops=126 vs target=127 (delta=-1)
- Day 2493: mi17 ops=126 vs target=127 (delta=-1)
- Day 2628: mi8 ops=46 vs target=47 (delta=-1)
- Day 3398: mi8 ops=46 vs target=47 (delta=-1)
- Day 3588: mi17 ops=126 vs target=127 (delta=-1)
- Day 3626: mi17 ops=128 vs target=129 (delta=-1)

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
