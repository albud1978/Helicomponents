# Validation (V2)

## Статусы и проверки

- Status 2 (эксплуатация)
  - Инкременты: sne += dt, ppr += dt, где dt = mp5(day, idx).
  - Прогноз: s_next = sne + dn, p_next = ppr + dn.
  - Переходы:
    - 2→6 при s_next >= ll (LL порог), s6_days=0, s6_started=1.
    - 2→4 при p_next >= oh и s_next < br (BR/repair ветка).

- Status 4 (ремонт)
  - repair_days растёт до repair_time.
  - Переход 4→5 при достижении repair_days >= repair_time.
  - assembly_time влияет на маркер сборки (планово D‑assembly_time до конца ремонта).

- Status 6 (хранение)
  - «Вечный»: значения повторяются, s6_days растёт только если s6_started=1.

## Логирование для smoke

- Итоговые счётчики по статусам: s2/s4/s6.
- Выборка нескольких бортов: (idx, status, sne, ppr, ll, oh, dt, dn).
- Трассировка одного idx по дням: (day, idx, dt, dn, sne, ppr, status).

## Источники данных

- MP5: линейный массив (DAYS+1)*FRAMES с паддингом, MacroProperty mp5_lin.
- MP3 пороги: ll, oh, br — сопоставлены по frames_index.

## Инварианты

- Длины массивов соответствуют DAYS/FRAMES.
- Индексация row = day*FRAMES + idx; row_next = row + FRAMES.
- Типы согласованы: UInt32 для MP5 и агентных накопителей.

