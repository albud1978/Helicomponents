#!/usr/bin/env python3
"""
Подготовка окружения симуляции: загрузка данных MP/Property, формирование массивов дня.
Дата: 2025-08-21
"""

from __future__ import annotations
from typing import Dict, List, NamedTuple, Tuple
from datetime import date

import os
import sys
import logging

sys.path.append(os.path.join(os.path.dirname(__file__), 'utils'))
from config_loader import get_clickhouse_client

SECOND_LL_SENTINEL = 0xFFFFFFFF
def _to_uint(value) -> int:
    """Безопасное преобразование значения к UInt (NULL/NaN → 0)."""
    if value is None:
        return 0
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return 0


class Mp1Maps(NamedTuple):
    mp1_map: Dict[int, Tuple[int, int, int, int, int, int]]
    mp1_oh_map: Dict[int, Tuple[int, int]]
    mp1_ll_map: Dict[int, int]
    mp1_ll_mi8_map: Dict[int, int]
    mp1_second_ll_map: Dict[int, int]
    mp1_sne_ppr_map: Dict[int, Tuple[int, int]]
    mp1_repair_number_map: Dict[int, int]


def get_client():
    return get_clickhouse_client()


def list_available_versions(client) -> List[Tuple[date, int, int]]:
    """
    Возвращает список доступных версий данных.
    
    Returns:
        List[(version_date, version_id, record_count)]
    """
    rows = client.execute("""
        SELECT version_date, version_id, count(*) as cnt
        FROM heli_pandas
        GROUP BY version_date, version_id
        ORDER BY version_date DESC, version_id DESC
    """)
    return [(row[0], int(row[1]), int(row[2])) for row in rows]


def select_version_interactive(client) -> Tuple[date, int]:
    """
    Интерактивный выбор версии данных для симуляции.
    
    Returns:
        (version_date, version_id)
    """
    versions = list_available_versions(client)
    
    if not versions:
        raise ValueError("❌ Нет доступных версий данных в heli_pandas!")
    
    if len(versions) == 1:
        vd, vid, cnt = versions[0]
        print(f"📅 Единственная версия данных: {vd} (v{vid}, {cnt:,} записей)")
        return vd, vid
    
    print("\n" + "=" * 60)
    print("📅 ВЫБОР ВЕРСИИ ДАННЫХ ДЛЯ СИМУЛЯЦИИ")
    print("=" * 60)
    
    for i, (vd, vid, cnt) in enumerate(versions, 1):
        print(f"  {i}. {vd} (version_id={vid}, {cnt:,} записей)")
    
    print("  0. ❌ Отмена")
    print("=" * 60)
    
    while True:
        try:
            choice = input(f"\nВыберите версию (0-{len(versions)}): ").strip()
            if choice == '0':
                raise KeyboardInterrupt("Отменено пользователем")
            idx = int(choice) - 1
            if 0 <= idx < len(versions):
                vd, vid, cnt = versions[idx]
                print(f"✅ Выбрана версия: {vd} (v{vid}, {cnt:,} записей)")
                return vd, vid
            else:
                print(f"❌ Неверный выбор. Введите число от 0 до {len(versions)}")
        except ValueError:
            print("❌ Введите число")
        except KeyboardInterrupt:
            raise


def fetch_versions(
    client,
    target_version_date: date = None,
    target_version_id: int = None,
) -> Tuple[date, int]:
    """
    Получает версию данных.
    
    Args:
        client: ClickHouse client
        target_version_date: Конкретная дата версии (опционально). 
                             Если None — берёт самую последнюю.
        target_version_id: Конкретный version_id для target_version_date (опционально).
    
    Returns:
        (version_date, version_id)
    """
    if target_version_date is not None and target_version_id is not None:
        # Явный version_id должен читать ровно заданную пару, без fallback на MAX(version_id).
        rows = client.execute(f"""
            SELECT version_date, version_id
            FROM heli_pandas
            WHERE version_date = '{target_version_date}'
              AND version_id = {target_version_id}
            LIMIT 1
        """)
        if not rows:
            raise ValueError(f"❌ Версия {target_version_date} v{target_version_id} не найдена в heli_pandas!")
    elif target_version_date is not None:
        # Ищем конкретную версию
        rows = client.execute(f"""
            SELECT version_date, version_id 
            FROM heli_pandas 
            WHERE version_date = '{target_version_date}'
            ORDER BY version_id DESC 
            LIMIT 1
        """)
        if not rows:
            raise ValueError(f"❌ Версия {target_version_date} не найдена в heli_pandas!")
    else:
        # Берём последнюю версию
        rows = client.execute(
            "SELECT version_date, version_id FROM heli_pandas ORDER BY version_date DESC, version_id DESC LIMIT 1"
        )
    
    vd, vid = rows[0]
    return vd, int(vid)


def fetch_mp1_all(client) -> Mp1Maps:
    """Возвращает все MP1-карты из md_components одним SELECT по partseqno_i."""
    rows = client.execute("""
SELECT toUInt32OrZero(toString(partseqno_i)) AS partseq,
  toUInt32OrZero(toString(br_mi8)) AS br_mi8,
  toUInt32OrZero(toString(br_mi17)) AS br_mi17,
  toUInt32OrZero(toString(ifNull(br2_mi17,0))) AS br2_mi17,
  toUInt32OrZero(toString(repair_time)) AS repair_time,
  toUInt32OrZero(toString(partout_time)) AS partout_time,
  toUInt32OrZero(toString(assembly_time)) AS assembly_time,
  toUInt32OrZero(toString(oh_mi8)) AS oh_mi8,
  toUInt32OrZero(toString(oh_mi17)) AS oh_mi17,
  toUInt32OrZero(toString(ll_mi17)) AS ll_mi17,
  toUInt32OrZero(toString(ll_mi8)) AS ll_mi8,
  second_ll,
  repair_number,
  CASE WHEN sne_new IS NULL THEN 4294967295 ELSE toUInt32OrZero(toString(sne_new)) END AS sne_new,
  CASE WHEN ppr_new IS NULL THEN 4294967295 ELSE toUInt32OrZero(toString(ppr_new)) END AS ppr_new
FROM md_components
""")
    mp1_map: Dict[int, Tuple[int, int, int, int, int, int]] = {}
    mp1_oh_map: Dict[int, Tuple[int, int]] = {}
    mp1_ll_map: Dict[int, int] = {}
    mp1_ll_mi8_map: Dict[int, int] = {}
    mp1_second_ll_map: Dict[int, int] = {}
    mp1_sne_ppr_map: Dict[int, Tuple[int, int]] = {}
    mp1_repair_number_map: Dict[int, int] = {}
    repair_number_non_sentinel = 0

    for (
        p,
        br_mi8,
        br_mi17,
        br2_mi17,
        repair_time,
        partout_time,
        assembly_time,
        oh_mi8,
        oh_mi17,
        ll_mi17,
        ll_mi8,
        second_ll,
        repair_number,
        sne_new,
        ppr_new,
    ) in rows:
        partseq = _to_uint(p)
        if partseq == 0:
            continue
        mp1_map[partseq] = (
            int(br_mi8 or 0),
            int(br_mi17 or 0),
            int(br2_mi17 or 0),
            int(repair_time or 0),
            int(partout_time or 0),
            int(assembly_time or 0),
        )
        mp1_oh_map[partseq] = (int(oh_mi8 or 0), int(oh_mi17 or 0))
        mp1_ll_map[partseq] = int(ll_mi17 or 0)
        mp1_ll_mi8_map[partseq] = int(ll_mi8 or 0)
        mp1_second_ll_map[partseq] = SECOND_LL_SENTINEL if second_ll is None else int(second_ll)
        mp1_sne_ppr_map[partseq] = (int(sne_new), int(ppr_new))
        repair_number_value = 255 if repair_number is None else int(repair_number)
        mp1_repair_number_map[partseq] = repair_number_value
        if repair_number_value > 0 and repair_number_value != 255:
            repair_number_non_sentinel += 1

    print(
        f"  📊 fetch_mp1_all repair_number: загружено {len(mp1_repair_number_map)} записей, "
        f"из них {repair_number_non_sentinel} с repair_number > 0 и != 255"
    )
    return Mp1Maps(
        mp1_map=mp1_map,
        mp1_oh_map=mp1_oh_map,
        mp1_ll_map=mp1_ll_map,
        mp1_ll_mi8_map=mp1_ll_mi8_map,
        mp1_second_ll_map=mp1_second_ll_map,
        mp1_sne_ppr_map=mp1_sne_ppr_map,
        mp1_repair_number_map=mp1_repair_number_map,
    )


def fetch_mp3(client, vdate: date, vid: int):
    fields = [
        'partseqno_i','psn','aircraft_number','ac_type_mask','group_by','status_id',
        'll','oh','oh_threshold','sne','ppr','repair_days','repair_time','mfg_date','version_date'
    ]
    sql = f"""
    SELECT {', '.join(fields)}
    FROM heli_pandas
    WHERE version_date = '{vdate}' AND version_id = {vid}
    ORDER BY psn
    """
    rows = client.execute(sql)
    return rows, fields


def preload_mp4_by_day(client, version_date: date = None, version_id: int = None) -> Dict[date, Dict[str,int]]:
    """
    Загружает данные программы (quota target) из flight_program_ac.
    
    Args:
        client: ClickHouse client
        version_date: Дата версии данных для фильтрации. Если None — берёт последнюю.
        version_id: Идентификатор версии. Если None — берёт max(version_id) для version_date
                    (консистентно с heli_pandas/fetch_versions для мультиверсионных дат).
    """
    # Определяем version_date если не передан
    if version_date is None:
        result = client.execute("SELECT MAX(version_date) FROM flight_program_ac")
        version_date = result[0][0] if result and result[0][0] else date.today()
    # version_id-aware чтение: для мультиверсионных дат (например 2026-06-22 с v1+v2)
    # фильтр только по version_date вернул бы обе версии → неоднозначный прогон.
    if version_id is None:
        r = client.execute(
            f"SELECT MAX(version_id) FROM flight_program_ac WHERE version_date = '{version_date}'"
        )
        version_id = int(r[0][0]) if r and r[0][0] is not None else 1
    
    rows = client.execute(
        f"""
        SELECT dates, ops_counter_mi8, ops_counter_mi17, trigger_program_mi8, trigger_program_mi17, new_counter_mi17, spawn_limit, spawn_limit_active
        FROM flight_program_ac
        WHERE version_date = '{version_date}' AND version_id = {version_id}
        ORDER BY dates
        """
    )
    result: Dict[date, Dict[str,int]] = {}
    for d, mi8, mi17, t8, t17, n17, spawn_limit, spawn_limit_active in rows:
        result[d] = {
            "ops_counter_mi8": int(mi8 or 0),
            "ops_counter_mi17": int(mi17 or 0),
            "trigger_program_mi8": int(t8 or 0),
            "trigger_program_mi17": int(t17 or 0),
            "new_counter_mi17": int(n17 or 0),
            "spawn_limit": int(spawn_limit or 0),
            # PRESENCE-based флаг (одинаков для всех строк версии): 1 если строка spawn_limit
            # присутствовала в Program_heli.xlsx (даже при нулях), 0 — backward-compat.
            "spawn_limit_active": int(spawn_limit_active or 0),
        }
    return result


def preload_mp5_maps(client, version_date: date = None, version_id: int = None) -> Dict[date, Dict[int,int]]:
    """
    Загружает данные программы полётов из flight_program_fl.
    
    Args:
        client: ClickHouse client
        version_date: Дата версии данных для фильтрации. Если None — берёт последнюю.
        version_id: Идентификатор версии. Если None — берёт max(version_id) для version_date
                    (консистентно с heli_pandas/fetch_versions для мультиверсионных дат).
    """
    # Определяем version_date если не передан
    if version_date is None:
        result = client.execute("SELECT MAX(version_date) FROM flight_program_fl")
        version_date = result[0][0] if result and result[0][0] else date.today()
    # version_id-aware чтение (см. preload_mp4_by_day): иначе мультиверсионная дата
    # вернёт несколько версий программы полётов.
    if version_id is None:
        r = client.execute(
            f"SELECT MAX(version_id) FROM flight_program_fl WHERE version_date = '{version_date}'"
        )
        version_id = int(r[0][0]) if r and r[0][0] is not None else 1
    
    dates_col, ac_col, hours_col = client.execute(
        f"""
        SELECT dates, aircraft_number, daily_hours
        FROM flight_program_fl
        WHERE version_date = '{version_date}' AND version_id = {version_id}
        ORDER BY dates, aircraft_number
        """,
        columnar=True,
    )
    result: Dict[date, Dict[int,int]] = {}
    for d, ac, h in zip(dates_col, ac_col, hours_col):
        m = result.setdefault(d, {})
        m[int(ac)] = int(h or 0)
    
    if not result:
        logging.warning(f"⚠️ flight_program_fl пуст для version_date={version_date}")
    else:
        logging.info(f"✅ MP5: загружено {len(result)} дней для version_date={version_date}")
    
    return result


def build_daily_arrays(mp3_rows, mp3_fields: List[str], mp1_br_rt_map: Dict[int, Tuple[int,int,int,int,int]], daily_today_map: Dict[int,int], daily_next_map: Dict[int,int]) -> Tuple[List[int], List[int], List[int], List[int]]:
    idx = {name: i for i, name in enumerate(mp3_fields)}
    daily_today: List[int] = []
    daily_next: List[int] = []
    partout_arr: List[int] = []
    assembly_arr: List[int] = []
    for r in mp3_rows:
        ac = int(r[idx['aircraft_number']] or 0)
        daily_today.append(int(daily_today_map.get(ac, 0)))
        daily_next.append(int(daily_next_map.get(ac, 0)))
        partseq = int(r[idx['partseqno_i']] or 0)
        # карта теперь (br_mi8, br_mi17, br2_mi17, repair_time, partout_time, assembly_time)
        _, _, _, _, pt, at = mp1_br_rt_map.get(partseq, (0,0,0,0,0,0))
        partout_arr.append(int(pt))
        assembly_arr.append(int(at))
    return daily_today, daily_next, partout_arr, assembly_arr


# === Full‑GPU подготовка окружения (Этап 0) ===

def build_frames_index(mp3_rows, mp3_fields: List[str]) -> Tuple[Dict[int, int], int]:
    """
    Строит frames_index с сортировкой по mfg_date (старые первые)
    Разделяет на Mi-8 и Mi-17: сначала все Mi-8 по возрасту, потом все Mi-17
    """
    from datetime import date as _date
    epoch = _date(1970, 1, 1)
    
    idx = {name: i for i, name in enumerate(mp3_fields)}
    
    # Собираем планеры с метаданными
    planes_mi8 = []  # (aircraft_number, mfg_date_days)
    planes_mi17 = []
    
    for r in mp3_rows:
        ac = int(r[idx['aircraft_number']] or 0)
        if ac <= 0:
            continue
        
        # Определяем тип и фильтруем
        gb = None
        if 'group_by' in idx:
            gb = int(r[idx['group_by']] or 0)
            if gb not in (1, 2):
                continue  # Пропускаем не-планеры
        elif 'ac_type_mask' in idx:
            m = int(r[idx['ac_type_mask']] or 0)
            if m & 32:  # Mi-8
                gb = 1
            elif m & 64:  # Mi-17
                gb = 2
            else:
                continue  # Не планер
        else:
            continue  # Нет признаков типа — пропускаем
        
        # Получаем mfg_date
        mfg_date_days = 0
        if 'mfg_date' in idx:
            md = r[idx['mfg_date']]
            if md:
                try:
                    mfg_date_days = max(0, int((md - epoch).days))
                except Exception:
                    mfg_date_days = 0
        
        # Добавляем в соответствующий список
        if gb == 1:
            planes_mi8.append((ac, mfg_date_days))
        elif gb == 2:
            planes_mi17.append((ac, mfg_date_days))
    
    # Убираем дубликаты и сортируем по mfg_date (старые первые)
    planes_mi8_unique = {ac: mfg for ac, mfg in planes_mi8}
    planes_mi17_unique = {ac: mfg for ac, mfg in planes_mi17}
    
    sorted_mi8 = sorted(planes_mi8_unique.items(), key=lambda x: (x[1], x[0]))  # (mfg_date, ac)
    sorted_mi17 = sorted(planes_mi17_unique.items(), key=lambda x: (x[1], x[0]))
    
    # Объединяем: сначала Mi-8, потом Mi-17
    ac_list_sorted = [ac for ac, _ in sorted_mi8] + [ac for ac, _ in sorted_mi17]
    
    # Создаём frames_index
    frames_index = {ac: i for i, ac in enumerate(ac_list_sorted)}
    
    print(f"  build_frames_index: Mi-8={len(sorted_mi8)}, Mi-17={len(sorted_mi17)}, total={len(ac_list_sorted)}")
    
    return frames_index, len(ac_list_sorted)


def get_days_sorted_union(mp4_by_day: Dict[date, Dict[str, int]], mp5_by_day: Dict[date, Dict[int,int]]) -> List[date]:
    days = set(mp4_by_day.keys())
    days.update(mp5_by_day.keys())
    return sorted(days)


def build_mp5_linear(mp5_by_day: Dict[date, Dict[int, int]], days_sorted: List[date], frames_index: Dict[int, int], frames_total: int, frames_total_base: int = None) -> List[int]:
    """
    Строит линейный массив MP5 для всех агентов.
    
    Для существующих агентов (idx < frames_total_base) берёт данные из mp5_by_day.
    Для зарезервированных слотов (idx >= frames_total_base) заполняет средним налётом по всем агентам.
    """
    days_total = len(days_sorted)
    # Паддинг D+1 в конце
    size = (days_total + 1) * frames_total
    arr = [0] * size
    
    # Заполняем для существующих агентов
    for d_idx, D in enumerate(days_sorted):
        by_ac = mp5_by_day.get(D, {})
        base = d_idx * frames_total
        for ac, hours in by_ac.items():
            fi = frames_index.get(int(ac), -1)
            if fi >= 0:
                arr[base + fi] = int(hours or 0)
    
    # Для зарезервированных слотов (новорожденные) заполняем средним налётом
    if frames_total_base is not None and frames_total_base < frames_total:
        for d_idx, D in enumerate(days_sorted):
            by_ac = mp5_by_day.get(D, {})
            # Считаем средний налёт за этот день по всем агентам
            if by_ac:
                avg_hours = sum(by_ac.values()) / len(by_ac)
                avg_hours_int = int(round(avg_hours))
            else:
                avg_hours_int = 0
            
            # Заполняем зарезервированные слоты средним значением
            base = d_idx * frames_total
            for fi in range(frames_total_base, frames_total):
                arr[base + fi] = avg_hours_int
    
    # Последний день (паддинг) оставляем нулями
    return arr


def build_mp1_arrays(mp1_map: Dict[int, Tuple[int, int, int, int, int, int]]) -> Tuple[List[int], List[int], List[int], List[int], List[int], List[int], Dict[int,int]]:
    """Строит SoA массивы MP1 и индекс partseqno_i->idx.
    
    mp1_map: partseq → (br_mi8, br_mi17, br2_mi17, repair_time, partout_time, assembly_time)
    br2_mi17 - порог межремонтного для подъёма из inactive (3500ч = 210000 мин)
    """
    keys = sorted(mp1_map.keys())
    idx_map: Dict[int,int] = {k: i for i, k in enumerate(keys)}
    br8: List[int] = []
    br17: List[int] = []
    br2_17: List[int] = []
    rt: List[int] = []
    pt: List[int] = []
    at: List[int] = []
    for k in keys:
        b8, b17, b2_17, rti, pti, ati = mp1_map.get(k, (0,0,0,0,0,0))
        br8.append(int(b8 or 0))
        br17.append(int(b17 or 0))
        br2_17.append(int(b2_17 or 0))
        rt.append(int(rti or 0))
        pt.append(int(pti or 0))
        at.append(int(ati or 0))
    return br8, br17, br2_17, rt, pt, at, idx_map


def build_mp3_arrays(mp3_rows, mp3_fields: List[str]) -> Dict[str, List[int]]:
    idx = {name: i for i, name in enumerate(mp3_fields)}
    to_u32 = lambda v: int(v or 0)
    to_u16 = lambda v: int(v or 0)
    arr: Dict[str, List[int]] = {
        'mp3_psn': [],
        'mp3_partseqno_i': [],
        'mp3_aircraft_number': [],
        'mp3_ac_type_mask': [],
        'mp3_group_by': [],
        'mp3_status_id': [],
        'mp3_sne': [],
        'mp3_ppr': [],
        'mp3_repair_days': [],
        'mp3_repair_time': [],
        'mp3_ll': [],
        'mp3_oh': [],
        'mp3_mfg_date_days': [],
    }
    from datetime import date as _date
    epoch = _date(1970,1,1)
    for r in mp3_rows:
        arr['mp3_psn'].append(to_u32(r[idx['psn']]))
        arr['mp3_partseqno_i'].append(to_u32(r[idx['partseqno_i']]))
        arr['mp3_aircraft_number'].append(to_u32(r[idx['aircraft_number']]))
        arr['mp3_ac_type_mask'].append(to_u16(r[idx['ac_type_mask']]))
        arr['mp3_group_by'].append(to_u16(r[idx.get('group_by', -1)] if 'group_by' in idx else 0))
        arr['mp3_status_id'].append(to_u16(r[idx['status_id']]))
        arr['mp3_sne'].append(to_u32(r[idx['sne']]))
        arr['mp3_ppr'].append(to_u32(r[idx['ppr']]))
        arr['mp3_repair_days'].append(to_u16(r[idx['repair_days']]))
        arr['mp3_repair_time'].append(to_u16(r[idx['repair_time']]))
        arr['mp3_ll'].append(to_u32(r[idx['ll']]))
        arr['mp3_oh'].append(to_u32(r[idx['oh']]))
        md = r[idx.get('mfg_date', -1)] if 'mfg_date' in idx else None
        ord_days = 0
        if md:
            try:
                ord_days = max(0, int((md - epoch).days))
            except Exception:
                ord_days = 0
        arr['mp3_mfg_date_days'].append(to_u16(ord_days))
    return arr


def build_mp4_arrays(mp4_by_day: Dict[date, Dict[str, int]], days_sorted: List[date]) -> Tuple[List[int], List[int]]:
    ops8: List[int] = []
    ops17: List[int] = []
    for D in days_sorted:
        m = mp4_by_day.get(D, {})
        ops8.append(int(m.get('ops_counter_mi8', 0)))
        ops17.append(int(m.get('ops_counter_mi17', 0)))
    return ops8, ops17


def days_to_epoch_u16(d: date) -> int:
    # ClickHouse Date совместимо: дни от 1970‑01‑01
    from datetime import date as _date
    epoch = _date(1970, 1, 1)
    diff = (d - epoch).days
    return max(0, int(diff))


def calculate_dynamic_spawn_reserve_mi17(
    avg_fleet_size: float,
    ll_minutes: int,
    avg_daily_minutes: float,
    simulation_days: int = 4000
) -> int:
    """
    Расчёт резерва для динамического spawn Mi-17 по формуле агрегатов.
    
    Формула (уточнённая):
    1. Средний налёт в день × Среднее количество бортов × Дни / LL
    2. Запас прочности 20%
    3. Округление до целых (без хардкода)
    
    Args:
        avg_fleet_size: Среднее количество бортов в программе (из MP4)
        ll_minutes: Life Limit в минутах (из MP1)
        avg_daily_minutes: Средний суточный налёт в минутах (из MP5)
        simulation_days: Горизонт симуляции (по умолчанию 4000 дней)
    
    Returns:
        Количество резервных слотов (округлено до целых)
    """
    if ll_minutes <= 0 or avg_daily_minutes <= 0 or avg_fleet_size <= 0:
        return 0  # Защита от некорректных данных
    
    # 1. Суммарный налёт за период
    total_flight_minutes = avg_fleet_size * simulation_days * avg_daily_minutes
    
    # 2. Количество планеров, которые выработают ресурс
    planers_consumed = total_flight_minutes / ll_minutes
    
    # 3. С запасом прочности 20%
    planers_needed = planers_consumed * 1.2
    
    # 4. Резервные слоты (округление до целых)
    reserve_slots = int(round(planers_needed))
    
    return reserve_slots


def prepare_env_arrays(
    client,
    version_date: date = None,
    version_id: int = None,
) -> Dict[str, object]:
    """
    Формирует все Env массивы/скаляры для full‑GPU окружения (без применения к модели).
    
    Args:
        client: ClickHouse client
        version_date: Конкретная дата версии (опционально). Если None — берёт последнюю.
        version_id: Конкретный version_id для version_date (опционально).
    """
    vdate, vid = fetch_versions(client, version_date, version_id)
    mp3_rows, mp3_fields = fetch_mp3(client, vdate, vid)
    (
        mp1_map,
        mp1_oh_map,
        mp1_ll_map,
        mp1_ll_mi8_map,
        mp1_second_ll_map,
        mp1_sne_ppr_map,
        mp1_repair_number_map,
    ) = fetch_mp1_all(client)
    mp4_by_day = preload_mp4_by_day(client, vdate, vid)
    mp5_by_day = preload_mp5_maps(client, vdate, vid)

    days_sorted = get_days_sorted_union(mp4_by_day, mp5_by_day)
    # Индексация кадров: объединение MP3 ∪ MP5 (MP3 сначала, затем будущие из MP5 по возрастанию)
    frames_index_mp3, _ = build_frames_index(mp3_rows, mp3_fields)
    ac_mp3_ordered = [ac for ac, _ in sorted(frames_index_mp3.items(), key=lambda kv: kv[1])]
    ac_mp5_set = set()
    for d, by_ac in mp5_by_day.items():
        for ac in by_ac.keys():
            try:
                ac_i = int(ac)
            except Exception:
                ac_i = 0
            if ac_i > 0:
                ac_mp5_set.add(ac_i)
    # План новых Ми-17 по дням (seed для MacroProperty на GPU)
    # Примечание: НЕ используем для расширения FRAMES; FRAMES = |MP3 ∪ MP5|.
    mp4_new_counter_mi17_seed: List[int] = []
    from datetime import date as _date
    for D in days_sorted:
        md = mp4_by_day.get(D, {})
        v = int(md.get('new_counter_mi17', 0))
        if v < 0:
            v = 0
        mp4_new_counter_mi17_seed.append(v)

    mp4_spawn_limit_seed: List[int] = []
    spawn_limit_cumulative: List[int] = []
    spawn_limit_running_total = 0
    for D in days_sorted:
        md = mp4_by_day.get(D, {})
        v = int(md.get('spawn_limit', 0))
        if v < 0:
            v = 0
        mp4_spawn_limit_seed.append(v)
        spawn_limit_running_total += v
        spawn_limit_cumulative.append(spawn_limit_running_total)
    # PRESENCE-based активация: источник — колонка flight_program_ac.spawn_limit_active
    # (одинакова для всех строк версии), а НЕ sum(seed)>0. Это позволяет включить лимит
    # при строке нулей: spawn_limit_active=1 + cumulative=0 → allowed=0 → dynamic Mi-17 spawn заблокирован.
    spawn_limit_active = max(
        (int(md.get('spawn_limit_active', 0)) for md in mp4_by_day.values()),
        default=0,
    )
    # FRAMES-upfront: отключено. Будущий спавн не расширяет FRAMES на этапе Env.
    future_spawn_total = 0
    frames_buffer = 0
    # База для ACN: максимум среди существующих/MP5 и порог 100000
    existing_set = set(ac_mp3_ordered)
    existing_set.update(ac_mp5_set)
    max_existing_acn = max(existing_set) if existing_set else 0
    base_acn_spawn = max(100000, max_existing_acn + 1)
    # Собираем объединение: MP3 → доп. из MP5 (без будущих ACN)
    ac_union = list(ac_mp3_ordered)
    extra_from_mp5 = sorted([ac for ac in ac_mp5_set if ac not in frames_index_mp3])
    ac_union.extend(extra_from_mp5)
    # Количество кадров без будущих (для выравнивания стартового индекса спавна)
    frames_union_no_future = len(ac_union)
    # Зарезервированные слоты под MP5-only планёры (без стартовых агентов): займём их спавном
    reserved_slots_count = len(extra_from_mp5)
    first_reserved_idx = max(0, frames_union_no_future - reserved_slots_count)
    # ═══════════════════════════════════════════════════════════════════════════
    # РАСЧЁТ ДИНАМИЧЕСКОГО РЕЗЕРВА ДЛЯ SPAWN (по формуле агрегатов)
    # ═══════════════════════════════════════════════════════════════════════════
    
    # 1. Подсчёт существующих Mi-17 из MP3 (group_by=2)
    initial_mi17_count = sum(1 for row in mp3_rows if row[mp3_fields.index('group_by')] == 2)
    
    # 2. Расчёт среднего налёта Mi-17 из MP5
    # Используем прямой запрос к ClickHouse для получения среднего налёта Mi-17
    try:
        avg_query = f"""
        SELECT AVG(daily_hours) as avg_minutes
        FROM flight_program_fl
        WHERE version_date = '{vdate}' AND version_id = {vid}
          AND ac_type_mask = 64  -- Mi-17
          AND daily_hours > 0
        """
        avg_result = client.execute(avg_query)
        avg_daily_minutes_mi17 = float(avg_result[0][0]) if avg_result and avg_result[0][0] else 110.5
    except Exception as e:
        print(f"⚠️  Ошибка при расчёте среднего налёта Mi-17: {e}")
        avg_daily_minutes_mi17 = 110.5  # fallback
    
    # 3. Получаем LL для Mi-17 из mp1_ll_map (partseqno=70386, МИ-8АМТ, group_by=2)
    SPAWN_PARTSEQNO_MI17 = 70386
    ll_mi17_minutes = mp1_ll_map.get(SPAWN_PARTSEQNO_MI17, 1080000)  # fallback = 18000 часов
    
    # Будущие ACN не включаем в FRAMES: индексация стабильна по |MP3 ∪ MP5|
    frames_index = {ac: i for i, ac in enumerate(ac_union)}
    # frames_total_base — количество РЕАЛЬНЫХ агентов из MP3 (без будущих из MP5)
    frames_total_base = len(frames_index_mp3)
    
    # Сначала создаём MP4 массивы (нужны для расчёта среднего количества бортов)
    mp4_ops8, mp4_ops17 = build_mp4_arrays(mp4_by_day, days_sorted)
    
    # 4. Расчёт детерминированного spawn из MP4
    deterministic_spawn_mi17 = sum(mp4_new_counter_mi17_seed)
    
    # 5. Расчёт среднего количества бортов Mi-17 в программе (из MP4)
    # Среднее значение mp4_ops_counter_mi17 за весь период
    avg_fleet_size_mi17 = sum(mp4_ops17) / len(mp4_ops17) if len(mp4_ops17) > 0 else float(initial_mi17_count)
    
    # 6. Расчёт динамического резерва по формуле агрегатов
    # Формула: avg_daily_minutes × avg_fleet_size × 4000 / ll × 1.2
    dynamic_reserve_mi17 = calculate_dynamic_spawn_reserve_mi17(
        avg_fleet_size=avg_fleet_size_mi17,
        ll_minutes=ll_mi17_minutes,
        avg_daily_minutes=avg_daily_minutes_mi17,
        simulation_days=4000  # Максимальный горизонт для расчёта резерва
    )
    
    # 7. Добавляем резервные слоты к frames_total
    # ВАЖНО: Резервируем слоты для ОБОИХ типов spawn (детерминированный + динамический)
    # Они будут использовать общий диапазон ACN (100000+), начиная с последнего свободного idx
    total_spawn_reserve = deterministic_spawn_mi17 + dynamic_reserve_mi17
    
    # Расширяем frames_total с учётом резерва для spawn
    frames_total = frames_total_base + total_spawn_reserve
    frames_total_before_clamp = frames_total
    frames_total = max(frames_total, len(ac_union))
    if frames_total != frames_total_before_clamp:
        print(
            f"  ⚠️ frames_total clamp: {frames_total_before_clamp} → {frames_total} "
            f"для покрытия MP3∪MP5 ({len(ac_union)} AC)"
        )
    
    # 8. Индексы для spawn
    # first_reserved_idx — для детерминированного spawn (начинается сразу после существующих)
    first_reserved_idx = frames_total_base
    
    # first_dynamic_idx — для динамического spawn (начинается после детерминированного)
    first_dynamic_idx = frames_total_base + deterministic_spawn_mi17
    
    # Индекс первого будущего борта (если присутствует в MP5/union)
    first_future_idx = int(frames_index.get(base_acn_spawn, frames_union_no_future))
    
    # Построение MP5 на расширенном FRAMES (для новых кадров заполняем средним налётом)
    mp5_linear = build_mp5_linear(mp5_by_day, days_sorted, frames_index, frames_total, frames_total_base)
    mp1_br8, mp1_br17, mp1_br2_17, mp1_rt, mp1_pt, mp1_at, mp1_index = build_mp1_arrays(mp1_map)
    # Соберём массивы OH по индексу MP1
    keys_sorted = sorted(mp1_index.keys(), key=lambda k: mp1_index[k])
    mp1_oh8_arr: List[int] = []
    mp1_oh17_arr: List[int] = []
    for k in keys_sorted:
        oh8, oh17 = mp1_oh_map.get(k, (0, 0))
        mp1_oh8_arr.append(int(oh8 or 0))
        mp1_oh17_arr.append(int(oh17 or 0))
    # Соберём массив LL по индексу MP1 (mi8/mi17)
    mp1_ll8_arr: List[int] = []
    mp1_ll17_arr: List[int] = []
    mp1_second_ll_arr: List[int] = []
    for k in keys_sorted:
        ll8 = mp1_ll_mi8_map.get(k, 0)
        llv = mp1_ll_map.get(k, 0)
        mp1_ll8_arr.append(int(ll8 or 0))
        mp1_ll17_arr.append(int(llv or 0))
        second_ll_val = mp1_second_ll_map.get(k, SECOND_LL_SENTINEL)
        mp1_second_ll_arr.append(int(second_ll_val))
    
    # Соберём массивы sne_new и ppr_new по индексу MP1
    # SENTINEL = 0xFFFFFFFF (4294967295) для NULL значений
    SENTINEL = 4294967295
    mp1_sne_new_arr: List[int] = []
    mp1_ppr_new_arr: List[int] = []
    mp1_repair_number_arr: List[int] = []
    SENTINEL_U8 = 255  # Sentinel для repair_number (UInt8)
    for k in keys_sorted:
        sne, ppr = mp1_sne_ppr_map.get(k, (SENTINEL, SENTINEL))
        mp1_sne_new_arr.append(int(sne))
        mp1_ppr_new_arr.append(int(ppr))
        rn = mp1_repair_number_map.get(k, SENTINEL_U8)
        mp1_repair_number_arr.append(int(rn))
    
    # Диагностика repair_number после преобразования в массив
    rn_non_sentinel = sum(1 for x in mp1_repair_number_arr if x > 0 and x != 255)
    print(f"  📊 mp1_repair_number_arr: размер={len(mp1_repair_number_arr)}, значений > 0 (не 255): {rn_non_sentinel}")
    if rn_non_sentinel > 0:
        sample_indices = [(i, mp1_repair_number_arr[i], keys_sorted[i]) for i in range(min(20, len(mp1_repair_number_arr))) if mp1_repair_number_arr[i] > 0 and mp1_repair_number_arr[i] != 255]
        if sample_indices:
            print(f"     Образцы (idx, repair_number, partseqno): {sample_indices[:5]}")
    
    mp3_arrays = build_mp3_arrays(mp3_rows, mp3_fields)

    # month_first_u32: ordinal первого дня месяца для каждого дня симуляции
    month_first_u32: List[int] = []
    for D in days_sorted:
        first = _date(D.year, D.month, 1)
        month_first_u32.append(days_to_epoch_u16(first))

    # Создаем mp1_arrays для удобства использования в orchestrator
    mp1_arrays = {
        'partseqno_i': keys_sorted,  # Отсортированные partseqno_i из mp1_index
        'br_mi8': mp1_br8,
        'br_mi17': mp1_br17,
        'br2_mi17': mp1_br2_17,  # Порог межремонтного для подъёма из inactive
        'repair_time': mp1_rt,
        'partout_time': mp1_pt,
        'assembly_time': mp1_at,
        'oh_mi8': mp1_oh8_arr,
        'oh_mi17': mp1_oh17_arr,
        'll_mi8': mp1_ll8_arr,
        'll_mi17': mp1_ll17_arr,
        'second_ll': mp1_second_ll_arr,
        'sne_new': mp1_sne_new_arr,
        'ppr_new': mp1_ppr_new_arr,
        'repair_number': mp1_repair_number_arr,
    }

    # Извлекаем константы для Mi-8 (partseqno_i=70387, МИ-8Т, group_by=1) и Mi-17 (partseqno_i=70386, МИ-8АМТ, group_by=2)
    # БЕЗ FALLBACK! Если данных нет → ошибка
    
    # Определяем partseqno для типов ВС (единая точка определения для всего проекта)
    SPAWN_PARTSEQNO_MI8 = 70387   # МИ-8Т, group_by=1
    SPAWN_PARTSEQNO_MI17 = 70386  # МИ-8АМТ, group_by=2
    
    # Mi-8: partseqno=70387 (МИ-8Т, group_by=1)
    if SPAWN_PARTSEQNO_MI8 not in mp1_map:
        raise ValueError(
            f"❌ partseqno={SPAWN_PARTSEQNO_MI8} (Mi-8, МИ-8Т) НЕ найден в справочнике md_components! "
            "Проверьте данные в таблице md_components."
        )
    
    mi8_tuple = mp1_map[SPAWN_PARTSEQNO_MI8]  # (br_mi8, br_mi17, br2_mi17, repair_time, partout_time, assembly_time)
    mi8_repair_time_const = int(mi8_tuple[3])
    mi8_partout_time_const = int(mi8_tuple[4])
    mi8_assembly_time_const = int(mi8_tuple[5])
    
    # Валидация: константы Mi-8 должны быть > 0
    if mi8_repair_time_const <= 0:
        raise ValueError(f"❌ Mi-8 repair_time={mi8_repair_time_const} <= 0 в справочнике md_components!")
    if mi8_partout_time_const <= 0:
        raise ValueError(f"❌ Mi-8 partout_time={mi8_partout_time_const} <= 0 в справочнике md_components!")
    if mi8_assembly_time_const <= 0:
        raise ValueError(f"❌ Mi-8 assembly_time={mi8_assembly_time_const} <= 0 в справочнике md_components!")
    
    # Mi-8: partseqno=70387 (МИ-8Т, group_by=1)
    mi8_pidx = mp1_index.get(SPAWN_PARTSEQNO_MI8, -1)
    if mi8_pidx < 0:
        raise ValueError(f"❌ partseqno={SPAWN_PARTSEQNO_MI8} (Mi-8) не найден в mp1_index!")
    
    mi8_ll_const = mp1_ll8_arr[mi8_pidx] if mi8_pidx < len(mp1_ll8_arr) else 0
    mi8_oh_const = mp1_oh8_arr[mi8_pidx] if mi8_pidx < len(mp1_oh8_arr) else 0
    mi8_br_const = int(mi8_tuple[0])
    
    # Валидация: нормативы Mi-8 должны быть > 0
    if mi8_ll_const <= 0:
        raise ValueError(f"❌ Mi-8 ll={mi8_ll_const} <= 0 в справочнике md_components!")
    if mi8_oh_const <= 0:
        raise ValueError(f"❌ Mi-8 oh={mi8_oh_const} <= 0 в справочнике md_components!")
    if mi8_br_const <= 0:
        raise ValueError(f"❌ Mi-8 br={mi8_br_const} <= 0 в справочнике md_components!")
    
    # Mi-17: partseqno=70386 (МИ-8АМТ, group_by=2)
    if SPAWN_PARTSEQNO_MI17 not in mp1_map:
        raise ValueError(
            f"❌ partseqno={SPAWN_PARTSEQNO_MI17} (Mi-17, МИ-8АМТ) НЕ найден в справочнике md_components! "
            "Проверьте данные в таблице md_components."
        )
    
    mi17_tuple = mp1_map[SPAWN_PARTSEQNO_MI17]  # (br_mi8, br_mi17, br2_mi17, repair_time, partout_time, assembly_time)
    mi17_repair_time_const = int(mi17_tuple[3])
    mi17_partout_time_const = int(mi17_tuple[4])
    mi17_assembly_time_const = int(mi17_tuple[5])
    mi17_br2_const = int(mi17_tuple[2])  # br2_mi17 - порог межремонтного для подъёма из inactive
    
    # Валидация: константы Mi-17 должны быть > 0
    if mi17_repair_time_const <= 0:
        raise ValueError(f"❌ Mi-17 repair_time={mi17_repair_time_const} <= 0 в справочнике md_components!")
    if mi17_partout_time_const <= 0:
        raise ValueError(f"❌ Mi-17 partout_time={mi17_partout_time_const} <= 0 в справочнике md_components!")
    if mi17_assembly_time_const <= 0:
        raise ValueError(f"❌ Mi-17 assembly_time={mi17_assembly_time_const} <= 0 в справочнике md_components!")
    
    # Извлекаем начальную наработку и нормативы для Mi-17
    # Используем индекс в mp1_index для получения данных из массивов
    mi17_pidx = mp1_index.get(SPAWN_PARTSEQNO_MI17, -1)
    if mi17_pidx < 0:
        raise ValueError(f"❌ partseqno={SPAWN_PARTSEQNO_MI17} (Mi-17) не найден в mp1_index!")
    
    mi17_sne_new, mi17_ppr_new = mp1_sne_ppr_map.get(SPAWN_PARTSEQNO_MI17, (0, 0))
    mi17_ll_const = mp1_ll17_arr[mi17_pidx] if mi17_pidx < len(mp1_ll17_arr) else 0
    mi17_oh_const = mp1_oh17_arr[mi17_pidx] if mi17_pidx < len(mp1_oh17_arr) else 0
    mi17_br_const = int(mi17_tuple[1])  # br_mi17 из mp1_map
    
    # Валидация: нормативы Mi-17 должны быть > 0
    if mi17_ll_const <= 0:
        raise ValueError(f"❌ Mi-17 ll={mi17_ll_const} <= 0 в справочнике md_components!")
    if mi17_oh_const <= 0:
        raise ValueError(f"❌ Mi-17 oh={mi17_oh_const} <= 0 в справочнике md_components!")
    if mi17_br_const <= 0:
        raise ValueError(f"❌ Mi-17 br={mi17_br_const} <= 0 в справочнике md_components!")

    env_data = {
        'version_date_u16': days_to_epoch_u16(vdate),
        'version_id_u32': int(vid),
        'frames_total_u16': int(frames_total),
        'days_total_u16': int(len(days_sorted)),
        'days_sorted': days_sorted,
        'frames_index': frames_index,
        'base_acn_spawn': int(base_acn_spawn),
        'first_future_idx': int(first_future_idx),
        'frames_union_no_future': int(frames_union_no_future),
        'reserved_slots_count': int(reserved_slots_count),
        'first_reserved_idx': int(first_reserved_idx),
        'future_spawn_total': int(future_spawn_total),
        'mp4_ops_counter_mi8': mp4_ops8,
        'mp4_ops_counter_mi17': mp4_ops17,
        'mp4_new_counter_mi17_seed': mp4_new_counter_mi17_seed,
        'mp4_spawn_limit_seed': mp4_spawn_limit_seed,
        'spawn_limit_cumulative': spawn_limit_cumulative,
        'spawn_limit_active': int(spawn_limit_active),
        'mp5_daily_hours_linear': mp5_linear,
        'month_first_u32': month_first_u32,
        'mp1_map': mp1_map,  # Добавляем mp1_map для прямого доступа (как в sim_master.py)
        'mp1_br_mi8': mp1_br8,
        'mp1_br_mi17': mp1_br17,
        'mp1_br2_mi17': mp1_br2_17,  # Порог межремонтного для подъёма из inactive
        'mp1_repair_time': mp1_rt,
        'mp1_partout_time': mp1_pt,
        'mp1_assembly_time': mp1_at,
        'mp1_oh_mi8': mp1_oh8_arr,
        'mp1_oh_mi17': mp1_oh17_arr,
        'mp1_second_ll': mp1_second_ll_arr,
        'mp1_ll_mi17': mp1_ll17_arr,
        'mp1_ll_mi8': mp1_ll8_arr,
        'mp1_sne_new': mp1_sne_new_arr,
        'mp1_ppr_new': mp1_ppr_new_arr,
        'mp1_repair_number': mp1_repair_number_arr,
        'mp1_index': mp1_index,
        'mp1_arrays': mp1_arrays,  # Добавляем сгруппированные mp1 данные
        'mp3_arrays': mp3_arrays,
        'mp3_count': len(mp3_rows),
        # Partseqno для типов ВС (единая точка определения для всего проекта)
        'spawn_partseqno_mi8': SPAWN_PARTSEQNO_MI8,
        'spawn_partseqno_mi17': SPAWN_PARTSEQNO_MI17,
        'spawn_group_by_mi8': 1,
        'spawn_group_by_mi17': 2,
        # Скалярные константы времени для Mi-8/Mi-17 (из mp1_map как в sim_master.py)
        'mi8_repair_time_const': mi8_repair_time_const,
        'mi8_partout_time_const': mi8_partout_time_const,
        'mi8_assembly_time_const': mi8_assembly_time_const,
        'mi8_ll_const': int(mi8_ll_const),
        'mi8_oh_const': int(mi8_oh_const),
        'mi8_br_const': int(mi8_br_const),
        'mi17_repair_time_const': mi17_repair_time_const,
        'mi17_partout_time_const': mi17_partout_time_const,
        'mi17_assembly_time_const': mi17_assembly_time_const,
        # Начальная наработка и нормативы для Mi-17 (для spawn)
        'mi17_sne_new_const': int(mi17_sne_new),
        'mi17_ppr_new_const': int(mi17_ppr_new),
        'mi17_ll_const': int(mi17_ll_const),
        'mi17_oh_const': int(mi17_oh_const),
        'mi17_br_const': int(mi17_br_const),
        'mi17_br2_const': int(mi17_br2_const),  # Порог межремонтного для подъёма из inactive
        'second_ll_sentinel': SECOND_LL_SENTINEL,
        # Параметры динамического spawn (расчёт по формуле агрегатов)
        'initial_mi17_count': int(initial_mi17_count),
        'deterministic_spawn_mi17': int(deterministic_spawn_mi17),
        'dynamic_reserve_mi17': int(dynamic_reserve_mi17),
        'total_spawn_reserve': int(total_spawn_reserve),
        'first_dynamic_idx': int(first_dynamic_idx),
        'avg_daily_minutes_mi17': float(avg_daily_minutes_mi17),
        'avg_fleet_size_mi17': float(avg_fleet_size_mi17),
        'll_mi17_minutes': int(ll_mi17_minutes),
        'frames_total_base': int(frames_total_base),  # Количество РЕАЛЬНЫХ агентов (без резерва)
    }
    # Валидации форм и размеров (жёсткие assert'ы для раннего обнаружения ошибок)
    dt = int(env_data['days_total_u16'])
    ft = int(env_data['frames_total_u16'])
    assert len(env_data['mp4_ops_counter_mi8']) == dt, "MP4_mi8 размер не равен days_total"
    assert len(env_data['mp4_ops_counter_mi17']) == dt, "MP4_mi17 размер не равен days_total"
    assert len(env_data['mp4_new_counter_mi17_seed']) == dt, "MP4 new_counter_mi17 seed размер не равен days_total"
    assert len(env_data['mp4_spawn_limit_seed']) == dt, "MP4 spawn_limit seed размер не равен days_total"
    assert len(env_data['spawn_limit_cumulative']) == dt, "spawn_limit_cumulative размер не равен days_total"
    assert len(env_data['mp5_daily_hours_linear']) == (dt + 1) * ft, "MP5_linear размер != (days_total+1)*frames_total"
    assert len(env_data['month_first_u32']) == dt, "month_first_u32 размер не равен days_total"
    # mp3_arrays длины согласованы
    a = env_data['mp3_arrays']
    n3 = int(env_data['mp3_count'])
    for k in ('mp3_psn','mp3_aircraft_number','mp3_ac_type_mask','mp3_group_by','mp3_status_id','mp3_sne','mp3_ppr','mp3_repair_days','mp3_repair_time','mp3_ll','mp3_oh','mp3_mfg_date_days'):
        assert len(a.get(k, [])) == n3, f"MP3 SoA поле {k} имеет несогласованную длину"
    return env_data


def apply_env_to_sim(sim, env_data: Dict[str, object]):
    """Применяет подготовленные массивы к Env pyflamegpu модели (скаляры и Property Arrays)."""
    # Скаляры
    sim.setEnvironmentPropertyUInt("version_date", int(env_data['version_date_u16']))
    sim.setEnvironmentPropertyUInt("frames_total", int(env_data['frames_total_u16']))
    sim.setEnvironmentPropertyUInt("days_total", int(env_data['days_total_u16']))
    # MP4
    sim.setEnvironmentPropertyArrayUInt32("mp4_ops_counter_mi8", list(env_data['mp4_ops_counter_mi8']))
    sim.setEnvironmentPropertyArrayUInt32("mp4_ops_counter_mi17", list(env_data['mp4_ops_counter_mi17']))
    # Seed планов спавна и инициализация MacroProperty (делается отдельной RTC-функцией позже)
    sim.setEnvironmentPropertyArrayUInt32("mp4_new_counter_mi17_seed", list(env_data['mp4_new_counter_mi17_seed']))
    # Первый день месяца (ord days) для mfg_date
    sim.setEnvironmentPropertyArrayUInt32("month_first_u32", list(env_data['month_first_u32']))
    # MP5 теперь инициализируется через HostFunction в MacroProperty mp5_lin
    # sim.setEnvironmentPropertyArrayUInt16("mp5_daily_hours", list(env_data['mp5_daily_hours_linear']))
    # MP1 (SoA)
    if 'mp1_br_mi8' in env_data:
        sim.setEnvironmentPropertyArrayUInt32("mp1_br_mi8", list(env_data['mp1_br_mi8']))
        sim.setEnvironmentPropertyArrayUInt32("mp1_br_mi17", list(env_data['mp1_br_mi17']))
        sim.setEnvironmentPropertyArrayUInt32("mp1_repair_time", list(env_data['mp1_repair_time']))
        sim.setEnvironmentPropertyArrayUInt32("mp1_partout_time", list(env_data['mp1_partout_time']))
        sim.setEnvironmentPropertyArrayUInt32("mp1_assembly_time", list(env_data['mp1_assembly_time']))
        if 'mp1_oh_mi8' in env_data:
            sim.setEnvironmentPropertyArrayUInt32("mp1_oh_mi8", list(env_data['mp1_oh_mi8']))
        if 'mp1_oh_mi17' in env_data:
            sim.setEnvironmentPropertyArrayUInt32("mp1_oh_mi17", list(env_data['mp1_oh_mi17']))
        if 'mp1_second_ll' in env_data:
            sim.setEnvironmentPropertyArrayUInt32("mp1_second_ll", list(env_data['mp1_second_ll']))
        if 'mp1_sne_new' in env_data:
            sim.setEnvironmentPropertyArrayUInt32("mp1_sne_new", list(env_data['mp1_sne_new']))
        if 'mp1_ppr_new' in env_data:
            sim.setEnvironmentPropertyArrayUInt32("mp1_ppr_new", list(env_data['mp1_ppr_new']))
        if 'mp1_repair_number' in env_data:
            sim.setEnvironmentPropertyArrayUInt8("mp1_repair_number", list(env_data['mp1_repair_number']))
    # MP3 (SoA)
    if 'mp3_arrays' in env_data:
        a = env_data['mp3_arrays']
        sim.setEnvironmentPropertyArrayUInt32("mp3_psn", list(a['mp3_psn']))
        sim.setEnvironmentPropertyArrayUInt32("mp3_aircraft_number", list(a['mp3_aircraft_number']))
        sim.setEnvironmentPropertyArrayUInt32("mp3_ac_type_mask", list(a['mp3_ac_type_mask']))
        sim.setEnvironmentPropertyArrayUInt32("mp3_status_id", list(a['mp3_status_id']))
        sim.setEnvironmentPropertyArrayUInt32("mp3_sne", list(a['mp3_sne']))
        sim.setEnvironmentPropertyArrayUInt32("mp3_ppr", list(a['mp3_ppr']))
        sim.setEnvironmentPropertyArrayUInt32("mp3_repair_days", list(a['mp3_repair_days']))
        sim.setEnvironmentPropertyArrayUInt32("mp3_ll", list(a['mp3_ll']))
        sim.setEnvironmentPropertyArrayUInt32("mp3_oh", list(a['mp3_oh']))
        sim.setEnvironmentPropertyArrayUInt32("mp3_mfg_date_days", list(a['mp3_mfg_date_days']))
    # MP6 будет инициализироваться на GPU из MP4 в отдельном шаге (rtc_quota_init или отдельный init)
    return None


