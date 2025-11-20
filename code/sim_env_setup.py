#!/usr/bin/env python3
"""
Подготовка окружения симуляции: загрузка данных MP/Property, формирование массивов дня.
Дата: 2025-08-21
"""

from __future__ import annotations
from typing import Dict, List, Tuple
from datetime import date

import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), 'utils'))
from config_loader import get_clickhouse_client


def get_client():
    return get_clickhouse_client()


def fetch_versions(client) -> Tuple[date, int]:
    rows = client.execute(
        "SELECT version_date, version_id FROM heli_pandas ORDER BY version_date DESC, version_id DESC LIMIT 1"
    )
    vd, vid = rows[0]
    return vd, int(vid)


def fetch_mp1_br_rt(client) -> Dict[int, Tuple[int, int, int, int, int]]:
    """Возвращает карту partseq → (br_mi8, br_mi17, repair_time, partout_time, assembly_time). BR в минутах.
    Подбирает: идентификатор (partseq) и имя колонок нормативов по нескольким вариантам.
    """
    id_candidates = ["partseqno_i", "`partno.comp`", "partno_comp", "partno"]
    rpa_variants = [
        ("repair_time", "partout_time", "assembly_time"),
        ("repair_time_mi17", "partout_time_mi17", "assembly_time_mi17"),
        ("rt_mi17", "pt_mi17", "at_mi17"),
    ]
    rows = []
    last_err: Exception | None = None
    for id_col in id_candidates:
        for (rt_col, pt_col, at_col) in rpa_variants:
            try:
                sql = (
                    "SELECT\n"
                    f"  toUInt32OrZero(toString({id_col})) AS partseq,\n"
                    "  toUInt32OrZero(toString(br_mi8))  AS br_mi8,\n"
                    "  toUInt32OrZero(toString(br_mi17)) AS br_mi17,\n"
                    f"  toUInt32OrZero(toString({rt_col})) AS repair_time,\n"
                    f"  toUInt32OrZero(toString({pt_col})) AS partout_time,\n"
                    f"  toUInt32OrZero(toString({at_col})) AS assembly_time\n"
                    "FROM md_components"
                )
                test = client.execute(sql)
                if test:
                    rows = test
                    raise StopIteration  # break both loops
            except StopIteration:
                last_err = None
                break
            except Exception as e:
                last_err = e
                rows = []
                continue
        if rows:
            break
    if not rows and last_err is not None:
        raise last_err
    return {int(p): (int(b8 or 0), int(b17 or 0), int(rt or 0), int(pt or 0), int(at or 0)) for p, b8, b17, rt, pt, at in rows}


def fetch_mp1_oh(client) -> Dict[int, Tuple[int, int]]:
    """Возвращает карту partseq → (oh_mi8, oh_mi17). Единицы в минутах.
    Подбирает корректную колонку идентификатора по очереди: partseqno_i, `partno.comp`, partno_comp, partno.
    """
    candidates = ["partseqno_i", "`partno.comp`", "partno_comp", "partno"]
    rows = []
    last_err: Exception | None = None
    for col in candidates:
        try:
            sql = (
                "SELECT\n"
                f"  toUInt32OrZero(toString({col})) AS partseq,\n"
                "  toUInt32OrZero(toString(oh_mi8))  AS oh_mi8,\n"
                "  toUInt32OrZero(toString(oh_mi17)) AS oh_mi17\n"
                "FROM md_components"
            )
            rows = client.execute(sql)
            if rows:
                break
        except Exception as e:
            last_err = e
            rows = []
            continue
    if not rows and last_err is not None:
        raise last_err
    return {int(p): (int(oh8 or 0), int(oh17 or 0)) for p, oh8, oh17 in rows}


def fetch_mp1_ll(client) -> Dict[int, int]:
    """Возвращает карту partseq → ll_mi17 (минуты)."""
    candidates = ["partseqno_i", "`partno.comp`", "partno_comp", "partno"]
    rows = []
    last_err: Exception | None = None
    for col in candidates:
        try:
            sql = (
                "SELECT\n"
                f"  toUInt32OrZero(toString({col})) AS partseq,\n"
                "  toUInt32OrZero(toString(ll_mi17)) AS ll_mi17\n"
                "FROM md_components"
            )
            rows = client.execute(sql)
            if rows:
                break
        except Exception as e:
            last_err = e
            rows = []
            continue
    if not rows and last_err is not None:
        raise last_err
    return {int(p): int(ll or 0) for p, ll in rows}


def fetch_mp1_repair_number(client) -> Dict[int, int]:
    """
    Возвращает карту partseq → repair_number.
    
    ⚠️ ВАЖНО: NULL значения преобразуются в sentinel value 0xFF (255)
    для совместимости с FLAME GPU (не поддерживает Nullable типы).
    
    Интерпретация значений:
    - 0xFF (255): квота ремонта не задана (было NULL в СУБД)
    - 0-254: номер квоты ремонта для группировки агрегатов
    """
    SENTINEL = 255  # 0xFF - максимальное значение UInt8
    
    candidates = ["partseqno_i", "`partno.comp`", "partno_comp", "partno"]
    rows = []
    last_err: Exception | None = None
    for col in candidates:
        try:
            sql = (
                "SELECT\n"
                f"  toUInt32OrZero(toString({col})) AS partseq,\n"
                "  repair_number\n"
                "FROM md_components"
            )
            rows = client.execute(sql)
            if rows:
                break
        except Exception as e:
            last_err = e
            rows = []
            continue
    if not rows and last_err is not None:
        raise last_err
    
    # Преобразуем NULL → SENTINEL (255)
    result = {}
    non_null_count = 0
    for p, rn in rows:
        if rn is None:
            result[int(p)] = SENTINEL
        else:
            result[int(p)] = int(rn)
            if int(rn) > 0:
                non_null_count += 1
    
    print(f"  📊 fetch_mp1_repair_number: загружено {len(result)} записей, из них {non_null_count} с repair_number > 0")
    if non_null_count > 0:
        sample = [(k, v) for k, v in list(result.items())[:20] if v > 0 and v != 255]
        if sample:
            print(f"     Образцы (partseq, repair_number): {sample[:5]}")
    
    return result


def fetch_mp1_sne_ppr_new(client) -> Dict[int, Tuple[int, int]]:
    """
    Возвращает карту partseq → (sne_new, ppr_new).
    
    ⚠️ ВАЖНО: NULL значения преобразуются в sentinel value 0xFFFFFFFF (4294967295)
    для совместимости с FLAME GPU (не поддерживает Nullable типы).
    
    Интерпретация значений:
    - 0xFFFFFFFF (4294967295): агрегат не выпускается (было NULL в СУБД)
    - 0: новый агрегат с нулевой наработкой
    - > 0 и < 4294967295: агрегат с начальной наработкой
    """
    SENTINEL = 4294967295  # 0xFFFFFFFF - максимальное значение UInt32
    
    candidates = ["partseqno_i", "`partno.comp`", "partno_comp", "partno"]
    rows = []
    last_err: Exception | None = None
    for col in candidates:
        try:
            sql = (
                "SELECT\n"
                f"  toUInt32OrZero(toString({col})) AS partseq,\n"
                f"  CASE WHEN sne_new IS NULL THEN {SENTINEL} ELSE toUInt32OrZero(toString(sne_new)) END AS sne_new,\n"
                f"  CASE WHEN ppr_new IS NULL THEN {SENTINEL} ELSE toUInt32OrZero(toString(ppr_new)) END AS ppr_new\n"
                "FROM md_components"
            )
            rows = client.execute(sql)
            if rows:
                break
        except Exception as e:
            last_err = e
            rows = []
            continue
    if not rows and last_err is not None:
        raise last_err
    return {int(p): (int(sne), int(ppr)) for p, sne, ppr in rows}


def fetch_mp3(client, vdate: date, vid: int):
    fields = [
        'partseqno_i','psn','aircraft_number','ac_type_mask','group_by','status_id',
        'll','oh','oh_threshold','sne','ppr','repair_days','mfg_date','version_date'
    ]
    sql = f"""
    SELECT {', '.join(fields)}
    FROM heli_pandas
    WHERE version_date = '{vdate}' AND version_id = {vid}
    ORDER BY psn
    """
    rows = client.execute(sql)
    return rows, fields


def preload_mp4_by_day(client) -> Dict[date, Dict[str,int]]:
    rows = client.execute(
        """
        SELECT dates, ops_counter_mi8, ops_counter_mi17, trigger_program_mi8, trigger_program_mi17, new_counter_mi17
        FROM flight_program_ac
        ORDER BY dates
        """
    )
    result: Dict[date, Dict[str,int]] = {}
    for d, mi8, mi17, t8, t17, n17 in rows:
        result[d] = {
            "ops_counter_mi8": int(mi8 or 0),
            "ops_counter_mi17": int(mi17 or 0),
            "trigger_program_mi8": int(t8 or 0),
            "trigger_program_mi17": int(t17 or 0),
            "new_counter_mi17": int(n17 or 0),
        }
    return result


def preload_mp5_maps(client) -> Dict[date, Dict[int,int]]:
    rows = client.execute(
        """
        SELECT dates, aircraft_number, daily_hours
        FROM flight_program_fl
        ORDER BY dates, aircraft_number
        """
    )
    result: Dict[date, Dict[int,int]] = {}
    for d, ac, h in rows:
        m = result.setdefault(d, {})
        m[int(ac)] = int(h or 0)
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
        # карта теперь (br_mi8, br_mi17, repair_time, partout_time, assembly_time)
        _, _, _, pt, at = mp1_br_rt_map.get(partseq, (0,0,0,0,0))
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


def build_mp1_arrays(mp1_map: Dict[int, Tuple[int, int, int, int, int]]) -> Tuple[List[int], List[int], List[int], List[int], List[int], Dict[int,int]]:
    """Строит SoA массивы MP1 и индекс partseqno_i->idx."""
    keys = sorted(mp1_map.keys())
    idx_map: Dict[int,int] = {k: i for i, k in enumerate(keys)}
    br8: List[int] = []
    br17: List[int] = []
    rt: List[int] = []
    pt: List[int] = []
    at: List[int] = []
    for k in keys:
        b8, b17, rti, pti, ati = mp1_map.get(k, (0,0,0,0,0))
        br8.append(int(b8 or 0))
        br17.append(int(b17 or 0))
        rt.append(int(rti or 0))
        pt.append(int(pti or 0))
        at.append(int(ati or 0))
    return br8, br17, rt, pt, at, idx_map


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
        'mp3_ll': [],
        'mp3_oh': [],
        'mp3_mfg_date_days': [],
    }
    from datetime import date as _date
    epoch = _date(1970,1,1)
    for r in mp3_rows:
        arr['mp3_psn'].append(to_u32(r[idx['psn']]))
        # partseqno_i может называться по-разному в источнике, подберём корректную колонку
        try:
            if 'partseqno_i' in idx:
                arr['mp3_partseqno_i'].append(to_u32(r[idx['partseqno_i']]))
            elif 'partno_comp' in idx:
                arr['mp3_partseqno_i'].append(to_u32(r[idx['partno_comp']]))
            elif '`partno.comp`' in idx:
                arr['mp3_partseqno_i'].append(to_u32(r[idx['`partno.comp`']]))
            else:
                arr['mp3_partseqno_i'].append(0)
        except Exception:
            arr['mp3_partseqno_i'].append(0)
        arr['mp3_aircraft_number'].append(to_u32(r[idx['aircraft_number']]))
        arr['mp3_ac_type_mask'].append(to_u16(r[idx['ac_type_mask']]))
        arr['mp3_group_by'].append(to_u16(r[idx.get('group_by', -1)] if 'group_by' in idx else 0))
        arr['mp3_status_id'].append(to_u16(r[idx['status_id']]))
        arr['mp3_sne'].append(to_u32(r[idx['sne']]))
        arr['mp3_ppr'].append(to_u32(r[idx['ppr']]))
        arr['mp3_repair_days'].append(to_u16(r[idx['repair_days']]))
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


def prepare_env_arrays(client) -> Dict[str, object]:
    """Формирует все Env массивы/скаляры для full‑GPU окружения (без применения к модели)."""
    vdate, vid = fetch_versions(client)
    mp3_rows, mp3_fields = fetch_mp3(client, vdate, vid)
    mp1_map = fetch_mp1_br_rt(client)
    mp1_oh_map = fetch_mp1_oh(client)
    mp1_ll_map = fetch_mp1_ll(client)
    mp1_sne_ppr_map = fetch_mp1_sne_ppr_new(client)
    mp1_repair_number_map = fetch_mp1_repair_number(client)
    mp4_by_day = preload_mp4_by_day(client)
    mp5_by_day = preload_mp5_maps(client)

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
        avg_query = """
        SELECT AVG(daily_hours) as avg_minutes
        FROM flight_program_fl
        WHERE ac_type_mask = 64  -- Mi-17
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
    
    # 8. Индексы для spawn
    # first_reserved_idx — для детерминированного spawn (начинается сразу после существующих)
    first_reserved_idx = frames_total_base
    
    # first_dynamic_idx — для динамического spawn (начинается после детерминированного)
    first_dynamic_idx = frames_total_base + deterministic_spawn_mi17
    
    # Индекс первого будущего борта (если присутствует в MP5/union)
    first_future_idx = int(frames_index.get(base_acn_spawn, frames_union_no_future))
    
    # Построение MP5 на расширенном FRAMES (для новых кадров заполняем средним налётом)
    mp5_linear = build_mp5_linear(mp5_by_day, days_sorted, frames_index, frames_total, frames_total_base)
    mp1_br8, mp1_br17, mp1_rt, mp1_pt, mp1_at, mp1_index = build_mp1_arrays(mp1_map)
    # Соберём массивы OH по индексу MP1
    keys_sorted = sorted(mp1_index.keys(), key=lambda k: mp1_index[k])
    mp1_oh8_arr: List[int] = []
    mp1_oh17_arr: List[int] = []
    for k in keys_sorted:
        oh8, oh17 = mp1_oh_map.get(k, (0, 0))
        mp1_oh8_arr.append(int(oh8 or 0))
        mp1_oh17_arr.append(int(oh17 or 0))
    # Соберём массив LL по индексу MP1 (для mi17)
    mp1_ll17_arr: List[int] = []
    for k in keys_sorted:
        llv = mp1_ll_map.get(k, 0)
        mp1_ll17_arr.append(int(llv or 0))
    
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
        'repair_time': mp1_rt,
        'partout_time': mp1_pt,
        'assembly_time': mp1_at,
        'oh_mi8': mp1_oh8_arr,
        'oh_mi17': mp1_oh17_arr,
        'll_mi8': [0] * len(keys_sorted),  # TODO: добавить загрузку ll_mi8
        'll_mi17': mp1_ll17_arr,
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
    
    mi8_tuple = mp1_map[SPAWN_PARTSEQNO_MI8]  # (br_mi8, br_mi17, repair_time, partout_time, assembly_time)
    mi8_repair_time_const = int(mi8_tuple[2])
    mi8_partout_time_const = int(mi8_tuple[3])
    mi8_assembly_time_const = int(mi8_tuple[4])
    
    # Валидация: константы Mi-8 должны быть > 0
    if mi8_repair_time_const <= 0:
        raise ValueError(f"❌ Mi-8 repair_time={mi8_repair_time_const} <= 0 в справочнике md_components!")
    if mi8_partout_time_const <= 0:
        raise ValueError(f"❌ Mi-8 partout_time={mi8_partout_time_const} <= 0 в справочнике md_components!")
    if mi8_assembly_time_const <= 0:
        raise ValueError(f"❌ Mi-8 assembly_time={mi8_assembly_time_const} <= 0 в справочнике md_components!")
    
    # Mi-17: partseqno=70386 (МИ-8АМТ, group_by=2)
    if SPAWN_PARTSEQNO_MI17 not in mp1_map:
        raise ValueError(
            f"❌ partseqno={SPAWN_PARTSEQNO_MI17} (Mi-17, МИ-8АМТ) НЕ найден в справочнике md_components! "
            "Проверьте данные в таблице md_components."
        )
    
    mi17_tuple = mp1_map[SPAWN_PARTSEQNO_MI17]  # (br_mi8, br_mi17, repair_time, partout_time, assembly_time)
    mi17_repair_time_const = int(mi17_tuple[2])
    mi17_partout_time_const = int(mi17_tuple[3])
    mi17_assembly_time_const = int(mi17_tuple[4])
    
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
        'mp5_daily_hours_linear': mp5_linear,
        'month_first_u32': month_first_u32,
        'mp1_map': mp1_map,  # Добавляем mp1_map для прямого доступа (как в sim_master.py)
        'mp1_br_mi8': mp1_br8,
        'mp1_br_mi17': mp1_br17,
        'mp1_repair_time': mp1_rt,
        'mp1_partout_time': mp1_pt,
        'mp1_assembly_time': mp1_at,
        'mp1_oh_mi8': mp1_oh8_arr,
        'mp1_oh_mi17': mp1_oh17_arr,
        'mp1_ll_mi17': mp1_ll17_arr,
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
        'mi17_repair_time_const': mi17_repair_time_const,
        'mi17_partout_time_const': mi17_partout_time_const,
        'mi17_assembly_time_const': mi17_assembly_time_const,
        # Начальная наработка и нормативы для Mi-17 (для spawn)
        'mi17_sne_new_const': int(mi17_sne_new),
        'mi17_ppr_new_const': int(mi17_ppr_new),
        'mi17_ll_const': int(mi17_ll_const),
        'mi17_oh_const': int(mi17_oh_const),
        'mi17_br_const': int(mi17_br_const),
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
    assert len(env_data['mp5_daily_hours_linear']) == (dt + 1) * ft, "MP5_linear размер != (days_total+1)*frames_total"
    assert len(env_data['month_first_u32']) == dt, "month_first_u32 размер не равен days_total"
    # mp3_arrays длины согласованы
    a = env_data['mp3_arrays']
    n3 = int(env_data['mp3_count'])
    for k in ('mp3_psn','mp3_aircraft_number','mp3_ac_type_mask','mp3_group_by','mp3_status_id','mp3_sne','mp3_ppr','mp3_repair_days','mp3_ll','mp3_oh','mp3_mfg_date_days'):
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


