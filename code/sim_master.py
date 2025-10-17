#!/usr/bin/env python3
"""
Repair-only runner:
- Загружает MP1 (repair_time) и MP3 (статус/ремонт/идентификаторы)
- Создаёт популяцию агентов
- Выполняет 1 шаг rtc_repair
- Экспортирует MP2 за D0 (daily_flight=0)
Дата: 2025-08-21
"""

from __future__ import annotations
from typing import Dict, List, Tuple
from datetime import date, timedelta
import time
import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), 'utils'))
from config_loader import get_clickhouse_client
from flame_macroproperty2_exporter import FlameMacroProperty2Exporter
from sim_env_setup import (
    fetch_versions,
    fetch_mp1_br_rt,
    fetch_mp3,
    preload_mp4_by_day,
    preload_mp5_maps,
    build_daily_arrays,
    prepare_env_arrays,
    apply_env_to_sim,
)

from model_build_old import HeliSimModel
from model_build_old import build_model_for_quota_smoke
from model_build_old import MAX_FRAMES, MAX_DAYS, MAX_SIZE

try:
    import pyflamegpu
except Exception:
    pyflamegpu = None


def main():
    if pyflamegpu is None:
        raise RuntimeError("pyflamegpu не установлен")
    # CLI
    import argparse
    p = argparse.ArgumentParser(description='Repair-only runner')
    p.add_argument('--days', type=int, default=1, help='Сколько суток прогонять (начиная с D0)')
    p.add_argument('--clean-mp2', action='store_true', help='TRUNCATE flame_macroproperty2_export перед вставкой')
    p.add_argument('--verify-agents', action='store_true', help='Проверить, что agent.mfg_date заполнен (ordinal)')
    # Отключено для отката к последней рабочей версии (env-only)
    p.add_argument('--rtc-smoke', action='store_true', help='[disabled] RTC smoke (ignored)')
    p.add_argument('--gpu-quota-smoke', action='store_true', help='GPU квоты: один шаг с intent/approve/apply без экспорта')
    p.add_argument('--gpu-quota-smoke-external', action='store_true', help='Запустить внешний стабильный smoke раннер (gpu_quota_smoke_from_env.py)')
    p.add_argument('--mp5-probe', action='store_true', help='Пробный запуск чтения MP5 (dt/dn) в агентные переменные')
    p.add_argument('--jit-log', action='store_true', help='Включить подробный вывод NVRTC/JIT (stdout on error)')
    p.add_argument('--spawn-smoke-real', action='store_true', help='Smoke спавна (GPU-only) на реальных MP4/MP1: шаги N, печать born по дням')
    p.add_argument('--spawn-days', type=int, default=365, help='Сколько суток шагать в spawn-smoke-real (по умолчанию 365)')
    p.add_argument('--seatbelts', choices=['on', 'off'], default='on', help='Включить/выключить FLAMEGPU seatbelts')
    p.add_argument('--status4-smoke', action='store_true', help='Smoke status_4: инкремент repair_days и переход в 5 при достижении repair_time')
    p.add_argument('--status4-smoke-real', action='store_true', help='Smoke status_4 на реальных MP3/MP1: N шагов, переходы 4→5')
    p.add_argument('--status4-days', type=int, default=3, help='Сколько суток шагать в status4-smoke-real (по умолчанию 3)')
    p.add_argument('--status6-smoke-real', action='store_true', help='Smoke status_6 на реальных MP3: N шагов, отсутствие изменений')
    p.add_argument('--status6-days', type=int, default=7, help='Сколько суток шагать в status6-smoke-real (по умолчанию 7)')
    p.add_argument('--status46-smoke-real', action='store_true', help='Совместный smoke status_4 + status_6 на реальных данных')
    p.add_argument('--status46-days', type=int, default=30, help='Сколько суток шагать в status46-smoke-real (по умолчанию 30)')
    p.add_argument('--status2-smoke-real', action='store_true', help='Smoke status_2 на реальных MP3/MP5: инкремент sne/ppr за N суток')
    p.add_argument('--status2-days', type=int, default=7, help='Сколько суток шагать в status2-smoke-real (по умолчанию 7)')
    p.add_argument('--status246-smoke-real', action='store_true', help='Совместный слой 2/4/6: реальный smoke, шаги N, метрики')
    p.add_argument('--status246-days', type=int, default=7, help='Сколько суток шагать в status246-smoke-real (по умолчанию 7)')
    # Расширенный режим: 1/2/4/5/6 с полным квотированием (включая статус 1)
    p.add_argument('--status12456-smoke-real', action='store_true', help='Совместный слой 1/2/4/5/6: реальный smoke с квотами 2,3,5,1')
    p.add_argument('--status12456-days', type=int, default=7, help='Сколько суток шагать в status12456-smoke-real (по умолчанию 7)')
    # Алиасы для сценария с явной фиксацией квотирования в статусе 2
    p.add_argument('--status246q-smoke-real', action='store_true', help='Алиас status246-smoke-real с квотированием в статусе 2 (intent→approve→apply)')
    p.add_argument('--status246q-days', type=int, default=None, help='Сутки для status246q-smoke-real (если не указано, используется --status246-days)')
    p.add_argument('--status2-case-ac', type=int, default=0, help='Целевой aircraft_number для статус-2 кейса (диагностика LL/OH/BR)')
    p.add_argument('--status2-case-days', type=int, default=7, help='Сколько суток шагать в статус-2 кейсе (по умолчанию 7)')
    # Экспорт результатов симуляции в ClickHouse
    p.add_argument('--export-sim', choices=['on', 'off'], default='off', help='Экспортировать результаты симуляции в ClickHouse (по умолчанию off)')
    p.add_argument('--export-sim-table', type=str, default='sim_results', help='Имя таблицы ClickHouse для экспорта (по умолчанию sim_results)')
    p.add_argument('--export-batch', type=int, default=250000, help='Размер батча вставки (по умолчанию 250000)')
    p.add_argument('--export-truncate', action='store_true', default=True, help='TRUNCATE таблицу перед экспортом (по умолчанию on)')
    p.add_argument('--export-triggers-only', action='store_true', help='Экспортировать только триггеры (active/assembly/partout) и ключи даты/идентификации')
    p.add_argument('--export-d0', choices=['on', 'off'], default='on', help='Экспортировать D0-снимок (day_u16=0) до первого шага')
    p.add_argument('--export-postprocess', choices=['on','off'], default='on', help='Выполнить GPU-постпроцессинг MP2 (export_phase=2) перед экспортом')
    a = p.parse_args()
    # CUDA_PATH fallback
    if not os.environ.get('CUDA_PATH'):
        for pth in [
            "/usr/local/cuda",
            "/usr/local/cuda-12.4",
            "/usr/local/cuda-12.3",
            "/usr/local/cuda-12.2",
            "/usr/local/cuda-12.1",
            "/usr/local/cuda-12.0",
        ]:
            if os.path.isdir(pth) and os.path.isdir(os.path.join(pth, 'include')):
                os.environ['CUDA_PATH'] = pth
                break

    # Seatbelts & JIT log env flags (до создания симуляции)
    if a.seatbelts == 'on':
        os.environ['FLAMEGPU_SEATBELTS'] = '1'
    else:
        os.environ['FLAMEGPU_SEATBELTS'] = '0'
    if a.jit_log:
        os.environ['PYTHONUNBUFFERED'] = '1'

    client = get_clickhouse_client()
    vdate, vid = fetch_versions(client)
    mp1_map = fetch_mp1_br_rt(client)  # partseqno_i -> (br_mi8, br_mi17, repair_time, partout_time, assembly_time)
    mp3_rows, mp3_fields = fetch_mp3(client, vdate, vid)
    n = len(mp3_rows)

    # Внешний стабильный smoke (не использует HeliSimModel)
    if a.gpu_quota_smoke_external:
        from gpu_quota_smoke_from_env import main as quota_smoke_main
        quota_smoke_main()
        return

    model = HeliSimModel()
    mp4 = preload_mp4_by_day(client)
    # Подготовка Env Property/MacroProperty (MP1/MP3/MP4/MP5, скаляры)
    env_data = prepare_env_arrays(client)

    # === Spawn smoke (GPU-only RTC спавн) ===
    if a.spawn_smoke_real:
        # Собираем минимальную модель с RTC-ядром спавна и запускаем симуляцию
        FRAMES = int(env_data['frames_total_u16'])
        DAYS = int(a.spawn_days)
        # Отключаем квотные RTC-слои: оставляем только спавн
        os.environ['HL_RTC_MODE'] = 'spawn_only'
        # Гарантируем корректный CUDA_PATH
        if not os.environ.get('CUDA_PATH'):
            for pth in ["/usr/local/cuda-12.8", "/usr/local/cuda", "/usr/local/cuda-12", "/usr/local/cuda-12.4", "/usr/local/cuda-12.3", "/usr/local/cuda-12.2", "/usr/local/cuda-12.1", "/usr/local/cuda-12.0"]:
                if os.path.isdir(pth) and os.path.isdir(os.path.join(pth, 'include')):
                    os.environ['CUDA_PATH'] = pth
                    break
        # Минимизируем окружение (без MP1/MP5), оставляем MP4 и служебные
        os.environ['HL_ENV_MINIMAL'] = '1'
        hm = HeliSimModel()
        hm.build_model(num_agents=max(1, FRAMES), env_sizes={
            'days_total': DAYS,
            'frames_total': FRAMES,
            'mp1_len': 1,
            'mp3_count': max(1, int(env_data.get('mp3_count', 1)))
        })
        sim = hm.build_simulation()
        # Устанавливаем скаляры и усечённые массивы на DAYS
        sim.setEnvironmentPropertyUInt("version_date", int(env_data['version_date_u16']))
        sim.setEnvironmentPropertyUInt("frames_total", FRAMES)
        sim.setEnvironmentPropertyUInt("days_total", DAYS)
        sim.setEnvironmentPropertyUInt("frames_initial", int(env_data.get('mp3_count', 0)))
        # MP4/seed/month_first (усечённые до DAYS)
        try:
            import numpy as _np
            _ops8  = _np.asarray(list(env_data['mp4_ops_counter_mi8'])[:DAYS], dtype=_np.uint32)
            _ops17 = _np.asarray(list(env_data['mp4_ops_counter_mi17'])[:DAYS], dtype=_np.uint32)
            _seed  = _np.asarray(list(env_data['mp4_new_counter_mi17_seed'])[:DAYS], dtype=_np.uint32)
            _m1    = _np.asarray(list(env_data['month_first_u32'])[:DAYS], dtype=_np.uint32)
            sim.setEnvironmentPropertyArrayUInt32("mp4_ops_counter_mi8", _ops8.tolist())
            sim.setEnvironmentPropertyArrayUInt32("mp4_ops_counter_mi17", _ops17.tolist())
            sim.setEnvironmentPropertyArrayUInt32("mp4_new_counter_mi17_seed", _seed.tolist())
            sim.setEnvironmentPropertyArrayUInt32("month_first_u32", _m1.tolist())
        except Exception:
            sim.setEnvironmentPropertyArrayUInt32("mp4_ops_counter_mi8", [int(x) for x in list(env_data['mp4_ops_counter_mi8')][:DAYS]])
            sim.setEnvironmentPropertyArrayUInt32("mp4_ops_counter_mi17", [int(x) for x in list(env_data['mp4_ops_counter_mi17'])[:DAYS]])
            sim.setEnvironmentPropertyArrayUInt32("mp4_new_counter_mi17_seed", [int(x) for x in list(env_data['mp4_new_counter_mi17_seed'])[:DAYS]])
            sim.setEnvironmentPropertyArrayUInt32("month_first_u32", [int(x) for x in list(env_data['month_first_u32'])[:DAYS]])
        # Инициализируем 16 тикетов (0..15) и одного менеджера
        spawn_desc = hm.model.getAgent("spawn_ticket")
        st = pyflamegpu.AgentVector(spawn_desc, 16)
        for i in range(16):
            st[i].setVariableUInt("ticket", i)
        mgr_desc = hm.model.getAgent("spawn_mgr")
        sm = pyflamegpu.AgentVector(mgr_desc, 1)
        sim.setPopulationData(st)
        sim.setPopulationData(sm)
        # Прогоним DAYS суток и печатаем рождение по дням
        import datetime as _dt
        base_epoch = _dt.date(1970, 1, 1)
        prev_total = int(env_data.get('mp3_count', 0))
        comp_desc = hm.model.getAgent("component")
        for d in range(DAYS):
            sim.step()
            comp_pop = pyflamegpu.AgentVector(comp_desc)
            sim.getPopulationData(comp_pop)
            total = len(comp_pop)
            born = max(0, total - prev_total)
            day_date = base_epoch + _dt.timedelta(days=int(env_data['version_date_u16']) + d + 1)
            print(f"D={d+1} date={day_date} born={born} total={total}")
            prev_total = total
        return

    # === Экспорт симуляции: подготовка DDL ===
    def ensure_sim_results_table(_client, table_name: str) -> None:
        ddl = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
          version_date     UInt32,
          version_id       UInt32,
          version_date_date Date,
          day_u16          UInt16,
          day_abs          UInt32,
          day_date         Date,
          idx              UInt16,
          aircraft_number  UInt32,
          mfg_date_date    Date,
          partseqno_i      UInt32,
          group_by         UInt8,
          status_id        UInt8,
          repair_days      UInt16,
          repair_time      UInt16,
          assembly_time    UInt16,
          partout_time     UInt16,
          sne              UInt32,
          ppr              UInt32,
          ll               UInt32,
          oh               UInt32,
          br               UInt32,
          daily_today_u32  UInt32,
          daily_next_u32   UInt32,
          ops_ticket       UInt8,
          intent_flag      UInt8,
          active_trigger   UInt16,
          assembly_trigger UInt16,
          partout_trigger  UInt8
        )
        ENGINE = MergeTree
        PARTITION BY version_date
        ORDER BY (version_date, day_u16, aircraft_number, idx)
        """
        _client.execute(ddl)
        # Эволюция схемы: добавим недостающие столбцы, если таблица уже была создана раньше
        try:
            cols = {row[0] for row in _client.execute(f"DESCRIBE TABLE {table_name}")}
            alters = []
            if 'version_date_date' not in cols:
                alters.append(f"ADD COLUMN version_date_date Date AFTER version_id")
            if 'day_date' not in cols:
                alters.append(f"ADD COLUMN day_date Date AFTER day_abs")
            if 'mfg_date_date' not in cols:
                alters.append(f"ADD COLUMN mfg_date_date Date AFTER aircraft_number")
            # psn и дополнительные служебные поля не используются в sim_results
            for stmt in alters:
                _client.execute(f"ALTER TABLE {table_name} {stmt}")
        except Exception as _e:
            print(f"⚠️ Не удалось выполнить DESCRIBE/ALTER {table_name}: {_e}")

    def export_day_snapshot(_client, table_name: str, version_date_u32: int, version_id_u32: int,
                             day_idx: int, rows_src, pop_after, frames_total: int,
                             idx_map_local, mp1_map_local, batch_buf: list, triggers_only: bool=False,
                             max_agents: int | None = None, include_psn: bool=False) -> None:
        day_u16 = int(day_idx)
        if day_u16 < 0:
            day_u16 = 0
        if day_u16 > 65535:
            day_u16 = 65535
        # Дата абсолютная должна соответствовать day_u16: D0 = version_date, D1 = version_date+1, ...
        day_abs = int(version_date_u32 + day_u16)
        # Даты ClickHouse (Date) считаем от эпохи 1970-01-01
        from datetime import date as _date, timedelta as _td
        base_epoch = _date(1970, 1, 1)
        version_date_date = base_epoch + _td(days=int(version_date_u32))
        day_date = base_epoch + _td(days=int(day_abs))
        K_loc = len(rows_src)
        N_pop = len(pop_after)
        if max_agents is not None:
            N_pop = min(N_pop, int(max_agents))
        # Предзагружаем partout_time/repair_time/assembly_time из MP1 по partseq для исходных K_loc
        partseq_list = [int(rows_src[i][idx_map_local.get('partseqno_i', -1)] or 0) for i in range(K_loc)]
        rt_cache = [0] * K_loc
        pt_cache = [0] * K_loc
        at_cache = [0] * K_loc
        for i in range(K_loc):
            ps = partseq_list[i]
            tpl = mp1_map_local.get(ps, (0,0,0,0,0))
            rt_cache[i] = int(tpl[2] or 0)
            pt_cache[i] = int(tpl[3] or 0)
            at_cache[i] = int(tpl[4] or 0)
        # Собираем строки по всей текущей популяции (Variant A)
        for i in range(N_pop):
            ag = pop_after[i]
            idx_v = int(ag.getVariableUInt('idx'))
            # aircraft_number из агента; фолбэк по исходным данным при наличии
            try:
                aircraft_v = int(ag.getVariableUInt('aircraft_number'))
            except Exception:
                aircraft_v = int(rows_src[i][idx_map_local.get('aircraft_number', -1)] or 0) if i < K_loc else 0
            # mfg_date_date: конвертируем ord-дни UInt → Date
            try:
                mfg_ord = int(ag.getVariableUInt('mfg_date'))
            except Exception:
                mfg_ord = 0
            from datetime import date as _date, timedelta as _td
            base_epoch = _date(1970, 1, 1)
            mfg_date_date = base_epoch + _td(days=max(0, mfg_ord)) if mfg_ord > 0 else base_epoch
            # partseq: из агента, иначе из rows_src (для исходных), иначе константа типа MI-17
            try:
                partseq_v = int(ag.getVariableUInt('partseqno_i'))
            except Exception:
                partseq_v = int(partseq_list[i]) if i < K_loc else 70482
            # Группа/статус/счётчики
            try:
                group_v = int(ag.getVariableUInt('group_by'))
            except Exception:
                group_v = (2 if i >= K_loc else 0)
            try:
                orig_status_v = int(ag.getVariableUInt('status_id'))
            except Exception:
                orig_status_v = 0
            try:
                orig_repair_days_v = int(ag.getVariableUInt('repair_days'))
            except Exception:
                orig_repair_days_v = 0
            try:
                ac_type_mask_v = int(ag.getVariableUInt('ac_type_mask'))
            except Exception:
                ac_type_mask_v = 0
            # Нормативы времени: сначала из агента, затем кэши исходных строк, затем 0
            try:
                repair_time_v = int(ag.getVariableUInt('repair_time'))
            except Exception:
                repair_time_v = (rt_cache[i] if i < K_loc else 0)
            try:
                assembly_time_v = int(ag.getVariableUInt('assembly_time'))
            except Exception:
                assembly_time_v = (at_cache[i] if i < K_loc else 0)
            try:
                partout_time_v = int(ag.getVariableUInt('partout_time'))
            except Exception:
                partout_time_v = (pt_cache[i] if i < K_loc else 0)
            # Пороги/счётчики
            try:
                s6_days_v = int(ag.getVariableUInt('s6_days'))
            except Exception:
                s6_days_v = 0
            try:
                s6_started_v = int(ag.getVariableUInt('s6_started'))
            except Exception:
                s6_started_v = 0
            try:
                sne_v = int(ag.getVariableUInt('sne'))
            except Exception:
                sne_v = 0
            try:
                ppr_v = int(ag.getVariableUInt('ppr'))
            except Exception:
                ppr_v = 0
            try:
                ll_v = int(ag.getVariableUInt('ll'))
            except Exception:
                ll_v = 0
            try:
                oh_v = int(ag.getVariableUInt('oh'))
            except Exception:
                oh_v = 0
            try:
                br_v = int(ag.getVariableUInt('br'))
            except Exception:
                br_v = 0
            try:
                dt_v = int(ag.getVariableUInt('daily_today_u32'))
            except Exception:
                dt_v = 0
            try:
                dn_v = int(ag.getVariableUInt('daily_next_u32'))
            except Exception:
                dn_v = 0
            try:
                ticket_v = int(ag.getVariableUInt('ops_ticket'))
            except Exception:
                ticket_v = 0
            try:
                intent_v = int(ag.getVariableUInt('intent_flag'))
            except Exception:
                intent_v = 0
            try:
                act_v = int(ag.getVariableUInt('active_trigger'))
            except Exception:
                act_v = 0
            try:
                asm_v = int(ag.getVariableUInt('assembly_trigger'))
            except Exception:
                asm_v = 0
            try:
                part_tr_v = int(ag.getVariableUInt('partout_trigger'))
            except Exception:
                part_tr_v = 0
            try:
                act_mark_v = int(ag.getVariableUInt('active_trigger_mark'))
            except Exception:
                act_mark_v = 0
            try:
                asm_mark_v = int(ag.getVariableUInt('assembly_trigger_mark'))
            except Exception:
                asm_mark_v = 0
            try:
                part_mark_v = int(ag.getVariableUInt('partout_trigger_mark'))
            except Exception:
                part_mark_v = 0
            # psn
            try:
                psn_v = int(ag.getVariableUInt('psn'))
            except Exception:
                psn_v = 0
            if triggers_only:
                batch_buf.append((
                    int(version_date_u32), int(version_id_u32), version_date_date, int(day_u16), int(day_abs), day_date,
                    int(idx_v), int(aircraft_v), int(act_v), int(asm_v), int(part_tr_v)
                ))
            else:
                if include_psn:
                    batch_buf.append((
                        int(version_date_u32), int(version_id_u32), version_date_date, int(day_u16), int(day_abs), day_date,
                        int(idx_v), int(aircraft_v), int(psn_v), mfg_date_date, int(partseq_v), int(group_v), int(orig_status_v),
                        int(orig_repair_days_v), int(repair_time_v), int(assembly_time_v), int(partout_time_v),
                        int(sne_v), int(ppr_v), int(ll_v), int(oh_v), int(br_v), int(dt_v), int(dn_v),
                        int(ticket_v), int(intent_v), int(act_v), int(asm_v), int(part_tr_v)
                    ))
                else:
                    batch_buf.append((
                        int(version_date_u32), int(version_id_u32), version_date_date, int(day_u16), int(day_abs), day_date,
                        int(idx_v), int(aircraft_v), mfg_date_date, int(partseq_v), int(group_v), int(orig_status_v),
                        int(orig_repair_days_v), int(repair_time_v), int(assembly_time_v), int(partout_time_v),
                        int(sne_v), int(ppr_v), int(ll_v), int(oh_v), int(br_v), int(dt_v), int(dn_v),
                        int(ticket_v), int(intent_v), int(act_v), int(asm_v), int(part_tr_v)
                    ))
        # Вставляем при переполнении буфера — делается снаружи
        return

    def flush_export_buffer(_client, table_name: str, buf: list, columns_override: str | None = None) -> float:
        if not buf:
            return 0.0
        if columns_override is not None:
            cols = columns_override
        else:
            cols = (
                "version_date,version_id,version_date_date,day_u16,day_abs,day_date,idx,aircraft_number,mfg_date_date,partseqno_i,group_by,status_id,"
                "repair_days,repair_time,assembly_time,partout_time,sne,ppr,ll,oh,br,daily_today_u32,daily_next_u32,"
                "ops_ticket,intent_flag,active_trigger,assembly_trigger,partout_trigger"
            )
        query = f"INSERT INTO {table_name} ({cols}) VALUES"
        import time as _t
        _t0 = _t.perf_counter()
        _client.execute(query, buf)
        buf.clear()
        return (_t.perf_counter() - _t0)

    # === Status_4 smoke через фабрику ===
    if a.status4_smoke:
        FRAMES = int(env_data['frames_total_u16'])
        DAYS = int(env_data['days_total_u16'])
        os.environ['HL_STATUS4_SMOKE'] = '1'
        model2, a_desc = build_model_for_quota_smoke(FRAMES, DAYS)
        sim2 = pyflamegpu.CUDASimulation(model2)
        sim2.setEnvironmentPropertyUInt("version_date", int(env_data['version_date_u16']))
        sim2.setEnvironmentPropertyUInt("frames_total", FRAMES)
        sim2.setEnvironmentPropertyUInt("days_total", DAYS)
        # Установим ENV‑константы нормативов для MI‑17 строго по partno_comp=70482 из MP1
        try:
            _mi17_partseq = 70482
            _mi17_tuple = mp1_map.get(70482, (0,0,0,0,0))
            sim2.setEnvironmentPropertyUInt("mi17_repair_time_const", int(_mi17_tuple[2] or 0))
            sim2.setEnvironmentPropertyUInt("mi17_partout_time_const", int(_mi17_tuple[3] or 0))
            sim2.setEnvironmentPropertyUInt("mi17_assembly_time_const", int(_mi17_tuple[4] or 0))
            try:
                print(f"spawn ENV consts (MI-17 from MP1): partseq={_mi17_partseq}, rt={int(_mi17_tuple[2] or 0)}, pt={int(_mi17_tuple[3] or 0)}, at={int(_mi17_tuple[4] or 0)}")
            except Exception:
                pass
        except Exception as _e:
            print(f"⚠️ MI-17 env consts set skipped: {_e}")
        # Передадим константы нормативов для MI-17 в ENV (берём partseq из MP3 при наличии, иначе 70482)
        _mi17_partseq = 70482
        try:
            pidx = idx_map.get('partseqno_i', -1)
            midx = idx_map.get('ac_type_mask', -1)
            gbix = idx_map.get('group_by', -1)
            if pidx >= 0 and rows:
                for _r in rows:
                    _mask = int(_r[midx] or 0) if midx >= 0 else 0
                    _gb = int(_r[gbix] or 0) if gbix >= 0 else 0
                    if (_mask & 64) or (_gb == 2):
                        _mi17_partseq = int(_r[pidx] or 0)
                        break
        except Exception:
            pass
        _mi17_tuple = mp1_map.get(_mi17_partseq, (0,0,0,0,0))
        sim2.setEnvironmentPropertyUInt("mi17_repair_time_const", int(_mi17_tuple[2] or 0))
        sim2.setEnvironmentPropertyUInt("mi17_partout_time_const", int(_mi17_tuple[3] or 0))
        sim2.setEnvironmentPropertyUInt("mi17_assembly_time_const", int(_mi17_tuple[4] or 0))
        try:
            print(f"spawn ENV consts (MI-17 from MP1): partseq={_mi17_partseq}, rt={int(_mi17_tuple[2] or 0)}, pt={int(_mi17_tuple[3] or 0)}, at={int(_mi17_tuple[4] or 0)}")
        except Exception:
            pass
        # Построим популяцию: возьмем K=8 агентов в статусе 4 с repair_time=3
        K = min(8, FRAMES)
        av = pyflamegpu.AgentVector(a_desc, K)
        for i in range(K):
            av[i].setVariableUInt("idx", i)
            av[i].setVariableUInt("group_by", 1)
            av[i].setVariableUInt("status_id", 4)
            av[i].setVariableUInt("repair_days", 0)
            av[i].setVariableUInt("repair_time", 3)
            av[i].setVariableUInt("ppr", 123)
        sim2.setPopulationData(av)
        # До шага: счётчики
        before = pyflamegpu.AgentVector(a_desc)
        sim2.getPopulationData(before)
        s4 = sum(1 for ag in before if int(ag.getVariableUInt('status_id')) == 4)
        rd = [int(ag.getVariableUInt('repair_days')) for ag in before]
        sim2.step()
        after = pyflamegpu.AgentVector(a_desc)
        sim2.getPopulationData(after)
        s4_a = sum(1 for ag in after if int(ag.getVariableUInt('status_id')) == 4)
        s5_a = sum(1 for ag in after if int(ag.getVariableUInt('status_id')) == 5)
        rd_a = [int(ag.getVariableUInt('repair_days')) for ag in after]
        print(f"status4_smoke: before s4={s4}, repair_days={rd}; after s4={s4_a}, s5={s5_a}, repair_days={rd_a}")
        # === GPU-постпроцессинг MP2 и экспорт после симуляции ===
        if export_on and getattr(a, 'export_postprocess', 'on') == 'on':
            # Постпроцессинг: export_phase=2, один шаг
            sim2.setEnvironmentPropertyUInt("export_phase", 2)
            t_gpp0 = _t.perf_counter()
            sim2.step()
            t_gpu_s += (_t.perf_counter() - t_gpp0)
            # Экспорт по дням через copyout (export_phase=1)
            sim2.setEnvironmentPropertyUInt("export_phase", 1)
            export_buf.clear()
            # Copyout: публикуем D1..Dsteps как day_u16=1..steps, D0 уже выгружен отдельно
            for d_exp in range(steps):
                sim2.setEnvironmentPropertyUInt("export_day", int(d_exp))
                t_gco0 = _t.perf_counter()
                sim2.step()
                t_gpu_s += (_t.perf_counter() - t_gco0)
                pop_day = pyflamegpu.AgentVector(a_desc)
                sim2.getPopulationData(pop_day)
                export_day_snapshot(
                    client, export_table, int(env_data['version_date_u16']), int(vid), d_exp,
                    rows, pop_day, FRAMES, idx_map, mp1_map, export_buf,
                    triggers_only=export_triggers_only
                )
                if len(export_buf) >= export_batch:
                    if export_triggers_only:
                        cols = "version_date,version_id,version_date_date,day_u16,day_abs,day_date,idx,aircraft_number,psn,active_trigger,assembly_trigger,partout_trigger"
                        t_db_s += flush_export_buffer(client, export_table, export_buf, columns_override=cols)
                    else:
                        t_db_s += flush_export_buffer(client, export_table, export_buf)
            if export_buf:
                if export_triggers_only:
                    cols = "version_date,version_id,version_date_date,day_u16,day_abs,day_date,idx,aircraft_number,psn,active_trigger,assembly_trigger,partout_trigger"
                    t_db_s += flush_export_buffer(client, export_table, export_buf, columns_override=cols)
                else:
                    t_db_s += flush_export_buffer(client, export_table, export_buf)
            # Сброс в симуляционную фазу
            sim2.setEnvironmentPropertyUInt("export_phase", 0)
        return

    # === Status_4 smoke (REAL data) через фабрику ===
    if a.status4_smoke_real:
        FRAMES = int(env_data['frames_total_u16'])
        DAYS = int(env_data['days_total_u16'])
        os.environ['HL_STATUS4_SMOKE'] = '1'
        model2, a_desc = build_model_for_quota_smoke(FRAMES, DAYS)
        sim2 = pyflamegpu.CUDASimulation(model2)
        sim2.setEnvironmentPropertyUInt("version_date", int(env_data['version_date_u16']))
        sim2.setEnvironmentPropertyUInt("frames_total", FRAMES)
        sim2.setEnvironmentPropertyUInt("days_total", DAYS)
        # Популяция: все агенты из MP3 со status_id=4
        idx_map = {name: i for i, name in enumerate(mp3_fields)}
        s4_rows = [r for r in mp3_rows if int(r[idx_map['status_id']] or 0) == 4]
        K = len(s4_rows)
        av = pyflamegpu.AgentVector(a_desc, K)
        for i, r in enumerate(s4_rows):
            av[i].setVariableUInt("idx", int(i % max(1, FRAMES)))
            av[i].setVariableUInt("group_by", 1)
            av[i].setVariableUInt("status_id", 4)
            av[i].setVariableUInt("repair_days", int(r[idx_map['repair_days']] or 0))
            partseq = int(r[idx_map['partseqno_i']] or 0) if 'partseqno_i' in idx_map else 0
            rt = mp1_map.get(partseq, (0,0,0,0,0))[2]
            av[i].setVariableUInt("repair_time", int(rt or 0))
            pt = mp1_map.get(partseq, (0,0,0,0,0))[3]
            at = mp1_map.get(partseq, (0,0,0,0,0))[4]
            av[i].setVariableUInt("partout_time", int(pt or 0))
            av[i].setVariableUInt("assembly_time", int(at or 0))
            av[i].setVariableUInt("partout_trigger", 0)
            av[i].setVariableUInt("assembly_trigger", 0)
            av[i].setVariableUInt("ppr", int(r[idx_map.get('ppr', -1)] or 0))
        sim2.setPopulationData(av)
        # Before
        before = pyflamegpu.AgentVector(a_desc)
        sim2.getPopulationData(before)
        s4_b = sum(1 for ag in before if int(ag.getVariableUInt('status_id')) == 4)
        # Подготовим гистограмму repair_time
        rt_hist: Dict[int,int] = {}
        for ag in before:
            rt = int(ag.getVariableUInt('repair_time'))
            rt_hist[rt] = rt_hist.get(rt, 0) + 1
        # N шагов
        steps = max(1, int(a.status4_days))
        K = len(s4_rows)
        transitioned = [0] * K  # день перехода 4->5 (1..steps), 0 если не перешёл
        for day in range(1, steps + 1):
            sim2.step()
            snap = pyflamegpu.AgentVector(a_desc)
            sim2.getPopulationData(snap)
            for i in range(K):
                if transitioned[i] == 0:
                    if int(snap[i].getVariableUInt('status_id')) == 5:
                        transitioned[i] = day
        # Итоговые метрики
        after = pyflamegpu.AgentVector(a_desc)
        sim2.getPopulationData(after)
        s4_a = sum(1 for ag in after if int(ag.getVariableUInt('status_id')) == 4)
        s5_a = sum(1 for ag in after if int(ag.getVariableUInt('status_id')) == 5)
        # Инварианты на перешедших
        ppr_ok = True
        for i, ag in enumerate(after):
            if int(ag.getVariableUInt('status_id')) == 5:
                if int(ag.getVariableUInt('ppr')) != 0 or int(ag.getVariableUInt('repair_days')) != 0:
                    ppr_ok = False
                    break
        # Гистограмма переходов
        trans_hist: Dict[int,int] = {}
        for d in transitioned:
            if d > 0:
                trans_hist[d] = trans_hist.get(d, 0) + 1
        # Печать
        rt_items = sorted(rt_hist.items())
        th_items = sorted(trans_hist.items())
        print(f"status4_smoke_real: steps={steps}, before s4={s4_b}, after s4={s4_a}, s5={s5_a}, inv(ppr=0,repair_days=0 on 5)={ppr_ok}")
        # ASCII-гистограммы
        print("repair_time_hist:")
        for k, v in rt_items:
            bar = '#'*min(v, 60)
            print(f"  {k:>5}: {bar} ({v})")
        print("transition_day_hist:")
        for k, v in th_items:
            bar = '#'*min(v, 60)
            print(f"  {k:>5}: {bar} ({v})")
        return

    # === Status_6 smoke (REAL data): пасс-тру, инварианты ===
    if a.status6_smoke_real:
        FRAMES = int(env_data['frames_total_u16'])
        DAYS = int(env_data['days_total_u16'])
        os.environ['HL_STATUS6_SMOKE'] = '1'
        model2, a_desc = build_model_for_quota_smoke(FRAMES, DAYS)
        sim2 = pyflamegpu.CUDASimulation(model2)
        sim2.setEnvironmentPropertyUInt("version_date", int(env_data['version_date_u16']))
        sim2.setEnvironmentPropertyUInt("frames_total", FRAMES)
        sim2.setEnvironmentPropertyUInt("days_total", DAYS)
        idx_map = {name: i for i, name in enumerate(mp3_fields)}
        s6_rows = [r for r in mp3_rows if int(r[idx_map['status_id']] or 0) == 6]
        K = len(s6_rows)
        av = pyflamegpu.AgentVector(a_desc, K)
        for i, r in enumerate(s6_rows):
            av[i].setVariableUInt("idx", int(i % max(1, FRAMES)))
            av[i].setVariableUInt("group_by", 1)
            av[i].setVariableUInt("status_id", 6)
            av[i].setVariableUInt("repair_days", int(r[idx_map['repair_days']] or 0))
            av[i].setVariableUInt("repair_time", 0)
            av[i].setVariableUInt("partout_time", 0)
            av[i].setVariableUInt("assembly_time", 0)
            av[i].setVariableUInt("partout_trigger", 0)
            av[i].setVariableUInt("assembly_trigger", 0)
            av[i].setVariableUInt("ppr", int(r[idx_map.get('ppr', -1)] or 0))
        sim2.setPopulationData(av)
        before = pyflamegpu.AgentVector(a_desc)
        sim2.getPopulationData(before)
        s6_b = sum(1 for ag in before if int(ag.getVariableUInt('status_id')) == 6)
        sne_b = [int(ag.getVariableUInt('sne')) if 'sne' in dir(ag) else 0 for ag in before]
        ppr_b = [int(ag.getVariableUInt('ppr')) for ag in before]
        rd_b = [int(ag.getVariableUInt('repair_days')) for ag in before]
        steps = max(1, int(a.status6_days))
        for _ in range(steps):
            sim2.step()
        after = pyflamegpu.AgentVector(a_desc)
        sim2.getPopulationData(after)
        s6_a = sum(1 for ag in after if int(ag.getVariableUInt('status_id')) == 6)
        sne_a = [int(ag.getVariableUInt('sne')) if 'sne' in dir(ag) else 0 for ag in after]
        ppr_a = [int(ag.getVariableUInt('ppr')) for ag in after]
        rd_a = [int(ag.getVariableUInt('repair_days')) for ag in after]
        invariants = (s6_b == s6_a) and (sne_b == sne_a) and (ppr_b == ppr_a) and (rd_b == rd_a)
        print(f"status6_smoke_real: steps={steps}, s6_before={s6_b}, s6_after={s6_a}, invariants={invariants}")
        return

    # === Совместный статус 4+6 (REAL) ===
    if a.status46_smoke_real:
        FRAMES = int(env_data['frames_total_u16'])
        DAYS = int(env_data['days_total_u16'])
        os.environ['HL_STATUS4_SMOKE'] = '1'
        os.environ['HL_STATUS6_SMOKE'] = '1'
        model2, a_desc = build_model_for_quota_smoke(FRAMES, DAYS)
        sim2 = pyflamegpu.CUDASimulation(model2)
        sim2.setEnvironmentPropertyUInt("version_date", int(env_data['version_date_u16']))
        sim2.setEnvironmentPropertyUInt("frames_total", FRAMES)
        sim2.setEnvironmentPropertyUInt("days_total", DAYS)
        idx_map = {name: i for i, name in enumerate(mp3_fields)}
        s4_rows = [r for r in mp3_rows if int(r[idx_map['status_id']] or 0) == 4]
        s6_rows = [r for r in mp3_rows if int(r[idx_map['status_id']] or 0) == 6]
        K4 = len(s4_rows)
        K6 = len(s6_rows)
        K = K4 + K6
        av = pyflamegpu.AgentVector(a_desc, K)
        # Заполняем сначала статус 4, затем 6
        for i, r in enumerate(s4_rows):
            av[i].setVariableUInt("idx", int(i % max(1, FRAMES)))
            av[i].setVariableUInt("group_by", 1)
            av[i].setVariableUInt("status_id", 4)
            av[i].setVariableUInt("repair_days", int(r[idx_map['repair_days']] or 0))
            partseq = int(r[idx_map['partseqno_i']] or 0) if 'partseqno_i' in idx_map else 0
            rt = mp1_map.get(partseq, (0,0,0,0,0))[2]
            av[i].setVariableUInt("repair_time", int(rt or 0))
            pt = mp1_map.get(partseq, (0,0,0,0,0))[3]
            at = mp1_map.get(partseq, (0,0,0,0,0))[4]
            av[i].setVariableUInt("partout_time", int(pt or 0))
            av[i].setVariableUInt("assembly_time", int(at or 0))
            av[i].setVariableUInt("partout_trigger", 0)
            av[i].setVariableUInt("assembly_trigger", 0)
            av[i].setVariableUInt("ppr", int(r[idx_map.get('ppr', -1)] or 0))
        for j, r in enumerate(s6_rows):
            i = K4 + j
            av[i].setVariableUInt("idx", int(i % max(1, FRAMES)))
            av[i].setVariableUInt("group_by", 1)
            av[i].setVariableUInt("status_id", 6)
            av[i].setVariableUInt("repair_days", int(r[idx_map['repair_days']] or 0))
            av[i].setVariableUInt("repair_time", 0)
            av[i].setVariableUInt("partout_time", 0)
            av[i].setVariableUInt("assembly_time", 0)
            av[i].setVariableUInt("partout_trigger", 0)
            av[i].setVariableUInt("assembly_trigger", 0)
            av[i].setVariableUInt("ppr", int(r[idx_map.get('ppr', -1)] or 0))
        sim2.setPopulationData(av)
        # Гистограмма repair_time по 4 и инварианты для 6
        before = pyflamegpu.AgentVector(a_desc)
        sim2.getPopulationData(before)
        rt_hist: Dict[int,int] = {}
        for i in range(K4):
            rt = int(before[i].getVariableUInt('repair_time'))
            rt_hist[rt] = rt_hist.get(rt, 0) + 1
        s6_b = sum(1 for i in range(K4, K) if int(before[i].getVariableUInt('status_id')) == 6)
        steps = max(1, int(a.status46_days))
        transitioned = [0] * K4
        for day in range(1, steps + 1):
            sim2.step()
            snap = pyflamegpu.AgentVector(a_desc)
            sim2.getPopulationData(snap)
            for i in range(K4):
                if transitioned[i] == 0 and int(snap[i].getVariableUInt('status_id')) == 5:
                    transitioned[i] = day
        after = pyflamegpu.AgentVector(a_desc)
        sim2.getPopulationData(after)
        s4_to5 = sum(1 for i in range(K4) if int(after[i].getVariableUInt('status_id')) == 5)
        # Инварианты для 6
        s6_a = sum(1 for i in range(K4, K) if int(after[i].getVariableUInt('status_id')) == 6)
        inv6 = True
        for i in range(K4, K):
            if int(after[i].getVariableUInt('status_id')) != 6:
                inv6 = False
                break
        trans_hist: Dict[int,int] = {}
        for d in transitioned:
            if d > 0:
                trans_hist[d] = trans_hist.get(d, 0) + 1
        print(f"status46_smoke_real: steps={steps}, s4_to5={s4_to5}/{K4}, s6_before={s6_b}, s6_after={s6_a}, inv6={inv6}")
        # ASCII-гистограммы
        print("repair_time_hist:")
        for k, v in sorted(rt_hist.items()):
            bar = '#'*min(v, 60)
            print(f"  {k:>5}: {bar} ({v})")
        print("transition_day_hist:")
        for k, v in sorted(trans_hist.items()):
            bar = '#'*min(v, 60)
            print(f"  {k:>5}: {bar} ({v})")
        return

    # === Status_2 smoke (REAL data): sne/ppr инкремент от MP5 ===
    if a.status2_smoke_real:
        FRAMES = int(env_data['frames_total_u16'])
        DAYS = int(env_data['days_total_u16'])
        os.environ['HL_STATUS2_SMOKE'] = '1'
        model2, a_desc = build_model_for_quota_smoke(FRAMES, DAYS)
        sim2 = pyflamegpu.CUDASimulation(model2)
        sim2.setEnvironmentPropertyUInt("version_date", int(env_data['version_date_u16']))
        sim2.setEnvironmentPropertyUInt("frames_total", FRAMES)
        sim2.setEnvironmentPropertyUInt("days_total", DAYS)
        # Заполним MP5 на host: day 0 dt, day 1 dn и так далее не требуются в статус2 smoke
        # Популяция: все агенты из MP3 со status_id=2
        idx_map = {name: i for i, name in enumerate(mp3_fields)}
        s2_rows = [r for r in mp3_rows if int(r[idx_map['status_id']] or 0) == 2]
        K = len(s2_rows)
        days_sorted = env_data['days_sorted']
        av = pyflamegpu.AgentVector(a_desc, K)
        frames_index = env_data.get('frames_index', {})
        for i, r in enumerate(s2_rows):
            ac = int(r[idx_map['aircraft_number']] or 0)
            fi = int(frames_index.get(ac, i % max(1, FRAMES)))
            av[i].setVariableUInt("idx", fi)
            av[i].setVariableUInt("group_by", 1)
            av[i].setVariableUInt("status_id", 2)
            av[i].setVariableUInt("sne", int(r[idx_map.get('sne', -1)] or 0))
            av[i].setVariableUInt("ppr", int(r[idx_map.get('ppr', -1)] or 0))
            # Для smoke берём dt из MP5 линейного массива D0
            base = 0 * FRAMES + (fi if fi < FRAMES else 0)
            dt = int(env_data['mp5_daily_hours_linear'][base]) if base < len(env_data['mp5_daily_hours_linear']) else 0
            av[i].setVariableUInt("daily_today_u32", dt)
            av[i].setVariableUInt("daily_next_u32", 0)
        sim2.setPopulationData(av)
        before = pyflamegpu.AgentVector(a_desc)
        sim2.getPopulationData(before)
        sne_b = sum(int(ag.getVariableUInt('sne')) for ag in before)
        ppr_b = sum(int(ag.getVariableUInt('ppr')) for ag in before)
        steps = max(1, int(a.status2_days))
        # Диагностика по дням (итог по dt на D0..Dsteps-1 для выбранных idx)
        per_day_totals: List[int] = []
        selected_idx = [int(before[i].getVariableUInt('idx')) for i in range(K)]
        for d in range(steps):
            tot = 0
            base = d * FRAMES
            for fi in selected_idx:
                pos = base + (fi if fi < FRAMES else 0)
                if 0 <= pos < len(env_data['mp5_daily_hours_linear']):
                    tot += int(env_data['mp5_daily_hours_linear'][pos])
            per_day_totals.append(tot)
        for _ in range(steps):
            # На каждом шаге перекладываем dt следующего дня в daily_today_u32
            pop = pyflamegpu.AgentVector(a_desc)
            sim2.getPopulationData(pop)
            for i, ag in enumerate(pop):
                fi = int(ag.getVariableUInt('idx'))
                base = _ * FRAMES + (fi if fi < FRAMES else 0)
                dt = int(env_data['mp5_daily_hours_linear'][base]) if base < len(env_data['mp5_daily_hours_linear']) else 0
                ag.setVariableUInt('daily_today_u32', dt)
            sim2.setPopulationData(pop)
            sim2.step()
        after = pyflamegpu.AgentVector(a_desc)
        sim2.getPopulationData(after)
        sne_a = sum(int(ag.getVariableUInt('sne')) for ag in after)
        ppr_a = sum(int(ag.getVariableUInt('ppr')) for ag in after)
        # Статусы (в этом smoke остаются 2)
        s2_after_cnt = sum(1 for ag in after if int(ag.getVariableUInt('status_id')) == 2)
        # Печать диапазона дат и статистики
        date_from = days_sorted[0] if len(days_sorted) > 0 else None
        date_to = days_sorted[steps-1] if len(days_sorted) > steps-1 else None
        print(f"status2_smoke_real: steps={steps}, dates=[{date_from}..{date_to}], agents_in_s2={K}")
        # per_day-* печать убрана как неинформативная
        print(f"  sne_inc={sne_a - sne_b}, ppr_inc={ppr_a - ppr_b}, s2_after={s2_after_cnt}")
        return

    # === Совместный слой 2/4/6 (REAL) ===
    if a.status246_smoke_real or getattr(a, 'status246q_smoke_real', False):
        FRAMES = int(env_data['frames_total_u16'])
        DAYS = int(env_data['days_total_u16'])
        os.environ['HL_STATUS246_SMOKE'] = '1'
        model2, a_desc = build_model_for_quota_smoke(FRAMES, DAYS)
        sim2 = pyflamegpu.CUDASimulation(model2)
        # Таймеры стадий
        t_load_s = 0.0
        t_gpu_s = 0.0
        t_cpu_s = 0.0
        import time as _t
        t0 = _t.perf_counter()
        sim2.setEnvironmentPropertyUInt("version_date", int(env_data['version_date_u16']))
        sim2.setEnvironmentPropertyUInt("frames_total", FRAMES)
        sim2.setEnvironmentPropertyUInt("days_total", DAYS)
        # MP4 квоты для менеджера intent→approve→apply
        sim2.setEnvironmentPropertyArrayUInt32("mp4_ops_counter_mi8", list(env_data['mp4_ops_counter_mi8']))
        sim2.setEnvironmentPropertyArrayUInt32("mp4_ops_counter_mi17", list(env_data['mp4_ops_counter_mi17']))
        idx_map = {name: i for i, name in enumerate(mp3_fields)}
        frames_index = env_data.get('frames_index', {})
        # Берём всех агентов с статусами 2,4,6
        rows = [r for r in mp3_rows if int(r[idx_map['status_id']] or 0) in (2,4,6)]
        # Диагностика BR (минуты) по реальным данным: отдельно MI-8 и MI-17
        br8_vals: List[int] = []
        br17_vals: List[int] = []
        for r in rows:
            partseq = int(r[idx_map.get('partseqno_i', -1)] or 0)
            mask = int(r[idx_map.get('ac_type_mask', -1)] or 0)
            br_val = 0
            if mask & 32:
                br_val = int(mp1_map.get(partseq, (0,0,0,0,0))[0])
                br8_vals.append(br_val)
            elif mask & 64:
                br_val = int(mp1_map.get(partseq, (0,0,0,0,0))[1])
                br17_vals.append(br_val)
        # Примеры пар (aircraft_number, partseqno_i, br)
        samples8: List[Tuple[int,int,int]] = []
        samples17: List[Tuple[int,int,int]] = []
        for r in rows:
            partseq = int(r[idx_map.get('partseqno_i', -1)] or 0)
            acn = int(r[idx_map.get('aircraft_number', -1)] or 0)
            mask = int(r[idx_map.get('ac_type_mask', -1)] or 0)
            if mask & 32 and len(samples8) < 5:
                br_val = int(mp1_map.get(partseq, (0,0,0,0,0))[0])
                samples8.append((acn, partseq, br_val))
            elif mask & 64 and len(samples17) < 5:
                br_val = int(mp1_map.get(partseq, (0,0,0,0,0))[1])
                samples17.append((acn, partseq, br_val))
        if br8_vals:
            print(f"BR[MI-8] minutes: count={len(br8_vals)}, min={min(br8_vals)}, max={max(br8_vals)}")
        else:
            print("BR[MI-8] minutes: count=0")
        if br17_vals:
            print(f"BR[MI-17] minutes: count={len(br17_vals)}, min={min(br17_vals)}, max={max(br17_vals)}")
        else:
            print("BR[MI-17] minutes: count=0")
        # Sample pairs скрыты для компактности вывода
        K = len(rows)
        av = pyflamegpu.AgentVector(a_desc, K)
        for i, r in enumerate(rows):
            sid = int(r[idx_map['status_id']] or 0)
            ac = int(r[idx_map['aircraft_number']] or 0)
            fi = int(frames_index.get(ac, i % max(1, FRAMES)))
            av[i].setVariableUInt("idx", fi)
            # Корректный group_by: сначала из MP3, иначе по ac_type_mask
            gb = int(r[idx_map.get('group_by', -1)] or 0) if 'group_by' in idx_map else 0
            if gb not in (1, 2):
                mask = int(r[idx_map.get('ac_type_mask', -1)] or 0)
                gb = 1 if (mask & 32) else (2 if (mask & 64) else 0)
            av[i].setVariableUInt("group_by", gb if gb in (1, 2) else 1)
            av[i].setVariableUInt("status_id", sid)
            av[i].setVariableUInt("aircraft_number", ac)
            av[i].setVariableUInt("repair_days", int(r[idx_map.get('repair_days', -1)] or 0))
            partseq = int(r[idx_map.get('partseqno_i', -1)] or 0)
            # repair_time / partout_time / assembly_time для всех статусов (нужно для последующих 2->4)
            _tup = mp1_map.get(partseq, (0,0,0,0,0))
            av[i].setVariableUInt("repair_time", int(_tup[2] or 0))
            av[i].setVariableUInt("partout_time", int(_tup[3] or 0))
            av[i].setVariableUInt("assembly_time", int(_tup[4] or 0))
            av[i].setVariableUInt("sne", int(r[idx_map.get('sne', -1)] or 0))
            av[i].setVariableUInt("ppr", int(r[idx_map.get('ppr', -1)] or 0))
            # ll берём из MP3 (как есть)
            av[i].setVariableUInt("ll", int(r[idx_map.get('ll', -1)] or 0))
            # oh берём из MP1 по типу ВС (минуты)
            mask = int(r[idx_map.get('ac_type_mask', -1)] or 0)
            # Для group_by 1/2 (МИ-8/МИ-17) определяем индекс MP1 для partseq
            mp1_idx_map = env_data.get('mp1_index', {})
            pidx = int(mp1_idx_map.get(partseq, -1))
            oh_val = 0
            if pidx >= 0:
                if mask & 32:
                    oh_arr = env_data.get('mp1_oh_mi8', [])
                    if pidx < len(oh_arr):
                        oh_val = int(oh_arr[pidx] or 0)
                elif mask & 64:
                    oh_arr = env_data.get('mp1_oh_mi17', [])
                    if pidx < len(oh_arr):
                        oh_val = int(oh_arr[pidx] or 0)
            av[i].setVariableUInt("oh", oh_val)
            # br по типу планера: br_mi8/br_mi17
            br = 0
            if mask & 32:
                br = int(mp1_map.get(partseq, (0,0,0,0,0))[0])
            elif mask & 64:
                br = int(mp1_map.get(partseq, (0,0,0,0,0))[1])
            av[i].setVariableUInt("br", br)
            # dt на D0
            base = 0 * FRAMES + (fi if fi < FRAMES else 0)
            dt = int(env_data['mp5_daily_hours_linear'][base]) if base < len(env_data['mp5_daily_hours_linear']) else 0
            av[i].setVariableUInt("daily_today_u32", dt)
            av[i].setVariableUInt("daily_next_u32", 0)
        sim2.setPopulationData(av)
        t_load_s += (_t.perf_counter() - t0)
        before = pyflamegpu.AgentVector(a_desc)
        t1 = _t.perf_counter()
        sim2.getPopulationData(before)
        t_cpu_s += (_t.perf_counter() - t1)
        cnt2_b = sum(1 for ag in before if int(ag.getVariableUInt('status_id')) == 2)
        cnt3_b = sum(1 for ag in before if int(ag.getVariableUInt('status_id')) == 3)
        cnt4_b = sum(1 for ag in before if int(ag.getVariableUInt('status_id')) == 4)
        cnt6_b = sum(1 for ag in before if int(ag.getVariableUInt('status_id')) == 6)
        # Поддержка алиаса --status246q-* с отдельным параметром дней
        steps_cfg = int(a.status246_days)
        if getattr(a, 'status246q_smoke_real', False) and getattr(a, 'status246q_days', None):
            try:
                steps_cfg = int(a.status246q_days)
            except Exception:
                steps_cfg = int(a.status246_days)
        steps = max(1, steps_cfg)
        # Посуточные метрики
        per_day_dt_totals: List[int] = []
        per_day_trans_24: List[int] = []
        per_day_trans_45: List[int] = []
        per_day_trans_26: List[int] = []
        per_day_trans_32: List[int] = []
        per_day_trans_23: List[int] = []
        per_day_sne_from_s2: List[int] = []
        per_day_ppr_from_s2: List[int] = []
        per_day_ppr_reset_45: List[int] = []
        # Зафиксируем базовый набор idx из статуса 2 на D0 для dt сумм
        selected_idx: List[int] = []
        for i in range(K):
            if int(before[i].getVariableUInt('status_id')) == 2:
                selected_idx.append(int(before[i].getVariableUInt('idx')))
        # Логи переходов по дням с датами и aircraft_number
        trans24_log: List[Tuple[str,int]] = []
        trans26_log: List[Tuple[str,int]] = []
        trans32_log: List[Tuple[str,int]] = []
        trans52_log: List[Tuple[str,int]] = []
        trans42_log: List[Tuple[str,int]] = []
        trans45_log: List[Tuple[str,int]] = []
        # Расширенный лог: day, ac, sne, ppr, ll, oh, br на момент выхода (после dt)
        trans24_info: List[Tuple[str,int,int,int,int,int,int]] = []
        trans26_info: List[Tuple[str,int,int,int,int,int,int]] = []
        trans32_info: List[Tuple[str,int,int,int,int,int,int]] = []
        trans52_info: List[Tuple[str,int,int,int,int,int,int]] = []
        trans42_info: List[Tuple[str,int,int,int,int,int,int]] = []
        trans45_info: List[Tuple[str,int,int,int,int,int,int]] = []
        total_5to2 = 0
        # Для вычисления day_abs и плановых дат триггеров
        vd_u32 = int(env_data['version_date_u16'])
        for d in range(steps):
            # Снимем состояние ДО шага
            pop_before = pyflamegpu.AgentVector(a_desc)
            t_bcpu0 = _t.perf_counter()
            sim2.getPopulationData(pop_before)
            status_before = [int(pop_before[i].getVariableUInt('status_id')) for i in range(K)]
            sne_before = [int(pop_before[i].getVariableUInt('sne')) for i in range(K)]
            ppr_before = [int(pop_before[i].getVariableUInt('ppr')) for i in range(K)]
            ops_before = [int(pop_before[i].getVariableUInt('ops_ticket')) for i in range(K)]
            ac_before = [int(pop_before[i].getVariableUInt('aircraft_number')) for i in range(K)]
            # Подготовка dt/dn только для агентов в статусе 2 на текущем d
            for i in range(K):
                if status_before[i] == 2:
                    ag = pop_before[i]
                    fi = int(ag.getVariableUInt('idx'))
                    base = d * FRAMES + (fi if fi < FRAMES else 0)
                    dt = int(env_data['mp5_daily_hours_linear'][base]) if base < len(env_data['mp5_daily_hours_linear']) else 0
                    dn = int(env_data['mp5_daily_hours_linear'][base + FRAMES]) if (base + FRAMES) < len(env_data['mp5_daily_hours_linear']) else 0
                    ag.setVariableUInt('daily_today_u32', dt)
                    ag.setVariableUInt('daily_next_u32', dn)
            # Посуточная сумма dt по зафиксированным idx
            tot_dt = 0
            base_day = d * FRAMES
            for fi in selected_idx:
                pos = base_day + (fi if fi < FRAMES else 0)
                if 0 <= pos < len(env_data['mp5_daily_hours_linear']):
                    tot_dt += int(env_data['mp5_daily_hours_linear'][pos])
            per_day_dt_totals.append(tot_dt)
            # Шаг симуляции
            sim2.setPopulationData(pop_before)
            t_cpu_s += (_t.perf_counter() - t_bcpu0)
            t_g0 = _t.perf_counter()
            sim2.step()
            t_gpu_s += (_t.perf_counter() - t_g0)
            # Состояние ПОСЛЕ шага
            pop_after = pyflamegpu.AgentVector(a_desc)
            t_acpu0 = _t.perf_counter()
            sim2.getPopulationData(pop_after)
            status_after = [int(pop_after[i].getVariableUInt('status_id')) for i in range(K)]
            sne_after = [int(pop_after[i].getVariableUInt('sne')) for i in range(K)]
            ppr_after = [int(pop_after[i].getVariableUInt('ppr')) for i in range(K)]
            ops_after = [int(pop_after[i].getVariableUInt('ops_ticket')) for i in range(K)]
            t_cpu_s += (_t.perf_counter() - t_acpu0)
            # Транзакции статусов
            t_24 = 0
            t_45 = 0
            t_26 = 0
            t_23 = 0
            t_32 = 0
            t_52 = 0
            sne_s2 = 0
            ppr_s2 = 0
            ppr_reset_45 = 0
            approved8_today = 0
            approved17_today = 0
            for i in range(K):
                sb = status_before[i]
                sa = status_after[i]
                if sb == 2 and sa == 4:
                    t_24 += 1
                    day_str = env_data['days_sorted'][d] if d < len(env_data['days_sorted']) else str(d)
                    trans24_log.append((day_str, ac_before[i]))
                    # значения после шага (после начисления dt)
                    sne_v = int(pop_after[i].getVariableUInt('sne'))
                    ppr_v = int(pop_after[i].getVariableUInt('ppr'))
                    ll_v = int(pop_after[i].getVariableUInt('ll'))
                    oh_v = int(pop_after[i].getVariableUInt('oh'))
                    br_v = int(pop_after[i].getVariableUInt('br'))
                    trans24_info.append((day_str, ac_before[i], sne_v, ppr_v, ll_v, oh_v, br_v))
                    # Плановые даты триггеров (ord days): partout d==pt; assembly флаг за 30 до конца rt
                    day_abs = vd_u32 + (d + 1)
                    rt_v = int(pop_after[i].getVariableUInt('repair_time'))
                    pt_v = int(pop_after[i].getVariableUInt('partout_time'))
                    part_day = (day_abs + (pt_v - 1)) if pt_v > 0 else 0
                    asm_day = (day_abs + (rt_v - 30 - 1)) if rt_v > 30 else 0
                    print(f"    planned_triggers: ac={ac_before[i]}, part_day={part_day}, asm_flag_day={asm_day}")
                if sb == 2 and sa == 6:
                    t_26 += 1
                    day_str = env_data['days_sorted'][d] if d < len(env_data['days_sorted']) else str(d)
                    trans26_log.append((day_str, ac_before[i]))
                    sne_v = int(pop_after[i].getVariableUInt('sne'))
                    ppr_v = int(pop_after[i].getVariableUInt('ppr'))
                    ll_v = int(pop_after[i].getVariableUInt('ll'))
                    oh_v = int(pop_after[i].getVariableUInt('oh'))
                    br_v = int(pop_after[i].getVariableUInt('br'))
                    trans26_info.append((day_str, ac_before[i], sne_v, ppr_v, ll_v, oh_v, br_v))
                if sb == 3 and sa == 2:
                    t_32 += 1
                    day_str = env_data['days_sorted'][d] if d < len(env_data['days_sorted']) else str(d)
                    trans32_log.append((day_str, ac_before[i]))
                    sne_v = int(pop_after[i].getVariableUInt('sne'))
                    ppr_v = int(pop_after[i].getVariableUInt('ppr'))
                    ll_v = int(pop_after[i].getVariableUInt('ll'))
                    oh_v = int(pop_after[i].getVariableUInt('oh'))
                    br_v = int(pop_after[i].getVariableUInt('br'))
                    trans32_info.append((day_str, ac_before[i], sne_v, ppr_v, ll_v, oh_v, br_v))
                if sb == 5 and sa == 2:
                    t_52 += 1
                    day_str = env_data['days_sorted'][d] if d < len(env_data['days_sorted']) else str(d)
                    trans52_log.append((day_str, ac_before[i]))
                    sne_v = int(pop_after[i].getVariableUInt('sne'))
                    ppr_v = int(pop_after[i].getVariableUInt('ppr'))
                    ll_v = int(pop_after[i].getVariableUInt('ll'))
                    oh_v = int(pop_after[i].getVariableUInt('oh'))
                    br_v = int(pop_after[i].getVariableUInt('br'))
                    trans52_info.append((day_str, ac_before[i], sne_v, ppr_v, ll_v, oh_v, br_v))
                if sb == 4 and sa == 5:
                    t_45 += 1
                    ppr_reset_45 += (ppr_after[i] - ppr_before[i])
                    day_str = env_data['days_sorted'][d] if d < len(env_data['days_sorted']) else str(d)
                    trans45_log.append((day_str, ac_before[i]))
                    sne_v = int(pop_after[i].getVariableUInt('sne'))
                    ppr_v = int(pop_after[i].getVariableUInt('ppr'))
                    ll_v = int(pop_after[i].getVariableUInt('ll'))
                    oh_v = int(pop_after[i].getVariableUInt('oh'))
                    br_v = int(pop_after[i].getVariableUInt('br'))
                    trans45_info.append((day_str, ac_before[i], sne_v, ppr_v, ll_v, oh_v, br_v))
                if sb == 4 and sa == 2:
                    day_str = env_data['days_sorted'][d] if d < len(env_data['days_sorted']) else str(d)
                    trans42_log.append((day_str, ac_before[i]))
                    sne_v = int(pop_after[i].getVariableUInt('sne'))
                    ppr_v = int(pop_after[i].getVariableUInt('ppr'))
                    ll_v = int(pop_after[i].getVariableUInt('ll'))
                    oh_v = int(pop_after[i].getVariableUInt('oh'))
                    br_v = int(pop_after[i].getVariableUInt('br'))
                    trans42_info.append((day_str, ac_before[i], sne_v, ppr_v, ll_v, oh_v, br_v))
                if sb == 2:
                    sne_s2 += (sne_after[i] - sne_before[i])
                    ppr_s2 += (ppr_after[i] - ppr_before[i])
                if sb == 2 and sa == 3:
                    t_23 += 1
                # Дневная выдача билетов: ops_after==1 и intent_flag==1 в текущем дне
                if ops_after[i] == 1 and int(pop_after[i].getVariableUInt('intent_flag')) == 1:
                    gbv = int(pop_after[i].getVariableUInt('group_by'))
                    if gbv == 1:
                        approved8_today += 1
                    elif gbv == 2:
                        approved17_today += 1
            per_day_trans_24.append(t_24)
            per_day_trans_45.append(t_45)
            per_day_trans_26.append(t_26)
            per_day_trans_23.append(t_23)
            per_day_sne_from_s2.append(sne_s2)
            per_day_ppr_from_s2.append(ppr_s2)
            per_day_ppr_reset_45.append(ppr_reset_45)
            per_day_trans_32.append(t_32)
            total_5to2 += t_52
            # Диагностика квоты на D+1 (пер-дневный вывод отключён как неинформативный)
            d1 = d + 1
            seed8 = int(env_data['mp4_ops_counter_mi8'][d1]) if d1 < len(env_data['mp4_ops_counter_mi8']) else int(env_data['mp4_ops_counter_mi8'][-1])
            seed17 = int(env_data['mp4_ops_counter_mi17'][d1]) if d1 < len(env_data['mp4_ops_counter_mi17']) else int(env_data['mp4_ops_counter_mi17'][-1])
            left8 = max(0, seed8 - approved8_today)
            left17 = max(0, seed17 - approved17_today)
        after = pyflamegpu.AgentVector(a_desc)
        t_endcpu0 = _t.perf_counter()
        sim2.getPopulationData(after)
        t_cpu_s += (_t.perf_counter() - t_endcpu0)
        cnt2_a = sum(1 for ag in after if int(ag.getVariableUInt('status_id')) == 2)
        cnt3_a = sum(1 for ag in after if int(ag.getVariableUInt('status_id')) == 3)
        cnt4_a = sum(1 for ag in after if int(ag.getVariableUInt('status_id')) == 4)
        cnt5_a = sum(1 for ag in after if int(ag.getVariableUInt('status_id')) == 5)
        cnt6_a = sum(1 for ag in after if int(ag.getVariableUInt('status_id')) == 6)
        sne_inc = sum(int(ag.getVariableUInt('sne')) for ag in after) - sum(int(ag.getVariableUInt('sne')) for ag in before)
        ppr_inc = sum(int(ag.getVariableUInt('ppr')) for ag in after) - sum(int(ag.getVariableUInt('ppr')) for ag in before)
        print(f"status246_smoke_real: steps={steps}, cnt2 {cnt2_b}->{cnt2_a}, cnt3 {cnt3_b}->{cnt3_a}, cnt4 {cnt4_b}->{cnt4_a}, cnt5={cnt5_a}, cnt6 {cnt6_b}->{cnt6_a}, sne_inc={sne_inc}, ppr_inc={ppr_inc}")
        # Диагностика по суткам
        # Уникализация логов 3→2 на случай двойной фиксации одного события
        if trans32_log:
            _seen = set()
            _uniq_log = []
            for itm in trans32_log:
                if itm not in _seen:
                    _seen.add(itm)
                    _uniq_log.append(itm)
            trans32_log = _uniq_log
        if trans32_info:
            _seen_i = set()
            _uniq_info = []
            for itm in trans32_info:
                if itm not in _seen_i:
                    _seen_i.add(itm)
                    _uniq_info.append(itm)
            trans32_info = _uniq_info
        if trans52_log:
            _seen5 = set()
            _uniq5 = []
            for itm in trans52_log:
                if itm not in _seen5:
                    _seen5.add(itm)
                    _uniq5.append(itm)
            trans52_log = _uniq5
        if trans52_info:
            _seen5i = set()
            _uniq5i = []
            for itm in trans52_info:
                if itm not in _seen5i:
                    _seen5i.add(itm)
                    _uniq5i.append(itm)
            trans52_info = _uniq5i
        # per_day-* печать убрана как неинформативная
        # Полные логи переходов 2->4 и 2->6 с датами и AC
        if trans24_log:
            print("  transitions_2to4:")
            for dstr, acn in trans24_log:
                print(f"    {dstr}: ac={acn}")
        if trans26_log:
            print("  transitions_2to6:")
            for dstr, acn in trans26_log:
                print(f"    {dstr}: ac={acn}")
        if trans32_log:
            print("  transitions_3to2:")
            for dstr, acn in trans32_log:
                print(f"    {dstr}: ac={acn}")
        if trans52_log:
            print("  transitions_5to2:")
            for dstr, acn in trans52_log:
                print(f"    {dstr}: ac={acn}")
        if trans42_log:
            print("  transitions_4to2:")
            for dstr, acn in trans42_log:
                print(f"    {dstr}: ac={acn}")
        if trans45_log:
            print("  transitions_4to5:")
            for dstr, acn in trans45_log:
                print(f"    {dstr}: ac={acn}")
        # Детальные значения на момент выхода
        if trans24_info:
            print("  details_2to4 (day, ac, sne, ppr, ll, oh, br):")
            for dstr, acn, sne_v, ppr_v, ll_v, oh_v, br_v in trans24_info:
                print(f"    {dstr}: ac={acn}, sne={sne_v}, ppr={ppr_v}, ll={ll_v}, oh={oh_v}, br={br_v}")
        if trans26_info:
            print("  details_2to6 (day, ac, sne, ppr, ll, oh, br):")
            for dstr, acn, sne_v, ppr_v, ll_v, oh_v, br_v in trans26_info:
                print(f"    {dstr}: ac={acn}, sne={sne_v}, ppr={ppr_v}, ll={ll_v}, oh={oh_v}, br={br_v}")
        if trans32_info:
            print("  details_3to2 (day, ac, sne, ppr, ll, oh, br):")
            for dstr, acn, sne_v, ppr_v, ll_v, oh_v, br_v in trans32_info:
                print(f"    {dstr}: ac={acn}, sne={sne_v}, ppr={ppr_v}, ll={ll_v}, oh={oh_v}, br={br_v}")
        if trans52_info:
            print("  details_5to2 (day, ac, sne, ppr, ll, oh, br):")
            for dstr, acn, sne_v, ppr_v, ll_v, oh_v, br_v in trans52_info:
                print(f"    {dstr}: ac={acn}, sne={sne_v}, ppr={ppr_v}, ll={ll_v}, oh={oh_v}, br={br_v}")
        if trans42_info:
            print("  details_4to2 (day, ac, sne, ppr, ll, oh, br):")
            for dstr, acn, sne_v, ppr_v, ll_v, oh_v, br_v in trans42_info:
                print(f"    {dstr}: ac={acn}, sne={sne_v}, ppr={ppr_v}, ll={ll_v}, oh={oh_v}, br={br_v}")
        if trans45_info:
            print("  details_4to5 (day, ac, sne, ppr, ll, oh, br):")
            for dstr, acn, sne_v, ppr_v, ll_v, oh_v, br_v in trans45_info:
                print(f"    {dstr}: ac={acn}, sne={sne_v}, ppr={ppr_v}, ll={ll_v}, oh={oh_v}, br={br_v}")
        # Итоги только по статусу 2
        sne_inc_s2_total = sum(per_day_sne_from_s2)
        ppr_inc_s2_total = sum(per_day_ppr_from_s2)
        print(f"  totals_s2_only: sne_inc_s2={sne_inc_s2_total}, ppr_inc_s2={ppr_inc_s2_total}")
        # Итоги по переходам
        total_2to3 = sum(per_day_trans_23)
        total_3to2 = sum(per_day_trans_32)
        total_2to4 = len(trans24_log)
        total_2to6 = len(trans26_log)
        total_4to2 = len(trans42_log)
        total_4to5 = len(trans45_log)
        total_5to2_all = len(trans52_log)
        print(
            "  totals_transitions: "
            f"2to4={total_2to4}, 2to6={total_2to6}, 2to3={total_2to3}, 3to2={total_3to2}, "
            f"4to2={total_4to2}, 4to5={total_4to5}, 5to2={total_5to2_all}"
        )
        # Сводка таймингов
        print(f"timing_ms: load_gpu={t_load_s*1000:.2f}, sim_gpu={t_gpu_s*1000:.2f}, cpu_log={t_cpu_s*1000:.2f}")
        return

    # === Совместный слой 1/2/4/5/6 (REAL) ===
    if getattr(a, 'status12456_smoke_real', False):
        FRAMES = int(env_data['frames_total_u16'])
        # Уважать ограничение по дням из CLI, но не превышать доступный горизонт
        DAYS = min(int(getattr(a, 'status12456_days', env_data['days_total_u16'])), int(env_data['days_total_u16']))
        os.environ['HL_STATUS246_SMOKE'] = '1'
        # Включаем MP2-лог, но постпроцессинг оставляем по внешнему флагу (не форсируем ON)
        # os.environ['HL_ENABLE_MP2'] = '1'  # Отключено для больших периодов из-за лимитов MacroProperty
        model2, a_desc = build_model_for_quota_smoke(FRAMES, DAYS)
        sim2 = pyflamegpu.CUDASimulation(model2)
        # Таймеры
        t_load_s = t_gpu_s = t_cpu_s = t_db_s = 0.0
        import time as _t
        t0 = _t.perf_counter()
        sim2.setEnvironmentPropertyUInt("version_date", int(env_data['version_date_u16']))
        sim2.setEnvironmentPropertyUInt("frames_total", FRAMES)
        sim2.setEnvironmentPropertyUInt("days_total", DAYS)
        # MP4 квоты (усечённые по DAYS)
        sim2.setEnvironmentPropertyArrayUInt32("mp4_ops_counter_mi8", list(env_data['mp4_ops_counter_mi8'])[:DAYS])
        sim2.setEnvironmentPropertyArrayUInt32("mp4_ops_counter_mi17", list(env_data['mp4_ops_counter_mi17'])[:DAYS])
        # MP5 теперь инициализируется через HostFunction в MacroProperty mp5_lin
        # if os.environ.get("HL_MP5_PROBE", "0") == "1":
        #     mp5_size = (DAYS + 1) * FRAMES
        #     sim2.setEnvironmentPropertyArrayUInt16("mp5_daily_hours", list(env_data['mp5_daily_hours_linear'])[:mp5_size])
        # Спавн: frames_initial и по-дневные массивы, а также базовые счётчики ACN/PSN
        try:
            # Спавн должен занимать сначала зарезервированные MP5-слоты (279..285),
            # поэтому задаём frames_initial как индекс первого зарезервированного слота
            frames_initial = int(env_data.get('first_reserved_idx', env_data.get('frames_union_no_future', env_data.get('mp3_count', 0))))
            sim2.setEnvironmentPropertyUInt("frames_initial", frames_initial)
        except Exception:
            pass
        if 'mp4_new_counter_mi17_seed' in env_data:
            sim2.setEnvironmentPropertyArrayUInt32("mp4_new_counter_mi17_seed", list(env_data['mp4_new_counter_mi17_seed'])[:DAYS])
        if 'month_first_u32' in env_data:
            sim2.setEnvironmentPropertyArrayUInt32("month_first_u32", list(env_data['month_first_u32'])[:DAYS])
        # Инициализация базовых значений для spawn_mgr (next_acn/next_psn через агентные переменные)
        try:
            spawn_mgr_desc = model2.getAgent("spawn_mgr")
            if spawn_mgr_desc is not None:
                sm = pyflamegpu.AgentVector(spawn_mgr_desc, 1)
                # next_idx стартует с позиции первого будущего борта:
                # если base_acn_spawn уже присутствует в union (зарезервирован MP5), используем его индекс;
                # иначе — frames_union_no_future
                # next_idx начинаем с первого зарезервированного индекса, чтобы заполнить 279..285
                next_idx = int(env_data.get('first_reserved_idx', env_data.get('first_future_idx', env_data.get('frames_union_no_future', env_data.get('mp3_count', 0)))))
                base_acn = int(env_data.get('base_acn_spawn', 100000))
                base_psn = int(os.environ.get('HL_BASE_PSN_SPAWN', '2000000'))
                sm[0].setVariableUInt('next_idx', next_idx)
                sm[0].setVariableUInt('next_acn', base_acn)
                sm[0].setVariableUInt('next_psn', base_psn)
                sim2.setPopulationData(sm)
        except Exception:
            pass
        # ENV-константы нормативов для новорождённых MI-17 (используются в rtc_spawn_mi17_atomic)
        try:
            _mi17_tuple = mp1_map.get(70482, (0,0,0,0,0))
            sim2.setEnvironmentPropertyUInt("mi17_repair_time_const", int(_mi17_tuple[2] or 0))
            sim2.setEnvironmentPropertyUInt("mi17_partout_time_const", int(_mi17_tuple[3] or 0))
            sim2.setEnvironmentPropertyUInt("mi17_assembly_time_const", int(_mi17_tuple[4] or 0))
            # BR/LL/OH для MI-17 из MP1
            _br17 = int(mp1_map.get(70482, (0,0,0,0,0))[1] or 0)
            _mp1_index = env_data.get('mp1_index', {})
            _pidx = int(_mp1_index.get(70482, -1))
            _ll17 = 0
            _oh17 = 0
            arr_ll17 = env_data.get('mp1_ll_mi17', [])
            arr_oh17 = env_data.get('mp1_oh_mi17', [])
            if _pidx >= 0 and _pidx < len(arr_ll17):
                _ll17 = int(arr_ll17[_pidx] or 0)
            if _pidx >= 0 and _pidx < len(arr_oh17):
                _oh17 = int(arr_oh17[_pidx] or 0)
            sim2.setEnvironmentPropertyUInt("mi17_br_const", int(_br17))
            sim2.setEnvironmentPropertyUInt("mi17_ll_const", int(_ll17))
            sim2.setEnvironmentPropertyUInt("mi17_oh_const", int(_oh17))
            try:
                print(
                    f"spawn ENV consts (MI-17): partseq=70482, rt={int(_mi17_tuple[2] or 0)}, pt={int(_mi17_tuple[3] or 0)}, at={int(_mi17_tuple[4] or 0)}, "
                    f"br17={_br17}, ll17={_ll17}, oh17={_oh17}"
                )
            except Exception:
                pass
        except Exception as _e:
            print(f"⚠️ MI-17 env consts set skipped (status12456_smoke_real): {_e}")
        idx_map = {name: i for i, name in enumerate(mp3_fields)}
        frames_index = env_data.get('frames_index', {})
        # Берём статусы 1,2,4,5,6
        rows = [r for r in mp3_rows if int(r[idx_map['status_id']] or 0) in (1,2,4,5,6)]
        # BR статистика (как выше)
        br8_vals: List[int] = []
        br17_vals: List[int] = []
        for r in rows:
            partseq = int(r[idx_map.get('partseqno_i', -1)] or 0)
            mask = int(r[idx_map.get('ac_type_mask', -1)] or 0)
            if mask & 32:
                br8_vals.append(int(mp1_map.get(partseq, (0,0,0,0,0))[0]))
            elif mask & 64:
                br17_vals.append(int(mp1_map.get(partseq, (0,0,0,0,0))[1]))
        if br8_vals:
            print(f"BR[MI-8] minutes: count={len(br8_vals)}, min={min(br8_vals)}, max={max(br8_vals)}")
        else:
            print("BR[MI-8] minutes: count=0")
        if br17_vals:
            print(f"BR[MI-17] minutes: count={len(br17_vals)}, min={min(br17_vals)}, max={max(br17_vals)}")
        else:
            print("BR[MI-17] minutes: count=0")
        K = len(rows)
        av = pyflamegpu.AgentVector(a_desc, K)
        # Подготовка массива mfg_date_days по кадрам (FRAMES), заполняем по индексу кадра fi
        mfg_by_frame = [0] * max(1, FRAMES)
        for i, r in enumerate(rows):
            ac = int(r[idx_map['aircraft_number']] or 0)
            fi = int(frames_index.get(ac, i % max(1, FRAMES)))
            # mfg_date -> ordinal days (уже подготовлено в env_data['mp3_arrays'] как mp3_mfg_date_days? при отсутствии — пересчёт)
            ord_val = 0
            try:
                from datetime import date as _date
                mfg_str = str(r[idx_map.get('mfg_date', -1)] or '')
                if mfg_str:
                    y, m, d = [int(x) for x in mfg_str.split('-')]
                    ord_val = _date(y, m, d).toordinal()
            except Exception:
                ord_val = 0
            if 0 <= fi < FRAMES:
                if ord_val > mfg_by_frame[fi]:
                    mfg_by_frame[fi] = ord_val
        sim2.setEnvironmentPropertyArrayUInt32("mp3_mfg_date_days", mfg_by_frame)
        # Инициализируем популяции спавна (если агенты присутствуют): тикеты и один менеджер
        try:
            K = int(os.environ.get('HL_SPAWN_MAX_PER_DAY', '64'))
            spawn_ticket_desc = model2.getAgent("spawn_ticket")
            if spawn_ticket_desc is not None:
                st = pyflamegpu.AgentVector(spawn_ticket_desc, max(1, K))
                for i in range(max(1, K)):
                    st[i].setVariableUInt("ticket", i)
                sim2.setPopulationData(st)
            spawn_mgr_desc = model2.getAgent("spawn_mgr")
            if spawn_mgr_desc is not None:
                sm = pyflamegpu.AgentVector(spawn_mgr_desc, 1)
                sim2.setPopulationData(sm)
        except Exception:
            pass
        for i, r in enumerate(rows):
            sid = int(r[idx_map['status_id']] or 0)
            ac = int(r[idx_map['aircraft_number']] or 0)
            fi = int(frames_index.get(ac, i % max(1, FRAMES)))
            av[i].setVariableUInt("idx", fi)
            # PSN из MP3 (если колонка присутствует)
            try:
                psn_init = int(r[idx_map.get('psn', -1)] or 0)
            except Exception:
                psn_init = 0
            av[i].setVariableUInt("psn", psn_init)
            gb = int(r[idx_map.get('group_by', -1)] or 0) if 'group_by' in idx_map else 0
            if gb not in (1,2):
                mask = int(r[idx_map.get('ac_type_mask', -1)] or 0)
                gb = 1 if (mask & 32) else (2 if (mask & 64) else 0)
            av[i].setVariableUInt("group_by", gb if gb in (1,2) else 1)
            av[i].setVariableUInt("status_id", sid)
            av[i].setVariableUInt("aircraft_number", ac)
            av[i].setVariableUInt("repair_days", int(r[idx_map.get('repair_days', -1)] or 0))
            partseq = int(r[idx_map.get('partseqno_i', -1)] or 0)
            # repair_time / partout_time / assembly_time из MP1 для всех (требуется для статуса 1 и 4)
            mp1_tuple = mp1_map.get(partseq, (0,0,0,0,0))
            rt = mp1_tuple[2]
            pt = mp1_tuple[3]
            at = mp1_tuple[4]
            av[i].setVariableUInt("repair_time", int(rt or 0))
            av[i].setVariableUInt("partout_time", int(pt or 0))
            av[i].setVariableUInt("assembly_time", int(at or 0))
            av[i].setVariableUInt("sne", int(r[idx_map.get('sne', -1)] or 0))
            av[i].setVariableUInt("ppr", int(r[idx_map.get('ppr', -1)] or 0))
            av[i].setVariableUInt("ll", int(r[idx_map.get('ll', -1)] or 0))
            # oh по типу из MP1
            mask = int(r[idx_map.get('ac_type_mask', -1)] or 0)
            mp1_idx_map = env_data.get('mp1_index', {})
            pidx = int(mp1_idx_map.get(partseq, -1))
            oh_val = 0
            if pidx >= 0:
                if mask & 32:
                    oh_arr = env_data.get('mp1_oh_mi8', [])
                    if pidx < len(oh_arr):
                        oh_val = int(oh_arr[pidx] or 0)
                elif mask & 64:
                    oh_arr = env_data.get('mp1_oh_mi17', [])
                    if pidx < len(oh_arr):
                        oh_val = int(oh_arr[pidx] or 0)
            av[i].setVariableUInt("oh", oh_val)
            # br по типу
            br = 0
            if mask & 32:
                br = int(mp1_map.get(partseq, (0,0,0,0,0))[0])
            elif mask & 64:
                br = int(mp1_map.get(partseq, (0,0,0,0,0))[1])
            av[i].setVariableUInt("br", br)
            # Дата производства: ord-дни UInt16
            # Источник: mp3_rows.mfg_date → ord; фолбэк 0
            try:
                from datetime import date as _date
                mfg_val = r[idx_map.get('mfg_date', -1)] if 'mfg_date' in idx_map else None
                ord_val = 0
                if mfg_val:
                    epoch = _date(1970, 1, 1)
                    ord_val = max(0, int((mfg_val - epoch).days))
            except Exception:
                ord_val = 0
            av[i].setVariableUInt("mfg_date", int(ord_val & 0xFFFF))
            # dt D0
            base = 0 * FRAMES + (fi if fi < FRAMES else 0)
            dt = int(env_data['mp5_daily_hours_linear'][base]) if base < len(env_data['mp5_daily_hours_linear']) else 0
            av[i].setVariableUInt("daily_today_u32", dt)
            av[i].setVariableUInt("daily_next_u32", 0)
        sim2.setPopulationData(av)
        t_load_s += (_t.perf_counter() - t0)
        
        # Проверка размеров для MacroProperty
        if FRAMES > MAX_FRAMES:
            print(f"ERROR: FRAMES={FRAMES} превышает MAX_FRAMES={MAX_FRAMES}")
            return
        if DAYS > MAX_DAYS:
            print(f"ERROR: DAYS={DAYS} превышает MAX_DAYS={MAX_DAYS}")
            return
        
        # Инициализация MP5 через HostFunction если включен MP5_PROBE
        if os.environ.get("HL_MP5_PROBE", "0") == "1":
            class HF_InitMP5(pyflamegpu.HostFunction):
                def __init__(self, data, frames, sim_days):
                    super().__init__()
                    self.data = data
                    self.frames = frames
                    self.sim_days = sim_days  # Количество дней симуляции
                
                def run(self, FLAMEGPU):
                    if FLAMEGPU.getStepCounter() > 0:
                        return
                    mp = FLAMEGPU.environment.getMacroPropertyUInt32("mp5_lin")
                    # Инициализируем весь буфер до MAX_DAYS
                    data_days = len(self.data) // self.frames - 1 if self.frames > 0 else 0
                    print(f"HF_InitMP5: Инициализация mp5_lin для FRAMES={self.frames}, SIM_DAYS={self.sim_days}, DATA_DAYS={data_days}")
                    
                    # Копируем реальные данные
                    for idx in range(len(self.data)):
                        d = idx // self.frames
                        f = idx % self.frames
                        dst_idx = d * MAX_FRAMES + f
                        mp[dst_idx] = int(self.data[idx])
                    
                    # Остальное уже заполнено нулями при создании MacroProperty
                    print(f"HF_InitMP5: Инициализировано {len(self.data)} элементов данных в буфере размером {MAX_SIZE}")
            
            # Подготовка данных MP5 - всегда загружаем полный объем до MAX_DAYS
            mp5_data = list(env_data['mp5_daily_hours_linear'])
            # Загружаем данные до фактического горизонта или MAX_DAYS (что меньше)
            data_days = min(int(env_data['days_total_u16']), MAX_DAYS)
            need = (data_days + 1) * FRAMES
            mp5_data = mp5_data[:need]
            # Дополняем нулями до MAX_DAYS если данных меньше
            if data_days < MAX_DAYS:
                mp5_data.extend([0] * ((MAX_DAYS - data_days) * FRAMES))
            
            # Создаем слой с HostFunction для инициализации MP5
            init_layer = model2.newLayer()
            init_layer.addHostFunction(HF_InitMP5(mp5_data, FRAMES, DAYS))
            
            # Инициализация MP5 будет выполнена на первом шаге симуляции
            print("MP5 будет инициализирован при первом шаге симуляции")
        
        # Логирование плана спавна: сумма по горизонту
        try:
            _seed_arr = list(env_data.get('mp4_new_counter_mi17_seed', []))[:DAYS]
            print(f"spawn plan total ({DAYS}d) = {sum(int(x) for x in _seed_arr)}")
        except Exception:
            pass
        before = pyflamegpu.AgentVector(a_desc)
        sim2.getPopulationData(before)
        cnt1_b = sum(1 for ag in before if int(ag.getVariableUInt('status_id')) == 1)
        cnt2_b = sum(1 for ag in before if int(ag.getVariableUInt('status_id')) == 2)
        cnt3_b = sum(1 for ag in before if int(ag.getVariableUInt('status_id')) == 3)
        cnt4_b = sum(1 for ag in before if int(ag.getVariableUInt('status_id')) == 4)
        cnt5_b = sum(1 for ag in before if int(ag.getVariableUInt('status_id')) == 5)
        cnt6_b = sum(1 for ag in before if int(ag.getVariableUInt('status_id')) == 6)
        steps = max(1, int(getattr(a, 'status12456_days', 7)))
        # Экспорт симуляции
        export_on = (getattr(a, 'export_sim', 'off') == 'on') if hasattr(a, 'export_sim') else False
        export_table = getattr(a, 'export_sim_table', 'sim_results') if hasattr(a, 'export_sim_table') else 'sim_results'
        export_batch = int(getattr(a, 'export_batch', 250000)) if hasattr(a, 'export_batch') else 250000
        export_truncate = bool(getattr(a, 'export_truncate', False)) if hasattr(a, 'export_truncate') else False
        export_triggers_only = bool(getattr(a, 'export_triggers_only', False)) if hasattr(a, 'export_triggers_only') else False
        export_buf: list = []
        export_has_psn = False
        if export_on:
            ensure_sim_results_table(client, export_table)
            # Определим, есть ли столбец psn в целевой таблице
            try:
                _cols = {row[0] for row in client.execute(f"DESCRIBE TABLE {export_table}")}
                export_has_psn = ('psn' in _cols)
            except Exception as _e:
                print(f"⚠️ DESCRIBE {export_table} не удалось: {_e}")
            if export_truncate:
                try:
                    client.execute(f"TRUNCATE TABLE {export_table}")
                except Exception as _e:
                    print(f"⚠️ TRUNCATE {export_table} пропущен: {_e}")
            # Экспортируем D0-снимок (до первого шага), если включено
            # Примечание: при export_postprocess=on D0 экспорт пропускается,
            # чтобы не дублировать день 0 (copyout покроет D0).
            if getattr(a, 'export_d0', 'off') == 'on' and getattr(a, 'export_postprocess', 'on') == 'off':
                should_export_d0 = True
                if not export_truncate:
                    try:
                        cnt = client.execute(
                            f"SELECT count() FROM {export_table} WHERE version_date = toUInt32({int(env_data['version_date_u16'])}) AND version_id = {int(vid)} AND day_u16 = 0 LIMIT 1"
                        )[0][0]
                        should_export_d0 = (cnt == 0)
                    except Exception as _e:
                        print(f"⚠️ Проверка наличия D0 пропущена: {_e}")
                if should_export_d0:
                    export_day_snapshot(
                        client, export_table, int(env_data['version_date_u16']), int(vid), -1,
                        rows, before, FRAMES, idx_map, mp1_map, export_buf,
                        triggers_only=export_triggers_only, include_psn=export_has_psn
                    )
                    if export_triggers_only:
                        cols = (
                            "version_date,version_id,version_date_date,day_u16,day_abs,day_date,idx,aircraft_number,"
                            + ("psn," if export_has_psn else "")
                            + "active_trigger,assembly_trigger,partout_trigger"
                        )
                        t_db_s += flush_export_buffer(client, export_table, export_buf, columns_override=cols)
                    else:
                        if export_has_psn:
                            cols = (
                                "version_date,version_id,version_date_date,day_u16,day_abs,day_date,idx,aircraft_number,psn,"
                                "mfg_date_date,partseqno_i,group_by,status_id,repair_days,repair_time,assembly_time,partout_time,"
                                "sne,ppr,ll,oh,br,daily_today_u32,daily_next_u32,ops_ticket,intent_flag,active_trigger,assembly_trigger,partout_trigger"
                            )
                            t_db_s += flush_export_buffer(client, export_table, export_buf, columns_override=cols)
                        else:
                            t_db_s += flush_export_buffer(client, export_table, export_buf)
        trans12_log: List[Tuple[str,int]] = []
        trans12_info: List[Tuple[str,int,int,int,int,int,int]] = []
        trans23_log: List[Tuple[str,int]] = []
        trans32_log: List[Tuple[str,int]] = []
        trans52_log: List[Tuple[str,int]] = []
        # Дополнительные переходы для полной сводки
        trans24_log: List[Tuple[str,int]] = []
        trans26_log: List[Tuple[str,int]] = []
        trans42_log: List[Tuple[str,int]] = []
        trans45_log: List[Tuple[str,int]] = []
        trans14_log: List[Tuple[str,int]] = []
        trans23_info: List[Tuple[str,int,int,int,int,int,int]] = []
        trans32_info: List[Tuple[str,int,int,int,int,int,int]] = []
        trans52_info: List[Tuple[str,int,int,int,int,int,int]] = []
        total_5to2 = 0
        prev_total = len(before)
        for d in range(steps):
            pop_before = pyflamegpu.AgentVector(a_desc)
            t_bcpu0 = _t.perf_counter()
            sim2.getPopulationData(pop_before)
            status_before = [int(pop_before[i].getVariableUInt('status_id')) for i in range(K)]
            t_cpu_s += (_t.perf_counter() - t_bcpu0)
            # MP5 теперь читается напрямую из MacroProperty в RTC функциях
            # Хост-инъекция больше не нужна
            # for i in range(K):
            #     if status_before[i] == 2:
            #         ag = pop_before[i]
            #         fi = int(ag.getVariableUInt('idx'))
            #         base = d * FRAMES + (fi if fi < FRAMES else 0)
            #         dt = int(env_data['mp5_daily_hours_linear'][base]) if base < len(env_data['mp5_daily_hours_linear']) else 0
            #         dn = int(env_data['mp5_daily_hours_linear'][base + FRAMES]) if (base + FRAMES) < len(env_data['mp5_daily_hours_linear']) else 0
            #         ag.setVariableUInt('daily_today_u32', dt)
            #         ag.setVariableUInt('daily_next_u32', dn)
            sim2.setPopulationData(pop_before)
            t_g0 = _t.perf_counter()
            sim2.step()
            t_gpu_s += (_t.perf_counter() - t_g0)
            pop_after = pyflamegpu.AgentVector(a_desc)
            t_acpu0 = _t.perf_counter()
            sim2.getPopulationData(pop_after)
            t_cpu_s += (_t.perf_counter() - t_acpu0)
            # Подсчёт переходов за текущий день (накапливаем суммарно)
            day_str = env_data['days_sorted'][d] if d < len(env_data['days_sorted']) else str(d)
            for i in range(K):
                sb = status_before[i]
                sa = int(pop_after[i].getVariableUInt('status_id'))
                if sb == 1 and sa == 2:
                    trans12_log.append((day_str, int(pop_after[i].getVariableUInt('aircraft_number'))))
                    sne_v = int(pop_after[i].getVariableUInt('sne'))
                    ppr_v = int(pop_after[i].getVariableUInt('ppr'))
                    ll_v = int(pop_after[i].getVariableUInt('ll'))
                    oh_v = int(pop_after[i].getVariableUInt('oh'))
                    br_v = int(pop_after[i].getVariableUInt('br'))
                    trans12_info.append((day_str, int(pop_after[i].getVariableUInt('aircraft_number')), sne_v, ppr_v, ll_v, oh_v, br_v))
                if sb == 1 and sa == 4:
                    trans14_log.append((day_str, int(pop_after[i].getVariableUInt('aircraft_number'))))
                if sb == 2 and sa == 3:
                    trans23_log.append((day_str, int(pop_after[i].getVariableUInt('aircraft_number'))))
                    sne_v = int(pop_after[i].getVariableUInt('sne'))
                    ppr_v = int(pop_after[i].getVariableUInt('ppr'))
                    ll_v = int(pop_after[i].getVariableUInt('ll'))
                    oh_v = int(pop_after[i].getVariableUInt('oh'))
                    br_v = int(pop_after[i].getVariableUInt('br'))
                    trans23_info.append((day_str, int(pop_after[i].getVariableUInt('aircraft_number')), sne_v, ppr_v, ll_v, oh_v, br_v))
                if sb == 2 and sa == 4:
                    trans24_log.append((day_str, int(pop_after[i].getVariableUInt('aircraft_number'))))
                if sb == 2 and sa == 6:
                    trans26_log.append((day_str, int(pop_after[i].getVariableUInt('aircraft_number'))))
                if sb == 3 and sa == 2:
                    trans32_log.append((day_str, int(pop_after[i].getVariableUInt('aircraft_number'))))
                    sne_v = int(pop_after[i].getVariableUInt('sne'))
                    ppr_v = int(pop_after[i].getVariableUInt('ppr'))
                    ll_v = int(pop_after[i].getVariableUInt('ll'))
                    oh_v = int(pop_after[i].getVariableUInt('oh'))
                    br_v = int(pop_after[i].getVariableUInt('br'))
                    trans32_info.append((day_str, int(pop_after[i].getVariableUInt('aircraft_number')), sne_v, ppr_v, ll_v, oh_v, br_v))
                if sb == 5 and sa == 2:
                    trans52_log.append((day_str, int(pop_after[i].getVariableUInt('aircraft_number'))))
                    sne_v = int(pop_after[i].getVariableUInt('sne'))
                    ppr_v = int(pop_after[i].getVariableUInt('ppr'))
                    ll_v = int(pop_after[i].getVariableUInt('ll'))
                    oh_v = int(pop_after[i].getVariableUInt('oh'))
                    br_v = int(pop_after[i].getVariableUInt('br'))
                    trans52_info.append((day_str, int(pop_after[i].getVariableUInt('aircraft_number')), sne_v, ppr_v, ll_v, oh_v, br_v))
                    total_5to2 += 1
                if sb == 4 and sa == 2:
                    trans42_log.append((day_str, int(pop_after[i].getVariableUInt('aircraft_number'))))
                if sb == 4 and sa == 5:
                    trans45_log.append((day_str, int(pop_after[i].getVariableUInt('aircraft_number'))))
            # Диагностика рождений по дням
            try:
                total = len(pop_after)
                born = max(0, total - prev_total)
                planned = int(env_data.get('mp4_new_counter_mi17_seed', [0]* (DAYS+1))[d] if d < DAYS else 0)
                print(f"D={d+1} spawn plan={planned} born={born} pop={total}")
                if born > 0:
                    sample = min(10, born)
                    for j in range(sample):
                        agn = pop_after[prev_total + j]
                        try:
                            _idx = int(agn.getVariableUInt('idx'))
                        except Exception:
                            _idx = -1
                        try:
                            _acn = int(agn.getVariableUInt('aircraft_number'))
                        except Exception:
                            _acn = 0
                        try:
                            _psn = int(agn.getVariableUInt('psn'))
                        except Exception:
                            _psn = 0
                        try:
                            _sid = int(agn.getVariableUInt('status_id'))
                        except Exception:
                            _sid = -1
                        try:
                            _gb = int(agn.getVariableUInt('group_by'))
                        except Exception:
                            _gb = 0
                        try:
                            _pseq = int(agn.getVariableUInt('partseqno_i'))
                        except Exception:
                            _pseq = 0
                        try:
                            _mfg = int(agn.getVariableUInt('mfg_date'))
                        except Exception:
                            _mfg = 0
                        try:
                            _rt = int(agn.getVariableUInt('repair_time'))
                        except Exception:
                            _rt = -1
                        try:
                            _at = int(agn.getVariableUInt('assembly_time'))
                        except Exception:
                            _at = -1
                        try:
                            _pt = int(agn.getVariableUInt('partout_time'))
                        except Exception:
                            _pt = -1
                        try:
                            _ll = int(agn.getVariableUInt('ll'))
                        except Exception:
                            _ll = -1
                        try:
                            _oh = int(agn.getVariableUInt('oh'))
                        except Exception:
                            _oh = -1
                        try:
                            _br = int(agn.getVariableUInt('br'))
                        except Exception:
                            _br = -1
                        print(f"  newborn[{j}]: idx={_idx} ac={_acn} psn={_psn} status_id={_sid} gb={_gb} partseqno_i={_pseq} mfg_ord={_mfg} rt={_rt} at={_at} pt={_pt} ll={_ll} oh={_oh} br={_br}")
                prev_total = total
            except Exception:
                pass
            # Экспорт после шага напрямую из agent vars (без MP2 copyout)
            if export_on and getattr(a, 'export_postprocess', 'on') == 'off':
                # Гейт pre-spawn: исключаем новорождённых дня d (они должны появиться с D+1)
                export_day_snapshot(
                    client, export_table, int(env_data['version_date_u16']), int(vid), d,
                    rows, pop_after, FRAMES, idx_map, mp1_map, export_buf,
                    triggers_only=export_triggers_only, max_agents=prev_total, include_psn=export_has_psn
                )
                if len(export_buf) >= export_batch:
                    if export_triggers_only:
                        cols = (
                            "version_date,version_id,version_date_date,day_u16,day_abs,day_date,idx,aircraft_number,"
                            + ("psn," if export_has_psn else "")
                            + "active_trigger,assembly_trigger,partout_trigger"
                        )
                        t_db_s += flush_export_buffer(client, export_table, export_buf, columns_override=cols)
                    else:
                        if export_has_psn:
                            cols = (
                                "version_date,version_id,version_date_date,day_u16,day_abs,day_date,idx,aircraft_number,psn,"
                                "mfg_date_date,partseqno_i,group_by,status_id,repair_days,repair_time,assembly_time,partout_time,"
                                "sne,ppr,ll,oh,br,daily_today_u32,daily_next_u32,ops_ticket,intent_flag,active_trigger,assembly_trigger,partout_trigger"
                            )
                            t_db_s += flush_export_buffer(client, export_table, export_buf, columns_override=cols)
                        else:
                            t_db_s += flush_export_buffer(client, export_table, export_buf)
        # Финальный сброс буфера для режима без постпроцессинга
        if export_on and getattr(a, 'export_postprocess', 'on') == 'off' and export_buf:
            if export_triggers_only:
                cols = (
                    "version_date,version_id,version_date_date,day_u16,day_abs,day_date,idx,aircraft_number,"
                    + ("psn," if export_has_psn else "")
                    + "active_trigger,assembly_trigger,partout_trigger"
                )
                t_db_s += flush_export_buffer(client, export_table, export_buf, columns_override=cols)
            else:
                if export_has_psn:
                    cols = (
                        "version_date,version_id,version_date_date,day_u16,day_abs,day_date,idx,aircraft_number,psn,"
                        "mfg_date_date,partseqno_i,group_by,status_id,repair_days,repair_time,assembly_time,partout_time,"
                        "sne,ppr,ll,oh,br,daily_today_u32,daily_next_u32,ops_ticket,intent_flag,active_trigger,assembly_trigger,partout_trigger"
                    )
                    t_db_s += flush_export_buffer(client, export_table, export_buf, columns_override=cols)
                else:
                    t_db_s += flush_export_buffer(client, export_table, export_buf)
        # Экспорт в режиме с постпроцессингом: используем те же собранные после шага данные
        if export_on and getattr(a, 'export_postprocess', 'on') == 'on':
            if export_buf:
                if export_triggers_only:
                    cols = "version_date,version_id,version_date_date,day_u16,day_abs,day_date,idx,aircraft_number,active_trigger,assembly_trigger,partout_trigger"
                    t_db_s += flush_export_buffer(client, export_table, export_buf, columns_override=cols)
                else:
                    t_db_s += flush_export_buffer(client, export_table, export_buf)
        after = pyflamegpu.AgentVector(a_desc)
        sim2.getPopulationData(after)
        cnt1_a = sum(1 for ag in after if int(ag.getVariableUInt('status_id')) == 1)
        cnt2_a = sum(1 for ag in after if int(ag.getVariableUInt('status_id')) == 2)
        cnt3_a = sum(1 for ag in after if int(ag.getVariableUInt('status_id')) == 3)
        cnt4_a = sum(1 for ag in after if int(ag.getVariableUInt('status_id')) == 4)
        cnt5_a = sum(1 for ag in after if int(ag.getVariableUInt('status_id')) == 5)
        cnt6_a = sum(1 for ag in after if int(ag.getVariableUInt('status_id')) == 6)
        print(f"status12456_smoke_real: steps={steps}, cnt1 {cnt1_b}->{cnt1_a}, cnt2 {cnt2_b}->{cnt2_a}, cnt3 {cnt3_b}->{cnt3_a}, cnt4 {cnt4_b}->{cnt4_a}, cnt5 {cnt5_b}->{cnt5_a}, cnt6 {cnt6_b}->{cnt6_a}")
        if trans12_log:
            print("  transitions_1to2:")
            for dstr, acn in trans12_log:
                print(f"    {dstr}: ac={acn}")
        if trans12_info:
            print("  details_1to2 (day, ac, sne, ppr, ll, oh, br):")
            for dstr, acn, sne_v, ppr_v, ll_v, oh_v, br_v in trans12_info:
                print(f"    {dstr}: ac={acn}, sne={sne_v}, ppr={ppr_v}, ll={ll_v}, oh={oh_v}, br={br_v}")
        if trans23_log:
            print("  transitions_2to3:")
            for dstr, acn in trans23_log:
                print(f"    {dstr}: ac={acn}")
        if trans23_info:
            print("  details_2to3 (day, ac, sne, ppr, ll, oh, br):")
            for dstr, acn, sne_v, ppr_v, ll_v, oh_v, br_v in trans23_info:
                print(f"    {dstr}: ac={acn}, sne={sne_v}, ppr={ppr_v}, ll={ll_v}, oh={oh_v}, br={br_v}")
        if trans32_log:
            print("  transitions_3to2:")
            for dstr, acn in trans32_log:
                print(f"    {dstr}: ac={acn}")
        if trans32_info:
            print("  details_3to2 (day, ac, sne, ppr, ll, oh, br):")
            for dstr, acn, sne_v, ppr_v, ll_v, oh_v, br_v in trans32_info:
                print(f"    {dstr}: ac={acn}, sne={sne_v}, ppr={ppr_v}, ll={ll_v}, oh={oh_v}, br={br_v}")
        if trans52_log:
            print("  transitions_5to2:")
            for dstr, acn in trans52_log:
                print(f"    {dstr}: ac={acn}")
        if trans52_info:
            print("  details_5to2 (day, ac, sne, ppr, ll, oh, br):")
            for dstr, acn, sne_v, ppr_v, ll_v, oh_v, br_v in trans52_info:
                print(f"    {dstr}: ac={acn}, sne={sne_v}, ppr={ppr_v}, ll={ll_v}, oh={oh_v}, br={br_v}")
        total_1to2 = len(trans12_log)
        total_1to4 = len(trans14_log)
        total_2to3 = len(trans23_log)
        total_3to2 = len(trans32_log)
        total_2to4 = len(trans24_log)
        total_2to6 = len(trans26_log)
        total_4to2 = len(trans42_log)
        total_4to5 = len(trans45_log)
        print(
            "  totals_transitions: "
            f"1to2={total_1to2}, 1to4={total_1to4}, 2to4={total_2to4}, 2to6={total_2to6}, "
            f"2to3={total_2to3}, 3to2={total_3to2}, 4to2={total_4to2}, 4to5={total_4to5}, 5to2={total_5to2}"
        )
        if export_on:
            print(f"timing_ms: load_gpu={t_load_s*1000:.2f}, sim_gpu={t_gpu_s*1000:.2f}, cpu_log={t_cpu_s*1000:.2f}, db_insert={t_db_s*1000:.2f}")
        else:
            print(f"timing_ms: load_gpu={t_load_s*1000:.2f}, sim_gpu={t_gpu_s*1000:.2f}, cpu_log={t_cpu_s*1000:.2f}")
        return

    # === Целевой кейс status_2 для одного aircraft_number: дневная траектория ===
    if int(getattr(a, 'status2_case_ac', 0)) > 0:
        FRAMES = int(env_data['frames_total_u16'])
        DAYS = int(env_data['days_total_u16'])
        os.environ['HL_STATUS246_SMOKE'] = '1'
        model2, a_desc = build_model_for_quota_smoke(FRAMES, DAYS)
        sim2 = pyflamegpu.CUDASimulation(model2)
        # Таймеры стадий
        t_load_s = 0.0
        t_gpu_s = 0.0
        t_cpu_s = 0.0
        import time as _t
        t0 = _t.perf_counter()
        sim2.setEnvironmentPropertyUInt("version_date", int(env_data['version_date_u16']))
        sim2.setEnvironmentPropertyUInt("frames_total", FRAMES)
        sim2.setEnvironmentPropertyUInt("days_total", DAYS)
        idx_map = {name: i for i, name in enumerate(mp3_fields)}
        frames_index = env_data.get('frames_index', {})
        ac_target = int(a.status2_case_ac)
        rows = [r for r in mp3_rows if int(r[idx_map['aircraft_number']] or 0) == ac_target and int(r[idx_map['status_id']] or 0) == 2]
        if not rows:
            print(f"status2_case: aircraft_number={ac_target} не найден(ы) в status_id=2")
            return
        K = len(rows)
        av = pyflamegpu.AgentVector(a_desc, K)
        for i, r in enumerate(rows):
            sid = int(r[idx_map['status_id']] or 0)
            ac = int(r[idx_map['aircraft_number']] or 0)
            fi = int(frames_index.get(ac, i % max(1, FRAMES)))
            av[i].setVariableUInt("idx", fi)
            av[i].setVariableUInt("group_by", 1)
            av[i].setVariableUInt("status_id", sid)
            av[i].setVariableUInt("repair_days", int(r[idx_map.get('repair_days', -1)] or 0))
            partseq = int(r[idx_map.get('partseqno_i', -1)] or 0)
            rt = mp1_map.get(partseq, (0,0,0,0,0))[2]
            av[i].setVariableUInt("repair_time", int(rt))
            av[i].setVariableUInt("sne", int(r[idx_map.get('sne', -1)] or 0))
            av[i].setVariableUInt("ppr", int(r[idx_map.get('ppr', -1)] or 0))
            av[i].setVariableUInt("ll", int(r[idx_map.get('ll', -1)] or 0))
            av[i].setVariableUInt("oh", int(r[idx_map.get('oh', -1)] or 0))
            # br по типу планера
            mask = int(r[idx_map.get('ac_type_mask', -1)] or 0)
            br = 0
            if mask & 32:
                br = int(mp1_map.get(partseq, (0,0,0,0,0))[0])
            elif mask & 64:
                br = int(mp1_map.get(partseq, (0,0,0,0,0))[1])
            av[i].setVariableUInt("br", br)
            # D0 dt/dn
            base = 0 * FRAMES + (fi if fi < FRAMES else 0)
            dt0 = int(env_data['mp5_daily_hours_linear'][base]) if base < len(env_data['mp5_daily_hours_linear']) else 0
            dn0 = int(env_data['mp5_daily_hours_linear'][base + FRAMES]) if (base + FRAMES) < len(env_data['mp5_daily_hours_linear']) else 0
            av[i].setVariableUInt("daily_today_u32", dt0)
            av[i].setVariableUInt("daily_next_u32", dn0)
        sim2.setPopulationData(av)
        t_load_s += (_t.perf_counter() - t0)
        steps = max(1, int(a.status2_case_days))
        print(f"status2_case: aircraft_number={ac_target}, agents={K}, days={steps}")
        for d in range(steps):
            pop = pyflamegpu.AgentVector(a_desc)
            t_cpu_pre = _t.perf_counter()
            sim2.getPopulationData(pop)
            # Печатаем состояние до шага
            for i in range(min(K, 5)):
                fi = int(pop[i].getVariableUInt('idx'))
                base = d * FRAMES + (fi if fi < FRAMES else 0)
                dt = int(env_data['mp5_daily_hours_linear'][base]) if base < len(env_data['mp5_daily_hours_linear']) else 0
                dn = int(env_data['mp5_daily_hours_linear'][base + FRAMES]) if (base + FRAMES) < len(env_data['mp5_daily_hours_linear']) else 0
                sid = int(pop[i].getVariableUInt('status_id'))
                sne = int(pop[i].getVariableUInt('sne'))
                ppr = int(pop[i].getVariableUInt('ppr'))
                llv = int(pop[i].getVariableUInt('ll'))
                ohv = int(pop[i].getVariableUInt('oh'))
                brv = int(pop[i].getVariableUInt('br'))
                print(f"D{d}: idx={fi}, sid={sid}, dt={dt}, dn={dn}, sne={sne}, ppr={ppr}, ll={llv}, oh={ohv}, br={brv}")
                pop[i].setVariableUInt('daily_today_u32', dt)
                pop[i].setVariableUInt('daily_next_u32', dn)
            sim2.setPopulationData(pop)
            t_cpu_s += (_t.perf_counter() - t_cpu_pre)
            t_g0 = _t.perf_counter()
            sim2.step()
            t_gpu_s += (_t.perf_counter() - t_g0)
        # Итог
        out = pyflamegpu.AgentVector(a_desc)
        t_cpu_post = _t.perf_counter()
        sim2.getPopulationData(out)
        t_cpu_s += (_t.perf_counter() - t_cpu_post)
        trans = [int(out[i].getVariableUInt('status_id')) for i in range(K)]
        print(f"status2_case_result: final_statuses={trans}")
        print(f"timing_ms: load_gpu={t_load_s*1000:.2f}, sim_gpu={t_gpu_s*1000:.2f}, cpu_log={t_cpu_s*1000:.2f}")
        return

    # Сборка модели (env-only): агентов не создаём
    model.build_model(num_agents=0, env_sizes={
        'days_total': int(env_data['days_total_u16']),
        'frames_total': int(env_data['frames_total_u16']),
        'mp1_len': len(env_data.get('mp1_br_mi8', [])),
        'mp3_count': int(env_data.get('mp3_count', 0)),
    })
    sim = model.build_simulation()
    # Ветвление режимов до применения Env
    if not a.gpu_quota_smoke and not a.mp5_probe:
        # Env-only: применяем Env и печатаем диагностические значения
        apply_env_to_sim(sim, env_data)
        vd = sim.getEnvironmentPropertyUInt("version_date")
        ft = sim.getEnvironmentPropertyUInt("frames_total")
        dtot = sim.getEnvironmentPropertyUInt("days_total")
        arr_mi8 = sim.getEnvironmentPropertyArrayUInt32("mp4_ops_counter_mi8")
        arr_mi17 = sim.getEnvironmentPropertyArrayUInt32("mp4_ops_counter_mi17")
        arr_mp5 = sim.getEnvironmentPropertyArrayUInt32("mp5_daily_hours")
        d0 = 0
        d1 = 1 if dtot > 1 else 0
        idx0 = 0
        base0 = d0 * ft + idx0
        base1 = d1 * ft + idx0
        print(f"EnvOnly sim_master: version_date={vd}, days_total={dtot}, frames_total={ft}, "
              f"mp4_mi8_D0={arr_mi8[d0]}, mp4_mi17_D1={arr_mi17[d1]}, "
              f"mp5_dt0={arr_mp5[base0]}, mp5_dn1={arr_mp5[base1]}")
        return

    # === GPU quota smoke: минимальный билдер модели через фабрику build_model_for_quota_smoke ===
    if a.gpu_quota_smoke or a.mp5_probe:
        FRAMES = int(env_data['frames_total_u16'])
        DAYS = int(env_data['days_total_u16'])
        mp4_ops8 = list(env_data['mp4_ops_counter_mi8'])
        mp4_ops17 = list(env_data['mp4_ops_counter_mi17'])
        if a.mp5_probe:
            os.environ['HL_MP5_PROBE'] = '1'
        model2, a_desc = build_model_for_quota_smoke(FRAMES, DAYS)
        sim2 = pyflamegpu.CUDASimulation(model2)
        sim2.setEnvironmentPropertyUInt("version_date", int(env_data['version_date_u16']))
        sim2.setEnvironmentPropertyUInt("frames_total", FRAMES)
        sim2.setEnvironmentPropertyUInt("days_total", DAYS)
        sim2.setEnvironmentPropertyUInt("approve_policy", 0)
        sim2.setEnvironmentPropertyArrayUInt32("mp4_ops_counter_mi8", mp4_ops8)
        sim2.setEnvironmentPropertyArrayUInt32("mp4_ops_counter_mi17", mp4_ops17)
        # Построим карту group_by по frames_index
        frame_gb = [0] * FRAMES
        ac_nums = env_data['mp3_arrays'].get('mp3_aircraft_number', [])
        gbs = env_data['mp3_arrays'].get('mp3_group_by', [1] * len(ac_nums))
        frames_index = env_data.get('frames_index', {})
        for i in range(min(len(ac_nums), len(gbs))):
            ac = int(ac_nums[i] or 0)
            gb = int(gbs[i] or 0)
            fi = int(frames_index.get(ac, -1)) if ac else -1
            if 0 <= fi < FRAMES and frame_gb[fi] == 0 and gb in (1, 2):
                frame_gb[fi] = gb
        frame_gb = [gb if gb in (1, 2) else 1 for gb in frame_gb]
        av2 = pyflamegpu.AgentVector(a_desc, FRAMES)
        for i in range(FRAMES):
            av2[i].setVariableUInt("idx", i)
            av2[i].setVariableUInt("group_by", int(frame_gb[i]))
            av2[i].setVariableUInt("ops_ticket", 0)
            # status4 smoke не активен здесь
        sim2.setPopulationData(av2)
        sim2.step()
        if a.mp5_probe:
            out_probe = pyflamegpu.AgentVector(a_desc)
            sim2.getPopulationData(out_probe)
            for i in range(min(3, FRAMES)):
                ag = out_probe[i]
                day = 0
                base = day * FRAMES + i
                dt_h = int(env_data['mp5_daily_hours_linear'][base]) if base < len(env_data['mp5_daily_hours_linear']) else 0
                dn_h = int(env_data['mp5_daily_hours_linear'][base + FRAMES]) if (base + FRAMES) < len(env_data['mp5_daily_hours_linear']) else 0
                dt_g = int(ag.getVariableUInt('daily_today_u32'))
                dn_g = int(ag.getVariableUInt('daily_next_u32'))
                print(f"MP5 probe idx={i}: host(dt,dn)=({dt_h},{dn_h}), gpu(dt,dn)=({dt_g},{dn_g})")
        else:
            out = pyflamegpu.AgentVector(a_desc)
            sim2.getPopulationData(out)
            claimed8 = claimed17 = 0
            for ag in out:
                if int(ag.getVariableUInt("ops_ticket")) == 1:
                    gb = int(ag.getVariableUInt("group_by"))
                    if gb == 1:
                        claimed8 += 1
                    elif gb == 2:
                        claimed17 += 1
            d1 = 1 if DAYS > 1 else 0
            print(f"GPU quota (internal): seed8[D1]={mp4_ops8[d1]}, claimed8={claimed8}; seed17[D1]={mp4_ops17[d1]}, claimed17={claimed17}")
        return

    # Создаём популяцию
    idx = {name: i for i, name in enumerate(mp3_fields)}
    # Маппинг точной даты производства по PSN из MP3
    psn_to_mfg: Dict[int, date] = {}
    av = pyflamegpu.AgentVector(agent_desc, n)
    # Плотный индекс борта по aircraft_number для соответствия линейной индексации MP5
    frames_index = env_data.get('frames_index', {})
    for i, r in enumerate(mp3_rows):
        ai = av[i]
        ac_num_for_idx = int(r[idx.get('aircraft_number', -1)] or 0)
        frame_idx = int(frames_index.get(ac_num_for_idx, 0)) if ac_num_for_idx else 0
        ai.setVariableUInt("idx", frame_idx)
        ai.setVariableUInt("psn", int(r[idx['psn']] or 0))
        ai.setVariableUInt("partseqno_i", int(r[idx['partseqno_i']] or 0))
        ai.setVariableUInt("group_by", int(r[idx.get('group_by', -1)] or 0))
        ai.setVariableUInt("aircraft_number", int(r[idx.get('aircraft_number', -1)] or 0))
        ai.setVariableUInt("ac_type_mask", int(r[idx.get('ac_type_mask', -1)] or 0))
        # Сохраняем исходную дату производства для экспорта и записываем epoch-days (UInt16) в агентную переменную
        ord_val = 0
        if 'mfg_date' in idx:
            mfg_val = r[idx['mfg_date']]
            if mfg_val:
                psn_to_mfg[int(r[idx['psn']] or 0)] = mfg_val
                try:
                    # ClickHouse Date = дни от 1970-01-01
                    epoch = date(1970, 1, 1)
                    ord_val = max(0, int((mfg_val - epoch).days))
                except Exception:
                    ord_val = 0
        ai.setVariableUInt("mfg_date", ord_val)
        ai.setVariableUInt("status_id", int(r[idx['status_id']] or 0))
        ai.setVariableUInt("repair_days", int(r[idx['repair_days']] or 0))
        partseq = int(r[idx['partseqno_i']] or 0)
        b8, b17, rt, pt, at = mp1_map.get(partseq, (0, 0, 0, 0, 0))
        # Выбор BR по маске типа планера (у планера всегда один бит). br=0 => неремонтопригоден.
        mask = int(r[idx.get('ac_type_mask', -1)] or 0)
        br_val = 0
        if mask & 32:
            br_val = int(b8)
        elif mask & 64:
            br_val = int(b17)
        else:
            # Если маска не задана или не планер — порог остаётся 0 (неремонтопригоден)
            br_val = 0
        ai.setVariableUInt("repair_time", int(rt))
        ai.setVariableUInt("ppr", int(r[idx.get('ppr', -1)] or 0))
        ai.setVariableUInt("sne", int(r[idx.get('sne', -1)] or 0))
        ai.setVariableUInt("ll", int(r[idx.get('ll', -1)] or 0))
        ai.setVariableUInt("oh", int(r[idx.get('oh', -1)] or 0))
        ai.setVariableUInt("br", int(br_val))
    sim.setPopulationData(av)

    # Опциональная верификация агентных переменных
    if a.verify_agents:
        pop0 = pyflamegpu.AgentVector(agent_desc)
        sim.getPopulationData(pop0)
        zero_cnt = 0
        min_ord = None
        max_ord = None
        for ag in pop0:
            v = int(ag.getVariableUInt('mfg_date'))
            if v == 0:
                zero_cnt += 1
            if min_ord is None or v < min_ord:
                min_ord = v
            if max_ord is None or v > max_ord:
                max_ord = v
        print(f"Agent mfg_date: zeros={zero_cnt}/{n}, min_ord={min_ord}, max_ord={max_ord}")

    # Выполняем 1 шаг (D0): rtc_repair
    sim.step()

    # Экспорт MP2 за D0
    exporter = FlameMacroProperty2Exporter(client=client)
    exporter.ensure_table()
    if a.clean_mp2:
        client.execute(f"TRUNCATE TABLE {exporter.table_name}")

    mp5_maps = preload_mp5_maps(client)
    all_days = sorted(mp4.keys())
    if not all_days:
        raise RuntimeError("Нет дат в MP4")
    # Смещение старта на D+1: пропускаем первый день (D0)
    start_idx = 1 if len(all_days) > 1 else 0
    days_list = all_days[start_idx : start_idx + (a.days if a.days else 1)]

    total_step_s = 0.0
    total_export_s = 0.0
    all_rows: List[Dict] = []
    for day_idx, D in enumerate(days_list):
        t0 = time.perf_counter()
        # Перед шагом заполняем массивы суточных часов для ops_check
        today_map = mp5_maps.get(D, {})
        next_map = mp5_maps.get(D + timedelta(days=1), {})
        daily_today, daily_next, _, _ = build_daily_arrays(mp3_rows, mp3_fields, mp1_map, today_map, next_map)
        # Запишем суточные часы прямо в агентные переменные для RTC совместимости
        pop_buf = pyflamegpu.AgentVector(agent_desc)
        sim.getPopulationData(pop_buf)
        for i, ag in enumerate(pop_buf):
            ag.setVariableUInt('daily_today_u32', int(daily_today[i] if i < len(daily_today) else 0))
            ag.setVariableUInt('daily_next_u32', int(daily_next[i] if i < len(daily_next) else 0))
        sim.setPopulationData(pop_buf)
        # Fallback: прокидываем квоты на D+1 с host (до фикса NVRTC)
        ops_targets = mp4.get(D + timedelta(days=1), {"ops_counter_mi8": 0, "ops_counter_mi17": 0})
        sim.setEnvironmentPropertyUInt32("quota_next_mi8", int(ops_targets.get('ops_counter_mi8', 0)))
        sim.setEnvironmentPropertyUInt32("quota_next_mi17", int(ops_targets.get('ops_counter_mi17', 0)))
        # Шаг суток
        sim.step()
        t1 = time.perf_counter()

        # Экспорт строк за день D
        pop = pyflamegpu.AgentVector(agent_desc)
        sim.getPopulationData(pop)
        ops_current = {1: 0, 2: 0}
        quota_claimed = {1: 0, 2: 0}
        for ag in pop:
            gb = int(ag.getVariableUInt('group_by'))
            if ag.getVariableUInt("status_id") == 2 and gb in (1,2):
                ops_current[gb] += 1
            if gb in (1,2) and int(ag.getVariableUInt('ops_ticket')) == 1:
                quota_claimed[gb] += 1
        epoch = date(1970,1,1)
        rows: List[Dict] = []
        for ag in pop:
            gb = int(ag.getVariableUInt('group_by'))
            if gb not in (1, 2):
                continue
            # Точная дата из MP3 по PSN; если нет — NULL
            mfg_date = psn_to_mfg.get(int(ag.getVariableUInt('psn')), None)
            sid = int(ag.getVariableUInt('status_id'))
            daily_f = int(ag.getVariableUInt('daily_today_u32')) if sid == 2 else 0
            # Возраст воздушного судна в полных годах (округление вниз)
            age_years = 0
            if mfg_date is not None:
                years = D.year - mfg_date.year
                if (D.month, D.day) < (mfg_date.month, mfg_date.day):
                    years -= 1
                age_years = max(0, min(255, years))
            rows.append({
                'dates': D,
                'psn': int(ag.getVariableUInt('psn')),
                'partseqno_i': int(ag.getVariableUInt('partseqno_i')),
                'aircraft_number': int(ag.getVariableUInt('aircraft_number')),
                'ac_type_mask': int(ag.getVariableUInt('ac_type_mask')),
                'status_id': sid,
                'daily_flight': daily_f,
                'ops_counter_mi8': int(ops_targets.get('ops_counter_mi8', 0)),
                'ops_counter_mi17': int(ops_targets.get('ops_counter_mi17', 0)),
                'ops_current_mi8': int(ops_current.get(1, 0)),
                'ops_current_mi17': int(ops_current.get(2, 0)),
                'partout_trigger': None,
                'assembly_trigger': None,
                'active_trigger': None,
                'aircraft_age_years': int(age_years),
                'mfg_date': mfg_date,
                'sne': int(ag.getVariableUInt('sne')),
                'ppr': int(ag.getVariableUInt('ppr')),
                'repair_days': int(ag.getVariableUInt('repair_days')),
                'simulation_metadata': (
                    f"v={vdate}/id={vid};D={D};mode=repair_only;"
                    f"quota_seed_mi8={int(ops_targets.get('ops_counter_mi8', 0))},"
                    f"quota_seed_mi17={int(ops_targets.get('ops_counter_mi17', 0))},"
                    f"quota_claimed_mi8={int(quota_claimed.get(1,0))},"
                    f"quota_claimed_mi17={int(quota_claimed.get(2,0))}"
                )
            })
        # Копим строки, вставим одним батчем после цикла
        all_rows.extend(rows)

        step_s = t1 - t0
        total_step_s += step_s
        print(f"D={D} step={step_s*1000:.2f} ms, rows={len(rows)}")

    # Один батч на весь период
    e0 = time.perf_counter()
    if all_rows:
        exporter.insert_rows(all_rows)
    e1 = time.perf_counter()
    total_export_s = e1 - e0
    print(f"Totals: step={total_step_s*1000:.2f} ms, export={total_export_s*1000:.2f} ms, days={len(days_list)}, rows={len(all_rows)}")


if __name__ == '__main__':
    main()


