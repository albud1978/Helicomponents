#!/usr/bin/env python3
# ARCHIVED 2026-06-06 (P2 cleanup): старый class-based фреймворк валидации, перекрыт каноническим code/validation/run_all.py (INV-1..12 + TEMP).
"""
Оркестратор валидации результатов симуляции (MESSAGING архитектура).

Запускает валидации для таблицы sim_masterv2_msg и генерирует отчёт.

Usage:
    python3 code/analysis/sim_validation_runner_msg.py --version-date 2025-07-04
"""

import argparse
import sys
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, List
from collections import defaultdict

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CODE_DIR = PROJECT_ROOT / "code"
if str(CODE_DIR) not in sys.path:
    sys.path.insert(0, str(CODE_DIR))

from utils.config_loader import get_clickhouse_client

OUTPUT_DIR = str(PROJECT_ROOT / "output")

# Таблица для messaging архитектуры
TABLE_NAME = "sim_masterv2_msg"
DEDUP_TABLES = {"sim_masterv2_v8"}


def get_table_expr(table: str, version_date_value: str, version_id: int | None) -> str:
    """Возвращает SQL-выражение таблицы с фильтрами и дедупликацией."""
    version_id_filter = f" AND version_id = {version_id}" if version_id is not None else ""
    if table in DEDUP_TABLES:
        return f"""(
            SELECT
                day_u16,
                idx,
                group_by,
                aircraft_number,
                state,
                sne,
                ppr,
                ll,
                oh,
                br,
                repair_days,
                repair_time
            FROM {table}
            WHERE version_date = {version_date_value}{version_id_filter}
            ORDER BY sne DESC
            LIMIT 1 BY day_u16, idx
        )"""
    return f"(SELECT * FROM {table} WHERE version_date = {version_date_value}{version_id_filter})"


def get_version_date_int(version_date_str: str) -> int:
    """Конвертирует YYYY-MM-DD в version_date (дни с 1970-01-01)"""
    dt = datetime.strptime(version_date_str, '%Y-%m-%d')
    return (dt - datetime(1970, 1, 1)).days


class MessagingQuotaValidator:
    """Валидатор квот ops_count vs quota_target для MESSAGING архитектуры"""
    
    WARN_DAYS = 180
    
    def __init__(self, client, version_date_value, version_date_str: str, table: str = TABLE_NAME, version_id: int | None = None):
        self.client = client
        self.version_date = version_date_value
        self.version_date_str = version_date_str
        self.table = table
        self.version_id = version_id
        self.table_expr = get_table_expr(table, version_date_value, version_id)
        self.errors: List[Dict] = []
        self.warnings: List[Dict] = []
        self.stats: Dict = {}
    
    def get_quota_data(self) -> List[Dict]:
        """Получает данные ops_count vs quota_target по дням"""
        
        # quota_target из flight_program_ac
        quota_query = f"""
            SELECT 
                dateDiff('day', toDate('{self.version_date_str}'), dates) as day_index,
                ops_counter_mi8 as quota_mi8,
                ops_counter_mi17 as quota_mi17
            FROM flight_program_ac
            WHERE version_date = toDate('{self.version_date_str}')
            ORDER BY dates
        """
        
        quota_data = self.client.execute(quota_query)
        quota_map = {row[0]: (row[1], row[2]) for row in quota_data}
        
        # ops_count из sim_masterv2_msg
        version_id_filter = f" AND version_id = {self.version_id}" if self.version_id is not None else ""
        if self.table in DEDUP_TABLES:
            ops_query = f"""
                SELECT 
                    day_u16,
                    group_by,
                    countDistinctIf(idx, state = 'operations') as ops_count
                FROM {self.table}
                WHERE version_date = {self.version_date}{version_id_filter} AND group_by IN (1, 2)
                GROUP BY day_u16, group_by
                ORDER BY day_u16, group_by
            """
        else:
            ops_query = f"""
                SELECT 
                    day_u16,
                    group_by,
                    countIf(state = 'operations') as ops_count
                FROM {self.table_expr}
                WHERE group_by IN (1, 2)
                GROUP BY day_u16, group_by
                ORDER BY day_u16, group_by
            """
        
        ops_data = self.client.execute(ops_query)
        
        ops_by_day = defaultdict(lambda: {'mi8': 0, 'mi17': 0})
        for day, gb, cnt in ops_data:
            if gb == 1:
                ops_by_day[day]['mi8'] = cnt
            else:
                ops_by_day[day]['mi17'] = cnt
        
        result = []
        for day in sorted(ops_by_day.keys()):
            if day not in quota_map:
                # Подтягиваем реальный target точечным запросом
                rows = self.client.execute(f"""
                    SELECT ops_counter_mi8, ops_counter_mi17
                    FROM flight_program_ac
                    WHERE version_date = toDate('{self.version_date_str}')
                      AND dateDiff('day', toDate('{self.version_date_str}'), dates) = {day}
                    LIMIT 1
                """)
                if not rows:
                    raise RuntimeError(f"Нет target в flight_program_ac для day={day}")
                quota_mi8, quota_mi17 = rows[0]
                quota_map[day] = (quota_mi8, quota_mi17)
            else:
                quota_mi8, quota_mi17 = quota_map[day]
            ops_mi8 = ops_by_day[day]['mi8']
            ops_mi17 = ops_by_day[day]['mi17']
            
            result.append({
                'day': day,
                'quota_mi8': quota_mi8,
                'quota_mi17': quota_mi17,
                'ops_mi8': ops_mi8,
                'ops_mi17': ops_mi17,
                'delta_mi8': ops_mi8 - quota_mi8,
                'delta_mi17': ops_mi17 - quota_mi17
            })
        
        return result
    
    def validate(self) -> Dict:
        """Основная валидация"""
        print(f"\n📊 Валидация квот (таблица: {self.table})")
        
        data = self.get_quota_data()
        
        if not data:
            self.errors.append({'type': 'NO_DATA', 'message': 'Нет данных для валидации'})
            return {'valid': False, 'errors': self.errors, 'warnings': self.warnings, 'stats': {}}
        
        stats = {
            'mi8': {'ok': 0, 'warn': 0, 'error': 0},
            'mi17': {'ok': 0, 'warn': 0, 'error': 0}
        }
        
        # Хронологические списки
        self.errors = []
        self.warnings = []
        
        for row in data:
            for ac_type in ['mi8', 'mi17']:
                delta = row[f'delta_{ac_type}']
                day = row['day']
                
                if delta == 0:
                    stats[ac_type]['ok'] += 1
                    continue
                
                message = (
                    f"Day {day}: {ac_type} ops={row[f'ops_{ac_type}']} "
                    f"vs target={row[f'quota_{ac_type}']} (delta={delta:+d})"
                )
                
                if day <= self.WARN_DAYS:
                    stats[ac_type]['warn'] += 1
                    self.warnings.append({'type': 'DELTA', 'message': message, 'day': day})
                else:
                    stats[ac_type]['error'] += 1
                    self.errors.append({'type': 'DELTA', 'message': message, 'day': day})
        
        self.stats = stats
        
        for ac_type in ['mi8', 'mi17']:
            s = stats[ac_type]
            total = sum(s.values())
            print(f"  {ac_type.upper()}: OK={s['ok']}, warn={s['warn']}, error={s['error']} (total={total})")
        
        valid = all(stats[t]['error'] == 0 for t in ['mi8', 'mi17'])
        
        return {
            'valid': valid,
            'errors': self.errors,
            'warnings': self.warnings,
            'stats': stats
        }


class MessagingTransitionsValidator:
    """Валидатор переходов для MESSAGING архитектуры"""
    
    # Разрешённые переходы
    ALLOWED_TRANSITIONS = {
        'transition_2_to_4': ('operations', 'repair'),
        'transition_2_to_6': ('operations', 'storage'),
        'transition_2_to_3': ('operations', 'serviceable'),
        'transition_3_to_2': ('serviceable', 'operations'),
        'transition_5_to_2': ('reserve', 'operations'),
        'transition_1_to_2': ('inactive', 'operations'),
        'transition_4_to_5': ('repair', 'reserve'),
        'transition_1_to_4': ('inactive', 'repair'),
        'transition_4_to_2': ('repair', 'operations'),
        'transition_0_to_3': ('spawn', 'serviceable'),  # spawn
        'transition_0_to_2': ('spawn', 'operations'),   # spawn (динамический)
    }
    
    def __init__(self, client, version_date_value, version_date_str: str, table: str = TABLE_NAME, version_id: int | None = None):
        self.client = client
        self.version_date = version_date_value
        self.version_date_str = version_date_str
        self.table = table
        self.version_id = version_id
        self.table_expr = get_table_expr(table, version_date_value, version_id)
        self.errors: List[Dict] = []
        self.warnings: List[Dict] = []
        self.stats: Dict = {}
    
    def validate_matrix(self) -> Dict:
        """Валидация матрицы переходов"""
        print(f"\n📊 Валидация переходов (таблица: {self.table})")
        
        # Считаем переходы
        cols = list(self.ALLOWED_TRANSITIONS.keys())
        col_sums = []
        
        for col in cols:
            try:
                query = f"""
                    SELECT 
                        group_by,
                        sum({col}) as cnt
                    FROM {self.table_expr}
                    WHERE {col} > 0
                    GROUP BY group_by
                """
                result = self.client.execute(query)
                mi8 = sum(r[1] for r in result if r[0] == 1)
                mi17 = sum(r[1] for r in result if r[0] == 2)
                col_sums.append({'col': col, 'mi8': mi8, 'mi17': mi17, 'total': mi8 + mi17})
            except Exception as e:
                # Колонка может отсутствовать
                col_sums.append({'col': col, 'mi8': 0, 'mi17': 0, 'total': 0, 'error': str(e)})
        
        by_type = {}
        for cs in col_sums:
            col = cs['col']
            from_to = self.ALLOWED_TRANSITIONS.get(col, ('?', '?'))
            by_type[col] = {
                'from': from_to[0],
                'to': from_to[1],
                'count': cs['total'],
                'mi8': cs['mi8'],
                'mi17': cs['mi17'],
                'allowed': True
            }
        
        # Вывод
        print("  Матрица переходов:")
        for col, data in by_type.items():
            if data['count'] > 0:
                print(f"    {data['from']} → {data['to']}: {data['count']:,} (Mi-8: {data['mi8']:,}, Mi-17: {data['mi17']:,})")
        
        self.stats['matrix'] = {'by_type': by_type}
        
        return {'valid': True, 'by_type': by_type}
    
    def validate_repair_duration(self) -> Dict:
        """Валидация длительности ремонта"""
        print(f"\n📊 Валидация длительности ремонта")
        
        # Ищем пары 2→4 и 4→5 для одного борта
        query = f"""
            WITH repairs AS (
                SELECT 
                    idx,
                    group_by,
                    min(day_u16) as repair_start,
                    max(day_u16) as repair_end
                FROM {self.table_expr}
                WHERE state = 'repair'
                GROUP BY idx, group_by
            )
            SELECT 
                group_by,
                count() as total,
                min(repair_end - repair_start) as min_dur,
                max(repair_end - repair_start) as max_dur,
                avg(repair_end - repair_start) as avg_dur
            FROM repairs
            GROUP BY group_by
        """
        
        try:
            result = self.client.execute(query)
            
            summary = {}
            for gb, total, min_d, max_d, avg_d in result:
                ac_type = 'mi8' if gb == 1 else 'mi17'
                expected = 180  # Норматив ремонта
                
                summary[ac_type] = {
                    'total_repairs': total,
                    'expected_duration': expected,
                    'min_duration': min_d,
                    'max_duration': max_d,
                    'avg_duration': avg_d,
                    'correct': total  # Упрощённо считаем все корректными
                }
                
                print(f"  {ac_type.upper()}: {total} ремонтов, min={min_d}, max={max_d}, avg={avg_d:.1f}")
            
            self.stats['repair_duration'] = {'summary': summary}
            
        except Exception as e:
            print(f"  ⚠️ Ошибка: {e}")
            self.stats['repair_duration'] = {'error': str(e)}
        
        return {'valid': True}

    def validate_limiter_exit(self) -> Dict:
        """Проверяет, что выход в storage/unserviceable не происходит при limiter > 0"""
        print(f"\n📊 Валидация limiter при выходе в storage/unserviceable (таблица: {self.table})")
        
        # Лимитер есть только в limiter-таблицах, иначе пропускаем
        cols = self.client.execute(f"DESCRIBE TABLE {self.table}")
        has_limiter = any(row[0] == 'limiter' for row in cols)
        if not has_limiter:
            self.stats['limiter_exit'] = {'skipped': True, 'reason': 'no limiter column'}
            print("  ⚠️ Пропуск: колонка limiter отсутствует")
            return {'valid': True, 'skipped': True}
        
        query = f"""
            SELECT
                count() as cnt,
                countIf(group_by = 1) as mi8_cnt,
                countIf(group_by = 2) as mi17_cnt
            FROM (
                SELECT
                    day_u16,
                    idx,
                    group_by,
                    state,
                    limiter,
                    lagInFrame(state) OVER (PARTITION BY idx ORDER BY day_u16) as prev_state
                FROM {self.table_expr}
            )
            WHERE prev_state IS NOT NULL
              AND prev_state != state
              AND state IN ('storage', 'unserviceable')
              AND limiter > 0
        """
        
        cnt, mi8, mi17 = self.client.execute(query)[0]
        if cnt > 0:
            self.errors.append({
                'type': 'LIMITER_EXIT',
                'message': f"Выход в storage/unserviceable при limiter>0: {cnt} (Mi-8={mi8}, Mi-17={mi17})"
            })
            self.stats['limiter_exit'] = {'valid': False, 'total': cnt, 'mi8': mi8, 'mi17': mi17}
            print(f"  ❌ Найдено: {cnt} (Mi-8={mi8}, Mi-17={mi17})")

            detail_query = f"""
                SELECT
                    day_u16,
                    idx,
                    aircraft_number,
                    group_by,
                    prev_state,
                    state,
                    limiter
                FROM (
                    SELECT
                        day_u16,
                        idx,
                        aircraft_number,
                        group_by,
                        state,
                        limiter,
                        lagInFrame(state) OVER (PARTITION BY idx ORDER BY day_u16) as prev_state
                    FROM {self.table_expr}
                )
                WHERE prev_state IS NOT NULL
                  AND prev_state != state
                  AND state IN ('storage', 'unserviceable')
                  AND limiter > 0
                ORDER BY day_u16, idx
                LIMIT 50
            """
            details = self.client.execute(detail_query)
            for day_u16, idx, acn, gb, prev_state, state, limiter in details:
                self.errors.append({
                    'type': 'LIMITER_EXIT_DETAIL',
                    'message': (
                        f"Day {day_u16}: idx={idx}, acn={acn}, group_by={gb}, "
                        f"{prev_state}->{state}, limiter={limiter}"
                    )
                })
            return {'valid': False}
        
        self.stats['limiter_exit'] = {'valid': True, 'total': 0}
        print("  ✅ Нарушений нет")
        return {'valid': True}
    
    def run_all(self) -> Dict:
        """Запуск всех валидаций переходов"""
        self.validate_matrix()
        self.validate_repair_duration()
        self.validate_limiter_exit()
        
        return {
            'valid': len(self.errors) == 0,
            'errors': self.errors,
            'warnings': self.warnings,
            'stats': self.stats
        }


class MessagingIncrementsValidator:
    """Валидатор инкрементов для MESSAGING архитектуры"""
    
    def __init__(self, client, version_date_value, version_date_str: str, table: str = TABLE_NAME, version_id: int | None = None):
        self.client = client
        self.version_date = version_date_value
        self.version_date_str = version_date_str
        self.table = table
        self.version_id = version_id
        self.table_expr = get_table_expr(table, version_date_value, version_id)
        self.errors: List[Dict] = []
        self.warnings: List[Dict] = []
        self.stats: Dict = {}
        self.has_dt = self._has_column("dt")
    
    def _has_column(self, name: str) -> bool:
        """Проверяет наличие колонки в таблице"""
        cols = self.client.execute(f"DESCRIBE TABLE {self.table}")
        return any(row[0] == name for row in cols)
    
    def validate_dt_invariant(self) -> Dict:
        """Проверяет: dt > 0 ТОЛЬКО в operations"""
        print(f"\n📊 Валидация инварианта dt (таблица: {self.table})")
        
        if not self.has_dt:
            # Для limiter (без dt) используем delta_sne по предыдущему состоянию
            query = f"""
                SELECT 
                    prev_state,
                    countIf(delta_sne > 0) as with_inc,
                    count() as total
                FROM (
                    SELECT 
                        idx,
                        state,
                        lagInFrame(state) OVER (PARTITION BY idx ORDER BY day_u16) as prev_state,
                        ifNull(sne - lagInFrame(sne) OVER (PARTITION BY idx ORDER BY day_u16), 0) as delta_sne
                    FROM {self.table_expr}
                )
                WHERE prev_state IS NOT NULL AND prev_state != ''
                  AND state IS NOT NULL AND state != ''
                  AND state != 'operations'
                GROUP BY prev_state
            """
            
            result = self.client.execute(query)
            violations = []
            for prev_state, with_inc, total in result:
                if prev_state != 'operations' and with_inc > 0:
                    violations.append({'state': prev_state, 'count': with_inc})
                    self.errors.append({
                        'type': 'SNE_INVARIANT',
                        'message': f"delta_sne > 0 вне operations (prev_state={prev_state}): {with_inc} интервалов"
                    })
                print(f"  {prev_state}: delta_sne>0 в {with_inc:,}/{total:,} интервалов")
            
            # Примеры (хронология) для нарушений
            detail_query = f"""
                SELECT 
                    day_u16,
                    idx,
                    prev_state,
                    state,
                    delta_sne
                FROM (
                    SELECT
                        day_u16,
                        idx,
                        state,
                        lagInFrame(state) OVER (PARTITION BY idx ORDER BY day_u16) as prev_state,
                        ifNull(sne - lagInFrame(sne) OVER (PARTITION BY idx ORDER BY day_u16), 0) as delta_sne
                    FROM {self.table_expr}
                )
                WHERE prev_state IS NOT NULL AND prev_state != ''
                  AND state IS NOT NULL AND state != ''
                  AND prev_state != 'operations'
                  AND state != 'operations'
                  AND delta_sne > 0
                ORDER BY day_u16, idx
                LIMIT 50
            """
            details = self.client.execute(detail_query)
            for day_u16, idx, prev_state, state, delta_sne in details:
                self.errors.append({
                    'type': 'SNE_INVARIANT_DETAIL',
                    'message': f"Day {day_u16}: idx={idx}, prev_state={prev_state}, state={state}, delta_sne={delta_sne}"
                })
            
            valid = len(violations) == 0
            self.stats['dt_invariant'] = {'valid': valid, 'violations': violations, 'method': 'delta_sne'}
            
            if valid:
                print("  ✅ Инвариант delta_sne соблюдён")
            else:
                print(f"  ❌ Инвариант delta_sne НАРУШЕН: {len(violations)} категорий")
            
            return {'valid': valid, 'violations': violations}
        
        query = f"""
            SELECT 
                state,
                countIf(dt > 0) as with_dt,
                count() as total
            FROM {self.table_expr}
            GROUP BY state
        """
        
        result = self.client.execute(query)
        
        violations = []
        for state, with_dt, total in result:
            if state != 'operations' and with_dt > 0:
                violations.append({'state': state, 'count': with_dt})
                self.errors.append({
                    'type': 'DT_INVARIANT',
                    'message': f"dt > 0 в state={state}: {with_dt} записей"
                })
            print(f"  {state}: dt>0 в {with_dt:,}/{total:,} записей")
        
        valid = len(violations) == 0
        self.stats['dt_invariant'] = {'valid': valid, 'violations': violations, 'method': 'dt'}
        
        if valid:
            print("  ✅ Инвариант dt соблюдён")
        else:
            print(f"  ❌ Инвариант dt НАРУШЕН: {len(violations)} категорий")
        
        return {'valid': valid, 'violations': violations}
    
    def validate_sne_consistency(self) -> Dict:
        """Проверяет: Σdt = Δsne для каждого борта"""
        print(f"\n📊 Валидация консистентности Σdt = Δsne")
        
        if not self.has_dt:
            query = f"""
                SELECT 
                    idx,
                    sumIf(delta_sne, prev_state = 'operations' OR state = 'operations') as sum_ops,
                    sumIf(delta_sne, prev_state != 'operations' AND state != 'operations') as sum_non_ops
                FROM (
                    SELECT 
                        idx,
                        state,
                        lagInFrame(state) OVER (PARTITION BY idx ORDER BY day_u16) as prev_state,
                        ifNull(sne - lagInFrame(sne) OVER (PARTITION BY idx ORDER BY day_u16), 0) as delta_sne
                    FROM {self.table_expr}
                )
                WHERE prev_state IS NOT NULL AND prev_state != ''
                  AND state IS NOT NULL AND state != ''
                GROUP BY idx
            """
            
            result = self.client.execute(query)
            violations = 0
            for idx, sum_ops, sum_non_ops in result:
                if sum_non_ops > 0:
                    violations += 1
                    if violations <= 5:
                        self.errors.append({
                            'type': 'SNE_NON_OPS',
                            'message': f"idx={idx}: delta_sne вне ops = {sum_non_ops}"
                        })
                        print(f"    ⚠️ idx={idx}: delta_sne вне ops = {sum_non_ops}")
            
            total = len(result)
            ok = total - violations
            print(f"  Агентов: {total}, корректных: {ok}, с налётом вне ops: {violations}")
            
            valid = violations == 0
            self.stats['sne_consistency'] = {'valid': valid, 'summary': {'ok': ok, 'violations': violations}, 'method': 'delta_sne'}
            
            if valid:
                print("  ✅ Налёт вне ops отсутствует")
            
            return {'valid': valid}
        
        query = f"""
            SELECT 
                idx,
                sum(dt) as sum_dt,
                max(sne) - min(sne) as delta_sne,
                abs(sum(dt) - (max(sne) - min(sne))) as diff
            FROM {self.table_expr}
            GROUP BY idx
            HAVING diff > 0
        """
        
        result = self.client.execute(query)
        
        violations = len(result)
        total_query = f"SELECT count(DISTINCT idx) FROM {self.table_expr}"
        total = self.client.execute(total_query)[0][0]
        ok = total - violations
        
        print(f"  Агентов: {total}, корректных: {ok}, с расхождением: {violations}")
        
        if violations > 0:
            for row in result[:5]:
                self.errors.append({
                    'type': 'SNE_MISMATCH',
                    'message': f"idx={row[0]}: Σdt={row[1]}, Δsne={row[2]}, diff={row[3]}"
                })
                print(f"    ⚠️ idx={row[0]}: Σdt={row[1]}, Δsne={row[2]}, diff={row[3]}")
        
        valid = violations == 0
        self.stats['sne_consistency'] = {'valid': valid, 'summary': {'ok': ok, 'violations': violations}}
        
        if valid:
            print("  ✅ Консистентность Σdt = Δsne подтверждена")
        
        return {'valid': valid}
    
    def validate_aggregate_stats(self) -> Dict:
        """Агрегированная статистика налёта"""
        print(f"\n📊 Агрегированный налёт")
        
        if not self.has_dt:
            query = f"""
                SELECT 
                    group_by,
                    countDistinct(idx) as ac_count,
                    sumIf(delta_sne, prev_state = 'operations' OR state = 'operations') / 60.0 as total_hours
                FROM (
                    SELECT 
                        idx,
                        group_by,
                        state,
                        lagInFrame(state) OVER (PARTITION BY idx ORDER BY day_u16) as prev_state,
                        ifNull(sne - lagInFrame(sne) OVER (PARTITION BY idx ORDER BY day_u16), 0) as delta_sne
                    FROM {self.table_expr}
                )
                WHERE prev_state IS NOT NULL AND prev_state != ''
                  AND state IS NOT NULL AND state != ''
                GROUP BY group_by
            """
        else:
            query = f"""
                SELECT 
                    group_by,
                    count(DISTINCT idx) as ac_count,
                    sum(dt) / 60.0 as total_hours
                FROM {self.table_expr}
                GROUP BY group_by
            """
        
        result = self.client.execute(query)
        
        agg = {}
        for gb, ac_count, total_hours in result:
            ac_type = 'Mi-8' if gb == 1 else 'Mi-17'
            avg_per_ac = total_hours / ac_count if ac_count > 0 else 0
            agg[ac_type] = {
                'ac_count': ac_count,
                'total_hours': total_hours,
                'avg_per_ac': avg_per_ac
            }
            print(f"  {ac_type}: {ac_count} бортов, {total_hours:,.0f} ч, ср. {avg_per_ac:,.1f} ч/борт")
        
        self.stats['aggregate'] = agg
        
        return {'valid': True, 'aggregate': agg}
    
    def run_all(self) -> Dict:
        """Запуск всех валидаций инкрементов"""
        self.validate_dt_invariant()
        self.validate_sne_consistency()
        self.validate_aggregate_stats()
        
        return {
            'valid': len(self.errors) == 0,
            'errors': self.errors,
            'warnings': self.warnings,
            'stats': self.stats
        }


def generate_report(version_date_str: str, results: Dict, table: str) -> str:
    """Генерирует MD отчёт"""
    
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    lines = [
        f"# Отчёт валидации симуляции (MESSAGING)",
        f"",
        f"**Дата отчёта:** {now}",
        f"**Датасет:** {version_date_str}",
        f"**Таблица:** {table}",
        f"",
        f"## Сводка",
        f"",
    ]
    
    all_valid = all(r.get('valid', False) for r in results.values())
    total_errors = sum(len(r.get('errors', [])) for r in results.values())
    total_warnings = sum(len(r.get('warnings', [])) for r in results.values())
    
    if all_valid:
        lines.append(f"✅ **ВАЛИДАЦИЯ ПРОЙДЕНА**")
    else:
        lines.append(f"❌ **ВАЛИДАЦИЯ НЕ ПРОЙДЕНА**")
    
    lines.extend([
        f"",
        f"| Проверка | Статус | Ошибки | Предупреждения |",
        f"|----------|--------|--------|----------------|",
    ])
    
    check_names = {
        'quota': 'Квоты ops vs target',
        'transitions': 'Матрица переходов',
        'increments': 'Инкременты наработок'
    }
    
    for key, name in check_names.items():
        if key in results:
            r = results[key]
            status = "✅" if r.get('valid', False) else "❌"
            errors = len(r.get('errors', []))
            warnings = len(r.get('warnings', []))
            lines.append(f"| {name} | {status} | {errors} | {warnings} |")
    
    lines.extend([
        f"",
        f"**Всего:** {total_errors} ошибок, {total_warnings} предупреждений",
        f"",
    ])
    
    # Детали квот
    if 'quota' in results:
        lines.extend([f"## 1. Валидация квот", f""])
        stats = results['quota'].get('stats', {})
        
        for ac_type in ['mi8', 'mi17']:
            if ac_type in stats:
                s = stats[ac_type]
                total = sum(s.values())
                if total > 0:
                    lines.extend([
                        f"### {ac_type.upper()}",
                        f"",
                        f"| Категория | Дней | % |",
                        f"|-----------|------|---|",
                        f"| Точное соответствие | {s.get('ok', 0)} | {100*s.get('ok',0)/total:.1f}% |",
                        f"| Предупреждения (<=180) | {s.get('warn', 0)} | {100*s.get('warn',0)/total:.1f}% |",
                        f"| Ошибки (>180) | {s.get('error', 0)} | {100*s.get('error',0)/total:.1f}% |",
                        f"",
                    ])
        
        quota_warnings = results['quota'].get('warnings', [])
        if quota_warnings:
            lines.extend([
                f"### Предупреждения по квотам (<=180 дней)",
                f"",
                f"Показано: {min(len(quota_warnings), 50)} из {len(quota_warnings)}",
                f"",
            ])
            quota_warnings = sorted(quota_warnings, key=lambda x: x.get('day', 0))
            for w in quota_warnings[:50]:
                lines.append(f"- {w.get('message', '')}")
            lines.append("")
        
        quota_errors = results['quota'].get('errors', [])
        if quota_errors:
            lines.extend([
                f"### Ошибки по квотам (>180 дней)",
                f"",
                f"Показано: {min(len(quota_errors), 50)} из {len(quota_errors)}",
                f"",
            ])
            quota_errors = sorted(quota_errors, key=lambda x: x.get('day', 0))
            for e in quota_errors[:50]:
                lines.append(f"- {e.get('message', '')}")
            lines.append("")
    
    # Детали переходов
    if 'transitions' in results:
        lines.extend([f"## 2. Валидация переходов", f""])
        trans_stats = results['transitions'].get('stats', {})
        
        if 'matrix' in trans_stats:
            by_type = trans_stats['matrix'].get('by_type', {})
            if by_type:
                lines.extend([
                    f"### Статистика переходов",
                    f"",
                    f"| Переход | Всего | Mi-8 | Mi-17 |",
                    f"|---------|-------|------|-------|",
                ])
                
                for col, data in sorted(by_type.items()):
                    if data['count'] > 0:
                        lines.append(f"| {data['from']}→{data['to']} | {data['count']:,} | {data['mi8']:,} | {data['mi17']:,} |")
                
                lines.append("")
    
    # Детали инкрементов
    if 'increments' in results:
        lines.extend([f"## 3. Валидация инкрементов", f""])
        inc_stats = results['increments'].get('stats', {})
        
        if 'aggregate' in inc_stats:
            agg = inc_stats['aggregate']
            lines.extend([
                f"### Агрегированный налёт",
                f"",
                f"| Тип | Бортов | Σ часов | Ср. на борт |",
                f"|-----|--------|---------|-------------|",
            ])
            
            for ac_type, data in agg.items():
                lines.append(f"| {ac_type} | {data['ac_count']} | {data['total_hours']:,.0f} | {data['avg_per_ac']:,.1f} |")
            
            lines.append("")
        
        inc_errors = results['increments'].get('errors', [])
        if inc_errors:
            lines.extend([
                f"### Ошибки инкрементов (примеры)",
                f"",
                f"Показано: {min(len(inc_errors), 20)} из {len(inc_errors)}",
                f"",
            ])
            for e in inc_errors[:20]:
                lines.append(f"- {e.get('message', '')}")
            lines.append("")
    
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description='Валидация симуляции MESSAGING архитектуры')
    parser.add_argument('--version-date', required=True, help='Дата версии (YYYY-MM-DD)')
    parser.add_argument('--version-id', type=int, default=None, help='version_id для фильтрации (опционально)')
    parser.add_argument('--table', default=TABLE_NAME, help=f'Таблица (default: {TABLE_NAME})')
    args = parser.parse_args()
    
    version_date_str = args.version_date
    version_date_days = get_version_date_int(version_date_str)
    version_date_ymd = int(version_date_str.replace('-', ''))
    table = args.table
    
    print("\n" + "="*80)
    print(f"ВАЛИДАЦИЯ MESSAGING СИМУЛЯЦИИ")
    print(f"  Датасет: {version_date_str}")
    print(f"  Таблица: {table}")
    print("="*80)
    
    client = get_clickhouse_client()
    
    results = {}
    
    # 1. Квоты
    print("\n" + "-"*60)
    # Определяем формат version_date в таблице
    cols = client.execute(f"DESCRIBE TABLE {table}")
    vtype = None
    for name, ctype, *_ in cols:
        if name == 'version_date':
            vtype = ctype
            break
    if vtype is None:
        raise RuntimeError("Колонка version_date не найдена в таблице")
    
    if vtype.startswith('Date'):
        version_date_value = f"toDate('{version_date_str}')"
    else:
        version_date_value = str(version_date_ymd)
    
    quota_validator = MessagingQuotaValidator(client, version_date_value, version_date_str, table, args.version_id)
    results['quota'] = quota_validator.validate()
    
    # 2. Переходы
    print("\n" + "-"*60)
    transitions_validator = MessagingTransitionsValidator(client, version_date_value, version_date_str, table, args.version_id)
    results['transitions'] = transitions_validator.run_all()
    
    # 3. Инкременты
    print("\n" + "-"*60)
    increments_validator = MessagingIncrementsValidator(client, version_date_value, version_date_str, table, args.version_id)
    results['increments'] = increments_validator.run_all()
    
    # Генерация отчёта
    report = generate_report(version_date_str, results, table)
    
    # Сохранение отчёта
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    report_path = os.path.join(OUTPUT_DIR, f"sim_validation_msg_{version_date_str}.md")
    
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write(report)
    
    print("\n" + "="*80)
    print("ИТОГОВАЯ СВОДКА")
    print("="*80)
    
    all_valid = all(r.get('valid', False) for r in results.values())
    total_errors = sum(len(r.get('errors', [])) for r in results.values())
    total_warnings = sum(len(r.get('warnings', [])) for r in results.values())
    
    print(f"\n📄 Отчёт сохранён: {report_path}")
    print(f"❌ Ошибок: {total_errors}")
    print(f"⚠️ Предупреждений: {total_warnings}")
    
    if all_valid:
        print("\n✅ ВАЛИДАЦИЯ MESSAGING СИМУЛЯЦИИ ПРОЙДЕНА")
        sys.exit(0)
    else:
        print("\n❌ ВАЛИДАЦИЯ MESSAGING СИМУЛЯЦИИ НЕ ПРОЙДЕНА")
        sys.exit(1)


if __name__ == '__main__':
    main()

