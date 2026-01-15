#!/usr/bin/env python3
"""
Оркестратор валидации результатов симуляции.

Запускает все валидации и генерирует отчёт в output/sim_validation_<version_date>.md

Валидации:
1. sim_validation_quota.py — ops_count vs quota_target
2. sim_validation_transitions.py — матрица переходов + длительность repair
3. sim_validation_increments.py — dt/sne/ppr инварианты

Usage:
    python3 code/analysis/sim_validation_runner.py --version-date 2025-07-04
    python3 code/analysis/sim_validation_runner.py --version-date 2025-12-30
    
    # Режим нулевой толерантности (warnings = failures)
    python3 code/analysis/sim_validation_runner.py --version-date 2025-07-04 --strict
    
    # Полный отчёт всех отклонений без лимитов
    python3 code/analysis/sim_validation_runner.py --version-date 2025-07-04 --strict --no-limit
"""

import argparse
import sys
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, List

# Портируемость: не привязываемся к Nextcloud/локальным абсолютным путям.
# Корень репозитория вычисляем относительно расположения этого файла:
#   <repo>/code/analysis/sim_validation_runner.py
PROJECT_ROOT = Path(__file__).resolve().parents[2]
CODE_DIR = PROJECT_ROOT / "code"
if str(CODE_DIR) not in sys.path:
    sys.path.insert(0, str(CODE_DIR))

from utils.config_loader import get_clickhouse_client

# Импорт валидаторов
from analysis.sim_validation_quota import QuotaValidator, get_version_date_int
from analysis.sim_validation_transitions import TransitionsValidator
from analysis.sim_validation_increments import IncrementsValidator


OUTPUT_DIR = str(PROJECT_ROOT / "output")


class InputDataValidator:
    """Валидатор входных данных heli_pandas"""
    
    # status_id включает как рабочие (1-6), так и ошибочные (10-15)
    STATUS_NAMES = {
        1: 'inactive',
        2: 'operations', 
        3: 'serviceable',
        4: 'repair',
        5: 'reserve',
        6: 'storage',
        10: 'err: NO_DATA (ll=0)',
        11: 'err: DATE_PAST',
        12: 'err: SNE_ZERO',
        13: 'err: OVER_LIMIT',
        14: 'err: BAD_COND',
        15: 'err: EARLY_DONOR',
    }
    
    def __init__(self, client, version_date: int):
        self.client = client
        self.version_date = version_date
        self.errors = []
        self.warnings = []
        self.stats = {}
    
    def validate(self) -> Dict:
        """Запускает валидацию входных данных"""
        print("\n" + "="*80)
        print(f"ВАЛИДАЦИЯ ВХОДНЫХ ДАННЫХ (heli_pandas) для version_date={self.version_date}")
        print("="*80)
        
        results = {
            'valid': True,
            'errors': [],
            'warnings': [],
            'stats': {}
        }
        
        # 1. Статистика по status_id (включая ошибочные 10-15)
        self._validate_status_distribution(results)
        
        # 2. Статистика по рабочим статусам (планеры)
        self._validate_working_statuses(results)
        
        # 3. Статистика по типам ВС и группам
        self._validate_aircraft_types(results)
        
        results['errors'] = self.errors
        results['warnings'] = self.warnings
        results['stats'] = self.stats
        results['valid'] = len(self.errors) == 0
        
        return results
    
    def _validate_status_distribution(self, results: Dict):
        """Проверяет распределение status_id (рабочие 1-6, ошибочные 10-15)"""
        print("\n--- Распределение status_id ---")
        
        query = f"""
            SELECT 
                status_id,
                count() as cnt,
                countIf(bitAnd(ac_type_mask, 32) > 0) as mi8,
                countIf(bitAnd(ac_type_mask, 64) > 0) as mi17
            FROM heli_pandas
            WHERE version_date = toDate({self.version_date})
            GROUP BY status_id
            ORDER BY status_id
        """
        
        rows = self.client.execute(query)
        
        status_stats = {'by_status': {}, 'total': 0, 'working': 0, 'errors': 0}
        
        for status_id, cnt, mi8, mi17 in rows:
            status_name = self.STATUS_NAMES.get(status_id, f'status_{status_id}')
            status_stats['by_status'][status_id] = {
                'name': status_name,
                'count': cnt,
                'mi8': mi8,
                'mi17': mi17
            }
            status_stats['total'] += cnt
            if status_id >= 10:
                status_stats['errors'] += cnt
            else:
                status_stats['working'] += cnt
        
        self.stats['status_distribution'] = status_stats
        
        print(f"\nВсего записей: {status_stats['total']:,}")
        print(f"  Рабочие (1-6): {status_stats['working']:,} ({100*status_stats['working']/status_stats['total']:.1f}%)")
        print(f"  Ошибочные (10-15): {status_stats['errors']:,} ({100*status_stats['errors']/status_stats['total']:.1f}%)")
        print(f"\n{'ID':>3} | {'Статус':<25} | {'Кол-во':>10} | {'Mi-8':>8} | {'Mi-17':>8}")
        print("-" * 70)
        
        for status_id in sorted(status_stats['by_status'].keys()):
            s = status_stats['by_status'][status_id]
            marker = "❌" if status_id >= 10 else "✅"
            print(f"{status_id:>3} | {s['name']:<25} | {s['count']:>10,} | {s['mi8']:>8,} | {s['mi17']:>8,} {marker}")
    
    def _validate_working_statuses(self, results: Dict):
        """Проверяет распределение рабочих статусов планеров (group_by=1,2)"""
        print("\n--- Распределение рабочих статусов (планеры group_by=1,2) ---")
        
        query = f"""
            SELECT 
                status_id,
                count() as cnt,
                countIf(group_by = 1) as mi8,
                countIf(group_by = 2) as mi17
            FROM heli_pandas
            WHERE version_date = toDate({self.version_date})
              AND group_by IN (1, 2)
              AND status_id BETWEEN 1 AND 6
            GROUP BY status_id
            ORDER BY status_id
        """
        
        rows = self.client.execute(query)
        
        status_stats = {'by_status': {}, 'total': 0}
        
        for status_id, cnt, mi8, mi17 in rows:
            status_name = self.STATUS_NAMES.get(status_id, f'status_{status_id}')
            status_stats['by_status'][status_id] = {
                'name': status_name,
                'total': cnt,
                'mi8': mi8,
                'mi17': mi17
            }
            status_stats['total'] += cnt
        
        self.stats['working_statuses'] = status_stats
        
        print(f"\n{'ID':>3} | {'Статус':<12} | {'Всего':>8} | {'Mi-8':>8} | {'Mi-17':>8}")
        print("-" * 55)
        
        for status_id in sorted(status_stats['by_status'].keys()):
            s = status_stats['by_status'][status_id]
            print(f"{status_id:>3} | {s['name']:<12} | {s['total']:>8,} | {s['mi8']:>8,} | {s['mi17']:>8,}")
        
        print("-" * 55)
        total_mi8 = sum(s['mi8'] for s in status_stats['by_status'].values())
        total_mi17 = sum(s['mi17'] for s in status_stats['by_status'].values())
        print(f"{'':>3} | {'ИТОГО':<12} | {status_stats['total']:>8,} | {total_mi8:>8,} | {total_mi17:>8,}")
    
    def _validate_aircraft_types(self, results: Dict):
        """Проверяет статистику по группам"""
        print("\n--- Статистика по группам ---")
        
        query = f"""
            SELECT 
                group_by,
                count() as cnt,
                countIf(status_id BETWEEN 1 AND 6) as working,
                countIf(status_id >= 10) as errors,
                countIf(bitAnd(ac_type_mask, 32) > 0) as mi8,
                countIf(bitAnd(ac_type_mask, 64) > 0) as mi17
            FROM heli_pandas
            WHERE version_date = toDate({self.version_date})
            GROUP BY group_by
            ORDER BY group_by
        """
        
        rows = self.client.execute(query)
        
        group_stats = {}
        GROUP_NAMES = {1: 'Mi-8 планеры', 2: 'Mi-17 планеры', 3: 'Двигатели', 
                       4: 'Редукторы', 5: 'Другие агрегаты', 6: 'Компоненты'}
        
        print(f"\n{'Group':>5} | {'Название':<18} | {'Всего':>10} | {'Рабочих':>10} | {'Ошибочных':>10}")
        print("-" * 75)
        
        for group_by, cnt, working, errors, mi8, mi17 in rows:
            name = GROUP_NAMES.get(group_by, f'group_{group_by}')
            group_stats[group_by] = {
                'name': name,
                'total': cnt,
                'working': working,
                'errors': errors,
                'mi8': mi8,
                'mi17': mi17
            }
            print(f"{group_by:>5} | {name:<18} | {cnt:>10,} | {working:>10,} | {errors:>10,}")
        
        self.stats['groups'] = group_stats


def generate_report(version_date_str: str, results: Dict, strict: bool = False, no_limit: bool = False) -> str:
    """
    Генерирует MD отчёт
    
    Args:
        version_date_str: дата версии
        results: результаты валидаций
        strict: режим нулевой толерантности (warnings = failures)
        no_limit: показывать все отклонения без лимита
    """
    
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    lines = [
        f"# Отчёт валидации симуляции",
        f"",
        f"**Дата отчёта:** {now}",
        f"**Датасет:** {version_date_str}",
        f"**Режим:** {'🔴 STRICT (нулевая толерантность)' if strict else '🟡 Стандартный'}",
        f"",
        f"## Сводка",
        f"",
    ]
    
    # Общий статус
    all_valid = all(r.get('valid', False) for r in results.values())
    total_errors = sum(len(r.get('errors', [])) for r in results.values())
    total_warnings = sum(len(r.get('warnings', [])) for r in results.values())
    total_deviations = total_errors + total_warnings
    
    # В strict режиме warnings также считаются failures
    if strict:
        passed = all_valid and total_warnings == 0
    else:
        passed = all_valid
    
    if passed:
        lines.append(f"✅ **ВАЛИДАЦИЯ ПРОЙДЕНА**")
    else:
        lines.append(f"❌ **ВАЛИДАЦИЯ НЕ ПРОЙДЕНА**")
    
    lines.extend([
        f"",
        f"| Проверка | Статус | Ошибки | Предупреждения | Всего отклонений |",
        f"|----------|--------|--------|----------------|------------------|",
    ])
    
    check_names = {
        'input_data': 'Входные данные (heli_pandas)',
        'quota': 'Квоты ops vs target',
        'transitions': 'Матрица переходов',
        'increments': 'Инкременты наработок'
    }
    
    for key, name in check_names.items():
        if key in results:
            r = results[key]
            errors = len(r.get('errors', []))
            warnings = len(r.get('warnings', []))
            deviations = errors + warnings
            
            # В strict режиме warnings также влияют на статус
            if strict:
                is_ok = r.get('valid', False) and warnings == 0
            else:
                is_ok = r.get('valid', False)
            
            status = "✅" if is_ok else "❌"
            lines.append(f"| {name} | {status} | {errors} | {warnings} | {deviations} |")
    
    lines.extend([
        f"",
        f"### Итоги",
        f"",
        f"| Метрика | Значение |",
        f"|---------|----------|",
        f"| ❌ Ошибки (CRITICAL) | **{total_errors}** |",
        f"| ⚠️ Предупреждения (WARNING) | **{total_warnings}** |",
        f"| 📊 Всего отклонений | **{total_deviations}** |",
        f"",
    ])
    
    # Детали по каждой проверке
    
    # 0. Входные данные
    if 'input_data' in results:
        input_errors = results['input_data'].get('errors', [])
        input_warnings = results['input_data'].get('warnings', [])
        input_status = "✅" if results['input_data'].get('valid', False) and (not strict or len(input_warnings) == 0) else "❌"
        
        lines.extend([
            f"## 0. Валидация входных данных {input_status}",
            f"",
            f"**Источник:** `heli_pandas` (таблица с исходными данными компонентов)",
            f"",
        ])
        
        input_stats = results['input_data'].get('stats', {})
        
        # status_distribution
        if 'status_distribution' in input_stats:
            sd = input_stats['status_distribution']
            lines.extend([
                f"### Распределение status_id",
                f"",
                f"| Метрика | Значение | % |",
                f"|---------|----------|---|",
                f"| Всего записей | {sd.get('total', 0):,} | 100% |",
                f"| Рабочие (1-6) | {sd.get('working', 0):,} | {100*sd.get('working',0)/sd.get('total',1):.1f}% |",
                f"| Ошибочные (10-15) | {sd.get('errors', 0):,} | {100*sd.get('errors',0)/sd.get('total',1):.1f}% |",
                f"",
                f"**Детализация по статусам:**",
                f"",
                f"| ID | Статус | Кол-во | Mi-8 | Mi-17 | Статус |",
                f"|----|--------|--------|------|-------|--------|",
            ])
            
            for status_id in sorted(sd.get('by_status', {}).keys()):
                s = sd['by_status'][status_id]
                status_mark = "❌" if status_id >= 10 else "✅"
                lines.append(f"| {status_id} | {s.get('name', '?')} | {s.get('count', 0):,} | {s.get('mi8', 0):,} | {s.get('mi17', 0):,} | {status_mark} |")
            
            lines.append("")
        
        # working_statuses  
        if 'working_statuses' in input_stats:
            ws = input_stats['working_statuses']
            lines.extend([
                f"### Рабочие статусы планеров (group_by=1,2)",
                f"",
                f"| ID | Статус | Всего | Mi-8 | Mi-17 |",
                f"|----|--------|-------|------|-------|",
            ])
            
            for status_id in sorted(ws.get('by_status', {}).keys()):
                s = ws['by_status'][status_id]
                lines.append(f"| {status_id} | {s.get('name', '?')} | {s.get('total', 0):,} | {s.get('mi8', 0):,} | {s.get('mi17', 0):,} |")
            
            total_mi8 = sum(s.get('mi8', 0) for s in ws.get('by_status', {}).values())
            total_mi17 = sum(s.get('mi17', 0) for s in ws.get('by_status', {}).values())
            lines.append(f"| — | **ИТОГО** | **{ws.get('total', 0):,}** | **{total_mi8:,}** | **{total_mi17:,}** |")
            lines.append("")
        
        # groups
        if 'groups' in input_stats:
            gs = input_stats['groups']
            lines.extend([
                f"### Статистика по группам",
                f"",
                f"| Group | Название | Всего | Рабочих | Ошибочных |",
                f"|-------|----------|-------|---------|-----------|",
            ])
            
            for group_id in sorted(gs.keys()):
                g = gs[group_id]
                lines.append(f"| {group_id} | {g.get('name', '?')} | {g.get('total', 0):,} | {g.get('working', 0):,} | {g.get('errors', 0):,} |")
            
            lines.append("")
    
    # 1. Квоты
    if 'quota' in results:
        quota_errors = results['quota'].get('errors', [])
        quota_warnings = results['quota'].get('warnings', [])
        quota_status = "✅" if results['quota'].get('valid', False) and (not strict or len(quota_warnings) == 0) else "❌"
        
        lines.extend([
            f"## 1. Валидация квот {quota_status}",
            f"",
            f"**Проверка:** ops_count vs quota_target по дням",
            f"",
            f"**Критерии:**",
            f"- ✅ Точное соответствие: delta = 0",
            f"- ⚪ Допустимо: |delta| ≤ 1 (TOLERANCE)",
            f"- ⚠️ Недобор: delta ∈ [-3, -2] (WARNING)",
            f"- ❌ Критичный: delta < -3 (ERROR)",
            f"- 📈 Избыток: delta > 0 (информационно)",
            f"",
        ])
        
        stats = results['quota'].get('stats', {})
        
        for ac_type in ['mi8', 'mi17']:
            if ac_type in stats:
                s = stats[ac_type]
                total = s.get('ok', 0) + s.get('minor', 0) + s.get('deficit', 0) + s.get('critical', 0) + s.get('excess', 0)
                if total > 0:
                    lines.extend([
                        f"### {ac_type.upper()}",
                        f"",
                        f"| Категория | Дней | % | Статус |",
                        f"|-----------|------|---|--------|",
                        f"| Точное соответствие | {s.get('ok', 0)} | {100*s.get('ok',0)/total:.1f}% | ✅ |",
                        f"| Отклонение ±1 | {s.get('minor', 0)} | {100*s.get('minor',0)/total:.1f}% | ⚪ |",
                        f"| Недобор 2-3 | {s.get('deficit', 0)} | {100*s.get('deficit',0)/total:.1f}% | ⚠️ WARNING |",
                        f"| Критичный >3 | {s.get('critical', 0)} | {100*s.get('critical',0)/total:.1f}% | ❌ ERROR |",
                        f"| Избыток | {s.get('excess', 0)} | {100*s.get('excess',0)/total:.1f}% | 📈 |",
                        f"",
                    ])
        
        # Ошибки этой проверки
        if quota_errors:
            lines.extend([
                f"### ❌ Ошибки квот ({len(quota_errors)})",
                f"",
                f"| # | День | Тип | Квота | Факт | Дефицит |",
                f"|---|------|-----|-------|------|---------|",
            ])
            display_errs = quota_errors if no_limit else quota_errors[:20]
            for i, e in enumerate(display_errs, 1):
                lines.append(f"| {i} | {e.get('day', '?')} | {e.get('ac_type', '?')} | {e.get('quota', '?')} | {e.get('ops', '?')} | {e.get('deficit', '?')} |")
            if not no_limit and len(quota_errors) > 20:
                lines.append(f"| ... | ... | ... | ... | ... | ещё {len(quota_errors) - 20} |")
            lines.append("")
        
        # Warnings этой проверки
        if quota_warnings:
            lines.extend([
                f"### ⚠️ Предупреждения квот ({len(quota_warnings)})",
                f"",
                f"| # | День | Тип | Дефицит | Описание |",
                f"|---|------|-----|---------|----------|",
            ])
            display_warns = quota_warnings if no_limit else quota_warnings[:20]
            for i, w in enumerate(display_warns, 1):
                lines.append(f"| {i} | {w.get('day', '?')} | {w.get('ac_type', '?')} | {w.get('deficit', '?')} | Недобор 2-3 борта |")
            if not no_limit and len(quota_warnings) > 20:
                lines.append(f"| ... | ... | ... | ... | ещё {len(quota_warnings) - 20} |")
            lines.append("")
    
    # 2. Переходы
    if 'transitions' in results:
        trans_errors = results['transitions'].get('errors', [])
        trans_warnings = results['transitions'].get('warnings', [])
        trans_status = "✅" if results['transitions'].get('valid', False) and (not strict or len(trans_warnings) == 0) else "❌"
        
        lines.extend([
            f"## 2. Валидация переходов {trans_status}",
            f"",
            f"**Проверки:**",
            f"1. Матрица разрешённых переходов (transition_X_to_Y → state)",
            f"2. Консистентность state vs transition flags",
            f"3. Длительность ремонта vs repair_time",
            f"4. Отсутствие запрещённых переходов",
            f"",
        ])
        
        trans_stats = results['transitions'].get('stats', {})
        
        if 'matrix' in trans_stats:
            by_type = trans_stats['matrix'].get('by_type', {})
            if by_type:
                lines.extend([
                    f"### Статистика переходов",
                    f"",
                    f"| Переход | Всего | Mi-8 | Mi-17 | Статус |",
                    f"|---------|-------|------|-------|--------|",
                ])
                
                for col, data in sorted(by_type.items()):
                    status = "✅" if data['allowed'] else "❌ ЗАПРЕЩЁН"
                    lines.append(f"| {data['from']}→{data['to']} | {data['count']:,} | {data['mi8']:,} | {data['mi17']:,} | {status} |")
                
                lines.append("")
        
        if 'repair_duration' in trans_stats:
            repair = trans_stats['repair_duration']
            if 'summary' in repair and repair['summary']:
                lines.extend([
                    f"### Длительность ремонта",
                    f"",
                    f"| Тип | Ремонтов | Норматив | Min | Max | Avg | Корректных |",
                    f"|-----|----------|----------|-----|-----|-----|------------|",
                ])
                
                for ac_type, s in repair['summary'].items():
                    correct_pct = 100 * s['correct'] / s['total_repairs'] if s['total_repairs'] > 0 else 0
                    status = "✅" if s['correct'] == s['total_repairs'] else "⚠️"
                    lines.append(f"| {ac_type} | {s['total_repairs']} | {s['expected_duration']} дн. | {s['min_duration']} | {s['max_duration']} | {s['avg_duration']:.1f} | {s['correct']}/{s['total_repairs']} ({correct_pct:.0f}%) {status} |")
                
                lines.append("")
        
        # Ошибки этой проверки
        if trans_errors:
            lines.extend([
                f"### ❌ Ошибки переходов ({len(trans_errors)})",
                f"",
                f"| # | Тип | AC | День | Описание |",
                f"|---|-----|-----|------|----------|",
            ])
            display_errs = trans_errors if no_limit else trans_errors[:20]
            for i, e in enumerate(display_errs, 1):
                msg = e.get('message', '')[:50]
                lines.append(f"| {i} | {e.get('type', '?')} | {e.get('aircraft_number', '?')} | {e.get('day', '?')} | {msg} |")
            if not no_limit and len(trans_errors) > 20:
                lines.append(f"| ... | ... | ... | ... | ещё {len(trans_errors) - 20} |")
            lines.append("")
        
        # Warnings этой проверки
        if trans_warnings:
            lines.extend([
                f"### ⚠️ Предупреждения переходов ({len(trans_warnings)})",
                f"",
                f"| # | Тип | Описание |",
                f"|---|-----|----------|",
            ])
            display_warns = trans_warnings if no_limit else trans_warnings[:20]
            for i, w in enumerate(display_warns, 1):
                msg = w.get('message', '')[:60]
                lines.append(f"| {i} | {w.get('type', '?')} | {msg} |")
            if not no_limit and len(trans_warnings) > 20:
                lines.append(f"| ... | ... | ещё {len(trans_warnings) - 20} |")
            lines.append("")
    
    # 3. Инкременты
    if 'increments' in results:
        inc_errors = results['increments'].get('errors', [])
        inc_warnings = results['increments'].get('warnings', [])
        inc_status = "✅" if results['increments'].get('valid', False) and (not strict or len(inc_warnings) == 0) else "❌"
        
        lines.extend([
            f"## 3. Валидация инкрементов {inc_status}",
            f"",
            f"**Проверки:**",
            f"0. **НУЛЕВАЯ ТОЛЕРАНТНОСТЬ**: dt=0 в operations vs flight_program",
            f"1. Инвариант dt: dt > 0 только в operations",
            f"2. Консистентность SNE: Σdt = Δsne",
            f"3. PPR reset: ppr = 0 после ремонта",
            f"4. Агрегированный налёт по типам",
            f"",
        ])
        
        inc_stats = results['increments'].get('stats', {})
        
        # dt_zero_in_ops - НУЛЕВАЯ ТОЛЕРАНТНОСТЬ
        if 'dt_zero_in_ops' in inc_stats:
            dtz = inc_stats['dt_zero_in_ops']
            stats = dtz.get('stats', {})
            bugs = dtz.get('bugs', [])
            
            dtz_status = "✅" if dtz.get('valid', False) else "❌"
            lines.extend([
                f"### 🎯 Нулевая толерантность: dt=0 в operations {dtz_status}",
                f"",
                f"**Каждый dt=0 в operations должен соответствовать dt=0 в flight_program (зимовка).**",
                f"",
                f"| Тип | Баги (fp>0, sim=0) | Валидные (fp=0) | Статус |",
                f"|-----|---------------------|-----------------|--------|",
                f"| Mi-8 | {stats.get('mi8_bugs', 0)} | {stats.get('mi8_valid_zeros', 0)} | {'✅' if stats.get('mi8_bugs', 0) == 0 else '❌'} |",
                f"| Mi-17 | {stats.get('mi17_bugs', 0)} | {stats.get('mi17_valid_zeros', 0)} | {'✅' if stats.get('mi17_bugs', 0) == 0 else '❌'} |",
                f"| **ИТОГО** | **{stats.get('total_bugs', 0)}** | **{stats.get('total_valid', 0)}** | {dtz_status} |",
                f"",
            ])
            
            if bugs:
                lines.extend([
                    f"**❌ КРИТИЧЕСКИЕ БАГИ: dt=0 при fp>0**",
                    f"",
                    f"| # | AC | Тип | День | fp_dt | prev_state |",
                    f"|---|-----|-----|------|-------|------------|",
                ])
                display_bugs = bugs if no_limit else bugs[:30]
                for i, b in enumerate(display_bugs, 1):
                    ac_type = 'Mi-8' if b.get('group_by') == 1 else 'Mi-17'
                    lines.append(f"| {i} | {b.get('aircraft_number', '?')} | {ac_type} | {b.get('day', '?')} | {b.get('fp_dt', '?')} | {b.get('prev_state', '?')} |")
                if not no_limit and len(bugs) > 30:
                    lines.append(f"| ... | ... | ... | ... | ... | ещё {len(bugs) - 30} |")
                lines.append("")
            else:
                lines.extend([
                    f"**✅ Все {stats.get('total_valid', 0)} случаев dt=0 объяснены flight_program (зимовка Mi-17)**",
                    f"",
                ])
        
        # dt invariant
        if 'dt_invariant' in inc_stats:
            inv = inc_stats['dt_invariant']
            by_state = inv.get('by_state', {})
            
            if by_state:
                lines.extend([
                    f"### Инвариант dt (налёт по состояниям)",
                    f"",
                    f"| Состояние | Тип | Записей | С dt>0 | Σdt | Статус |",
                    f"|-----------|-----|---------|--------|-----|--------|",
                ])
                
                for (state, gb), data in sorted(by_state.items()):
                    ac_type = 'Mi-8' if gb == 1 else 'Mi-17'
                    with_dt = data.get('with_dt', 0)
                    sum_dt = data.get('sum_dt', 0)
                    
                    if state == 'operations':
                        status = "✅ ожидаемо"
                    elif with_dt > 0:
                        status = f"📝 дни перехода ({with_dt})"
                    else:
                        status = "✅"
                    
                    lines.append(f"| {state} | {ac_type} | {data.get('total', 0):,} | {with_dt:,} | {sum_dt:,.0f} | {status} |")
                
                lines.append("")
        
        # SNE consistency
        if 'sne_consistency' in inc_stats:
            sne = inc_stats['sne_consistency']
            summary = sne.get('summary', {})
            violations = sne.get('violations', [])
            
            lines.extend([
                f"### Консистентность SNE (Σdt = Δsne)",
                f"",
                f"| Метрика | Значение |",
                f"|---------|----------|",
                f"| Проверено бортов | {summary.get('total_checked', 0)} |",
                f"| Корректных | {summary.get('ok', 0)} |",
                f"| С расхождением | {summary.get('violations', 0)} |",
                f"",
            ])
            
            if violations:
                lines.extend([
                    f"**Расхождения (WARNING):**",
                    f"",
                    f"| AC | Тип | sne_start | sne_end | Δsne | Σdt | Разница |",
                    f"|----|-----|-----------|---------|------|-----|---------|",
                ])
                display_viols = violations if no_limit else violations[:10]
                for v in display_viols:
                    ac_type = 'Mi-8' if v.get('group_by') == 1 else 'Mi-17'
                    lines.append(f"| {v.get('aircraft_number', '?')} | {ac_type} | {v.get('sne_start', 0):,} | {v.get('sne_end', 0):,} | {v.get('delta_sne', 0):,} | {v.get('sum_dt', 0):,} | {v.get('diff', 0):+,} |")
                if not no_limit and len(violations) > 10:
                    lines.append(f"| ... | ... | ... | ... | ... | ... | ещё {len(violations) - 10} |")
                lines.append("")
        
        # PPR reset
        if 'ppr_reset' in inc_stats:
            ppr = inc_stats['ppr_reset']
            summary = ppr.get('summary', {})
            
            lines.extend([
                f"### PPR reset после ремонта",
                f"",
                f"| Тип | Нарушений | Комментарий |",
                f"|-----|-----------|-------------|",
                f"| Mi-8 | {summary.get('mi8_violations', 0)} | ppr должен быть 0 после ремонта |",
                f"| Mi-17 (ожидаемо) | {summary.get('mi17_expected', 0)} | ppr < br (комплектация) |",
                f"| Mi-17 (нарушения) | {summary.get('mi17_violations', 0)} | ppr >= br (реальные ошибки) |",
                f"",
            ])
        
        # Aggregate
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
        
        # Ошибки этой проверки
        if inc_errors:
            lines.extend([
                f"### ❌ Ошибки инкрементов ({len(inc_errors)})",
                f"",
                f"| # | Тип | AC | День | Описание |",
                f"|---|-----|-----|------|----------|",
            ])
            display_errs = inc_errors if no_limit else inc_errors[:20]
            for i, e in enumerate(display_errs, 1):
                msg = e.get('message', '')[:50]
                lines.append(f"| {i} | {e.get('type', '?')} | {e.get('aircraft_number', '?')} | {e.get('day', '?')} | {msg} |")
            if not no_limit and len(inc_errors) > 20:
                lines.append(f"| ... | ... | ... | ... | ещё {len(inc_errors) - 20} |")
            lines.append("")
        
        # Warnings этой проверки
        if inc_warnings:
            lines.extend([
                f"### ⚠️ Предупреждения инкрементов ({len(inc_warnings)})",
                f"",
                f"| # | Тип | AC | Описание |",
                f"|---|-----|-----|----------|",
            ])
            display_warns = inc_warnings if no_limit else inc_warnings[:20]
            for i, w in enumerate(display_warns, 1):
                msg = w.get('message', '')[:60]
                lines.append(f"| {i} | {w.get('type', '?')} | {w.get('aircraft_number', '?')} | {msg} |")
            if not no_limit and len(inc_warnings) > 20:
                lines.append(f"| ... | ... | ... | ещё {len(inc_warnings) - 20} |")
            lines.append("")
    
    # Сбор всех отклонений для сводной таблицы
    all_errors = []
    all_warnings = []
    
    for key, r in results.items():
        for err in r.get('errors', []):
            err['source'] = key
            all_errors.append(err)
        for warn in r.get('warnings', []):
            warn['source'] = key
            all_warnings.append(warn)
    
    # Консолидированная сводка в конце
    lines.extend([
        f"---",
        f"",
        f"## 📊 Консолидированная сводка отклонений",
        f"",
    ])
    
    if not all_errors and not all_warnings:
        lines.extend([
            f"✅ **Отклонений не обнаружено**",
            f"",
            f"Все проверки пройдены без ошибок и предупреждений.",
            f"",
        ])
    else:
        lines.extend([
            f"| Источник | Ошибки | Предупреждения |",
            f"|----------|--------|----------------|",
        ])
        
        source_stats = {}
        for e in all_errors:
            src = e.get('source', 'unknown')
            source_stats.setdefault(src, {'errors': 0, 'warnings': 0})
            source_stats[src]['errors'] += 1
        for w in all_warnings:
            src = w.get('source', 'unknown')
            source_stats.setdefault(src, {'errors': 0, 'warnings': 0})
            source_stats[src]['warnings'] += 1
        
        source_names = {'input_data': 'Входные данные', 'quota': 'Квоты', 'transitions': 'Переходы', 'increments': 'Инкременты'}
        for src, stats in source_stats.items():
            name = source_names.get(src, src)
            lines.append(f"| {name} | {stats['errors']} | {stats['warnings']} |")
        
        lines.extend([
            f"| **ИТОГО** | **{len(all_errors)}** | **{len(all_warnings)}** |",
            f"",
        ])
        
        if strict and all_warnings:
            lines.extend([
                f"🔴 **STRICT режим:** {len(all_warnings)} предупреждений считаются критическими!",
                f"",
            ])
    
    # Расшифровка типов отклонений
    deviation_types = set()
    for e in all_errors:
        deviation_types.add(('ERROR', e.get('type', 'UNKNOWN')))
    for w in all_warnings:
        deviation_types.add(('WARNING', w.get('type', 'UNKNOWN')))
    
    if deviation_types:
        lines.extend([
            f"### Расшифровка типов отклонений",
            f"",
            f"| Уровень | Тип | Описание |",
            f"|---------|-----|----------|",
        ])
        
        type_descriptions = {
            'CRITICAL_DEFICIT': 'Критический недобор квоты (>3 борта)',
            'DEFICIT': 'Недобор квоты (2-3 борта)',
            'FORBIDDEN_TRANSITION': 'Запрещённый переход состояния',
            'STATE_MISMATCH': 'Несоответствие state и transition flag',
            'IMPOSSIBLE_TRANSITION': 'Невозможный переход между днями',
            'REPAIR_DURATION_MISMATCH': 'Отклонение длительности ремонта',
            'PPR_NOT_RESET_MI8': 'Mi-8: ppr > 0 после ремонта',
            'PPR_NOT_RESET_MI17': 'Mi-17: ppr >= br после ремонта',
            'SNE_CONSISTENCY_WARNING': 'Расхождение Σdt ≠ Δsne',
        }
        
        for level, dtype in sorted(deviation_types):
            icon = "❌" if level == 'ERROR' else "⚠️"
            desc = type_descriptions.get(dtype, 'Неизвестный тип')
            lines.append(f"| {icon} {level} | {dtype} | {desc} |")
        
        lines.append("")
    
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description='Оркестратор валидации симуляции')
    parser.add_argument('--version-date', required=True, help='Дата версии (YYYY-MM-DD)')
    parser.add_argument('--strict', action='store_true', 
                        help='Режим нулевой толерантности: warnings = failures')
    parser.add_argument('--no-limit', action='store_true',
                        help='Показывать все отклонения без лимита')
    args = parser.parse_args()
    
    version_date_str = args.version_date
    version_date = get_version_date_int(version_date_str)
    strict = args.strict
    no_limit = args.no_limit
    
    print("\n" + "="*80)
    print(f"ВАЛИДАЦИЯ СИМУЛЯЦИИ: {version_date_str} (version_date={version_date})")
    if strict:
        print("🔴 РЕЖИМ: STRICT (нулевая толерантность к warnings)")
    if no_limit:
        print("📋 РЕЖИМ: NO-LIMIT (полный вывод всех отклонений)")
    print("="*80)
    
    client = get_clickhouse_client()
    
    results = {}
    
    # 0. Входные данные (heli_pandas)
    print("\n" + "="*80)
    print("ЗАПУСК: Валидация входных данных")
    print("="*80)
    input_validator = InputDataValidator(client, version_date)
    results['input_data'] = input_validator.validate()
    
    # 1. Квоты
    print("\n" + "="*80)
    print("ЗАПУСК: Валидация квот")
    print("="*80)
    quota_validator = QuotaValidator(client, version_date)
    results['quota'] = quota_validator.validate()
    
    # 2. Переходы
    print("\n" + "="*80)
    print("ЗАПУСК: Валидация переходов")
    print("="*80)
    transitions_validator = TransitionsValidator(client, version_date)
    results['transitions'] = transitions_validator.run_all()
    
    # 3. Инкременты
    print("\n" + "="*80)
    print("ЗАПУСК: Валидация инкрементов")
    print("="*80)
    increments_validator = IncrementsValidator(client, version_date)
    results['increments'] = increments_validator.run_all()
    
    # Генерация отчёта
    report = generate_report(version_date_str, results, strict=strict, no_limit=no_limit)
    
    # Сохранение отчёта
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    suffix = "_strict" if strict else ""
    report_path = os.path.join(OUTPUT_DIR, f"sim_validation_{version_date_str}{suffix}.md")
    
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write(report)
    
    print("\n" + "="*80)
    print("ИТОГОВАЯ СВОДКА")
    print("="*80)
    
    all_valid = all(r.get('valid', False) for r in results.values())
    total_errors = sum(len(r.get('errors', [])) for r in results.values())
    total_warnings = sum(len(r.get('warnings', [])) for r in results.values())
    total_deviations = total_errors + total_warnings
    
    print(f"\n📄 Отчёт сохранён: {report_path}")
    print(f"❌ Ошибок (CRITICAL): {total_errors}")
    print(f"⚠️ Предупреждений (WARNING): {total_warnings}")
    print(f"📊 Всего отклонений: {total_deviations}")
    
    # В strict режиме warnings также считаются failures
    if strict:
        passed = all_valid and total_warnings == 0
        if not passed and total_warnings > 0:
            print(f"\n🔴 STRICT: {total_warnings} предупреждений считаются failures!")
    else:
        passed = all_valid
    
    if passed:
        print("\n✅ ВАЛИДАЦИЯ СИМУЛЯЦИИ ПРОЙДЕНА")
        sys.exit(0)
    else:
        print("\n❌ ВАЛИДАЦИЯ СИМУЛЯЦИИ НЕ ПРОЙДЕНА")
        sys.exit(1)


if __name__ == '__main__':
    main()








