#!/usr/bin/env python3
# ARCHIVED 2026-06-06 (P2 cleanup): старый class-based фреймворк валидации, перекрыт каноническим code/validation/run_all.py (INV-1..12 + TEMP).
"""
Валидация соответствия количества планеров в эксплуатации квотам.

Проверяет:
1. ops_count vs quota_target для каждого дня
2. Допустимое отклонение ±1 на переходных днях
3. Критичный недобор: quota_target - ops_count > 3

Usage:
    python3 code/analysis/sim_validation_quota.py --version-date 2025-07-04
"""

import argparse
import sys
from datetime import datetime
from typing import Dict, List, Tuple
from collections import defaultdict

sys.path.insert(0, '/media/albud/8C327EB0327E9F40/Projects/Heli/Helicomponents/code')
from utils.config_loader import get_clickhouse_client


def get_version_date_int(version_date_str: str) -> int:
    """Конвертирует YYYY-MM-DD в version_date (дни с 1970-01-01)"""
    dt = datetime.strptime(version_date_str, '%Y-%m-%d')
    return (dt - datetime(1970, 1, 1)).days


class QuotaValidator:
    """Валидатор квот ops_count vs quota_target"""
    
    # Пороги
    TOLERANCE = 1  # ±1 допустимо
    CRITICAL_DEFICIT = 3  # Критичный недобор
    
    def __init__(self, client, version_date: int):
        self.client = client
        self.version_date = version_date
        self.errors: List[Dict] = []
        self.warnings: List[Dict] = []
        self.stats: Dict = {}
    
    def get_quota_data(self) -> List[Tuple]:
        """Получает данные ops_count vs quota_target по дням"""
        
        # Получаем quota_target из flight_program_ac
        # ВАЖНО: Симуляция использует target СЛЕДУЮЩЕГО дня (D+1) для демоута!
        # Поэтому для корректного сравнения используем min(target[D], target[D+1])
        # Это учитывает превентивное квотирование на границах периодов
        quota_query = f"""
            WITH base AS (
                SELECT 
                    toInt32(dates - version_date) as day_index,
                    ops_counter_mi8 as t8,
                    ops_counter_mi17 as t17,
                    leadInFrame(ops_counter_mi8, 1, ops_counter_mi8) OVER (ORDER BY dates) as t8_next,
                    leadInFrame(ops_counter_mi17, 1, ops_counter_mi17) OVER (ORDER BY dates) as t17_next
                FROM flight_program_ac
                WHERE version_date = toDate({self.version_date})
            )
            SELECT 
                day_index,
                least(t8, t8_next) as quota_mi8,
                least(t17, t17_next) as quota_mi17
            FROM base
            ORDER BY day_index
        """
        
        quota_data = self.client.execute(quota_query)
        quota_map = {row[0]: (row[1], row[2]) for row in quota_data}
        
        # Получаем ops_count из sim_masterv2
        ops_query = f"""
            SELECT 
                day_u16,
                group_by,
                countIf(state = 'operations') as ops_count
            FROM sim_masterv2
            WHERE version_date = {self.version_date}
              AND group_by IN (1, 2)
            GROUP BY day_u16, group_by
            ORDER BY day_u16, group_by
        """
        
        ops_data = self.client.execute(ops_query)
        
        # Собираем данные по дням
        ops_by_day = defaultdict(lambda: {'mi8': 0, 'mi17': 0})
        for day, gb, cnt in ops_data:
            if gb == 1:
                ops_by_day[day]['mi8'] = cnt
            else:
                ops_by_day[day]['mi17'] = cnt
        
        # Объединяем
        result = []
        for day in sorted(ops_by_day.keys()):
            quota_mi8, quota_mi17 = quota_map.get(day, (0, 0))
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
        print("\n" + "="*80)
        print(f"ВАЛИДАЦИЯ КВОТ ДЛЯ version_date={self.version_date}")
        print("="*80)
        
        data = self.get_quota_data()
        
        if not data:
            print("❌ Нет данных для валидации")
            return {'valid': False, 'errors': ['Нет данных']}
        
        results = {
            'total_days': len(data),
            'mi8': {'ok': 0, 'minor': 0, 'deficit': 0, 'critical': 0, 'excess': 0},
            'mi17': {'ok': 0, 'minor': 0, 'deficit': 0, 'critical': 0, 'excess': 0},
            'critical_days': [],
            'valid': True
        }
        
        # Анализ по дням
        for row in data:
            day = row['day']
            
            for ac_type in ['mi8', 'mi17']:
                quota = row[f'quota_{ac_type}']
                ops = row[f'ops_{ac_type}']
                delta = row[f'delta_{ac_type}']
                
                if quota == 0:
                    continue  # Нет квоты на этот тип
                
                if delta == 0:
                    results[ac_type]['ok'] += 1
                elif abs(delta) <= self.TOLERANCE:
                    results[ac_type]['minor'] += 1
                elif delta < -self.CRITICAL_DEFICIT:
                    results[ac_type]['critical'] += 1
                    results['critical_days'].append({
                        'day': day,
                        'type': ac_type.upper(),
                        'quota': quota,
                        'ops': ops,
                        'deficit': -delta
                    })
                    results['valid'] = False
                    self.errors.append({
                        'type': 'CRITICAL_DEFICIT',
                        'day': day,
                        'ac_type': ac_type.upper(),
                        'quota': quota,
                        'ops': ops,
                        'deficit': -delta,
                        'message': f"День {day}: критический недобор {ac_type.upper()} = {-delta} (ops={ops}, target={quota})"
                    })
                elif delta < 0:
                    results[ac_type]['deficit'] += 1
                    self.warnings.append({
                        'type': 'DEFICIT',
                        'day': day,
                        'ac_type': ac_type.upper(),
                        'deficit': -delta,
                        'message': f"День {day}: недобор {ac_type.upper()} = {-delta}"
                    })
                else:
                    results[ac_type]['excess'] += 1
        
        # Вывод результатов
        print(f"\n📊 Всего дней в симуляции: {results['total_days']}")
        
        for ac_type in ['mi8', 'mi17']:
            stats = results[ac_type]
            total = stats['ok'] + stats['minor'] + stats['deficit'] + stats['critical'] + stats['excess']
            
            if total == 0:
                continue
            
            print(f"\n{ac_type.upper()}:")
            print(f"   ✅ Точное соответствие: {stats['ok']} дней ({100*stats['ok']/total:.1f}%)")
            print(f"   ⚪ Отклонение ±1: {stats['minor']} дней ({100*stats['minor']/total:.1f}%)")
            print(f"   ⚠️ Недобор 2-3: {stats['deficit']} дней ({100*stats['deficit']/total:.1f}%)")
            print(f"   ❌ Критичный >3: {stats['critical']} дней ({100*stats['critical']/total:.1f}%)")
            print(f"   📈 Избыток: {stats['excess']} дней ({100*stats['excess']/total:.1f}%)")
        
        # Первые 5 критичных дней
        if results['critical_days']:
            print(f"\n❌ Критические дни (первые 5):")
            for cd in results['critical_days'][:5]:
                print(f"   День {cd['day']}: {cd['type']} недобор {cd['deficit']} (target={cd['quota']}, ops={cd['ops']})")
        
        # Сводка
        print("\n" + "="*80)
        print("ИТОГОВАЯ СВОДКА")
        print("="*80)
        
        if results['valid']:
            print("✅ ВАЛИДАЦИЯ КВОТ ПРОЙДЕНА")
        else:
            print(f"❌ ВАЛИДАЦИЯ КВОТ НЕ ПРОЙДЕНА: {len(self.errors)} критических дней")
        
        print(f"   Ошибок: {len(self.errors)}")
        print(f"   Предупреждений: {len(self.warnings)}")
        
        self.stats = results
        return {
            'version_date': self.version_date,
            'errors': self.errors,
            'warnings': self.warnings,
            'stats': results,
            'valid': results['valid']
        }


def main():
    parser = argparse.ArgumentParser(description='Валидация квот ops_count vs quota_target')
    parser.add_argument('--version-date', required=True, help='Дата версии (YYYY-MM-DD)')
    args = parser.parse_args()
    
    version_date = get_version_date_int(args.version_date)
    
    client = get_clickhouse_client()
    validator = QuotaValidator(client, version_date)
    result = validator.validate()
    
    if result['valid']:
        sys.exit(0)
    else:
        sys.exit(1)


if __name__ == '__main__':
    main()

