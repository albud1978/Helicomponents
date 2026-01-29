#!/usr/bin/env python3
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
from datetime import datetime, timedelta
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
    TOLERANCE = 0  # Любое отклонение считается ошибкой
    ALLOW_NEGATIVE_UNTIL_DAY = 180  # До этого дня допускаются минусы
    
    def __init__(self, client, version_date: int):
        self.client = client
        self.version_date = version_date
        self.version_id = self._resolve_version_id()
        self.export_from = self._resolve_export_from()
        self.errors: List[Dict] = []
        self.warnings: List[Dict] = []
        self.stats: Dict = {}
    
    def _resolve_version_id(self) -> int:
        """Берем максимальный version_id для version_date (последний прогон)."""
        row = self.client.execute(
            "SELECT max(version_id) FROM sim_masterv2 WHERE version_date = %(v)s",
            {'v': self.version_date}
        )
        return int(row[0][0] or 0)
    
    def _resolve_export_from(self) -> datetime:
        """Берем окно последней выгрузки по export_timestamp (10 минут от max)."""
        row = self.client.execute(
            "SELECT max(export_timestamp) FROM sim_masterv2 WHERE version_date = %(v)s",
            {'v': self.version_date}
        )
        max_ts = row[0][0]
        if not max_ts:
            return datetime(1970, 1, 1)
        return max_ts - timedelta(minutes=10)
    
    def get_quota_data(self) -> List[Tuple]:
        """Получает данные ops_count vs quota_target по дням"""
        
        # Получаем quota_target и ops_count из sim_masterv2 (D+1 уже учтён при записи quota_target_*)
        data_query = f"""
            SELECT 
                day_u16,
                group_by,
                countIf(state = 'operations' AND intent_state = 2) as ops_count,
                max(quota_target_mi8) as quota_mi8,
                max(quota_target_mi17) as quota_mi17
            FROM sim_masterv2
            WHERE version_date = {self.version_date}
              AND version_id = {self.version_id}
              AND export_timestamp >= toDateTime('{self.export_from.strftime('%Y-%m-%d %H:%M:%S')}')
              AND group_by IN (1, 2)
            GROUP BY day_u16, group_by
            ORDER BY day_u16, group_by
        """
        
        data = self.client.execute(data_query)
        
        # Собираем данные по дням
        by_day = defaultdict(lambda: {'mi8': {'ops': 0, 'quota': 0},
                                      'mi17': {'ops': 0, 'quota': 0}})
        for day, gb, ops_cnt, quota_mi8, quota_mi17 in data:
            if gb == 1:
                by_day[day]['mi8']['ops'] = ops_cnt
                by_day[day]['mi8']['quota'] = quota_mi8
            else:
                by_day[day]['mi17']['ops'] = ops_cnt
                by_day[day]['mi17']['quota'] = quota_mi17
        
        # Объединяем
        result = []
        for day in sorted(by_day.keys()):
            quota_mi8 = by_day[day]['mi8']['quota']
            quota_mi17 = by_day[day]['mi17']['quota']
            ops_mi8 = by_day[day]['mi8']['ops']
            ops_mi17 = by_day[day]['mi17']['ops']
            
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
        print(f"ВАЛИДАЦИЯ КВОТ ДЛЯ version_date={self.version_date}, version_id={self.version_id}")
        print("="*80)
        
        data = self.get_quota_data()
        
        if not data:
            print("❌ Нет данных для валидации")
            return {'valid': False, 'errors': ['Нет данных']}
        
        results = {
            'total_days': len(data),
            'mi8': {'ok': 0, 'allowed_deficit': 0, 'critical': 0},
            'mi17': {'ok': 0, 'allowed_deficit': 0, 'critical': 0},
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
                elif day <= self.ALLOW_NEGATIVE_UNTIL_DAY and delta < 0:
                    results[ac_type]['allowed_deficit'] += 1
                    self.warnings.append({
                        'type': 'DEFICIT_ALLOWED',
                        'day': day,
                        'ac_type': ac_type.upper(),
                        'quota': quota,
                        'ops': ops,
                        'deficit': -delta,
                        'message': f"День {day}: допустимый минус до D={self.ALLOW_NEGATIVE_UNTIL_DAY} ({ac_type.upper()} = {-delta})"
                    })
                else:
                    results[ac_type]['critical'] += 1
                    results['critical_days'].append({
                        'day': day,
                        'type': ac_type.upper(),
                        'quota': quota,
                        'ops': ops,
                        'delta': delta
                    })
                    results['valid'] = False
                    self.errors.append({
                        'type': 'MISMATCH',
                        'day': day,
                        'ac_type': ac_type.upper(),
                        'quota': quota,
                        'ops': ops,
                        'delta': delta,
                        'message': f"День {day}: расхождение {ac_type.upper()} = {delta} (ops={ops}, target={quota})"
                    })
        
        # Вывод результатов
        print(f"\n📊 Всего дней в симуляции: {results['total_days']}")
        
        for ac_type in ['mi8', 'mi17']:
            stats = results[ac_type]
            total = stats['ok'] + stats['allowed_deficit'] + stats['critical']
            
            if total == 0:
                continue
            
            print(f"\n{ac_type.upper()}:")
            print(f"   ✅ Точное соответствие: {stats['ok']} дней ({100*stats['ok']/total:.1f}%)")
            print(f"   🟡 Допустимый минус до D={self.ALLOW_NEGATIVE_UNTIL_DAY}: {stats['allowed_deficit']} дней ({100*stats['allowed_deficit']/total:.1f}%)")
            print(f"   ❌ Любое отклонение вне допуска: {stats['critical']} дней ({100*stats['critical']/total:.1f}%)")
        
        # Первые 5 критичных дней
        if results['critical_days']:
            print(f"\n❌ Критические дни (первые 5):")
            for cd in results['critical_days'][:5]:
                print(f"   День {cd['day']}: {cd['type']} Δ={cd['delta']} (target={cd['quota']}, ops={cd['ops']})")
        
        # Первые 5 предупреждений (допустимые минусы до D=180)
        if self.warnings:
            print(f"\n⚠️ Предупреждения (первые 5):")
            for w in self.warnings[:5]:
                print(f"   День {w['day']}: {w['ac_type']} минус {w['deficit']} (target={w['quota']}, ops={w['ops']})")
        
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

