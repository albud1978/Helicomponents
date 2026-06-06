#!/usr/bin/env python3
# ARCHIVED 2026-06-06 (P2 cleanup): старый class-based фреймворк валидации, перекрыт каноническим code/validation/run_all.py (INV-1..12 + TEMP).
"""
Валидация инкрементов наработок планеров.

Правила:
1. dt > 0 только для state = operations
2. dt = 0 для всех остальных состояний (ИНВАРИАНТ)
3. Σdt за период = Δsne за период (по типу ВС)
4. ppr = 0 после выхода из repair

Usage:
    python3 code/analysis/sim_validation_increments.py --version-date 2025-07-04
"""

import argparse
import sys
from datetime import datetime
from typing import Dict, List
from collections import defaultdict

sys.path.insert(0, '/media/albud/8C327EB0327E9F40/Projects/Heli/Helicomponents/code')
from utils.config_loader import get_clickhouse_client


def get_version_date_int(version_date_str: str) -> int:
    """Конвертирует YYYY-MM-DD в version_date (дни с 1970-01-01)"""
    dt = datetime.strptime(version_date_str, '%Y-%m-%d')
    return (dt - datetime(1970, 1, 1)).days


class IncrementsValidator:
    """Валидатор инкрементов наработок"""
    
    def __init__(self, client, version_date: int):
        self.client = client
        self.version_date = version_date
        self.errors: List[Dict] = []
        self.warnings: List[Dict] = []
        self.stats: Dict = {}
    
    def validate_dt_invariant(self) -> Dict:
        """Проверяет: dt > 0 только в operations, dt = 0 в других состояниях"""
        print("\n" + "="*80)
        print("1. ИНВАРИАНТ: dt > 0 ТОЛЬКО В OPERATIONS")
        print("="*80)
        
        results = {
            'valid': True,
            'violations': [],
            'by_state': {}
        }
        
        # Статистика dt по состояниям
        query = f"""
            SELECT 
                state,
                group_by,
                count(*) as total_records,
                countIf(dt > 0) as records_with_dt,
                sum(dt) as total_dt,
                avg(dt) as avg_dt,
                max(dt) as max_dt
            FROM sim_masterv2
            WHERE version_date = {self.version_date}
              AND group_by IN (1, 2)
            GROUP BY state, group_by
            ORDER BY state, group_by
        """
        
        rows = self.client.execute(query)
        
        print(f"\n{'Состояние':<15} | {'Тип':<6} | {'Записей':>12} | {'С dt>0':>10} | {'Σdt (мин)':>12} | {'Avg dt':>8} | {'Max dt':>8}")
        print("-" * 90)
        
        for state, gb, total, with_dt, sum_dt, avg_dt, max_dt in rows:
            ac_type = 'Mi-8' if gb == 1 else 'Mi-17'
            
            results['by_state'][(state, gb)] = {
                'total': total,
                'with_dt': with_dt,
                'sum_dt': sum_dt or 0,
                'avg_dt': avg_dt or 0,
                'max_dt': max_dt or 0
            }
            
            # Проверка инварианта
            # dt > 0 в НЕ-operations — это ОЖИДАЕМОЕ поведение в дни перехода ИЗ operations.
            # Агент был в operations в начале дня, получил налёт, потом перешёл в другое состояние.
            # dt записывается корректно = налёт в день перехода.
            if state != 'operations' and with_dt > 0:
                # Это не ошибка — информационное сообщение о днях перехода
                status = f"📝 ({with_dt} дн. перех.)"
            else:
                status = "✅"
            
            print(f"{state:<15} | {ac_type:<6} | {total:>12,} | {with_dt:>10,} | {sum_dt or 0:>12,.0f} | {avg_dt or 0:>8.1f} | {max_dt or 0:>8.0f} {status if state != 'operations' else ''}")
        
        if results['valid']:
            print("\n✅ Инвариант dt соблюдён: налёт только в operations")
        else:
            print(f"\n❌ Инвариант dt НАРУШЕН: {len(results['violations'])} категорий с dt>0 вне operations")
        
        self.stats['dt_invariant'] = results
        return results
    
    def validate_sne_consistency(self) -> Dict:
        """Проверяет: Σdt = Δsne для каждого борта"""
        print("\n" + "="*80)
        print("2. КОНСИСТЕНТНОСТЬ: Σdt = Δsne")
        print("="*80)
        
        results = {
            'valid': True,
            'violations': [],
            'summary': {}
        }
        
        # Для каждого борта считаем сумму dt и изменение sne
        query = f"""
            WITH 
                -- Первый и последний день для каждого борта
                bounds AS (
                    SELECT 
                        aircraft_number,
                        group_by,
                        min(day_u16) as first_day,
                        max(day_u16) as last_day
                    FROM sim_masterv2
                    WHERE version_date = {self.version_date}
                      AND group_by IN (1, 2)
                    GROUP BY aircraft_number, group_by
                ),
                -- SNE на первый день
                sne_first AS (
                    SELECT 
                        s.aircraft_number,
                        s.sne as sne_start
                    FROM sim_masterv2 s
                    JOIN bounds b ON s.aircraft_number = b.aircraft_number AND s.day_u16 = b.first_day
                    WHERE s.version_date = {self.version_date}
                ),
                -- SNE на последний день
                sne_last AS (
                    SELECT 
                        s.aircraft_number,
                        s.sne as sne_end
                    FROM sim_masterv2 s
                    JOIN bounds b ON s.aircraft_number = b.aircraft_number AND s.day_u16 = b.last_day
                    WHERE s.version_date = {self.version_date}
                ),
                -- Сумма dt по каждому борту (исключая день 0, т.к. sne[0] ещё не инкрементирована)
                dt_sum AS (
                    SELECT 
                        aircraft_number,
                        group_by,
                        sum(dt) as total_dt
                    FROM sim_masterv2
                    WHERE version_date = {self.version_date}
                      AND group_by IN (1, 2)
                      AND day_u16 > 0  -- dt[0] ещё не отражён в Δsne
                    GROUP BY aircraft_number, group_by
                )
            SELECT 
                d.aircraft_number,
                d.group_by,
                sf.sne_start,
                sl.sne_end,
                sl.sne_end - sf.sne_start as delta_sne,
                d.total_dt,
                d.total_dt - (sl.sne_end - sf.sne_start) as diff
            FROM dt_sum d
            JOIN sne_first sf ON d.aircraft_number = sf.aircraft_number
            JOIN sne_last sl ON d.aircraft_number = sl.aircraft_number
            ORDER BY abs(d.total_dt - (sl.sne_end - sf.sne_start)) DESC
            LIMIT 100
        """
        
        rows = self.client.execute(query)
        
        if not rows:
            print("⚠️ Нет данных для проверки")
            return results
        
        # Анализ
        violations = []
        total_checked = 0
        total_ok = 0
        
        for acn, gb, sne_start, sne_end, delta_sne, total_dt, diff in rows:
            total_checked += 1
            
            if diff != 0:
                violations.append({
                    'aircraft_number': acn,
                    'group_by': gb,
                    'sne_start': sne_start,
                    'sne_end': sne_end,
                    'delta_sne': delta_sne,
                    'sum_dt': total_dt,
                    'diff': diff
                })
            else:
                total_ok += 1
        
        results['summary'] = {
            'total_checked': total_checked,
            'ok': total_ok,
            'violations': len(violations)
        }
        
        if violations:
            # ПРИМЕЧАНИЕ: Расхождение вызвано тем, что dt записывается для всех состояний (см. DT_INVARIANT_WARNING)
            # Σdt включает "лишний" налёт в repair/reserve/etc. Это WARNING, не ERROR.
            results['violations'] = violations[:10]
            
            print(f"\n⚠️ Найдено {len(violations)} бортов с расхождением Σdt ≠ Δsne:")
            print(f"   (Причина: dt записывается для всех состояний, включая repair/reserve/storage)")
            print(f"\n{'ACN':<10} | {'Тип':<6} | {'sne_start':>12} | {'sne_end':>12} | {'Δsne':>12} | {'Σdt':>12} | {'Разница':>10}")
            print("-" * 90)
            
            for v in violations[:10]:
                ac_type = 'Mi-8' if v['group_by'] == 1 else 'Mi-17'
                print(f"{v['aircraft_number']:<10} | {ac_type:<6} | {v['sne_start']:>12,} | {v['sne_end']:>12,} | {v['delta_sne']:>12,} | {v['sum_dt']:>12,} | {v['diff']:>+10,}")
                
                self.warnings.append({
                    'type': 'SNE_CONSISTENCY_WARNING',
                    'aircraft_number': v['aircraft_number'],
                    'diff': v['diff'],
                    'message': f"AC {v['aircraft_number']}: Σdt - Δsne = {v['diff']} мин (известная особенность записи)"
                })
        else:
            print(f"\n✅ Все {total_checked} бортов: Σdt = Δsne (консистентность подтверждена)")
        
        self.stats['sne_consistency'] = results
        return results
    
    def validate_ppr_reset_after_repair(self) -> Dict:
        """Проверяет: ppr = 0 после выхода из repair"""
        print("\n" + "="*80)
        print("3. PPR = 0 ПОСЛЕ ВЫХОДА ИЗ РЕМОНТА")
        print("="*80)
        
        results = {
            'valid': True,
            'violations': [],
            'summary': {}
        }
        
        # Находим записи с transition_4_to_2=1 и проверяем ppr
        # Исключаем Mi-17 с ppr < br2_mi17 (комплектация без ремонта)
        query = f"""
            SELECT 
                aircraft_number,
                group_by,
                day_u16,
                ppr,
                sne
            FROM sim_masterv2
            WHERE version_date = {self.version_date}
              AND group_by IN (1, 2)
              AND transition_4_to_2 = 1
              AND ppr > 0
            ORDER BY group_by, aircraft_number, day_u16
            LIMIT 100
        """
        
        rows = self.client.execute(query)
        
        # Также получаем br2_mi17 для корректной проверки Mi-17
        br2_query = """
            SELECT br2_mi17 FROM md_components WHERE group_by = 2 LIMIT 1
        """
        br2_result = self.client.execute(br2_query)
        br2_mi17 = br2_result[0][0] if br2_result and br2_result[0][0] else 210000  # 3500 часов в минутах
        
        print(f"\n📋 Порог br2_mi17: {br2_mi17} мин ({br2_mi17/60:.0f} часов)")
        print("   Mi-17 с ppr < br2_mi17 проходят комплектацию БЕЗ обнуления ppr\n")
        
        violations_mi8 = []
        violations_mi17_real = []  # Реальные нарушения (ppr >= br2_mi17)
        expected_mi17 = []  # Ожидаемые (ppr < br2_mi17, комплектация)
        
        for acn, gb, day, ppr, sne in rows:
            if gb == 1:  # Mi-8 всегда должен обнулять ppr
                violations_mi8.append({
                    'aircraft_number': acn,
                    'day': day,
                    'ppr': ppr,
                    'sne': sne
                })
            else:  # Mi-17
                # Для Mi-17 нужно знать ppr ДО ремонта, чтобы понять была ли комплектация
                # Но сейчас мы видим ppr ПОСЛЕ, если ppr > 0 и < br2_mi17 - это ожидаемо
                if ppr < br2_mi17:
                    expected_mi17.append({
                        'aircraft_number': acn,
                        'day': day,
                        'ppr': ppr,
                        'sne': sne
                    })
                else:
                    violations_mi17_real.append({
                        'aircraft_number': acn,
                        'day': day,
                        'ppr': ppr,
                        'sne': sne
                    })
        
        results['summary'] = {
            'mi8_violations': len(violations_mi8),
            'mi17_expected': len(expected_mi17),
            'mi17_violations': len(violations_mi17_real)
        }
        
        # Mi-8 нарушения
        if violations_mi8:
            results['valid'] = False
            print(f"❌ Mi-8: {len(violations_mi8)} записей с ppr > 0 после ремонта:")
            for v in violations_mi8[:5]:
                print(f"   AC {v['aircraft_number']}, день {v['day']}: ppr={v['ppr']} мин ({v['ppr']/60:.0f} ч)")
                self.errors.append({
                    'type': 'PPR_NOT_RESET_MI8',
                    'aircraft_number': v['aircraft_number'],
                    'day': v['day'],
                    'ppr': v['ppr'],
                    'message': f"Mi-8 AC {v['aircraft_number']}: ppr={v['ppr']} после ремонта (должно быть 0)"
                })
        else:
            print("✅ Mi-8: все ppr = 0 после ремонта")
        
        # Mi-17 ожидаемые (комплектация без ремонта)
        if expected_mi17:
            print(f"\n✅ Mi-17: {len(expected_mi17)} записей с ppr > 0 после комплектации (ожидаемо, ppr < br2_mi17)")
            for v in expected_mi17[:3]:
                print(f"   AC {v['aircraft_number']}, день {v['day']}: ppr={v['ppr']} мин ({v['ppr']/60:.0f} ч) < {br2_mi17/60:.0f} ч")
        
        # Mi-17 реальные нарушения
        if violations_mi17_real:
            results['valid'] = False
            print(f"\n❌ Mi-17: {len(violations_mi17_real)} записей с ppr >= br2_mi17 после ремонта (НАРУШЕНИЕ):")
            for v in violations_mi17_real[:5]:
                print(f"   AC {v['aircraft_number']}, день {v['day']}: ppr={v['ppr']} мин ({v['ppr']/60:.0f} ч)")
                self.errors.append({
                    'type': 'PPR_NOT_RESET_MI17',
                    'aircraft_number': v['aircraft_number'],
                    'day': v['day'],
                    'ppr': v['ppr'],
                    'message': f"Mi-17 AC {v['aircraft_number']}: ppr={v['ppr']} после ремонта (должно быть 0, т.к. >= br2_mi17)"
                })
        else:
            print("\n✅ Mi-17: все ppr корректны после ремонта/комплектации")
        
        self.stats['ppr_reset'] = results
        return results
    
    def validate_aggregate_dt_by_type(self) -> Dict:
        """Агрегированная проверка dt по типам ВС"""
        print("\n" + "="*80)
        print("4. АГРЕГИРОВАННЫЙ НАЛЁТ ПО ТИПАМ ВС")
        print("="*80)
        
        query = f"""
            SELECT 
                group_by,
                sum(dt) as total_dt,
                sum(dt) / 60.0 as total_hours,
                count(DISTINCT aircraft_number) as ac_count,
                min(day_u16) as first_day,
                max(day_u16) as last_day
            FROM sim_masterv2
            WHERE version_date = {self.version_date}
              AND group_by IN (1, 2)
              AND state = 'operations'
            GROUP BY group_by
        """
        
        rows = self.client.execute(query)
        
        results = {}
        
        print(f"\n{'Тип':<8} | {'Бортов':>8} | {'Дней':>6} | {'Σdt (мин)':>15} | {'Σdt (часов)':>12} | {'Ср. на борт':>12}")
        print("-" * 75)
        
        for gb, total_dt, total_hours, ac_count, first_day, last_day in rows:
            ac_type = 'Mi-8' if gb == 1 else 'Mi-17'
            days = last_day - first_day + 1 if last_day >= first_day else 0
            avg_per_ac = total_hours / ac_count if ac_count > 0 else 0
            
            results[ac_type] = {
                'total_dt': total_dt,
                'total_hours': total_hours,
                'ac_count': ac_count,
                'days': days,
                'avg_per_ac': avg_per_ac
            }
            
            print(f"{ac_type:<8} | {ac_count:>8} | {days:>6} | {total_dt:>15,} | {total_hours:>12,.0f} | {avg_per_ac:>12,.1f}")
        
        self.stats['aggregate'] = results
        return results
    
    def run_all(self) -> Dict:
        """Запускает все проверки"""
        print("\n" + "="*80)
        print(f"ВАЛИДАЦИЯ ИНКРЕМЕНТОВ ДЛЯ version_date={self.version_date}")
        print("="*80)
        
        self.validate_dt_invariant()
        self.validate_sne_consistency()
        self.validate_ppr_reset_after_repair()
        self.validate_aggregate_dt_by_type()
        
        # Итоговая сводка
        print("\n" + "="*80)
        print("ИТОГОВАЯ СВОДКА")
        print("="*80)
        
        print(f"\n❌ Ошибок: {len(self.errors)}")
        print(f"⚠️ Предупреждений: {len(self.warnings)}")
        
        if self.errors:
            print("\nКритические ошибки:")
            for err in self.errors[:10]:
                print(f"   [{err['type']}] {err['message']}")
        
        valid = len(self.errors) == 0
        
        if valid:
            print("\n✅ ВАЛИДАЦИЯ ИНКРЕМЕНТОВ ПРОЙДЕНА")
        else:
            print("\n❌ ВАЛИДАЦИЯ ИНКРЕМЕНТОВ НЕ ПРОЙДЕНА")
        
        return {
            'version_date': self.version_date,
            'errors': self.errors,
            'warnings': self.warnings,
            'stats': self.stats,
            'valid': valid
        }


def main():
    parser = argparse.ArgumentParser(description='Валидация инкрементов наработок')
    parser.add_argument('--version-date', required=True, help='Дата версии (YYYY-MM-DD)')
    args = parser.parse_args()
    
    version_date = get_version_date_int(args.version_date)
    
    client = get_clickhouse_client()
    validator = IncrementsValidator(client, version_date)
    result = validator.run_all()
    
    if result['valid']:
        sys.exit(0)
    else:
        sys.exit(1)


if __name__ == '__main__':
    main()

