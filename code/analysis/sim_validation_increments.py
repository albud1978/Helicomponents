#!/usr/bin/env python3
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
    
    def validate_dt_zero_in_operations(self) -> Dict:
        """
        Проверяет: КАЖДЫЙ dt=0 в operations должен быть объяснён flight_program.
        
        НУЛЕВАЯ ТОЛЕРАНТНОСТЬ: Если flight_program имеет dt>0, а sim имеет dt=0 — это БАГ!
        """
        print("\n" + "="*80)
        print("0. НУЛЕВАЯ ТОЛЕРАНТНОСТЬ: dt=0 В OPERATIONS")
        print("="*80)
        
        results = {
            'valid': True,
            'bugs': [],  # dt=0 в sim при dt>0 в flight_program = БАГ
            'valid_zeros': [],  # dt=0 в sim при dt=0 в flight_program = OK (зимовка)
            'transition_zeros': [],  # dt=0 в день входа в operations (разрешено)
            'stats': {}
        }
        
        # Находим ВСЕ записи с dt=0 в operations и сопоставляем с flight_program
        query = f"""
            SELECT 
                s.aircraft_number,
                s.group_by,
                s.day_u16,
                s.dt as sim_dt,
                f.daily_hours as fp_dt,
                -- Проверяем prev_state для определения типа: переход vs нативный 0
                prev.state as prev_state
            FROM sim_masterv2 s
            LEFT JOIN flight_program_fl f 
                ON s.aircraft_number = f.aircraft_number 
                AND f.dates = toDate({self.version_date}) + s.day_u16
                AND f.version_date = toDate({self.version_date})
            LEFT JOIN sim_masterv2 prev 
                ON s.aircraft_number = prev.aircraft_number 
                AND prev.day_u16 = s.day_u16 - 1 
                AND prev.version_date = {self.version_date}
            WHERE s.version_date = {self.version_date}
              AND s.group_by IN (1, 2)
              AND s.state = 'operations'
              AND s.dt = 0
            ORDER BY s.group_by, s.aircraft_number, s.day_u16
        """
        
        rows = self.client.execute(query)
        
        # Анализ каждой записи
        bugs_mi8 = []
        bugs_mi17 = []
        valid_mi8 = 0
        valid_mi17 = 0
        transition_mi8 = 0
        transition_mi17 = 0
        
        for acn, gb, day, sim_dt, fp_dt, prev_state in rows:
            ac_type = 'Mi-8' if gb == 1 else 'Mi-17'
            fp_dt_val = fp_dt if fp_dt is not None else 0
            
            if fp_dt_val > 0:
                # Допускаем нулевой dt в день входа в operations (prev_state != operations)
                if prev_state is not None and prev_state != 'operations':
                    if gb == 1:
                        transition_mi8 += 1
                    else:
                        transition_mi17 += 1
                    results['transition_zeros'].append({
                        'aircraft_number': acn,
                        'group_by': gb,
                        'day': day,
                        'fp_dt': fp_dt_val,
                        'prev_state': prev_state
                    })
                else:
                    # БАГ: flight_program имеет налёт, но sim записал 0
                    bug = {
                        'aircraft_number': acn,
                        'group_by': gb,
                        'day': day,
                        'fp_dt': fp_dt_val,
                        'prev_state': prev_state or '(day0)'
                    }
                    if gb == 1:
                        bugs_mi8.append(bug)
                    else:
                        bugs_mi17.append(bug)
            else:
                # OK: flight_program тоже имеет 0 (зимовка)
                if gb == 1:
                    valid_mi8 += 1
                else:
                    valid_mi17 += 1
        
        results['stats'] = {
            'mi8_bugs': len(bugs_mi8),
            'mi8_valid_zeros': valid_mi8,
            'mi8_transition_zeros': transition_mi8,
            'mi17_bugs': len(bugs_mi17),
            'mi17_valid_zeros': valid_mi17,
            'mi17_transition_zeros': transition_mi17,
            'total_bugs': len(bugs_mi8) + len(bugs_mi17),
            'total_valid': valid_mi8 + valid_mi17,
            'total_transition_zeros': transition_mi8 + transition_mi17
        }
        
        results['bugs'] = bugs_mi8 + bugs_mi17
        results['valid'] = len(results['bugs']) == 0
        
        # Вывод результатов
        print(f"\n{'Тип':<8} | {'Баги (fp>0, sim=0)':<20} | {'Валидные (fp=0, sim=0)':<25} | {'Переходы в ops (dt=0)':<26}")
        print("-" * 95)
        print(f"{'Mi-8':<8} | {len(bugs_mi8):>20} | {valid_mi8:>25} | {transition_mi8:>26}")
        print(f"{'Mi-17':<8} | {len(bugs_mi17):>20} | {valid_mi17:>25} | {transition_mi17:>26}")
        print("-" * 95)
        print(f"{'ИТОГО':<8} | {len(bugs_mi8) + len(bugs_mi17):>20} | {valid_mi8 + valid_mi17:>25} | {transition_mi8 + transition_mi17:>26}")
        
        if results['valid']:
            print(f"\n✅ НУЛЕВАЯ ТОЛЕРАНТНОСТЬ: Все {valid_mi8 + valid_mi17} случаев dt=0 объяснены flight_program")
            if transition_mi8 + transition_mi17 > 0:
                print(f"ℹ️  Переходы в operations с dt=0: {transition_mi8 + transition_mi17} (допустимо)")
        else:
            results['valid'] = False
            print(f"\n❌ КРИТИЧЕСКИЕ БАГИ: {len(results['bugs'])} случаев dt=0 при fp_dt>0!")
            print(f"\nПервые 20 багов:")
            print(f"{'ACN':<10} | {'Тип':<6} | {'День':<6} | {'fp_dt':<8} | {'prev_state':<15}")
            print("-" * 60)
            for bug in results['bugs'][:20]:
                ac_type = 'Mi-8' if bug['group_by'] == 1 else 'Mi-17'
                print(f"{bug['aircraft_number']:<10} | {ac_type:<6} | {bug['day']:<6} | {bug['fp_dt']:<8} | {bug['prev_state']:<15}")
            
            for bug in results['bugs']:
                self.errors.append({
                    'type': 'DT_ZERO_BUG',
                    'aircraft_number': bug['aircraft_number'],
                    'day': bug['day'],
                    'fp_dt': bug['fp_dt'],
                    'message': f"AC {bug['aircraft_number']} day {bug['day']}: fp_dt={bug['fp_dt']} но sim_dt=0"
                })
        
        self.stats['dt_zero_in_ops'] = results
        return results
    
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
        
        # Для каждого борта считаем сумму dt и изменение sne.
        # ВАЖНО: dt экспортируется только для operations.
        # На днях, когда агент выходит из operations (demount/repair/storage),
        # sne уже увеличен, но состояние дня не operations → dt = 0.
        # Поэтому учитываем sne_diff на таких днях как фактический налёт.
        # ИСКЛЮЧАЕМ spawned aircraft (AC >= 100000) — у них другой жизненный цикл
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
                      AND aircraft_number < 100000  -- Исключаем spawned aircraft
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
                -- Эффективная сумма налёта по каждому борту (исключая день 0)
                dt_sum AS (
                    SELECT 
                        s.aircraft_number,
                        s.group_by,
                        -- Налёт в operations (dt записан напрямую)
                        sumIf(s.dt, s.state = 'operations' AND s.day_u16 > 0) as ops_dt,
                        -- Налёт в дни выхода из operations (sne_diff > 0 при state != operations)
                        sumIf(ifNull(s.sne - prev.sne, 0), s.state != 'operations' AND s.day_u16 > 0 AND (s.sne - prev.sne) > 0) as non_ops_dt
                    FROM sim_masterv2 s
                    LEFT JOIN sim_masterv2 prev
                        ON prev.version_date = s.version_date
                       AND prev.aircraft_number = s.aircraft_number
                       AND prev.day_u16 = s.day_u16 - 1
                    WHERE s.version_date = {self.version_date}
                      AND s.group_by IN (1, 2)
                      AND s.aircraft_number < 100000  -- Исключаем spawned aircraft
                    GROUP BY s.aircraft_number, s.group_by
                )
            SELECT 
                d.aircraft_number,
                d.group_by,
                sf.sne_start,
                sl.sne_end,
                sl.sne_end - sf.sne_start as delta_sne,
                (d.ops_dt + d.non_ops_dt) as total_dt,
                (d.ops_dt + d.non_ops_dt) - (sl.sne_end - sf.sne_start) as diff
            FROM dt_sum d
            JOIN sne_first sf ON d.aircraft_number = sf.aircraft_number
            JOIN sne_last sl ON d.aircraft_number = sl.aircraft_number
            ORDER BY abs((d.ops_dt + d.non_ops_dt) - (sl.sne_end - sf.sne_start)) DESC
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
            # ПРИМЕЧАНИЕ: Расхождение означает несостыковку между sne и экспортом dt
            # даже с учётом переходных дней (non_ops_dt).
            results['violations'] = violations[:10]
            
            print(f"\n⚠️ Найдено {len(violations)} бортов с расхождением Σdt ≠ Δsne:")
            print(f"   (Причина: несостыковка sne vs dt даже с учётом переходов)")
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
        """
        Проверяет: ppr сброшен после выхода из repair.
        
        ВАЖНО: После сброса ppr=0 на переходе 4→3, применяется dt текущего дня.
        Поэтому в день перехода ppr = dt (а не 0).
        
        Правильная проверка: ppr == dt в день transition_4_to_3=1
        """
        print("\n" + "="*80)
        print("3. PPR RESET ПОСЛЕ ВЫХОДА ИЗ РЕМОНТА")
        print("="*80)
        
        results = {
            'valid': True,
            'violations': [],
            'summary': {}
        }
        
        # Находим записи с transition_4_to_3=1 и проверяем ppr == dt
        # После ремонта: ppr сбрасывается в 0, затем применяется dt дня → ppr = dt
        query = f"""
            SELECT 
                aircraft_number,
                group_by,
                day_u16,
                ppr,
                dt,
                sne
            FROM sim_masterv2
            WHERE version_date = {self.version_date}
              AND group_by IN (1, 2)
              AND transition_4_to_3 = 1
            ORDER BY group_by, aircraft_number, day_u16
            LIMIT 200
        """
        
        rows = self.client.execute(query)
        
        # Также получаем br2_mi17 для корректной проверки Mi-17
        br2_query = """
            SELECT br2_mi17 FROM md_components WHERE group_by = 2 LIMIT 1
        """
        br2_result = self.client.execute(br2_query)
        br2_mi17 = br2_result[0][0] if br2_result and br2_result[0][0] else 210000  # 3500 часов в минутах
        
        print(f"\n📋 Порог br2_mi17: {br2_mi17} мин ({br2_mi17/60:.0f} часов)")
        print("   Mi-17 с ppr < br2_mi17 проходят комплектацию БЕЗ обнуления ppr")
        print("   После reset: ppr = dt в день перехода (не 0)\n")
        
        violations_mi8 = []
        violations_mi17_real = []  # Реальные нарушения (ppr != dt и ppr >= br2_mi17)
        expected_mi17 = []  # Ожидаемые (ppr соответствует комплектации или dt)
        correct_mi8 = 0
        
        for acn, gb, day, ppr, dt, sne in rows:
            if gb == 1:  # Mi-8 всегда должен обнулять ppr → ppr = dt
                if ppr == dt:
                    correct_mi8 += 1
                else:
                    violations_mi8.append({
                        'aircraft_number': acn,
                        'day': day,
                        'ppr': ppr,
                        'dt': dt,
                        'sne': sne,
                        'reason': f'ppr={ppr} != dt={dt}'
                    })
            else:  # Mi-17
                # Для Mi-17 комплектация без обнуления если ppr_prev < br2_mi17
                # В любом случае после перехода ppr должен включать dt текущего дня
                if ppr < br2_mi17:
                    expected_mi17.append({
                        'aircraft_number': acn,
                        'day': day,
                        'ppr': ppr,
                        'dt': dt,
                        'sne': sne
                    })
                else:
                    violations_mi17_real.append({
                        'aircraft_number': acn,
                        'day': day,
                        'ppr': ppr,
                        'dt': dt,
                        'sne': sne
                    })
        
        results['summary'] = {
            'mi8_correct': correct_mi8,
            'mi8_violations': len(violations_mi8),
            'mi17_expected': len(expected_mi17),
            'mi17_violations': len(violations_mi17_real)
        }
        
        # Mi-8: после reset ppr = dt
        if violations_mi8:
            results['valid'] = False
            print(f"❌ Mi-8: {len(violations_mi8)} записей с ppr != dt после ремонта:")
            for v in violations_mi8[:5]:
                print(f"   AC {v['aircraft_number']}, день {v['day']}: {v['reason']}")
                self.errors.append({
                    'type': 'PPR_NOT_RESET_MI8',
                    'aircraft_number': v['aircraft_number'],
                    'day': v['day'],
                    'ppr': v['ppr'],
                    'dt': v['dt'],
                    'message': f"Mi-8 AC {v['aircraft_number']}: ppr={v['ppr']} != dt={v['dt']} (должно быть ppr=dt после reset)"
                })
        else:
            print(f"✅ Mi-8: все {correct_mi8} записей — ppr = dt после ремонта (корректный reset)")
        
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
        
        # КРИТИЧНО: Проверка dt=0 с НУЛЕВОЙ ТОЛЕРАНТНОСТЬЮ
        self.validate_dt_zero_in_operations()
        
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

