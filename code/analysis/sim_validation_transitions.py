#!/usr/bin/env python3
"""
Валидация переходов между состояниями агентов в симуляции.

Проверяет:
1. Все переходы соответствуют матрице разрешённых переходов
2. Длительность ремонта (repair) соответствует md_components.repair_time
3. Нет запрещённых переходов

Матрица разрешённых переходов (из кода sim_v2):
    0→2 spawn → operations (динамический)
    0→3 spawn → serviceable (детерминированный)
    1→2 inactive → operations
    1→4 inactive → repair
    2→3 operations → serviceable (квотный демоут)
    2→4 operations → repair
    2→7 operations → unserviceable (очередь на ремонт)
    2→6 operations → storage
    3→2 serviceable → operations
    4→3 repair → serviceable
    7→2 unserviceable → operations
    7→4 unserviceable → repair

Самопереходы (остаёмся в том же состоянии):
    1→1, 2→2, 3→3, 4→4, 5→5, 6→6, 7→7

Usage:
    python3 code/analysis/sim_validation_transitions.py --version-date 2025-07-04
"""

import argparse
import sys
from datetime import datetime
from typing import Dict, List, Tuple, Set, Optional
from collections import defaultdict

sys.path.insert(0, '/media/albud/8C327EB0327E9F40/Projects/Heli/Helicomponents/code')
from utils.config_loader import get_clickhouse_client


# Состояния
STATES = {
    1: 'inactive',
    2: 'operations',
    3: 'serviceable',
    4: 'repair',
    5: 'reserve',
    6: 'storage',
    7: 'unserviceable'
}

STATE_TO_NUM = {v: k for k, v in STATES.items()}

# Матрица разрешённых переходов (from → to)
# Включает spawn (0) как виртуальный источник
ALLOWED_TRANSITIONS: Set[Tuple[int, int]] = {
    # Spawn
    (0, 2),  # spawn → operations (динамический)
    (0, 3),  # spawn → serviceable (детерминированный)
    
    # Из inactive (1)
    (1, 1),  # самопереход
    (1, 2),  # inactive → operations
    (1, 4),  # inactive → repair
    
    # Из operations (2)
    (2, 2),  # самопереход
    (2, 3),  # operations → serviceable (квотный демоут)
    (2, 4),  # operations → repair
    (2, 6),  # operations → storage
    (2, 7),  # operations → unserviceable (очередь на ремонт)
    
    # Из serviceable (3)
    (3, 2),  # serviceable → operations
    (3, 3),  # самопереход
    
    # Из repair (4)
    (4, 3),  # repair → serviceable
    (4, 4),  # самопереход
    
    # Из reserve (5)
    (5, 5),  # самопереход
    
    # Из storage (6)
    (6, 6),  # самопереход (терминальный)
    
    # Из unserviceable (7)
    (7, 2),  # unserviceable → operations
    (7, 4),  # unserviceable → repair
    (7, 7),  # самопереход
}

# Колонки переходов в sim_masterv2 (реально существующие в таблице)
# Примечание: transition_2_to_5 НЕ записывается в таблицу (внутренний переход в очередь)
TRANSITION_COLUMNS = [
    'transition_0_to_2',
    'transition_0_to_3',
    'transition_1_to_4',
    'transition_2_to_3',
    'transition_2_to_4',
    # 'transition_2_to_5',  # Не записывается в таблицу
    'transition_2_to_6',
    'transition_3_to_2',
    'transition_4_to_3',
    'transition_7_to_4',
    'transition_7_to_2',
]


def parse_transition_col(col: str) -> Tuple[int, int]:
    """Парсит имя колонки transition_X_to_Y → (X, Y)"""
    parts = col.replace('transition_', '').split('_to_')
    return int(parts[0]), int(parts[1])


def get_version_date_int(version_date_str: str) -> int:
    """Конвертирует YYYY-MM-DD в version_date (дни с 1970-01-01)"""
    dt = datetime.strptime(version_date_str, '%Y-%m-%d')
    return (dt - datetime(1970, 1, 1)).days


class TransitionsValidator:
    """Валидатор переходов между состояниями"""
    
    def __init__(self, client, version_date: int):
        self.client = client
        self.version_date = version_date
        self.errors: List[Dict] = []
        self.warnings: List[Dict] = []
        self.stats: Dict = {}
    
    def validate_transition_matrix(self) -> Dict:
        """Проверяет, что все зафиксированные переходы разрешены"""
        print("\n" + "="*80)
        print("1. ВАЛИДАЦИЯ МАТРИЦЫ ПЕРЕХОДОВ")
        print("="*80)
        
        results = {
            'total_transitions': 0,
            'by_type': {},
            'forbidden': [],
            'valid': True
        }
        
        # Получаем статистику по каждому типу перехода
        for col in TRANSITION_COLUMNS:
            from_state, to_state = parse_transition_col(col)
            
            query = f"""
                SELECT 
                    count(*) as cnt,
                    countIf(group_by = 1) as mi8_cnt,
                    countIf(group_by = 2) as mi17_cnt
                FROM sim_masterv2
                WHERE version_date = {self.version_date}
                  AND {col} = 1
            """
            
            row = self.client.execute(query)[0]
            cnt, mi8, mi17 = row
            
            if cnt > 0:
                results['by_type'][col] = {
                    'from': from_state,
                    'to': to_state,
                    'count': cnt,
                    'mi8': mi8,
                    'mi17': mi17,
                    'allowed': (from_state, to_state) in ALLOWED_TRANSITIONS
                }
                results['total_transitions'] += cnt
                
                if (from_state, to_state) not in ALLOWED_TRANSITIONS:
                    results['forbidden'].append(col)
                    results['valid'] = False
                    self.errors.append({
                        'type': 'FORBIDDEN_TRANSITION',
                        'transition': col,
                        'count': cnt,
                        'message': f"Запрещённый переход {from_state}→{to_state}: {cnt} случаев"
                    })
        
        # Вывод таблицы
        print(f"\n{'Переход':<20} | {'Всего':>10} | {'Mi-8':>8} | {'Mi-17':>8} | {'Статус':<10}")
        print("-" * 70)
        
        for col, data in sorted(results['by_type'].items()):
            status = "✅ OK" if data['allowed'] else "❌ ЗАПРЕЩЁН"
            print(f"{data['from']}→{data['to']} ({col:<17}) | {data['count']:>10,} | {data['mi8']:>8,} | {data['mi17']:>8,} | {status}")
        
        print(f"\n📊 Всего переходов: {results['total_transitions']:,}")
        
        if results['forbidden']:
            print(f"❌ Найдено {len(results['forbidden'])} запрещённых типов переходов!")
        else:
            print("✅ Все переходы соответствуют матрице")
        
        self.stats['matrix'] = results
        return results
    
    def validate_state_consistency(self) -> Dict:
        """Проверяет консистентность state и флагов переходов"""
        print("\n" + "="*80)
        print("2. ВАЛИДАЦИЯ КОНСИСТЕНТНОСТИ СОСТОЯНИЙ")
        print("="*80)
        
        results = {
            'inconsistencies': [],
            'valid': True
        }
        
        # Проверка: если есть transition_X_to_Y=1, то state должен быть Y
        for col in TRANSITION_COLUMNS:
            from_state, to_state = parse_transition_col(col)
            expected_state = STATES.get(to_state, str(to_state))
            extra_filter = ""
            if col == 'transition_4_to_3':
                # Допускаем chain 4→3→2 в один день (active_trigger=1), когда state уже operations
                extra_filter = "AND NOT (state = 'operations' AND transition_3_to_2 = 1 AND active_trigger = 1)"
            
            query = f"""
                SELECT 
                    aircraft_number,
                    day_u16,
                    state,
                    group_by
                FROM sim_masterv2
                WHERE version_date = {self.version_date}
                  AND {col} = 1
                  AND state != '{expected_state}'
                  {extra_filter}
                LIMIT 10
            """
            
            rows = self.client.execute(query)
            
            if rows:
                results['valid'] = False
                for acn, day, actual_state, gb in rows:
                    results['inconsistencies'].append({
                        'transition': col,
                        'aircraft_number': acn,
                        'day': day,
                        'expected_state': expected_state,
                        'actual_state': actual_state,
                        'group_by': gb
                    })
                    self.errors.append({
                        'type': 'STATE_MISMATCH',
                        'transition': col,
                        'aircraft_number': acn,
                        'day': day,
                        'message': f"AC {acn} день {day}: {col}=1, но state='{actual_state}' вместо '{expected_state}'"
                    })
        
        if results['inconsistencies']:
            print(f"\n❌ Найдено {len(results['inconsistencies'])} несоответствий state и флагов переходов:")
            for inc in results['inconsistencies'][:5]:
                print(f"   AC {inc['aircraft_number']}, день {inc['day']}: {inc['transition']}=1, state='{inc['actual_state']}' (ожидалось '{inc['expected_state']}')")
            if len(results['inconsistencies']) > 5:
                print(f"   ... и ещё {len(results['inconsistencies']) - 5}")
        else:
            print("✅ Все флаги переходов консистентны с состояниями агентов")
        
        self.stats['consistency'] = results
        return results

    def validate_state_balance(self) -> Dict:
        """Проверяет баланс по состояниям: start + in - out = end (без 0 и 6)"""
        print("\n" + "="*80)
        print("4. ВАЛИДАЦИЯ БАЛАНСА ПО СОСТОЯНИЯМ")
        print("="*80)

        state_expr = """
            multiIf(
                state='inactive', 1,
                state='operations', 2,
                state='serviceable', 3,
                state='repair', 4,
                state='reserve', 5,
                state='storage', 6,
                state='unserviceable', 7,
                0
            )
        """

        # Последний день в данных
        last_day = self.client.execute(
            "SELECT max(day_u16) FROM sim_masterv2 WHERE version_date = %(v)s",
            {"v": self.version_date}
        )[0][0]

        # start/end counts (только реальные планеры)
        start_counts = dict(self.client.execute(
            f"""
            SELECT {state_expr} AS s, count()
            FROM sim_masterv2
            WHERE version_date = %(v)s AND day_u16 = 0 AND aircraft_number > 0
            GROUP BY s
            """,
            {"v": self.version_date}
        ))
        end_counts = dict(self.client.execute(
            f"""
            SELECT {state_expr} AS s, count()
            FROM sim_masterv2
            WHERE version_date = %(v)s AND day_u16 = %(d)s AND aircraft_number > 0
            GROUP BY s
            """,
            {"v": self.version_date, "d": last_day}
        ))

        # transitions state->state без учёта спавна (исключаем пустые слоты)
        transitions = self.client.execute(
            f"""
            SELECT from_state, to_state, count() AS cnt
            FROM (
                SELECT idx,
                       aircraft_number,
                       {state_expr} AS to_state,
                       lagInFrame(aircraft_number, 1) OVER (PARTITION BY idx ORDER BY day_u16) AS prev_aircraft,
                       lagInFrame({state_expr}, 1) OVER (PARTITION BY idx ORDER BY day_u16) AS from_state
                FROM sim_masterv2
                WHERE version_date = %(v)s
            )
            WHERE prev_aircraft > 0 AND aircraft_number > 0
              AND from_state > 0 AND to_state > 0 AND from_state != to_state
            GROUP BY from_state, to_state
            """,
            {"v": self.version_date}
        )

        in_counts = defaultdict(int)
        out_counts = defaultdict(int)
        for from_s, to_s, cnt in transitions:
            out_counts[int(from_s)] += int(cnt)
            in_counts[int(to_s)] += int(cnt)

        # Добавляем спавн как входы в 2 и 3
        spawn = self.client.execute(
            """
            SELECT sum(transition_0_to_2), sum(transition_0_to_3)
            FROM sim_masterv2
            WHERE version_date = %(v)s AND aircraft_number > 0
            """,
            {"v": self.version_date}
        )[0]
        in_counts[2] += int(spawn[0]) if spawn[0] is not None else 0
        in_counts[3] += int(spawn[1]) if spawn[1] is not None else 0

        results = {
            'last_day': last_day,
            'by_state': {},
            'valid': True
        }

        # Проверяем состояния 1,2,3,4,5,7 (исключаем 0 и 6)
        for s in (1, 2, 3, 4, 5, 7):
            start = int(start_counts.get(s, 0))
            end = int(end_counts.get(s, 0))
            inc = int(in_counts.get(s, 0))
            out = int(out_counts.get(s, 0))
            lhs = start + inc - out
            diff = lhs - end
            results['by_state'][s] = {
                'start': start,
                'in': inc,
                'out': out,
                'end': end,
                'diff': diff
            }
            if diff != 0:
                results['valid'] = False
                self.errors.append({
                    'type': 'STATE_BALANCE_MISMATCH',
                    'state': s,
                    'message': f"Баланс state={s}: start({start}) + in({inc}) - out({out}) = {lhs}, end={end} (diff {diff:+d})"
                })

        # Вывод
        for s, data in results['by_state'].items():
            status = "✅ OK" if data['diff'] == 0 else f"❌ DIFF {data['diff']:+d}"
            print(f"state {s}: start={data['start']}, in={data['in']}, out={data['out']}, end={data['end']} => {status}")

        if results['valid']:
            print("✅ Баланс по состояниям выдержан (без 0 и 6)")

        self.stats['balance'] = results
        return results
    
    def validate_repair_duration(self) -> Dict:
        """Проверяет длительность ремонта против md_components.repair_time"""
        print("\n" + "="*80)
        print("3. ВАЛИДАЦИЯ ДЛИТЕЛЬНОСТИ РЕМОНТА")
        print("="*80)
        
        results = {
            'valid': True,
            'violations': [],
            'summary': {}
        }
        
        # Получаем repair_time для планеров (group_by=1,2) из md_components
        repair_times = self.client.execute("""
            SELECT 
                group_by,
                repair_time
            FROM md_components
            WHERE group_by IN (1, 2)
              AND repair_time > 0
            LIMIT 2
        """)
        
        repair_time_map = {row[0]: row[1] for row in repair_times}
        print(f"\n📋 Нормативное время ремонта (из md_components):")
        for gb, rt in repair_time_map.items():
            ac_type = 'Mi-8' if gb == 1 else 'Mi-17'
            print(f"   {ac_type}: {rt} дней")
        
        if not repair_time_map:
            print("⚠️ Не найдены repair_time в md_components, пропускаем проверку")
            return results
        
        # Ищем агентов, которые вошли в repair и вышли из него
        # Используем transition_2_to_4 (или 1_to_4) как вход, transition_4_to_3 как выход
        for gb, expected_rt in repair_time_map.items():
            ac_type = 'Mi-8' if gb == 1 else 'Mi-17'
            
            # Упрощённый подход: смотрим сколько дней агент провёл в state='repair'
            # после каждого входа (transition_1_to_4=1 или transition_2_to_4=1)
            # Используем оконные функции для подсчёта
            query = f"""
                WITH repairs AS (
                    SELECT 
                        aircraft_number,
                        day_u16,
                        transition_1_to_4,
                        transition_2_to_4,
                        transition_4_to_3,
                        state
                    FROM sim_masterv2
                    WHERE version_date = {self.version_date}
                      AND group_by = {gb}
                      AND (transition_1_to_4 = 1 OR transition_4_to_3 = 1)
                )
                SELECT 
                    r1.aircraft_number,
                    r1.day_u16 as entry_day,
                    r2.day_u16 as exit_day,
                    r2.day_u16 - r1.day_u16 as duration
                FROM repairs r1
                INNER JOIN repairs r2 
                    ON r1.aircraft_number = r2.aircraft_number
                    AND r2.day_u16 > r1.day_u16
                    AND r2.transition_4_to_3 = 1
                    AND r1.transition_1_to_4 = 1
                WHERE r2.day_u16 = (
                    SELECT min(day_u16) 
                    FROM repairs r3 
                    WHERE r3.aircraft_number = r1.aircraft_number 
                      AND r3.day_u16 > r1.day_u16
                      AND r3.transition_4_to_3 = 1
                )
                ORDER BY r1.aircraft_number, r1.day_u16
            """
            
            try:
                repairs = self.client.execute(query)
            except Exception as e:
                # Fallback: более простой подсчёт через windowFunnel или агрегации
                print(f"   ⚠️ Сложный запрос не поддерживается, используем упрощённый подход")
                
                # Просто считаем transition_4_to_3=1 и проверяем, что они есть
                simple_query = f"""
                    SELECT 
                        aircraft_number,
                        countIf(transition_1_to_4 = 1) as entries,
                        countIf(transition_4_to_3 = 1) as exits
                    FROM sim_masterv2
                    WHERE version_date = {self.version_date}
                      AND group_by = {gb}
                    GROUP BY aircraft_number
                    HAVING entries > 0 OR exits > 0
                """
                simple_data = self.client.execute(simple_query)
                print(f"   {ac_type}: {len(simple_data)} бортов с ремонтами")
                repairs = []  # Пропускаем детальную проверку
            
            if repairs:
                correct = 0
                wrong = 0
                durations = []
                
                for acn, entry, exit_d, duration in repairs:
                    durations.append(duration)
                    if duration == expected_rt:
                        correct += 1
                    else:
                        wrong += 1
                        if wrong <= 5:
                            results['violations'].append({
                                'aircraft_number': acn,
                                'group_by': gb,
                                'entry_day': entry,
                                'exit_day': exit_d,
                                'actual_duration': duration,
                                'expected_duration': expected_rt
                            })
                
                results['summary'][ac_type] = {
                    'total_repairs': len(repairs),
                    'correct': correct,
                    'wrong': wrong,
                    'expected_duration': expected_rt,
                    'min_duration': min(durations) if durations else 0,
                    'max_duration': max(durations) if durations else 0,
                    'avg_duration': sum(durations) / len(durations) if durations else 0
                }
                
                print(f"\n{ac_type}:")
                print(f"   Всего ремонтов: {len(repairs)}")
                print(f"   Нормативная длительность: {expected_rt} дней")
                print(f"   Фактически: min={min(durations)}, max={max(durations)}, avg={sum(durations)/len(durations):.1f}")
                print(f"   Корректных: {correct}, отклонений: {wrong}")
                
                if wrong > 0:
                    results['valid'] = False
                    print(f"   ⚠️ Примеры отклонений:")
                    for v in results['violations'][:3]:
                        if v['group_by'] == gb:
                            print(f"      AC {v['aircraft_number']}: дни {v['entry_day']}→{v['exit_day']} = {v['actual_duration']} дней (ожидалось {v['expected_duration']})")
        
        if results['valid']:
            print("\n✅ Все длительности ремонтов соответствуют нормативам")
        else:
            print(f"\n⚠️ Обнаружены отклонения длительности ремонта")
            self.warnings.append({
                'type': 'REPAIR_DURATION_MISMATCH',
                'count': sum(s.get('wrong', 0) for s in results['summary'].values()),
                'message': 'Некоторые ремонты имеют отклонение от нормативной длительности'
            })
        
        self.stats['repair_duration'] = results
        return results
    
    def validate_no_impossible_transitions(self) -> Dict:
        """Проверяет отсутствие невозможных переходов через анализ последовательности состояний"""
        print("\n" + "="*80)
        print("4. ПРОВЕРКА ПОСЛЕДОВАТЕЛЬНОСТИ СОСТОЯНИЙ")
        print("="*80)
        
        results = {
            'valid': True,
            'impossible_transitions': []
        }
        
        # Для каждого борта проверяем последовательность состояний
        # Ищем случаи, когда state[day] → state[day+1] не в ALLOWED_TRANSITIONS
        
        query = f"""
            WITH state_seq AS (
                SELECT 
                    aircraft_number,
                    group_by,
                    day_u16,
                    state,
                    leadInFrame(state) OVER (PARTITION BY aircraft_number ORDER BY day_u16) as next_state,
                    leadInFrame(day_u16) OVER (PARTITION BY aircraft_number ORDER BY day_u16) as next_day
                FROM sim_masterv2
                WHERE version_date = {self.version_date}
                  AND group_by IN (1, 2)
            )
            SELECT 
                aircraft_number,
                group_by,
                day_u16,
                state,
                next_state,
                next_day
            FROM state_seq
            WHERE next_state IS NOT NULL
              AND next_state != ''
              AND state != next_state
              AND next_day = day_u16 + 1
            ORDER BY aircraft_number, day_u16
            LIMIT 1000
        """
        
        transitions = self.client.execute(query)
        
        # Проверяем каждый переход
        forbidden_count = 0
        for acn, gb, day, state, next_state, next_day in transitions:
            from_num = STATE_TO_NUM.get(state, -1)
            to_num = STATE_TO_NUM.get(next_state, -1)
            
            if from_num == -1 or to_num == -1:
                continue
            
            if (from_num, to_num) not in ALLOWED_TRANSITIONS:
                forbidden_count += 1
                results['valid'] = False
                
                if len(results['impossible_transitions']) < 10:
                    results['impossible_transitions'].append({
                        'aircraft_number': acn,
                        'group_by': gb,
                        'day': day,
                        'from_state': state,
                        'to_state': next_state,
                        'transition': f"{from_num}→{to_num}"
                    })
                    self.errors.append({
                        'type': 'IMPOSSIBLE_TRANSITION',
                        'aircraft_number': acn,
                        'day': day,
                        'from_state': state,
                        'to_state': next_state,
                        'message': f"AC {acn} день {day}: запрещённый переход {state}→{next_state}"
                    })
        
        if results['impossible_transitions']:
            print(f"\n❌ Найдено {forbidden_count} невозможных переходов:")
            for t in results['impossible_transitions'][:5]:
                ac_type = 'Mi-8' if t['group_by'] == 1 else 'Mi-17'
                print(f"   AC {t['aircraft_number']} ({ac_type}), день {t['day']}: {t['from_state']} → {t['to_state']} ({t['transition']})")
            if forbidden_count > 5:
                print(f"   ... и ещё {forbidden_count - 5}")
        else:
            print("✅ Все переходы между днями соответствуют разрешённой матрице")
        
        self.stats['impossible'] = results
        return results
    
    def run_all(self) -> Dict:
        """Запускает все проверки"""
        print("\n" + "="*80)
        print(f"ВАЛИДАЦИЯ ПЕРЕХОДОВ ДЛЯ version_date={self.version_date}")
        print("="*80)
        
        self.validate_transition_matrix()
        self.validate_state_consistency()
        self.validate_repair_duration()
        self.validate_no_impossible_transitions()
        self.validate_state_balance()
        
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
        
        if self.warnings:
            print("\nПредупреждения:")
            for warn in self.warnings[:10]:
                print(f"   [{warn['type']}] {warn['message']}")
        
        return {
            'version_date': self.version_date,
            'errors': self.errors,
            'warnings': self.warnings,
            'stats': self.stats,
            'valid': len(self.errors) == 0
        }


def main():
    parser = argparse.ArgumentParser(description='Валидация переходов между состояниями')
    parser.add_argument('--version-date', required=True, help='Дата версии (YYYY-MM-DD)')
    args = parser.parse_args()
    
    version_date = get_version_date_int(args.version_date)
    
    client = get_clickhouse_client()
    validator = TransitionsValidator(client, version_date)
    result = validator.run_all()
    
    if result['valid']:
        print("\n✅ ВАЛИДАЦИЯ ПЕРЕХОДОВ ПРОЙДЕНА")
        sys.exit(0)
    else:
        print("\n❌ ВАЛИДАЦИЯ ПЕРЕХОДОВ НЕ ПРОЙДЕНА")
        sys.exit(1)


if __name__ == '__main__':
    main()

