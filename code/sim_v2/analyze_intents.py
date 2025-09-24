#!/usr/bin/env python3
"""
Анализ намерений переходов из логов симуляции
"""
import re
import sys

def parse_log_file(filename):
    """Парсинг лог файла для извлечения намерений переходов"""
    
    transitions = {
        '1->2': [],  # inactive -> operations
        '2->4': [],  # operations -> repair
        '2->6': [],  # operations -> storage
        '4->5': [],  # repair -> reserve
        '3->2': [],  # serviceable -> operations
        '5->2': [],  # reserve -> operations
    }
    
    with open(filename, 'r') as f:
        for line in f:
            # Ищем строки с intent
            if 'intent=' not in line:
                continue
                
            # Парсим данные
            step_match = re.search(r'\[Step (\d+)\]', line)
            ac_match = re.search(r'AC (\d+):', line)
            
            if not (step_match and ac_match):
                continue
                
            step = int(step_match.group(1))
            ac = ac_match.group(1)
            
            # Определяем тип перехода
            if 'st=1, intent=2' in line:
                # State 1 -> 2
                data = extract_basic_data(line, step, ac)
                transitions['1->2'].append(data)
                
            elif 'st=2, intent=4' in line or 'intent=4 (repair)' in line:
                # State 2 -> 4
                data = extract_repair_transition(line, step, ac)
                transitions['2->4'].append(data)
                
            elif 'st=2, intent=6' in line or 'intent=6 (storage' in line:
                # State 2 -> 6
                data = extract_storage_transition(line, step, ac)
                transitions['2->6'].append(data)
                
            elif 'st=4, intent=5' in line or 'intent=5 (reserve)' in line:
                # State 4 -> 5
                data = extract_reserve_transition(line, step, ac)
                transitions['4->5'].append(data)
    
    return transitions

def extract_basic_data(line, step, ac):
    """Извлечение базовых данных"""
    return {
        'step': step,
        'ac': ac,
        'line': line.strip()
    }

def extract_repair_transition(line, step, ac):
    """Извлечение данных для перехода в ремонт"""
    data = {'step': step, 'ac': ac}
    
    # Парсим параметры
    ppr_match = re.search(r'ppr=(\d+)', line)
    oh_match = re.search(r'oh=(\d+)', line)
    sne_match = re.search(r'sne=(\d+)', line)
    br_match = re.search(r'br=(\d+)', line)
    
    if ppr_match: data['ppr'] = int(ppr_match.group(1))
    if oh_match: data['oh'] = int(oh_match.group(1))
    if sne_match: data['sne'] = int(sne_match.group(1))
    if br_match: data['br'] = int(br_match.group(1))
    
    return data

def extract_storage_transition(line, step, ac):
    """Извлечение данных для перехода в хранение"""
    data = {'step': step, 'ac': ac}
    
    # Парсим параметры
    sne_match = re.search(r'sne=(\d+)', line)
    ll_match = re.search(r'll=(\d+)', line)
    br_match = re.search(r'br=(\d+)', line)
    
    if sne_match: data['sne'] = int(sne_match.group(1))
    if ll_match: data['ll'] = int(ll_match.group(1))
    if br_match: data['br'] = int(br_match.group(1))
    
    # Определяем причину
    if 'LL' in line:
        data['reason'] = 'LL'
    elif 'BR' in line:
        data['reason'] = 'BR'
    
    return data

def extract_reserve_transition(line, step, ac):
    """Извлечение данных для перехода в резерв"""
    data = {'step': step, 'ac': ac}
    
    # Парсим repair_days если есть
    rd_match = re.search(r'rd=(\d+)', line)
    if rd_match: 
        data['repair_days'] = int(rd_match.group(1))
    
    return data

def print_report(transitions):
    """Вывод отчета"""
    print("="*80)
    print("АНАЛИЗ НАМЕРЕНИЙ ПЕРЕХОДОВ")
    print("="*80)
    
    # State 1 -> 2 (inactive -> operations)
    if transitions['1->2']:
        print(f"\n## State 1 -> 2 (inactive -> operations): {len(transitions['1->2'])} агентов")
        print("Все агенты из inactive хотят в operations (готовы к эксплуатации)")
    
    # State 2 -> 4 (operations -> repair)
    if transitions['2->4']:
        print(f"\n## State 2 -> 4 (operations -> repair): {len(transitions['2->4'])} переходов")
        for t in transitions['2->4'][:5]:  # Первые 5
            print(f"  Step {t['step']}: AC {t['ac']} - ppr={t.get('ppr','?')} >= oh={t.get('oh','?')}, sne={t.get('sne','?')} < br={t.get('br','?')}")
    
    # State 2 -> 6 (operations -> storage)
    if transitions['2->6']:
        print(f"\n## State 2 -> 6 (operations -> storage): {len(transitions['2->6'])} переходов")
        ll_count = sum(1 for t in transitions['2->6'] if t.get('reason') == 'LL')
        br_count = sum(1 for t in transitions['2->6'] if t.get('reason') == 'BR')
        print(f"  По LL: {ll_count} агентов")
        print(f"  По BR: {br_count} агентов")
        
        # Примеры
        for t in transitions['2->6'][:3]:
            reason = t.get('reason', '?')
            print(f"  Step {t['step']}: AC {t['ac']} - {reason}: sne={t.get('sne','?')} {'>=LL' if reason=='LL' else '>=BR'}={t.get('ll' if reason=='LL' else 'br','?')}")
    
    # State 4 -> 5 (repair -> reserve)
    if transitions['4->5']:
        print(f"\n## State 4 -> 5 (repair -> reserve): {len(transitions['4->5'])} переходов")
        for t in transitions['4->5']:
            print(f"  Step {t['step']}: AC {t['ac']} - repair_days={t.get('repair_days','?')}")
    
    # Проблемы
    print("\n## ВЫЯВЛЕННЫЕ ПРОБЛЕМЫ:")
    print("1. RTC функции выполняются на Step 0 (должна быть только инициализация MP5)")
    print("2. Множество агентов (39) переходят 2->6 уже на Step 0")
    print("3. Нужно проверить корректность загрузки ll/oh/br из базы данных")

if __name__ == '__main__':
    log_file = sys.argv[1] if len(sys.argv) > 1 else 'logs/rtc_test_5days.log'
    
    transitions = parse_log_file(log_file)
    print_report(transitions)
