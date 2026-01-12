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


def generate_report(version_date_str: str, results: Dict) -> str:
    """Генерирует MD отчёт"""
    
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    lines = [
        f"# Отчёт валидации симуляции",
        f"",
        f"**Дата отчёта:** {now}",
        f"**Датасет:** {version_date_str}",
        f"",
        f"## Сводка",
        f"",
    ]
    
    # Общий статус
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
    
    # Детали по каждой проверке
    
    # 1. Квоты
    if 'quota' in results:
        lines.extend([
            f"## 1. Валидация квот",
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
                        f"| Категория | Дней | % |",
                        f"|-----------|------|---|",
                        f"| Точное соответствие | {s.get('ok', 0)} | {100*s.get('ok',0)/total:.1f}% |",
                        f"| Отклонение ±1 | {s.get('minor', 0)} | {100*s.get('minor',0)/total:.1f}% |",
                        f"| Недобор 2-3 | {s.get('deficit', 0)} | {100*s.get('deficit',0)/total:.1f}% |",
                        f"| Критичный >3 | {s.get('critical', 0)} | {100*s.get('critical',0)/total:.1f}% |",
                        f"| Избыток | {s.get('excess', 0)} | {100*s.get('excess',0)/total:.1f}% |",
                        f"",
                    ])
    
    # 2. Переходы
    if 'transitions' in results:
        lines.extend([
            f"## 2. Валидация переходов",
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
                    status = "✅" if data['allowed'] else "❌"
                    lines.append(f"| {data['from']}→{data['to']} | {data['count']:,} | {data['mi8']:,} | {data['mi17']:,} | {status} |")
                
                lines.append("")
        
        if 'repair_duration' in trans_stats:
            repair = trans_stats['repair_duration']
            if 'summary' in repair:
                lines.extend([
                    f"### Длительность ремонта",
                    f"",
                    f"| Тип | Ремонтов | Норматив | Min | Max | Avg | Корректных |",
                    f"|-----|----------|----------|-----|-----|-----|------------|",
                ])
                
                for ac_type, s in repair['summary'].items():
                    lines.append(f"| {ac_type} | {s['total_repairs']} | {s['expected_duration']} дн. | {s['min_duration']} | {s['max_duration']} | {s['avg_duration']:.1f} | {s['correct']}/{s['total_repairs']} |")
                
                lines.append("")
    
    # 3. Инкременты
    if 'increments' in results:
        lines.extend([
            f"## 3. Валидация инкрементов",
            f"",
        ])
        
        inc_stats = results['increments'].get('stats', {})
        
        if 'dt_invariant' in inc_stats:
            inv = inc_stats['dt_invariant']
            if inv.get('valid', False):
                lines.append("✅ Инвариант dt соблюдён: налёт только в operations")
            else:
                lines.append(f"❌ Инвариант dt НАРУШЕН: {len(inv.get('violations', []))} категорий")
            lines.append("")
        
        if 'sne_consistency' in inc_stats:
            sne = inc_stats['sne_consistency']
            summary = sne.get('summary', {})
            if sne.get('valid', False):
                lines.append(f"✅ Консистентность Σdt = Δsne подтверждена ({summary.get('ok', 0)} бортов)")
            else:
                lines.append(f"❌ Расхождение Σdt ≠ Δsne: {summary.get('violations', 0)} бортов")
            lines.append("")
        
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
    
    # Детали ошибок
    all_errors = []
    for key, r in results.items():
        for err in r.get('errors', []):
            err['source'] = key
            all_errors.append(err)
    
    if all_errors:
        lines.extend([
            f"## Детали ошибок",
            f"",
            f"| Источник | Тип | Сообщение |",
            f"|----------|-----|-----------|",
        ])
        
        for err in all_errors[:20]:
            lines.append(f"| {err['source']} | {err['type']} | {err['message'][:60]}... |")
        
        if len(all_errors) > 20:
            lines.append(f"| ... | ... | ещё {len(all_errors) - 20} ошибок |")
        
        lines.append("")
    
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description='Оркестратор валидации симуляции')
    parser.add_argument('--version-date', required=True, help='Дата версии (YYYY-MM-DD)')
    args = parser.parse_args()
    
    version_date_str = args.version_date
    version_date = get_version_date_int(version_date_str)
    
    print("\n" + "="*80)
    print(f"ВАЛИДАЦИЯ СИМУЛЯЦИИ: {version_date_str} (version_date={version_date})")
    print("="*80)
    
    client = get_clickhouse_client()
    
    results = {}
    
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
    report = generate_report(version_date_str, results)
    
    # Сохранение отчёта
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    report_path = os.path.join(OUTPUT_DIR, f"sim_validation_{version_date_str}.md")
    
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
        print("\n✅ ВАЛИДАЦИЯ СИМУЛЯЦИИ ПРОЙДЕНА")
        sys.exit(0)
    else:
        print("\n❌ ВАЛИДАЦИЯ СИМУЛЯЦИИ НЕ ПРОЙДЕНА")
        sys.exit(1)


if __name__ == '__main__':
    main()





