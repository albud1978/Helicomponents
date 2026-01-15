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
    
    # Сбор всех отклонений
    all_errors = []
    all_warnings = []
    
    for key, r in results.items():
        for err in r.get('errors', []):
            err['source'] = key
            all_errors.append(err)
        for warn in r.get('warnings', []):
            warn['source'] = key
            all_warnings.append(warn)
    
    # Лимит вывода (0 = без лимита)
    limit = 0 if no_limit else 50
    
    # Детали ошибок
    if all_errors:
        lines.extend([
            f"## ❌ Детали ошибок ({len(all_errors)})",
            f"",
            f"| # | Источник | Тип | Сообщение |",
            f"|---|----------|-----|-----------|",
        ])
        
        display_errors = all_errors if limit == 0 else all_errors[:limit]
        for i, err in enumerate(display_errors, 1):
            msg = err.get('message', '')
            # Не обрезаем сообщение если no_limit
            if not no_limit and len(msg) > 80:
                msg = msg[:77] + "..."
            lines.append(f"| {i} | {err['source']} | {err['type']} | {msg} |")
        
        if limit > 0 and len(all_errors) > limit:
            lines.append(f"| ... | ... | ... | ещё {len(all_errors) - limit} ошибок (используйте --no-limit) |")
        
        lines.append("")
    
    # Детали предупреждений
    if all_warnings:
        lines.extend([
            f"## ⚠️ Детали предупреждений ({len(all_warnings)})",
            f"",
            f"| # | Источник | Тип | Сообщение |",
            f"|---|----------|-----|-----------|",
        ])
        
        display_warnings = all_warnings if limit == 0 else all_warnings[:limit]
        for i, warn in enumerate(display_warnings, 1):
            msg = warn.get('message', '')
            if not no_limit and len(msg) > 80:
                msg = msg[:77] + "..."
            lines.append(f"| {i} | {warn['source']} | {warn['type']} | {msg} |")
        
        if limit > 0 and len(all_warnings) > limit:
            lines.append(f"| ... | ... | ... | ещё {len(all_warnings) - limit} предупреждений (используйте --no-limit) |")
        
        lines.append("")
    
    # Если нет отклонений
    if not all_errors and not all_warnings:
        lines.extend([
            f"## ✅ Отклонений не обнаружено",
            f"",
            f"Все проверки пройдены без ошибок и предупреждений.",
            f"",
        ])
    
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








