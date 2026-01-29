#!/usr/bin/env python3
"""
Анализ планеров, выходящих из оборота по достижению LL (Lifecycle Limit).

Выгружает информацию о всех переходах 2→6 (operations → storage) с параметрами
на момент выхода: sne, ppr, ll, oh, br, дата, причина выхода.

Usage:
    python3 code/analysis/export_planers_ll_retirement.py --version-date 2025-07-04
    python3 code/analysis/export_planers_ll_retirement.py --version-date 2025-12-30
    python3 code/analysis/export_planers_ll_retirement.py --all  # оба датасета

Выход:
    output/planers_ll_retirement_<version_date>.xlsx
"""

import argparse
import sys
from datetime import datetime
from pathlib import Path
import pandas as pd

# Портируемость: корень репозитория относительно файла
PROJECT_ROOT = Path(__file__).resolve().parents[2]
CODE_DIR = PROJECT_ROOT / "code"
if str(CODE_DIR) not in sys.path:
    sys.path.insert(0, str(CODE_DIR))

from utils.config_loader import get_clickhouse_client


def get_version_date_int(version_date_str: str) -> int:
    """Конвертирует YYYY-MM-DD в days since 1970-01-01"""
    dt = datetime.strptime(version_date_str, '%Y-%m-%d')
    return (dt - datetime(1970, 1, 1)).days


def export_ll_retirement(version_date_str: str) -> str:
    """
    Выгружает планеры, вышедшие из оборота по LL, в Excel.
    
    Returns:
        Путь к созданному Excel файлу
    """
    client = get_clickhouse_client()
    version_date = get_version_date_int(version_date_str)
    
    print(f"\n{'='*70}")
    print(f"АНАЛИЗ ВЫХОДА ПЛАНЕРОВ ПО LL: {version_date_str}")
    print(f"{'='*70}")
    
    # Запрос: все переходы 2→6 с параметрами
    sql = f"""
    SELECT 
        aircraft_number,
        CASE WHEN group_by = 1 THEN 'Mi-8' ELSE 'Mi-17' END as ac_type,
        day_u16 as day_sim,
        day_date,
        sne / 60.0 as sne_hours,
        ppr / 60.0 as ppr_hours,
        ll / 60.0 as ll_hours,
        oh / 60.0 as oh_hours,
        br / 60.0 as br_hours,
        sne as sne_min,
        ppr as ppr_min,
        ll as ll_min,
        oh as oh_min,
        br as br_min,
        -- Причина выхода
        CASE 
            WHEN sne >= ll AND ll > 0 THEN 'SNE >= LL'
            WHEN sne >= br AND br > 0 THEN 'SNE >= BR'
            WHEN ppr >= oh AND sne >= br AND br > 0 THEN 'PPR >= OH && SNE >= BR'
            ELSE 'Другое'
        END as retirement_reason,
        -- Остаток до лимитов
        (ll - sne) / 60.0 as remaining_to_ll_hours,
        (br - sne) / 60.0 as remaining_to_br_hours,
        (oh - ppr) / 60.0 as remaining_to_oh_hours,
        -- % использования
        CASE WHEN ll > 0 THEN round(sne * 100.0 / ll, 1) ELSE 0 END as ll_used_pct,
        CASE WHEN br > 0 THEN round(sne * 100.0 / br, 1) ELSE 0 END as br_used_pct,
        CASE WHEN oh > 0 THEN round(ppr * 100.0 / oh, 1) ELSE 0 END as oh_used_pct,
        group_by,
        idx
    FROM sim_masterv2
    WHERE version_date = {version_date}
      AND transition_2_to_6 = 1
      AND group_by IN (1, 2)  -- только планеры
    ORDER BY day_u16, aircraft_number
    """
    
    rows = client.execute(sql)
    
    if not rows:
        print("⚠️ Нет переходов 2→6 в данном датасете")
        return None
    
    # Создаём DataFrame
    columns = [
        'aircraft_number', 'ac_type', 'day_sim', 'day_date',
        'sne_hours', 'ppr_hours', 'll_hours', 'oh_hours', 'br_hours',
        'sne_min', 'ppr_min', 'll_min', 'oh_min', 'br_min',
        'retirement_reason',
        'remaining_to_ll_hours', 'remaining_to_br_hours', 'remaining_to_oh_hours',
        'll_used_pct', 'br_used_pct', 'oh_used_pct',
        'group_by', 'idx'
    ]
    
    df = pd.DataFrame(rows, columns=columns)
    
    print(f"\n📊 Найдено {len(df)} планеров, вышедших из оборота")
    
    # Статистика по типам
    print(f"\n📋 По типам ВС:")
    for ac_type, cnt in df.groupby('ac_type').size().items():
        print(f"   {ac_type}: {cnt}")
    
    # Статистика по причинам
    print(f"\n📋 По причинам выхода:")
    for reason, cnt in df.groupby('retirement_reason').size().items():
        print(f"   {reason}: {cnt}")
    
    # Временная статистика
    print(f"\n📋 Временные рамки:")
    print(f"   Первый выход: день {df['day_sim'].min()} ({df['day_date'].min()})")
    print(f"   Последний выход: день {df['day_sim'].max()} ({df['day_date'].max()})")
    
    # Статистика по наработке
    print(f"\n📋 Наработка на момент выхода (часы):")
    print(f"   SNE min: {df['sne_hours'].min():,.0f}, max: {df['sne_hours'].max():,.0f}, avg: {df['sne_hours'].mean():,.0f}")
    print(f"   PPR min: {df['ppr_hours'].min():,.0f}, max: {df['ppr_hours'].max():,.0f}, avg: {df['ppr_hours'].mean():,.0f}")
    
    # Использование лимитов
    print(f"\n📋 Использование лимитов (%):")
    print(f"   LL: min {df['ll_used_pct'].min():.1f}%, max {df['ll_used_pct'].max():.1f}%, avg {df['ll_used_pct'].mean():.1f}%")
    print(f"   BR: min {df['br_used_pct'].min():.1f}%, max {df['br_used_pct'].max():.1f}%, avg {df['br_used_pct'].mean():.1f}%")
    
    # Сохраняем в Excel
    output_dir = PROJECT_ROOT / "output"
    output_dir.mkdir(exist_ok=True)
    output_path = output_dir / f"planers_ll_retirement_{version_date_str}.xlsx"
    
    # Создаём Excel с несколькими листами
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        # Основной лист с данными
        df.to_excel(writer, index=False, sheet_name='Retirement_Details')
        
        # Сводка по типам
        summary_type = df.groupby('ac_type').agg({
            'aircraft_number': 'count',
            'sne_hours': ['min', 'max', 'mean'],
            'ppr_hours': ['min', 'max', 'mean'],
            'll_used_pct': ['min', 'max', 'mean'],
            'day_sim': ['min', 'max']
        }).round(1)
        summary_type.columns = ['_'.join(col).strip() for col in summary_type.columns.values]
        summary_type.to_excel(writer, sheet_name='Summary_by_Type')
        
        # Сводка по причинам
        summary_reason = df.groupby('retirement_reason').agg({
            'aircraft_number': 'count',
            'sne_hours': 'mean',
            'll_used_pct': 'mean',
            'br_used_pct': 'mean'
        }).round(1)
        summary_reason.columns = ['count', 'avg_sne_hours', 'avg_ll_used_pct', 'avg_br_used_pct']
        summary_reason.to_excel(writer, sheet_name='Summary_by_Reason')
        
        # Хронология выхода по месяцам (группировка по day_sim // 30)
        df['month_sim'] = df['day_sim'] // 30
        chronology = df.groupby(['month_sim', 'ac_type']).size().unstack(fill_value=0)
        chronology.to_excel(writer, sheet_name='Chronology_Monthly')
    
    print(f"\n✅ Выгружено в: {output_path}")
    
    return str(output_path)


def main():
    parser = argparse.ArgumentParser(
        description='Анализ планеров, выходящих из оборота по LL',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры:
    python3 code/analysis/export_planers_ll_retirement.py --version-date 2025-07-04
    python3 code/analysis/export_planers_ll_retirement.py --version-date 2025-12-30
    python3 code/analysis/export_planers_ll_retirement.py --all
        """
    )
    parser.add_argument('--version-date', help='Дата версии (YYYY-MM-DD)')
    parser.add_argument('--all', action='store_true', help='Обработать оба датасета')
    args = parser.parse_args()
    
    if args.all:
        # Оба датасета
        for vd in ['2025-07-04', '2025-12-30']:
            export_ll_retirement(vd)
    elif args.version_date:
        export_ll_retirement(args.version_date)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == '__main__':
    main()










