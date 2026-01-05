#!/usr/bin/env python3
"""
Выгрузка агрегатов с превышением SNE/PPR в состоянии ИСПРАВНЫЙ.

Критическая ошибка данных — требует ручного разбора.

Usage:
    python3 code/analysis/export_over_limit_serviceable.py --version-date 2025-07-04
    python3 code/analysis/export_over_limit_serviceable.py --version-date 2025-12-30
"""

import argparse
import sys
from datetime import datetime
import pandas as pd

sys.path.insert(0, '/media/albud/8C327EB0327E9F40/Projects/Heli/Helicomponents/code')
from utils.config_loader import get_clickhouse_client


def get_version_date_int(version_date_str: str) -> int:
    """Конвертирует YYYY-MM-DD в days since 1970-01-01"""
    dt = datetime.strptime(version_date_str, '%Y-%m-%d')
    return (dt - datetime(1970, 1, 1)).days


def export_over_limit_serviceable(version_date_str: str) -> str:
    """Выгружает агрегаты с превышением в Excel."""
    
    client = get_clickhouse_client()
    version_date = get_version_date_int(version_date_str)
    
    print(f"\n{'='*70}")
    print(f"ВЫГРУЗКА ИСПРАВНЫХ С ПРЕВЫШЕНИЕМ SNE/PPR: {version_date_str}")
    print(f"{'='*70}")
    
    # Запрос — агрегаты с превышением SNE > LL или PPR > OH в состоянии ИСПРАВНЫЙ
    sql = f"""
    SELECT 
        h.partno as partno,
        h.serialno as serialno,
        h.ac_typ as ac_type,
        h.location as location,
        h.condition as condition,
        h.sne as sne_min,
        h.ppr as ppr_min,
        CASE 
            WHEN bitAnd(h.ac_type_mask, 32) > 0 THEN m.ll_mi8 
            WHEN bitAnd(h.ac_type_mask, 64) > 0 THEN m.ll_mi17 
            ELSE 0 
        END as ll_min,
        CASE 
            WHEN bitAnd(h.ac_type_mask, 32) > 0 THEN m.oh_mi8 
            WHEN bitAnd(h.ac_type_mask, 64) > 0 THEN m.oh_mi17 
            ELSE 0 
        END as oh_min,
        h.sne / 60.0 as sne_hours,
        h.ppr / 60.0 as ppr_hours,
        CASE 
            WHEN bitAnd(h.ac_type_mask, 32) > 0 THEN m.ll_mi8 / 60.0 
            WHEN bitAnd(h.ac_type_mask, 64) > 0 THEN m.ll_mi17 / 60.0 
            ELSE 0 
        END as ll_hours,
        CASE 
            WHEN bitAnd(h.ac_type_mask, 32) > 0 THEN m.oh_mi8 / 60.0 
            WHEN bitAnd(h.ac_type_mask, 64) > 0 THEN m.oh_mi17 / 60.0 
            ELSE 0 
        END as oh_hours,
        CASE 
            WHEN bitAnd(h.ac_type_mask, 32) > 0 AND h.sne > m.ll_mi8 AND m.ll_mi8 > 0 THEN 'SNE > LL'
            WHEN bitAnd(h.ac_type_mask, 64) > 0 AND h.sne > m.ll_mi17 AND m.ll_mi17 > 0 THEN 'SNE > LL'
            ELSE ''
        END as sne_issue,
        CASE 
            WHEN bitAnd(h.ac_type_mask, 32) > 0 AND h.ppr > m.oh_mi8 AND m.oh_mi8 > 0 THEN 'PPR > OH'
            WHEN bitAnd(h.ac_type_mask, 64) > 0 AND h.ppr > m.oh_mi17 AND m.oh_mi17 > 0 THEN 'PPR > OH'
            ELSE ''
        END as ppr_issue,
        h.group_by as group_by,
        m.partno as md_partno,
        h.owner as owner,
        h.mfg_date as mfg_date
    FROM heli_pandas h
    LEFT JOIN md_components m ON h.partseqno_i = m.partno_comp
    WHERE h.version_date = toDate({version_date})
      AND h.group_by > 2  -- только агрегаты
      AND h.condition = 'ИСПРАВНЫЙ'  -- только исправные!
      AND (
        -- Превышение SNE над LL
        (bitAnd(h.ac_type_mask, 32) > 0 AND h.sne > m.ll_mi8 AND m.ll_mi8 > 0)
        OR 
        (bitAnd(h.ac_type_mask, 64) > 0 AND h.sne > m.ll_mi17 AND m.ll_mi17 > 0)
        OR
        -- Превышение PPR над OH
        (bitAnd(h.ac_type_mask, 32) > 0 AND h.ppr > m.oh_mi8 AND m.oh_mi8 > 0)
        OR 
        (bitAnd(h.ac_type_mask, 64) > 0 AND h.ppr > m.oh_mi17 AND m.oh_mi17 > 0)
      )
    ORDER BY h.group_by, h.partno, h.serialno
    """
    
    rows = client.execute(sql)
    
    if not rows:
        print("✅ Нет исправных агрегатов с превышением!")
        return None
    
    # Создаём DataFrame
    columns = [
        'partno', 'serialno', 'ac_type', 'location', 'condition',
        'sne_min', 'ppr_min', 'll_min', 'oh_min',
        'sne_hours', 'ppr_hours', 'll_hours', 'oh_hours',
        'sne_issue', 'ppr_issue', 'group_by', 'md_partno', 'owner', 'mfg_date'
    ]
    
    df = pd.DataFrame(rows, columns=columns)
    
    print(f"\n📊 Найдено {len(df)} исправных агрегатов с превышением")
    
    # Статистика по типам проблем
    sne_issues = len(df[df['sne_issue'] != ''])
    ppr_issues = len(df[df['ppr_issue'] != ''])
    both_issues = len(df[(df['sne_issue'] != '') & (df['ppr_issue'] != '')])
    
    print(f"\n   Превышение SNE > LL: {sne_issues}")
    print(f"   Превышение PPR > OH: {ppr_issues}")
    print(f"   Обе проблемы: {both_issues}")
    
    # Статистика по group_by
    print(f"\n   По группам номенклатуры:")
    for gb, cnt in df.groupby('group_by').size().items():
        print(f"      group_by={gb}: {cnt}")
    
    # Сохраняем в Excel
    output_path = f'/media/albud/8C327EB0327E9F40/Projects/Heli/Helicomponents/output/over_limit_serviceable_{version_date_str}.xlsx'
    
    df.to_excel(output_path, index=False, sheet_name='Over_Limit_Serviceable')
    
    print(f"\n✅ Выгружено в: {output_path}")
    
    return output_path


def main():
    parser = argparse.ArgumentParser(description='Выгрузка исправных с превышением SNE/PPR')
    parser.add_argument('--version-date', required=True, help='Дата версии (YYYY-MM-DD)')
    args = parser.parse_args()
    
    export_over_limit_serviceable(args.version_date)


if __name__ == '__main__':
    main()

