"""
Скрипт анализа и валидации heli_pandas.

Колонки в heli_pandas:
- ll_mi8, oh_mi8, br_mi8: ресурсы из md_components
- error_flags: битовая маска ошибок валидации

Битовая маска error_flags:
- bit 0 (1):  status 10 - недостаточные данные ll/oh
- bit 1 (2):  status 11 - дата ремонта в прошлом
- bit 2 (4):  status 12 - неисправен при sne=0
- bit 3 (8):  status 13 - превышение ресурса
- bit 4 (16): status 14 - некорректный condition
- bit 5 (32): status 15 - донор при ремонтопригодном

Рабочие статусы (1-6): назначаются если error_flags = 0
Status 0: новый неучтённый случай
"""

import sys
import time
import argparse

sys.path.insert(0, '/media/albud/8C327EB0327E9F40/Projects/Heli/Helicomponents/code')

from utils.config_loader import get_clickhouse_client

# Константы для битовых флагов
FLAG_NO_DATA = 1       # bit 0: status 10 - ll/oh пустые
FLAG_DATE_PAST = 2     # bit 1: status 11 - target_date < version_date
FLAG_SNE_ZERO = 4      # bit 2: status 12 - неисправен при sne=0
FLAG_OVER_LIMIT = 8    # bit 3: status 13 - превышение ресурса
FLAG_BAD_COND = 16     # bit 4: status 14 - некорректный condition
FLAG_EARLY_DONOR = 32  # bit 5: status 15 - донор при ремонтопригодном


def wait_for_mutations(client, table='heli_pandas', timeout=60):
    """Ожидание завершения мутаций таблицы."""
    for i in range(timeout):
        result = client.execute(f"""
            SELECT count(*) FROM system.mutations 
            WHERE table = '{table}' AND is_done = 0
        """)
        if result[0][0] == 0:
            return True
        time.sleep(1)
        if i % 5 == 0:
            print(f"  ... ждём завершения мутаций ({i}с)")
    return False


def update_resources(client):
    """Заполняем ll_mi8, oh_mi8, br_mi8 из md_components через пересоздание данных."""
    print("\n" + "=" * 80)
    print("ЗАПОЛНЕНИЕ РЕСУРСОВ ИЗ MD_COMPONENTS")
    print("=" * 80)
    
    # ClickHouse не поддерживает UPDATE с подзапросами на внешние таблицы
    # Используем INSERT ... SELECT с заменой данных
    
    print("Создаю временную таблицу с обновлёнными данными...")
    
    # Удаляем временную таблицу если есть
    client.execute("DROP TABLE IF EXISTS heli_pandas_temp")
    
    # Создаём временную таблицу с JOIN
    client.execute("""
    CREATE TABLE heli_pandas_temp 
    ENGINE = MergeTree() 
    ORDER BY (version_date, version_id)
    SETTINGS allow_nullable_key = 1
    AS SELECT 
        hp.partno, hp.serialno, hp.ac_typ, hp.location, hp.mfg_date, 
        hp.removal_date, hp.target_date, hp.condition, hp.owner,
        hp.lease_restricted, hp.oh, hp.oh_threshold, hp.ll, hp.sne, hp.ppr,
        hp.version_date, hp.version_id, hp.partseqno_i, hp.psn, hp.address_i,
        hp.ac_type_i, hp.status_id, hp.repair_days, hp.aircraft_number,
        hp.ac_type_mask, hp.group_by,
        md.ll_mi8 AS ll_mi8,
        md.oh_mi8 AS oh_mi8,
        md.br_mi8 AS br_mi8,
        md.ll_mi17 AS ll_mi17,
        md.oh_mi17 AS oh_mi17,
        md.br_mi17 AS br_mi17,
        toUInt8(0) AS error_flags
    FROM heli_pandas hp
    LEFT JOIN md_components md ON hp.partseqno_i = md.partno_comp
    """)
    print("  ✓ Временная таблица создана")
    
    # Проверяем количество
    cnt_temp = client.execute("SELECT count(*) FROM heli_pandas_temp")[0][0]
    cnt_orig = client.execute("SELECT count(*) FROM heli_pandas")[0][0]
    print(f"  Записей: temp={cnt_temp:,}, orig={cnt_orig:,}")
    
    if cnt_temp != cnt_orig:
        print("  ⚠ ВНИМАНИЕ: количество записей не совпадает!")
        client.execute("DROP TABLE heli_pandas_temp")
        return
    
    # Меняем таблицы местами
    print("Меняю таблицы местами...")
    client.execute("RENAME TABLE heli_pandas TO heli_pandas_old, heli_pandas_temp TO heli_pandas")
    print("  ✓ Таблицы переименованы")
    
    # Удаляем старую
    client.execute("DROP TABLE heli_pandas_old")
    print("  ✓ Старая таблица удалена")
    
    # Проверка
    check = client.execute("""
    SELECT 
        countIf(ll_mi8 IS NOT NULL AND ll_mi8 > 0) as has_ll8,
        countIf(ll_mi17 IS NOT NULL AND ll_mi17 > 0) as has_ll17,
        countIf(ll_mi8 > 0 OR ll_mi17 > 0) as has_ll_any,
        countIf(br_mi8 IS NOT NULL OR br_mi17 IS NOT NULL) as has_br,
        count(*) as total
    FROM heli_pandas WHERE group_by >= 1
    """)[0]
    
    print(f"\nРезультат:")
    print(f"  Всего агрегатов: {check[4]:,}")
    print(f"  С ll_mi8 > 0: {check[0]:,} ({check[0]/check[4]*100:.1f}%)")
    print(f"  С ll_mi17 > 0: {check[1]:,} ({check[1]/check[4]*100:.1f}%)")
    print(f"  С ll (mi8 OR mi17): {check[2]:,} ({check[2]/check[4]*100:.1f}%)")
    print(f"  С br: {check[3]:,} ({check[3]/check[4]*100:.1f}%)")


def update_error_flags(client):
    """Вычисляем и заполняем error_flags."""
    print("\n" + "=" * 80)
    print("РАСЧЁТ ERROR_FLAGS")
    print("=" * 80)
    
    # Сначала сбрасываем флаги
    print("Сбрасываю error_flags = 0...")
    client.execute("ALTER TABLE heli_pandas UPDATE error_flags = 0 WHERE 1=1")
    wait_for_mutations(client)
    
    # Устанавливаем каждый флаг отдельно
    flags = [
        (FLAG_NO_DATA, "Status 10: недостаточные данные (ll_mi8 И ll_mi17)",
         "((ll_mi8 IS NULL OR ll_mi8 = 0) AND (ll_mi17 IS NULL OR ll_mi17 = 0))"),
        
        (FLAG_DATE_PAST, "Status 11: дата ремонта в прошлом",
         "(target_date < version_date AND target_date IS NOT NULL)"),
        
        (FLAG_SNE_ZERO, "Status 12: неисправен при sne=0",
         "(condition != 'ИСПРАВНЫЙ' AND sne = 0)"),
        
        (FLAG_OVER_LIMIT, "Status 13: превышение ресурса",
         """(
            (ac_type_mask = 32 AND (
                (ll_mi8 > 0 AND sne > ll_mi8) OR 
                (oh_mi8 > 0 AND ppr > oh_mi8 AND group_by != 6)
            ))
            OR
            (ac_type_mask = 64 AND (
                (ll_mi17 > 0 AND sne > ll_mi17) OR 
                (oh_mi17 > 0 AND ppr > oh_mi17 AND group_by != 6)
            ))
         )"""),
        
        (FLAG_BAD_COND, "Status 14: некорректный condition",
         "(condition NOT IN ('ИСПРАВНЫЙ', 'НЕИСПРАВНЫЙ', 'ДОНОР', 'ВОЗМОЖНОЕ ПРОДЛЕНИЕ НР'))"),
        
        (FLAG_EARLY_DONOR, "Status 15: донор при ремонтопригодном",
         """(condition = 'ДОНОР' AND (
            (ac_type_mask = 32 AND br_mi8 > 0 AND sne < br_mi8)
            OR
            (ac_type_mask = 64 AND br_mi17 > 0 AND sne < br_mi17)
         ))"""),
    ]
    
    for flag_value, description, condition in flags:
        print(f"\n{description}...")
        query = f"""
        ALTER TABLE heli_pandas UPDATE 
            error_flags = bitOr(error_flags, {flag_value})
        WHERE group_by >= 1 AND {condition}
        """
        client.execute(query)
        wait_for_mutations(client)
        
        # Подсчёт
        cnt = client.execute(f"""
            SELECT count(*) FROM heli_pandas 
            WHERE group_by >= 1 AND bitAnd(error_flags, {flag_value}) > 0
        """)[0][0]
        print(f"  ✓ Установлено для {cnt:,} агрегатов")


def run_validation_analysis(client):
    """Анализ данных по error_flags (используя новые колонки в heli_pandas)."""
    print("\n" + "=" * 80)
    print("АНАЛИЗ ВАЛИДАЦИИ HELI_PANDAS (по error_flags)")
    print("=" * 80)
    
    # Общее количество агрегатов (group_by >= 1)
    total = client.execute("SELECT count(*) FROM heli_pandas WHERE group_by >= 1")[0][0]
    print(f"\nВсего агрегатов (group_by >= 1): {total:,}")
    
    # Статистика по error_flags
    flags_info = [
        (FLAG_NO_DATA, "10", "Недостаточные данные (ll_mi8=0 И ll_mi17=0)"),
        (FLAG_DATE_PAST, "11", "Дата ремонта в прошлом"),
        (FLAG_SNE_ZERO, "12", "Неисправен при sne=0"),
        (FLAG_OVER_LIMIT, "13", "Превышение ресурса"),
        (FLAG_BAD_COND, "14", "Некорректный condition"),
        (FLAG_EARLY_DONOR, "15", "Донор при ремонтопригодном (warning)"),
    ]
    
    print(f"\n{'Status':<8} {'Описание':<45} {'Кол-во':>10} {'%':>8}")
    print("-" * 75)
    
    counts = {}
    for flag_value, status, description in flags_info:
        cnt = client.execute(f"""
            SELECT count(*) FROM heli_pandas 
            WHERE group_by >= 1 AND bitAnd(error_flags, {flag_value}) > 0
        """)[0][0]
        counts[status] = cnt
        print(f"{status:<8} {description:<45} {cnt:>10,} {cnt/total*100:>7.1f}%")
    
    print("-" * 75)
    
    # Агрегаты БЕЗ ошибок
    cnt_clean = client.execute("""
        SELECT count(*) FROM heli_pandas 
        WHERE group_by >= 1 AND error_flags = 0
    """)[0][0]
    
    print(f"\n{'БЕЗ ошибок (error_flags=0)':<45} {cnt_clean:>10,} {cnt_clean/total*100:>7.1f}%")
    print(f"{'С ошибками':<45} {total-cnt_clean:>10,} {(total-cnt_clean)/total*100:>7.1f}%")
    
    # =========================================================================
    # ДЕТАЛИЗАЦИЯ ПО КАЖДОМУ СТАТУСУ
    # =========================================================================
    
    # Status 10: Топ номенклатур без данных
    print("\n" + "=" * 80)
    print("STATUS 10: Топ-10 номенклатур без ll (ll_mi8=0 И ll_mi17=0)")
    print("=" * 80)
    query = f"""
    SELECT partno, count(*) as cnt
    FROM heli_pandas 
    WHERE group_by >= 1 AND bitAnd(error_flags, {FLAG_NO_DATA}) > 0
    GROUP BY partno ORDER BY cnt DESC LIMIT 10
    """
    for pn, cnt in client.execute(query):
        print(f"  {pn}: {cnt}")
    
    # Status 12: По condition
    print("\n" + "=" * 80)
    print("STATUS 12: Неисправные при sne=0 по condition")
    print("=" * 80)
    query = f"""
    SELECT condition, count(*) as cnt
    FROM heli_pandas 
    WHERE group_by >= 1 AND bitAnd(error_flags, {FLAG_SNE_ZERO}) > 0
    GROUP BY condition ORDER BY cnt DESC
    """
    for cond, cnt in client.execute(query):
        print(f"  {cond}: {cnt}")
    
    # Status 13: По причине и типу ВС
    print("\n" + "=" * 80)
    print("STATUS 13: Превышение ресурса по типу ВС")
    print("=" * 80)
    
    # Ми-8 (mask=32)
    mi8_sne = client.execute(f"""
        SELECT count(*) FROM heli_pandas 
        WHERE group_by >= 1 AND ac_type_mask = 32 AND ll_mi8 > 0 AND sne > ll_mi8
    """)[0][0]
    mi8_ppr = client.execute(f"""
        SELECT count(*) FROM heli_pandas 
        WHERE group_by >= 1 AND ac_type_mask = 32 AND oh_mi8 > 0 AND ppr > oh_mi8
    """)[0][0]
    
    # Ми-17 (mask=64)
    mi17_sne = client.execute(f"""
        SELECT count(*) FROM heli_pandas 
        WHERE group_by >= 1 AND ac_type_mask = 64 AND ll_mi17 > 0 AND sne > ll_mi17
    """)[0][0]
    mi17_ppr = client.execute(f"""
        SELECT count(*) FROM heli_pandas 
        WHERE group_by >= 1 AND ac_type_mask = 64 AND oh_mi17 > 0 AND ppr > oh_mi17
    """)[0][0]
    
    print(f"  Ми-8 (mask=32): sne>ll_mi8={mi8_sne}, ppr>oh_mi8={mi8_ppr}")
    print(f"  Ми-17 (mask=64): sne>ll_mi17={mi17_sne}, ppr>oh_mi17={mi17_ppr}")
    
    # Status 14: Некорректные condition
    if counts.get("14", 0) > 0:
        print("\n" + "=" * 80)
        print("STATUS 14: Некорректные значения condition")
        print("=" * 80)
        query = f"""
        SELECT condition, count(*) as cnt
        FROM heli_pandas 
        WHERE group_by >= 1 AND bitAnd(error_flags, {FLAG_BAD_COND}) > 0
        GROUP BY condition ORDER BY cnt DESC LIMIT 10
        """
        for cond, cnt in client.execute(query):
            print(f"  '{cond}': {cnt}")
    
    # =========================================================================
    # ПРОВЕРКА PPR=0 ДЛЯ АГРЕГАТОВ
    # =========================================================================
    print("\n" + "=" * 80)
    print("ПРОВЕРКА PPR=0 (ДЕФЕКТ ИСХОДНЫХ ДАННЫХ)")
    print("=" * 80)
    
    # Статистика PPR=0 по группам
    ppr_query = """
    SELECT 
        group_by,
        count(*) as total,
        countIf(ppr = 0 OR ppr IS NULL) as ppr_zero,
        countIf(sne > 0 AND (ppr = 0 OR ppr IS NULL)) as sne_pos_ppr_zero,
        countIf(sne > 0 AND sne < ll AND (ppr = 0 OR ppr IS NULL)) as candidates_fix,
        countIf(sne = 0 AND (ppr = 0 OR ppr IS NULL)) as new_units,
        countIf(sne > 0 AND sne >= ll AND (ppr = 0 OR ppr IS NULL)) as over_ll
    FROM heli_pandas
    WHERE group_by >= 3
    GROUP BY group_by
    ORDER BY candidates_fix DESC
    LIMIT 15
    """
    ppr_result = client.execute(ppr_query)
    
    print(f"\nТоп-15 групп по количеству кандидатов для FIX (PPR=SNE при sne>0, sne<ll):")
    print(f"{'group':>6} | {'total':>6} | {'ppr=0':>6} | {'sne>0':>6} | {'FIX':>6} | {'new':>6} | {'>=ll':>6}")
    print("-" * 60)
    
    total_fix = 0
    total_new = 0
    total_over = 0
    for row in ppr_result:
        gb, total_g, ppr_zero, sne_pos, fix, new, over = row
        total_fix += fix
        total_new += new
        total_over += over
        print(f"{gb:>6} | {total_g:>6} | {ppr_zero:>6} | {sne_pos:>6} | {fix:>6} | {new:>6} | {over:>6}")
    
    print(f"\nСВОДКА PPR=0:")
    print(f"  Кандидаты для FIX (sne>0, sne<ll): {total_fix:,}")
    print(f"  Новые агрегаты (sne=0): {total_new:,}")
    print(f"  Превысили LL (sne>=ll): {total_over:,}")
    print(f"\n⚠️ FIX PPR=SNE применяется при загрузке в симуляцию (agent_population_units.py)")
    
    # =========================================================================
    # ПЕРЕСЕЧЕНИЯ ФЛАГОВ
    # =========================================================================
    print("\n" + "=" * 80)
    print("ПЕРЕСЕЧЕНИЯ ФЛАГОВ")
    print("=" * 80)
    
    # Флаг 10 и 12
    overlap_10_12 = client.execute(f"""
        SELECT count(*) FROM heli_pandas 
        WHERE group_by >= 1 
          AND bitAnd(error_flags, {FLAG_NO_DATA}) > 0 
          AND bitAnd(error_flags, {FLAG_SNE_ZERO}) > 0
    """)[0][0]
    print(f"Status 10 + 12 (нет данных + sne=0): {overlap_10_12:,}")
    
    # Флаг 12 и 13
    overlap_12_13 = client.execute(f"""
        SELECT count(*) FROM heli_pandas 
        WHERE group_by >= 1 
          AND bitAnd(error_flags, {FLAG_SNE_ZERO}) > 0 
          AND bitAnd(error_flags, {FLAG_OVER_LIMIT}) > 0
    """)[0][0]
    print(f"Status 12 + 13 (sne=0 + превышение): {overlap_12_13:,}")


def validate_ppr_zero(client, version_date='2025-07-04', version_id=1, export=False):
    """Проверка PPR=0 для агрегатов (дефект исходных данных)."""
    print("\n" + "=" * 80)
    print("ПРОВЕРКА PPR=0 (ДЕФЕКТ ИСХОДНЫХ ДАННЫХ)")
    print(f"Версия: {version_date} v{version_id}")
    print("=" * 80)
    
    # Статистика PPR=0 по группам
    ppr_query = f"""
    SELECT 
        group_by,
        count(*) as total,
        countIf(ppr = 0 OR ppr IS NULL) as ppr_zero,
        countIf(sne > 0 AND (ppr = 0 OR ppr IS NULL)) as sne_pos_ppr_zero,
        countIf(sne > 0 AND sne < ll AND (ppr = 0 OR ppr IS NULL)) as candidates_fix,
        countIf(sne = 0 AND (ppr = 0 OR ppr IS NULL)) as new_units,
        countIf(sne > 0 AND sne >= ll AND (ppr = 0 OR ppr IS NULL)) as over_ll
    FROM heli_pandas
    WHERE version_date = toDate('{version_date}')
      AND version_id = {version_id}
      AND group_by >= 3
    GROUP BY group_by
    ORDER BY group_by
    """
    ppr_result = client.execute(ppr_query)
    
    print(f"\n{'group':>6} | {'total':>6} | {'ppr=0':>6} | {'sne>0':>6} | {'FIX':>6} | {'new':>6} | {'>=ll':>6}")
    print("-" * 60)
    
    total_all = 0
    ppr_zero_all = 0
    total_fix = 0
    total_new = 0
    total_over = 0
    
    for row in ppr_result:
        gb, total_g, ppr_zero, sne_pos, fix, new, over = row
        total_all += total_g
        ppr_zero_all += ppr_zero
        total_fix += fix
        total_new += new
        total_over += over
        print(f"{gb:>6} | {total_g:>6} | {ppr_zero:>6} | {sne_pos:>6} | {fix:>6} | {new:>6} | {over:>6}")
    
    print("-" * 60)
    print(f"{'ИТОГО':>6} | {total_all:>6} | {ppr_zero_all:>6} | {'':>6} | {total_fix:>6} | {total_new:>6} | {total_over:>6}")
    
    print(f"\n📊 СВОДКА:")
    print(f"  Всего агрегатов: {total_all:,}")
    print(f"  PPR=0/NULL: {ppr_zero_all:,} ({100*ppr_zero_all/total_all:.1f}%)")
    print(f"  Кандидаты для FIX (sne>0, sne<ll): {total_fix:,}")
    print(f"  Новые агрегаты (sne=0): {total_new:,}")
    print(f"  Превысили LL (sne>=ll): {total_over:,}")
    print(f"\n⚠️ FIX PPR=SNE применяется при загрузке в симуляцию")
    print(f"   Файлы: agent_population_units.py, agent_population.py")
    
    # Экспорт в Excel
    if export:
        import pandas as pd
        from datetime import datetime
        
        export_query = f"""
        SELECT 
            group_by, psn, serialno, partno, aircraft_number,
            sne, ppr, ll, status_id, condition, owner, mfg_date
        FROM heli_pandas
        WHERE version_date = toDate('{version_date}')
          AND version_id = {version_id}
          AND group_by >= 3
          AND (ppr = 0 OR ppr IS NULL)
        ORDER BY group_by, sne DESC
        """
        result = client.execute(export_query)
        columns = ['group_by', 'psn', 'serialno', 'partno', 'aircraft_number',
                   'sne', 'ppr', 'll', 'status_id', 'condition', 'owner', 'mfg_date']
        df = pd.DataFrame(result, columns=columns)
        
        # Добавляем вычисляемые поля
        df['sne_hours'] = df['sne'] / 60
        df['ll_hours'] = df['ll'] / 60
        df['needs_fix'] = (df['sne'] > 0) & (df['sne'] < df['ll'])
        df['category'] = df.apply(
            lambda r: 'FIX' if r['needs_fix'] else ('NEW' if r['sne'] == 0 else 'OVER_LL'), axis=1
        )
        
        output_path = f'output/units_ppr_zero_{datetime.now().strftime("%Y-%m-%d")}.xlsx'
        df.to_excel(output_path, index=False)
        print(f"\n✅ Выгружено {len(df):,} записей в {output_path}")
    
    return {
        'total': total_all,
        'ppr_zero': ppr_zero_all,
        'fix': total_fix,
        'new': total_new,
        'over_ll': total_over
    }


def main():
    """Главная функция с аргументами командной строки."""
    parser = argparse.ArgumentParser(
        description='Валидация и анализ heli_pandas',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры:
  python validate_heli_pandas.py --ppr               # Проверка PPR=0 (быстро)
  python validate_heli_pandas.py --ppr --export      # Проверка PPR=0 + экспорт в Excel
  python validate_heli_pandas.py --analyze           # Полный анализ по error_flags
  python validate_heli_pandas.py --update            # Обновить ресурсы и флаги
  python validate_heli_pandas.py --all               # Всё вместе
        """
    )
    parser.add_argument('--update', action='store_true',
                        help='Обновить ll_mi8/oh_mi8/br_mi8 из md_components и пересчитать error_flags')
    parser.add_argument('--analyze', action='store_true',
                        help='Показать анализ по error_flags')
    parser.add_argument('--ppr', action='store_true',
                        help='Проверка PPR=0 для агрегатов')
    parser.add_argument('--export', action='store_true',
                        help='Экспорт результатов в Excel (с --ppr)')
    parser.add_argument('--version-date', type=str, default='2025-07-04',
                        help='Дата версии данных (по умолчанию: 2025-07-04)')
    parser.add_argument('--version-id', type=int, default=1,
                        help='ID версии (по умолчанию: 1)')
    parser.add_argument('--all', action='store_true',
                        help='Выполнить --update и --analyze')
    
    args = parser.parse_args()
    
    # Если ничего не указано — показать справку
    if not (args.update or args.analyze or args.ppr or args.all):
        parser.print_help()
        return
    
    client = get_clickhouse_client()
    
    print("=" * 80)
    print("ВАЛИДАЦИЯ HELI_PANDAS")
    print("=" * 80)
    
    if args.ppr:
        validate_ppr_zero(client, args.version_date, args.version_id, export=args.export)
    
    if args.update or args.all:
        update_resources(client)
        update_error_flags(client)
    
    if args.analyze or args.all:
        run_validation_analysis(client)
    
    print("\n" + "=" * 80)
    print("ГОТОВО!")
    print("=" * 80)


if __name__ == "__main__":
    main()

