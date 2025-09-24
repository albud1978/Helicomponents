#!/usr/bin/env python3
"""
Проверка корректности загрузки данных агентов из базы
"""
import sys
sys.path.append('/home/budnik_an/cube_linux/cube/code')

from config_loader import get_clickhouse_client

def check_agent_data(aircraft_numbers):
    """Проверка данных агентов в базе"""
    client = get_clickhouse_client()
    
    # Запрос данных из heli_pandas
    ac_list = ','.join(str(ac) for ac in aircraft_numbers)
    query = f"""
    SELECT 
        aircraft_number,
        status_id,
        sne,
        ppr,
        repair_days,
        group_by,
        partseqno_i
    FROM heli_pandas
    WHERE aircraft_number IN ({ac_list})
        AND group_by IN (1, 2)  -- Только планеры
    ORDER BY aircraft_number
    """
    
    print("=== Данные из heli_pandas ===")
    result = client.execute(query)
    for row in result:
        print(f"AC {row[0]}: status={row[1]}, sne={row[2]}, ppr={row[3]}, repair_days={row[4]}, group_by={row[5]}, partseqno={row[6]}")
    
    # Проверка нормативов из md_components
    query_norms = f"""
    SELECT 
        c.partno,
        c.ll_mi8,
        c.ll_mi17,
        c.oh_mi8,
        c.oh_mi17,
        c.br_mi8,
        c.br_mi17,
        h.group_by,
        h.aircraft_number,
        h.partseqno_i
    FROM md_components c
    JOIN heli_pandas h ON c.partno = h.partseqno_i
    WHERE h.aircraft_number IN ({ac_list})
        AND h.group_by IN (1, 2)
    ORDER BY h.aircraft_number
    """
    
    print("\n=== Нормативы из md_components ===")
    result_norms = client.execute(query_norms)
    for row in result_norms:
        partno, ll_mi8, ll_mi17, oh_mi8, oh_mi17, br_mi8, br_mi17, group_by, ac, partseqno = row
        
        # Выбор нормативов по group_by
        if group_by == 1:  # Mi-8
            ll, oh, br = ll_mi8, oh_mi8, br_mi8
            type_str = "Mi-8"
        elif group_by == 2:  # Mi-17
            ll, oh, br = ll_mi17, oh_mi17, br_mi17
            type_str = "Mi-17"
        else:
            ll, oh, br = 0, 0, 0
            type_str = "Unknown"
            
        print(f"AC {ac} ({type_str}): partseqno={partseqno}, LL={ll}, OH={oh}, BR={br}")

    # Проверка нормативов MP1
    query_mp1 = f"""
    SELECT 
        partseqno_i,
        ll_mi8, ll_mi17,
        oh_mi8, oh_mi17,
        br_mi8, br_mi17
    FROM mp1
    WHERE partseqno_i IN (
        SELECT DISTINCT partseqno_i 
        FROM heli_pandas 
        WHERE aircraft_number IN ({ac_list})
            AND group_by IN (1, 2)
    )
    """
    
    print("\n=== Нормативы из MP1 (приоритетные) ===")
    result_mp1 = client.execute(query_mp1)
    for row in result_mp1:
        partseqno = row[0]
        print(f"Partseqno {partseqno}: OH_mi8={row[3]}, OH_mi17={row[4]}")

if __name__ == '__main__':
    # Агенты для проверки
    test_agents = [25355, 22418, 25484, 22607]
    
    check_agent_data(test_agents)
