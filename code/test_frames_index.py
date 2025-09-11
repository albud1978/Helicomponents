#!/usr/bin/env python3
"""
Тест для проверки, что frames_index корректно учитывает MP3 ∪ MP5
"""

import sys
from utils.config_loader import get_clickhouse_client
from sim_env_setup import (
    fetch_versions,
    fetch_mp3,
    preload_mp5_maps,
    build_frames_index,
)

def main():
    client = get_clickhouse_client()
    
    # Загружаем данные
    vdate, vid = fetch_versions(client)
    mp3_rows, mp3_fields = fetch_mp3(client, vdate, vid)
    mp5_by_day = preload_mp5_maps(client)
    
    # Строим индексы БЕЗ учета MP5 (старый способ)
    frames_index_old, frames_total_old = build_frames_index(mp3_rows, mp3_fields, None)
    
    # Строим индексы С учетом MP5 (новый способ)
    frames_index_new, frames_total_new = build_frames_index(mp3_rows, mp3_fields, mp5_by_day)
    
    print(f"📊 Результаты анализа frames_index:")
    print(f"   Старый способ (только MP3): frames_total = {frames_total_old}")
    print(f"   Новый способ (MP3 ∪ MP5):   frames_total = {frames_total_new}")
    print(f"   Разница: {frames_total_new - frames_total_old} дополнительных aircraft_number из MP5")
    
    # Найдем aircraft_number которые есть только в MP5
    mp3_aircraft_numbers = set()
    idx = {name: i for i, name in enumerate(mp3_fields)}
    for r in mp3_rows:
        ac = int(r[idx['aircraft_number']] or 0)
        if ac > 0:
            mp3_aircraft_numbers.add(ac)
    
    mp5_aircraft_numbers = set()
    for day_data in mp5_by_day.values():
        for ac in day_data.keys():
            if ac > 0:
                mp5_aircraft_numbers.add(ac)
    
    only_in_mp5 = mp5_aircraft_numbers - mp3_aircraft_numbers
    print(f"\n📋 Aircraft_number только в MP5: {len(only_in_mp5)}")
    if only_in_mp5:
        print(f"   Примеры (первые 10): {sorted(list(only_in_mp5))[:10]}")
    
    # Проверяем индексацию
    print(f"\n🔍 Проверка индексации:")
    print(f"   frames_initial (из MP3) должно быть около: {len(mp3_aircraft_numbers)}")
    print(f"   Новые агенты для спавна начнутся с индекса: {frames_total_old}")
    
    # Примеры индексов для новых aircraft_number
    if only_in_mp5:
        print(f"\n📍 Примеры индексов для новых aircraft_number:")
        for ac in sorted(list(only_in_mp5))[:5]:
            if ac in frames_index_new:
                print(f"   aircraft_number {ac} -> index {frames_index_new[ac]}")

if __name__ == "__main__":
    main()
