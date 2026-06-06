#!/usr/bin/env python3
# ARCHIVED 2026-06-06 (code rudiments cleanup): orphan, не импортируется живым кодом (sim_v2/extract/validation).
"""
Детальный анализ Excel файла OLAP MultiBOM Flame GPU.xlsx
с обработкой объединенных ячеек и сложной структуры
"""

import pandas as pd
import numpy as np
from pathlib import Path
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

EXCEL_FILE = "data_input/analytics/OLAP MultiBOM Flame GPU.xlsx"

def analyze_excel_detailed():
    """Детальный анализ Excel файла с разными параметрами чтения"""
    
    logger.info(f"📊 Детальный анализ файла: {EXCEL_FILE}")
    
    # Попробуем разные варианты чтения
    variants = [
        {"header": None, "description": "Без заголовков"},
        {"header": 0, "description": "Первая строка как заголовок"},
        {"header": 1, "description": "Вторая строка как заголовок"},
        {"header": 2, "description": "Третья строка как заголовок"},
        {"header": [0, 1], "description": "Первые две строки как заголовок"},
        {"header": [0, 1, 2], "description": "Первые три строки как заголовок"},
    ]
    
    for i, variant in enumerate(variants):
        try:
            print(f"\n{'='*80}")
            print(f"📋 ВАРИАНТ {i+1}: {variant['description']}")
            print(f"{'='*80}")
            
            # Убираем description для передачи в pandas
            params = {k: v for k, v in variant.items() if k != 'description'}
            
            df = pd.read_excel(EXCEL_FILE, engine='openpyxl', **params)
            
            print(f"📊 Размер: {df.shape[0]} строк × {df.shape[1]} столбцов")
            print(f"📋 Столбцы: {list(df.columns)}")
            
            # Показываем первые 5 строк
            print("\n🔍 Первые 5 строк:")
            print("-" * 80)
            for idx, row in df.head().iterrows():
                print(f"Строка {idx}: {dict(row)}")
            
            # Ищем ключевые слова в данных
            print("\n🔍 Поиск ключевых слов:")
            print("-" * 40)
            
            keywords = ["Поле", "Источник", "DWH", "Flame", "GPU", "TRANSFORM", "cudf"]
            
            for keyword in keywords:
                found_locations = []
                for col_idx, col in enumerate(df.columns):
                    for row_idx, cell_value in enumerate(df[col]):
                        if isinstance(cell_value, str) and keyword.lower() in cell_value.lower():
                            found_locations.append(f"Столбец {col_idx} ({col}), строка {row_idx}: '{cell_value}'")
                
                if found_locations:
                    print(f"  🔍 '{keyword}' найдено в {len(found_locations)} местах:")
                    for loc in found_locations[:3]:  # Показываем только первые 3
                        print(f"    • {loc}")
                    if len(found_locations) > 3:
                        print(f"    ... и еще {len(found_locations) - 3} мест")
                else:
                    print(f"  ❌ '{keyword}' не найдено")
                
        except Exception as e:
            print(f"❌ Ошибка при чтении варианта {i+1}: {e}")
    
    # Пробуем прочитать отдельные листы
    try:
        print(f"\n{'='*80}")
        print("📋 АНАЛИЗ ЛИСТОВ EXCEL")
        print(f"{'='*80}")
        
        xl_file = pd.ExcelFile(EXCEL_FILE)
        print(f"📊 Найдено листов: {len(xl_file.sheet_names)}")
        
        for sheet_name in xl_file.sheet_names:
            print(f"\n📋 Лист: {sheet_name}")
            print("-" * 40)
            
            try:
                df_sheet = pd.read_excel(EXCEL_FILE, sheet_name=sheet_name, header=None)
                print(f"   Размер: {df_sheet.shape[0]} строк × {df_sheet.shape[1]} столбцов")
                
                # Показываем первые несколько непустых ячеек
                print("   Первые непустые значения:")
                count = 0
                for row_idx in range(min(10, df_sheet.shape[0])):
                    for col_idx in range(min(10, df_sheet.shape[1])):
                        cell_value = df_sheet.iloc[row_idx, col_idx]
                        if pd.notna(cell_value) and str(cell_value).strip():
                            print(f"     [{row_idx},{col_idx}]: '{cell_value}'")
                            count += 1
                            if count >= 20:  # Ограничиваем вывод
                                break
                    if count >= 20:
                        break
                        
            except Exception as e:
                print(f"   ❌ Ошибка при чтении листа {sheet_name}: {e}")
                
    except Exception as e:
        print(f"❌ Ошибка при анализе листов: {e}")

def find_field_mappings():
    """Поиск маппинга полей в Excel файле"""
    
    print(f"\n{'='*80}")
    print("🔍 ПОИСК МАППИНГА ПОЛЕЙ")
    print(f"{'='*80}")
    
    try:
        # Читаем как сырые данные
        df = pd.read_excel(EXCEL_FILE, header=None, engine='openpyxl')
        
        # Ищем структуру данных
        field_mappings = []
        
        for row_idx in range(df.shape[0]):
            for col_idx in range(df.shape[1]):
                cell_value = df.iloc[row_idx, col_idx]
                
                if isinstance(cell_value, str):
                    # Ищем строки, похожие на названия полей
                    field_like_patterns = [
                        'partno', 'serialno', 'ac_typ', 'location', 'owner',
                        'mfg_date', 'removal_date', 'target_date', 'condition',
                        'oh', 'oh_threshold', 'll', 'sne', 'ppr', 'repair_days',
                        'aircraft_number', 'status', 'lease_restricted'
                    ]
                    
                    for pattern in field_like_patterns:
                        if pattern.lower() in cell_value.lower():
                            # Смотрим на соседние ячейки
                            mapping_info = {
                                'field': cell_value,
                                'position': f"[{row_idx},{col_idx}]",
                                'pattern': pattern
                            }
                            
                            # Проверяем соседние ячейки
                            neighbors = []
                            for d_row in [-1, 0, 1]:
                                for d_col in [-1, 0, 1]:
                                    if d_row == 0 and d_col == 0:
                                        continue
                                    new_row = row_idx + d_row
                                    new_col = col_idx + d_col
                                    
                                    if (0 <= new_row < df.shape[0] and 
                                        0 <= new_col < df.shape[1]):
                                        neighbor = df.iloc[new_row, new_col]
                                        if pd.notna(neighbor):
                                            neighbors.append(f"[{new_row},{new_col}]: '{neighbor}'")
                            
                            mapping_info['neighbors'] = neighbors[:5]  # Ограничиваем вывод
                            field_mappings.append(mapping_info)
        
        print(f"📋 Найдено потенциальных маппингов: {len(field_mappings)}")
        
        for mapping in field_mappings:
            print(f"\n🔍 Поле: {mapping['field']}")
            print(f"   Позиция: {mapping['position']}")
            print(f"   Паттерн: {mapping['pattern']}")
            print(f"   Соседние ячейки:")
            for neighbor in mapping['neighbors']:
                print(f"     • {neighbor}")
                
    except Exception as e:
        print(f"❌ Ошибка при поиске маппинга: {e}")

def main():
    """Основная функция"""
    
    # Проверяем существование файла
    if not Path(EXCEL_FILE).exists():
        logger.error(f"❌ Файл {EXCEL_FILE} не найден!")
        return
    
    # Детальный анализ
    analyze_excel_detailed()
    
    # Поиск маппинга полей
    find_field_mappings()
    
    logger.info("✅ Детальный анализ завершен")

if __name__ == "__main__":
    main() 