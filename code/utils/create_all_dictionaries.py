#!/usr/bin/env python3
"""
Создание ВСЕХ словарей системы с полной поддержкой dictGet и аддитивности

Создает все необходимые словари:
- АДДИТИВНЫЕ: partno, serialno, owner, ac_type, aircraft_number
- НЕ АДДИТИВНЫЙ: status

Поддержка dictGet для всех словарей с FLAT layout для максимальной производительности.

Использование:
    python3 create_all_dictionaries.py
"""

import sys
from pathlib import Path
import logging

# Добавляем путь к проекту
sys.path.append(str(Path(__file__).parent))

from dictionary_creator import DictionaryCreator


def setup_logging():
    """Настройка логирования"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)


def main():
    """Основная функция создания всех словарей"""
    logger = setup_logging()
    
    print("🚀 === СОЗДАНИЕ ВСЕХ СЛОВАРЕЙ СИСТЕМЫ ===")
    print("✨ Аддитивные словари: partno, serialno, owner, ac_type, aircraft_number")
    print("📋 Не аддитивный словарь: status")
    print("🔥 Поддержка dictGet: ПОЛНАЯ для всех словарей")
    print()
    
    try:
        # Создаем экземпляр создателя словарей
        creator = DictionaryCreator()
        
        # Запускаем создание всех словарей
        success = creator.create_all_dictionaries_with_dictget()
        
        if success:
            print(f"\n🎯 === ВСЕ СЛОВАРИ УСПЕШНО СОЗДАНЫ ===")
            print("✅ АДДИТИВНЫЕ словари готовы:")
            print("   - dict_partno_flat → partno_dict_flat (dictGet)")
            print("   - dict_serialno_flat → serialno_dict_flat (dictGet)")
            print("   - dict_owner_flat → owner_dict_flat (dictGet)")
            print("   - dict_ac_type_flat → ac_type_dict_flat (dictGet)")
            print("   - dict_aircraft_number_flat → aircraft_number_dict_flat (dictGet + ac_type_mask)")
            print("✅ НЕ АДДИТИВНЫЙ словарь готов:")
            print("   - dict_status_flat → status_dict_flat (dictGet)")
            print()
            print("🔥 Примеры использования dictGet:")
            print("   SELECT dictGet('partno_dict_flat', 'partno', partseqno_i) FROM heli_pandas")
            print("   SELECT dictGet('status_dict_flat', 'status_name', status_id) FROM heli_pandas")
            print("   SELECT dictGet('aircraft_number_dict_flat', 'registration_code', aircraft_number) FROM heli_pandas")
            print("   SELECT dictGet('aircraft_number_dict_flat', 'ac_type_mask', aircraft_number) FROM heli_pandas")
            print()
            print("💡 Аддитивные словари автоматически обновляются при новых данных!")
            print("📋 Словарь статусов пересоздается при каждом запуске")
            print("🚁 aircraft_number_dict_flat содержит ac_type_mask для Flame GPU операций")
            
            return 0
        else:
            print(f"\n❌ === ОШИБКА СОЗДАНИЯ СЛОВАРЕЙ ===")
            print("Проверьте логи выше для деталей ошибки")
            return 1
            
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
        print(f"\n❌ === КРИТИЧЕСКАЯ ОШИБКА ===")
        print(f"Ошибка: {e}")
        return 1


if __name__ == "__main__":
    exit(main()) 