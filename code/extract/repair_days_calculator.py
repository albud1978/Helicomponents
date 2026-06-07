#!/usr/bin/env python3
"""
Калькулятор repair_days для ВС в ремонте

Формула: repair_days = repair_time - (sched_end_date - version_date)

Логика:
- Работает с ВС в статусе 4 (Ремонт) в таблице heli_pandas
- Получает repair_time из md_components (заполненного md_components_enricher.py)
- Берет sched_end_date из target_date в heli_pandas
- Рассчитывает сколько дней ремонта уже прошло

Зависимости:
- md_components (с заполненным repair_time)
- heli_pandas (с установленным status_id=4)
- status_overhaul (для дат ремонта)

Автор: AI Assistant  
Дата: 2025-07-26
"""

import sys
import logging
from pathlib import Path
from datetime import datetime, date
import argparse

# Добавляем пути к utils и общему коду
code_root = Path(__file__).resolve().parents[1]
sys.path.append(str(code_root / 'utils'))
sys.path.append(str(code_root))
from config_loader import load_clickhouse_config
import clickhouse_connect


class RepairDaysCalculator:
    """Калькулятор repair_days для ВС в ремонте"""
    
    def __init__(self, version_date=None, version_id=None):
        """Инициализация калькулятора"""
        self.logger = self._setup_logging()
        self.client = None
        self.version_date = version_date
        self.version_id = version_id
    
    def _setup_logging(self) -> logging.Logger:
        """Настройка логирования"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        return logging.getLogger(__name__)
    
    def connect_to_database(self) -> bool:
        """Подключение к ClickHouse"""
        try:
            config = load_clickhouse_config()
            # Исправляем конфигурацию для работы с ClickHouse
            config['port'] = 8123  # HTTP порт
            if 'settings' in config:
                config['settings'] = {k: v for k, v in config['settings'].items() if k != 'use_numpy'}
            self.client = clickhouse_connect.get_client(**config)
            result = self.client.query('SELECT 1 as test')
            self.logger.info(f"✅ Подключение к ClickHouse успешно!")
            return True
        except Exception as e:
            self.logger.error(f"❌ Ошибка подключения: {e}")
            return False
    
    def get_repair_aircraft(self):
        """Получает ВС в ремонте (status_id=4) из heli_pandas"""
        try:
            self.logger.info("🔍 Поиск ВС в ремонте (status_id=4)...")
            
            if self.version_date and self.version_id:
                query = """
                SELECT serialno, target_date, version_date, partseqno_i
                FROM heli_pandas 
                WHERE status_id = 4 
                    AND version_date = %s 
                    AND version_id = %s
                ORDER BY serialno
                """
                result = self.client.query(query, [self.version_date, self.version_id])
            else:
                query = """
                SELECT serialno, target_date, version_date, partseqno_i
                FROM heli_pandas 
                WHERE status_id = 4
                ORDER BY serialno
                """
                result = self.client.query(query)
            
            if not result.result_rows:
                self.logger.info("ℹ️ Не найдено ВС в ремонте (status_id=4)")
                return []
            
            repair_aircraft = []
            for row in result.result_rows:
                serialno, target_date, version_date, partseqno_i = row
                repair_aircraft.append({
                    'serialno': serialno,
                    'target_date': target_date,
                    'version_date': version_date, 
                    'partseqno_i': partseqno_i
                })
            
            self.logger.info(f"✅ Найдено {len(repair_aircraft)} ВС в ремонте")
            for aircraft in repair_aircraft[:5]:  # Показываем первые 5
                self.logger.info(f"   ВС {aircraft['serialno']}: target_date={aircraft['target_date']}")
            
            return repair_aircraft
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка получения ВС в ремонте: {e}")
            return []
    
    def get_repair_times(self, repair_aircraft):
        """Получает repair_time из md_components для ВС в ремонте"""
        try:
            self.logger.info("📋 Получение repair_time из md_components...")
            
            if not repair_aircraft:
                return {}
            
            # Получаем уникальные partseqno_i
            partseqno_list = list(set([aircraft['partseqno_i'] for aircraft in repair_aircraft]))
            
            # md_components — ЕДИНЫЙ справочник без версионности
            # НЕ фильтруем по version_date/version_id
            placeholders = ','.join(['%s'] * len(partseqno_list))
            query = f"""
            SELECT partseqno_i, repair_time
            FROM md_components 
            WHERE partseqno_i IN ({placeholders})
            """
            result = self.client.query(query, partseqno_list)
            
            repair_times = {}
            for row in result.result_rows:
                partseqno_i, repair_time = row
                repair_times[partseqno_i] = repair_time
            
            self.logger.info(f"✅ Получено repair_time для {len(repair_times)} компонентов")
            for partseqno_i, repair_time in list(repair_times.items())[:3]:
                self.logger.info(f"   partseqno_i {partseqno_i}: repair_time={repair_time}")
            
            return repair_times
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка получения repair_time: {e}")
            return {}
    
    def calculate_repair_days(self, repair_aircraft, repair_times):
        """Рассчитывает repair_days по новой формуле"""
        try:
            self.logger.info("🔢 Расчет repair_days по формуле: repair_time - (target_date - version_date)")
            
            updates = []
            calculated_count = 0
            
            for aircraft in repair_aircraft:
                serialno = aircraft['serialno']
                target_date = aircraft['target_date']
                version_date = aircraft['version_date']
                partseqno_i = aircraft['partseqno_i']
                
                # Получаем repair_time для этого компонента
                repair_time = repair_times.get(partseqno_i)
                
                if not repair_time:
                    self.logger.warning(f"⚠️ ВС {serialno}: не найден repair_time для partseqno_i={partseqno_i}")
                    continue
                
                if not target_date or not version_date:
                    self.logger.warning(f"⚠️ ВС {serialno}: отсутствуют даты target_date={target_date}, version_date={version_date}")
                    continue
                
                # Формула: repair_days = max(0, repair_time - (target_date - version_date))
                days_remaining = (target_date - version_date).days
                repair_days = max(0, repair_time - days_remaining)
                
                self.logger.info(f"✅ {serialno}: repair_days = {repair_time} - ({target_date} - {version_date}) = {repair_time} - {days_remaining} = {repair_days}")
                
                # Подготавливаем UPDATE
                if self.version_date and self.version_id:
                    updates.append({
                        'serialno': serialno,
                        'repair_days': repair_days,
                        'version_date': self.version_date,
                        'version_id': self.version_id
                    })
                else:
                    updates.append({
                        'serialno': serialno,
                        'repair_days': repair_days
                    })
                
                calculated_count += 1
            
            self.logger.info(f"✅ Рассчитано repair_days для {calculated_count} ВС")
            return updates
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка расчета repair_days: {e}")
            return []
    
    def update_repair_days(self, updates):
        """Обновляет repair_days в таблице heli_pandas"""
        try:
            self.logger.info("💾 Обновление repair_days в heli_pandas...")
            
            if not updates:
                self.logger.info("ℹ️ Нет обновлений для применения")
                return True
            
            updated_count = 0
            
            for update in updates:
                serialno = update['serialno']
                repair_days = update['repair_days']
                
                if self.version_date and self.version_id:
                    query = """
                    ALTER TABLE heli_pandas 
                    UPDATE repair_days = %s
                    WHERE serialno = %s 
                        AND version_date = %s 
                        AND version_id = %s
                    """
                    self.client.query(query, [repair_days, serialno, self.version_date, self.version_id])
                else:
                    query = """
                    ALTER TABLE heli_pandas 
                    UPDATE repair_days = %s
                    WHERE serialno = %s
                    """
                    self.client.query(query, [repair_days, serialno])
                
                updated_count += 1
                self.logger.info(f"   ✅ ВС {serialno}: repair_days = {repair_days}")
            
            self.logger.info(f"✅ Обновлено {updated_count} записей с repair_days")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка обновления repair_days: {e}")
            return False
    
    def run(self):
        """Основной метод выполнения"""
        self.logger.info("🚀 === РАСЧЕТ REPAIR_DAYS ===")
        
        try:
            # 1. Подключение к базе
            if not self.connect_to_database():
                return False
            
            # 2. Получение ВС в ремонте
            repair_aircraft = self.get_repair_aircraft()
            if not repair_aircraft:
                self.logger.info("ℹ️ Нет ВС в ремонте для обработки")
                return True
            
            # 3. Получение repair_time из md_components
            repair_times = self.get_repair_times(repair_aircraft)
            if not repair_times:
                self.logger.error("❌ Не удалось получить repair_time из md_components")
                return False
            
            # 4. Расчет repair_days
            updates = self.calculate_repair_days(repair_aircraft, repair_times)
            if not updates:
                self.logger.warning("⚠️ Нет данных для обновления repair_days")
                return True
            
            # 5. Обновление в базе
            if not self.update_repair_days(updates):
                return False
            
            self.logger.info("✅ === РАСЧЕТ ЗАВЕРШЕН ===")
            self.logger.info(f"📊 Итого записей с repair_days: {len(updates)}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Критическая ошибка: {e}")
            return False


def main():
    """Основная функция"""
    parser = argparse.ArgumentParser(description='Расчет repair_days для ВС в ремонте')
    parser.add_argument('--version-date', type=str, help='Дата версии данных (YYYY-MM-DD)')
    parser.add_argument('--version-id', type=int, help='ID версии данных')
    
    args = parser.parse_args()
    
    # Преобразуем строку даты в объект date
    version_date = None
    if args.version_date:
        try:
            version_date = datetime.strptime(args.version_date, '%Y-%m-%d').date()
        except ValueError:
            print(f"❌ Неверный формат даты: {args.version_date}. Используйте YYYY-MM-DD")
            return False
    
    calculator = RepairDaysCalculator(version_date=version_date, version_id=args.version_id)
    
    if calculator.run():
        print("🎯 Успешно!")
        return True
    else:
        print("❌ Ошибка!")
        return False


if __name__ == "__main__":
    if main():
        sys.exit(0)
    else:
        sys.exit(1) 