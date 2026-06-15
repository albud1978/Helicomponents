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
from config_loader import get_clickhouse_client


class RepairDaysCalculator:
    """Калькулятор repair_days для ВС в ремонте"""
    
    def __init__(self, version_date=None, version_id=None, dry_run=False):
        """Инициализация калькулятора"""
        self.logger = self._setup_logging()
        self.client = None
        self.version_date = version_date
        self.version_id = version_id
        self.dry_run = dry_run
    
    def _setup_logging(self) -> logging.Logger:
        """Настройка логирования"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        return logging.getLogger(__name__)
    
    def connect_to_database(self) -> bool:
        """Подключение к ClickHouse"""
        self.client = get_clickhouse_client()
        self.client.execute('SELECT 1')
        self.logger.info("✅ Подключение к ClickHouse успешно!")
        return True
    
    def get_repair_aircraft(self):
        """Получает ВС в ремонте (status_id=4) из heli_pandas"""
        self.logger.info("🔍 Поиск ВС в ремонте (status_id=4)...")

        if self.version_date and self.version_id:
            query = """
            SELECT serialno, target_date, version_date, partseqno_i
            FROM heli_pandas
            WHERE status_id = 4
                AND version_date = %(version_date)s
                AND version_id = %(version_id)s
            ORDER BY serialno
            """
            rows = self.client.execute(
                query,
                {"version_date": self.version_date, "version_id": self.version_id},
            )
        else:
            query = """
            SELECT serialno, target_date, version_date, partseqno_i
            FROM heli_pandas
            WHERE status_id = 4
            ORDER BY serialno
            """
            rows = self.client.execute(query)

        if not rows:
            self.logger.info("ℹ️ Не найдено ВС в ремонте (status_id=4)")
            return []

        repair_aircraft = []
        for row in rows:
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
    
    def get_repair_times(self, repair_aircraft):
        """Получает repair_time из md_components для ВС в ремонте"""
        self.logger.info("📋 Получение repair_time из md_components...")

        if not repair_aircraft:
            return {}

        # md_components — ЕДИНЫЙ справочник без версионности.
        partseqno_list = tuple(set([aircraft['partseqno_i'] for aircraft in repair_aircraft]))
        query = """
        SELECT partseqno_i, repair_time
        FROM md_components
        WHERE partseqno_i IN %(partseqno_list)s
        """
        rows = self.client.execute(query, {"partseqno_list": partseqno_list})

        repair_times = {}
        for row in rows:
            partseqno_i, repair_time = row
            repair_times[partseqno_i] = repair_time

        self.logger.info(f"✅ Получено repair_time для {len(repair_times)} компонентов")
        for partseqno_i, repair_time in list(repair_times.items())[:3]:
            self.logger.info(f"   partseqno_i {partseqno_i}: repair_time={repair_time}")

        return repair_times
    
    def calculate_repair_days(self, repair_aircraft, repair_times):
        """Рассчитывает repair_days по новой формуле"""
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

            if repair_time is None:
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
    
    def update_repair_days(self, updates):
        """Обновляет repair_days в таблице heli_pandas"""
        self.logger.info("💾 Обновление repair_days в heli_pandas...")

        if not updates:
            self.logger.info("ℹ️ Нет обновлений для применения")
            return True

        updated_count = 0
        self.client.execute("SET mutations_sync = 1")

        for update in updates:
            serialno = update['serialno']
            repair_days = update['repair_days']

            if self.version_date and self.version_id:
                query = """
                ALTER TABLE heli_pandas
                UPDATE repair_days = %(repair_days)s
                WHERE serialno = %(serialno)s
                    AND version_date = %(version_date)s
                    AND version_id = %(version_id)s
                """
                self.client.execute(query, {
                    "repair_days": repair_days,
                    "serialno": serialno,
                    "version_date": self.version_date,
                    "version_id": self.version_id,
                })
            else:
                query = """
                ALTER TABLE heli_pandas
                UPDATE repair_days = %(repair_days)s
                WHERE serialno = %(serialno)s
                """
                self.client.execute(query, {
                    "repair_days": repair_days,
                    "serialno": serialno,
                })

            updated_count += 1
            self.logger.info(f"   ✅ ВС {serialno}: repair_days = {repair_days}")

        self.logger.info(f"✅ Обновлено {updated_count} записей с repair_days")
        return True
    
    def run(self):
        """Основной метод выполнения"""
        self.logger.info("🚀 === РАСЧЕТ REPAIR_DAYS ===")

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

        if self.dry_run:
            self.logger.info("📝 DRY-RUN: обновление repair_days не выполнялось")
        elif not self.update_repair_days(updates):
            return False

        self.logger.info("✅ === РАСЧЕТ ЗАВЕРШЕН ===")
        self.logger.info(f"📊 Итого записей с repair_days: {len(updates)}")

        return True


def main():
    """Основная функция"""
    parser = argparse.ArgumentParser(description='Расчет repair_days для ВС в ремонте')
    parser.add_argument('--version-date', type=str, help='Дата версии данных (YYYY-MM-DD)')
    parser.add_argument('--version-id', type=int, help='ID версии данных')
    parser.add_argument('--dry-run', action='store_true', help='Только вывести расчет без UPDATE')
    
    args = parser.parse_args()
    
    # Преобразуем строку даты в объект date
    version_date = None
    if args.version_date:
        version_date = datetime.strptime(args.version_date, '%Y-%m-%d').date()
    
    calculator = RepairDaysCalculator(
        version_date=version_date,
        version_id=args.version_id,
        dry_run=args.dry_run,
    )
    
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