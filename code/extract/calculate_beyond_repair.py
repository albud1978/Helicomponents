#!/usr/bin/env python3
"""
Расчет полей Beyond Repair по типам в таблице md_components: br_mi8, br_mi17
Поле br (единое) более не используется и не заполняется.
"""

import sys
import logging
from pathlib import Path
from typing import Dict, List, Tuple, Any

# Добавляем пути к utils и общему коду
code_root = Path(__file__).resolve().parents[1]
sys.path.append(str(code_root / 'utils'))
sys.path.append(str(code_root))
from config_loader import get_clickhouse_client

class BeyondRepairCalculator:
    """Калькулятор Beyond Repair для md_components"""
    
    def __init__(self):
        """Инициализация калькулятора"""
        self.logger = self._setup_logging()
        self.client = None
    
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
            self.client = get_clickhouse_client()
            result = self.client.execute('SELECT 1 as test')
            self.logger.info(f"✅ Подключение к ClickHouse успешно!")
            return True
        except Exception as e:
            self.logger.error(f"❌ Ошибка подключения: {e}")
            return False

    def add_br_columns(self) -> bool:
        """Добавление колонок br_mi8 и br_mi17 в md_components"""
        self.logger.info("🔧 Добавление колонок br_mi8/br_mi17 в md_components...")
        try:
            self.client.execute("ALTER TABLE md_components ADD COLUMN IF NOT EXISTS br_mi8 Nullable(UInt32) DEFAULT NULL")
            self.client.execute("ALTER TABLE md_components ADD COLUMN IF NOT EXISTS br_mi17 Nullable(UInt32) DEFAULT NULL")
            self.logger.info("✅ Колонки br_mi8/br_mi17 добавлены в md_components")
            return True
        except Exception as e:
            self.logger.error(f"❌ Ошибка добавления колонок: {e}")
            return False

    def update_br_in_database(self) -> bool:
        """Массовое обновление полей br_mi8/br_mi17 в md_components"""
        self.logger.info("💾 Массовое обновление br_mi8/br_mi17 в md_components...")
        try:
            # Ми-8 → минуты (ll/oh уже в минутах; считаем BR в минутах без доп. умножения)
            self.client.execute(
                """
                ALTER TABLE md_components UPDATE
                  br_mi8 = if(
                    ll_mi8 > 0 AND oh_mi8 > 0 AND purchase_price > 0 AND repair_price > 0,
                    toUInt32(
                      round(
                        greatest(
                          0.0,
                          least(
                            ( toFloat64(ll_mi8) - (
                              toFloat64(repair_price) / greatest(
                                ((toFloat64(purchase_price) - toFloat64(repair_price)) / toFloat64(ll_mi8))
                                + (toFloat64(repair_price) / toFloat64(oh_mi8)),
                                1e-6
                              )
                            ) ),
                            toFloat64(ll_mi8)
                          )
                        )
                      )
                    ),
                    NULL
                  )
                WHERE 1
                """
            )

            # Ми-17 → минуты (ll/oh уже в минутах)
            self.client.execute(
                """
                ALTER TABLE md_components UPDATE
                  br_mi17 = if(
                    ll_mi17 > 0 AND oh_mi17 > 0 AND purchase_price > 0 AND repair_price > 0,
                    toUInt32(
                      round(
                        greatest(
                          0.0,
                          least(
                            ( toFloat64(ll_mi17) - (
                              toFloat64(repair_price) / greatest(
                                ((toFloat64(purchase_price) - toFloat64(repair_price)) / toFloat64(ll_mi17))
                                + (toFloat64(repair_price) / toFloat64(oh_mi17)),
                                1e-6
                              )
                            ) ),
                            toFloat64(ll_mi17)
                          )
                        )
                      )
                    ),
                    NULL
                  )
                WHERE 1
                """
            )

            # Невыгодный ремонт → 0
            self.client.execute(
                """
                ALTER TABLE md_components UPDATE
                  br_mi8  = if(repair_price >= purchase_price AND ll_mi8  > 0 AND oh_mi8  > 0, toUInt32(0), br_mi8),
                  br_mi17 = if(repair_price >= purchase_price AND ll_mi17 > 0 AND oh_mi17 > 0, toUInt32(0), br_mi17)
                WHERE 1
                """
            )

            # Неремонтопригодные компоненты: ll = oh → br = 0
            # Такие компоненты не ремонтируются, при поломке сразу в хранение
            self.logger.info("🔧 Обработка неремонтопригодных компонентов (ll = oh)...")
            
            # Ми-8: если ll = oh и br ещё не заполнен → br = 0
            self.client.execute(
                """
                ALTER TABLE md_components UPDATE
                  br_mi8 = 0
                WHERE ll_mi8 > 0 AND ll_mi8 = oh_mi8 AND br_mi8 IS NULL
                """
            )
            
            # Ми-17: если ll = oh и br ещё не заполнен → br = 0
            self.client.execute(
                """
                ALTER TABLE md_components UPDATE
                  br_mi17 = 0
                WHERE ll_mi17 > 0 AND ll_mi17 = oh_mi17 AND br_mi17 IS NULL
                """
            )
            
            self.logger.info("✅ Неремонтопригодные компоненты обработаны (br = 0)")

            self.logger.info("✅ Массовое обновление br_mi8/br_mi17 выполнено (единицы: минуты)")
            return True
        except Exception as e:
            self.logger.error(f"❌ Ошибка обновления: {e}")
            return False

    def verify_br_calculation(self) -> bool:
        """Проверка качества расчёта BR по типам"""
        self.logger.info("🔍 Проверка качества расчёта BR (br_mi8/br_mi17)...")
        try:
            stats_result = self.client.execute(
                """
                SELECT 
                    COUNT(*) as total_components,
                    countIf(br_mi8 IS NOT NULL)  as with_br_mi8,
                    countIf(br_mi17 IS NOT NULL) as with_br_mi17,
                    MIN(br_mi8)  as mi8_min,
                    MAX(br_mi8)  as mi8_max,
                    MIN(br_mi17) as mi17_min,
                    MAX(br_mi17) as mi17_max
                FROM md_components 
                """
            )

            total, with_mi8, with_mi17, mi8_min, mi8_max, mi17_min, mi17_max = stats_result[0]
            self.logger.info(f"📊 Статистика BR (в минутах):")
            self.logger.info(f"   Всего компонентов: {total}")
            self.logger.info(f"   br_mi8: рассчитано {with_mi8}, диапазон [{mi8_min}, {mi8_max}]")
            self.logger.info(f"   br_mi17: рассчитано {with_mi17}, диапазон [{mi17_min}, {mi17_max}]")

            # Проверка инвариантов br <= ll (все в минутах)
            inv = self.client.execute(
                """
                SELECT 
                  sum(br_mi8  > ll_mi8)  as mi8_viol,
                  sum(br_mi17 > ll_mi17) as mi17_viol
                FROM md_components
                """
            )[0]
            self.logger.info(f"   Инварианты: mi8_viol={inv[0]}, mi17_viol={inv[1]}")
            return True
        except Exception as e:
            self.logger.error(f"❌ Ошибка проверки: {e}")
            return False

    def run_calculation(self) -> bool:
        """Запуск полного расчета Beyond Repair"""
        self.logger.info("🚀 Запуск расчёта BR по типам для md_components")
        try:
            if not self.connect_to_database():
                return False
            if not self.add_br_columns():
                return False
            if not self.update_br_in_database():
                return False
            if not self.verify_br_calculation():
                return False
            self.logger.info("🎯 Расчёт BR по типам завершён!")
            self.logger.info("📊 Поля br_mi8/br_mi17 (в минутах) заполнены в md_components")
            self.logger.info("🚀 Master data готова для MacroProperty1 (без поля br)")
            return True
        except Exception as e:
            self.logger.error(f"❌ Ошибка расчета: {e}")
            return False

def main():
    """Основная функция"""
    calculator = BeyondRepairCalculator()
    return 0 if calculator.run_calculation() else 1

if __name__ == "__main__":
    exit(main()) 