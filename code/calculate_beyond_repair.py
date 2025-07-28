#!/usr/bin/env python3
"""
Расчет поля Beyond Repair (br) в таблице md_components
Вычисляет экономический порог списания для каждого партномера
"""

import sys
import logging
from pathlib import Path
from typing import Dict, List, Tuple, Any

# Добавляем путь к утилитам
sys.path.append(str(Path(__file__).parent / 'utils'))
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
    
    def add_br_column(self) -> bool:
        """Добавление колонки br в md_components"""
        self.logger.info("🔧 Добавление колонки br в md_components...")
        
        try:
            # Добавляем колонку если её нет
            alter_query = "ALTER TABLE md_components ADD COLUMN IF NOT EXISTS br Nullable(UInt16) DEFAULT NULL"
            self.client.execute(alter_query)
            
            self.logger.info("✅ Колонка br (UInt16) добавлена в md_components")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка добавления колонки: {e}")
            return False
    
    def analyze_components_data(self) -> Dict[str, Dict]:
        """Анализ данных компонентов для расчета BR"""
        self.logger.info("📊 Анализ данных компонентов...")
        
        try:
            # Получаем компоненты с полными данными для расчета BR
            components_result = self.client.execute("""
                SELECT 
                    partno,
                    ll_mi8, ll_mi17,
                    oh_threshold_mi8, oh_mi17,
                    repair_price,
                    purchase_price,
                    ac_type_mask
                FROM md_components 
                WHERE purchase_price > 0 
                  AND repair_price > 0 
                  AND (ll_mi8 > 0 OR ll_mi17 > 0)
                  AND (oh_threshold_mi8 > 0 OR oh_mi17 > 0)
                ORDER BY partno
            """)
            
            components_data = {}
            for row in components_result:
                partno, ll_mi8, ll_mi17, oh_mi8, oh_mi17, repair_price, purchase_price, ac_type_mask = row
                
                components_data[partno] = {
                    'll_mi8': ll_mi8 or 0,
                    'll_mi17': ll_mi17 or 0,
                    'oh_threshold_mi8': oh_mi8 or 0,
                    'oh_threshold_mi17': oh_mi17 or 0,
                    'repair_price': repair_price,
                    'purchase_price': purchase_price,
                    'ac_type_mask': ac_type_mask
                }
            
            self.logger.info(f"📋 Найдено {len(components_data)} компонентов с полными данными")
            
            # Показываем примеры
            self.logger.info("📋 Примеры компонентов для расчета:")
            for i, (partno, data) in enumerate(list(components_data.items())[:3]):
                self.logger.info(f"   {partno}: repair={data['repair_price']:,.0f}, purchase={data['purchase_price']:,.0f}")
            
            return components_data
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка анализа данных: {e}")
            return {}
    
    def calculate_beyond_repair(self, nr: float, mrr: float, repair_price: float, purchase_price: float) -> float:
        """
        Расчет Beyond Repair по формуле из архива
        
        BR = NR - (RepairPrice / ((PurchasePrice - RepairPrice) / NR + RepairPrice / MRR))
        
        Args:
            nr: Назначенный ресурс (ll)
            mrr: Межремонтный ресурс (oh_threshold) 
            repair_price: Стоимость ремонта
            purchase_price: Стоимость покупки
            
        Returns:
            int: Пороговая наработка Beyond Repair (округленная до целых)
        """
        try:
            if nr <= 0 or mrr <= 0 or purchase_price <= 0 or repair_price <= 0:
                return None
            
            # Проверяем что ремонт дешевле покупки
            if repair_price >= purchase_price:
                return 0  # Ремонт дороже покупки - сразу Beyond Repair
            
            # Формула из архива
            denominator = (purchase_price - repair_price) / nr + repair_price / mrr
            
            if denominator <= 0:
                return None
                
            equivalent_resource = repair_price / denominator
            br = nr - equivalent_resource
            
            # BR не может быть отрицательным или больше NR
            br_final = max(0, min(br, nr))
            # Округляем до целых для UInt16
            return round(br_final)
            
        except (ZeroDivisionError, TypeError):
            return None
    
    def calculate_br_for_components(self, components_data: Dict[str, Dict]) -> Dict[str, int]:
        """Расчет BR для всех компонентов"""
        self.logger.info("🧮 Расчет Beyond Repair для компонентов...")
        
        br_results = {}
        calculated_count = 0
        
        for partno, data in components_data.items():
            # Определяем какие данные использовать (Ми-8 или Ми-17)
            # Используем данные для Ми-8 как базовые, если есть
            if data['ll_mi8'] > 0 and data['oh_threshold_mi8'] > 0:
                nr = data['ll_mi8']
                mrr = data['oh_threshold_mi8']
                ac_type = 'Ми-8'
            elif data['ll_mi17'] > 0 and data['oh_threshold_mi17'] > 0:
                nr = data['ll_mi17'] 
                mrr = data['oh_threshold_mi17']
                ac_type = 'Ми-17'
            else:
                continue
            
            # Рассчитываем BR
            br_value = self.calculate_beyond_repair(
                nr, mrr, data['repair_price'], data['purchase_price']
            )
            
            if br_value is not None:
                br_results[partno] = br_value
                calculated_count += 1
                
                # Логируем первые несколько примеров
                if calculated_count <= 5:
                    repair_pct = data['repair_price'] / data['purchase_price'] * 100
                    self.logger.info(f"   {partno} ({ac_type}): NR={nr:.0f}h, MRR={mrr:.0f}h, " +
                                   f"ремонт={repair_pct:.1f}% → BR={br_value:.0f}h")
        
        self.logger.info(f"✅ Рассчитано BR для {calculated_count} компонентов")
        return br_results
    
    def update_br_in_database(self, br_results: Dict[str, int]) -> bool:
        """Обновление поля br в md_components"""
        self.logger.info("💾 Обновление поля br в md_components...")
        
        try:
            # Сначала очищаем поле
            self.client.execute("ALTER TABLE md_components UPDATE br = NULL WHERE 1=1")
            
            # Обновляем значения BR
            updated_count = 0
            for partno, br_value in br_results.items():
                update_query = f"""
                ALTER TABLE md_components 
                UPDATE br = {br_value}
                WHERE partno = '{partno}'
                """
                self.client.execute(update_query)
                updated_count += 1
            
            self.logger.info(f"✅ Обновлено {updated_count} записей с BR")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка обновления: {e}")
            return False
    
    def verify_br_calculation(self) -> bool:
        """Проверка качества расчета BR"""
        self.logger.info("🔍 Проверка качества расчета BR...")
        
        try:
            # Статистика BR
            stats_result = self.client.execute("""
                SELECT 
                    COUNT(*) as total_components,
                    countIf(br IS NOT NULL) as with_br,
                    AVG(br) as avg_br,
                    MIN(br) as min_br,
                    MAX(br) as max_br
                FROM md_components 
                WHERE purchase_price > 0 AND repair_price > 0
            """)
            
            total, with_br, avg_br, min_br, max_br = stats_result[0]
            coverage = (with_br / total) * 100 if total > 0 else 0
            
            self.logger.info(f"📊 Статистика BR:")
            self.logger.info(f"   Всего компонентов с ценами: {total}")
            self.logger.info(f"   С рассчитанным BR: {with_br} ({coverage:.1f}%)")
            if avg_br is not None:
                self.logger.info(f"   Средний BR: {avg_br:.0f} часов")
                self.logger.info(f"   Диапазон BR: {min_br:.0f} - {max_br:.0f} часов")
            
            # Примеры с BR
            examples_result = self.client.execute("""
                SELECT partno, purchase_price, repair_price, br,
                       (repair_price / purchase_price * 100) as repair_pct
                FROM md_components 
                WHERE br IS NOT NULL
                ORDER BY br DESC
                LIMIT 5
            """)
            
            self.logger.info("📋 Примеры рассчитанных BR:")
            for row in examples_result:
                partno, purchase, repair, br, repair_pct = row
                self.logger.info(f"   {partno}: BR={br:.0f}h (ремонт {repair_pct:.1f}%)")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка проверки: {e}")
            return False
    
    def run_calculation(self) -> bool:
        """Запуск полного расчета Beyond Repair"""
        self.logger.info("🚀 Запуск расчета Beyond Repair для md_components")
        
        try:
            # 1. Подключение
            if not self.connect_to_database():
                return False
            
            # 2. Добавление колонки br
            if not self.add_br_column():
                return False
            
            # 3. Анализ данных компонентов
            components_data = self.analyze_components_data()
            if not components_data:
                self.logger.error("❌ Нет данных для расчета BR")
                return False
            
            # 4. Расчет BR
            br_results = self.calculate_br_for_components(components_data)
            if not br_results:
                self.logger.error("❌ Не удалось рассчитать BR")
                return False
            
            # 5. Обновление базы данных
            if not self.update_br_in_database(br_results):
                return False
            
            # 6. Проверка качества
            if not self.verify_br_calculation():
                return False
            
            self.logger.info("🎯 РАСЧЕТ BEYOND REPAIR ЗАВЕРШЕН!")
            self.logger.info("📊 Поле br добавлено в md_components")
            self.logger.info("🚀 Master data готова для Flame GPU environment")
            
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