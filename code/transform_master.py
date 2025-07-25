#!/usr/bin/env python3
"""
Transform Master - Оркестратор Transform этапа
Микросервисная архитектура ETL: Extract → Transform → Load

Дата создания: 24-07-2025
Автор: ETL Helicopter Project
Версия: 1.0

Роль: Координация всех Transform процессов
- Flame GPU симуляция планеров
- Постпроцессинг результатов
- Валидация выходных данных
- Передача в Load этап
"""

import sys
import os
import logging
from datetime import datetime, timedelta
import json
from typing import Dict, List, Optional, Tuple

# Добавляем пути для импортов
sys.path.append('/home/budnik_an/cube linux/cube/code')
sys.path.append('/home/budnik_an/cube linux/cube/config')

# Импорт модели Flame GPU
from flame_gpu_helicopter_model import FlameGPUHelicopterModel
sys.path.append('/home/budnik_an/cube linux/cube/code/utils')
from config_loader import get_clickhouse_client as get_clickhouse_connection

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/transform_master.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class TransformMaster:
    """
    Мастер-оркестратор Transform этапа
    
    Координирует:
    1. Flame GPU симуляцию планеров  
    2. Постпроцессинг LoggingLayer_Planes
    3. Валидацию результатов Transform
    4. Интеграцию с Load этапом
    """
    
    def __init__(self, simulation_config: Optional[Dict] = None):
        """
        Инициализация Transform Master
        
        Args:
            simulation_config: Конфигурация симуляции
        """
        self.session_id = f"TRANSFORM_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.start_time = datetime.now()
        
        # Конфигурация симуляции
        self.config = simulation_config or {
            'simulation_start_date': '2025-01-01',
            'simulation_days': 365,
            'validation_level': 'full',
            'export_to_clickhouse': True,
            'generate_reports': True
        }
        
        # Результаты этапов
        self.transform_results = {
            'flame_gpu_success': False,
            'postprocessing_success': False,
            'validation_success': False,
            'export_success': False,
            'total_records': 0,
            'total_agents': 0,
            'simulation_version_id': None
        }
        
        # Статистика выполнения
        self.execution_stats = {
            'extract_verification': {},
            'transform_duration': None,
            'validation_metrics': {},
            'performance_indicators': {}
        }
        
        logger.info(f"Transform Master инициализирован: {self.session_id}")
        logger.info(f"Конфигурация: {self.config}")

    def verify_extract_completion(self) -> bool:
        """
        Проверка завершения Extract этапа
        Валидация готовности входных данных для Transform
        
        Returns:
            bool: True если Extract завершен успешно
        """
        try:
            logger.info("🔍 ПРОВЕРКА: Готовность данных Extract этапа")
            
            client = get_clickhouse_connection()
            
            # Проверяем наличие обязательных таблиц
            required_tables = {
                'md_components': 'Нормативы компонентов',
                'heli_pandas': 'Данные планеров', 
                'flight_program_ac': 'Программы и триггеры',
                'flight_program_fl': 'Плановые налеты',
                'dict_digital_values_flat': 'Словарь field_id'
            }
            
            verification_results = {}
            
            for table, description in required_tables.items():
                try:
                    result = client.execute(f"SELECT COUNT(*) FROM {table}")
                    count = result[0][0]
                    verification_results[table] = {
                        'exists': True,
                        'count': count,
                        'description': description
                    }
                    logger.info(f"✅ {table}: {count} записей")
                    
                except Exception as e:
                    verification_results[table] = {
                        'exists': False,
                        'error': str(e),
                        'description': description
                    }
                    logger.error(f"❌ {table}: {e}")
            
            # Проверяем обязательные минимумы
            success_criteria = {
                'md_components': verification_results.get('md_components', {}).get('count', 0) > 0,
                'heli_pandas': verification_results.get('heli_pandas', {}).get('count', 0) > 0,
                'flight_program_ac': verification_results.get('flight_program_ac', {}).get('count', 0) > 0,
                'dict_digital_values_flat': verification_results.get('dict_digital_values_flat', {}).get('count', 0) > 0
            }
            
            extract_ready = all(success_criteria.values())
            
            self.execution_stats['extract_verification'] = {
                'verification_results': verification_results,
                'success_criteria': success_criteria,
                'extract_ready': extract_ready,
                'verification_time': datetime.now().isoformat()
            }
            
            if extract_ready:
                logger.info("✅ Extract этап: Данные готовы для Transform")
            else:
                logger.error("❌ Extract этап: Данные НЕ готовы")
                missing = [k for k, v in success_criteria.items() if not v]
                logger.error(f"Отсутствуют: {missing}")
            
            return extract_ready
            
        except Exception as e:
            logger.error(f"Ошибка проверки Extract: {e}")
            return False

    def execute_flame_gpu_simulation(self) -> bool:
        """
        Выполнение основной Flame GPU симуляции планеров
        
        Returns:
            bool: True если симуляция успешна
        """
        try:
            logger.info("🚁 FLAME GPU: Запуск симуляции планеров")
            
            simulation_start = datetime.now()
            
            # Создание модели с конфигурацией
            model = FlameGPUHelicopterModel(
                simulation_start_date=self.config['simulation_start_date'],
                simulation_days=self.config['simulation_days']
            )
            
            # Этап 1: Загрузка MacroProperty
            step1_start = datetime.now()
            macro_success = model.load_macro_properties()
            step1_duration = datetime.now() - step1_start
            logger.info(f"⏱️ Этап 1 (MacroProperty): {step1_duration}")
            
            if not macro_success:
                logger.error("❌ Ошибка загрузки MacroProperty")
                return False
            
            # Этап 2: Создание агентов
            step2_start = datetime.now()
            agents_success = model.create_agents()
            step2_duration = datetime.now() - step2_start
            logger.info(f"⏱️ Этап 2 (Агенты): {step2_duration}")
            
            if not agents_success:
                logger.error("❌ Ошибка создания агентов")
                return False
            
            # Этап 3: Симуляция
            step3_start = datetime.now()
            simulation_success = model.run_simulation()
            step3_duration = datetime.now() - step3_start
            logger.info(f"⏱️ Этап 3 (Симуляция): {step3_duration}")
            
            total_duration = datetime.now() - simulation_start
            
            if simulation_success:
                # Сохраняем результаты симуляции
                self.transform_results.update({
                    'flame_gpu_success': True,
                    'total_agents': len(model.agents),
                    'total_records': len(model.macro_property_2),
                    'simulation_version_id': model.version_id,
                    'step1_duration': str(step1_duration),
                    'step2_duration': str(step2_duration), 
                    'step3_duration': str(step3_duration),
                    'total_simulation_duration': str(total_duration)
                })
                
                # Сохраняем ссылку на модель для постпроцессинга
                self.flame_gpu_model = model
                
                logger.info(f"✅ Flame GPU: Симуляция завершена успешно")
                logger.info(f"📊 Агенты: {len(model.agents)}, Записи: {len(model.macro_property_2)}")
                logger.info(f"⏱️ Общая длительность: {total_duration}")
                
            else:
                logger.error("❌ Flame GPU: Симуляция завершена с ошибками")
            
            return simulation_success
            
        except Exception as e:
            logger.error(f"Ошибка Flame GPU симуляции: {e}")
            return False

    def execute_postprocessing(self) -> bool:
        """
        Постпроцессинг результатов симуляции
        Дополнительные вычисления и обогащение данных
        
        Returns:
            bool: True если постпроцессинг успешен
        """
        try:
            logger.info("⚙️ ПОСТПРОЦЕССИНГ: Обогащение результатов симуляции")
            
            if not hasattr(self, 'flame_gpu_model'):
                logger.error("Flame GPU модель недоступна для постпроцессинга")
                return False
            
            model = self.flame_gpu_model
            postprocessing_start = datetime.now()
            
            # 1. Коррекция active_trigger (ретроспективная)
            trigger_start = datetime.now()
            self._process_active_triggers(model)
            trigger_duration = datetime.now() - trigger_start
            logger.info(f"⏱️ Active triggers: {trigger_duration}")
            
            # 2. Обогащение дополнительными метриками
            enrich_start = datetime.now()
            self._enrich_logging_layer(model)
            enrich_duration = datetime.now() - enrich_start
            logger.info(f"⏱️ Enrichment: {enrich_duration}")
            
            # 3. Агрегированная статистика
            metrics_start = datetime.now()
            self._calculate_aggregate_metrics(model)
            metrics_duration = datetime.now() - metrics_start
            logger.info(f"⏱️ Metrics: {metrics_duration}")
            
            postprocessing_duration = datetime.now() - postprocessing_start
            
            self.transform_results['postprocessing_success'] = True
            self.execution_stats['postprocessing_duration'] = str(postprocessing_duration)
            self.execution_stats['postprocessing_breakdown'] = {
                'trigger_processing': str(trigger_duration),
                'enrichment': str(enrich_duration),
                'metrics_calculation': str(metrics_duration)
            }
            
            logger.info(f"✅ Постпроцессинг завершен за {postprocessing_duration}")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка постпроцессинга: {e}")
            return False

    def _process_active_triggers(self, model: FlameGPUHelicopterModel) -> None:
        """
        Обработка active_trigger с ретроспективной коррекцией
        
        Args:
            model: Модель Flame GPU
        """
        logger.info("Обработка active_trigger коррекций")
        
        corrections_count = 0
        
        for record in model.macro_property_2:
            trigger_value = record.get('active_trigger')
            if trigger_value is not None and trigger_value != 0:
                # Ретроспективная коррекция статусов
                target_date = record['active_trigger']
                aircraft_number = record['aircraft_number']
                
                # Находим записи для коррекции
                for correction_record in model.macro_property_2:
                    if (correction_record['aircraft_number'] == aircraft_number and
                        correction_record['dates'] == target_date):
                        correction_record['status_id'] = 4  # Ремонт
                        corrections_count += 1
                        break
        
        logger.info(f"Применено {corrections_count} active_trigger коррекций")

    def _enrich_logging_layer(self, model: FlameGPUHelicopterModel) -> None:
        """
        Обогащение LoggingLayer дополнительными полями
        
        Args:
            model: Модель Flame GPU
        """
        logger.info("Обогащение LoggingLayer дополнительными метриками")
        
        for record in model.macro_property_2:
            # Дополнительные вычисляемые поля
            record['utilization_factor'] = self._calculate_utilization(record)
            record['wear_indicator'] = self._calculate_wear_indicator(record)
            record['efficiency_score'] = self._calculate_efficiency_score(record)

    def _calculate_utilization(self, record: Dict) -> float:
        """Расчет коэффициента использования"""
        if record['daily_hours'] > 0 and record['status_id'] == 2:
            return min(record['daily_hours'] / 8.0, 1.0)  # Нормализация к 8 часам
        return 0.0

    def _calculate_wear_indicator(self, record: Dict) -> float:
        """Расчет индикатора износа"""
        sne_factor = record['sne'] / 3000.0 if record['sne'] > 0 else 0
        ppr_factor = record['ppr'] / 1500.0 if record['ppr'] > 0 else 0
        return min((sne_factor + ppr_factor) / 2.0, 1.0)

    def _calculate_efficiency_score(self, record: Dict) -> float:
        """Расчет оценки эффективности"""
        if record['status_id'] == 2:  # Операции
            return record['daily_hours'] / max(record['aircraft_age_years'], 1.0)
        return 0.0

    def _calculate_aggregate_metrics(self, model: FlameGPUHelicopterModel) -> None:
        """
        Расчет агрегированных метрик симуляции
        
        Args:
            model: Модель Flame GPU
        """
        logger.info("Расчет агрегированных метрик")
        
        import pandas as pd
        df = pd.DataFrame(model.macro_property_2)
        
        metrics = {
            'average_utilization': df['utilization_factor'].mean(),
            'average_wear': df['wear_indicator'].mean(),
            'operations_percentage': (df['status_id'] == 2).mean() * 100,
            'repair_percentage': (df['status_id'] == 4).mean() * 100,
            'total_flight_hours': df['daily_hours'].sum(),
            'unique_aircraft_count': df['aircraft_number'].nunique(),
            'simulation_coverage_days': df['dates'].nunique()
        }
        
        self.execution_stats['aggregate_metrics'] = metrics
        logger.info(f"Агрегированные метрики: {metrics}")

    def validate_transform_results(self) -> bool:
        """
        Валидация результатов Transform этапа
        
        Returns:
            bool: True если валидация прошла успешно
        """
        try:
            logger.info("🔍 ВАЛИДАЦИЯ: Проверка результатов Transform")
            
            if not hasattr(self, 'flame_gpu_model'):
                logger.error("Нет данных для валидации")
                return False
            
            model = self.flame_gpu_model
            validation_start = datetime.now()
            
            # Выполняем валидацию модели
            model_validation_start = datetime.now()
            validation_metrics = model.validate_results()
            model_validation_duration = datetime.now() - model_validation_start
            logger.info(f"⏱️ Model validation: {model_validation_duration}")
            
            # Дополнительные проверки Transform уровня
            checks_start = datetime.now()
            additional_checks = {
                'data_continuity': self._check_data_continuity(model),
                'status_transitions': self._check_status_transitions(model),
                'balance_compliance': self._check_balance_compliance(model),
                'resource_utilization': self._check_resource_utilization(model)
            }
            checks_duration = datetime.now() - checks_start
            logger.info(f"⏱️ Additional checks: {checks_duration}")
            
            validation_duration = datetime.now() - validation_start
            
            # Оценка успешности валидации
            validation_success = all([
                validation_metrics['total_records'] > 0,
                validation_metrics['unique_aircraft'] > 0,
                validation_metrics['simulation_days'] == self.config['simulation_days'],
                all(additional_checks.values())
            ])
            
            self.transform_results['validation_success'] = validation_success
            self.execution_stats['validation_metrics'] = {
                **validation_metrics,
                **additional_checks,
                'validation_duration': str(validation_duration),
                'model_validation_duration': str(model_validation_duration),
                'checks_duration': str(checks_duration)
            }
            
            if validation_success:
                logger.info("✅ Валидация: Результаты Transform корректны")
            else:
                logger.error("❌ Валидация: Обнаружены проблемы в результатах")
            
            return validation_success
            
        except Exception as e:
            logger.error(f"Ошибка валидации: {e}")
            return False

    def _check_data_continuity(self, model: FlameGPUHelicopterModel) -> bool:
        """Проверка непрерывности данных"""
        import pandas as pd
        df = pd.DataFrame(model.macro_property_2)
        
        expected_days = set(
            (model.simulation_start_date + timedelta(days=i)).strftime('%Y-%m-%d')
            for i in range(model.simulation_days)
        )
        actual_days = set(df['dates'].unique())
        
        return expected_days == actual_days

    def _check_status_transitions(self, model: FlameGPUHelicopterModel) -> bool:
        """Проверка корректности переходов статусов"""
        # Базовая проверка - все статусы в допустимом диапазоне
        import pandas as pd
        df = pd.DataFrame(model.macro_property_2)
        
        valid_statuses = {1, 2, 3, 4, 5, 6}
        actual_statuses = set(df['status_id'].unique())
        
        return actual_statuses.issubset(valid_statuses)

    def _check_balance_compliance(self, model: FlameGPUHelicopterModel) -> bool:
        """Проверка соответствия программе балансировки"""
        # Упрощенная проверка - есть агенты в операциях
        import pandas as pd
        df = pd.DataFrame(model.macro_property_2)
        
        ops_count = (df['status_id'] == 2).sum()
        return ops_count > 0

    def _check_resource_utilization(self, model: FlameGPUHelicopterModel) -> bool:
        """Проверка использования ресурсов"""
        # Проверяем что есть накопление наработки
        import pandas as pd
        df = pd.DataFrame(model.macro_property_2)
        
        max_sne = df['sne'].max()
        max_ppr = df['ppr'].max()
        
        return max_sne > 0 and max_ppr > 0

    def export_to_clickhouse(self) -> bool:
        """
        Экспорт результатов Transform в ClickHouse
        
        Returns:
            bool: True если экспорт успешен
        """
        try:
            logger.info("📤 ЭКСПОРТ: Сохранение результатов в ClickHouse")
            
            if not hasattr(self, 'flame_gpu_model'):
                logger.error("Нет данных для экспорта")
                return False
            
            # Используем метод экспорта модели
            export_start = datetime.now()
            export_success = self.flame_gpu_model.export_results()
            export_duration = datetime.now() - export_start
            logger.info(f"⏱️ Export duration: {export_duration}")
            
            self.transform_results['export_success'] = export_success
            
            if export_success:
                logger.info("✅ Экспорт: Данные сохранены в LoggingLayer_Planes")
            else:
                logger.error("❌ Экспорт: Ошибка сохранения в ClickHouse")
            
            return export_success
            
        except Exception as e:
            logger.error(f"Ошибка экспорта: {e}")
            return False

    def generate_transform_report(self) -> str:
        """
        Генерация отчета Transform этапа
        
        Returns:
            str: Путь к файлу отчета
        """
        try:
            report_file = f"logs/transform_report_{self.session_id}.json"
            
            # Генерируем отчет модели если доступна
            model_report_file = None
            if hasattr(self, 'flame_gpu_model'):
                model_report_file = self.flame_gpu_model.generate_report()
            
            # Завершаем статистику выполнения
            self.execution_stats['transform_duration'] = str(datetime.now() - self.start_time)
            
            transform_report = {
                'transform_master_info': {
                    'session_id': self.session_id,
                    'start_time': self.start_time.isoformat(),
                    'end_time': datetime.now().isoformat(),
                    'configuration': self.config
                },
                'transform_results': self.transform_results,
                'execution_statistics': self.execution_stats,
                'model_report_file': model_report_file,
                'success_summary': {
                    'extract_verified': bool(self.execution_stats.get('extract_verification', {}).get('extract_ready')),
                    'simulation_completed': self.transform_results['flame_gpu_success'],
                    'postprocessing_completed': self.transform_results['postprocessing_success'],
                    'validation_passed': self.transform_results['validation_success'],
                    'export_completed': self.transform_results['export_success'],
                    'overall_success': all([
                        self.transform_results['flame_gpu_success'],
                        self.transform_results['validation_success']
                    ])
                }
            }
            
            # Конвертер для numpy типов
            def convert_numpy_types(obj):
                if hasattr(obj, 'item'):
                    return obj.item()
                elif hasattr(obj, 'tolist'):
                    return obj.tolist()
                return obj
            
            with open(report_file, 'w', encoding='utf-8') as f:
                json.dump(transform_report, f, indent=2, ensure_ascii=False, default=convert_numpy_types)
            
            logger.info(f"📊 Отчет Transform сохранен: {report_file}")
            return report_file
            
        except Exception as e:
            logger.error(f"Ошибка генерации отчета: {e}")
            return ""

    def execute_full_transform(self) -> bool:
        """
        Выполнение полного цикла Transform этапа
        
        Returns:
            bool: True если Transform выполнен успешно
        """
        try:
            logger.info("🚀 СТАРТ: Transform Master - полный цикл")
            logger.info(f"Сессия: {self.session_id}")
            
            # Последовательность выполнения Transform
            steps = [
                ("Проверка Extract", self.verify_extract_completion),
                ("Flame GPU симуляция", self.execute_flame_gpu_simulation),
                ("Постпроцессинг", self.execute_postprocessing),
                ("Валидация результатов", self.validate_transform_results)
            ]
            
            # Опциональные шаги
            if self.config.get('export_to_clickhouse', True):
                steps.append(("Экспорт в ClickHouse", self.export_to_clickhouse))
            
            # Выполнение шагов
            success = True
            for step_name, step_func in steps:
                logger.info(f"▶️ Выполнение: {step_name}")
                step_success = step_func()
                
                if not step_success:
                    logger.error(f"❌ Ошибка на шаге: {step_name}")
                    success = False
                    break
                else:
                    logger.info(f"✅ Завершено: {step_name}")
            
            # Генерация отчета
            if self.config.get('generate_reports', True):
                report_file = self.generate_transform_report()
            
            # Финальное резюме
            if success:
                logger.info("🎯 Transform Master: ВСЕ ЭТАПЫ ЗАВЕРШЕНЫ УСПЕШНО")
                logger.info(f"📊 Агенты: {self.transform_results.get('total_agents', 0)}")
                logger.info(f"📋 Записи: {self.transform_results.get('total_records', 0)}")
                logger.info(f"🆔 Version: {self.transform_results.get('simulation_version_id', 'N/A')}")
                
            else:
                logger.error("❌ Transform Master: ЗАВЕРШЕН С ОШИБКАМИ")
            
            return success
            
        except Exception as e:
            logger.error(f"Критическая ошибка Transform Master: {e}")
            return False


def main():
    """
    Основная функция запуска Transform Master
    """
    try:
        # Конфигурация по умолчанию
        default_config = {
            'simulation_start_date': '2025-01-01',
            'simulation_days': 365,
            'validation_level': 'full',
            'export_to_clickhouse': True,
            'generate_reports': True
        }
        
        # Создание и запуск Transform Master
        transform_master = TransformMaster(default_config)
        success = transform_master.execute_full_transform()
        
        if success:
            print("\n" + "="*70)
            print("🎯 TRANSFORM MASTER - УСПЕШНОЕ ЗАВЕРШЕНИЕ")
            print("="*70)
            print(f"Session ID: {transform_master.session_id}")
            print(f"Agents: {transform_master.transform_results.get('total_agents', 0)}")
            print(f"Records: {transform_master.transform_results.get('total_records', 0)}")
            print(f"Duration: {transform_master.execution_stats.get('transform_duration', 'N/A')}")
            print("="*70)
            print("🔄 Готово для Load этапа")
            print("="*70)
        else:
            print("\n❌ TRANSFORM MASTER - ЗАВЕРШЕН С ОШИБКАМИ")
            
    except Exception as e:
        logger.error(f"Критическая ошибка main: {e}")


if __name__ == "__main__":
    main() 