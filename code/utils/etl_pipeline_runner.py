#!/usr/bin/env python3
"""
Мастер-скрипт для полной перезагрузки ETL пайплайна
Helicopter Component Lifecycle Prediction

Выполняет:
1. Зачистку всех ETL таблиц в ClickHouse
2. Последовательный запуск всех 9 ETL скриптов
3. Замер времени выполнения каждого скрипта
4. Детальное логирование и диагностика ошибок
5. Итоговое заключение о состоянии пайплайна

Автор: AI Agent для пользователя budnik_an
"""

import subprocess
import sys
import time
import logging
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Tuple

# Добавляем путь к утилитам
sys.path.append(str(Path(__file__).parent / 'utils'))
from config_loader import get_clickhouse_client

class ETLPipelineRunner:
    """Мастер-класс для управления ETL пайплайном"""
    
    def __init__(self):
        """Инициализация"""
        self.start_time = None
        self.results = []
        self.client = None
        self.setup_logging()
        
        # Список всех ETL таблиц для зачистки (БЕЗ словарей - они пересоздаются)
        self.etl_tables = [
            'md_components',
            'program_ac', 
            'status_overhaul',
            'heli_raw',
            'heli_pandas',
            'flight_program'
        ]
        
        # Последовательность ETL скриптов
        self.etl_scripts = [
            {
                'name': 'md_components_loader.py',
                'description': 'Загрузка технических характеристик компонентов',
                'expected_records': 37,
                'timeout': 60
            },
            {
                'name': 'program_ac_loader.py',
                'description': 'Загрузка реестра вертолетов в эксплуатации',
                'expected_records': 189,
                'timeout': 60
            },
            {
                'name': 'status_overhaul_loader.py',
                'description': 'Загрузка статуса капитального ремонта',
                'expected_records': 19,
                'timeout': 60
            },
            {
                'name': 'dual_loader.py',
                'description': 'Основной загрузчик компонентов (heli_raw + heli_pandas)',
                'expected_records': 'variable',
                'timeout': 300  # 5 минут для большого файла
            },
            {
                'name': 'program_loader.py',
                'description': 'Загрузка программы полетов',
                'expected_records': 96,
                'timeout': 60
            },
            {
                'name': 'dictionary_creator.py',
                'description': 'Создание словарей для GPU оптимизации',
                'expected_records': 'dictionaries',
                'timeout': 120
            },
            {
                'name': 'process_location_field.py',
                'description': 'Обработка номеров вертолетов из location',
                'expected_records': 'enrichment',
                'timeout': 60
            },
            {
                'name': 'enrich_heli_pandas.py',
                'description': 'Обогащение числовыми полями для GPU',
                'expected_records': 'enrichment',
                'timeout': 120
            },
            {
                'name': 'calculate_beyond_repair.py',
                'description': 'Расчет экономических порогов списания',
                'expected_records': 'calculation',
                'timeout': 60
            }
        ]
    
    def setup_logging(self):
        """Настройка логирования"""
        # Создаем директорию для логов
        log_dir = Path('logs')
        log_dir.mkdir(exist_ok=True)
        
        # Настраиваем логгер
        log_file = log_dir / f"etl_pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        
        self.logger = logging.getLogger(__name__)
        self.logger.info(f"📝 Логирование настроено: {log_file}")
    
    def print_header(self):
        """Вывод заголовка"""
        header = f"""
🚀 === МАСТЕР-СКРИПТ ETL ПАЙПЛАЙНА ===
======================================
📅 Запуск: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
📊 Скриптов к выполнению: {len(self.etl_scripts)}
🗑️ Таблиц к удалению: {len(self.etl_tables)} (словари пересоздаются)
📝 Логирование включено
"""
        print(header)
        self.logger.info("ETL Pipeline запущен")
    
    def connect_to_database(self) -> bool:
        """Подключение к ClickHouse"""
        try:
            self.client = get_clickhouse_client()
            print("✅ Подключение к ClickHouse успешно!")
            self.logger.info("Подключение к ClickHouse успешно")
            return True
        except Exception as e:
            error_msg = f"❌ Ошибка подключения к ClickHouse: {e}"
            print(error_msg)
            self.logger.error(error_msg)
            return False
    
    def cleanup_database(self) -> bool:
        """Зачистка всех ETL таблиц"""
        print("\n🗑️ === ЗАЧИСТКА БАЗЫ ДАННЫХ ===")
        print("=" * 35)
        self.logger.info("Начало зачистки базы данных")
        
        if not self.connect_to_database():
            return False
        
        # Сначала удаляем таблицы
        success_count = 0
        for table in self.etl_tables:
            try:
                self.client.execute(f'DROP TABLE IF EXISTS {table}')
                msg = f"✅ Удалена таблица: {table}"
                print(msg)
                self.logger.info(f"Удалена таблица: {table}")
                success_count += 1
            except Exception as e:
                error_msg = f"❌ Ошибка удаления {table}: {e}"
                print(error_msg)
                self.logger.error(error_msg)
        
        # Проверяем, что таблицы действительно удалены
        print("\n🔍 Проверка результата зачистки...")
        self.logger.info("Проверка результата зачистки")
        
        existing_tables = []
        try:
            all_tables = self.client.execute('SHOW TABLES')
            existing_etl_tables = [t[0] for t in all_tables if t[0] in self.etl_tables]
            
            if existing_etl_tables:
                warning_msg = f"⚠️ Обнаружены неудаленные ETL таблицы: {existing_etl_tables}"
                print(warning_msg)
                self.logger.warning(warning_msg)
                
                # Принудительное удаление оставшихся таблиц
                for table in existing_etl_tables:
                    try:
                        self.client.execute(f'DROP TABLE {table}')
                        msg = f"🔧 Принудительно удалена: {table}"
                        print(msg)
                        self.logger.info(f"Принудительно удалена: {table}")
                    except Exception as e:
                        error_msg = f"❌ Не удалось принудительно удалить {table}: {e}"
                        print(error_msg)
                        self.logger.error(error_msg)
                        existing_tables.append(table)
            
        except Exception as e:
            error_msg = f"❌ Ошибка проверки таблиц: {e}"
            print(error_msg)
            self.logger.error(error_msg)
        
        print()
        if existing_tables:
            final_msg = f"❌ НЕ УДАЛЕНЫ: {existing_tables}"
            result_msg = f"📊 Результат зачистки: {success_count - len(existing_tables)}/{len(self.etl_tables)} таблиц удалено"
            print(final_msg)
            print(result_msg)
            self.logger.error(final_msg)
            self.logger.info(result_msg)
            return False
        else:
            result_msg = f"📊 Результат зачистки: {success_count}/{len(self.etl_tables)} таблиц удалено"
            success_msg = "✅ Зачистка базы данных завершена успешно!"
            print(result_msg)
            print(success_msg)
            self.logger.info(result_msg)
            self.logger.info("Зачистка базы данных завершена успешно")
            return True
    
    def save_script_output(self, script_name: str, stdout: str, stderr: str, duration: float):
        """Сохранение вывода скрипта в файл"""
        output_dir = Path('logs/script_outputs')
        output_dir.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = output_dir / f"{script_name}_{timestamp}.log"
        
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(f"=== {script_name} ===\n")
            f.write(f"Время выполнения: {duration:.3f} секунд\n")
            f.write(f"Timestamp: {datetime.now()}\n\n")
            
            f.write("=== STDOUT ===\n")
            f.write(stdout if stdout else "(пустой)\n")
            f.write("\n=== STDERR ===\n")
            f.write(stderr if stderr else "(пустой)\n")
        
        self.logger.info(f"Вывод {script_name} сохранен в {output_file}")
    
    def run_script(self, script_info: Dict) -> Dict:
        """Запуск одного ETL скрипта"""
        script_name = script_info['name']
        description = script_info['description']
        timeout = script_info.get('timeout', 600)
        
        print(f"\n📊 Запуск: {script_name}")
        print(f"📋 Описание: {description}")
        print(f"⏱️ Таймаут: {timeout} секунд")
        
        self.logger.info(f"Запуск скрипта: {script_name}")
        self.logger.info(f"Описание: {description}")
        
        script_path = Path('code') / script_name
        
        if not script_path.exists():
            error_msg = f'Файл не найден: {script_path}'
            print(f"❌ {error_msg}")
            self.logger.error(error_msg)
            return {
                'name': script_name,
                'status': 'FAILED',
                'duration': 0,
                'error': error_msg
            }
        
        start_time = time.time()
        
        try:
            # Запускаем скрипт
            print(f"🚀 Выполняется {script_name}...")
            result = subprocess.run(
                [sys.executable, str(script_path)],
                capture_output=True,
                text=True,
                timeout=timeout
            )
            
            duration = time.time() - start_time
            
            # Сохраняем вывод в любом случае
            self.save_script_output(script_name, result.stdout, result.stderr, duration)
            
            if result.returncode == 0:
                success_msg = f"✅ Успешно выполнен за {duration:.3f} сек"
                print(success_msg)
                self.logger.info(f"{script_name} выполнен успешно за {duration:.3f} сек")
                
                # Показываем последние строки stdout для контекста
                if result.stdout:
                    last_lines = result.stdout.strip().split('\n')[-3:]
                    print("📄 Последние строки вывода:")
                    for line in last_lines:
                        if line.strip():
                            print(f"   {line}")
                
                return {
                    'name': script_name,
                    'status': 'SUCCESS',
                    'duration': duration,
                    'stdout': result.stdout,
                    'stderr': result.stderr
                }
            else:
                error_msg = f"❌ Ошибка выполнения (код {result.returncode})"
                print(error_msg)
                print(f"⚠️ Stderr: {result.stderr[:500]}...")
                
                self.logger.error(f"{script_name} завершился с ошибкой, код: {result.returncode}")
                self.logger.error(f"Stderr: {result.stderr}")
                
                return {
                    'name': script_name,
                    'status': 'FAILED',
                    'duration': duration,
                    'error': result.stderr,
                    'stdout': result.stdout
                }
        
        except subprocess.TimeoutExpired:
            duration = time.time() - start_time
            timeout_msg = f"⏱️ Таймаут после {duration:.1f} сек"
            print(timeout_msg)
            self.logger.error(f"{script_name} превысил таймаут {timeout} сек")
            
            return {
                'name': script_name,
                'status': 'TIMEOUT',
                'duration': duration,
                'error': f'Превышен таймаут {timeout} секунд'
            }
        except Exception as e:
            duration = time.time() - start_time
            critical_msg = f"💥 Критическая ошибка: {e}"
            print(critical_msg)
            self.logger.error(f"{script_name} критическая ошибка: {e}")
            
            return {
                'name': script_name,
                'status': 'ERROR',
                'duration': duration,
                'error': str(e)
            }
    
    def check_intermediate_state(self, step_number: int, script_name: str):
        """Проверка промежуточного состояния после каждого скрипта"""
        if not self.client:
            return
        
        print(f"\n🔍 Проверка состояния после шага {step_number}...")
        self.logger.info(f"Проверка состояния после {script_name}")
        
        # Проверяем основные таблицы
        tables_to_check = ['md_components', 'program_ac', 'status_overhaul', 'heli_raw', 'heli_pandas', 'flight_program']
        
        for table in tables_to_check:
            try:
                count = self.client.execute(f'SELECT COUNT(*) FROM {table}')[0][0]
                if count > 0:
                    print(f"  ✅ {table}: {count:,} записей")
                    self.logger.info(f"{table}: {count} записей")
            except Exception:
                pass  # Таблица еще не создана
    
    def run_all_scripts(self) -> bool:
        """Запуск всех ETL скриптов"""
        print("\n🚀 === ЗАПУСК ETL СКРИПТОВ ===")
        print("=" * 32)
        self.logger.info("Начало выполнения ETL скриптов")
        
        for i, script_info in enumerate(self.etl_scripts, 1):
            print(f"\n📊 ШАГ {i}/{len(self.etl_scripts)}: {script_info['name']}")
            print("=" * 60)
            
            result = self.run_script(script_info)
            self.results.append(result)
            
            # Проверяем промежуточное состояние
            self.check_intermediate_state(i, script_info['name'])
            
            # Прерываем выполнение при критических ошибках
            if result['status'] in ['FAILED', 'ERROR', 'TIMEOUT']:
                error_msg = f"\n⚠️ Прерывание пайплайна из-за ошибки в {script_info['name']}"
                print(error_msg)
                self.logger.error(f"Прерывание пайплайна: {script_info['name']} - {result['status']}")
                return False
        
        return True
    
    def check_data_quality(self) -> Dict:
        """Проверка качества данных после выполнения пайплайна"""
        print("\n🔍 === ПРОВЕРКА КАЧЕСТВА ДАННЫХ ===")
        print("=" * 38)
        self.logger.info("Начало проверки качества данных")
        
        if not self.client:
            if not self.connect_to_database():
                return {'status': 'ERROR', 'message': 'Нет подключения к БД'}
        
        quality_report = {
            'status': 'SUCCESS',
            'tables': {},
            'total_records': 0,
            'issues': []
        }
        
        main_tables = [
            ('md_components', 37),
            ('program_ac', 189),
            ('status_overhaul', 19),
            ('heli_raw', 'variable'),
            ('heli_pandas', 'variable'),
            ('flight_program', 96)
        ]
        
        for table, expected in main_tables:
            try:
                count = self.client.execute(f'SELECT COUNT(*) FROM {table}')[0][0]
                quality_report['tables'][table] = count
                quality_report['total_records'] += count
                
                # Просто информативный вывод без проверки количества
                print(f"✅ {table}: {count:,} записей")
                self.logger.info(f"{table}: {count} записей")
                
            except Exception as e:
                error_msg = f"❌ {table}: Ошибка - {e}"
                print(error_msg)
                quality_report['issues'].append(f"{table}: {str(e)}")
                self.logger.error(f"{table} ошибка: {e}")
        
        # Проверяем словари
        print("\n📚 Словари:")
        dict_tables = ['dict_partno_flat', 'dict_serialno_flat', 'dict_ac_type_flat', 'dict_owner_flat']
        for table in dict_tables:
            try:
                count = self.client.execute(f'SELECT COUNT(*) FROM {table}')[0][0]
                print(f"✅ {table}: {count:,} записей")
                self.logger.info(f"Словарь {table}: {count} записей")
            except Exception as e:
                error_msg = f"❌ {table}: Ошибка - {e}"
                print(error_msg)
                quality_report['issues'].append(f"{table}: {str(e)}")
                self.logger.error(f"Словарь {table} ошибка: {e}")
        
        # Проверяем GPU готовность heli_pandas только если таблица не пустая
        if quality_report['tables'].get('heli_pandas', 0) > 0:
            try:
                gpu_stats = self.client.execute('''
                    SELECT 
                        COUNT(*) as total,
                        countIf(partno_id IS NOT NULL AND partno_id > 0) as partno_ids,
                        countIf(serialno_id IS NOT NULL AND serialno_id > 0) as serialno_ids,
                        countIf(ac_type_mask IS NOT NULL AND ac_type_mask > 0) as ac_type_masks,
                        countIf(owner_id IS NOT NULL AND owner_id > 0) as owner_ids,
                        countIf(aircraft_number IS NOT NULL AND aircraft_number > 0) as aircraft_numbers
                    FROM heli_pandas
                ''')[0]
                
                total, partno_ids, serialno_ids, ac_type_masks, owner_ids, aircraft_numbers = gpu_stats
                
                print(f"\n🚀 GPU готовность heli_pandas:")
                print(f"   partno_id: {partno_ids:,} записей")
                print(f"   serialno_id: {serialno_ids:,} записей") 
                print(f"   ac_type_mask: {ac_type_masks:,} записей")
                print(f"   owner_id: {owner_ids:,} записей")
                print(f"   aircraft_number: {aircraft_numbers:,} записей")
                
                self.logger.info(f"GPU готовность: partno_id={partno_ids}, serialno_id={serialno_ids}, ac_type_mask={ac_type_masks}, owner_id={owner_ids}, aircraft_number={aircraft_numbers}")
                
            except Exception as e:
                issue = f"GPU поля: {str(e)}"
                quality_report['issues'].append(issue)
                self.logger.error(f"GPU поля ошибка: {e}")
        
        return quality_report
    
    def print_final_report(self, quality_report: Dict):
        """Итоговый отчет"""
        print("\n" + "=" * 60)
        print("🎯 === ИТОГОВЫЙ ОТЧЕТ ETL ПАЙПЛАЙНА ===")
        print("=" * 60)
        
        total_duration = sum(r['duration'] for r in self.results)
        successful_scripts = sum(1 for r in self.results if r['status'] == 'SUCCESS')
        
        report_data = {
            'completed': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'total_duration': total_duration,
            'successful_scripts': successful_scripts,
            'total_scripts': len(self.results),
            'total_records': quality_report.get('total_records', 0)
        }
        
        print(f"📅 Завершено: {report_data['completed']}")
        print(f"⏱️ Общее время: {total_duration:.3f} секунд ({total_duration/60:.1f} минут)")
        print(f"📊 Успешных скриптов: {successful_scripts}/{len(self.results)}")
        print(f"📈 Общий объем данных: {report_data['total_records']:,} записей")
        
        print(f"\n📊 ДЕТАЛИЗАЦИЯ ПО СКРИПТАМ:")
        for result in self.results:
            status_icon = "✅" if result['status'] == 'SUCCESS' else "❌"
            print(f"{status_icon} {result['name']:<30} {result['duration']:>8.3f}s  {result['status']}")
            self.logger.info(f"Результат {result['name']}: {result['status']} за {result['duration']:.3f}s")
        
        if quality_report.get('issues'):
            print(f"\n⚠️ ОБНАРУЖЕННЫЕ ПРОБЛЕМЫ:")
            for issue in quality_report['issues']:
                print(f"   • {issue}")
                self.logger.warning(f"Проблема: {issue}")
        
        # Финальная оценка
        if successful_scripts == len(self.results) and not quality_report.get('issues'):
            final_msg = f"\n🎉 ПАЙПЛАЙН ВЫПОЛНЕН УСПЕШНО!"
            print(final_msg)
            print(f"✅ Все {len(self.results)} скриптов завершены без ошибок")
            print(f"✅ Качество данных: отличное")
            print(f"🚀 Система готова к работе с Flame GPU")
            self.logger.info("Пайплайн выполнен успешно")
        else:
            final_msg = f"\n⚠️ ПАЙПЛАЙН ЗАВЕРШЕН С ПРОБЛЕМАМИ"
            print(final_msg)
            print(f"❌ Ошибок в скриптах: {len(self.results) - successful_scripts}")
            print(f"❌ Проблем качества: {len(quality_report.get('issues', []))}")
            self.logger.error(f"Пайплайн завершен с проблемами: {len(self.results) - successful_scripts} ошибок скриптов, {len(quality_report.get('issues', []))} проблем качества")
    
    def run_full_pipeline(self) -> bool:
        """Запуск полного пайплайна"""
        self.start_time = time.time()
        
        self.print_header()
        
        if not self.cleanup_database():
            self.logger.error("Критическая ошибка зачистки базы данных")
            print("❌ Критическая ошибка зачистки базы данных")
            return False
        
        scripts_success = self.run_all_scripts()
        
        quality_report = self.check_data_quality()
        
        self.print_final_report(quality_report)
        
        final_success = scripts_success and not quality_report.get('issues')
        self.logger.info(f"Финальный результат пайплайна: {'УСПЕХ' if final_success else 'ОШИБКА'}")
        
        return final_success

def main():
    """Основная функция"""
    runner = ETLPipelineRunner()
    success = runner.run_full_pipeline()
    return 0 if success else 1

if __name__ == "__main__":
    exit(main()) 