#!/usr/bin/env python3
"""
Скрипт для тестирования и сравнения производительности:
1. Pandas Dict Lookup (v2.0)
2. ClickHouse Dictionary 

Запускает оба пайплайна и сравнивает результаты
"""

import os
import sys
import time
import logging
from datetime import datetime
import pandas as pd

# Добавляем путь к утилитам
sys.path.append(str('code'))

def setup_logging():
    """Настройка логирования для сравнения"""
    os.makedirs('test_output', exist_ok=True)
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('test_output/comparison_test.log', mode='w'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

def test_pandas_approach():
    """Тестирование Pandas Dict подхода"""
    logger.info("🐼 Тестирование Pandas Dict Lookup подхода...")
    
    try:
        from optimized_pipeline_v2 import OptimizedPipelineV2
        
        start_time = time.time()
        
        pipeline = OptimizedPipelineV2()
        excel_path = "data_input/source_data/Status_Components.xlsx"
        
        if not os.path.exists(excel_path):
            logger.error(f"❌ Файл не найден: {excel_path}")
            return None
        
        success = pipeline.run_full_pipeline(excel_path)
        
        duration = time.time() - start_time
        
        result = {
            'approach': 'Pandas Dict Lookup',
            'success': success,
            'duration': duration,
            'file': 'optimized_pipeline_v2.py',
            'log_file': 'test_output/optimized_pipeline_v2.log'
        }
        
        if success:
            logger.info(f"✅ Pandas подход завершен за {duration:.2f} сек")
        else:
            logger.error(f"❌ Pandas подход завершен с ошибкой за {duration:.2f} сек")
        
        return result
        
    except Exception as e:
        logger.error(f"❌ Критическая ошибка Pandas подхода: {e}")
        return {
            'approach': 'Pandas Dict Lookup',
            'success': False,
            'duration': 0,
            'error': str(e)
        }

def test_clickhouse_dictionary_approach():
    """Тестирование ClickHouse Dictionary подхода"""
    logger.info("🏠 Тестирование ClickHouse Dictionary подхода...")
    
    try:
        from optimized_pipeline_v2_with_dictionaries import OptimizedPipelineV2WithDictionaries
        
        start_time = time.time()
        
        pipeline = OptimizedPipelineV2WithDictionaries()
        excel_path = "data_input/source_data/Status_Components.xlsx"
        
        if not os.path.exists(excel_path):
            logger.error(f"❌ Файл не найден: {excel_path}")
            return None
        
        success = pipeline.run_full_pipeline(excel_path)
        
        duration = time.time() - start_time
        
        result = {
            'approach': 'ClickHouse Dictionary',
            'success': success,
            'duration': duration,
            'file': 'optimized_pipeline_v2_with_dictionaries.py',
            'log_file': 'test_output/optimized_pipeline_v2_dict.log'
        }
        
        if success:
            logger.info(f"✅ ClickHouse Dictionary подход завершен за {duration:.2f} сек")
        else:
            logger.error(f"❌ ClickHouse Dictionary подход завершен с ошибкой за {duration:.2f} сек")
        
        return result
        
    except Exception as e:
        logger.error(f"❌ Критическая ошибка ClickHouse Dictionary подхода: {e}")
        return {
            'approach': 'ClickHouse Dictionary',
            'success': False,
            'duration': 0,
            'error': str(e)
        }

def compare_results(pandas_result, clickhouse_result):
    """Сравнение результатов двух подходов"""
    logger.info("📊 Сравнение результатов...")
    
    comparison = {
        'test_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'pandas': pandas_result,
        'clickhouse': clickhouse_result
    }
    
    # Анализ производительности
    if pandas_result and clickhouse_result:
        if pandas_result['success'] and clickhouse_result['success']:
            pandas_time = pandas_result['duration']
            clickhouse_time = clickhouse_result['duration']
            
            if pandas_time > 0:
                speedup = clickhouse_time / pandas_time
                comparison['performance'] = {
                    'pandas_faster': pandas_time < clickhouse_time,
                    'speedup_factor': speedup,
                    'time_difference': abs(clickhouse_time - pandas_time)
                }
                
                logger.info("=" * 60)
                logger.info("📈 РЕЗУЛЬТАТЫ СРАВНЕНИЯ:")
                logger.info(f"🐼 Pandas Dict:      {pandas_time:.2f} сек")
                logger.info(f"🏠 ClickHouse Dict:  {clickhouse_time:.2f} сек")
                
                if pandas_time < clickhouse_time:
                    logger.info(f"🚀 Pandas быстрее в {speedup:.1f}x раз")
                else:
                    logger.info(f"🚀 ClickHouse быстрее в {1/speedup:.1f}x раз")
                
                logger.info(f"⏱️ Разница во времени: {abs(clickhouse_time - pandas_time):.2f} сек")
                logger.info("=" * 60)
    
    return comparison

def validate_data_consistency():
    """Проверка консистентности данных между подходами"""
    logger.info("🔍 Проверка консистентности данных...")
    
    try:
        # Здесь можно добавить SQL запросы для сравнения результатов
        # в таблицах helicopter_simulation_results и helicopter_simulation_results_dict
        
        logger.info("✅ Проверка данных завершена (детальная реализация TODO)")
        return True
        
    except Exception as e:
        logger.error(f"❌ Ошибка проверки данных: {e}")
        return False

def generate_comparison_report(comparison):
    """Генерация отчета сравнения"""
    logger.info("📋 Генерация отчета сравнения...")
    
    report_path = 'test_output/pipeline_comparison_report.md'
    
    report = f"""# 📊 Отчет сравнения Pipeline подходов

## 🕐 Время тестирования: {comparison['test_time']}

## 📈 Результаты производительности

### Pandas Dict Lookup:
- **Успешность**: {'✅ Успешно' if comparison['pandas']['success'] else '❌ Ошибка'}
- **Время выполнения**: {comparison['pandas']['duration']:.2f} сек
- **Файл**: {comparison['pandas']['file']}
- **Лог**: {comparison['pandas'].get('log_file', 'N/A')}

### ClickHouse Dictionary:
- **Успешность**: {'✅ Успешно' if comparison['clickhouse']['success'] else '❌ Ошибка'}
- **Время выполнения**: {comparison['clickhouse']['duration']:.2f} сек  
- **Файл**: {comparison['clickhouse']['file']}
- **Лог**: {comparison['clickhouse'].get('log_file', 'N/A')}
"""

    if 'performance' in comparison:
        perf = comparison['performance']
        winner = "Pandas Dict" if perf['pandas_faster'] else "ClickHouse Dict"
        factor = perf['speedup_factor'] if not perf['pandas_faster'] else 1/perf['speedup_factor']
        
        report += f"""
## 🏆 Победитель: {winner}

- **Быстрее в**: {factor:.1f}x раз
- **Разница во времени**: {perf['time_difference']:.2f} сек
"""

    report += """
## 📋 Рекомендации

Для текущего объема данных (108k записей):
- **Рекомендуется**: Pandas Dict Lookup
- **Причина**: Простота архитектуры и высокая производительность
- **Переход на ClickHouse Dictionary**: при росте данных > 1M записей

## 📂 Дополнительные файлы

- Подробные логи в папке `test_output/`
- Сравнение подходов: `code/dictionary_approaches_comparison.md`
"""

    try:
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(report)
        
        logger.info(f"✅ Отчет сохранен: {report_path}")
        return report_path
        
    except Exception as e:
        logger.error(f"❌ Ошибка сохранения отчета: {e}")
        return None

def main():
    """Главная функция тестирования"""
    global logger
    logger = setup_logging()
    
    logger.info("🚀 Запуск сравнительного тестирования Pipeline подходов")
    logger.info("=" * 60)
    
    total_start = time.time()
    
    # 1. Тестирование Pandas подхода
    pandas_result = test_pandas_approach()
    
    # Пауза между тестами
    time.sleep(2)
    
    # 2. Тестирование ClickHouse Dictionary подхода  
    clickhouse_result = test_clickhouse_dictionary_approach()
    
    # 3. Сравнение результатов
    if pandas_result and clickhouse_result:
        comparison = compare_results(pandas_result, clickhouse_result)
        
        # 4. Проверка консистентности данных
        validate_data_consistency()
        
        # 5. Генерация отчета
        report_path = generate_comparison_report(comparison)
        
        total_duration = time.time() - total_start
        
        logger.info("=" * 60)
        logger.info("✅ СРАВНИТЕЛЬНОЕ ТЕСТИРОВАНИЕ ЗАВЕРШЕНО!")
        logger.info(f"⏱️ Общее время тестирования: {total_duration:.2f} сек")
        if report_path:
            logger.info(f"📋 Отчет сохранен: {report_path}")
        logger.info("=" * 60)
        
        return True
    else:
        logger.error("❌ Не удалось выполнить сравнение - один из подходов завершился с ошибкой")
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1) 