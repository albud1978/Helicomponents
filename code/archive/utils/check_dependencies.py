#!/usr/bin/env python3
# ARCHIVED 2026-06-06 (code utils cleanup): orphan/устаревшая утилита, 0 импортёров; перекрыта живыми модулями или старым workflow.
"""
Проверка всех зависимостей для Helicopter Component Lifecycle Prediction
"""
import sys

def check_dependencies():
    """Проверка всех необходимых зависимостей"""
    print("🔍 === ПРОВЕРКА ЗАВИСИМОСТЕЙ ===\n")
    
    # Основные зависимости (обязательные)
    required_packages = [
        ('pandas', '1.5.0', 'Обработка Excel файлов'),
        ('numpy', '1.21.0', 'Численные операции'),
        ('clickhouse_driver', '0.2.6', 'Подключение к ClickHouse'),
        ('yaml', '6.0', 'Парсинг конфигурации'),
        ('openpyxl', '3.0.0', 'Метаданные Excel файлов')
    ]
    
    # GPU зависимости (опциональные, но уже установлены)
    gpu_packages = [
        ('cudf', '24.0.0', 'GPU-ускоренная обработка данных'),
        ('pyflamegpu', '2.0.0', 'GPU симуляции и агентное моделирование')
    ]
    
    missing_packages = []
    gpu_missing = []
    
    print("📦 === ОСНОВНЫЕ ЗАВИСИМОСТИ ===")
    for package_name, min_version, description in required_packages:
        try:
            if package_name == 'yaml':
                # PyYAML импортируется как yaml
                import yaml
                version = yaml.__version__
            elif package_name == 'clickhouse_driver':
                from clickhouse_driver import __version__
                version = __version__
            else:
                exec(f"import {package_name}")
                version = eval(f"{package_name}.__version__")
            
            print(f"✅ {package_name:20} {version:10} - {description}")
            
        except ImportError:
            print(f"❌ {package_name:20} {'MISSING':10} - {description}")
            missing_packages.append((package_name, min_version))
        except AttributeError:
            print(f"⚠️  {package_name:20} {'UNKNOWN':10} - {description} (версия не определена)")
    
    print(f"\n🚀 === GPU ЗАВИСИМОСТИ ===")
    for package_name, min_version, description in gpu_packages:
        try:
            if package_name == 'pyflamegpu':
                import pyflamegpu
                # Версия может быть не доступна
                try:
                    version = pyflamegpu.__version__
                except AttributeError:
                    version = "INSTALLED"
            else:
                exec(f"import {package_name}")
                version = eval(f"{package_name}.__version__")
            
            print(f"✅ {package_name:20} {version:10} - {description}")
            
        except ImportError:
            print(f"❌ {package_name:20} {'MISSING':10} - {description}")
            gpu_missing.append((package_name, min_version))
        except AttributeError:
            print(f"⚠️  {package_name:20} {'UNKNOWN':10} - {description} (версия не определена)")
    
    print(f"\n📊 === РЕЗУЛЬТАТ ===")
    
    if missing_packages:
        print(f"❌ Отсутствуют {len(missing_packages)} основных пакетов:")
        for package, version in missing_packages:
            print(f"   {package}>={version}")
        
        print(f"\n💡 Для установки выполните:")
        print(f"   pip install -r requirements.txt")
        return False
    else:
        print(f"✅ Все основные зависимости установлены!")
        
        if gpu_missing:
            print(f"⚠️  Отсутствуют {len(gpu_missing)} GPU пакетов:")
            for package, version in gpu_missing:
                print(f"   {package}>={version}")
            print(f"💡 GPU функции будут недоступны")
        else:
            print(f"🚀 Все GPU зависимости установлены!")
        
        print(f"✅ Проект готов к запуску!")
        return True

if __name__ == '__main__':
    success = check_dependencies()
    sys.exit(0 if success else 1) 