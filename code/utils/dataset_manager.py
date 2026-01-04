"""
Dataset Manager — управление датасетами для мультизагрузки

Функционал:
1. Обнаружение датасетов в папках v_YYYY-MM-DD
2. Валидация комплектности (обязательные файлы)
3. Выбор датасета для загрузки
4. Извлечение version_date из имени папки или метаданных Excel
"""

import os
import re
import logging
from pathlib import Path
from datetime import datetime, date
from typing import List, Dict, Optional, Tuple
import openpyxl

logger = logging.getLogger(__name__)


class DatasetInfo:
    """Информация о датасете"""
    
    def __init__(self, path: Path, version_date: date):
        self.path = path
        self.version_date = version_date
        self.name = path.name
        
        # Обязательные файлы
        self.status_components = path / 'Status_Components.xlsx'
        self.status_overhaul = path / 'Status_Overhaul.xlsx'
        self.program_ac = path / 'Program_AC.xlsx'
        
        # Опциональные статические файлы
        self.program_heli = path / 'Program_heli.xlsx'
        self.program = path / 'Program.xlsx'
    
    @property
    def is_complete(self) -> bool:
        """Проверка комплектности (все обязательные файлы есть)"""
        return (
            self.status_components.exists() and
            self.status_overhaul.exists() and
            self.program_ac.exists()
        )
    
    @property
    def has_static_files(self) -> bool:
        """Проверка наличия статических файлов"""
        return self.program_heli.exists() and self.program.exists()
    
    def get_file_sizes(self) -> Dict[str, int]:
        """Размеры файлов датасета"""
        sizes = {}
        for name, path in [
            ('Status_Components', self.status_components),
            ('Status_Overhaul', self.status_overhaul),
            ('Program_AC', self.program_ac),
            ('Program_heli', self.program_heli),
            ('Program', self.program)
        ]:
            if path.exists():
                sizes[name] = path.stat().st_size
        return sizes
    
    def __repr__(self):
        status = "✅" if self.is_complete else "❌"
        static = "📦" if self.has_static_files else "⚠️"
        return f"{status} {self.name} ({self.version_date}) {static}"


class DatasetManager:
    """Менеджер датасетов для мультизагрузки"""
    
    # Паттерн для папок датасетов: v_YYYY-MM-DD
    DATASET_PATTERN = re.compile(r'^v_(\d{4}-\d{2}-\d{2})$')
    
    # Обязательные файлы
    REQUIRED_FILES = [
        'Status_Components.xlsx',
        'Status_Overhaul.xlsx',
        'Program_AC.xlsx'
    ]
    
    def __init__(self, source_data_path: str = 'data_input/source_data'):
        """
        Инициализация менеджера датасетов
        
        Args:
            source_data_path: Путь к папке source_data
        """
        self.source_data_path = Path(source_data_path)
        self.datasets: List[DatasetInfo] = []
        
    def discover_datasets(self) -> List[DatasetInfo]:
        """
        Обнаружение всех датасетов в папке source_data
        
        Returns:
            Список DatasetInfo для каждого найденного датасета
        """
        self.datasets = []
        
        if not self.source_data_path.exists():
            logger.error(f"❌ Папка {self.source_data_path} не найдена")
            return []
        
        logger.info(f"🔍 Поиск датасетов в {self.source_data_path}...")
        
        for item in sorted(self.source_data_path.iterdir()):
            if not item.is_dir():
                continue
            
            # Проверяем соответствие паттерну v_YYYY-MM-DD
            match = self.DATASET_PATTERN.match(item.name)
            if not match:
                continue
            
            # Парсим дату из имени папки
            try:
                version_date = datetime.strptime(match.group(1), '%Y-%m-%d').date()
            except ValueError:
                logger.warning(f"⚠️ Некорректная дата в имени папки: {item.name}")
                continue
            
            # Создаём DatasetInfo
            dataset = DatasetInfo(item, version_date)
            
            if dataset.is_complete:
                self.datasets.append(dataset)
                logger.info(f"  ✅ {dataset.name}: комплектный датасет")
            else:
                logger.warning(f"  ⚠️ {dataset.name}: неполный датасет")
                # Показываем какие файлы отсутствуют
                for f in self.REQUIRED_FILES:
                    if not (item / f).exists():
                        logger.warning(f"      ❌ Отсутствует: {f}")
        
        logger.info(f"📂 Найдено {len(self.datasets)} комплектных датасетов")
        return self.datasets
    
    def select_dataset_interactive(self) -> Optional[DatasetInfo]:
        """
        Интерактивный выбор датасета пользователем
        
        Returns:
            Выбранный DatasetInfo или None если отмена
        """
        if not self.datasets:
            self.discover_datasets()
        
        if not self.datasets:
            logger.error("❌ Не найдено ни одного комплектного датасета")
            return None
        
        print("\n" + "="*60)
        print("📂 ДОСТУПНЫЕ ДАТАСЕТЫ")
        print("="*60)
        
        for i, dataset in enumerate(self.datasets, 1):
            sizes = dataset.get_file_sizes()
            total_size = sum(sizes.values())
            
            static_status = "✅ статика" if dataset.has_static_files else "⚠️ без статики"
            
            print(f"\n  {i}. {dataset.name}")
            print(f"     📅 Версия: {dataset.version_date}")
            print(f"     📊 Размер: {total_size / 1024 / 1024:.1f} MB")
            print(f"     📦 {static_status}")
        
        print(f"\n  0. ❌ Отмена")
        print("="*60)
        
        while True:
            try:
                choice = input(f"\nВыберите датасет (0-{len(self.datasets)}): ").strip()
                
                if choice == '0':
                    logger.info("❌ Выбор датасета отменён пользователем")
                    return None
                
                idx = int(choice) - 1
                if 0 <= idx < len(self.datasets):
                    selected = self.datasets[idx]
                    logger.info(f"✅ Выбран датасет: {selected.name}")
                    return selected
                else:
                    print(f"❌ Введите число от 0 до {len(self.datasets)}")
                    
            except ValueError:
                print("❌ Введите число")
            except KeyboardInterrupt:
                print("\n❌ Отмена")
                return None
    
    def get_dataset_by_date(self, version_date: date) -> Optional[DatasetInfo]:
        """
        Получение датасета по дате
        
        Args:
            version_date: Дата версии
            
        Returns:
            DatasetInfo или None
        """
        if not self.datasets:
            self.discover_datasets()
        
        for dataset in self.datasets:
            if dataset.version_date == version_date:
                return dataset
        
        return None
    
    def get_latest_dataset(self) -> Optional[DatasetInfo]:
        """
        Получение самого нового датасета
        
        Returns:
            DatasetInfo с максимальной датой или None
        """
        if not self.datasets:
            self.discover_datasets()
        
        if not self.datasets:
            return None
        
        return max(self.datasets, key=lambda d: d.version_date)
    
    def extract_version_date_from_dataset(self, dataset: DatasetInfo) -> date:
        """
        Извлекает version_date из метаданных Status_Components.xlsx датасета
        
        Args:
            dataset: DatasetInfo
            
        Returns:
            date из метаданных Excel или из имени папки
        """
        try:
            status_path = dataset.status_components
            
            if not status_path.exists():
                logger.warning(f"⚠️ Файл {status_path} не найден, используем дату из имени папки")
                return dataset.version_date
            
            # Открываем Excel файл для чтения метаданных
            workbook = openpyxl.load_workbook(status_path, read_only=True)
            props = workbook.properties
            
            current_year = datetime.now().year
            version_date = dataset.version_date  # fallback на дату из имени
            version_source = "folder name"
            
            # Приоритет 1: дата создания файла (с проверкой года)
            if props.created:
                created_date = props.created
                if abs(created_date.year - current_year) <= 1:
                    version_date = created_date.date()
                    version_source = "Excel created"
                    logger.info(f"📅 Дата создания Excel: {created_date}")
            
            # Приоритет 2: дата модификации
            if props.modified and version_source == "folder name":
                version_date = props.modified.date()
                version_source = "Excel modified"
                logger.info(f"📅 Дата модификации Excel: {props.modified}")
            
            workbook.close()
            
            logger.info(f"✅ version_date: {version_date} (источник: {version_source})")
            return version_date
            
        except Exception as e:
            logger.error(f"❌ Ошибка извлечения версии: {e}")
            return dataset.version_date


def discover_datasets(source_data_path: str = 'data_input/source_data') -> List[DatasetInfo]:
    """
    Удобная функция для обнаружения датасетов
    
    Args:
        source_data_path: Путь к папке source_data
        
    Returns:
        Список DatasetInfo
    """
    manager = DatasetManager(source_data_path)
    return manager.discover_datasets()


def select_dataset(source_data_path: str = 'data_input/source_data') -> Optional[DatasetInfo]:
    """
    Удобная функция для выбора датасета
    
    Args:
        source_data_path: Путь к папке source_data
        
    Returns:
        DatasetInfo или None
    """
    manager = DatasetManager(source_data_path)
    manager.discover_datasets()
    return manager.select_dataset_interactive()


if __name__ == "__main__":
    """Тестовый запуск"""
    logging.basicConfig(level=logging.INFO)
    
    manager = DatasetManager()
    datasets = manager.discover_datasets()
    
    print(f"\n📂 Найдено датасетов: {len(datasets)}")
    for ds in datasets:
        print(f"  {ds}")
    
    if datasets:
        selected = manager.select_dataset_interactive()
        if selected:
            print(f"\n✅ Выбран: {selected.name}")
            print(f"📁 Путь: {selected.path}")



