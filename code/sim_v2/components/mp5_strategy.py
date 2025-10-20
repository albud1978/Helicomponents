"""
MP5Strategy - стратегии загрузки MP5 данных

Ответственность:
- Различные стратегии инициализации MP5 (host-only, RTC-copy, hybrid)
- Подготовка данных MP5 (обрезка, паддинг D+1)
- Регистрация HostFunction для загрузки MP5

Архитектурный принцип:
- Strategy Pattern для гибкости выбора метода загрузки
- Изолированный модуль, не зависит от orchestrator
- Работает только с данными env_data и моделью FLAME GPU

История:
- V1: RTC-копирование из mp5_src в mp5_lin (NVRTC ошибки при DAYS>=90)
- V2: Host-only инициализация напрямую в mp5_lin (текущая, стабильная)
"""

from typing import List, Dict, Union
import pyflamegpu as fg
from .data_adapters import EnvDataAdapter
from .validation_rules import DimensionValidator


class MP5Strategy:
    """Базовый класс стратегии загрузки MP5"""
    
    def __init__(self, env_data: Union[Dict[str, object], EnvDataAdapter], frames: int, days: int):
        """
        Инициализация стратегии
        
        Args:
            env_data: словарь с данными окружения или EnvDataAdapter
            frames: количество кадров
            days: горизонт симуляции
        """
        # Поддержка обратной совместимости
        if isinstance(env_data, EnvDataAdapter):
            self.adapter = env_data
            self.env_data = env_data._raw_data
        else:
            self.adapter = EnvDataAdapter(env_data)
            self.env_data = env_data
        
        self.frames = frames
        self.days = days
    
    def prepare_data(self) -> List[int]:
        """
        Подготавливает данные MP5 для загрузки
        
        Returns:
            список значений mp5_daily_hours с D+1 паддингом
        
        Raises:
            ValueError: если размер данных недостаточен
        
        Note:
            Сортировка по mfg_date УЖЕ выполнена в build_frames_index() на этапе ETL
        """
        mp5_data = list(self.env_data['mp5_daily_hours_linear'])
        need = (self.days + 1) * self.frames  # D+1 для безопасного чтения daily_next
        
        # Валидация размера через DimensionValidator
        validation = DimensionValidator.validate_mp5_size(mp5_data, self.frames, self.days)
        if not validation:
            raise ValueError(validation.message)
        
        # Обрезаем до нужного размера
        mp5_data = mp5_data[:need]
        
        return mp5_data
    
    def register(self, model: fg.ModelDescription):
        """
        Регистрирует стратегию загрузки в модели
        
        Args:
            model: модель FLAME GPU
        
        Raises:
            NotImplementedError: должен быть переопределён в подклассах
        """
        raise NotImplementedError("Метод register() должен быть переопределён в подклассе")


class HostOnlyMP5Strategy(MP5Strategy):
    """
    Host-only стратегия загрузки MP5
    
    Преимущества:
    - Стабильная, без NVRTC ошибок
    - Простая реализация
    - Поддерживает любые DAYS (включая 3650+)
    
    Недостатки:
    - Загрузка происходит на CPU перед первым шагом
    - Нет гибкости изменения данных во время симуляции
    """
    
    def register(self, model: fg.ModelDescription):
        """Регистрирует HostFunction для инициализации MP5"""
        mp5_data = self.prepare_data()
        
        # Создаём HostFunction
        hf_init = self._create_host_function(mp5_data)
        
        # Добавляем слой инициализации в начало модели
        # Важно: этот слой должен выполниться ДО всех RTC функций
        init_layer = model.newLayer()
        init_layer.addHostFunction(hf_init)
        
        print(f"MP5 будет инициализирован через HostFunction ({len(mp5_data)} элементов)")
    
    def _create_host_function(self, mp5_data: List[int]) -> fg.HostFunction:
        """
        Создаёт HostFunction для загрузки MP5
        
        Args:
            mp5_data: подготовленные данные MP5
        
        Returns:
            объект HostFunction
        """
        frames = self.frames
        days = self.days
        
        class HF_InitMP5(fg.HostFunction):
            def __init__(self, data, frames_val, days_val):
                super().__init__()
                self.data = data
                self.frames = frames_val
                self.days = days_val
                self.initialized = False  # Флаг для выполнения только один раз
            
            def run(self, FLAMEGPU):
                """Выполняет загрузку MP5 данных в MacroProperty (только один раз)"""
                if self.initialized:
                    return  # Уже инициализировано, пропускаем
                
                print(f"HF_InitMP5: Инициализация mp5_lin для FRAMES={self.frames}, DAYS={self.days}")
                
                # Получаем MacroProperty
                mp = FLAMEGPU.environment.getMacroPropertyUInt("mp5_lin")
                
                # Заполняем данными напрямую из Python
                for i, val in enumerate(self.data):
                    mp[i] = int(val)
                
                print(f"HF_InitMP5: Инициализировано {len(self.data)} элементов")
                self.initialized = True  # Отмечаем как выполненное
        
        return HF_InitMP5(mp5_data, frames, days)


class MP5StrategyFactory:
    """Фабрика для создания стратегий MP5"""
    
    @staticmethod
    def create(strategy_name: str, env_data: Dict[str, object], 
               frames: int, days: int) -> MP5Strategy:
        """
        Создаёт стратегию по имени
        
        Args:
            strategy_name: имя стратегии ('host_only', 'rtc_copy', etc)
            env_data: данные окружения
            frames: количество кадров
            days: горизонт симуляции
        
        Returns:
            объект стратегии
        
        Raises:
            ValueError: если стратегия не найдена
        """
        strategies = {
            'host_only': HostOnlyMP5Strategy,
            # Будущие стратегии:
            # 'rtc_copy': RTCCopyMP5Strategy,
            # 'hybrid': HybridMP5Strategy,
        }
        
        if strategy_name not in strategies:
            available = ', '.join(strategies.keys())
            raise ValueError(
                f"Неизвестная стратегия MP5: '{strategy_name}'. "
                f"Доступные: {available}"
            )
        
        strategy_class = strategies[strategy_name]
        return strategy_class(env_data, frames, days)
