"""
DataAdapters - адаптеры для изоляции от схемы БД

Ответственность:
- Преобразование данных из ClickHouse в доменные модели
- Изоляция компонентов от структуры env_data
- Валидация и типизация данных
- Единая точка доступа к данным окружения

Архитектурный принцип:
- Adapter Pattern для изоляции от внешних источников данных
- Typed API для компонентов (вместо Dict[str, object])
- Fail-fast валидация на границе системы
"""

from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass


@dataclass
class VersionInfo:
    """Информация о версии снапшота"""
    date_ordinal: int  # Дата версии (ordinal days от 1970-01-01)
    version_id: int    # ID версии


@dataclass
class FramesInfo:
    """Информация о кадрах (frames)"""
    total: int                          # Общее количество кадров
    index: Dict[int, int]              # Маппинг aircraft_number -> frame_idx
    first_reserved_idx: int            # Индекс первого зарезервированного слота
    reserved_slots_count: int          # Количество зарезервированных слотов (MP5-only)
    base_acn_spawn: int                # Базовый ACN для будущего спавна
    
    def is_reserved(self, frame_idx: int) -> bool:
        """Проверяет, является ли кадр зарезервированным (MP5-only)"""
        return frame_idx >= self.first_reserved_idx
    
    def get_aircraft_number(self, frame_idx: int) -> Optional[int]:
        """Возвращает aircraft_number по frame_idx (обратная индексация)"""
        for acn, idx in self.index.items():
            if idx == frame_idx:
                return acn
        return None


@dataclass
class SimulationDimensions:
    """Размерности симуляции"""
    days_total: int              # Горизонт симуляции (дни)
    frames_total: int            # Количество кадров
    days_sorted: List            # Отсортированные даты
    
    def validate(self):
        """Валидирует размерности"""
        if self.days_total <= 0:
            raise ValueError(f"days_total должен быть > 0, получено: {self.days_total}")
        if self.frames_total <= 0:
            raise ValueError(f"frames_total должен быть > 0, получено: {self.frames_total}")
        if len(self.days_sorted) != self.days_total:
            raise ValueError(
                f"Несоответствие: days_sorted={len(self.days_sorted)} != days_total={self.days_total}"
            )


@dataclass
class MP1Data:
    """Данные MP1 (нормативы компонентов)"""
    partseqno_list: List[int]    # Список partseqno_i
    index: Dict[int, int]        # Маппинг partseqno_i -> index
    br_mi8: List[int]            # Beyond Repair для Mi-8
    br_mi17: List[int]           # Beyond Repair для Mi-17
    oh_mi8: List[int]            # Overhaul Hours для Mi-8
    oh_mi17: List[int]           # Overhaul Hours для Mi-17
    ll_mi8: List[int]            # Life Limit для Mi-8
    ll_mi17: List[int]           # Life Limit для Mi-17
    repair_time: List[int]       # Время ремонта
    partout_time: List[int]      # Время разборки
    assembly_time: List[int]     # Время сборки
    
    def get_oh(self, partseqno: int, group_by: int) -> int:
        """Получить OH по partseqno и типу вертолёта"""
        idx = self.index.get(partseqno, -1)
        if idx < 0:
            return 0
        if group_by == 1:  # Mi-8
            return self.oh_mi8[idx] if idx < len(self.oh_mi8) else 0
        elif group_by == 2:  # Mi-17
            return self.oh_mi17[idx] if idx < len(self.oh_mi17) else 0
        return 0
    
    def get_br(self, partseqno: int, group_by: int) -> int:
        """Получить BR по partseqno и типу вертолёта"""
        idx = self.index.get(partseqno, -1)
        if idx < 0:
            return 0
        if group_by == 1:  # Mi-8
            return self.br_mi8[idx] if idx < len(self.br_mi8) else 0
        elif group_by == 2:  # Mi-17
            return self.br_mi17[idx] if idx < len(self.br_mi17) else 0
        return 0
    
    def get_ll(self, partseqno: int, group_by: int) -> int:
        """Получить LL по partseqno и типу вертолёта"""
        idx = self.index.get(partseqno, -1)
        if idx < 0:
            return 0
        if group_by == 1:  # Mi-8
            return self.ll_mi8[idx] if idx < len(self.ll_mi8) else 0
        elif group_by == 2:  # Mi-17
            return self.ll_mi17[idx] if idx < len(self.ll_mi17) else 0
        return 0


@dataclass
class MP3Data:
    """Данные MP3 (агрегированное состояние планёров)"""
    aircraft_number: List[int]
    status_id: List[int]
    sne: List[int]
    ppr: List[int]
    repair_days: List[int]
    group_by: List[int]
    partseqno_i: List[int]
    ll: List[int]
    oh: List[int]
    mfg_date_days: List[int]
    count: int


@dataclass
class MP4Data:
    """Данные MP4 (планы квот по дням)"""
    ops_counter_mi8: List[int]       # Квоты operations для Mi-8
    ops_counter_mi17: List[int]      # Квоты operations для Mi-17
    new_counter_mi17_seed: List[int] # Seed планов спавна Mi-17


@dataclass
class MP5Data:
    """Данные MP5 (суточные налёты)"""
    daily_hours_linear: List[int]    # Линейный массив (days+1) * frames
    
    def get_value(self, day: int, frame_idx: int, frames_total: int) -> int:
        """Получить значение по дню и кадру"""
        pos = day * frames_total + frame_idx
        if 0 <= pos < len(self.daily_hours_linear):
            return self.daily_hours_linear[pos]
        return 0


class EnvDataAdapter:
    """
    Адаптер для доступа к данным окружения
    
    Преобразует Dict[str, object] из prepare_env_arrays в типизированные доменные модели
    """
    
    def __init__(self, env_data: Dict[str, object]):
        """
        Инициализация адаптера
        
        Args:
            env_data: словарь с данными из prepare_env_arrays
        """
        self._raw_data = env_data
        self._version_info: Optional[VersionInfo] = None
        self._frames_info: Optional[FramesInfo] = None
        self._dimensions: Optional[SimulationDimensions] = None
        self._mp1_data: Optional[MP1Data] = None
        self._mp3_data: Optional[MP3Data] = None
        self._mp4_data: Optional[MP4Data] = None
        self._mp5_data: Optional[MP5Data] = None
    
    @property
    def version(self) -> VersionInfo:
        """Информация о версии снапшота"""
        if self._version_info is None:
            self._version_info = VersionInfo(
                date_ordinal=int(self._raw_data['version_date_u16']),
                version_id=int(self._raw_data.get('version_id_u32', 0))
            )
        return self._version_info
    
    @property
    def frames(self) -> FramesInfo:
        """Информация о кадрах"""
        if self._frames_info is None:
            self._frames_info = FramesInfo(
                total=int(self._raw_data['frames_total_u16']),
                index=dict(self._raw_data['frames_index']),
                first_reserved_idx=int(self._raw_data.get('first_reserved_idx', 0)),
                reserved_slots_count=int(self._raw_data.get('reserved_slots_count', 0)),
                base_acn_spawn=int(self._raw_data.get('base_acn_spawn', 100000))
            )
        return self._frames_info
    
    @property
    def dimensions(self) -> SimulationDimensions:
        """Размерности симуляции"""
        if self._dimensions is None:
            dims = SimulationDimensions(
                days_total=int(self._raw_data['days_total_u16']),
                frames_total=int(self._raw_data['frames_total_u16']),
                days_sorted=list(self._raw_data.get('days_sorted', []))
            )
            dims.validate()  # Ранняя валидация
            self._dimensions = dims
        return self._dimensions
    
    @property
    def mp1(self) -> MP1Data:
        """Данные MP1 (нормативы компонентов)"""
        if self._mp1_data is None:
            mp1_arrays = self._raw_data.get('mp1_arrays', {})
            self._mp1_data = MP1Data(
                partseqno_list=list(mp1_arrays.get('partseqno_i', [])),
                index=dict(self._raw_data.get('mp1_index', {})),
                br_mi8=list(mp1_arrays.get('br_mi8', [])),
                br_mi17=list(mp1_arrays.get('br_mi17', [])),
                oh_mi8=list(self._raw_data.get('mp1_oh_mi8', [])),
                oh_mi17=list(self._raw_data.get('mp1_oh_mi17', [])),
                ll_mi8=list(mp1_arrays.get('ll_mi8', [])),
                ll_mi17=list(mp1_arrays.get('ll_mi17', [])),
                repair_time=list(mp1_arrays.get('repair_time', [])),
                partout_time=list(mp1_arrays.get('partout_time', [])),
                assembly_time=list(mp1_arrays.get('assembly_time', []))
            )
        return self._mp1_data
    
    @property
    def mp3(self) -> MP3Data:
        """Данные MP3 (агрегированное состояние планёров)"""
        if self._mp3_data is None:
            mp3_arrays = self._raw_data.get('mp3_arrays', {})
            self._mp3_data = MP3Data(
                aircraft_number=list(mp3_arrays.get('mp3_aircraft_number', [])),
                status_id=list(mp3_arrays.get('mp3_status_id', [])),
                sne=list(mp3_arrays.get('mp3_sne', [])),
                ppr=list(mp3_arrays.get('mp3_ppr', [])),
                repair_days=list(mp3_arrays.get('mp3_repair_days', [])),
                group_by=list(mp3_arrays.get('mp3_group_by', [])),
                partseqno_i=list(mp3_arrays.get('mp3_partseqno_i', [])),
                ll=list(mp3_arrays.get('mp3_ll', [])),
                oh=list(mp3_arrays.get('mp3_oh', [])),
                mfg_date_days=list(mp3_arrays.get('mp3_mfg_date_days', [])),
                count=int(self._raw_data.get('mp3_count', 0))
            )
        return self._mp3_data
    
    @property
    def mp4(self) -> MP4Data:
        """Данные MP4 (планы квот)"""
        if self._mp4_data is None:
            self._mp4_data = MP4Data(
                ops_counter_mi8=list(self._raw_data.get('mp4_ops_counter_mi8', [])),
                ops_counter_mi17=list(self._raw_data.get('mp4_ops_counter_mi17', [])),
                new_counter_mi17_seed=list(self._raw_data.get('mp4_new_counter_mi17_seed', []))
            )
        return self._mp4_data
    
    @property
    def mp5(self) -> MP5Data:
        """Данные MP5 (суточные налёты)"""
        if self._mp5_data is None:
            self._mp5_data = MP5Data(
                daily_hours_linear=list(self._raw_data.get('mp5_daily_hours_linear', []))
            )
        return self._mp5_data
    
    def get_raw(self, key: str, default=None) -> object:
        """
        Получить сырые данные по ключу (fallback для нетипизированных полей)
        
        Args:
            key: ключ в env_data
            default: значение по умолчанию
        
        Returns:
            значение из env_data или default
        """
        return self._raw_data.get(key, default)
