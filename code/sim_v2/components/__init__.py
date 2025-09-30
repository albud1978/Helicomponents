"""
Components для V2 архитектуры

Модульные компоненты для декомпозиции orchestrator_v2.py
"""

from .agent_population import AgentPopulationBuilder
from .telemetry_collector import TelemetryCollector
from .mp5_strategy import MP5StrategyFactory, MP5Strategy, HostOnlyMP5Strategy
from .data_adapters import (
    EnvDataAdapter,
    VersionInfo,
    FramesInfo,
    SimulationDimensions,
    MP1Data,
    MP3Data,
    MP4Data,
    MP5Data
)
from .validation_rules import (
    ValidationLevel,
    ValidationResult,
    DimensionValidator,
    StateTransitionValidator,
    InvariantValidator,
    DataQualityValidator,
    ValidationSuite
)

__all__ = [
    'AgentPopulationBuilder', 
    'TelemetryCollector',
    'MP5StrategyFactory',
    'MP5Strategy',
    'HostOnlyMP5Strategy',
    'EnvDataAdapter',
    'VersionInfo',
    'FramesInfo',
    'SimulationDimensions',
    'MP1Data',
    'MP3Data',
    'MP4Data',
    'MP5Data',
    'ValidationLevel',
    'ValidationResult',
    'DimensionValidator',
    'StateTransitionValidator',
    'InvariantValidator',
    'DataQualityValidator',
    'ValidationSuite'
]

