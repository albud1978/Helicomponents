#!/usr/bin/env python3
"""
Универсальная загрузка конфигурации
=================================

Загружает конфиг из YAML с поддержкой environment variables
На основе архивного проекта

Автор: AI Assistant
Дата: 2025-01-09
"""

import os
import yaml
from pathlib import Path
from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)

def load_database_config(config_path: str = 'config/database_config.yaml') -> Dict[str, Any]:
    """
    Загружает конфигурацию ClickHouse с поддержкой environment variables
    
    Args:
        config_path: Путь к файлу конфигурации
        
    Returns:
        Словарь с параметрами подключения
    """
    try:
        # Загружаем базовый конфиг из файла
        config_file = Path(config_path)
        if not config_file.exists():
            raise FileNotFoundError(f"Конфиг файл не найден: {config_path}")
            
        with open(config_file, 'r', encoding='utf-8') as f:
            base_config = yaml.safe_load(f)['database']
        
        # Переопределяем через environment variables (как в архивном проекте)
        config = {
            'host': os.getenv('CLICKHOUSE_HOST', base_config.get('host', '10.95.19.132')),
            'port': int(os.getenv('CLICKHOUSE_PORT', base_config.get('port', 9000))),
            'database': os.getenv('CLICKHOUSE_DATABASE', base_config.get('database', 'default')),
            'user': os.getenv('CLICKHOUSE_USER', base_config.get('user', 'default')),
            'password': os.getenv('CLICKHOUSE_PASSWORD', 'quie1ahpoo5Su0wohpaedae8keeph6bi'),  # default из архива
        }
        
        # Дополнительные настройки
        config.update({
            'settings': base_config.get('settings', {}),
            'batch': base_config.get('batch', {'size': 5000})
        })
        
        logger.info(f"✅ Конфиг загружен: {config['host']}:{config['port']}/{config['database']}")
        
        return config
        
    except Exception as e:
        logger.error(f"❌ Ошибка загрузки конфига: {e}")
        raise 