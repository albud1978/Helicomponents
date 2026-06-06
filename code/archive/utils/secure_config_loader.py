#!/usr/bin/env python3
# ARCHIVED 2026-06-06 (code utils cleanup): orphan/устаревшая утилита, 0 импортёров; перекрыто code/utils/config_loader.py.
"""
Безопасная загрузка конфигурации
==============================

Поддерживает несколько источников секретов по приоритету:
1. Файлы секретов (для продакшена)
2. Environment variables (для разработки)
3. Конфигурационные файлы (fallback)

Автор: AI Assistant
Дата: 2025-01-09
"""

import os
import json
import yaml
from pathlib import Path
from typing import Dict, Any, Optional
import logging
import stat

logger = logging.getLogger(__name__)

class SecureConfigLoader:
    """Безопасный загрузчик конфигурации с поддержкой разных источников"""
    
    def __init__(self):
        self.config_sources = [
            self._load_from_secrets_file,
            self._load_from_env_vars,
            self._load_from_config_file
        ]
    
    def load_database_config(self, config_path: str = 'config/database_config.yaml') -> Dict[str, Any]:
        """
        Загружает конфигурацию ClickHouse из безопасных источников
        
        Приоритет источников:
        1. /etc/clickhouse/secrets.json (производство)
        2. Environment variables (разработка)
        3. YAML конфиг (fallback)
        
        Args:
            config_path: Путь к fallback конфигу
            
        Returns:
            Словарь с параметрами подключения
        """
        config = {}
        
        # Пробуем загрузить из источников по приоритету
        for source_func in self.config_sources:
            try:
                source_config = source_func(config_path)
                if source_config:
                    config.update(source_config)
                    logger.info(f"✅ Конфиг загружен из {source_func.__name__}")
                    break
            except Exception as e:
                logger.debug(f"Источник {source_func.__name__} недоступен: {e}")
                continue
        
        if not config:
            raise RuntimeError("❌ Не удалось загрузить конфигурацию из всех источников")
        
        # Валидация обязательных полей
        required_fields = ['host', 'port', 'database', 'user', 'password']
        missing_fields = [field for field in required_fields if not config.get(field)]
        
        if missing_fields:
            raise ValueError(f"❌ Отсутствуют обязательные поля: {missing_fields}")
        
        # Маскируем пароль в логах
        safe_config = {k: '***' if k == 'password' else v for k, v in config.items()}
        logger.info(f"🔐 Безопасная конфигурация: {safe_config}")
        
        return config
    
    def _load_from_secrets_file(self, config_path: str) -> Optional[Dict[str, Any]]:
        """Загружает секреты из защищенного файла (для продакшена)"""
        secrets_paths = [
            '/etc/clickhouse/secrets.json',
            '/var/secrets/clickhouse.json',
            'secrets/clickhouse.json'  # для локальной разработки
        ]
        
        for secrets_path in secrets_paths:
            secrets_file = Path(secrets_path)
            if not secrets_file.exists():
                continue
                
            # Проверяем права доступа к файлу
            file_stat = secrets_file.stat()
            if file_stat.st_mode & (stat.S_IRGRP | stat.S_IROTH):
                logger.warning(f"⚠️ Файл секретов {secrets_path} доступен для чтения другим пользователям!")
            
            try:
                with open(secrets_file, 'r', encoding='utf-8') as f:
                    secrets = json.load(f)
                    
                # Ожидаем структуру: {"clickhouse": {"host": "...", "password": "..."}}
                if 'clickhouse' in secrets:
                    logger.info(f"🔐 Секреты загружены из {secrets_path}")
                    return secrets['clickhouse']
                    
            except (json.JSONDecodeError, OSError) as e:
                logger.warning(f"⚠️ Ошибка чтения {secrets_path}: {e}")
                continue
        
        return None
    
    def _load_from_env_vars(self, config_path: str) -> Optional[Dict[str, Any]]:
        """Загружает конфигурацию из environment variables"""
        env_config = {}
        
        env_mapping = {
            'CLICKHOUSE_HOST': 'host',
            'CLICKHOUSE_PORT': 'port', 
            'CLICKHOUSE_DATABASE': 'database',
            'CLICKHOUSE_USER': 'user',
            'CLICKHOUSE_PASSWORD': 'password'
        }
        
        for env_var, config_key in env_mapping.items():
            value = os.getenv(env_var)
            if value:
                env_config[config_key] = int(value) if config_key == 'port' else value
        
        # Возвращаем только если есть хотя бы пароль
        if env_config.get('password'):
            logger.info(f"🌍 Конфигурация загружена из environment variables")
            return env_config
            
        return None
    
    def _load_from_config_file(self, config_path: str) -> Optional[Dict[str, Any]]:
        """Загружает базовую конфигурацию из YAML (fallback)"""
        try:
            config_file = Path(config_path)
            if not config_file.exists():
                return None
                
            with open(config_file, 'r', encoding='utf-8') as f:
                base_config = yaml.safe_load(f)['database']
            
            # КРИТИЧНО: Пароль должен быть в environment variable или secrets файле!
            if not base_config.get('password'):
                logger.error("❌ КРИТИЧЕСКАЯ ОШИБКА: Пароль не найден ни в конфиге, ни в environment variables!")
                logger.error("   Настройте переменную CLICKHOUSE_PASSWORD или файл secrets/clickhouse.json")
                return None
            
            logger.info(f"📁 Базовая конфигурация загружена из {config_path}")
            return base_config
            
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки конфига из файла: {e}")
            return None

# Глобальный экземпляр для использования в проекте
_secure_loader = SecureConfigLoader()

def load_database_config(config_path: str = 'config/database_config.yaml') -> Dict[str, Any]:
    """
    Удобная функция для загрузки безопасной конфигурации
    
    Args:
        config_path: Путь к fallback конфигу
        
    Returns:
        Словарь с параметрами подключения
    """
    return _secure_loader.load_database_config(config_path) 