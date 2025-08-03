#!/usr/bin/env python3
"""
Локальная безопасная система секретов
===================================

Поддерживает шифрование секретов локально без внешних зависимостей:
1. Шифрованные файлы секретов
2. Защищенные файлы (chmod 600)
3. Environment variables
4. Конфигурационные файлы (fallback)
"""

import os
import json
import yaml
import base64
import getpass
from pathlib import Path
from typing import Dict, Any, Optional
import logging
import stat
import hashlib

logger = logging.getLogger(__name__)

class LocalSecureConfig:
    """Локальная система безопасных секретов"""
    
    def __init__(self):
        self.secrets_locations = [
            '/etc/clickhouse/secrets.json',      # Системные секреты
            'secrets/clickhouse.json',           # Локальные секреты  
            'secrets/clickhouse.enc',            # Шифрованные секреты
        ]
    
    def load_database_config(self, config_path: str = 'config/database_config.yaml') -> Dict[str, Any]:
        """Загружает конфигурацию из безопасных локальных источников"""
        
        # Пробуем источники по убыванию безопасности
        config = (
            self._load_from_encrypted_file() or
            self._load_from_protected_file() or 
            self._load_from_env_vars() or
            self._load_from_config_file(config_path)
        )
        
        if not config:
            raise RuntimeError("❌ Не удалось загрузить конфигурацию")
        
        self._validate_config(config)
        return config
    
    def _load_from_encrypted_file(self) -> Optional[Dict[str, Any]]:
        """Загружает из шифрованного файла секретов"""
        encrypted_file = Path('secrets/clickhouse.enc')
        if not encrypted_file.exists():
            return None
            
        try:
            password = os.getenv('SECRETS_PASSWORD')
            if not password:
                logger.info("🔐 Для расшифровки файла введите пароль:")
                password = getpass.getpass("Пароль: ")
            
            with open(encrypted_file, 'rb') as f:
                encrypted_data = f.read()
            
            decrypted_data = self._decrypt_data(encrypted_data, password)
            secrets = json.loads(decrypted_data)
            
            if 'clickhouse' in secrets:
                logger.info("🔐 Секреты загружены из шифрованного файла")
                return secrets['clickhouse']
                
        except Exception as e:
            logger.warning(f"⚠️ Ошибка расшифровки: {e}")
            
        return None
    
    def _load_from_protected_file(self) -> Optional[Dict[str, Any]]:
        """Загружает из защищенного файла (chmod 600)"""
        for secrets_path in self.secrets_locations[:2]:  # Только .json файлы
            secrets_file = Path(secrets_path)
            if not secrets_file.exists():
                continue
                
            # Проверяем права доступа
            if self._check_file_permissions(secrets_file):
                try:
                    with open(secrets_file, 'r', encoding='utf-8') as f:
                        secrets = json.load(f)
                        
                    if 'clickhouse' in secrets:
                        logger.info(f"🔐 Секреты загружены из {secrets_path}")
                        return secrets['clickhouse']
                        
                except Exception as e:
                    logger.warning(f"⚠️ Ошибка чтения {secrets_path}: {e}")
                    
        return None
    
    def _load_from_env_vars(self) -> Optional[Dict[str, Any]]:
        """Загружает из environment variables"""
        env_vars = {
            'host': os.getenv('CLICKHOUSE_HOST'),
            'port': int(os.getenv('CLICKHOUSE_PORT', 0)) or None,
            'database': os.getenv('CLICKHOUSE_DATABASE'),
            'user': os.getenv('CLICKHOUSE_USER'),
            'password': os.getenv('CLICKHOUSE_PASSWORD')
        }
        
        # Фильтруем пустые значения
        config = {k: v for k, v in env_vars.items() if v}
        
        if config.get('password'):
            logger.info("🌍 Конфигурация загружена из environment variables")
            return config
            
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
            logger.error(f"❌ Ошибка загрузки конфига: {e}")
            return None
    
    def _check_file_permissions(self, file_path: Path) -> bool:
        """Проверяет права доступа к файлу секретов"""
        try:
            file_stat = file_path.stat()
            
            # Проверяем что файл доступен только владельцу (600 или 400)
            permissions = file_stat.st_mode & 0o777
            if permissions & (stat.S_IRGRP | stat.S_IROTH | stat.S_IWGRP | stat.S_IWOTH):
                logger.error(f"🚨 НЕБЕЗОПАСНЫЕ ПРАВА ДОСТУПА: {file_path}")
                logger.error(f"   Текущие права: {oct(permissions)}")
                logger.error(f"   Выполните: chmod 600 {file_path}")
                return False
                
            logger.debug(f"✅ Права доступа к {file_path} корректны: {oct(permissions)}")
            return True
            
        except OSError as e:
            logger.error(f"❌ Ошибка проверки прав доступа {file_path}: {e}")
            return False
    
    def _encrypt_data(self, data: str, password: str) -> bytes:
        """Простое XOR шифрование (для локального использования)"""
        # Создаем ключ из пароля
        key = hashlib.sha256(password.encode()).digest()
        
        # XOR шифрование
        data_bytes = data.encode('utf-8')
        encrypted = bytearray()
        
        for i, byte in enumerate(data_bytes):
            encrypted.append(byte ^ key[i % len(key)])
        
        # Добавляем простую контрольную сумму
        checksum = hashlib.md5(data_bytes).digest()
        return checksum + bytes(encrypted)
    
    def _decrypt_data(self, encrypted_data: bytes, password: str) -> str:
        """Расшифровка XOR данных"""
        # Создаем ключ из пароля
        key = hashlib.sha256(password.encode()).digest()
        
        # Извлекаем контрольную сумму и данные
        checksum = encrypted_data[:16]  # MD5 = 16 bytes
        encrypted = encrypted_data[16:]
        
        # XOR расшифровка
        decrypted = bytearray()
        for i, byte in enumerate(encrypted):
            decrypted.append(byte ^ key[i % len(key)])
        
        # Проверяем контрольную сумму
        data_str = decrypted.decode('utf-8')
        if hashlib.md5(data_str.encode()).digest() != checksum:
            raise ValueError("Неверный пароль или поврежденные данные")
        
        return data_str
    
    def _validate_config(self, config: Dict[str, Any]) -> None:
        """Валидация конфигурации"""
        required_fields = ['host', 'port', 'database', 'user', 'password']
        missing_fields = [field for field in required_fields if not config.get(field)]
        
        if missing_fields:
            raise ValueError(f"❌ Отсутствуют обязательные поля: {missing_fields}")
        
        # Маскируем пароль в логах
        safe_config = {k: '***' if k == 'password' else v for k, v in config.items()}
        logger.info(f"✅ Конфигурация валидна: {safe_config}")
    
    def create_encrypted_secrets(self, config: Dict[str, Any], password: str) -> None:
        """Создает шифрованный файл секретов"""
        secrets_dir = Path('secrets')
        secrets_dir.mkdir(exist_ok=True)
        
        secrets_data = {'clickhouse': config}
        json_data = json.dumps(secrets_data, indent=2)
        
        encrypted_data = self._encrypt_data(json_data, password)
        
        encrypted_file = secrets_dir / 'clickhouse.enc'
        with open(encrypted_file, 'wb') as f:
            f.write(encrypted_data)
        
        # Устанавливаем безопасные права доступа
        encrypted_file.chmod(0o600)
        
        logger.info(f"🔐 Шифрованный файл секретов создан: {encrypted_file}")

# Глобальный экземпляр
_local_secure_config = LocalSecureConfig()

def load_database_config(config_path: str = 'config/database_config.yaml') -> Dict[str, Any]:
    """Удобная функция для загрузки локальной безопасной конфигурации"""
    return _local_secure_config.load_database_config(config_path)

def create_encrypted_secrets(config: Dict[str, Any], password: str) -> None:
    """Создает шифрованный файл секретов"""
    return _local_secure_config.create_encrypted_secrets(config, password) 