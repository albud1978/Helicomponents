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
from typing import Dict, Any, List, Optional
import logging
import sys

logger = logging.getLogger(__name__)

def auto_load_env_file():
    """
    Автоматически загружает .env файл проекта если он существует
    Поиск выполняется в нескольких возможных локациях для универсальности
    """
    try:
        # Находим корень проекта (где находится .env)
        current_dir = Path(__file__).parent
        
        # Список возможных путей к .env файлу в порядке приоритета
        possible_paths: List[Path] = [
            current_dir.parent.parent,  # из code/utils/ -> корень
            current_dir.parent,         # из code/utils/ -> code/
            Path.home(),                # домашняя директория пользователя
            Path.cwd(),                 # текущая рабочая директория
        ]
        
        # Добавляем путь из переменной окружения, если она установлена
        if os.getenv('CUBE_CONFIG_PATH'):
            possible_paths.insert(0, Path(os.getenv('CUBE_CONFIG_PATH')))
        
        # Ищем .env файл в возможных локациях
        env_file: Optional[Path] = None
        for path in possible_paths:
            candidate = path / '.env'
            if candidate.exists():
                env_file = candidate
                break
        
        if env_file and env_file.exists():
            # Читаем .env файл и устанавливаем переменные
            with open(env_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        key, value = line.split('=', 1)
                        # Убираем кавычки если есть
                        value = value.strip('"\'')
                        os.environ[key.strip()] = value
            
            # Глобальный CUDA fallback для NVRTC в WSL/Linux
            if 'CUDA_PATH' not in os.environ:
                for p in ['/usr/local/cuda','/usr/local/cuda-12.9','/usr/local/cuda-12.4','/usr/local/cuda-12.3','/usr/local/cuda-12.2','/usr/local/cuda-12.1','/usr/local/cuda-12.0']:
                    if (Path(p)/'include'/'cuda_runtime.h').exists():
                        os.environ['CUDA_PATH'] = p
                        break
            print(f"✅ Environment variables автоматически загружены из {env_file}")
            return True
        else:
            print(f"⚠️ Файл .env не найден в проверенных директориях")
            print(f"   Проверенные пути: {[str(p) for p in possible_paths]}")
            print(f"   Для указания пути к .env установите переменную CUBE_CONFIG_PATH")
            return False
            
    except Exception as e:
        print(f"⚠️ Ошибка автозагрузки .env: {e}")
        return False

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
        password = os.getenv('CLICKHOUSE_PASSWORD')
        if not password:
            logger.error("❌ КРИТИЧЕСКАЯ ОШИБКА: Пароль CLICKHOUSE_PASSWORD не найден!")
            logger.error("   Настройте environment variable CLICKHOUSE_PASSWORD")
            logger.error("   Или создайте файл .env с CLICKHOUSE_PASSWORD=ваш_пароль")
            raise ValueError("Отсутствует обязательная переменная CLICKHOUSE_PASSWORD")
        
        config = {
            'host': os.getenv('CLICKHOUSE_HOST', base_config.get('host', '10.95.19.132')),
            'port': int(os.getenv('CLICKHOUSE_PORT', base_config.get('port', 9000))),
            'database': os.getenv('CLICKHOUSE_DATABASE', base_config.get('database', 'default')),
            'user': os.getenv('CLICKHOUSE_USER', base_config.get('user', 'default')),
            'password': password,  # ОБЯЗАТЕЛЬНО из environment variable
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

def load_clickhouse_config():
    """
    Загружает конфигурацию ClickHouse из YAML + environment variables
    
    Returns:
        dict: Параметры подключения к ClickHouse
        
    Raises:
        SystemExit: Если конфигурация недоступна или пароль не найден
    """
    try:
        # АВТОМАТИЧЕСКАЯ ЗАГРУЗКА .env ФАЙЛА
        auto_load_env_file()
        
        # Поиск конфигурационного файла в нескольких возможных местах
        possible_config_paths = [
            Path(__file__).parent.parent.parent / 'config' / 'database_config.yaml',  # из code/utils/ -> корень/config/
            Path(__file__).parent.parent / 'config' / 'database_config.yaml',         # из code/utils/ -> code/config/
            Path.cwd() / 'config' / 'database_config.yaml',                           # текущая директория/config/
        ]
        
        # Добавляем путь из переменной окружения, если она установлена
        if os.getenv('CUBE_CONFIG_PATH'):
            possible_config_paths.insert(0, Path(os.getenv('CUBE_CONFIG_PATH')) / 'database_config.yaml')
        
        config_path = None
        for path in possible_config_paths:
            if path.exists():
                config_path = path
                break
        
        if not config_path:
            print(f"❌ Файл конфигурации не найден в проверенных директориях:")
            for path in possible_config_paths:
                print(f"   - {path}")
            print(f"Для указания пути к конфигурации установите переменную CUBE_CONFIG_PATH")
            sys.exit(1)
        
        # Загружаем YAML конфигурацию
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        
        db_config = config['database']
        
        # Получаем пароль из environment variable
        password_var = db_config['env']['password_var']
        password = os.getenv(password_var)
        
        if not password:
            print(f"❌ ОШИБКА БЕЗОПАСНОСТИ: Пароль не найден в environment variable '{password_var}'")
            print(f"🔒 Установите пароль одним из способов:")
            print(f"   1. export {password_var}='ваш_пароль'")
            print(f"   2. Добавьте в ~/.bashrc для постоянного использования")
            print(f"   3. Создайте файл .env с {password_var}=ваш_пароль")
            print(f"   4. Запустите: source config/load_env.sh (если файл .env существует)")
            sys.exit(1)
        
        # Собираем параметры подключения
        connection_config = {
            'host': os.getenv(db_config['env']['host_var'], db_config['host']),
            'port': int(os.getenv(db_config['env']['port_var'], db_config['port'])),
            'user': os.getenv(db_config['env']['user_var'], db_config['user']),
            'password': password,  # Только из environment variable!
            'database': db_config['database'],
            'settings': db_config['settings']
        }
        
        print(f"✅ Конфигурация загружена из: {config_path}")
        print(f"✅ Подключение: {connection_config['host']}:{connection_config['port']}")
        print(f"🔒 Пароль получен из environment variable: {password_var}")
        
        return connection_config
        
    except Exception as e:
        print(f"❌ Ошибка загрузки конфигурации: {e}")
        sys.exit(1)


def _apply_clickhouse_bind_device(bind_device: str) -> None:
    """Привязка native TCP к интерфейсу (например ppp0 для 10.95.x)."""
    import socket

    import clickhouse_driver.connection as ch_conn

    if getattr(ch_conn.Connection, "_heli_bind_device_patched", False):
        return

    device = bind_device.encode("utf-8") + b"\x00"
    so_bind_to_device = 25
    orig_create_socket = ch_conn.Connection._create_socket

    def _create_socket_bound(self, host, port):
        err = None
        for res in socket.getaddrinfo(host, port, 0, socket.SOCK_STREAM):
            af, socktype, proto, canonname, sa = res
            sock = None
            try:
                sock = socket.socket(af, socktype, proto)
                sock.setsockopt(socket.SOL_SOCKET, so_bind_to_device, device)
                sock.settimeout(self.connect_timeout)

                if self.secure_socket:
                    ssl_context = self._create_ssl_context(self.ssl_options.copy())
                    sock = ssl_context.wrap_socket(
                        sock, server_hostname=self.server_hostname or host
                    )

                sock.connect(sa)
                return sock
            except OSError as exc:
                err = exc
                if sock is not None:
                    sock.close()

        if err is not None:
            raise err
        raise OSError("getaddrinfo returns an empty list")

    ch_conn.Connection._create_socket = _create_socket_bound
    ch_conn.Connection._heli_bind_device_patched = True
    ch_conn.Connection._heli_bind_device_name = bind_device


def get_clickhouse_client():
    """
    Создает клиент ClickHouse с безопасной конфигурацией
    
    Returns:
        Client: Настроенный клиент ClickHouse
    """
    try:
        from clickhouse_driver import Client
        
        config = load_clickhouse_config()

        bind_device = os.getenv("CLICKHOUSE_BIND_DEVICE", "").strip()
        if bind_device:
            _apply_clickhouse_bind_device(bind_device)
            print(f"🔗 ClickHouse bind device: {bind_device}")
        
        client = Client(
            host=config['host'],
            port=config['port'],
            user=config['user'],
            password=config['password'],
            database=config['database'],
            settings={
                'strings_encoding': 'utf-8',
                'max_threads': config['settings']['max_threads']
            }
        )
        
        return client
        
    except Exception as e:
        print(f"❌ Ошибка создания клиента ClickHouse: {e}")
        sys.exit(1)

if __name__ == "__main__":
    # Тест конфигурации
    print("🧪 Тестируем загрузку конфигурации...")
    config = load_clickhouse_config()
    print("✅ Конфигурация корректна!")
    
    print("\n🧪 Тестируем подключение к ClickHouse...")
    client = get_clickhouse_client()
    result = client.execute("SELECT 1")
    print(f"✅ Подключение успешно: {result}") 