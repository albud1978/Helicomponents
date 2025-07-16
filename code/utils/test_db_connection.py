#!/usr/bin/env python3
"""
Простой тест подключения к ClickHouse
"""
import os

print("=== ТЕСТ ПОДКЛЮЧЕНИЯ К CLICKHOUSE ===")
print()

# Проверяем переменные окружения
print("1. Переменные окружения:")
print(f"   CLICKHOUSE_HOST: {os.getenv('CLICKHOUSE_HOST', 'НЕ УСТАНОВЛЕН')}")
print(f"   CLICKHOUSE_PORT: {os.getenv('CLICKHOUSE_PORT', 'НЕ УСТАНОВЛЕН')}")
print(f"   CLICKHOUSE_USER: {os.getenv('CLICKHOUSE_USER', 'НЕ УСТАНОВЛЕН')}")
print(f"   CLICKHOUSE_PASSWORD: {'УСТАНОВЛЕН' if os.getenv('CLICKHOUSE_PASSWORD') else 'НЕ УСТАНОВЛЕН'}")
print()

# Тест 1: clickhouse_connect (HTTP)
print("2. Тест clickhouse_connect (HTTP протокол):")
try:
    import clickhouse_connect
    
    client = clickhouse_connect.get_client(
        host=os.getenv('CLICKHOUSE_HOST', '10.95.19.132'),
        port=int(os.getenv('CLICKHOUSE_PORT', '8123')),
        username=os.getenv('CLICKHOUSE_USER', 'default'),
        password=os.getenv('CLICKHOUSE_PASSWORD', '')
    )
    
    result = client.command('SELECT version()')
    print(f"   ✅ clickhouse_connect OK: {result}")
    
except Exception as e:
    print(f"   ❌ clickhouse_connect ERROR: {e}")

print()

# Тест 2: clickhouse_driver (Native) - проверяем разные порты
print("3. Тест clickhouse_driver (Native протокол):")
try:
    from clickhouse_driver import connect
    
    # Пробуем сначала порт из переменной, потом стандартный Native порт
    ports_to_try = [int(os.getenv('CLICKHOUSE_PORT', '8123')), 9000]
    
    success = False
    for port in ports_to_try:
        try:
            print(f"   Пробуем порт {port}...")
            conn = connect(
                host=os.getenv('CLICKHOUSE_HOST', '10.95.19.132'),
                port=port,
                user=os.getenv('CLICKHOUSE_USER', 'default'),
                password=os.getenv('CLICKHOUSE_PASSWORD', '')
            )
            
            cursor = conn.cursor()
            cursor.execute('SELECT version()')
            result = cursor.fetchone()
            print(f"   ✅ clickhouse_driver OK на порту {port}: {result}")
            success = True
            break
        except Exception as e:
            print(f"   ❌ Порт {port} не работает: {e}")
    
    if not success:
        print("   ❌ clickhouse_driver: Все порты недоступны")
        
except ImportError:
    print("   ❌ clickhouse_driver не установлен")
except Exception as e:
    print(f"   ❌ clickhouse_driver ERROR: {e}")

print()

# Тест 3: requests (прямой HTTP)
print("4. Тест прямого HTTP запроса:")
try:
    import requests
    from requests.auth import HTTPBasicAuth
    
    url = f"http://{os.getenv('CLICKHOUSE_HOST', '10.95.19.132')}:{os.getenv('CLICKHOUSE_PORT', '8123')}"
    
    auth = HTTPBasicAuth(
        os.getenv('CLICKHOUSE_USER', 'default'),
        os.getenv('CLICKHOUSE_PASSWORD', '')
    )
    
    response = requests.get(f"{url}/?query=SELECT version()", auth=auth, timeout=5)
    
    if response.status_code == 200:
        print(f"   ✅ HTTP OK: {response.text.strip()}")
    else:
        print(f"   ❌ HTTP ERROR: {response.status_code} - {response.text}")
        
except Exception as e:
    print(f"   ❌ HTTP ERROR: {e}")

print()
print("=== КОНЕЦ ТЕСТА ===") 