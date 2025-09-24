#!/usr/bin/env python3
"""
Скрипт для тестирования RTC функций с логированием
"""
import subprocess
import datetime
import os
import sys

def run_test(days, log_file):
    """Запуск теста на заданное количество дней"""
    print(f"\n{'='*60}")
    print(f"Тест на {days} дней - {datetime.datetime.now()}")
    print(f"Лог файл: {log_file}")
    print(f"{'='*60}\n")
    
    cmd = [
        "python3", "-u", "code/sim_v2/orchestrator_v2.py",
        "--modules", "mp5_probe", "state_2_operations", "states_stub",
        "--steps", str(days)
    ]
    
    # Запускаем с выводом и в файл и на экран
    with open(log_file, 'w') as f:
        # Записываем заголовок в файл
        f.write(f"{'='*60}\n")
        f.write(f"Тест на {days} дней - {datetime.datetime.now()}\n")
        f.write(f"{'='*60}\n\n")
        f.flush()
        
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True,
            bufsize=1
        )
        
        # Читаем построчно для вывода и в файл и на экран
        for line in process.stdout:
            print(line, end='')  # На экран
            f.write(line)        # В файл
            f.flush()
        
        process.wait()
        
        if process.returncode != 0:
            print(f"\nОШИБКА: Код возврата {process.returncode}")
            f.write(f"\nОШИБКА: Код возврата {process.returncode}\n")
    
    print(f"\nТест завершен. Лог сохранен в {log_file}")

def main():
    # Создаем директорию для логов
    log_dir = "logs/rtc_tests"
    os.makedirs(log_dir, exist_ok=True)
    
    # Тесты по схеме 5/365/3650 дней
    test_days = [5, 365, 3650]
    
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    
    for days in test_days:
        log_file = f"{log_dir}/test_{days}days_{timestamp}.log"
        
        # Автоматический запуск если не в интерактивном режиме
        import sys
        if sys.stdin.isatty():
            response = input(f"\nЗапустить тест на {days} дней? (y/n): ")
            if response.lower() != 'y':
                print(f"Пропускаем тест на {days} дней")
                continue
        else:
            print(f"\nЗапуск теста на {days} дней (неинтерактивный режим)...")
            
        run_test(days, log_file)
        
        # Показываем краткую статистику из лога
        print("\nКраткая статистика:")
        os.system(f"echo '--- Intent статистика ---' && grep -c 'intent=' {log_file} | xargs echo 'Всего intent записей:'")
        os.system(f"grep 'intent=' {log_file} | grep -o 'intent=[0-9]' | sort | uniq -c")
        os.system(f"echo '--- Assembly trigger ---' && grep -c 'assembly_trigger=1' {log_file} | xargs echo 'Assembly trigger установлен:'")
        os.system(f"echo '--- Переходы ---' && grep -E '(4->5|2->4|2->6)' {log_file} | wc -l | xargs echo 'Переходов состояний:'")

if __name__ == '__main__':
    main()
