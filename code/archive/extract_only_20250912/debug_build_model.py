#!/usr/bin/env python3
"""
Отладка сборки модели build_model_for_quota_smoke
"""
import os
import sys
import io
import contextlib

# Настройка окружения для Mode A
os.environ['HL_ENABLE_SPAWN'] = '1'
os.environ['HL_ENABLE_MP2'] = '1'
os.environ['HL_STATUS246_SMOKE'] = '1'
os.environ['PYTHONUNBUFFERED'] = '1'
os.environ['JITIFY_PRINT_LOG'] = '1'  # Включаем логи Jitify
os.environ['FLAMEGPU_VERBOSE'] = '1'

# Перехват stdout для ошибок NVRTC
class TeeOutput:
    def __init__(self, *files):
        self.files = files
    
    def write(self, data):
        for f in self.files:
            f.write(data)
            f.flush()
    
    def flush(self):
        for f in self.files:
            f.flush()

# Буфер для перехвата stdout
captured_output = io.StringIO()
tee = TeeOutput(sys.stdout, captured_output)

try:
    import pyflamegpu
except ImportError:
    print("pyflamegpu не установлен")
    sys.exit(1)

from model_build import build_model_for_quota_smoke

def main():
    FRAMES = 286
    DAYS = 7
    
    print(f"=== Отладка build_model_for_quota_smoke ===")
    print(f"FRAMES={FRAMES}, DAYS={DAYS}")
    print(f"Переменные окружения:")
    print(f"  HL_ENABLE_SPAWN={os.environ.get('HL_ENABLE_SPAWN')}")
    print(f"  HL_ENABLE_MP2={os.environ.get('HL_ENABLE_MP2')}")
    print(f"  JITIFY_PRINT_LOG={os.environ.get('JITIFY_PRINT_LOG')}")
    print()
    
    # Перехватываем stdout
    old_stdout = sys.stdout
    sys.stdout = tee
    
    try:
        print("Создание модели...")
        model, agent_desc = build_model_for_quota_smoke(FRAMES, DAYS)
        print("✅ Модель создана")
        
        print("\nСоздание симуляции (здесь происходит компиляция)...")
        sim = pyflamegpu.CUDASimulation(model)
        print("✅ Симуляция создана успешно!")
        
    except Exception as e:
        print(f"\n❌ ОШИБКА: {type(e).__name__}")
        print(f"Сообщение: {e}")
        
        # Восстанавливаем stdout
        sys.stdout = old_stdout
        
        # Выводим перехваченный вывод
        captured = captured_output.getvalue()
        if captured:
            print("\n=== ПЕРЕХВАЧЕННЫЙ ВЫВОД (возможно содержит ошибки NVRTC) ===")
            print(captured)
            print("=== КОНЕЦ ПЕРЕХВАЧЕННОГО ВЫВОДА ===\n")
        
        # Анализируем ошибку
        error_str = str(e)
        if "rtc_log_day" in error_str:
            print("\n💡 Проблема с rtc_log_day")
            print("Проверьте:")
            print("1. Правильность синтаксиса getMacroProperty")
            print("2. Размерность массивов MP2")
            print("3. Константы FRAMES*DAYS в шаблонах")
            
            # Попробуем найти RTC источник
            print("\nПопытка вывести источник rtc_log_day...")
            try:
                # Создаем модель снова чтобы получить источник
                model2, _ = build_model_for_quota_smoke(FRAMES, DAYS)
                # Ищем функцию
                agent = model2.getAgent("component")
                fn = agent.getFunction("rtc_log_day")
                print("Функция найдена в модели, но источник недоступен через API")
            except Exception as e2:
                print(f"Не удалось получить источник: {e2}")
        
        import traceback
        print("\n=== ПОЛНЫЙ TRACEBACK ===")
        traceback.print_exc()
        
    finally:
        sys.stdout = old_stdout

if __name__ == "__main__":
    main()
