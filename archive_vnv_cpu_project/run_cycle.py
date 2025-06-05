#!/usr/bin/env python3

import sys
import subprocess
import logging

def check_cuda_and_cudf():
    """
    Проверка:
      1) Установлен ли cudf?
      2) (Опционально) Есть ли вообще GPU (через nvidia-smi)?
    Возвращает True, если GPU-режим поддерживается.
    """
    try:
        import cudf  # Если cudf не установлен, тут будет ImportError
    except ImportError:
        return False
    
    # Дополнительно можно проверить физическое наличие GPU.
    # Например, вызов nvidia-smi и проверка кода выхода:
    # (Эту часть добавьте, если действительно нужно проверить *физическую* GPU)
    try:
        result = subprocess.run(["nvidia-smi"], capture_output=True, text=True)
        if result.returncode != 0:
            # nvidia-smi не отработал, значит GPU нет или драйвера не настроены
            return False
    except FileNotFoundError:
        # nvidia-smi вообще не найден
        return False
    
    return True

if __name__ == "__main__":
    # Инициализируем логгер (по желанию)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    
    # Проверяем наличие cudf и GPU
    gpu_available = check_cuda_and_cudf()
    
    if gpu_available:
        logging.info("Обнаружены CUDA и cudf. Запускаем GPU-версию.")
        subprocess.run([sys.executable, "cycle_full_GPU.py"])
    else:
        logging.info("GPU или cudf недоступны. Запускаем CPU-версию.")
        subprocess.run([sys.executable, "cycle_full_CPU.py"])
