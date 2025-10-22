# Исправление конфликта чтения/записи MP5

## Суть проблемы
В `sim_master.py` возникал конфликт при работе с MP5 (daily hours) из-за:
1. Отсутствия MacroProperty `mp5_lin` в модели
2. Попытки одновременного чтения и записи в один слой
3. Отсутствия разделения слоев для WRITE и READ операций

## Внесенные изменения

### 1. В `model_build.py`:
- **HeliSimModel**: Добавлено создание MacroProperty `mp5_lin` и флага `mp5_ready` при `HL_USE_MP5_MACRO=1`
- **build_model_for_quota_smoke**: 
  - Добавлено создание PropertyArray `mp5_daily_hours` (всегда)
  - Добавлено создание MacroProperty `mp5_lin` при включенном режиме
  - Добавлены RTC функции для копирования PropertyArray → MacroProperty
  - Реорганизованы слои с правильным разделением:
    - `mp5_copy_once` - WRITE mp5_lin (только на первом шаге)
    - `mp5_mark_ready` - установка флага готовности
    - `probe_mp5` - READ mp5_lin (только после готовности)
    - Статусные слои - READ через dt/dn
  - Добавлены HostCondition для управления выполнением слоев

### 2. В `sim_master.py`:
- Добавлено автоматическое включение `HL_USE_MP5_MACRO=1` при FRAMES*DAYS > 10000
- Добавлена инициализация PropertyArray `mp5_daily_hours` из `env_data`
- Добавлены диагностические сообщения для отладки

## Ключевые инварианты
1. **Один писатель**: Только `rtc_mp5_copy_once` пишет в `mp5_lin`
2. **Барьер готовности**: Чтение начинается только после `mp5_ready=1`
3. **Разделение слоев**: WRITE и READ операции в разных слоях
4. **Порядок слоев**: 
   - Копирование MP5 (WRITE)
   - Установка флага готовности
   - Probe MP5 (READ)
   - Статусы 2/4/6 (READ)

## Тестирование
Используйте `code/test_mp5_fix.sh` для проверки:
```bash
./code/test_mp5_fix.sh
```

Или вручную:
```bash
# С MacroProperty (большие данные)
HL_JIT_LOG=1 python3 code/sim_master.py --status246-smoke-real --status246-days 100

# Без MacroProperty (малые данные)
python3 code/sim_master.py --status246-smoke-real --status246-days 5
```

## Диагностика
При запуске будут выводиться сообщения:
- `[MP5] Включен режим MacroProperty` - когда активирован режим
- `[BUILD] Создание слоев для MacroProperty MP5` - при построении модели
- `[MP5] Инициализировано N значений` - после загрузки данных
