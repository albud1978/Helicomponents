# Infra – ClickHouse + GPU/ETL Containers

Этот набор инфраструктуры разворачивает:
- ClickHouse (9000/8123)
- GPU-контейнер для FLAME GPU 2 / cuDF (требует NVIDIA GPU)
- Dev-контейнер для ETL (без GPU)

## Версии (обновлено 03-01-2026)

| Компонент | Версия | Примечание |
|-----------|--------|------------|
| Python | 3.12 | |
| CUDA | 13.0 | Поддерживает RTX 30xx/40xx/50xx |
| pandas | 2.3.3 | |
| numpy | 2.2.6 | |
| cudf-cu12 | 25.12.0 | GPU-ускоренный pandas |
| pyflamegpu | 2.0.0rc4+cuda130 | Agent-Based моделирование |
| clickhouse-driver | 0.2.10 | |
| clickhouse-connect | 0.10.0 | |
| PyYAML | 6.0.3 | |

## Требования

- Docker 20.10+
- Docker Compose v2+
- (GPU) NVIDIA драйвер 550+ для CUDA 13.0
- (GPU) nvidia-container-toolkit:
  ```bash
  sudo apt install nvidia-container-toolkit
  sudo nvidia-ctk runtime configure
  sudo systemctl restart docker
  ```

## Быстрый старт

```bash
cd infra/
cp .env.example .env   # выставьте CLICKHOUSE_PASSWORD

# Запуск ClickHouse + ETL dev (без GPU)
docker compose up -d clickhouse etl-dev

# Логи ClickHouse
docker logs -f clickhouse

# Войти в dev контейнер и запустить ETL
docker exec -it etl-dev bash
python3 code/extract_master.py
```

## GPU контейнер

```bash
# Построить GPU-образ
docker compose build gpu-sim

# Запуск GPU контейнера (требует GPU)
docker compose up -d gpu-sim

# Войти в GPU контейнер
docker exec -it flamegpu bash

# Запуск симуляции
cd code/sim_v2 && python3 orchestrator_v2.py \
  --version-date 2025-07-04 \
  --modules state_2_operations states_stub count_ops quota_repair quota_ops_excess \
    quota_promote_serviceable quota_promote_reserve quota_promote_inactive spawn_dynamic \
    compute_transitions \
    state_manager_serviceable state_manager_operations state_manager_repair \
    state_manager_storage state_manager_reserve state_manager_inactive spawn_v2 \
  --steps 3650 --enable-mp2 --enable-mp2-postprocess --drop-table
```

## Параметры

- `CLICKHOUSE_PASSWORD` — обязателен в `.env`
- `PYFLAMEGPU_WHEEL_URL` — опционально, прямая ссылка на wheel если автоустановка не работает
- `FLAMEGPU_RTC_EXPORT_CACHE_PATH` — путь к RTC-кэшу (по умолчанию `/app/.rtc_cache`)

## RTC кэширование

GPU-контейнер монтирует `.rtc_cache/` из проекта. При первом запуске симуляции ядра компилируются (~8 мин), затем используется кэш.

```bash
# Очистка кэша (только при изменении RTC кода!)
rm -rf .rtc_cache/* ~/.cache/flamegpu/* /tmp/flamegpu/*
```

## Безопасность и изоляция

- Том `../:/app` монтируется для удобства разработки
- Все ETL скрипты работают с реальными данными из ClickHouse
- Предсимуляционные скрипты безопасны (dry-run по умолчанию)

## V2 Pipeline (RTC)

- Оркестрация через `code/sim_v2/orchestrator_v2.py`
- Конфиг модулей в порядке согласно матрице состояний
- См. `docs/rtc_pipeline_architecture.md` — полная архитектура
