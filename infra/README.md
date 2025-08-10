# Infra – ClickHouse + GPU/ETL Containers

Этот набор инфраструктуры разворачивает:
- ClickHouse (9000/8123)
- GPU-контейнер для Flame GPU/cuDF (требует NVIDIA GPU)
- Dev-контейнер для ETL (без GPU)

## Требования
- Docker 20.10+
- Docker Compose v2+
- (GPU) NVIDIA драйвер + nvidia-container-toolkit (`sudo apt install nvidia-container-toolkit && sudo nvidia-ctk runtime configure && sudo systemctl restart docker`)

## Быстрый старт

```bash
cd infra/
cp .env.example .env   # выставьте CLICKHOUSE_PASSWORD

# Запуск ClickHouse + ETL dev (без GPU)
docker compose up -d clickhouse etl-dev

# Логи ClickHouse
docker logs -f clickhouse

# Войти в dev контейнер и запустить dry-run предсимуляции
docker exec -it etl-dev bash
python3 code/utils/mp3_group_by_filler.py           # печатает SQL
python3 code/pre_simulation_status_change.py        # печатает SQL + генерирует temp_data/*.sql
```

## GPU контейнер

```bash
# Построить GPU-образ (укажите URL колеса pyflamegpu при необходимости)
PYFLAMEGPU_WHEEL_URL=<wheel_url> docker compose build gpu-sim

# Запуск GPU контейнера (требует GPU)
docker compose up -d gpu-sim

# Войти в GPU контейнер
docker exec -it flamegpu bash
python3 code/flame_gpu_helicopter_model.py   # каркас, безопасный запуск без симуляции
```

## Параметры
- env: `CLICKHOUSE_PASSWORD` обязателен.
- GPU образ требует `PIP_EXTRA_INDEX_URL=https://pypi.nvidia.com` (уже задано) для cuDF.
- Для Flame GPU передайте `PYFLAMEGPU_WHEEL_URL` (или установите вручную в контейнере).

## Безопасность и изоляция
- Инфраструктура не изменяет ваш код; том `../:/app` монтируется только для удобства разработки.
- Все предсимуляционные скрипты работают в режиме dry-run по умолчанию (без изменения данных).