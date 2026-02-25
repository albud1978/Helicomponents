# AGENTS.md

## Cursor Cloud specific instructions

### Overview

Helicomponents — система прогнозирования жизненного цикла компонентов вертолётов (Mi-8/Mi-17/Mi-26).
Два основных пайплайна: **ETL** (Extract/Transform/Load из Excel → ClickHouse) и **GPU Simulation** (FLAME GPU 2, требует NVIDIA GPU).

### Services

| Сервис | Назначение | Как запустить |
|--------|-----------|--------------|
| **ClickHouse** | Аналитическая БД, обязательна для ETL | `sudo CLICKHOUSE_PASSWORD=dev123 docker compose -f infra/docker-compose.yml up -d clickhouse` |
| **Python venv** | Все Python-зависимости | `source .venv/bin/activate` |

### Key gotchas

- **Docker daemon**: в Cloud VM нужно запускать вручную: `sudo dockerd &>/tmp/dockerd.log &` (подождать ~3 сек).
- **ClickHouse password**: docker-compose использует `${CLICKHOUSE_PASSWORD}` из env. Передавайте при запуске: `sudo CLICKHOUSE_PASSWORD=dev123 docker compose ...`. Файл `.env` в корне проекта должен содержать `CLICKHOUSE_PASSWORD=dev123`.
- **config_loader auto-loads `.env`**: код `code/utils/config_loader.py` автоматически загружает `.env` из корня проекта. Не нужно вручную `source config/load_env.sh` при вызове Python-скриптов.
- **test_db_connection.py НЕ использует config_loader**: скрипт `code/utils/test_db_connection.py` читает `os.getenv()` напрямую. Нужно заранее выставить `CLICKHOUSE_HOST=localhost` и `CLICKHOUSE_PASSWORD=dev123`.
- **GPU simulation недоступна**: в Cloud VM нет NVIDIA GPU. Пайплайн GPU (FLAME GPU 2, cuDF) работает только на машинах с RTX GPU.
- **ETL interactive**: `code/extract_master.py` — интерактивный (выбор датасета/режима). Для автоматизации нужен mock stdin или прямой вызов отдельных loader-ов.
- **Нет формального linter/test framework**: проект не использует pytest, flake8, mypy. Тесты — standalone Python-скрипты в `code/sim_v2/test_*.py` и `code/archive/`.
- **Правила `.cursor/rules/`**: запрещено запускать `code/sim_v2/orchestrator_v2.py` без явного разрешения; мутирующие SQL к ClickHouse только по разрешению.
