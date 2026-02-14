"""Утилита подключения к ClickHouse для скриптов валидации."""
import os
import yaml
from clickhouse_driver import Client


def get_client():
    base = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.join(base, "..", "..")
    cfg_path = os.path.join(project_root, "config", "database_config.yaml")
    with open(cfg_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f).get("database", {})
    password = os.environ.get("CLICKHOUSE_PASSWORD", "")
    return Client(
        host=cfg.get("host", "10.95.19.132"),
        port=int(cfg.get("port", 9000)),
        database=cfg.get("database", "default"),
        password=password,
    )
