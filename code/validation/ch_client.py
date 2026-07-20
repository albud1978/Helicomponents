"""Утилита подключения к ClickHouse для скриптов валидации."""
import os
import sys

BASE = os.path.dirname(os.path.abspath(__file__))
CODE_DIR = os.path.join(BASE, "..")
if CODE_DIR not in sys.path:
    sys.path.insert(0, CODE_DIR)

from utils.config_loader import get_clickhouse_client


def get_client():
    return get_clickhouse_client()
