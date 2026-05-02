#!/usr/bin/env python3
"""
Точка входа для прямых загрузок из DWH (алиас к dwh_golden_replay_export).

Использование:
  source config/load_env.sh
  python3 code/utils/dwh_direct_load.py --report-date 2026-04-08 --step program_ac
  python3 code/utils/dwh_direct_load.py --report-date 2026-04-08 --step status_overhaul
  python3 code/utils/dwh_direct_load.py --report-date 2026-04-08 --step status_components

Полная выгрузка + golden:
  python3 code/utils/dwh_direct_load.py --report-date 2026-04-08

См. также: python3 code/utils/dwh_golden_replay_export.py --help
"""
from __future__ import annotations

from dwh_golden_replay_export import main

if __name__ == "__main__":
    main()
