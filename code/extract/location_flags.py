#!/usr/bin/env python3
"""Flags derived from AMOS location (before/after aircraft_number wipe)."""

from __future__ import annotations

import re

_FOREIGN_REG = re.compile(r"^[A-Za-z]{2}-")


def is_foreign_location(loc: str) -> bool:
    """HELISUR (Перу) или иноборт XX-… (не RA-)."""
    x = str(loc or "").strip().upper()
    if not x:
        return False
    if "HELISUR" in x:
        return True
    return bool(_FOREIGN_REG.match(x)) and not x.startswith("RA-")
