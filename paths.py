from __future__ import annotations

import os
from pathlib import Path

ROOT = Path(__file__).parent


def runtime_path(env_name: str, default_name: str) -> Path:
    """Return an absolute runtime path, optionally overridden by environment."""
    raw = os.environ.get(env_name, "").strip()
    if raw:
        path = Path(raw).expanduser()
        return path if path.is_absolute() else ROOT / path
    return ROOT / default_name


DB_PATH = runtime_path("TRADER_DB_PATH", "tracker.db")
