from __future__ import annotations

import os
import sys
from pathlib import Path

if sys.version_info >= (3, 11):
    import tomllib
else:
    try:
        import tomli as tomllib
    except ImportError as exc:
        raise ImportError("Python < 3.11 richiede 'tomli': pip install tomli") from exc


class ConfigError(ValueError):
    """Configurazione non valida."""


def _as_bool(value, *, field: str) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"1", "true", "yes", "on"}:
            return True
        if lowered in {"0", "false", "no", "off"}:
            return False
    raise ConfigError(f"Campo {field!r} deve essere booleano")


def _as_int(value, *, field: str, minimum: int | None = None) -> int:
    try:
        ivalue = int(value)
    except Exception as exc:  # noqa: BLE001
        raise ConfigError(f"Campo {field!r} deve essere intero") from exc
    if minimum is not None and ivalue < minimum:
        raise ConfigError(f"Campo {field!r} deve essere >= {minimum}")
    return ivalue


def _as_float(value, *, field: str, minimum: float | None = None) -> float:
    try:
        fvalue = float(value)
    except Exception as exc:  # noqa: BLE001
        raise ConfigError(f"Campo {field!r} deve essere numerico") from exc
    if minimum is not None and fvalue < minimum:
        raise ConfigError(f"Campo {field!r} deve essere >= {minimum}")
    return fvalue


def _as_str(value, *, field: str, allow_empty: bool = False) -> str:
    if not isinstance(value, str):
        raise ConfigError(f"Campo {field!r} deve essere stringa")
    result = value.strip()
    if not allow_empty and not result:
        raise ConfigError(f"Campo {field!r} non puo' essere vuoto")
    return result


def _env_int(name: str, default: int) -> int:
    value = os.environ.get(name)
    if value is None:
        return default
    return _as_int(value, field=name)


def _env_float(name: str, default: float) -> float:
    value = os.environ.get(name)
    if value is None:
        return default
    return _as_float(value, field=name)


def _env_bool(name: str, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return _as_bool(value, field=name)


def _env_str(name: str, default: str) -> str:
    value = os.environ.get(name)
    return default if value is None else value.strip()


def load_config(config_path: Path) -> dict:
    """Carica e valida config.toml applicando override ENV."""
    with config_path.open("rb") as fh:
        raw = tomllib.load(fh)

    if not isinstance(raw, dict):
        raise ConfigError("config.toml non contiene un oggetto valido")

    common = raw.get("common")
    data = raw.get("data")
    viewer = raw.get("viewer")
    sources = raw.get("sources")

    if not isinstance(common, dict):
        raise ConfigError("Sezione [common] mancante o non valida")
    if not isinstance(data, dict):
        raise ConfigError("Sezione [data] mancante o non valida")
    if not isinstance(viewer, dict):
        raise ConfigError("Sezione [viewer] mancante o non valida")
    if not isinstance(sources, dict):
        raise ConfigError("Sezione [sources] mancante o non valida")

    validated = {
        "common": {
            "user_agent": _env_str(
                "TRADER_USER_AGENT",
                _as_str(common.get("user_agent", ""), field="common.user_agent"),
            ),
            "viewport_width": _env_int(
                "TRADER_VIEWPORT_WIDTH",
                _as_int(common.get("viewport_width", 1280), field="common.viewport_width", minimum=320),
            ),
            "viewport_height": _env_int(
                "TRADER_VIEWPORT_HEIGHT",
                _as_int(common.get("viewport_height", 900), field="common.viewport_height", minimum=320),
            ),
            "locale": _env_str(
                "TRADER_LOCALE",
                _as_str(common.get("locale", "it-IT"), field="common.locale"),
            ),
            "nav_timeout_ms": _env_int(
                "TRADER_NAV_TIMEOUT_MS",
                _as_int(common.get("nav_timeout_ms", 60000), field="common.nav_timeout_ms", minimum=1000),
            ),
            "request_delay": _env_float(
                "TRADER_REQUEST_DELAY",
                _as_float(common.get("request_delay", 1.5), field="common.request_delay", minimum=0.0),
            ),
            "playwright_channel": _env_str(
                "TRADER_PLAYWRIGHT_CHANNEL",
                _as_str(common.get("playwright_channel", "chrome"), field="common.playwright_channel"),
            ),
        },
        "data": {
            "output_dir": _env_str(
                "TRADER_OUTPUT_DIR",
                _as_str(data.get("output_dir", "data"), field="data.output_dir"),
            ),
            "retention_keep": _env_int(
                "TRADER_RETENTION_KEEP",
                _as_int(data.get("retention_keep", 30), field="data.retention_keep", minimum=0),
            ),
            "archive_after_days": _env_int(
                "TRADER_ARCHIVE_AFTER_DAYS",
                _as_int(data.get("archive_after_days", 45), field="data.archive_after_days", minimum=1),
            ),
        },
        "viewer": {
            "port": _env_int(
                "TRADER_VIEWER_PORT",
                _as_int(viewer.get("port", 8080), field="viewer.port", minimum=1),
            ),
            "host": _env_str(
                "TRADER_VIEWER_HOST",
                _as_str(viewer.get("host", "127.0.0.1"), field="viewer.host"),
            ),
            "open_browser": _env_bool(
                "TRADER_VIEWER_OPEN_BROWSER",
                _as_bool(viewer.get("open_browser", True), field="viewer.open_browser"),
            ),
            "api_token": _env_str(
                "TRADER_API_TOKEN",
                _as_str(viewer.get("api_token", ""), field="viewer.api_token", allow_empty=True),
            ),
        },
        "sources": {},
    }

    for source_name, cfg in sources.items():
        if not isinstance(cfg, dict):
            raise ConfigError(f"Sezione [sources.{source_name}] non valida")
        enabled = _as_bool(cfg.get("enabled", True), field=f"sources.{source_name}.enabled")
        label = _as_str(cfg.get("label", source_name), field=f"sources.{source_name}.label")
        color = _as_str(cfg.get("color", "#888"), field=f"sources.{source_name}.color")
        validated_cfg = dict(cfg)
        validated_cfg["enabled"] = enabled
        validated_cfg["label"] = label
        validated_cfg["color"] = color
        validated["sources"][source_name] = validated_cfg

    return validated


def load_default_config(root: Path | None = None) -> dict:
    base = root or Path(__file__).parent
    return load_config(base / "config.toml")
