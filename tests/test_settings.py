from pathlib import Path

import pytest

from settings import ConfigError, load_config


_VALID = """
[common]
user_agent = "UA"
viewport_width = 1280
viewport_height = 900
locale = "it-IT"
nav_timeout_ms = 60000
request_delay = 1.5
playwright_channel = "chromium"

[data]
output_dir = "data"
retention_keep = 10
archive_after_days = 45

[viewer]
port = 8080
host = "127.0.0.1"
open_browser = true
api_token = ""

[sources.cex]
enabled = true
label = "CEX"
color = "#e6b800"
"""


def test_load_config_with_env_override(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    cfg_file = tmp_path / "config.toml"
    cfg_file.write_text(_VALID, encoding="utf-8")

    monkeypatch.setenv("TRADER_VIEWER_HOST", "0.0.0.0")
    cfg = load_config(cfg_file)

    assert cfg["viewer"]["host"] == "0.0.0.0"
    assert cfg["common"]["playwright_channel"] == "chromium"


def test_load_config_raises_on_missing_sections(tmp_path: Path) -> None:
    cfg_file = tmp_path / "bad.toml"
    cfg_file.write_text("[common]\nuser_agent='x'\n", encoding="utf-8")

    with pytest.raises(ConfigError):
        load_config(cfg_file)
