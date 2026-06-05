"""Setup di logging centralizzato per tutto il progetto.

Sostituisce i vari logging.basicConfig sparsi con un'unica configurazione:
- formato coerente (timestamp + livello), livello da env TRADER_LOG_LEVEL
- redazione automatica dei segreti (token Telegram, API key, Bearer, password)
- opzione JSON via TRADER_LOG_JSON=1

Idempotente: chiamare setup() più volte è sicuro.
"""
from __future__ import annotations

import json
import logging
import os
import re

_REDACT_PATTERNS = [
    re.compile(r"\b\d{6,}:[A-Za-z0-9_-]{20,}\b"),                 # token bot Telegram
    re.compile(r"sk-[A-Za-z0-9-]{16,}"),                          # API key stile OpenAI
    re.compile(r"(?i)(token|api[_-]?key|password)\s*[=:]\s*\S+"),
    re.compile(r"(?i)bearer\s+[A-Za-z0-9._\-]+"),
]


def _redact(text: str) -> str:
    out = text
    for rx in _REDACT_PATTERNS:
        out = rx.sub("***", out)
    return out


class _RedactFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:  # noqa: A003
        try:
            msg = record.getMessage()
            red = _redact(msg)
            if red != msg:
                record.msg = red
                record.args = ()
        except Exception:  # noqa: BLE001
            pass
        return True


class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        return json.dumps({
            "ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
            "level": record.levelname,
            "logger": record.name,
            "msg": _redact(record.getMessage()),
        }, ensure_ascii=False)


_configured = False


def setup(level: str | None = None) -> None:
    """Configura il root logger una sola volta."""
    global _configured
    lvl = (level or os.environ.get("TRADER_LOG_LEVEL") or "INFO").upper()
    root = logging.getLogger()
    if not _configured:
        handler = logging.StreamHandler()
        if os.environ.get("TRADER_LOG_JSON") == "1":
            handler.setFormatter(_JsonFormatter())
        else:
            handler.setFormatter(logging.Formatter(
                "%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S"))
        handler.addFilter(_RedactFilter())
        # Rimuove eventuali handler preesistenti (es. basicConfig) per evitare doppioni.
        for h in list(root.handlers):
            root.removeHandler(h)
        root.addHandler(handler)
        _configured = True
    root.setLevel(lvl)
