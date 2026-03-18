from __future__ import annotations

import json
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter
from typing import Iterator


@dataclass
class RunReport:
    command: str
    started_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    finished_at: str | None = None
    status: str = "running"
    steps: list[dict] = field(default_factory=list)
    errors: list[dict] = field(default_factory=list)

    @contextmanager
    def step(self, name: str, details: dict | None = None) -> Iterator[dict]:
        payload = {
            "name": name,
            "details": details or {},
            "started_at": datetime.now(timezone.utc).isoformat(),
            "finished_at": None,
            "duration_s": None,
            "status": "running",
        }
        self.steps.append(payload)
        started = perf_counter()
        try:
            yield payload
            payload["status"] = "ok"
        except Exception as exc:  # noqa: BLE001
            payload["status"] = "error"
            payload["error"] = str(exc)
            self.errors.append({"step": name, "error": str(exc)})
            raise
        finally:
            payload["finished_at"] = datetime.now(timezone.utc).isoformat()
            payload["duration_s"] = round(perf_counter() - started, 3)

    def note_error(self, step: str, error: str, **extra) -> None:
        self.errors.append({"step": step, "error": error, **extra})

    def finalize(self, ok: bool) -> None:
        self.finished_at = datetime.now(timezone.utc).isoformat()
        self.status = "ok" if ok else "error"

    def to_dict(self) -> dict:
        return {
            "command": self.command,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "status": self.status,
            "steps": self.steps,
            "errors": self.errors,
        }

    def write(self, logs_dir: Path) -> Path:
        logs_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        path = logs_dir / f"run_report_{ts}.json"
        latest = logs_dir / "run_report_latest.json"
        payload = json.dumps(self.to_dict(), ensure_ascii=False, indent=2)
        path.write_text(payload + "\n", encoding="utf-8")
        latest.write_text(payload + "\n", encoding="utf-8")
        return path
