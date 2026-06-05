"""Registro eventi dei job (osservabilità deterministica).

Ogni esecuzione di un job (scrape/verifica/AI/backup/deploy), ovunque giri
(server, Mac residenziale, GitHub), registra un evento strutturato nella tabella
``job_runs`` di tracker.db. La pagina /log e gli alert si costruiscono su questa
tabella, senza più dedurre lo stato dal testo dei log.
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from paths import DB_PATH

# Catalogo job: id -> (etichetta, cadenza attesa in ore). L'ordine definisce
# anche l'ordine di visualizzazione su /log.
JOBS: list[tuple[str, str, int]] = [
    ("scrape-cex", "CEX (store)", 36),
    ("scrape-gameshock", "GameShock (store)", 36),
    ("scrape-gamepeople", "GamePeople (store)", 36),
    ("scrape-gamelife", "GameLife (residenziale)", 18),
    ("scrape-rebuy", "rebuy (GitHub)", 30),
    ("scrape-subito", "Subito — annunci (residenziale)", 18),
    ("scrape-ebay", "eBay — venduti (scrape)", 30),
    ("verify-sold", "Verifica venduti Subito", 18),
    ("ai-cascade", "Classificazione AI", 18),
    ("backup", "Backup DB", 30),
    ("deploy", "Auto-deploy", 0),  # cadenza 0 = nessun controllo di staleness
]
_LABELS = {jid: label for jid, label, _ in JOBS}
_CADENCE = {jid: cad for jid, _, cad in JOBS}
_VALID_STATUS = {"ok", "warn", "error"}


def init(db_path: Path = DB_PATH) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS job_runs (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                job        TEXT NOT NULL,
                host       TEXT,
                source     TEXT,
                status     TEXT NOT NULL,
                started_at TEXT,
                ended_at   TEXT NOT NULL,
                duration_s REAL,
                counts     TEXT,
                error      TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_job_runs_job ON job_runs(job, ended_at);
            """
        )


def record(
    job: str,
    status: str,
    *,
    db_path: Path = DB_PATH,
    host: str | None = None,
    source: str | None = None,
    started_at: str | None = None,
    ended_at: str | None = None,
    duration_s: float | None = None,
    counts: dict | None = None,
    error: str | None = None,
) -> None:
    """Registra l'esito di un job. status ∈ {ok, warn, error}."""
    if status not in _VALID_STATUS:
        status = "error"
    init(db_path)
    ended_at = ended_at or datetime.now(timezone.utc).isoformat()
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO job_runs (job, host, source, status, started_at, ended_at, "
            "duration_s, counts, error) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                job, host, source, status, started_at, ended_at, duration_s,
                json.dumps(counts, ensure_ascii=False) if counts else None,
                (error or None),
            ),
        )


def _age_hours(ended_at: str | None) -> float | None:
    if not ended_at:
        return None
    try:
        dt = datetime.fromisoformat(ended_at)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return max(0.0, (datetime.now(timezone.utc) - dt).total_seconds() / 3600.0)
    except ValueError:
        return None


def status(db_path: Path = DB_PATH) -> dict:
    """Stato di ogni job dall'ultima esecuzione registrata.

    Stato finale: lo status registrato, salvo override a 'warn' se il job non
    gira da oltre 2x la cadenza attesa (stale). Job senza run -> 'unknown'.
    """
    init(db_path)
    rank = {"error": 0, "warn": 1, "unknown": 2, "ok": 3}
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        latest: dict[str, sqlite3.Row] = {}
        for row in conn.execute(
            "SELECT * FROM job_runs WHERE id IN "
            "(SELECT MAX(id) FROM job_runs GROUP BY job)"
        ):
            latest[row["job"]] = row

    jobs_out: list[dict] = []
    overall = "ok"
    for jid, label, cadence in JOBS:
        row = latest.get(jid)
        if row is None:
            entry = {"id": jid, "label": label, "status": "unknown",
                     "summary": "Nessuna esecuzione registrata",
                     "last_run": None, "age_hours": None}
        else:
            st = row["status"] if row["status"] in _VALID_STATUS else "error"
            age = _age_hours(row["ended_at"])
            if cadence and age is not None and age > cadence * 2 and st == "ok":
                st = "warn"
            counts = {}
            try:
                counts = json.loads(row["counts"]) if row["counts"] else {}
            except (ValueError, TypeError):
                counts = {}
            summary = row["error"] or (
                ", ".join(f"{k}={v}" for k, v in counts.items()) if counts else "OK"
            )
            entry = {"id": jid, "label": label, "status": st,
                     "summary": summary[:200],
                     "last_run": row["ended_at"],
                     "age_hours": round(age, 1) if age is not None else None,
                     "host": row["host"], "source": row["source"]}
        if rank.get(entry["status"], 2) < rank.get(overall, 3):
            overall = entry["status"]
        jobs_out.append(entry)

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "overall": overall,
        "jobs": jobs_out,
    }
