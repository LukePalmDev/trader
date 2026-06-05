"""Registro eventi dei job (osservabilità deterministica).

Ogni esecuzione di un job (scrape/verifica/AI/backup/deploy), ovunque giri
(server, Mac residenziale, GitHub), registra un evento strutturato nella tabella
``job_runs`` di tracker.db. La pagina /log e gli alert si costruiscono su questa
tabella, senza più dedurre lo stato dal testo dei log.
"""
from __future__ import annotations

import json
import os
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

from paths import DB_PATH

# Host di provenienza dell'esecuzione (server / github / mac). Override via env.
HOST = os.environ.get("TRADER_HOST", "server")

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
    if host is None:
        host = HOST
    init(db_path)
    ended_at = ended_at or datetime.now(timezone.utc).isoformat()
    with sqlite3.connect(db_path) as conn:
        prev = conn.execute(
            "SELECT status FROM job_runs WHERE job = ? ORDER BY id DESC LIMIT 1", (job,)
        ).fetchone()
        prev_status = prev[0] if prev else None
        conn.execute(
            "INSERT INTO job_runs (job, host, source, status, started_at, ended_at, "
            "duration_s, counts, error) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                job, host, source, status, started_at, ended_at, duration_s,
                json.dumps(counts, ensure_ascii=False) if counts else None,
                (error or None),
            ),
        )

    # Alert solo alla transizione verso error/warn (evita spam su errori ripetuti).
    if status in ("error", "warn") and status != prev_status:
        _alert(job, status, error)


def _alert(job: str, status: str, error: str | None) -> None:
    try:
        import alerts
        label = _LABELS.get(job, job)
        icon = "🔴" if status == "error" else "🟠"
        alerts._send_telegram(
            f"{icon} Job '{label}' → {status}",
            (error or "Vedi https://trader.byluke.org/log"),
        )
    except Exception:  # noqa: BLE001 — l'alert non deve mai rompere il recording
        pass


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
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        latest: dict[str, sqlite3.Row] = {}
        for row in conn.execute(
            "SELECT * FROM job_runs WHERE id IN "
            "(SELECT MAX(id) FROM job_runs GROUP BY job)"
        ):
            latest[row["job"]] = row

    jobs_out: list[dict] = []
    # overall riflette solo gli stati azionabili (error > warn); 'unknown' (job
    # senza dati) non degrada lo stato generale.
    has_error = has_warn = False
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
        if entry["status"] == "error":
            has_error = True
        elif entry["status"] == "warn":
            has_warn = True
        jobs_out.append(entry)

    overall = "error" if has_error else ("warn" if has_warn else "ok")
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "overall": overall,
        "jobs": jobs_out,
    }


def check_and_alert(db_path: Path = DB_PATH) -> dict:
    """Watchdog: alza un alert Telegram per i job in error/warn (incl. stale),
    con dedup: avvisa solo quando l'insieme dei job problematici cambia."""
    s = status(db_path)
    bad = {j["id"]: j["status"] for j in s["jobs"] if j["status"] in ("error", "warn")}
    state_file = Path(os.environ.get("TRADER_LOG_DIR", "/var/log/trader")) / ".watchdog.json"
    try:
        prev = json.loads(state_file.read_text()) if state_file.exists() else {}
    except (OSError, ValueError):
        prev = {}
    if bad and bad != prev:
        labels = {jid: lbl for jid, lbl, _ in JOBS}
        righe = "\n".join(f"{'🔴' if st == 'error' else '🟠'} {labels.get(j, j)}: {st}"
                          for j, st in bad.items())
        try:
            import alerts
            alerts._send_telegram("Watchdog job trader", righe +
                                  "\nhttps://trader.byluke.org/log")
        except Exception:  # noqa: BLE001
            pass
    try:
        state_file.write_text(json.dumps(bad))
    except OSError:
        pass
    return {"bad": bad, "changed": bad != prev}


def _main(argv: list[str]) -> int:
    """CLI per i job in bash: job_runs.py record JOB STATUS [--source S] [--error E]."""
    if argv and argv[0] == "check":
        res = check_and_alert()
        print(f"watchdog: bad={res['bad']} changed={res['changed']}")
        return 0
    if len(argv) < 3 or argv[0] != "record":
        print("uso: job_runs.py record <job> <ok|warn|error> [--source S] [--error E]\n"
              "     job_runs.py check", file=sys.stderr)
        return 64
    job, st = argv[1], argv[2]
    source = err = None
    rest = argv[3:]
    for i, tok in enumerate(rest):
        if tok == "--source" and i + 1 < len(rest):
            source = rest[i + 1]
        elif tok == "--error" and i + 1 < len(rest):
            err = rest[i + 1]
    record(job, st, source=source, error=err)
    return 0


if __name__ == "__main__":
    raise SystemExit(_main(sys.argv[1:]))
