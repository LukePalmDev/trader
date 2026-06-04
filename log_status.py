"""Raccoglie lo stato dei log per la pagina /log.

Due fonti:
  - job server (systemd): file in /var/log/trader/<job>.log, append per run.
  - storico GitHub Actions: LogGitHub/<workflow>/#<n>/run.log (archiviati).

Per ognuno calcola uno stato sintetico: "ok" (verde), "warn" (arancione),
"error" (rosso) o "stale"/"unknown". Tutto best-effort e difensivo: cartelle o
file mancanti non sollevano eccezioni.
"""
from __future__ import annotations

import re
from datetime import datetime, timezone
from pathlib import Path

# Per ogni job: cadenza attesa (ore), file di log e pattern d'esito opzionali.
#   log_file   -> file in TRADER_LOG_DIR (default: "<id>.log")
#   error_re   -> forza ROSSO (problema che richiede intervento)
#   problem_re -> forza ARANCIONE (es. 0 risultati / fonte bloccata)
#   ok_re      -> conferma VERDE anche con warning transitori (run completato)
#
# Ogni fonte è una voce separata, in ordine. Le fonti scrivono un marker
# per-fonte in source-<fonte>.log (vedi run.py:_write_source_marker), così
# l'esito è indipendente ovunque giri lo scrape (server, Mac, GitHub).
def _src(label: str, src: str, cadence: int) -> dict:
    return {"label": label, "cadence_h": cadence,
            "log_file": f"source-{src}.log", "ok_re": rf"job scrape-{src} OK"}


_SERVER_JOBS: dict[str, dict] = {
    # --- Fonti negozi ---
    "scrape-cex": _src("CEX (store)", "cex", 36),
    "scrape-gameshock": _src("GameShock (store)", "gameshock", 36),
    "scrape-gamepeople": _src("GamePeople (store)", "gamepeople", 36),
    "scrape-gamelife": _src("GameLife (residenziale)", "gamelife", 18),
    "scrape-rebuy": _src("rebuy (GitHub)", "rebuy", 30),
    # --- Marketplace ---
    "scrape-subito": _src("Subito — annunci (residenziale)", "subito", 18),
    "scrape-ebay": _src("eBay — venduti (scrape)", "ebay", 30),
    # --- Verifiche / AI ---
    "verify-sold": {
        "label": "Verifica venduti Subito", "cadence_h": 12,
        "ok_re": r"Verifica completata",
    },
    "ai-classify": {
        "label": "Classificazione AI", "cadence_h": 12,
        "error_re": r"credit balance is too low|insufficient_quota|authentication_error",
        "ok_re": r"[Cc]lassificazione completata",
    },
    # --- Sistema ---
    "backup": {
        "label": "Backup DB", "cadence_h": 24,
        "ok_re": r"\[backup\] OK",
    },
}

# Cartelle storiche GitHub -> etichetta leggibile.
_GH_WORKFLOWS: dict[str, str] = {
    "Scraper_Fonti": "GitHub · Scrape Fonti",
    "Subito.it": "GitHub · Scrape Subito",
    "eBay": "GitHub · Scrape eBay",
    "AI_Classify": "GitHub · Classificazione AI",
    "Verify_Sold": "GitHub · Verifica venduti",
}

# Fallimento REALE del job (rosso): crash/uscita non-zero, non i singoli
# errori di fetch transitori che non interrompono il run.
_FATAL_RE = re.compile(
    r"traceback \(most recent call last\)|exit code [1-9]|"
    r"another job is already running|unhandledexception|\bfatal\b|"
    r"\bcritical\b|killed|\boom\b|segmentation fault|modulenotfounderror",
    re.I,
)
# Problemi non fatali (arancione): errori/warning di singole richieste, retry.
_ISSUE_RE = re.compile(
    r"\[error\]|\[warning\]|\berrore\b|\bwarn(ing)?\b|\bfailed\b|"
    r"429|too many requests|timeout|attenzione",
    re.I,
)
# Run GitHub Actions fallito (storico).
_GH_FAIL_RE = re.compile(r"##\[error\]|exit code [1-9]|\bfailed\b|\berror:\b", re.I)
_STARTED_RE = re.compile(r"job\s+\S+\s+started at\s+(\S+)")


def _tail(path: Path, max_bytes: int = 60_000) -> str:
    try:
        size = path.stat().st_size
        with path.open("rb") as fh:
            if size > max_bytes:
                fh.seek(-max_bytes, 2)
            return fh.read().decode("utf-8", "replace")
    except OSError:
        return ""


def _last_run_segment(text: str) -> str:
    """Ritorna il testo dell'ultimo run (dall'ultimo marker di avvio in poi)."""
    idx = max(text.rfind("] job "), text.rfind("[backup] start"))
    return text[idx:] if idx >= 0 else text


def _age_hours(mtime: float) -> float:
    return max(0.0, (datetime.now(timezone.utc).timestamp() - mtime) / 3600.0)


def _classify(segment: str, age_h: float, meta: dict) -> tuple[str, str]:
    """Ritorna (stato, riga di sintesi) in base all'esito reale del run."""
    cadence_h = meta["cadence_h"]
    # 1) Errore specifico del job (es. credito API esaurito) -> rosso.
    if meta.get("error_re"):
        rx = re.compile(meta["error_re"], re.I)
        if rx.search(segment):
            return "error", _first_match_line(segment, rx)
    # 2) Crash generico -> rosso.
    if _FATAL_RE.search(segment):
        return "error", _first_match_line(segment, _FATAL_RE)
    # 3) Problema d'esito (0 risultati / fonte bloccata) -> arancione.
    if meta.get("problem_re"):
        rx = re.compile(meta["problem_re"], re.I)
        if rx.search(segment):
            return "warn", _first_match_line(segment, rx)
    # 4) Non gira da troppo tempo.
    if age_h > cadence_h * 2:
        return "stale", f"Nessun run da {age_h:.0f}h (cadenza ~{cadence_h:.0f}h)"
    # 5) Run completato con successo -> verde (ignora warning transitori).
    if meta.get("ok_re"):
        rx = re.compile(meta["ok_re"], re.I)
        if rx.search(segment):
            return "ok", _last_meaningful_line(segment)
    # 6) Warning transitori senza conferma di completamento -> arancione.
    if _ISSUE_RE.search(segment):
        return "warn", _first_match_line(segment, _ISSUE_RE)
    return "ok", _last_meaningful_line(segment)


def _first_match_line(text: str, rx: re.Pattern[str]) -> str:
    for line in text.splitlines():
        if rx.search(line):
            return line.strip()[:200]
    return ""


def _last_meaningful_line(text: str) -> str:
    for line in reversed(text.splitlines()):
        s = line.strip()
        if s:
            return s[:200]
    return ""


def _server_jobs(log_dir: Path) -> list[dict]:
    out: list[dict] = []
    for job, meta in _SERVER_JOBS.items():
        path = log_dir / meta.get("log_file", f"{job}.log")
        entry = {
            "id": job,
            "label": meta["label"],
            "category": "server",
            "status": "unknown",
            "last_run": None,
            "age_hours": None,
            "summary": "Nessun log trovato",
        }
        if path.exists():
            text = _tail(path)
            seg = _last_run_segment(text)
            age = _age_hours(path.stat().st_mtime)
            status, summary = _classify(seg, age, meta)
            started = _STARTED_RE.search(seg)
            entry.update(
                status=status,
                last_run=(started.group(1) if started else
                          datetime.fromtimestamp(path.stat().st_mtime, timezone.utc).isoformat()),
                age_hours=round(age, 1),
                summary=summary or "—",
            )
        out.append(entry)
    return out


def _github_archive(app_dir: Path) -> list[dict]:
    base = app_dir / "LogGitHub"
    out: list[dict] = []
    if not base.is_dir():
        return out
    for folder, label in _GH_WORKFLOWS.items():
        wdir = base / folder
        if not wdir.is_dir():
            continue
        runs = []
        for d in wdir.iterdir():
            m = re.match(r"#(\d+)", d.name)
            if m and d.is_dir():
                runs.append((int(m.group(1)), d))
        if not runs:
            continue
        runs.sort()
        last_n, last_dir = runs[-1]
        run_log = last_dir / "run.log"
        seg = _tail(run_log) if run_log.exists() else ""
        if _GH_FAIL_RE.search(seg):
            status, summary = "error", _first_match_line(seg, _GH_FAIL_RE)
        elif seg:
            status, summary = "ok", "Ultimo run archiviato completato"
        else:
            status, summary = "unknown", "Log non disponibile"
        out.append({
            "id": folder,
            "label": label,
            "category": "github",
            "status": status,
            "runs": len(runs),
            "last_run": f"#{last_n}",
            "summary": summary or "—",
        })
    return out


def _github_last_run_log(app_dir: Path, job: str) -> Path | None:
    if job not in _GH_WORKFLOWS:
        return None
    wdir = app_dir / "LogGitHub" / job
    if not wdir.is_dir():
        return None
    runs = [(int(m.group(1)), d) for d in wdir.iterdir()
            if d.is_dir() and (m := re.match(r"#(\d+)", d.name))]
    if not runs:
        return None
    runs.sort()
    return runs[-1][1] / "run.log"


def raw_log(app_dir: Path, log_dir: Path, job: str, lines: int = 200) -> str | None:
    """Tail (max 1000 righe) del log di un job. None se job sconosciuto/assente.

    Il job è validato contro le whitelist (job server o workflow GitHub), quindi
    niente path traversal.
    """
    lines = max(1, min(int(lines), 1000))
    if job in _SERVER_JOBS:
        path = Path(log_dir) / _SERVER_JOBS[job].get("log_file", f"{job}.log")
    else:
        path = _github_last_run_log(Path(app_dir), job)
    if not path or not path.exists():
        return None
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return None
    rows = text.splitlines()
    return "\n".join(rows[-lines:])


def collect(app_dir: Path, log_dir: Path) -> dict:
    """Stato completo dei log (server + storico GitHub)."""
    jobs = _server_jobs(Path(log_dir))
    archive = _github_archive(Path(app_dir))
    rank = {"error": 0, "stale": 1, "warn": 2, "unknown": 3, "ok": 4}
    overall = "ok"
    for j in jobs:
        if rank.get(j["status"], 3) < rank.get(overall, 4):
            overall = j["status"]
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "overall": overall,
        "jobs": jobs,
        "archive": archive,
    }
