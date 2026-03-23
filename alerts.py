"""
Sistema di alert prezzi — Xbox Tracker

Logica:
  Per ogni famiglia console, calcola la soglia come:
    soglia = MIN(prezzo_CEX_base_model) × (1 - 0.22)
    (se non ci sono base model → usa MIN(prezzi_CEX_famiglia) × 0.78)

  Dopo ogni scrape Subito, cerca annunci disponibili con prezzo < soglia.
  Invia notifica macOS per ogni nuovo match (urn_id non già notificato).
  Salva i notificati in alert_log.json per evitare duplicati.

Invocato da run.py dopo _update_db_from_snapshot() per la sorgente "subito".
"""

from __future__ import annotations

import json
import logging
import subprocess
import urllib.request
import urllib.parse
from datetime import datetime, timedelta, timezone
from pathlib import Path

log = logging.getLogger(__name__)

_ROOT      = Path(__file__).parent
_LOG_PATH  = _ROOT / "alert_log.json"
IVA_RATE   = 0.22   # sconto IVA applicato al prezzo CEX
_ALERT_LOG_RETENTION_DAYS = 90  # purge entry più vecchie di N giorni


# ---------------------------------------------------------------------------
# Calcolo soglie da prezzi CEX
# ---------------------------------------------------------------------------

def _compute_thresholds() -> dict[str, float]:
    """
    Restituisce { family_key: soglia_prezzo } calcolata su prezzi CEX.
    Priorità: base_models → tutti i prodotti CEX disponibili.
    """
    import db as _db

    thresholds: dict[str, float] = {}

    # 1) Base models CEX per famiglia
    base_models = _db.get_base_models()
    by_fam: dict[str, list[float]] = {}
    for p in base_models:
        if p["source"] == "cex" and p["last_price"] and p["last_available"]:
            fam = p["console_family"] or "other"
            by_fam.setdefault(fam, []).append(p["last_price"])

    for fam, prices in by_fam.items():
        thresholds[fam] = min(prices) * (1 - IVA_RATE)

    # 2) Per famiglie senza base model, usa min CEX globale
    all_products = _db.get_all_products()
    cex_by_fam: dict[str, list[float]] = {}
    for p in all_products:
        if p["source"] == "cex" and p["last_price"] and p["last_available"]:
            fam = p["console_family"] or "other"
            cex_by_fam.setdefault(fam, []).append(p["last_price"])

    for fam, prices in cex_by_fam.items():
        if fam not in thresholds:
            thresholds[fam] = min(prices) * (1 - IVA_RATE)

    return thresholds


# ---------------------------------------------------------------------------
# Log alert
# ---------------------------------------------------------------------------

def _load_log() -> dict:
    """Carica il log dei urn_id già notificati."""
    if _LOG_PATH.exists():
        try:
            return json.loads(_LOG_PATH.read_text(encoding="utf-8"))
        except Exception as exc:  # noqa: BLE001
            log.warning("Alert log corrotto (%s), ricreo il file.", exc)
    return {"notified": {}}   # { urn_id: iso_timestamp }


def _save_log(log_data: dict) -> None:
    _LOG_PATH.write_text(
        json.dumps(log_data, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _purge_old_entries(log_data: dict) -> int:
    """Rimuove dal log le entry più vecchie di _ALERT_LOG_RETENTION_DAYS giorni.

    Returns:
        Numero di entry rimosse.
    """
    notified = log_data.get("notified", {})
    if not notified:
        return 0

    cutoff = datetime.now(timezone.utc) - timedelta(days=_ALERT_LOG_RETENTION_DAYS)
    to_remove: list[str] = []

    for urn_id, ts_str in notified.items():
        try:
            ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
            if ts < cutoff:
                to_remove.append(urn_id)
        except (ValueError, TypeError, AttributeError):
            # Entry malformata → rimuovi per sicurezza
            to_remove.append(urn_id)

    for urn_id in to_remove:
        del notified[urn_id]

    if to_remove:
        log.info("Alert log: purgate %d entry più vecchie di %d giorni.", len(to_remove), _ALERT_LOG_RETENTION_DAYS)

    return len(to_remove)


# ---------------------------------------------------------------------------
# Config Telegram (caricata lazy)
# ---------------------------------------------------------------------------

_telegram_cfg: dict | None = None

def _get_telegram_cfg() -> dict:
    """Carica la config Telegram da config.toml (lazy, una sola volta)."""
    global _telegram_cfg
    if _telegram_cfg is not None:
        return _telegram_cfg
    try:
        from settings import load_default_config
        cfg = load_default_config()
        _telegram_cfg = cfg.get("telegram", {})
    except Exception:  # noqa: BLE001
        _telegram_cfg = {}
    return _telegram_cfg


# ---------------------------------------------------------------------------
# Notifica macOS
# ---------------------------------------------------------------------------

def _escape_applescript(text: str) -> str:
    """Escape per stringhe AppleScript: backslash e virgolette doppie."""
    return text.replace("\\", "\\\\").replace('"', '\\"')


def _notify_macos(title: str, message: str) -> None:
    """Invia una notifica nativa macOS tramite osascript."""
    safe_title = _escape_applescript(title)
    safe_message = _escape_applescript(message)
    script = (
        f'display notification "{safe_message}" '
        f'with title "{safe_title}" '
        f'sound name "Ping"'
    )
    try:
        subprocess.run(
            ["osascript", "-e", script],
            capture_output=True, timeout=5,
        )
        log.info("Notifica macOS: %s — %s", title, message)
    except Exception as exc:
        log.warning("Notifica macOS fallita: %s", exc)


# ---------------------------------------------------------------------------
# Notifica Telegram
# ---------------------------------------------------------------------------

def _send_telegram(title: str, message: str) -> bool:
    """Invia un messaggio Telegram via Bot API (urllib, zero dipendenze extra).

    Returns:
        True se il messaggio è stato inviato con successo, False altrimenti.
    """
    cfg = _get_telegram_cfg()
    if not cfg.get("enabled"):
        return False

    bot_token = cfg.get("bot_token", "")
    chat_id = cfg.get("chat_id", "")
    if not bot_token or not chat_id:
        return False

    text = f"*{title}*\n{message}"
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = urllib.parse.urlencode({
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "Markdown",
        "disable_web_page_preview": "true",
    }).encode("utf-8")

    try:
        req = urllib.request.Request(url, data=payload, method="POST")
        with urllib.request.urlopen(req, timeout=10) as resp:
            ok = resp.status == 200
        if ok:
            log.info("Telegram inviato: %s", title)
        return ok
    except Exception as exc:
        log.warning("Telegram fallito: %s", exc)
        return False


# ---------------------------------------------------------------------------
# Notifica unificata (macOS + Telegram)
# ---------------------------------------------------------------------------

def _notify(title: str, message: str) -> None:
    """Invia notifica via tutti i canali attivi (macOS + Telegram)."""
    _notify_macos(title, message)
    _send_telegram(title, message)


# ---------------------------------------------------------------------------
# Entry point principale
# ---------------------------------------------------------------------------

def check_alerts() -> int:
    """
    Controlla gli annunci Subito contro le soglie CEX.
    Invia notifiche macOS per i nuovi match.

    Returns:
        numero di notifiche inviate
    """
    import db_subito as _db_subito

    thresholds = _compute_thresholds()
    if not thresholds:
        log.info("Alert: nessun prezzo CEX disponibile per calcolare soglie.")
        return 0

    log.info(
        "Alert soglie (CEX - 22%% IVA): %s",
        {k: f"€{v:.0f}" for k, v in thresholds.items()},
    )

    ads     = _db_subito.get_all_ads()
    log_data = _load_log()
    _purge_old_entries(log_data)
    already_notified: set[str] = set(log_data.get("notified", {}).keys())

    sent = 0
    now  = datetime.now(timezone.utc).isoformat()

    for ad in ads:
        if not ad["last_available"]:
            continue
            
        # Saltiamo gli annunci non esplicitamente approvati dall'AI
        if ad.get("ai_status") != "approved":
            continue

        price  = ad["last_price"]
        family = ad["console_family"] or "other"
        urn_id = ad["urn_id"]

        if price is None or price <= 0:
            continue

        threshold = thresholds.get(family)
        if threshold is None:
            continue

        if price >= threshold:
            continue

        if urn_id in already_notified:
            continue

        # Nuovo match sotto soglia → notifica
        fam_label = {
            "series-x": "Xbox Series X",
            "series-s": "Xbox Series S",
            "one-x":    "Xbox One X",
            "one-s":    "Xbox One S",
            "one":      "Xbox One",
            "360":      "Xbox 360",
            "original": "Xbox Original",
        }.get(family, family.upper())

        city_part = f" · {ad['city']}" if ad.get("city") else ""
        msg = f"€{price:.0f} (soglia €{threshold:.0f}){city_part} — {ad['name'][:60]}"
        _notify(f"🎮 Deal {fam_label}!", msg)

        log_data["notified"][urn_id] = now
        sent += 1

    if sent:
        _save_log(log_data)
        log.info("Alert: %d nuove notifiche inviate.", sent)
    else:
        log.info("Alert: nessun nuovo match sotto soglia.")

    return sent
