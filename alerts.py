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
from datetime import datetime, timezone
from pathlib import Path

log = logging.getLogger(__name__)

_ROOT      = Path(__file__).parent
_LOG_PATH  = _ROOT / "alert_log.json"
IVA_RATE   = 0.22   # sconto IVA applicato al prezzo CEX


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


# ---------------------------------------------------------------------------
# Notifica macOS
# ---------------------------------------------------------------------------

def _notify(title: str, message: str) -> None:
    """Invia una notifica nativa macOS tramite osascript."""
    script = (
        f'display notification "{message}" '
        f'with title "{title}" '
        f'sound name "Ping"'
    )
    try:
        subprocess.run(
            ["osascript", "-e", script],
            capture_output=True, timeout=5,
        )
        log.info("Notifica inviata: %s — %s", title, message)
    except Exception as exc:
        log.warning("Notifica fallita: %s", exc)


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
    already_notified: set[str] = set(log_data.get("notified", {}).keys())

    sent = 0
    now  = datetime.now(timezone.utc).isoformat()

    for ad in ads:
        if not ad["last_available"]:
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
