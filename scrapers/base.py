"""
Utilities condivise tra tutti gli scraper.

Ogni scraper deve:
  1. Importare clean_price, retry, save_snapshot da qui
  2. Produrre prodotti nel formato standard (vedi PRODUCT_SCHEMA)
  3. Chiamare save_snapshot(source, products, url, data_dir) per salvare

Formato standard prodotto:
  {
    "name":          str,        # nome completo del prodotto
    "sku":           str,        # codice univoco (es. "HWXX0001_U")
    "price":         float|None, # prezzo in euro (None se non disponibile)
    "price_display": str,        # es. "549,99 €"
    "condition":     str,        # "Nuovo" | "Usato" | "N/D"
    "available":     bool,       # True se il prodotto è acquistabile ora
    "url":           str,        # URL pagina prodotto
    "image_url":     str,        # URL immagine (può essere "")
    "source":        str,        # nome sorgente, es. "gamelife"
  }

  Campi opzionali (presenti solo in alcune sorgenti):
    "grade":        str,   # grading qualità (es. "Imballata", "Eccellente")
    "availability": str,   # etichetta testuale (es. "Ordinabile", "Esaurito")
    "category":     str,   # categoria originale del sito sorgente
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from datetime import datetime
from pathlib import Path

log = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Pulizia prezzi
# --------------------------------------------------------------------------- #

def clean_price(raw: str) -> float | None:
    """Converte stringhe prezzo in float.

    Gestisce:
      - formato italiano:       "349,99 €"  → 349.99
      - formato internazionale: "349.99"    → 349.99
      - separatore migliaia:    "1.349,99"  → 1349.99
      - solo cifre intere:      "350"       → 350.0
    """
    if not raw:
        return None
    s = raw.strip()
    s = re.sub(r"[€$£\s]", "", s)
    if not s:
        return None
    # Entrambi separatori → punto = migliaia, virgola = decimale
    if "." in s and "," in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        s = s.replace(",", ".")
    # Rimuovi qualsiasi carattere non numerico (tranne punto)
    s = re.sub(r"[^\d.]", "", s)
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


# --------------------------------------------------------------------------- #
# Retry asincrono
# --------------------------------------------------------------------------- #

async def retry(coro_fn, retries: int = 3, delay: float = 2.0, label: str = ""):
    """Esegue coro_fn() con retry esponenziale.

    Args:
        coro_fn: callable async senza argomenti
        retries: numero massimo di tentativi
        delay:   attesa base in secondi (raddoppia ad ogni tentativo)
        label:   stringa per i log di warning
    Raises:
        RuntimeError se tutti i tentativi falliscono
    """
    last_exc = None
    for attempt in range(1, retries + 1):
        try:
            return await coro_fn()
        except Exception as exc:
            last_exc = exc
            wait = delay * (2 ** (attempt - 1))
            log.warning(
                "%s — tentativo %d/%d fallito: %s. Attendo %.1fs...",
                label, attempt, retries, exc, wait,
            )
            await asyncio.sleep(wait)
    raise RuntimeError(f"{label} — tutti i {retries} tentativi falliti") from last_exc


# --------------------------------------------------------------------------- #
# Retry sincrono (per scraper requests-based)
# --------------------------------------------------------------------------- #

def retry_sync(fn, retries: int = 3, delay: float = 2.0, label: str = ""):
    """Versione sincrona di retry per scraper che usano requests."""
    import time
    last_exc = None
    for attempt in range(1, retries + 1):
        try:
            return fn()
        except Exception as exc:
            last_exc = exc
            wait = delay * (2 ** (attempt - 1))
            log.warning(
                "%s — tentativo %d/%d fallito: %s. Attendo %.1fs...",
                label, attempt, retries, exc, wait,
            )
            time.sleep(wait)
    raise RuntimeError(f"{label} — tutti i {retries} tentativi falliti") from last_exc


# --------------------------------------------------------------------------- #
# Salvataggio snapshot
# --------------------------------------------------------------------------- #

def save_snapshot(
    source:   str,
    products: list[dict],
    url:      str,
    data_dir: Path,
) -> Path:
    """Salva uno snapshot JSON standardizzato.

    Nome file: data/{source}_{YYYY-MM-DD_HH-MM-SS}.json

    Args:
        source:   nome della sorgente (es. "gamelife", "cex")
        products: lista prodotti nel formato standard
        url:      URL di partenza usato per lo scrape
        data_dir: directory di output
    Returns:
        Path del file salvato
    """
    data_dir.mkdir(parents=True, exist_ok=True)
    now = datetime.now()
    ts = now.strftime("%Y-%m-%d_%H-%M-%S")
    out_path = data_dir / f"{source}_{ts}.json"
    payload = {
        "source":     source,
        "url":        url,
        "scraped_at": now.isoformat(),
        "total":      len(products),
        "products":   products,
    }
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    log.info("Salvato: %s  (%d prodotti)", out_path.name, len(products))
    return out_path


# --------------------------------------------------------------------------- #
# Deduplicazione
# --------------------------------------------------------------------------- #

def deduplicate(products: list[dict]) -> list[dict]:
    """Rimuove duplicati per SKU (mantiene prima occorrenza).
    Se lo SKU è assente, usa l'URL come chiave.
    """
    seen: set[str] = set()
    unique: list[dict] = []
    for p in products:
        key = p.get("sku") or p.get("url") or p.get("name", "")
        if key and key not in seen:
            seen.add(key)
            unique.append(p)
    return unique


# --------------------------------------------------------------------------- #
# Playwright browser launch con fallback
# --------------------------------------------------------------------------- #

async def launch_chromium(playwright, headless: bool = True, preferred_channel: str = "chrome"):
    """Avvia Chromium con fallback robusto tra channel e bundled binary.

    Ordine tentativi:
      1) channel da env/config (es. \"chrome\")
      2) chromium bundled Playwright (nessun channel)
    """
    requested = (os.environ.get("TRADER_PLAYWRIGHT_CHANNEL") or preferred_channel or "").strip().lower()

    attempts: list[dict] = []
    if requested and requested != "chromium":
        attempts.append({"channel": requested})
    attempts.append({})

    last_exc = None
    for opts in attempts:
        label = opts.get("channel", "bundled-chromium")
        try:
            browser = await playwright.chromium.launch(headless=headless, **opts)
            log.info("Playwright browser avviato: %s", label)
            return browser
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            log.warning("Playwright launch fallito (%s): %s", label, exc)

    raise RuntimeError("Impossibile avviare Playwright Chromium con fallback") from last_exc

