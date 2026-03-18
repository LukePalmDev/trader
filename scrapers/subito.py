"""
Scraper Subito.it — Console Xbox (annunci privati e professionali)

Tecnica: Playwright + channel="chrome" + window.__NEXT_DATA__
  Subito.it usa Akamai Bot Manager → Chromium bundled → 403.
  Usando il Chrome di sistema (channel="chrome") si bypassa il blocco.
  Context fresco per ogni pagina (stesso pattern di gamelife).
  I dati sono in window.__NEXT_DATA__.props.pageProps.initialState.items.originalList

URL di ricerca: /annunci-italia/vendita/usato/?q=<keyword>
  (la categoria /vendita/console/ non esiste — Akamai restituirebbe 404)

Salva in: data/subito_YYYY-MM-DD_HH-MM-SS.json

Query tracciate:
  - "xbox series x console"
  - "xbox series s console"
  - "xbox one console"
  - "xbox 360 console"
  - "xbox original console"

Filtri titolo annuncio — ESCLUDI se contiene:
  controller, gamepad, joypad, joystick, pad, manetta/e,
  headset, cuffie, auricolari, microfono,
  cavo/i, alimentatore, adattatore, psu, caricatore,
  hard disk, hdd, ssd, disco rigido, pendrive, chiavetta,
  game pass, xbox live, gold, abbonamento, dlc, codice, voucher, gift card,
  hdmi, monitor, schermo, televisore,
  kinect, stand, ventola, dock, batteria/e, stazione di ricarica,
  gioco, giochi

ACCETTA solo se il titolo contiene: console | xbox series x/s | xbox one x/s | xbox 360 | xbox original

Campi extra rispetto allo schema standard (base.py):
  "city"         — comune del venditore  (geo.town.value)
  "region"       — regione del venditore (geo.region.value)
  "published_at" — data pubblicazione    (item.date, es. "2026-03-18 08:21:53")
  "seller_type"  — "privato" | "professionale"  (advertiser.company)
"""

from __future__ import annotations

import asyncio
import logging
import re
from pathlib import Path

from playwright.async_api import async_playwright

from id_utils import stable_item_id
from scrapers.base import (
    clean_price,
    deduplicate,
    launch_chromium,
    retry,
    save_snapshot,
)
from settings import load_config

log = logging.getLogger("subito")

# --- Config ---
_CONFIG_PATH = Path(__file__).parent.parent / "config.toml"
_CFG = load_config(_CONFIG_PATH)

_COMMON = _CFG["common"]
_DATA   = _CFG["data"]

BASE_URL    = "https://www.subito.it"
# /vendita/console/ non esiste — si usa /vendita/usato/ con query
SEARCH_BASE = f"{BASE_URL}/annunci-italia/vendita/usato/"
DATA_DIR    = Path(__file__).parent.parent / _DATA["output_dir"]
SOURCE      = "subito"

# Limite massimo pagine per query (ogni pagina ≈ 30 annunci)
# Subito.it supporta fino a 300 pagine per query (≈9.000 risultati/query)
# 30 pagine × 5 query = max ~4.500 annunci lordi (deduplicati a fine run)
MAX_PAGES = 30

# Query di ricerca: (label, keyword_per_URL)
_QUERIES = [
    ("Xbox Series X",  "xbox series x console"),
    ("Xbox Series S",  "xbox series s console"),
    ("Xbox One",       "xbox one console"),
    ("Xbox 360",       "xbox 360 console"),
    ("Xbox Original",  "xbox original console"),
]


# --------------------------------------------------------------------------- #
# Filtri titolo annuncio
# --------------------------------------------------------------------------- #

_BLOCKLIST = re.compile(
    r"""
    \b(
      # Input / periferiche
      controller | gamepad | joypad | joystick | \bpad\b | manett[ae]
      # Audio
    | headset | cuffie? | auricolari | microfono
      # Cavi / alimentazione
    | \bcavo\b | \bcavi\b | alimentator[ei] | adattattor[ei] | \bpsu\b | caricatore
      # Storage esterno
    | hard\s*disk | \bhdd\b | \bssd\b | disco\s+rigido | pendrive | chiavetta
      # Digitale / software / abbonamenti
    | game\s*pass | xbox\s*live | \bgold\b | abbonamento | \bdlc\b
    | \bcodice\b | voucher | gift\s*card | carta\s*regalo
      # Video output
    | \bhdmi\b | \bmonitor\b | \bschermo\b | televisore
      # Accessori fisici
    | kinect | \bstand\b | \bventola\b | \bdock\b | batterie? | stazione\s+di\s+ricarica
    | \bskin\b | pellicola | \bcover\b | copertura | antipolvere | custodia
    | \bsupporto\b | \blettore\b | mascherina | \bricambi?\b | \bmanuale\b
    | \bcopertine?\b | \blotto\b | \bpezzi\b | \bparte\b | \bricambio\b
      # Giochi (keywords generici + titoli noti)
    | \bgioco\b | \bgiochi\b | videogioco | videogiochi | \bedizione\s+classic
    | \bfifa\d* | \bnba\b | \bpes\b | \bcall\s+of\b | minecraft | battlefield
    | \bassassin | \bforza\b | \bgta\b | \bhalo\b(?!\s+console)
    | \bcod\b | \boverwatch\b | \briders?\b | \bcrysis\b | \balone\b
    | dead\s+or\s+alive | resident\s+evil | burnout | lego\b | \bfor\s+honor\b
    | def\s+jam | \bthe\s+club\b | \bwwe\b | \bufc\b | \bmortal\b | \bdoom\b
    )\b
    """,
    re.IGNORECASE | re.VERBOSE,
)

_IS_CONSOLE = re.compile(
    r"""
    \b(
      console
    | xbox\s+series\s+x\b
    | xbox\s+series\s+s\b
    | xbox\s+one\s+x\b
    | xbox\s+one\s+s\b
    | xbox\s+one\b
    | xbox\s+360\b
    | xbox\s+original\b
    )\b
    """,
    re.IGNORECASE | re.VERBOSE,
)

# Mappa chiave condizione Subito → label standard
_CONDITION_MAP = {
    "10": "Nuovo",      # Nuovo
    "20": "Usato",      # Come nuovo / ricondizionato
    "30": "Usato",      # Buone condizioni
    "40": "Usato",      # Accettabile
    "50": "Usato",      # Solo per i pezzi
}


# --------------------------------------------------------------------------- #
# Parsing di un singolo annuncio
# --------------------------------------------------------------------------- #

def _parse_ad(item: dict) -> dict | None:
    """Converte un annuncio Subito nel formato standard."""
    subject = (item.get("subject") or "").strip()
    if not subject:
        return None

    # --- Filtri ---
    if _BLOCKLIST.search(subject):
        log.debug("Blocklist: %r", subject)
        return None
    if not _IS_CONSOLE.search(subject):
        log.debug("Non console: %r", subject)
        return None

    # --- URL ---
    urls = item.get("urls") or {}
    url  = urls.get("default") or ""

    # --- SKU: estrae ID numerico da urn "id:ad:{uuid}:list:{id}" ---
    urn         = item.get("urn") or ""
    sku_match   = re.search(r":list:(\d+)$", urn)
    sku         = f"SUBITO-{sku_match.group(1)}" if sku_match else stable_item_id("SUBITO", url)

    # --- Prezzo: features./price.values[0] ---
    features   = item.get("features") or {}
    price_feat = features.get("/price") or {}
    price_vals = price_feat.get("values") or []
    if price_vals:
        price_raw     = str(price_vals[0].get("key") or "")
        price_display = str(price_vals[0].get("value") or price_raw or "N/D")
        price         = clean_price(price_raw)
        if price is not None and price <= 0:
            price         = None     # Annuncio "Trattabile" senza prezzo fisso
            price_display = "Trattabile"
    else:
        price_raw     = ""
        price_display = "N/D"
        price         = None

    # --- Condizione: features./item_condition ---
    cond_feat = features.get("/item_condition") or {}
    cond_vals = cond_feat.get("values") or []
    if cond_vals:
        cond_key  = str(cond_vals[0].get("key") or "")
        condition = _CONDITION_MAP.get(cond_key, "Usato")
    else:
        condition = "N/D"

    # --- Geo ---
    geo    = item.get("geo") or {}
    city   = ((geo.get("town")   or {}).get("value") or "").strip()
    region = ((geo.get("region") or {}).get("value") or "").strip()

    # --- Immagine: images[0].cdnBaseUrl ---
    images    = item.get("images") or []
    image_url = (images[0].get("cdnBaseUrl") or "") if images else ""

    # --- Data pubblicazione: campo top-level "date" ---
    published_at = (item.get("date") or "").strip()

    # --- Tipo venditore: advertiser.company ---
    advertiser  = item.get("advertiser") or {}
    seller_type = "professionale" if advertiser.get("company") else "privato"

    # --- Venduto: type.key != "s" (s=In vendita, k=Venduto) ---
    item_type = item.get("type") or {}
    sold = (item_type.get("key") or "s") != "s"

    return {
        # Campi standard
        "name":          subject,
        "sku":           sku,
        "price":         price,
        "price_display": price_display,
        "condition":     condition,
        "available":     not sold,
        "url":           url,
        "image_url":     image_url,
        "source":        SOURCE,
        # Campi extra Subito
        "city":          city,
        "region":        region,
        "published_at":  published_at,
        "seller_type":   seller_type,
    }


# --------------------------------------------------------------------------- #
# Playwright helpers
# --------------------------------------------------------------------------- #

async def _new_context(browser):
    """Context fresco — bypassa il bot-detection di Akamai."""
    return await browser.new_context(
        user_agent=_COMMON["user_agent"],
        viewport={
            "width":  _COMMON["viewport_width"],
            "height": _COMMON["viewport_height"],
        },
        locale=_COMMON["locale"],
    )


async def _fetch_next_data(browser, url: str) -> dict | None:
    """Apre la pagina in un context fresco e restituisce window.__NEXT_DATA__."""
    ctx  = await _new_context(browser)
    page = await ctx.new_page()
    try:
        async def _do():
            await page.goto(url, wait_until="domcontentloaded",
                            timeout=_COMMON["nav_timeout_ms"])
            return await page.evaluate("() => window.__NEXT_DATA__ || null")

        return await retry(_do, retries=3, delay=2.0, label=url)
    except Exception as exc:
        log.error("Errore fetch %s: %s", url, exc)
        return None
    finally:
        await ctx.close()


# --------------------------------------------------------------------------- #
# Scraper con paginazione
# --------------------------------------------------------------------------- #

async def _scrape_query(browser, label: str, keyword: str) -> list[dict]:
    """Scarica e analizza tutte le pagine per una singola query."""
    log.info("Query: %s — %r", label, keyword)
    all_ads: list[dict] = []
    q_encoded = keyword.replace(" ", "+")

    for page_num in range(1, MAX_PAGES + 1):
        url = (
            f"{SEARCH_BASE}?q={q_encoded}"
            if page_num == 1
            else f"{SEARCH_BASE}?q={q_encoded}&o={page_num}"
        )
        log.info("  Pagina %d: %s", page_num, url)

        next_data = await _fetch_next_data(browser, url)
        if not next_data:
            log.warning("  __NEXT_DATA__ non disponibile — stop query.")
            break

        try:
            items = (
                next_data["props"]["pageProps"]["initialState"]["items"]["originalList"]
                or []
            )
        except (KeyError, TypeError):
            log.warning("  Struttura originalList non trovata — stop query.")
            break

        if not items:
            log.info("  → Pagina vuota, stop.")
            break

        parsed     = [ad for item in items if (ad := _parse_ad(item)) is not None]
        filtered_n = len(items) - len(parsed)
        log.info("  → %d/%d annunci validi (scartati %d)", len(parsed), len(items), filtered_n)
        all_ads.extend(parsed)

        # Se la pagina restituisce meno di 25 annunci è probabilmente l'ultima
        if len(items) < 25:
            log.info("  → Pagina parziale (%d items) — ultima pagina.", len(items))
            break

    return all_ads


async def run_scraper() -> list[dict]:
    """Scrape di tutte le query console Xbox su Subito.it."""
    all_ads: list[dict] = []

    async with async_playwright() as p:
        browser = await launch_chromium(
            p,
            headless=True,
            preferred_channel=_COMMON.get("playwright_channel", "chrome"),
        )

        for label, keyword in _QUERIES:
            try:
                ads = await _scrape_query(browser, label, keyword)
                all_ads.extend(ads)
            except Exception as exc:
                log.error("Errore query %r: %s", label, exc)

        await browser.close()

    unique = deduplicate(all_ads)
    log.info("Annunci unici dopo deduplica: %d (da %d totali)", len(unique), len(all_ads))
    return unique


# --------------------------------------------------------------------------- #
# Entry point
# --------------------------------------------------------------------------- #

def main() -> Path:
    log.info("=" * 60)
    log.info("Subito.it Scraper — Console Xbox")
    log.info("=" * 60)
    products = asyncio.run(run_scraper())
    log.info("Totale annunci unici: %d", len(products))
    return save_snapshot(SOURCE, products, BASE_URL, DATA_DIR)


if __name__ == "__main__":
    main()
