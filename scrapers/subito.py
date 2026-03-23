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
from db_subito import DB_PATH, _connect

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
# 300 pagine massime supportate da Subito (≈9.000 annunci/query)
MAX_PAGES = 300

# Query per scraping massivo (senza "console" per prendere anche i giochi/bundle)
_QUERIES = [
    # ── Modelli principali ──────────────────────────────────────────────────
    ("Xbox",             "xbox"),
    ("Xbox Series X",    "xbox series x"),
    ("Xbox Series S",    "xbox series s"),
    ("Xbox One",         "xbox one"),
    ("Xbox One X",       "xbox one x"),
    ("Xbox One S",       "xbox one s"),
    ("Xbox 360",         "xbox 360"),
    ("Xbox 360 E",       "xbox 360 e"),
    ("Xbox 360 Slim",    "xbox 360 slim"),
    ("Xbox Original",    "xbox original"),
    ("Xbox Classic",     "xbox classic"),
    ("Xbox Crystal",     "xbox crystal"),
    # ── Abbreviazioni comuni ───────────────────────────────────────────────
    ("Xbox Serie",       "xbox serie"),       # singolare — errore frequente in italiano
    ("Xbox Serie X",     "xbox serie x"),
    ("Xbox Serie S",     "xbox serie s"),
    ("Xbox 1",           "xbox 1"),           # "uno" scritto come numero
    # ── Storpiature e typo ────────────────────────────────────────────────
    ("X Box",            "x box"),            # spazio nel nome
    ("X-Box",            "x-box"),            # trattino
    ("Xboks",            "xboks"),            # k finale
    ("Xbox Ome",         "xbox ome"),         # n→m (one)
    ("Xbox On",          "xbox on"),          # "one" troncato
    ("Xbox Seres",       "xbox seres"),       # typo di "series"
    ("Xbox Sereis",      "xbox sereis"),      # inversione lettere
]


# --------------------------------------------------------------------------- #
# Filtri titolo annuncio
# --------------------------------------------------------------------------- #

# I filtri hardware precedentemente qui presenti (_BLOCKLIST e _IS_CONSOLE)
# sono stati rimossi. Ora classifichiamo tutto tramite intelligenza artificiale.

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

    # Niente filtri Regex hardware: prendiamo tutto per farglielo classificare dall'AI

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

async def _scrape_query(browser, label: str, keyword: str, known_urns: set[str]) -> list[dict]:
    """Scarica e analizza tutte le pagine per una singola query, con stop cronologico."""
    log.info("Query: %s — %r", label, keyword)
    all_ads: list[dict] = []
    q_encoded = keyword.replace(" ", "+")
    
    consecutive_known = 0
    MAX_CONSECUTIVE = 15 # Supera abbondantemente gli annunci "Vetrina/In Cima" in una singola pagina

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

        parsed_this_page = []
        for item in items:
            ad = _parse_ad(item)
            if not ad:
                continue
            parsed_this_page.append(ad)
            
            # Check for chronological stop
            if ad["sku"] in known_urns:
                consecutive_known += 1
            else:
                consecutive_known = 0

        filtered_n = len(items) - len(parsed_this_page)
        log.info("  → %d/%d annunci estratti (scartati vuoti %d). Known consecutivi: %d", 
                 len(parsed_this_page), len(items), filtered_n, consecutive_known)
        all_ads.extend(parsed_this_page)

        if consecutive_known >= MAX_CONSECUTIVE:
            log.info("  → Raggiunto limite STOP cronologico (%d annunci noti consecutivi). Query completata.", MAX_CONSECUTIVE)
            break

        # Se la pagina restituisce meno di 25 annunci è probabilmente l'ultima
        if len(items) < 25:
            log.info("  → Pagina parziale (%d items) — ultima pagina.", len(items))
            break

    return all_ads


async def run_scraper() -> list[dict]:
    """Scrape massivo di tutte le query console Xbox su Subito.it."""
    all_ads: list[dict] = []

    # Recupero URN già noti dal database per lo stop cronologico
    known_urns = set()
    try:
        conn = _connect(DB_PATH)
        rows = conn.execute("SELECT urn_id FROM ads").fetchall()
        known_urns = {row[0] for row in rows}
        log.info("Trovati %d annunci gia noti nel DB per filtro cronologico.", len(known_urns))
    except Exception as exc:
        log.warning("Impossibile caricare known_urns: %s", exc)

    async with async_playwright() as p:
        browser = await launch_chromium(
            p,
            headless=True,
            preferred_channel=_COMMON.get("playwright_channel", "chrome"),
        )

        for label, keyword in _QUERIES:
            try:
                ads = await _scrape_query(browser, label, keyword, known_urns)
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
