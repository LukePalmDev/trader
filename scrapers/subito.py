"""
Scraper Subito.it — modalità full regional xbox.

Strategia:
  - query principale unica: "xbox"
  - split geografico su tutte le 20 regioni italiane
  - paginazione completa fino a pagina vuota (max 300)
  - deduplica finale per URN

Tecnica: Playwright + window.__NEXT_DATA__.
Campi estratti: titolo, descrizione (body), prezzo, geo, seller, pubblicazione.
"""

from __future__ import annotations

import asyncio
import logging
import re
import unicodedata
from pathlib import Path
from typing import Sequence

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
DATA_DIR    = Path(__file__).parent.parent / _DATA["output_dir"]
SOURCE      = "subito"
KEYWORD     = "xbox"

# Limite massimo pagine per regione/query (Subito supporta fino a 300 pagine)
MAX_PAGES = 300
PAGE_SIZE = 30
MAX_CONSECUTIVE_PAGE_ERRORS = 3

REGIONS: list[tuple[str, str]] = [
    ("Lombardia", "lombardia"),
    ("Campania", "campania"),
    ("Lazio", "lazio"),
    ("Veneto", "veneto"),
    ("Puglia", "puglia"),
    ("Sicilia", "sicilia"),
    ("Piemonte", "piemonte"),
    ("Emilia-Romagna", "emilia-romagna"),
    ("Toscana", "toscana"),
    ("Marche", "marche"),
    ("Sardegna", "sardegna"),
    ("Liguria", "liguria"),
    ("Friuli-Venezia Giulia", "friuli-venezia-giulia"),
    ("Calabria", "calabria"),
    ("Abruzzo", "abruzzo"),
    ("Trentino-Alto Adige", "trentino-alto-adige"),
    ("Umbria", "umbria"),
    ("Molise", "molise"),
    ("Basilicata", "basilicata"),
    ("Valle d'Aosta", "valle-d-aosta"),
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

_XBOX_TOKEN_RE = re.compile(r"\bx[\s\-]*box\b", re.IGNORECASE)
_SERIES_TOKEN_RE = re.compile(r"\b(serie|series)\s*[sx]\b", re.IGNORECASE)
_XBOX_CONTEXT_RE = re.compile(
    r"\b(console|microsoft|gamepass|controller|joystick|videogioc)\b",
    re.IGNORECASE,
)


def _normalize_token(text: str) -> str:
    """Normalizza una stringa per confronto tra label/slug regioni."""
    norm = unicodedata.normalize("NFKD", (text or "").strip().lower())
    norm = "".join(ch for ch in norm if not unicodedata.combining(ch))
    norm = norm.replace("'", " ").replace("_", " ")
    norm = re.sub(r"[^a-z0-9]+", "-", norm)
    return norm.strip("-")


def resolve_regions(region_tokens: Sequence[str] | None) -> list[tuple[str, str]]:
    """Risolvi token regione (slug o nome) in lista (label, slug)."""
    if not region_tokens:
        return REGIONS.copy()

    by_slug = {slug: (label, slug) for label, slug in REGIONS}
    by_label = {_normalize_token(label): (label, slug) for label, slug in REGIONS}
    selected: list[tuple[str, str]] = []
    seen: set[str] = set()

    for raw in region_tokens:
        token = (raw or "").strip()
        if not token:
            continue
        key_slug = _normalize_token(token)
        if key_slug in by_slug:
            region = by_slug[key_slug]
        elif key_slug in by_label:
            region = by_label[key_slug]
        else:
            valid = ", ".join(sorted(by_slug))
            raise ValueError(f"Regione non riconosciuta: {token!r}. Slug validi: {valid}")

        if region[1] not in seen:
            selected.append(region)
            seen.add(region[1])

    if not selected:
        raise ValueError("Nessuna regione valida selezionata.")
    return selected


def _is_xbox_relevant(subject: str, body_text: str) -> bool:
    """Filtro locale anti-rumore (evita 'box' non Xbox)."""
    text = f"{subject}\n{body_text}".strip()
    if not text:
        return False
    if _XBOX_TOKEN_RE.search(text):
        return True
    # fallback per alcuni annunci "Series X/S" senza token xbox esplicito
    return bool(_SERIES_TOKEN_RE.search(text) and _XBOX_CONTEXT_RE.search(text))


# --------------------------------------------------------------------------- #
# Parsing di un singolo annuncio
# --------------------------------------------------------------------------- #

def _parse_ad(item: dict, strict_xbox: bool = True) -> dict | None:
    """Converte un annuncio Subito nel formato standard."""
    subject = (item.get("subject") or "").strip()
    if not subject:
        return None

    body_text = (item.get("body") or "").strip()
    if strict_xbox and not _is_xbox_relevant(subject, body_text):
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
        "body_text":     body_text,
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

def _region_search_base(region_slug: str) -> str:
    return f"{BASE_URL}/annunci-{region_slug}/vendita/usato/"


async def _scrape_region(
    browser,
    region_label: str,
    region_slug: str,
    keyword: str,
    *,
    strict_xbox: bool = True,
) -> list[dict]:
    """Scarica tutte le pagine della query keyword in una regione."""
    log.info("Regione: %s — query %r", region_label, keyword)
    all_ads: list[dict] = []
    q_encoded = keyword.replace(" ", "+")
    search_base = _region_search_base(region_slug)
    page_num = 1
    consecutive_errors = 0

    while page_num <= MAX_PAGES:
        url = (
            f"{search_base}?q={q_encoded}"
            if page_num == 1
            else f"{search_base}?q={q_encoded}&o={page_num}"
        )
        log.info("  Pagina %d: %s", page_num, url)

        next_data = await _fetch_next_data(browser, url)
        if not next_data:
            consecutive_errors += 1
            if consecutive_errors >= MAX_CONSECUTIVE_PAGE_ERRORS:
                log.warning(
                    "  __NEXT_DATA__ non disponibile per %d volte consecutive — stop query.",
                    consecutive_errors,
                )
                break
            wait_s = min(8.0, 2.0 * consecutive_errors)
            log.warning(
                "  __NEXT_DATA__ non disponibile (tentativo locale %d/%d). Retry pagina tra %.1fs…",
                consecutive_errors,
                MAX_CONSECUTIVE_PAGE_ERRORS,
                wait_s,
            )
            await asyncio.sleep(wait_s)
            continue

        try:
            items = (
                next_data["props"]["pageProps"]["initialState"]["items"]["originalList"]
                or []
            )
        except (KeyError, TypeError):
            consecutive_errors += 1
            if consecutive_errors >= MAX_CONSECUTIVE_PAGE_ERRORS:
                log.warning(
                    "  Struttura originalList non trovata per %d volte consecutive — stop query.",
                    consecutive_errors,
                )
                break
            wait_s = min(8.0, 2.0 * consecutive_errors)
            log.warning(
                "  Struttura originalList non trovata (tentativo locale %d/%d). Retry pagina tra %.1fs…",
                consecutive_errors,
                MAX_CONSECUTIVE_PAGE_ERRORS,
                wait_s,
            )
            await asyncio.sleep(wait_s)
            continue

        consecutive_errors = 0

        if not items:
            log.info("  → Pagina vuota, stop.")
            break

        parsed_this_page = []
        for item in items:
            ad = _parse_ad(item, strict_xbox=strict_xbox)
            if not ad:
                continue
            parsed_this_page.append(ad)

        filtered_n = len(items) - len(parsed_this_page)
        log.info(
            "  → %d/%d annunci estratti (scartati vuoti %d).",
            len(parsed_this_page),
            len(items),
            filtered_n,
        )
        all_ads.extend(parsed_this_page)

        # Subito espone max 30 risultati/pagina: pagina parziale => ultima.
        if len(items) < PAGE_SIZE:
            log.info("  → Pagina parziale (%d items) — ultima pagina.", len(items))
            break
        page_num += 1

    return all_ads


async def run_scraper(
    *,
    regions: Sequence[str] | None = None,
    keyword: str = KEYWORD,
    max_pages: int = MAX_PAGES,
    strict_xbox: bool = True,
    dedup_results: bool = True,
    region_concurrency: int = 1,
) -> list[dict]:
    """Scrape Subito per keyword/regioni con dedup opzionale."""
    all_ads: list[dict] = []
    selected_regions = resolve_regions(regions)
    keyword = (keyword or KEYWORD).strip() or KEYWORD
    effective_max_pages = max(1, int(max_pages))
    effective_region_concurrency = max(1, int(region_concurrency))

    async with async_playwright() as p:
        browser = await launch_chromium(
            p,
            headless=True,
            preferred_channel=_COMMON.get("playwright_channel", "chrome"),
        )
        old_max_pages = None

        # MAX_PAGES è un valore modulo-level usato da _scrape_region.
        # Lo forziamo runtime per evitare una refactor più invasiva.
        global MAX_PAGES
        old_max_pages = MAX_PAGES
        MAX_PAGES = effective_max_pages
        try:
            if effective_region_concurrency <= 1:
                for region_label, region_slug in selected_regions:
                    try:
                        ads = await _scrape_region(
                            browser,
                            region_label,
                            region_slug,
                            keyword,
                            strict_xbox=strict_xbox,
                        )
                        all_ads.extend(ads)
                    except Exception as exc:
                        log.error("Errore regione %r: %s", region_label, exc)
            else:
                sem = asyncio.Semaphore(effective_region_concurrency)

                async def _worker(region_label: str, region_slug: str) -> list[dict]:
                    async with sem:
                        return await _scrape_region(
                            browser,
                            region_label,
                            region_slug,
                            keyword,
                            strict_xbox=strict_xbox,
                        )

                jobs = [_worker(label, slug) for label, slug in selected_regions]
                for result in await asyncio.gather(*jobs, return_exceptions=True):
                    if isinstance(result, Exception):
                        log.error("Errore regione in parallelo: %s", result)
                        continue
                    all_ads.extend(result)
        finally:
            if old_max_pages is not None:
                MAX_PAGES = old_max_pages

        await browser.close()

    if dedup_results:
        unique = deduplicate(all_ads)
        log.info("Annunci unici dopo deduplica: %d (da %d totali)", len(unique), len(all_ads))
        return unique

    log.info("Deduplica disabilitata: %d annunci grezzi.", len(all_ads))
    return all_ads


# --------------------------------------------------------------------------- #
# Entry point
# --------------------------------------------------------------------------- #

def main(
    *,
    regions: Sequence[str] | None = None,
    keyword: str = KEYWORD,
    max_pages: int = MAX_PAGES,
    strict_xbox: bool = True,
    dedup_results: bool = True,
    region_concurrency: int = 1,
) -> Path:
    log.info("=" * 60)
    log.info("Subito.it Scraper — Console Xbox")
    log.info("=" * 60)
    products = asyncio.run(
        run_scraper(
            regions=regions,
            keyword=keyword,
            max_pages=max_pages,
            strict_xbox=strict_xbox,
            dedup_results=dedup_results,
            region_concurrency=region_concurrency,
        )
    )
    log.info("Totale annunci unici: %d", len(products))
    return save_snapshot(SOURCE, products, BASE_URL, DATA_DIR)


if __name__ == "__main__":
    main()
