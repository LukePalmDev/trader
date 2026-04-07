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
import json as _json
import logging
import re
import unicodedata
from pathlib import Path
from typing import Sequence

import aiohttp
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

# Delay in secondi tra pagine consecutive (anti-ban adattivo).
# Con concurrency=4 e 0.3s → ~13 req/s globali, accettabile per Subito.
PAGE_DELAY_S: float = float(_COMMON.get("subito_page_delay_s", 0.3))

# Regex per estrarre __NEXT_DATA__ dall'HTML senza parsare l'intero DOM.
# Next.js garantisce che il JSON interno non contenga "</script>" non escaped.
_NEXT_DATA_RE = re.compile(
    r'<script[^>]+id=["\']__NEXT_DATA__["\'][^>]*>(.*?)</script>',
    re.DOTALL,
)

REGIONS: list[tuple[str, str]] = [
    # Ordinate per volume annunci atteso (pop. + attività marketplace).
    # Con concurrency=4 le prime 4 formano la wave iniziale più importante.
    ("Lombardia", "lombardia"),         # 1 — Milano, massimo volume
    ("Lazio", "lazio"),                 # 2 — Roma
    ("Campania", "campania"),           # 3 — Napoli
    ("Sicilia", "sicilia"),             # 4 — alta popolazione
    ("Veneto", "veneto"),               # 5 — Venezia/Padova/Verona
    ("Piemonte", "piemonte"),           # 6 — Torino
    ("Emilia-Romagna", "emilia-romagna"),  # 7 — Bologna
    ("Toscana", "toscana"),             # 8 — Firenze
    ("Puglia", "puglia"),               # 9 — Bari/Taranto
    ("Calabria", "calabria"),           # 10
    ("Sardegna", "sardegna"),           # 11
    ("Liguria", "liguria"),             # 12 — Genova
    ("Abruzzo", "abruzzo"),             # 13
    ("Marche", "marche"),               # 14
    ("Friuli-Venezia Giulia", "friuli-venezia-giulia"),  # 15
    ("Trentino-Alto Adige", "trentino-alto-adige"),      # 16
    ("Umbria", "umbria"),               # 17
    ("Basilicata", "basilicata"),       # 18
    ("Molise", "molise"),               # 19
    ("Valle d'Aosta", "valle-d-aosta"),  # 20 — minimo volume
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


_HTTP_HEADERS = {
    "User-Agent": _COMMON["user_agent"],
    "Accept-Language": f"{_COMMON['locale']},{_COMMON['locale'].split('-')[0]};q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


async def _fetch_next_data_http(session: aiohttp.ClientSession, url: str) -> dict | None:
    """Estrae __NEXT_DATA__ tramite aiohttp GET + regex (async nativo, no browser)."""
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            if resp.status != 200:
                return None
            text = await resp.text()
            m = _NEXT_DATA_RE.search(text)
            if m:
                return _json.loads(m.group(1))
    except Exception:  # noqa: BLE001
        pass
    return None


async def _fetch_pw_with_ctx(ctx, url: str) -> dict | None:
    """Estrae __NEXT_DATA__ tramite Playwright usando un context già esistente."""
    page = await ctx.new_page()
    try:
        async def _do():
            await page.goto(url, wait_until="domcontentloaded",
                            timeout=_COMMON["nav_timeout_ms"])
            return await page.evaluate("() => window.__NEXT_DATA__ || null")

        return await retry(_do, retries=3, delay=2.0, label=url)
    except Exception as exc:
        log.error("Errore fetch Playwright %s: %s", url, exc)
        return None
    finally:
        await page.close()


# --------------------------------------------------------------------------- #
# Scraper con paginazione
# --------------------------------------------------------------------------- #

def _region_search_base(region_slug: str) -> str:
    return f"{BASE_URL}/annunci-{region_slug}/vendita/usato/"


async def _scrape_region(
    session: aiohttp.ClientSession,
    browser,
    region_label: str,
    region_slug: str,
    keyword: str,
    *,
    strict_xbox: bool = True,
    max_pages: int = MAX_PAGES,
) -> list[dict]:
    """Scarica tutte le pagine della query keyword in una regione."""
    log.info("Regione: %s — query %r", region_label, keyword)
    all_ads: list[dict] = []
    q_encoded = keyword.replace(" ", "+")
    search_base = _region_search_base(region_slug)
    page_num = 1
    consecutive_errors = 0
    _pw_ctx = None  # Context Playwright creato lazy al primo fallback HTTP

    async def _fetch(url: str) -> dict | None:
        nonlocal _pw_ctx
        data = await _fetch_next_data_http(session, url)
        if data:
            return data
        # HTTP fallback: Playwright con context riusato per tutta la regione
        if _pw_ctx is None:
            log.info("  HTTP fallback Playwright — regione %s", region_label)
            _pw_ctx = await _new_context(browser)
        return await _fetch_pw_with_ctx(_pw_ctx, url)

    try:
        while page_num <= max_pages:
            url = (
                f"{search_base}?q={q_encoded}"
                if page_num == 1
                else f"{search_base}?q={q_encoded}&o={page_num}"
            )
            log.info("  Pagina %d: %s", page_num, url)

            next_data = await _fetch(url)
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
            if PAGE_DELAY_S > 0:
                await asyncio.sleep(PAGE_DELAY_S)

    finally:
        if _pw_ctx is not None:
            await _pw_ctx.close()

    return all_ads


async def run_scraper(
    *,
    regions: Sequence[str] | None = None,
    keyword: str = KEYWORD,
    max_pages: int = MAX_PAGES,
    strict_xbox: bool = True,
    dedup_results: bool = True,
    region_concurrency: int = 4,
) -> list[dict]:
    """Scrape Subito per keyword/regioni con dedup opzionale."""
    all_ads: list[dict] = []
    selected_regions = resolve_regions(regions)
    keyword = (keyword or KEYWORD).strip() or KEYWORD
    effective_max_pages = max(1, int(max_pages))
    effective_region_concurrency = max(1, int(region_concurrency))

    connector = aiohttp.TCPConnector(limit=20, ttl_dns_cache=300)
    async with aiohttp.ClientSession(
        headers=_HTTP_HEADERS,
        connector=connector,
    ) as session, async_playwright() as p:
        browser = await launch_chromium(
            p,
            headless=True,
            preferred_channel=_COMMON.get("playwright_channel", "chrome"),
        )
        try:
            if effective_region_concurrency <= 1:
                for region_label, region_slug in selected_regions:
                    try:
                        ads = await _scrape_region(
                            session,
                            browser,
                            region_label,
                            region_slug,
                            keyword,
                            strict_xbox=strict_xbox,
                            max_pages=effective_max_pages,
                        )
                        all_ads.extend(ads)
                    except Exception as exc:
                        log.error("Errore regione %r: %s", region_label, exc)
            else:
                sem = asyncio.Semaphore(effective_region_concurrency)

                async def _worker(region_label: str, region_slug: str) -> list[dict]:
                    async with sem:
                        return await _scrape_region(
                            session,
                            browser,
                            region_label,
                            region_slug,
                            keyword,
                            strict_xbox=strict_xbox,
                            max_pages=effective_max_pages,
                        )

                jobs = [_worker(label, slug) for label, slug in selected_regions]
                for result in await asyncio.gather(*jobs, return_exceptions=True):
                    if isinstance(result, Exception):
                        log.error("Errore regione in parallelo: %s", result)
                        continue
                    all_ads.extend(result)
        finally:
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
    region_concurrency: int = 4,
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
