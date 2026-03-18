"""
Scraper eBay.it — Console Xbox (lotti VENDUTI)

Tecnica: Playwright + channel="chrome" + parsing DOM
  eBay usa Akamai → Chrome di sistema per evitare blocchi.
  Estrae lotti completati (LH_Sold=1&LH_Complete=1):
  prezzi di mercato realizzati, utili come riferimento.

Ottimizzazioni attive:
  A) Query parallele   — max PARALLEL_QUERIES contesti simultanei (semaforo)
                         + stagger QUERY_STAGGER_S secondi tra i lanci
  B) Context riutilizzato — un solo context+page per tutta la query (no cold-start)
  C) domcontentloaded  — eBay è SSR: carica i .s-card nell'HTML iniziale;
                         fallback automatico a "load" se timeout

URL di ricerca:
  /sch/i.html?_nkw=<keyword>&LH_Sold=1&LH_Complete=1&_ipg=60&_pgn=<page>

Query tracciate:
  - "xbox series x console"
  - "xbox series s console"
  - "xbox one console"
  - "xbox 360 console"
  - "xbox original console"

Salva in: data/ebay_YYYY-MM-DD_HH-MM-SS.json
"""

from __future__ import annotations

import asyncio
import logging
import re
from pathlib import Path

from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright

from id_utils import stable_item_id
from scrapers.base import deduplicate, launch_chromium, retry, save_snapshot
from settings import load_config

log = logging.getLogger("ebay")

_CONFIG_PATH = Path(__file__).parent.parent / "config.toml"
_CFG = load_config(_CONFIG_PATH)

_COMMON = _CFG["common"]
_DATA   = _CFG["data"]

BASE_URL    = "https://www.ebay.it"
SEARCH_BASE = f"{BASE_URL}/sch/i.html"
DATA_DIR    = Path(__file__).parent.parent / _DATA["output_dir"]
SOURCE      = "ebay"

MAX_PAGES       = 10    # 10 pagine × 60 item = max 600 lotti per query
PARALLEL_QUERIES = 3    # max contesti simultanei (Fase A)
QUERY_STAGGER_S  = 3.0  # secondi di stagger tra lanci (Fase A)

_QUERIES = [
    ("Xbox Series X", "xbox series x console"),
    ("Xbox Series S", "xbox series s console"),
    ("Xbox One",      "xbox one console"),
    ("Xbox 360",      "xbox 360 console"),
    ("Xbox Original", "xbox original console"),
]

# Risorse da bloccare per velocizzare il caricamento pagina
_BLOCK_RESOURCES = re.compile(
    r"\.(png|jpg|jpeg|gif|webp|svg|ico|woff2?|ttf|eot|otf)(\?|$)",
    re.IGNORECASE,
)

# Filtri specifici per eBay (diversi da Subito: titoli spesso in inglese/misti)
_EBAY_BLOCKLIST = re.compile(
    r"""
    \b(
      # Periferiche / accessori
      controller | gamepad | joypad | joystick | \bpad\b | headset
    | \bcavo\b   | \bcavi\b | alimentator[ei] | caricatore | \bhdmi\b
    | hard\s*disk | \bhdd\b | \bssd\b
      # Software / abbonamenti
    | game\s*pass | xbox\s*live | \bgold\b | abbonamento | \bdlc\b
    | \bcodice\b  | voucher | gift\s*card
      # Solo giochi senza "console"
    | \bfifa\d*  | \bnba\b  | \bcall\s+of\b | minecraft | battlefield
    | \bassassin | \bgta\b  | \bwwe\b        | \bufc\b   | \bmortal\b
    )\b
    """,
    re.IGNORECASE | re.VERBOSE,
)

_EBAY_IS_XBOX = re.compile(
    r"""
    \b(
      xbox\s+series\s+[xs]\b
    | xbox\s+one\s+[xs]\b
    | xbox\s+one\b
    | xbox\s+360\b
    | xbox\s+original\b
    | microsoft\s+xbox\b
    | console\b
    )\b
    """,
    re.IGNORECASE | re.VERBOSE,
)

# Prezzo minimo per considerare un lotto "console" (non accessorio)
_PRICE_FLOOR = 8.0


# --------------------------------------------------------------------------- #
# Parsing prezzo eBay.it (formato IT: "EUR 350,00" o "EUR 1.350,00")
# --------------------------------------------------------------------------- #

def _parse_ebay_price(text: str) -> float | None:
    if not text:
        return None
    text = text.strip().replace("EUR", "").replace("\xa0", " ").strip()
    m = re.search(r"([\d.,]+)", text)
    if not m:
        return None
    s = m.group(1)
    if re.match(r"^\d{1,3}(\.\d{3})*(,\d{1,2})?$", s):
        s = s.replace(".", "").replace(",", ".")
    elif re.match(r"^\d+(,\d{1,2})?$", s):
        s = s.replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None


# --------------------------------------------------------------------------- #
# Parsing di un singolo item eBay
# --------------------------------------------------------------------------- #

def _parse_item(raw: dict, query_label: str) -> dict | None:
    title = (raw.get("title") or "").strip()
    if not title or title.lower() == "shop on ebay":
        return None

    if _EBAY_BLOCKLIST.search(title):
        log.debug("Blocklist eBay: %r", title)
        return None
    if not _EBAY_IS_XBOX.search(title):
        log.debug("Non Xbox: %r", title)
        return None

    price = _parse_ebay_price(raw.get("price_text") or "")
    if price is None or price < _PRICE_FLOOR:
        return None

    url       = raw.get("url") or ""
    sold_date = (raw.get("sold_date") or "").strip()

    m = re.search(r"/itm/(\d+)", url)
    item_id = f"EBAY-{m.group(1)}" if m else stable_item_id("EBAY", url)

    return {
        "name":        title,
        "sku":         item_id,
        "price":       price,
        "sold_date":   sold_date,
        "url":         url,
        "query_label": query_label,
        "source":      SOURCE,
        "available":   True,
    }


# --------------------------------------------------------------------------- #
# Playwright helpers
# --------------------------------------------------------------------------- #

async def _new_context(browser):
    return await browser.new_context(
        user_agent=_COMMON["user_agent"],
        viewport={
            "width":  _COMMON["viewport_width"],
            "height": _COMMON["viewport_height"],
        },
        locale=_COMMON["locale"],
    )


_EXTRACT_JS = """() => {
    const items = document.querySelectorAll('li.s-card, li[class*="s-card"]');
    return [...items].map(el => ({
        title: (
            el.querySelector('img.s-card__image')?.getAttribute('alt') ||
            el.querySelector('img[alt]')?.getAttribute('alt') ||
            el.querySelector('[class*="title"]')?.textContent ||
            ''
        ).trim(),
        price_text: (
            el.querySelector('[class*="price"]')?.textContent ||
            ''
        ).trim(),
        sold_date: (
            el.querySelector('[class*="sold"]')?.textContent ||
            el.querySelector('[class*="date"]')?.textContent ||
            ''
        ).trim(),
        url: (
            el.querySelector('a[href*="/itm/"]')?.href ||
            ''
        ),
    }));
}"""


async def _fetch_page_items(page, url: str) -> list[dict]:
    """
    Fase C: prova domcontentloaded (SSR, più veloce).
    Se li.s-card non appare entro 3s → fallback a load completo.
    Riusa il page object esistente (Fase B).
    """
    async def _do():
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=20_000)
            await page.wait_for_selector("li.s-card", timeout=3_000)
        except PlaywrightTimeoutError:
            log.debug("domcontentloaded timeout → fallback load: %s", url)
            await page.goto(url, wait_until="load",
                            timeout=_COMMON["nav_timeout_ms"])
            await page.wait_for_selector("li.s-card", timeout=15_000)

        return await page.evaluate(_EXTRACT_JS)

    try:
        return await retry(_do, retries=3, delay=2.0, label=url)
    except Exception as exc:
        log.error("Errore fetch %s: %s", url, exc)
        return []


# --------------------------------------------------------------------------- #
# Scraper con paginazione — Fase B: context riutilizzato per tutta la query
# --------------------------------------------------------------------------- #

async def _scrape_query(browser, label: str, keyword: str) -> list[dict]:
    log.info("eBay query: %s — %r", label, keyword)
    all_items: list[dict] = []
    q_encoded = keyword.replace(" ", "+")

    # Fase B: un solo context+page per tutte le pagine della query
    ctx  = await _new_context(browser)
    page = await ctx.new_page()

    # Blocca immagini/font per ridurre tempi di caricamento
    async def _block(route):
        if _BLOCK_RESOURCES.search(route.request.url):
            await route.abort()
        else:
            await route.continue_()

    await page.route("**/*", _block)

    try:
        for page_num in range(1, MAX_PAGES + 1):
            url = (
                f"{SEARCH_BASE}?_nkw={q_encoded}&LH_Sold=1&LH_Complete=1&_ipg=60"
                if page_num == 1
                else f"{SEARCH_BASE}?_nkw={q_encoded}&LH_Sold=1&LH_Complete=1&_ipg=60&_pgn={page_num}"
            )
            log.info("  [%s] Pagina %d: %s", label, page_num, url)

            raws = await _fetch_page_items(page, url)
            if not raws:
                log.info("  [%s] → Nessun item — stop.", label)
                break

            parsed     = [r for raw in raws if (r := _parse_item(raw, label)) is not None]
            filtered_n = len(raws) - len(parsed)
            log.info("  [%s] → %d/%d item validi (scartati %d)",
                     label, len(parsed), len(raws), filtered_n)
            all_items.extend(parsed)

            if len(raws) < 50:
                log.info("  [%s] → Pagina parziale (%d items) — ultima.", label, len(raws))
                break
    finally:
        await ctx.close()

    log.info("  [%s] Totale: %d item", label, len(all_items))
    return all_items


# --------------------------------------------------------------------------- #
# Run — Fase A: query parallele con semaforo e stagger
# --------------------------------------------------------------------------- #

async def run_scraper() -> list[dict]:
    all_items: list[dict] = []
    sem = asyncio.Semaphore(PARALLEL_QUERIES)

    async def _run_bounded(idx: int, label: str, keyword: str) -> list[dict]:
        # Stagger: ogni query aspetta idx * QUERY_STAGGER_S prima di partire
        await asyncio.sleep(idx * QUERY_STAGGER_S)
        async with sem:
            try:
                return await _scrape_query(browser, label, keyword)
            except Exception as exc:
                log.error("Errore query %r: %s", label, exc)
                return []

    async with async_playwright() as p:
        browser = await launch_chromium(
            p,
            headless=True,
            preferred_channel=_COMMON.get("playwright_channel", "chrome"),
        )

        tasks = [
            _run_bounded(i, label, keyword)
            for i, (label, keyword) in enumerate(_QUERIES)
        ]
        results = await asyncio.gather(*tasks)

        for result in results:
            all_items.extend(result)

        await browser.close()

    unique = deduplicate(all_items)
    log.info("eBay item unici dopo deduplica: %d (da %d totali)",
             len(unique), len(all_items))
    return unique


# --------------------------------------------------------------------------- #
# Entry point
# --------------------------------------------------------------------------- #

def main() -> Path:
    log.info("=" * 60)
    log.info("eBay.it Scraper — Console Xbox (Venduto)")
    log.info("=" * 60)
    items = asyncio.run(run_scraper())
    log.info("Totale item unici: %d", len(items))
    return save_snapshot(SOURCE, items, BASE_URL, DATA_DIR)


if __name__ == "__main__":
    main()
