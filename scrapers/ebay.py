"""
Scraper eBay.it — Console Xbox (lotti VENDUTI)

Tecnica: Playwright + channel="chrome" + parsing DOM
  eBay usa Akamai → Chrome di sistema per evitare blocchi.
  Estrae lotti completati (LH_Sold=1&LH_Complete=1):
  prezzi di mercato realizzati, utili come riferimento.

URL di ricerca:
  /sch/i.html?_nkw=<keyword>&LH_Sold=1&LH_Complete=1&_ipg=60&_pgn=<page>

Query tracciate:
  - "xbox series x console"
  - "xbox series s console"
  - "xbox one console"
  - "xbox 360 console"
  - "xbox original console"

Filtri titolo: stessi blocklist/allowlist di Subito (riutilizzati).

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

MAX_PAGES = 10   # 10 pagine × 60 item = max 600 lotti per query

_QUERIES = [
    ("Xbox Series X", "xbox series x console"),
    ("Xbox Series S", "xbox series s console"),
    ("Xbox One",      "xbox one console"),
    ("Xbox 360",      "xbox 360 console"),
    ("Xbox Original", "xbox original console"),
]

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
    # Rimuovi "EUR", spazi, gestisci separatori IT (punto=migliaia, virgola=decimali)
    text = text.strip().replace("EUR", "").replace("\xa0", " ").strip()
    # Prendo solo il primo prezzo (ignoro range "250,00 a 400,00")
    m = re.search(r"([\d.,]+)", text)
    if not m:
        return None
    s = m.group(1)
    # Formato italiano: "1.350,00" → "1350.00" oppure "350,00" → "350.00"
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

    # Filtri eBay-specifici
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

    # Item ID dall'URL: /itm/123456789 → EBAY-123456789
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


async def _fetch_page_items(browser, url: str) -> list[dict]:
    """Naviga a una pagina eBay e restituisce i raw item via DOM."""
    ctx  = await _new_context(browser)
    page = await ctx.new_page()
    try:
        async def _do():
            await page.goto(url, wait_until="load",
                            timeout=_COMMON["nav_timeout_ms"])
            # Aspetta che gli item siano presenti nel DOM (JS-rendered)
            try:
                await page.wait_for_selector(".s-item", timeout=15000)
            except PlaywrightTimeoutError:
                log.debug("Timeout attesa selector .s-item su %s", url)

            # eBay usa ora li.s-card con titolo nell'alt dell'immagine
            return await page.evaluate("""() => {
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
            }""")
        return await retry(_do, retries=3, delay=2.0, label=url)
    except Exception as exc:
        log.error("Errore fetch %s: %s", url, exc)
        return []
    finally:
        await ctx.close()


# --------------------------------------------------------------------------- #
# Scraper con paginazione
# --------------------------------------------------------------------------- #

async def _scrape_query(browser, label: str, keyword: str) -> list[dict]:
    log.info("eBay query: %s — %r", label, keyword)
    all_items: list[dict] = []
    q_encoded = keyword.replace(" ", "+")

    for page_num in range(1, MAX_PAGES + 1):
        url = (
            f"{SEARCH_BASE}?_nkw={q_encoded}&LH_Sold=1&LH_Complete=1&_ipg=60"
            if page_num == 1
            else f"{SEARCH_BASE}?_nkw={q_encoded}&LH_Sold=1&LH_Complete=1&_ipg=60&_pgn={page_num}"
        )
        log.info("  Pagina %d: %s", page_num, url)

        raws = await _fetch_page_items(browser, url)
        if not raws:
            log.info("  → Nessun item — stop.")
            break

        parsed     = [r for raw in raws if (r := _parse_item(raw, label)) is not None]
        filtered_n = len(raws) - len(parsed)
        log.info("  → %d/%d item validi (scartati %d)", len(parsed), len(raws), filtered_n)
        all_items.extend(parsed)

        if len(raws) < 50:
            log.info("  → Pagina parziale (%d items) — ultima.", len(raws))
            break

    return all_items


async def run_scraper() -> list[dict]:
    all_items: list[dict] = []

    async with async_playwright() as p:
        browser = await launch_chromium(
            p,
            headless=True,
            preferred_channel=_COMMON.get("playwright_channel", "chrome"),
        )

        for label, keyword in _QUERIES:
            try:
                items = await _scrape_query(browser, label, keyword)
                all_items.extend(items)
            except Exception as exc:
                log.error("Errore query %r: %s", label, exc)

        await browser.close()

    unique = deduplicate(all_items)
    log.info("eBay item unici dopo deduplica: %d (da %d totali)", len(unique), len(all_items))
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
