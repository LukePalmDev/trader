"""
Scraper Gamelife.it — Console Xbox (usato + nuovo)
Categorie configurate in config.toml → [sources.gamelife].cat_urls
Salva i dati in: data/gamelife_YYYY-MM-DD_HH-MM-SS.json
"""

from __future__ import annotations

import asyncio
import logging
import re
from pathlib import Path

from playwright.async_api import async_playwright

from scrapers.base import clean_price, deduplicate, launch_chromium, retry, save_snapshot
from settings import load_config

log = logging.getLogger("gamelife")

# --- Carica configurazione ---
_CONFIG_PATH = Path(__file__).parent.parent / "config.toml"
_CFG = load_config(_CONFIG_PATH)

_COMMON  = _CFG["common"]
_SRC     = _CFG["sources"]["gamelife"]
_DATA    = _CFG["data"]

BASE_URL      = "https://www.gamelife.it"
CATEGORIES    = list(zip(_SRC["cat_labels"], _SRC["cat_urls"]))  # [(label, url), ...]
DATA_DIR      = Path(__file__).parent.parent / _DATA["output_dir"]
SOURCE        = "gamelife"
# URL da ignorare (normalizzati lowercase, senza slash finale)
_URL_BLACKLIST = {u.lower().rstrip("/") for u in _SRC.get("url_blacklist", [])}

# Prodotti NON Xbox da escludere (es. ASUS ROG Ally, Steam Deck, ecc.)
_NON_XBOX_RE = re.compile(
    r"\bROG\s+Ally\b|\bSteam\s+Deck\b|\bPlayStation\b|\bPS[345]\b|\bNintendo\b",
    re.IGNORECASE,
)

# Selettore container prodotti — atteso dopo ogni navigazione
_PRODUCTS_SELECTOR = "#products_grid .o_wsale_product_grid_wrapper"


# --------------------------------------------------------------------------- #
# Helpers locali
# --------------------------------------------------------------------------- #

# Alias locale per compatibilità interna
_clean_price = clean_price


# --------------------------------------------------------------------------- #
# Playwright scraper
# --------------------------------------------------------------------------- #

async def _navigate_and_wait(page, url: str) -> bool:
    """Naviga all'URL e attende che i prodotti siano presenti nel DOM.
    Restituisce True se i prodotti sono presenti, False se la pagina è vuota.

    Strategia: poll ogni 2s fino a 16s dopo il load event.
    Su Odoo eCommerce il load event può precedere il render dei prodotti
    (JS-side filtering). Il polling evita falsi negativi da race condition.
    """
    await page.goto(url, wait_until="domcontentloaded", timeout=_COMMON["nav_timeout_ms"])
    # Poll ogni 2 secondi, max 8 tentativi (16s totali)
    for _ in range(8):
        count = await page.locator(_PRODUCTS_SELECTOR).count()
        if count > 0:
            return True
        await page.wait_for_timeout(2000)
    log.warning("Nessun prodotto trovato su: %s (pagina vuota o inesistente)", url)
    return False


async def _scrape_page(page, url: str) -> list[dict]:
    """Naviga alla pagina e restituisce la lista di prodotti trovati.
    Usa retry automatico in caso di errori di rete. Pagine vuote vengono skippate.
    """
    log.info("Fetching: %s", url)

    async def _do():
        has_products = await _navigate_and_wait(page, url)
        if not has_products:
            return []
        return await _extract_products(page)

    return await retry(_do, retries=3, delay=2.0, label=url)


async def _extract_products(page) -> list[dict]:
    """Estrae tutti i prodotti dalla pagina corrente usando i Locator di Playwright."""
    wrappers = page.locator(".o_wsale_product_grid_wrapper")
    count = await wrappers.count()
    products = []

    for i in range(count):
        w = wrappers.nth(i)
        # Ogni card può restituire 0, 1 o 2 prodotti (Nuovo + Usato)
        prods = await _parse_product_locator(w)
        products.extend(prods)

    return products


async def _parse_product_locator(w) -> list[dict]:
    """Estrae le varianti Nuovo e Usato da una singola card prodotto.

    Ogni card GameLife contiene due righe di prezzo/disponibilità:
      - Riga Nuovo: span.available / span.unavailable con testo "Nuovo"
      - Riga Usato: span.available / span.unavailable con testo "Usato"
    Restituisce una lista di 0, 1 o 2 prodotti.
    """
    # Nome dal form aria-label
    form = w.locator("form").first
    name = (await form.get_attribute("aria-label") or "").strip()
    if not name:
        return []

    # Filtro prodotti non-Xbox (es. ROG Ally)
    if _NON_XBOX_RE.search(name):
        log.debug("Escluso prodotto non-Xbox: %r", name)
        return []

    # URL del prodotto
    h2 = w.locator("h2 a").first
    relative_url = (await h2.get_attribute("href") or "") if await h2.count() > 0 else ""
    url = BASE_URL + relative_url if relative_url.startswith("/") else relative_url

    # Blacklist URL
    if url.lower().rstrip("/") in _URL_BLACKLIST:
        log.debug("Escluso per blacklist URL: %r", url)
        return []

    # Immagine + SKU base dall'alt (es. "[HWXX0001_U] Xbox Series X (Usato)")
    img_loc = w.locator("img.oe_product_image_img").first
    img_alt = ""
    img_src = ""
    if await img_loc.count() > 0:
        img_alt = (await img_loc.get_attribute("alt") or "")
        img_src = (await img_loc.get_attribute("src") or "")
        if img_src.startswith("/"):
            img_src = BASE_URL + img_src

    sku_match = re.match(r"\[([^\]]+)\]", img_alt)
    if sku_match:
        # Rimuove il suffisso _U/_N per ottenere lo SKU base comune a entrambe le varianti
        base_sku = re.sub(r"_[UN]$", "", sku_match.group(1), flags=re.IGNORECASE)
    else:
        base_sku = url.rstrip("/").split("/")[-1] if url else ""
        if base_sku:
            log.warning("SKU non trovato in alt per %r — uso slug: %r", name, base_sku)
        else:
            log.warning("SKU e URL assenti per: %r", name)

    # Ribbon / badge
    ribbon_loc = w.locator(".o_ribbons").first
    ribbon = ""
    if await ribbon_loc.count() > 0:
        ribbon = (await ribbon_loc.text_content() or "").strip()

    # ── Estrae Nuovo e Usato via JavaScript ──────────────────────────────────
    # Ogni riga di prezzo ha: span.available|unavailable (con testo condizione)
    # + span.oe_currency_value (con il prezzo) all'interno dello stesso div.row
    variants: list[dict] = await w.evaluate("""wrapper => {
        const rows = wrapper.querySelectorAll(
            '.col-12.d-flex.flex-column.justify-content-center > .row'
        );
        const results = [];
        for (const row of rows) {
            const condSpan = row.querySelector('span.available, span.unavailable');
            if (!condSpan) continue;
            const isAvail = condSpan.classList.contains('available');
            const text = condSpan.textContent.trim();
            const label = text.includes('Nuovo') ? 'Nuovo'
                        : text.includes('Usato') ? 'Usato'
                        : null;
            if (!label) continue;
            const priceEl = row.querySelector('.oe_currency_value');
            const priceRaw = priceEl ? priceEl.textContent.trim() : '';
            results.push({ condition: label, available: isAvail, price_raw: priceRaw });
        }
        return results;
    }""")

    unavailable_ribbon = bool(
        ribbon and re.search(r"esaurit|non\s+disp", ribbon, re.IGNORECASE)
    )

    products = []
    for v in variants:
        condition  = v["condition"]
        price_raw  = v.get("price_raw", "")
        price      = _clean_price(price_raw)
        is_avail   = v["available"] and price is not None and not unavailable_ribbon
        sku        = base_sku + ("_U" if condition == "Usato" else "_N")

        products.append({
            "name":          name,
            "sku":           sku,
            "price":         price,
            "price_display": f"{price_raw} €" if price_raw else "N/D",
            "condition":     condition,
            "available":     is_avail,
            "url":           url,
            "image_url":     img_src,
            "ribbon":        ribbon,
        })
        log.debug("  %s — %s — €%s — avail=%s", name, condition, price, is_avail)

    return products


async def _get_page_urls(page) -> list[str]:
    """Estrae gli URL di tutte le pagine del pager."""
    pager = page.locator("#o_wsale_pager")
    if await pager.count() == 0:
        return []
    links = pager.locator("a[href]")
    n = await links.count()
    urls = []
    for i in range(n):
        href = (await links.nth(i).get_attribute("href") or "")
        full = BASE_URL + href if href.startswith("/") else href
        if full not in urls:
            urls.append(full)
    return urls


async def _new_context(browser):
    """Crea un browser context fresco con le impostazioni comuni."""
    return await browser.new_context(
        user_agent=_COMMON["user_agent"],
        viewport={
            "width":  _COMMON["viewport_width"],
            "height": _COMMON["viewport_height"],
        },
        locale=_COMMON["locale"],
    )


async def _scrape_category(browser, cat_label: str, cat_url: str) -> list[dict]:
    """Scrape completo di una categoria (tutte le pagine).

    Usa un context Playwright fresco per ogni URL per aggirare il blocco
    Cloudflare che si attiva sulle navigazioni successive nello stesso context.
    """
    log.info("Categoria: %s — %s", cat_label, cat_url)
    cat_products: list[dict] = []

    # Pagina 1 — apre context fresco, raccoglie prodotti + URL del pager
    ctx1 = await _new_context(browser)
    page1 = await ctx1.new_page()
    prods1 = await _scrape_page(page1, cat_url)
    cat_products.extend(prods1)
    log.info("  Pagina 1: → %d prodotti trovati", len(prods1))

    page_urls = await _get_page_urls(page1)
    await ctx1.close()

    visited = {cat_url}
    remaining = [u for u in page_urls if u not in visited]

    for idx, url in enumerate(remaining, start=2):
        ctx_n = await _new_context(browser)
        page_n = await ctx_n.new_page()
        prods = await _scrape_page(page_n, url)
        await ctx_n.close()
        cat_products.extend(prods)
        log.info("  Pagina %d: → %d prodotti trovati", idx, len(prods))
        visited.add(url)

    log.info("  Totale categoria %s: %d prodotti", cat_label, len(cat_products))
    return cat_products


async def run_scraper() -> list[dict]:
    """Scrape di tutte le categorie configurate — restituisce lista di prodotti."""
    all_products: list[dict] = []

    async with async_playwright() as p:
        browser = await launch_chromium(
            p,
            headless=True,
            preferred_channel=_COMMON.get("playwright_channel", "chrome"),
        )

        for cat_label, cat_url in CATEGORIES:
            prods = await _scrape_category(browser, cat_label, cat_url)
            all_products.extend(prods)

        await browser.close()

    # Aggiungi campo source + deduplicazione
    for prod in all_products:
        prod["source"] = SOURCE
    return deduplicate(all_products)


# --------------------------------------------------------------------------- #
# Save
# --------------------------------------------------------------------------- #

def save_data(products: list[dict]) -> Path:
    return save_snapshot(SOURCE, products, BASE_URL, DATA_DIR)


# --------------------------------------------------------------------------- #
# Entry point
# --------------------------------------------------------------------------- #

def main() -> Path:
    log.info("=" * 60)
    log.info("Gamelife Scraper — Console Xbox")
    log.info("Categorie: %s", ", ".join(lbl for lbl, _ in CATEGORIES))
    log.info("=" * 60)
    products = asyncio.run(run_scraper())
    log.info("Totale prodotti unici: %d", len(products))
    return save_data(products)


if __name__ == "__main__":
    main()
