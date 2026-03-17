"""
Scraper GamePeople (gamepeople.it) — Console Xbox (solo Nuovo)
Usa Playwright per aggirare il WAF SafeLine (status 468).
Salva in: data/gamepeople_YYYY-MM-DD_HH-MM-SS.json

Note:
- GamePeople vende solo prodotti NUOVI.
- Disponibilità: img src contiene 'disponibile.gif' (sì) o 'non_disponibile.gif' (no).
- Testo disponibilità: span.float_right span.bold → "Disponibile" / "Ordinabile" / "Esaurito"
"""

from __future__ import annotations

import asyncio
import logging
import re
import sys
from pathlib import Path

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib

from playwright.async_api import async_playwright

from scrapers.base import clean_price, retry, save_snapshot, deduplicate

log = logging.getLogger("gamepeople")

# --- Carica configurazione ---
_CONFIG_PATH = Path(__file__).parent.parent / "config.toml"
with open(_CONFIG_PATH, "rb") as _f:
    _CFG = tomllib.load(_f)

_COMMON = _CFG["common"]
_SRC    = _CFG["sources"]["gamepeople"]
_DATA   = _CFG["data"]

BASE_URL   = "https://www.gamepeople.it"
CATEGORIES = list(zip(_SRC["cat_labels"], _SRC["cat_urls"]))
DATA_DIR   = Path(__file__).parent.parent / _DATA["output_dir"]
SOURCE     = "gamepeople"

# Selettore atteso dopo navigazione
_PRODUCT_SELECTOR = ".lista_prodotto"


# --------------------------------------------------------------------------- #
# Playwright helpers
# --------------------------------------------------------------------------- #

async def _new_context(browser):
    return await browser.new_context(
        user_agent=_COMMON["user_agent"],
        viewport={"width": _COMMON["viewport_width"], "height": _COMMON["viewport_height"]},
        locale=_COMMON["locale"],
    )


async def _navigate_and_wait(page, url: str) -> bool:
    """Naviga e attende i prodotti nel DOM (poll ogni 2s, max 16s)."""
    await page.goto(url, wait_until="domcontentloaded", timeout=_COMMON["nav_timeout_ms"])
    for _ in range(8):
        count = await page.locator(_PRODUCT_SELECTOR).count()
        if count > 0:
            return True
        await page.wait_for_timeout(2000)
    log.warning("Nessun prodotto trovato su: %s", url)
    return False


# --------------------------------------------------------------------------- #
# Parsing
# --------------------------------------------------------------------------- #

async def _extract_products(page) -> list[dict]:
    """Estrae tutti i prodotti dalla pagina corrente via JavaScript."""
    products: list[dict] = await page.evaluate("""() => {
        const cards = document.querySelectorAll('.lista_prodotto');
        const results = [];
        for (const card of cards) {
            // Nome + URL
            const nameEl = card.querySelector('.lista_prodotto_titolo a');
            if (!nameEl) continue;
            const name = nameEl.textContent.trim();
            if (!name) continue;
            const url = nameEl.href || '';

            // SKU
            const fullText = card.textContent;
            const skuMatch = fullText.match(/Cod\\.\\s*prodotto:\\s*([A-Z0-9]+)/);
            const sku = skuMatch ? skuMatch[1] : '';

            // Prezzo
            const prezzoEl = card.querySelector('.prezzo_finale');
            const priceRaw = prezzoEl ? prezzoEl.textContent.trim().replace('€', '').trim() : '';

            // Disponibilità — testo
            const dispTextEl = card.querySelector('span.float_right span.bold');
            const dispText = dispTextEl ? dispTextEl.textContent.trim() : '';

            // Disponibilità — immagine (disponibile.gif vs non_disponibile.gif)
            const dispImg = card.querySelector('span.float_right img');
            const dispSrc = dispImg ? (dispImg.getAttribute('src') || '') : '';
            // "disponibile.gif" senza "non_" = in stock
            const available = dispSrc.includes('disponibile.gif') && !dispSrc.includes('non_disponibile.gif');

            // Immagine prodotto
            const imgEl = card.querySelector('td img');
            const imgSrc = imgEl ? (imgEl.getAttribute('src') || '') : '';
            const imgUrl = imgSrc.startsWith('http') ? imgSrc
                         : imgSrc.startsWith('/')    ? 'https://www.gamepeople.it' + imgSrc
                         : imgSrc;

            results.push({ name, url, sku, priceRaw, dispText, available, imgUrl });
        }
        return results;
    }""")

    out = []
    for p in products:
        price = clean_price(p["priceRaw"])
        price_display = f"{p['priceRaw']} €" if p["priceRaw"] else "N/D"
        # Mappa testo disponibilità → etichetta normalizzata
        dt = p["dispText"].lower()
        if "disponib" in dt and "non" not in dt:
            avail_label = "Disponibile"
        elif "ordinab" in dt or "prenotab" in dt:
            avail_label = "Ordinabile"
        elif "esaur" in dt:
            avail_label = "Esaurito"
        else:
            avail_label = p["dispText"] or "N/D"

        out.append({
            "name":          p["name"],
            "sku":           p["sku"],
            "price":         price,
            "price_display": price_display,
            "condition":     "Nuovo",
            "availability":  avail_label,
            "available":     p["available"] and price is not None,
            "url":           p["url"],
            "image_url":     p["imgUrl"],
            "source":        SOURCE,
        })
        log.debug("  %s — %s — €%s — avail=%s", p["name"], avail_label, price, p["available"])

    return out


# --------------------------------------------------------------------------- #
# Per-categoria
# --------------------------------------------------------------------------- #

async def _scrape_category(browser, cat_label: str, cat_url: str) -> list[dict]:
    """Scrape di una categoria — context Playwright fresco per aggirare il WAF."""
    log.info("Categoria: %s", cat_label)

    async def _do():
        ctx = await _new_context(browser)
        page = await ctx.new_page()
        try:
            has = await _navigate_and_wait(page, cat_url)
            if not has:
                return []
            prods = await _extract_products(page)
            log.info("  → %d prodotti trovati", len(prods))
            return prods
        finally:
            await ctx.close()

    return await retry(_do, retries=3, delay=3.0, label=cat_url)


# --------------------------------------------------------------------------- #
# Entry point
# --------------------------------------------------------------------------- #

async def run_scraper() -> list[dict]:
    all_products: list[dict] = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)

        for cat_label, cat_url in CATEGORIES:
            prods = await _scrape_category(browser, cat_label, cat_url)
            all_products.extend(prods)

        await browser.close()

    return deduplicate(all_products)


def main() -> Path:
    log.info("=" * 60)
    log.info("GamePeople Scraper — Console Xbox")
    log.info("Categorie: %s", ", ".join(lbl for lbl, _ in CATEGORIES))
    log.info("=" * 60)
    products = asyncio.run(run_scraper())
    log.info("Totale prodotti unici: %d", len(products))
    return save_snapshot(SOURCE, products, BASE_URL, DATA_DIR)


if __name__ == "__main__":
    main()
