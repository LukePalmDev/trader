"""
Scraper JollyRogerBay (jollyrogerbay.it) — Prodotti Xbox (usato + nuovo)
PrestaShop 1.7, server-side rendering, nessun JS necessario.
Salva in: data/jollyrogerbay_YYYY-MM-DD_HH-MM-SS.json

Categorie tracciate:
  - Xbox Original    /shop/140-xbox
  - Xbox 360         /shop/54-xbox-360
  - Xbox One         /shop/14-xbox-one
  - Xbox Series X    /shop/183-xbox-series-x

Paginazione: ?page=N  (32 prodotti per pagina)
Condizione: rilevata dall'URL del prodotto (contiene "usato" o "#/XX-condizioni-usato")
"""

from __future__ import annotations

import logging
import re
import sys
import time
from pathlib import Path

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib

import requests
from bs4 import BeautifulSoup

from scrapers.base import clean_price, retry_sync, save_snapshot, deduplicate

log = logging.getLogger("jollyrogerbay")

# --- Config ---
_CONFIG_PATH = Path(__file__).parent.parent / "config.toml"
with open(_CONFIG_PATH, "rb") as _f:
    _CFG = tomllib.load(_f)

_COMMON = _CFG["common"]
_SRC    = _CFG["sources"]["jollyrogerbay"]
_DATA   = _CFG["data"]

BASE_URL = "https://www.jollyrogerbay.it"
DATA_DIR = Path(__file__).parent.parent / _DATA["output_dir"]
SOURCE   = "jollyrogerbay"
DELAY    = _COMMON["request_delay"]

# Categorie Xbox da scrapare
_CATEGORIES = [
    ("Xbox Original",  f"{BASE_URL}/shop/140-xbox"),
    ("Xbox 360",       f"{BASE_URL}/shop/54-xbox-360"),
    ("Xbox One",       f"{BASE_URL}/shop/14-xbox-one"),
    ("Xbox Series X",  f"{BASE_URL}/shop/183-xbox-series-x"),
]

_HEADERS = {
    "User-Agent":      _COMMON["user_agent"],
    "Accept":          "text/html,application/xhtml+xml",
    "Accept-Language": "it-IT,it;q=0.9",
}


# --------------------------------------------------------------------------- #
# HTTP
# --------------------------------------------------------------------------- #

def _get(url: str) -> str:
    def _do():
        r = requests.get(url, headers=_HEADERS, timeout=20)
        r.raise_for_status()
        return r.text
    return retry_sync(_do, retries=3, delay=2.0, label=url)


# --------------------------------------------------------------------------- #
# Condizione dal URL
# --------------------------------------------------------------------------- #

def _condition_from_url(href: str) -> str:
    """Rileva condizione dall'URL del prodotto.

    JollyRogerBay aggiunge 'usato' o 'nuovo' al nome del prodotto nell'URL
    e nel fragment dell'ancoraggio (#/53-condizioni-usato).
    """
    lower = href.lower()
    # Verifica fragment ancora: #/XX-condizioni-usato
    if "condizioni-usato" in lower or "-usato-" in lower or lower.endswith("-usato"):
        return "Usato"
    if "condizioni-nuovo" in lower or "-nuovo-" in lower or lower.endswith("-nuovo"):
        return "Nuovo"
    # Nessun indicatore → assume Nuovo (pre-ordini senza condizione esplicita)
    return "Nuovo"


# --------------------------------------------------------------------------- #
# Parsing pagina
# --------------------------------------------------------------------------- #

def _parse_page(html: str, category_label: str) -> list[dict]:
    """Estrae prodotti da una pagina PrestaShop di JollyRogerBay."""
    soup = BeautifulSoup(html, "html.parser")
    products = []

    for card in soup.select(".js-product.product"):
        article = card.select_one("article.product-miniature")
        if not article:
            continue

        # Nome
        name_el = card.select_one(".product-title a")
        if not name_el:
            continue
        name = name_el.get_text(strip=True)
        # Sostituisci con il titolo completo se disponibile nell'attributo title
        # NOTA: l'attributo "content" contiene l'URL prodotto (non il nome),
        # perciò NON va usato come fallback del nome.
        title_full = name_el.get("title", "")
        if title_full and not title_full.startswith("http") and len(title_full) > len(name):
            name = title_full.strip()

        # URL
        url = name_el.get("href", "")

        # Condizione (dall'URL)
        condition = _condition_from_url(url)

        # Prezzo
        price_el = card.select_one("span.price[aria-label]")
        price_raw = price_el.get_text(strip=True) if price_el else ""
        price = clean_price(price_raw)

        # Immagine
        img_el = card.select_one(".thumbnail-container img")
        img_url = ""
        if img_el:
            img_url = (img_el.get("data-full-size-image-url")
                       or img_el.get("src")
                       or "")

        # SKU: data-id-product + data-id-product-attribute
        pid   = article.get("data-id-product", "")
        pattr = article.get("data-id-product-attribute", "")
        sku = f"JRB-{pid}-{pattr}" if pid else ""

        products.append({
            "name":          name,
            "sku":           sku,
            "price":         price,
            "price_display": price_raw,
            "condition":     condition,
            "category":      category_label,
            "url":           url,
            "image_url":     img_url,
            "source":        SOURCE,
        })

    return products


def _has_next_page(html: str) -> bool:
    """Verifica se esiste una pagina successiva nel pager."""
    soup = BeautifulSoup(html, "html.parser")
    return bool(soup.select_one("a.next.js-search-link, li.next a"))


# --------------------------------------------------------------------------- #
# Scraper
# --------------------------------------------------------------------------- #

def _scrape_category(label: str, base_url: str) -> list[dict]:
    """Scrape tutte le pagine di una categoria Xbox."""
    log.info("Categoria: %s", label)
    all_products = []
    page = 1

    while True:
        url = base_url if page == 1 else f"{base_url}?page={page}"
        log.info("  Pagina %d: %s", page, url)
        html = _get(url)
        products = _parse_page(html, label)

        if not products:
            log.info("  → Pagina vuota, stop.")
            break

        all_products.extend(products)
        log.info("  → %d prodotti trovati", len(products))

        if not _has_next_page(html):
            break

        page += 1
        time.sleep(DELAY)

    return all_products


def run_scraper() -> list[dict]:
    """Scrape tutte le categorie Xbox su JollyRogerBay."""
    all_products = []

    for label, url in _CATEGORIES:
        try:
            products = _scrape_category(label, url)
            all_products.extend(products)
        except Exception as exc:
            log.error("Errore categoria %r: %s", label, exc)
        time.sleep(DELAY)

    return deduplicate(all_products)


# --------------------------------------------------------------------------- #
# Entry point
# --------------------------------------------------------------------------- #

def main() -> Path:
    log.info("=" * 60)
    log.info("JollyRogerBay Scraper — Prodotti Xbox")
    log.info("=" * 60)
    products = run_scraper()
    log.info("Totale prodotti unici: %d", len(products))
    return save_snapshot(SOURCE, products, BASE_URL, DATA_DIR)


if __name__ == "__main__":
    main()
