"""
Scraper Gameshock (gameshock.it) — Console Xbox (usato + nuovo)
PrestaShop 1.x legacy, server-side rendering.
Salva in: data/gameshock_YYYY-MM-DD_HH-MM-SS.json

Categorie tracciate (solo console, non giochi né accessori):
  - /77-xbox-serie-x-e-s  (Series X / Series S)
  - /45-console-xbox-one  (Xbox One)
  - /46-console-xbox360   (Xbox 360)

Condizione: rilevata dal nome del prodotto (contiene "usata"/"usato")
Paginazione: ?p=N
"""

import logging
import re
import time
from pathlib import Path

import requests
from bs4 import BeautifulSoup

from scrapers.base import clean_price, retry_sync, save_snapshot, deduplicate
from settings import load_config

log = logging.getLogger("gameshock")

# --- Config ---
_CONFIG_PATH = Path(__file__).parent.parent / "config.toml"
_CFG = load_config(_CONFIG_PATH)

_COMMON = _CFG["common"]
_SRC    = _CFG["sources"]["gameshock"]
_DATA   = _CFG["data"]

BASE_URL = "https://www.gameshock.it"
DATA_DIR = Path(__file__).parent.parent / _DATA["output_dir"]
SOURCE   = "gameshock"
DELAY    = _COMMON["request_delay"]

# Categorie console Xbox (solo hardware, no giochi)
_CATEGORIES = [
    ("Xbox Series X/S", f"{BASE_URL}/77-xbox-serie-x-e-s"),
    ("Xbox One",        f"{BASE_URL}/45-console-xbox-one"),
    ("Xbox 360",        f"{BASE_URL}/46-console-xbox360"),
]

_HEADERS = {
    "User-Agent":      _COMMON["user_agent"],
    "Accept":          "text/html,application/xhtml+xml",
    "Accept-Language": "it-IT,it;q=0.9",
}

_USATO_PATTERN = re.compile(r"\busata?\b|\bused\b", re.IGNORECASE)


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
# Parsing
# --------------------------------------------------------------------------- #

def _parse_page(html: str, category_label: str) -> list[dict]:
    """Estrae prodotti da una pagina categoria Gameshock."""
    soup = BeautifulSoup(html, "html.parser")
    products = []

    for card in soup.select(".ajax_block_product"):
        # Nome
        name_el = card.select_one("h3 a")
        if not name_el:
            continue
        name = name_el.get_text(strip=True)
        if not name:
            continue

        # URL
        url = name_el.get("href", "")
        # Gameshock usa http:// nei link interni
        if url.startswith("http://www.gameshock.it"):
            url = url.replace("http://", "https://")

        # SKU: dall'URL (slug finale senza estensione)
        slug_match = re.search(r"/(\d+-[^/]+)\.html$", url)
        sku = slug_match.group(1) if slug_match else re.sub(r"[^a-z0-9-]", "", name.lower())[:30]

        # Prezzo
        price_el = card.select_one(".price")
        price_raw = price_el.get_text(strip=True) if price_el else ""
        price = clean_price(price_raw)

        # Immagine
        img_el = card.select_one("img")
        img_url = img_el.get("src", "") if img_el else ""
        if img_url and not img_url.startswith("http"):
            img_url = BASE_URL + img_url

        # Condizione: rilevata dal nome prodotto O dall'URL (slug URL spesso
        # contiene "usata" anche quando il nome display non lo esplicita)
        condition = "Usato" if (_USATO_PATTERN.search(name) or _USATO_PATTERN.search(url)) else "Nuovo"

        # Disponibilità: legge elemento .availability ("Disponibile" / "Non disponibile")
        avail_el = card.select_one(".availability")
        avail_text = avail_el.get_text(strip=True).lower() if avail_el else ""
        available = "non disp" not in avail_text and "esaur" not in avail_text

        products.append({
            "name":          name,
            "sku":           f"GSK-{sku}",
            "price":         price,
            "price_display": price_raw,
            "condition":     condition,
            "available":     available,
            "category":      category_label,
            "url":           url,
            "image_url":     img_url,
            "source":        SOURCE,
        })

    return products


def _has_next_page(html: str) -> bool:
    """Verifica se esiste il link 'Successivo' nel pager.

    Gameshock (PrestaShop) restituisce la stessa pagina per qualsiasi ?p=N
    fuori range. L'unico modo affidabile è cercare il link 'Successivo'.
    """
    soup = BeautifulSoup(html, "html.parser")
    for a in soup.select("#pagination a, .pagination a"):
        text = a.get_text(strip=True).lower()
        if "successivo" in text or "next" in text:
            return True
    return False


# --------------------------------------------------------------------------- #
# Scraper
# --------------------------------------------------------------------------- #

def _scrape_category(label: str, base_url: str) -> list[dict]:
    """Scrape tutte le pagine di una categoria console Gameshock."""
    log.info("Categoria: %s", label)
    all_products = []
    page = 1

    while True:
        url = base_url if page == 1 else f"{base_url}?p={page}"
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
    """Scrape tutte le categorie console Xbox su Gameshock."""
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
    log.info("Gameshock Scraper — Console Xbox")
    log.info("=" * 60)
    products = run_scraper()
    log.info("Totale prodotti unici: %d", len(products))
    return save_snapshot(SOURCE, products, BASE_URL, DATA_DIR)


if __name__ == "__main__":
    main()
