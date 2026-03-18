"""
Scraper rebuy.it — Console Xbox (usato)
Angular SPA con SSR: accessibile tramite requests standard.
Salva in: data/rebuy_YYYY-MM-DD_HH-MM-SS.json

Categorie tracciate (solo console, non giochi né accessori):
  - /comprare/console-e-accessori/xbox/xbox-series-x/console
  - /comprare/console-e-accessori/xbox/xbox-series-s/console
  - /comprare/console-e-accessori/xbox/xbox-one/console
  - /comprare/console-e-accessori/xbox/xbox-360/console

Condizione: sempre "Usato" (rebuy vende solo ricondizionato).
Grading qualità: Eccellente / Molto buono / Buono / Accettabile
"""

import logging
import re
import time
from pathlib import Path

import requests
from bs4 import BeautifulSoup

from scrapers.base import clean_price, retry_sync, save_snapshot, deduplicate
from settings import load_config

log = logging.getLogger("rebuy")

# --- Config ---
_CONFIG_PATH = Path(__file__).parent.parent / "config.toml"
_CFG = load_config(_CONFIG_PATH)

_COMMON = _CFG["common"]
_SRC    = _CFG["sources"]["rebuy"]
_DATA   = _CFG["data"]

BASE_URL = "https://www.rebuy.it"
DATA_DIR = Path(__file__).parent.parent / _DATA["output_dir"]
SOURCE   = "rebuy"
DELAY    = _COMMON["request_delay"]

# Categorie console Xbox
_CATEGORIES = [
    ("Xbox Series X", f"{BASE_URL}/comprare/console-e-accessori/xbox/xbox-series-x/console"),
    ("Xbox Series S", f"{BASE_URL}/comprare/console-e-accessori/xbox/xbox-series-s/console"),
    ("Xbox One",      f"{BASE_URL}/comprare/console-e-accessori/xbox/xbox-one/console"),
    ("Xbox 360",      f"{BASE_URL}/comprare/console-e-accessori/xbox/xbox-360/console"),
]

_HEADERS = {
    "User-Agent":      _COMMON["user_agent"],
    "Accept":          "text/html,application/xhtml+xml",
    "Accept-Language": "it-IT,it;q=0.9",
}

# Pattern grading qualità rebuy
_GRADE_PATTERN = re.compile(
    r"\b(Eccellente|Molto buono|Buono|Accettabile)\b", re.IGNORECASE
)


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
    """Estrae prodotti da una pagina categoria rebuy.it.

    Seleziona tutti i `.ry-card` (incluso il prodotto "in evidenza" e
    i prodotti in lista con classe `.host-product-link`).
    """
    soup = BeautifulSoup(html, "html.parser")
    products = []

    for card in soup.select(".ry-card"):
        # Nome: usa `.title` (presente sia nel card evidenziato sia nella lista)
        title_el = card.select_one(".title")
        if not title_el:
            continue
        name = title_el.get_text(strip=True)
        if not name:
            continue

        # URL prodotto
        link_el = card.select_one("a[href]")
        if not link_el:
            continue
        href = link_el.get("href", "")
        url = href if href.startswith("http") else BASE_URL + href

        # SKU: da URL pattern /i,{ID}/
        sku_match = re.search(r"/i,(\d+)/", href)
        if not sku_match:
            continue
        sku = f"RBY-{sku_match.group(1)}"

        # Filtro: solo prodotti Xbox/Microsoft (esclude Samsung, ecc.)
        if not re.search(r"\bxbox\b|\bmicrosoft\b", name, re.IGNORECASE):
            log.debug("Escluso prodotto non-Xbox: %r", name)
            continue

        # Prezzo: cerca "NNN,NN €" nel testo del card
        card_text = card.get_text(" ", strip=True)
        price_match = re.search(r"([\d]+[,.][\d]+)\s*\u20ac", card_text)
        price_raw = price_match.group(0) if price_match else ""
        price = clean_price(price_raw)

        # Grading qualità (se presente)
        grade_match = _GRADE_PATTERN.search(card_text)
        grade = grade_match.group(1).capitalize() if grade_match else ""

        # Immagine
        img_el = card.select_one("img[src]")
        img_url = img_el.get("src", "") if img_el else ""

        # Disponibilità (prodotti non disponibili hanno ancora prezzo "da X€")
        unavailable = "product--unavailable" in card.get("class", [])

        products.append({
            "name":          name,
            "sku":           sku,
            "price":         price,
            "price_display": price_raw if price_raw else "N/D",
            "condition":     "Usato",
            "grade":         grade,
            "category":      category_label,
            "url":           url,
            "image_url":     img_url,
            "available":     not unavailable,
            "source":        SOURCE,
        })

    return products


# --------------------------------------------------------------------------- #
# Scraper
# --------------------------------------------------------------------------- #

def _has_next_page(html: str) -> bool:
    """Verifica se esiste un link alla pagina successiva nel pager rebuy."""
    soup = BeautifulSoup(html, "html.parser")
    for a in soup.select("a[aria-label], a.pagination__item--next, .pagination a"):
        text = a.get_text(strip=True).lower()
        label_attr = (a.get("aria-label") or "").lower()
        if "next" in text or "successiv" in text or "next" in label_attr or "successiv" in label_attr:
            return True
    return False


def _scrape_category(label: str, url: str) -> list[dict]:
    """Scarica e analizza tutte le pagine di una categoria rebuy.it."""
    log.info("Categoria: %s — %s", label, url)
    all_products: list[dict] = []
    page = 1

    while True:
        page_url = url if page == 1 else f"{url}?page={page}"
        log.info("  Pagina %d: %s", page, page_url)
        html = _get(page_url)
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
    """Scrape tutte le categorie console Xbox su rebuy.it."""
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
    log.info("rebuy.it Scraper — Console Xbox")
    log.info("=" * 60)
    products = run_scraper()
    log.info("Totale prodotti unici: %d", len(products))
    return save_snapshot(SOURCE, products, BASE_URL, DATA_DIR)


if __name__ == "__main__":
    main()
