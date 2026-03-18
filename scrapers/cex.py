"""
Scraper CEX (it.webuy.com) — Console Xbox (usato)
Usa l'API Algolia pubblica di WeBuy (nessun Playwright necessario).
Salva in: data/cex_YYYY-MM-DD_HH-MM-SS.json

Categorie tracciate (solo console hardware, no giochi né accessori):
  - categoryId 1090: Xbox Series Console  (Series X / Series S)
  - categoryId 1003: Xbox One Console
  - categoryId 829:  Xbox 360 Console
  - categoryId 1031: Xbox Console (originale)

Condizione: sempre "Usato" (CEX vende solo ricondizionato).
Grading CEX: Imballata / Non Imballata / Scontata (estratto dal nome).
"""

from __future__ import annotations

import logging
import re
import time
from pathlib import Path

import requests

from scrapers.base import deduplicate, retry_sync, save_snapshot
from settings import load_config

log = logging.getLogger("cex")

# --- Config ---
_CONFIG_PATH = Path(__file__).parent.parent / "config.toml"
_CFG = load_config(_CONFIG_PATH)

_COMMON = _CFG["common"]
_DATA   = _CFG["data"]

BASE_URL     = "https://it.webuy.com"
PRODUCT_URL  = f"{BASE_URL}/product-detail/?id="
DATA_DIR     = Path(__file__).parent.parent / _DATA["output_dir"]
SOURCE       = "cex"
DELAY        = _COMMON["request_delay"]

# Algolia — credenziali pubbliche (browser-extractable)
_ALGOLIA_ENDPOINT  = "https://search.webuy.io/1/indexes/*/queries"
_ALGOLIA_APP_ID    = "LNNFEEWZVA"
_ALGOLIA_API_KEY   = "bf79f2b6699e60a18ae330a1248b452c"
_ALGOLIA_INDEX     = "prod_cex_it_box_name_asc"
_HITS_PER_PAGE     = 1000

_ALGOLIA_HEADERS = {
    "Content-Type":  "application/json",
    "Origin":        "https://it.webuy.com",
    "Referer":       "https://it.webuy.com/",
    "User-Agent":    _COMMON["user_agent"],
}

# Categorie console Xbox (solo hardware, no giochi né accessori)
_CATEGORIES = [
    (1090, "Xbox Series Console"),
    (1003, "Xbox One Console"),
    (829,  "Xbox 360 Console"),
    (1031, "Xbox Console"),
]

# Grading CEX estratto dalla parte finale del nome prodotto
_GRADE_RE = re.compile(
    r",\s*(Imballata|Non Imballata|Scontata)\s*$", re.IGNORECASE
)

_ATTRIBUTES = [
    "boxId", "boxName", "sellPrice",
    "ecomQuantity", "collectionQuantity",
    "categoryFriendlyName", "categoryId",
]


# --------------------------------------------------------------------------- #
# Algolia query
# --------------------------------------------------------------------------- #

def _algolia_query(session: requests.Session, filters: str) -> dict | None:
    """Esegue una POST all'endpoint Algolia e restituisce il risultato grezzo."""
    params = "&".join([
        f"attributesToRetrieve={','.join(_ATTRIBUTES)}",
        f"filters={filters}",
        f"hitsPerPage={_HITS_PER_PAGE}",
        "query=",
    ])
    payload = {"requests": [{"indexName": _ALGOLIA_INDEX, "params": params}]}
    url = (
        f"{_ALGOLIA_ENDPOINT}"
        f"?x-algolia-api-key={_ALGOLIA_API_KEY}"
        f"&x-algolia-application-id={_ALGOLIA_APP_ID}"
    )

    def _do():
        r = session.post(url, json=payload, headers=_ALGOLIA_HEADERS, timeout=15)
        r.raise_for_status()
        return r.json()["results"][0]

    return retry_sync(_do, retries=3, delay=2.0, label=f"algolia cat filter={filters}")


# --------------------------------------------------------------------------- #
# Parsing
# --------------------------------------------------------------------------- #

def _parse_hit(hit: dict) -> dict:
    """Converte un hit Algolia nel formato standard dello scraper."""
    box_id   = hit.get("boxId", "")
    name     = (hit.get("boxName") or "").strip()
    price_v  = hit.get("sellPrice")
    ecom     = hit.get("ecomQuantity") or 0
    coll     = hit.get("collectionQuantity") or 0
    available = ecom > 0 or coll > 0
    category  = hit.get("categoryFriendlyName", "")

    # Estrai grading dal nome (es. "Xbox Series X, 1TB, ..., Imballata")
    grade_match = _GRADE_RE.search(name)
    grade = grade_match.group(1).strip() if grade_match else ""

    # Prezzo: Algolia restituisce già un float
    price = float(price_v) if price_v is not None else None
    price_display = f"{price:.2f}".replace(".", ",") + " €" if price is not None else "N/D"

    return {
        "name":          name,
        "sku":           f"CEX-{box_id}",
        "price":         price,
        "price_display": price_display,
        "condition":     "Usato",
        "grade":         grade,
        "category":      category,
        "available":     available,
        "url":           f"{PRODUCT_URL}{box_id}",
        "image_url":     "",
        "source":        SOURCE,
    }


# --------------------------------------------------------------------------- #
# Scraper
# --------------------------------------------------------------------------- #

def _scrape_category(session: requests.Session, cat_id: int, label: str) -> list[dict]:
    """Scarica tutti i prodotti di una categoria console CEX via Algolia.

    Algolia limita a 1000 hit per query. Le categorie console Xbox
    sono tutte abbondantemente sotto questo limite, quindi una query sola basta.
    Se in futuro superassero 1000, la stessa strategia del cex-price-tracker
    (query disponibili + query esauriti) è già documentata nell'altro progetto.
    """
    log.info("Categoria: %s (id=%d)", label, cat_id)
    filters = f"boxVisibilityOnWeb=1 AND boxSaleAllowed=1 AND categoryId:{cat_id}"
    result = _algolia_query(session, filters)
    if result is None:
        log.error("Nessuna risposta da Algolia per categoria %s", label)
        return []

    hits = result.get("hits", [])
    total = result.get("nbHits", 0)
    log.info("  → %d prodotti totali (restituiti: %d)", total, len(hits))

    if total > _HITS_PER_PAGE:
        log.warning(
            "Categoria %s ha %d prodotti > limite %d: alcuni potrebbero mancare. "
            "Implementare split disponibili/esauriti se necessario.",
            label, total, _HITS_PER_PAGE,
        )

    products = [_parse_hit(h) for h in hits]
    avail = sum(1 for p in products if p["available"])
    log.info("  → %d disponibili, %d esauriti", avail, len(products) - avail)
    return products


def run_scraper() -> list[dict]:
    """Scrape tutte le categorie console Xbox su CEX via Algolia."""
    all_products: list[dict] = []

    with requests.Session() as session:
        for cat_id, label in _CATEGORIES:
            try:
                products = _scrape_category(session, cat_id, label)
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
    log.info("CEX Scraper — Console Xbox (Algolia API)")
    log.info("=" * 60)
    products = run_scraper()
    log.info("Totale prodotti unici: %d", len(products))
    return save_snapshot(SOURCE, products, BASE_URL, DATA_DIR)


if __name__ == "__main__":
    main()
