"""
Database SQLite dedicato a eBay.it — tracking prezzi venduto.

Schema separato dagli altri DB perché gli item eBay "venduto" sono
entità diverse: rappresentano prezzi di mercato realizzati, non annunci
attivi da monitorare.

Tabelle:
  sold_items   — un record per ogni lotto venduto (unico per item_id)
  sold_changes — storico aggiornamenti (prezzo cambiato tra due scrape)
"""

from __future__ import annotations

import logging
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from migrations import Migration, run_migrations

log = logging.getLogger(__name__)

DB_PATH = Path(__file__).parent / "ebay.db"

_FAMILY_PATTERNS: list[tuple[str, re.Pattern]] = [
    ('series-x', re.compile(r'series\s*x', re.I)),
    ('series-s', re.compile(r'series\s*s', re.I)),
    ('one-x',    re.compile(r'one\s*x',    re.I)),
    ('one-s',    re.compile(r'one\s*s',    re.I)),
    ('one',      re.compile(r'\bone\b',    re.I)),
    ('360',      re.compile(r'\b360\b')),
    ('original', re.compile(r'\boriginal\b', re.I)),
]

_MIGRATIONS = (
    Migration(
        1,
        "ebay-extra-indexes",
        (
            "CREATE INDEX IF NOT EXISTS idx_sold_item_id ON sold_items(item_id)",
            "CREATE INDEX IF NOT EXISTS idx_sold_url ON sold_items(url)",
            "CREATE INDEX IF NOT EXISTS idx_sold_last_seen ON sold_items(last_seen)",
            "CREATE INDEX IF NOT EXISTS idx_sold_query_label ON sold_items(query_label)",
        ),
    ),
    Migration(
        2,
        "ebay-fill-family",
        (
            "UPDATE sold_items SET console_family = 'other' "
            "WHERE console_family IS NULL OR TRIM(console_family) = ''",
        ),
    ),
)


def _detect_family(name: str) -> str:
    for key, pattern in _FAMILY_PATTERNS:
        if pattern.search(name):
            return key
    return 'other'


def _connect(db_path: Path = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


# ---------------------------------------------------------------------------
# Inizializzazione
# ---------------------------------------------------------------------------

def init_db(db_path: Path = DB_PATH) -> None:
    """Crea le tabelle se non esistono già."""
    with _connect(db_path) as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS sold_items (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            item_id        TEXT    NOT NULL UNIQUE,   -- es. "EBAY-123456789"
            name           TEXT    NOT NULL,
            console_family TEXT,
            sold_price     REAL,
            sold_date      TEXT,                       -- data vendita (stringa dal sito)
            url            TEXT,
            query_label    TEXT,                       -- query che ha trovato questo item
            first_seen     TEXT    NOT NULL,
            last_seen      TEXT    NOT NULL
        );

        CREATE TABLE IF NOT EXISTS sold_changes (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            item_id      INTEGER NOT NULL REFERENCES sold_items(id),
            changed_at   TEXT    NOT NULL,
            price_old    REAL,
            price_new    REAL
        );

        CREATE INDEX IF NOT EXISTS idx_sold_family ON sold_items(console_family);
        CREATE INDEX IF NOT EXISTS idx_sold_price  ON sold_items(sold_price);
        CREATE INDEX IF NOT EXISTS idx_sold_date   ON sold_items(sold_date);
        """)
    applied = run_migrations(db_path, _MIGRATIONS)
    if applied:
        log.info("Migrazioni ebay.db applicate: %s", applied)
    log.info("eBay DB pronto: %s", db_path)


# ---------------------------------------------------------------------------
# Processamento items
# ---------------------------------------------------------------------------

def process_sold_items(
    products:  list[dict],
    db_path:   Path = DB_PATH,
) -> dict[str, int]:
    """Inserisce/aggiorna i lotti venduti e registra variazioni di prezzo.

    Chiave univoca: item_id (campo "sku" nel formato standard).

    Returns:
        dict con chiavi: new, price_changes, unchanged
    """
    now   = datetime.now(timezone.utc).isoformat()
    stats = {"new": 0, "price_changes": 0, "unchanged": 0}

    with _connect(db_path) as conn:
        for p in products:
            item_id = (p.get("sku") or "").strip()
            name    = (p.get("name") or "").strip()
            if not item_id or not name:
                continue

            price       = p.get("price")
            sold_date   = p.get("sold_date") or ""
            url         = p.get("url") or ""
            query_label = p.get("query_label") or ""
            family      = _detect_family(name)

            existing = conn.execute(
                "SELECT * FROM sold_items WHERE item_id = ?", (item_id,)
            ).fetchone()
            if existing is None and url:
                # Fallback ID migration: riallinea record storici creati con ID non stabili
                existing = conn.execute(
                    "SELECT * FROM sold_items WHERE url = ? ORDER BY id LIMIT 1",
                    (url,),
                ).fetchone()
                if existing is not None and existing["item_id"] != item_id:
                    try:
                        conn.execute(
                            "UPDATE sold_items SET item_id = ? WHERE id = ?",
                            (item_id, existing["id"]),
                        )
                    except sqlite3.IntegrityError:
                        # item_id già presente: mantieni record originale
                        pass

            if existing is None:
                conn.execute("""
                    INSERT INTO sold_items
                        (item_id, name, console_family, sold_price, sold_date,
                         url, query_label, first_seen, last_seen)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (item_id, name, family, price, sold_date,
                      url, query_label, now, now))
                stats["new"] += 1

            else:
                row_id    = existing["id"]
                old_price = existing["sold_price"]

                conn.execute("""
                    UPDATE sold_items
                    SET last_seen      = ?,
                        url            = COALESCE(NULLIF(?, ''), url),
                        console_family = ?,
                        sold_price     = ?,
                        sold_date      = COALESCE(NULLIF(?, ''), sold_date)
                    WHERE id = ?
                """, (now, url, family, price, sold_date, row_id))

                if old_price != price:
                    conn.execute("""
                        INSERT INTO sold_changes (item_id, changed_at, price_old, price_new)
                        VALUES (?, ?, ?, ?)
                    """, (row_id, now, old_price, price))
                    stats["price_changes"] += 1
                else:
                    stats["unchanged"] += 1

    return stats


# ---------------------------------------------------------------------------
# Lettura dati
# ---------------------------------------------------------------------------

def get_all_sold(db_path: Path = DB_PATH) -> list[dict]:
    """Tutti i lotti venduti, ordinati per famiglia e data."""
    with _connect(db_path) as conn:
        rows = conn.execute("""
            SELECT id, item_id, name, console_family,
                   sold_price, sold_date, url, query_label,
                   first_seen, last_seen
            FROM sold_items
            ORDER BY console_family, sold_date DESC NULLS LAST
        """).fetchall()
    return [dict(r) for r in rows]


def get_stats(db_path: Path = DB_PATH) -> dict:
    """Statistiche: totale item, famiglie, prezzo medio per famiglia."""
    with _connect(db_path) as conn:
        overall = conn.execute("""
            SELECT
                COUNT(*)                                        AS total,
                MIN(CASE WHEN sold_price > 0 THEN sold_price END) AS min_price,
                MAX(sold_price)                                 AS max_price,
                AVG(CASE WHEN sold_price > 0 THEN sold_price END) AS avg_price
            FROM sold_items
        """).fetchone()

        by_family = conn.execute("""
            SELECT
                console_family,
                COUNT(*)                                          AS count,
                MIN(CASE WHEN sold_price > 0 THEN sold_price END) AS min_price,
                MAX(sold_price)                                   AS max_price,
                AVG(CASE WHEN sold_price > 0 THEN sold_price END) AS avg_price
            FROM sold_items
            GROUP BY console_family
            ORDER BY console_family
        """).fetchall()

    return {
        "overall":   dict(overall) if overall else {},
        "by_family": [dict(r) for r in by_family],
    }
