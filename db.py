"""
Database SQLite — tracking storico prezzi e disponibilità.

Schema:
  categories    — categorie console (Xbox Series, Xbox One, Xbox 360, Xbox Original)
  storage_sizes — dimensioni di archiviazione (512 GB, 1 TB, 2 TB…)
  products      — prodotto unico per (source, name, condition)
  state_changes — un record per ogni cambio di prezzo o disponibilità
"""

from __future__ import annotations

import logging
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from migrations import Migration, run_migrations

log = logging.getLogger(__name__)

DB_PATH = Path(__file__).parent / "trader.db"

# Regex per estrarre dimensione archiviazione dal nome prodotto
_STORAGE_RE = re.compile(r'(\d[\d.]*)\s*(GB|TB)', re.IGNORECASE)

# Mapping famiglia console (stesso ordine del frontend)
_FAMILY_PATTERNS: list[tuple[str, re.Pattern]] = [
    ('series-x', re.compile(r'series\s*x', re.I)),
    ('series-s', re.compile(r'series\s*s', re.I)),
    ('one-x',    re.compile(r'one\s*x',    re.I)),
    ('one-s',    re.compile(r'one\s*s',    re.I)),
    ('one',      re.compile(r'\bone\b',    re.I)),
    ('360',      re.compile(r'\b360\b')),
    ('original', re.compile(r'\boriginal\b', re.I)),
]

_FAMILY_TO_CATEGORY = {
    'series-x': 'Xbox Series',
    'series-s': 'Xbox Series',
    'one-x':    'Xbox One',
    'one-s':    'Xbox One',
    'one':      'Xbox One',
    '360':      'Xbox 360',
    'original': 'Xbox Original',
}

_MIGRATIONS = (
    Migration(
        1,
        "baseline-indexes",
        (
            "CREATE INDEX IF NOT EXISTS idx_products_sku ON products(sku)",
            "CREATE INDEX IF NOT EXISTS idx_products_last_seen ON products(last_seen)",
            "CREATE INDEX IF NOT EXISTS idx_products_price ON products(last_price)",
        ),
    ),
    Migration(
        2,
        "fill-missing-family",
        (
            "UPDATE products SET console_family = 'other' "
            "WHERE console_family IS NULL OR TRIM(console_family) = ''",
        ),
    ),
)


# ---------------------------------------------------------------------------
# Helpers interni
# ---------------------------------------------------------------------------

def _detect_family(name: str) -> str:
    for key, pattern in _FAMILY_PATTERNS:
        if pattern.search(name):
            return key
    return 'other'


def _extract_storage(name: str) -> str | None:
    """Estrae la dimensione di archiviazione dal nome prodotto.

    Examples:
        "Xbox Series X 1TB Carbon Black" → "1 TB"
        "Xbox Series S 512GB"            → "512 GB"
        "Xbox 360 250 GB"                → "250 GB"
    """
    m = _STORAGE_RE.search(name)
    if not m:
        return None
    value = m.group(1).replace('.', '')   # rimuovi separatori migliaia
    unit  = m.group(2).upper()
    return f"{value} {unit}"


def _connect(db_path: Path = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def _get_or_create_category(conn: sqlite3.Connection, name: str) -> int | None:
    if not name:
        return None
    row = conn.execute("SELECT id FROM categories WHERE name = ?", (name,)).fetchone()
    if row:
        return row["id"]
    cur = conn.execute("INSERT INTO categories (name) VALUES (?)", (name,))
    return cur.lastrowid


def _get_or_create_storage(conn: sqlite3.Connection, label: str) -> int | None:
    if not label:
        return None
    row = conn.execute("SELECT id FROM storage_sizes WHERE label = ?", (label,)).fetchone()
    if row:
        return row["id"]
    cur = conn.execute("INSERT INTO storage_sizes (label) VALUES (?)", (label,))
    return cur.lastrowid


# ---------------------------------------------------------------------------
# Inizializzazione
# ---------------------------------------------------------------------------

def init_db(db_path: Path = DB_PATH) -> None:
    """Crea le tabelle se non esistono già."""
    with _connect(db_path) as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS categories (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            name      TEXT    NOT NULL UNIQUE,
            parent_id INTEGER REFERENCES categories(id)
        );

        CREATE TABLE IF NOT EXISTS storage_sizes (
            id    INTEGER PRIMARY KEY AUTOINCREMENT,
            label TEXT    NOT NULL UNIQUE
        );

        CREATE TABLE IF NOT EXISTS products (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            name           TEXT    NOT NULL,
            source         TEXT    NOT NULL,
            condition      TEXT    NOT NULL,
            sku            TEXT,
            category_id    INTEGER REFERENCES categories(id),
            storage_id     INTEGER REFERENCES storage_sizes(id),
            console_family TEXT,
            url            TEXT,
            image_url      TEXT,
            is_base_model  INTEGER NOT NULL DEFAULT 0,
            first_seen     TEXT    NOT NULL,
            last_seen      TEXT    NOT NULL,
            last_price     REAL,
            last_available INTEGER,
            UNIQUE(source, name, condition)
        );

        CREATE TABLE IF NOT EXISTS state_changes (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id    INTEGER NOT NULL REFERENCES products(id),
            changed_at    TEXT    NOT NULL,
            price_old     REAL,
            price_new     REAL,
            available_old INTEGER,
            available_new INTEGER,
            change_type   TEXT    NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_changes_product ON state_changes(product_id);
        CREATE INDEX IF NOT EXISTS idx_changes_date    ON state_changes(changed_at);
        CREATE INDEX IF NOT EXISTS idx_products_source ON products(source);
        CREATE INDEX IF NOT EXISTS idx_products_family ON products(console_family);
        """)
    applied = run_migrations(db_path, _MIGRATIONS)
    if applied:
        log.info("Migrazioni trader.db applicate: %s", applied)
    log.info("DB pronto: %s", db_path)


# ---------------------------------------------------------------------------
# Processamento prodotti (change detection)
# ---------------------------------------------------------------------------

def process_products(
    products: list[dict],
    db_path:  Path = DB_PATH,
) -> dict[str, int]:
    """Processa una lista di prodotti, aggiorna il DB e registra i cambi.

    Logica:
      - Prodotto nuovo         → INSERT + state_change 'new'
      - Prezzo cambiato        → UPDATE + state_change 'price'
      - Disponibilità cambiata → UPDATE + state_change 'availability'
      - Entrambi cambiati      → UPDATE + state_change 'both'
      - Nessun cambio          → solo aggiornamento last_seen

    Returns:
        dict con chiavi: new, price_changes, avail_changes, unchanged
    """
    now   = datetime.now(timezone.utc).isoformat()
    stats = {"new": 0, "price_changes": 0, "avail_changes": 0, "unchanged": 0}

    with _connect(db_path) as conn:
        for p in products:
            name      = (p.get("name") or "").strip()
            source    = (p.get("source") or "").strip()
            condition = (p.get("condition") or "N/D").strip()
            price     = p.get("price")
            available = int(bool(p.get("available", True)))
            sku       = p.get("sku") or None
            url       = p.get("url") or ""
            image_url = p.get("image_url") or ""

            if not name or not source:
                continue

            family        = _detect_family(name)
            category_name = _FAMILY_TO_CATEGORY.get(family)
            storage_label = _extract_storage(name)

            category_id = _get_or_create_category(conn, category_name)
            storage_id  = _get_or_create_storage(conn, storage_label)

            existing = conn.execute(
                "SELECT * FROM products WHERE source = ? AND name = ? AND condition = ?",
                (source, name, condition),
            ).fetchone()

            if existing is None:
                cur = conn.execute("""
                    INSERT INTO products
                        (name, source, condition, sku, category_id, storage_id,
                         console_family, url, image_url,
                         first_seen, last_seen, last_price, last_available)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (name, source, condition, sku, category_id, storage_id,
                      family, url, image_url,
                      now, now, price, available))
                pid = cur.lastrowid
                conn.execute("""
                    INSERT INTO state_changes
                        (product_id, changed_at,
                         price_old, price_new,
                         available_old, available_new, change_type)
                    VALUES (?, ?, NULL, ?, NULL, ?, 'new')
                """, (pid, now, price, available))
                stats["new"] += 1

            else:
                pid           = existing["id"]
                old_price     = existing["last_price"]
                old_available = existing["last_available"]
                price_changed = (old_price != price)
                avail_changed = (old_available != available)

                conn.execute("""
                    UPDATE products
                    SET last_seen      = ?,
                        url            = ?,
                        image_url      = ?,
                        sku            = COALESCE(?, sku),
                        category_id    = COALESCE(?, category_id),
                        storage_id     = COALESCE(?, storage_id),
                        console_family = COALESCE(?, console_family),
                        last_price     = ?,
                        last_available = ?
                    WHERE id = ?
                """, (now, url, image_url, sku, category_id, storage_id,
                      family, price, available, pid))

                if price_changed or avail_changed:
                    if price_changed and avail_changed:
                        change_type = "both"
                        stats["price_changes"] += 1
                        stats["avail_changes"]  += 1
                    elif price_changed:
                        change_type = "price"
                        stats["price_changes"] += 1
                    else:
                        change_type = "availability"
                        stats["avail_changes"] += 1

                    conn.execute("""
                        INSERT INTO state_changes
                            (product_id, changed_at,
                             price_old, price_new,
                             available_old, available_new, change_type)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, (pid, now,
                          old_price, price,
                          old_available, available,
                          change_type))
                else:
                    stats["unchanged"] += 1

    return stats


# ---------------------------------------------------------------------------
# Lettura dati (usata dalle API del viewer)
# ---------------------------------------------------------------------------

def get_all_products(db_path: Path = DB_PATH) -> list[dict]:
    """Tutti i prodotti con metadati categoria e storage."""
    with _connect(db_path) as conn:
        rows = conn.execute("""
            SELECT
                p.id, p.name, p.source, p.condition, p.sku,
                p.console_family, p.url, p.image_url,
                p.is_base_model,
                p.first_seen, p.last_seen,
                p.last_price, p.last_available,
                c.name  AS category_name,
                s.label AS storage_label
            FROM products p
            LEFT JOIN categories   c ON c.id = p.category_id
            LEFT JOIN storage_sizes s ON s.id = p.storage_id
            ORDER BY p.console_family, p.name, p.source, p.condition
        """).fetchall()
    return [dict(r) for r in rows]


def get_base_models(db_path: Path = DB_PATH) -> list[dict]:
    """Solo i prodotti marcati come modello base."""
    with _connect(db_path) as conn:
        rows = conn.execute("""
            SELECT
                p.id, p.name, p.source, p.condition, p.sku,
                p.console_family, p.url, p.image_url,
                p.last_price, p.last_available,
                c.name  AS category_name,
                s.label AS storage_label
            FROM products p
            LEFT JOIN categories   c ON c.id = p.category_id
            LEFT JOIN storage_sizes s ON s.id = p.storage_id
            WHERE p.is_base_model = 1
            ORDER BY p.console_family, p.name, p.source
        """).fetchall()
    return [dict(r) for r in rows]


def get_recent_changes(days: int = 30, db_path: Path = DB_PATH) -> list[dict]:
    """Cambi di prezzo/disponibilità degli ultimi N giorni."""
    with _connect(db_path) as conn:
        rows = conn.execute("""
            SELECT
                sc.id, sc.changed_at, sc.change_type,
                sc.price_old, sc.price_new,
                sc.available_old, sc.available_new,
                p.id   AS product_id,
                p.name, p.source, p.condition,
                p.console_family, p.url
            FROM state_changes sc
            JOIN products p ON p.id = sc.product_id
            WHERE sc.changed_at >= datetime('now', ?)
            ORDER BY sc.changed_at DESC
        """, (f"-{days} days",)).fetchall()
    return [dict(r) for r in rows]


def set_base_model(product_id: int, value: bool, db_path: Path = DB_PATH) -> bool:
    """Imposta is_base_model. Returns True se il prodotto esiste."""
    with _connect(db_path) as conn:
        cur = conn.execute(
            "UPDATE products SET is_base_model = ? WHERE id = ?",
            (int(value), product_id),
        )
    return cur.rowcount > 0


def get_storage_sizes(db_path: Path = DB_PATH) -> list[dict]:
    """Tutte le dimensioni di archiviazione note."""
    with _connect(db_path) as conn:
        rows = conn.execute(
            "SELECT id, label FROM storage_sizes ORDER BY label"
        ).fetchall()
    return [dict(r) for r in rows]


def get_categories(db_path: Path = DB_PATH) -> list[dict]:
    """Tutte le categorie."""
    with _connect(db_path) as conn:
        rows = conn.execute(
            "SELECT id, name, parent_id FROM categories ORDER BY name"
        ).fetchall()
    return [dict(r) for r in rows]
