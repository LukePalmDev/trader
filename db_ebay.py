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
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from migrations import Migration, run_migrations
from model_rules import classify_title, detect_family

log = logging.getLogger(__name__)

DB_PATH = Path(__file__).parent / "tracker.db"

def _table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {str(row[1]) for row in rows}


def _add_column_if_missing(conn: sqlite3.Connection, table_name: str, definition: str) -> None:
    column_name = definition.split()[0]
    if column_name not in _table_columns(conn, table_name):
        conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {definition}")


def _migration_v3_segment_models(conn: sqlite3.Connection) -> None:
    _add_column_if_missing(conn, "sold_items", "model_segment TEXT NOT NULL DEFAULT 'unknown'")
    _add_column_if_missing(conn, "sold_items", "edition_class TEXT NOT NULL DEFAULT 'standard'")
    _add_column_if_missing(conn, "sold_items", "canonical_model TEXT")
    _add_column_if_missing(conn, "sold_items", "classify_confidence REAL")
    _add_column_if_missing(conn, "sold_items", "classify_method TEXT")

    rows = conn.execute("SELECT id, name, console_family FROM sold_items").fetchall()
    payload = []
    for row in rows:
        row_id = int(row[0])
        name = str(row[1] or "")
        family_hint = row[2]
        classified = classify_title(name, family_hint=family_hint)
        payload.append(
            (
                classified.console_family,
                classified.model_segment,
                classified.edition_class,
                classified.canonical_model,
                classified.classify_confidence,
                classified.classify_method,
                row_id,
            )
        )

    if payload:
        conn.executemany(
            """
            UPDATE sold_items
            SET console_family = ?,
                model_segment = ?,
                edition_class = ?,
                canonical_model = ?,
                classify_confidence = ?,
                classify_method = ?
            WHERE id = ?
            """,
            payload,
        )

    conn.execute("CREATE INDEX IF NOT EXISTS idx_sold_segment ON sold_items(model_segment)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_sold_canonical ON sold_items(canonical_model)")

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
    Migration(
        3,
        "ebay-segment-models",
        callback=_migration_v3_segment_models,
    ),
    Migration(
        4,
        "add-check-triggers",
        (
            """
            CREATE TRIGGER IF NOT EXISTS chk_sold_items_price_insert
            BEFORE INSERT ON sold_items
            WHEN NEW.sold_price < 0
            BEGIN
                SELECT RAISE(ABORT, 'sold_price must be >= 0');
            END;
            """,
            """
            CREATE TRIGGER IF NOT EXISTS chk_sold_items_price_update
            BEFORE UPDATE ON sold_items
            WHEN NEW.sold_price < 0
            BEGIN
                SELECT RAISE(ABORT, 'sold_price must be >= 0');
            END;
            """,
        ),
    ),
)


def _detect_family(name: str) -> str:
    return detect_family(name)


def _connect(db_path: Path = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path), timeout=30.0)
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
            model_segment  TEXT    NOT NULL DEFAULT 'unknown',
            edition_class  TEXT    NOT NULL DEFAULT 'standard',
            canonical_model TEXT,
            classify_confidence REAL,
            classify_method TEXT,
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
    applied = run_migrations(db_path, _MIGRATIONS, namespace="ebay")
    if applied:
        log.info("Migrazioni sold_items (ebay) applicate: %s", applied)
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

    conn = _connect(db_path)
    conn.isolation_level = None
    conn.execute("BEGIN IMMEDIATE")
    try:
        for p in products:
            item_id = (p.get("sku") or "").strip()
            name    = (p.get("name") or "").strip()
            if not item_id or not name:
                continue

            price       = p.get("price")
            sold_date   = p.get("sold_date") or ""
            url         = p.get("url") or ""
            query_label = p.get("query_label") or ""
            initial_family = _detect_family(name)
            classified = classify_title(name, family_hint=initial_family)
            family = classified.console_family
            model_segment = classified.model_segment
            edition_class = classified.edition_class
            canonical_model = classified.canonical_model
            classify_confidence = classified.classify_confidence
            classify_method = classified.classify_method

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
                        (item_id, name, console_family,
                         model_segment, edition_class, canonical_model,
                         classify_confidence, classify_method,
                         sold_price, sold_date,
                         url, query_label, first_seen, last_seen)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (item_id, name, family,
                      model_segment, edition_class, canonical_model,
                      classify_confidence, classify_method,
                      price, sold_date,
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
                        model_segment  = ?,
                        edition_class  = ?,
                        canonical_model = ?,
                        classify_confidence = ?,
                        classify_method = ?,
                        sold_price     = ?,
                        sold_date      = COALESCE(NULLIF(?, ''), sold_date)
                    WHERE id = ?
                """, (now, url, family,
                      model_segment, edition_class, canonical_model,
                      classify_confidence, classify_method,
                      price, sold_date, row_id))

                if old_price != price:
                    conn.execute("""
                        INSERT INTO sold_changes (item_id, changed_at, price_old, price_new)
                        VALUES (?, ?, ?, ?)
                    """, (row_id, now, old_price, price))
                    stats["price_changes"] += 1
                else:
                    stats["unchanged"] += 1

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    return stats


# ---------------------------------------------------------------------------
# Lettura dati
# ---------------------------------------------------------------------------

def get_all_sold(db_path: Path = DB_PATH) -> list[dict]:
    """Tutti i lotti venduti, ordinati per famiglia e data."""
    with _connect(db_path) as conn:
        rows = conn.execute("""
            SELECT id, item_id, name, console_family,
                   model_segment, edition_class, canonical_model,
                   classify_confidence, classify_method,
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
