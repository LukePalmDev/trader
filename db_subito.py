"""
Database SQLite dedicato a Subito.it — tracking annunci e storico prezzi.

Schema separato dal DB principale (trader.db) perché gli annunci Subito
sono entità diverse dai prodotti shop:
  - chiave univoca: urn_id (es. "SUBITO-639766302")
  - campi specifici: city, region, published_at, seller_type
  - nessun concetto di "storage_sizes" o "categories" normalizzate
  - gli annunci sono efimeri (scadono, vengono rimossi)

Tabelle:
  ads        — un record per ogni annuncio visto (unico per urn_id)
  ad_changes — storico cambi prezzo / disponibilità
"""

from __future__ import annotations

import logging
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from migrations import Migration, run_migrations

log = logging.getLogger(__name__)

DB_PATH = Path(__file__).parent / "subito.db"

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
        "subito-extra-indexes",
        (
            "CREATE INDEX IF NOT EXISTS idx_ads_urn_id ON ads(urn_id)",
            "CREATE INDEX IF NOT EXISTS idx_ads_url ON ads(url)",
            "CREATE INDEX IF NOT EXISTS idx_ads_last_seen ON ads(last_seen)",
            "CREATE INDEX IF NOT EXISTS idx_ads_region ON ads(region)",
        ),
    ),
    Migration(
        2,
        "subito-fill-family",
        (
            "UPDATE ads SET console_family = 'other' "
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
        CREATE TABLE IF NOT EXISTS ads (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            urn_id         TEXT    NOT NULL UNIQUE,   -- es. "SUBITO-639766302"
            name           TEXT    NOT NULL,           -- titolo annuncio
            console_family TEXT,
            url            TEXT,
            image_url      TEXT,
            city           TEXT,
            region         TEXT,
            seller_type    TEXT,                       -- "privato" | "professionale"
            published_at   TEXT,                       -- data dal sito (es. "2026-03-18 08:21:53")
            first_seen     TEXT    NOT NULL,
            last_seen      TEXT    NOT NULL,
            last_price     REAL,
            last_available INTEGER NOT NULL DEFAULT 1
        );

        CREATE TABLE IF NOT EXISTS ad_changes (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            ad_id         INTEGER NOT NULL REFERENCES ads(id),
            changed_at    TEXT    NOT NULL,
            price_old     REAL,
            price_new     REAL,
            available_old INTEGER,
            available_new INTEGER,
            change_type   TEXT    NOT NULL   -- 'new' | 'price' | 'availability' | 'both'
        );

        CREATE INDEX IF NOT EXISTS idx_adchanges_ad   ON ad_changes(ad_id);
        CREATE INDEX IF NOT EXISTS idx_adchanges_date ON ad_changes(changed_at);
        CREATE INDEX IF NOT EXISTS idx_ads_family     ON ads(console_family);
        CREATE INDEX IF NOT EXISTS idx_ads_price      ON ads(last_price);
        """)
    applied = run_migrations(db_path, _MIGRATIONS)
    if applied:
        log.info("Migrazioni subito.db applicate: %s", applied)
    log.info("Subito DB pronto: %s", db_path)


# ---------------------------------------------------------------------------
# Processamento annunci (change detection)
# ---------------------------------------------------------------------------

def process_ads(
    products: list[dict],
    db_path:  Path = DB_PATH,
) -> dict[str, int]:
    """Processa una lista di annunci Subito, aggiorna il DB e registra i cambi.

    Chiave univoca: urn_id (campo "sku" nel formato standard, es. SUBITO-639766302).

    Returns:
        dict con chiavi: new, price_changes, avail_changes, unchanged
    """
    now   = datetime.now(timezone.utc).isoformat()
    stats = {"new": 0, "price_changes": 0, "avail_changes": 0, "unchanged": 0}

    with _connect(db_path) as conn:
        for p in products:
            urn_id    = (p.get("sku") or "").strip()
            name      = (p.get("name") or "").strip()
            if not urn_id or not name:
                continue

            price     = p.get("price")
            available = int(bool(p.get("available", True)))
            url       = p.get("url") or ""
            image_url = p.get("image_url") or ""
            city      = p.get("city") or ""
            region    = p.get("region") or ""
            seller    = p.get("seller_type") or "privato"
            pub_at    = p.get("published_at") or ""
            family    = _detect_family(name)

            existing = conn.execute(
                "SELECT * FROM ads WHERE urn_id = ?", (urn_id,)
            ).fetchone()
            if existing is None and url:
                # Fallback ID migration: riallinea record storici creati con ID non stabili
                existing = conn.execute(
                    "SELECT * FROM ads WHERE url = ? ORDER BY id LIMIT 1",
                    (url,),
                ).fetchone()
                if existing is not None and existing["urn_id"] != urn_id:
                    try:
                        conn.execute(
                            "UPDATE ads SET urn_id = ? WHERE id = ?",
                            (urn_id, existing["id"]),
                        )
                    except sqlite3.IntegrityError:
                        # urn_id già presente: mantieni riga esistente e ignora migrazione
                        pass

            if existing is None:
                cur = conn.execute("""
                    INSERT INTO ads
                        (urn_id, name, console_family, url, image_url,
                         city, region, seller_type, published_at,
                         first_seen, last_seen, last_price, last_available)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (urn_id, name, family, url, image_url,
                      city, region, seller, pub_at,
                      now, now, price, available))
                ad_id = cur.lastrowid
                conn.execute("""
                    INSERT INTO ad_changes
                        (ad_id, changed_at, price_old, price_new,
                         available_old, available_new, change_type)
                    VALUES (?, ?, NULL, ?, NULL, ?, 'new')
                """, (ad_id, now, price, available))
                stats["new"] += 1

            else:
                ad_id         = existing["id"]
                old_price     = existing["last_price"]
                old_available = existing["last_available"]
                price_changed = (old_price != price)
                avail_changed = (old_available != available)

                conn.execute("""
                    UPDATE ads
                    SET last_seen      = ?,
                        url            = ?,
                        image_url      = ?,
                        city           = COALESCE(NULLIF(?, ''), city),
                        region         = COALESCE(NULLIF(?, ''), region),
                        seller_type    = ?,
                        published_at   = COALESCE(NULLIF(?, ''), published_at),
                        console_family = ?,
                        last_price     = ?,
                        last_available = ?
                    WHERE id = ?
                """, (now, url, image_url,
                      city, region, seller, pub_at, family,
                      price, available, ad_id))

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
                        INSERT INTO ad_changes
                            (ad_id, changed_at, price_old, price_new,
                             available_old, available_new, change_type)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, (ad_id, now,
                          old_price, price,
                          old_available, available,
                          change_type))
                else:
                    stats["unchanged"] += 1

    return stats


# ---------------------------------------------------------------------------
# Lettura dati
# ---------------------------------------------------------------------------

def get_all_ads(db_path: Path = DB_PATH) -> list[dict]:
    """Tutti gli annunci, ordinati per famiglia e prezzo."""
    with _connect(db_path) as conn:
        rows = conn.execute("""
            SELECT
                id, urn_id, name, console_family,
                url, image_url, city, region,
                seller_type, published_at,
                first_seen, last_seen,
                last_price, last_available
            FROM ads
            ORDER BY console_family, last_price ASC NULLS LAST
        """).fetchall()
    return [dict(r) for r in rows]


def get_recent_changes(days: int = 30, db_path: Path = DB_PATH) -> list[dict]:
    """Cambi di prezzo/disponibilità degli ultimi N giorni."""
    with _connect(db_path) as conn:
        rows = conn.execute("""
            SELECT
                c.id, c.changed_at, c.change_type,
                c.price_old, c.price_new,
                c.available_old, c.available_new,
                a.id AS ad_id, a.name, a.console_family,
                a.url, a.city, a.region
            FROM ad_changes c
            JOIN ads a ON a.id = c.ad_id
            WHERE c.changed_at >= datetime('now', ?)
            ORDER BY c.changed_at DESC
        """, (f"-{days} days",)).fetchall()
    return [dict(r) for r in rows]


_PRICE_FLOORS: dict[str, float] = {
    # Nessuna console di questa famiglia viene venduta sotto questa soglia
    "series-x":  80.0,
    "series-s":  50.0,
    "one-x":     40.0,
    "one-s":     35.0,
    "one":       30.0,
    "360":       10.0,
    "original":   8.0,
    "other":      8.0,
}


def refilter_ads(
    blocklist,
    is_console,
    db_path: Path = DB_PATH,
) -> int:
    """Applica blocklist/allowlist + price floor agli annunci disponibili nel DB.

    Segna come non disponibili (last_available=0) gli annunci che:
      - matchano la blocklist (accessori/giochi noti), oppure
      - non matchano l'allowlist (nessun termine console), oppure
      - hanno un prezzo inferiore al floor per la loro famiglia.

    Returns:
        numero di annunci marcati come non disponibili
    """
    with _connect(db_path) as conn:
        rows = conn.execute(
            "SELECT id, name, console_family, last_price FROM ads WHERE last_available = 1"
        ).fetchall()

        bad_ids = []
        for row in rows:
            name   = row["name"]
            family = row["console_family"] or "other"
            price  = row["last_price"]

            # Filtro testuale
            if blocklist.search(name) or not is_console.search(name):
                bad_ids.append(row["id"])
                continue

            # Filtro price floor per famiglia
            floor = _PRICE_FLOORS.get(family, 8.0)
            if price is not None and price < floor:
                bad_ids.append(row["id"])

        if bad_ids:
            conn.execute(
                "UPDATE ads SET last_available = 0"
                f" WHERE id IN ({','.join('?' * len(bad_ids))})",
                bad_ids,
            )

    log.info("Refilter Subito DB: %d annunci marcati non disponibili", len(bad_ids))
    return len(bad_ids)


def get_ad_history(urn_id: str, db_path: Path = DB_PATH) -> dict:
    """Metadati + storico cambi prezzo/disponibilità per un annuncio."""
    with _connect(db_path) as conn:
        ad = conn.execute(
            "SELECT * FROM ads WHERE urn_id = ?", (urn_id,)
        ).fetchone()
        if not ad:
            return {}
        rows = conn.execute("""
            SELECT changed_at, price_old, price_new,
                   available_old, available_new, change_type
            FROM ad_changes
            WHERE ad_id = ?
            ORDER BY changed_at ASC
        """, (ad["id"],)).fetchall()
    return {
        "ad":      dict(ad),
        "changes": [dict(r) for r in rows],
    }


def get_stats(db_path: Path = DB_PATH) -> dict:
    """Statistiche rapide: totale annunci, disponibili, prezzo min/avg."""
    with _connect(db_path) as conn:
        row = conn.execute("""
            SELECT
                COUNT(*)                                    AS total,
                SUM(last_available)                         AS available,
                MIN(CASE WHEN last_available=1 AND last_price > 0 THEN last_price END) AS min_price,
                AVG(CASE WHEN last_available=1 AND last_price > 0 THEN last_price END) AS avg_price
            FROM ads
        """).fetchone()
    return dict(row) if row else {}
