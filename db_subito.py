"""
Database SQLite — tracking annunci Subito.it e storico prezzi.

Tabelle: ads, ad_changes (in tracker.db condiviso).
Gli annunci Subito sono entità diverse dai prodotti shop:
  - chiave univoca: urn_id (es. "SUBITO-639766302")
  - campi specifici: city, region, published_at, seller_type
  - nessun concetto di "storage_sizes" o "categories" normalizzate
  - gli annunci sono efimeri (scadono, vengono rimossi)

Tabelle:
  ads        — un record per ogni annuncio visto (unico per urn_id)
  ad_changes — storico cambi prezzo / disponibilità
"""

from __future__ import annotations

import hashlib
import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from migrations import Migration, run_migrations
from model_rules import classify_title, detect_family

log = logging.getLogger(__name__)

DB_PATH = Path(__file__).parent / "tracker.db"
CLASSIFY_VERSION_LEGACY = "legacy-v1"
CLASSIFY_VERSION_RULES_TITLE = "rules:title:v1"

def _table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {str(row[1]) for row in rows}


def _add_column_if_missing(conn: sqlite3.Connection, table_name: str, definition: str) -> None:
    column_name = definition.split()[0]
    if column_name not in _table_columns(conn, table_name):
        conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {definition}")


def _normalize_body_text(raw: str | None) -> str:
    if not raw:
        return ""
    return " ".join(str(raw).split()).strip()


def _compute_text_hash(name: str, body_text: str = "") -> str:
    payload = f"{(name or '').strip().lower()}\n{_normalize_body_text(body_text).lower()}"
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def _to_utc(raw: str | None) -> datetime | None:
    if not raw:
        return None
    text = str(raw).strip()
    if not text:
        return None
    text = text.replace("Z", "+00:00")
    if "T" not in text and " " in text:
        text = text.replace(" ", "T")
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _estimate_sold_window(
    *,
    last_active_seen: str | None,
    first_inactive_seen: str | None,
) -> tuple[str | None, float | None]:
    end_dt = _to_utc(first_inactive_seen)
    if end_dt is None:
        return None, None

    start_dt = _to_utc(last_active_seen)
    if start_dt is None or end_dt < start_dt:
        return end_dt.isoformat(), None

    window_hours = (end_dt - start_dt).total_seconds() / 3600.0
    midpoint = start_dt + (end_dt - start_dt) / 2
    return midpoint.isoformat(), round(window_hours, 3)


def _migration_v3_segment_models(conn: sqlite3.Connection) -> None:
    _add_column_if_missing(conn, "ads", "model_segment TEXT NOT NULL DEFAULT 'unknown'")
    _add_column_if_missing(conn, "ads", "edition_class TEXT NOT NULL DEFAULT 'standard'")
    _add_column_if_missing(conn, "ads", "canonical_model TEXT")
    _add_column_if_missing(conn, "ads", "classify_confidence REAL")
    _add_column_if_missing(conn, "ads", "classify_method TEXT")

    rows = conn.execute("SELECT id, name, console_family FROM ads").fetchall()
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
            UPDATE ads
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

    conn.execute("CREATE INDEX IF NOT EXISTS idx_ads_segment ON ads(model_segment)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ads_canonical ON ads(canonical_model)")

def _migration_v5_ai_columns(conn: sqlite3.Connection) -> None:
    _add_column_if_missing(conn, "ads", "ai_status TEXT NOT NULL DEFAULT 'pending'")
    _add_column_if_missing(conn, "ads", "ai_confidence REAL")
    _add_column_if_missing(conn, "ads", "sold_at TEXT")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ads_ai_status ON ads(ai_status)")


def _migration_v6_text_hash_and_sold_window(conn: sqlite3.Connection) -> None:
    _add_column_if_missing(conn, "ads", "body_text TEXT")
    _add_column_if_missing(conn, "ads", "text_hash TEXT")
    _add_column_if_missing(conn, "ads", "classify_version TEXT")
    _add_column_if_missing(conn, "ads", "last_active_seen TEXT")
    _add_column_if_missing(conn, "ads", "first_inactive_seen TEXT")
    _add_column_if_missing(conn, "ads", "sold_at_estimated TEXT")
    _add_column_if_missing(conn, "ads", "sold_window_hours REAL")

    conn.execute("CREATE INDEX IF NOT EXISTS idx_ads_text_hash ON ads(text_hash)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ads_classify_version ON ads(classify_version)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ads_sold_estimated ON ads(sold_at_estimated)")

    conn.execute(
        "UPDATE ads SET classify_version = ? "
        "WHERE classify_version IS NULL OR TRIM(classify_version) = ''",
        (CLASSIFY_VERSION_LEGACY,),
    )
    conn.execute(
        "UPDATE ads SET last_active_seen = last_seen "
        "WHERE (last_active_seen IS NULL OR TRIM(last_active_seen) = '') "
        "AND last_available = 1 AND sold_at IS NULL",
    )
    conn.execute(
        "UPDATE ads SET first_inactive_seen = sold_at "
        "WHERE sold_at IS NOT NULL AND (first_inactive_seen IS NULL OR TRIM(first_inactive_seen) = '')",
    )
    conn.execute(
        "UPDATE ads SET sold_at_estimated = sold_at "
        "WHERE sold_at IS NOT NULL AND (sold_at_estimated IS NULL OR TRIM(sold_at_estimated) = '')",
    )
    conn.execute(
        "UPDATE ads SET sold_window_hours = 0.0 "
        "WHERE sold_at IS NOT NULL AND sold_window_hours IS NULL",
    )

    rows = conn.execute("SELECT id, name, body_text FROM ads").fetchall()
    payload = []
    for row in rows:
        row_id = int(row[0])
        name = str(row[1] or "")
        body = str(row[2] or "")
        payload.append((_compute_text_hash(name, body), row_id))
    if payload:
        conn.executemany("UPDATE ads SET text_hash = ? WHERE id = ?", payload)

def _migration_v7_verify_status(conn: sqlite3.Connection) -> None:
    _add_column_if_missing(conn, "ads", "verify_status TEXT NOT NULL DEFAULT 'buyable'")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ads_verify_status ON ads(verify_status)")
    # Backfill: annunci già venduti → sold, resto rimane buyable
    conn.execute("UPDATE ads SET verify_status = 'sold' WHERE sold_at IS NOT NULL")


def _migration_v8_last_verified_at(conn: sqlite3.Connection) -> None:
    _add_column_if_missing(conn, "ads", "last_verified_at TEXT")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ads_last_verified_at ON ads(last_verified_at)")


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
    Migration(
        3,
        "subito-segment-models",
        callback=_migration_v3_segment_models,
    ),
    Migration(
        4,
        "add-check-triggers",
        (
            """
            CREATE TRIGGER IF NOT EXISTS chk_ads_price_insert
            BEFORE INSERT ON ads
            WHEN NEW.last_price < 0
            BEGIN
                SELECT RAISE(ABORT, 'last_price must be >= 0');
            END;
            """,
            """
            CREATE TRIGGER IF NOT EXISTS chk_ads_price_update
            BEFORE UPDATE ON ads
            WHEN NEW.last_price < 0
            BEGIN
                SELECT RAISE(ABORT, 'last_price must be >= 0');
            END;
            """,
            """
            CREATE TRIGGER IF NOT EXISTS chk_ads_avail_insert
            BEFORE INSERT ON ads
            WHEN NEW.last_available NOT IN (0, 1)
            BEGIN
                SELECT RAISE(ABORT, 'last_available must be 0 or 1');
            END;
            """,
            """
            CREATE TRIGGER IF NOT EXISTS chk_ads_avail_update
            BEFORE UPDATE ON ads
            WHEN NEW.last_available NOT IN (0, 1)
            BEGIN
                SELECT RAISE(ABORT, 'last_available must be 0 or 1');
            END;
            """,
        ),
    ),
    Migration(
        5,
        "add-ai-columns",
        callback=_migration_v5_ai_columns,
    ),
    Migration(
        6,
        "add-text-hash-sold-window",
        callback=_migration_v6_text_hash_and_sold_window,
    ),
    Migration(
        7,
        "add-verify-status",
        callback=_migration_v7_verify_status,
    ),
    Migration(
        8,
        "add-last-verified-at",
        callback=_migration_v8_last_verified_at,
    ),
)


# ---------------------------------------------------------------------------
# Helpers interni
# ---------------------------------------------------------------------------

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
        CREATE TABLE IF NOT EXISTS ads (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            urn_id         TEXT    NOT NULL UNIQUE,   -- es. "SUBITO-639766302"
            name           TEXT    NOT NULL,           -- titolo annuncio
            body_text      TEXT,
            text_hash      TEXT,
            classify_version TEXT,
            console_family TEXT,
            model_segment  TEXT    NOT NULL DEFAULT 'unknown',
            edition_class  TEXT    NOT NULL DEFAULT 'standard',
            canonical_model TEXT,
            classify_confidence REAL,
            classify_method TEXT,
            url            TEXT,
            image_url      TEXT,
            city           TEXT,
            region         TEXT,
            seller_type    TEXT,                       -- "privato" | "professionale"
            published_at   TEXT,                       -- data dal sito (es. "2026-03-18 08:21:53")
            first_seen     TEXT    NOT NULL,
            last_seen      TEXT    NOT NULL,
            last_active_seen TEXT,
            first_inactive_seen TEXT,
            last_price     REAL,
            last_available INTEGER NOT NULL DEFAULT 1,
            ai_status      TEXT    NOT NULL DEFAULT 'pending',
            ai_confidence  REAL,
            verify_status  TEXT    NOT NULL DEFAULT 'buyable',
            sold_at        TEXT,
            sold_at_estimated TEXT,
            sold_window_hours REAL
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
    applied = run_migrations(db_path, _MIGRATIONS, namespace="ads")
    if applied:
        log.info("Migrazioni ads (subito) applicate: %s", applied)
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

    conn = _connect(db_path)
    conn.isolation_level = None
    conn.execute("BEGIN IMMEDIATE")
    try:
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
            body_text = _normalize_body_text((p.get("body_text") or p.get("body") or ""))

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
                sold_at = now if available == 0 else None
                sold_at_estimated = sold_at
                sold_window_hours = 0.0 if sold_at else None
                last_active_seen = now if available else None
                first_inactive_seen = now if available == 0 else None
                text_hash = _compute_text_hash(name, body_text)
                cur = conn.execute("""
                    INSERT INTO ads
                        (urn_id, name, body_text, text_hash, classify_version, console_family,
                         model_segment, edition_class, canonical_model,
                         classify_confidence, classify_method,
                         url, image_url,
                         city, region, seller_type, published_at,
                         first_seen, last_seen, last_active_seen, first_inactive_seen,
                         last_price, last_available, ai_status,
                         sold_at, sold_at_estimated, sold_window_hours)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (urn_id, name, body_text, text_hash, None, None,
                      'unknown', 'standard', None,
                      None, None,
                      url, image_url,
                      city, region, seller, pub_at,
                      now, now, last_active_seen, first_inactive_seen,
                      price, available, 'pending',
                      sold_at, sold_at_estimated, sold_window_hours))
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
                existing_body = _normalize_body_text(str(existing["body_text"] or ""))
                effective_body = body_text or existing_body
                text_hash = _compute_text_hash(name, effective_body)

                last_active_seen = existing["last_active_seen"]
                first_inactive_seen = existing["first_inactive_seen"]
                sold_at = existing["sold_at"]
                sold_at_estimated = existing["sold_at_estimated"]
                sold_window_hours = existing["sold_window_hours"]

                if available:
                    last_active_seen = now
                    first_inactive_seen = None
                    sold_at = None
                    sold_at_estimated = None
                    sold_window_hours = None
                else:
                    if not first_inactive_seen:
                        first_inactive_seen = now
                    if not sold_at:
                        sold_at = now
                    if not sold_at_estimated:
                        sold_at_estimated, sold_window_hours = _estimate_sold_window(
                            last_active_seen=(last_active_seen or existing["last_seen"]),
                            first_inactive_seen=first_inactive_seen,
                        )

                conn.execute("""
                    UPDATE ads
                    SET last_seen      = ?,
                        body_text       = COALESCE(NULLIF(?, ''), body_text),
                        text_hash       = ?,
                        url            = ?,
                        image_url      = ?,
                        city           = COALESCE(NULLIF(?, ''), city),
                        region         = COALESCE(NULLIF(?, ''), region),
                        seller_type    = ?,
                        published_at   = COALESCE(NULLIF(?, ''), published_at),
                        last_active_seen = ?,
                        first_inactive_seen = ?,
                        last_price     = ?,
                        last_available = ?,
                        sold_at        = ?,
                        sold_at_estimated = ?,
                        sold_window_hours = ?
                    WHERE id = ?
                """, (now, body_text, text_hash, url, image_url,
                      city, region, seller, pub_at,
                      last_active_seen, first_inactive_seen,
                      price, available, sold_at, sold_at_estimated, sold_window_hours,
                      ad_id))

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

def get_all_ads(db_path: Path = DB_PATH) -> list[dict]:
    """Tutti gli annunci, ordinati per famiglia e prezzo.
    Restituisce solo i campi usati dal viewer (payload ridotto ~40%)."""
    with _connect(db_path) as conn:
        rows = conn.execute("""
            SELECT
                id, urn_id, name, console_family,
                model_segment, edition_class,
                url, city, region,
                seller_type, published_at,
                last_price, last_available,
                ai_status, ai_confidence,
                verify_status,
                sold_at_estimated, sold_window_hours
            FROM ads
            ORDER BY console_family, last_price ASC NULLS LAST
        """).fetchall()
    return [dict(r) for r in rows]

def update_ai_status(ad_id: int, status: str, db_path: Path = DB_PATH) -> None:
    """Aggiorna lo stato AI di un annuncio."""
    with _connect(db_path) as conn:
        conn.execute("UPDATE ads SET ai_status = ? WHERE id = ?", (status, ad_id))

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


def get_sold_ads(db_path: Path = DB_PATH) -> list[dict]:
    """Annunci rilevati come venduti (sold_at IS NOT NULL), più recenti prima."""
    with _connect(db_path) as conn:
        rows = conn.execute("""
            SELECT
                id, urn_id, name, console_family,
                model_segment, edition_class, canonical_model,
                url, city, region,
                seller_type, published_at,
                first_seen, sold_at,
                last_active_seen, first_inactive_seen,
                sold_at_estimated, sold_window_hours,
                last_price, ai_status
            FROM ads
            WHERE sold_at IS NOT NULL
            ORDER BY sold_at DESC
        """).fetchall()
    return [dict(r) for r in rows]


def get_sold_stats(db_path: Path = DB_PATH) -> dict:
    """Statistiche sui venduti: prezzo medio/min/max e tempo attivo per famiglia."""
    with _connect(db_path) as conn:
        g = conn.execute("""
            SELECT
                COUNT(*)                                             AS total_sold,
                AVG(CASE WHEN last_price > 0 THEN last_price END)   AS avg_price,
                MIN(CASE WHEN last_price > 0 THEN last_price END)   AS min_price,
                MAX(CASE WHEN last_price > 0 THEN last_price END)   AS max_price,
                AVG(
                    CASE
                        WHEN sold_at_estimated IS NOT NULL
                        THEN (julianday(sold_at_estimated) - julianday(COALESCE(published_at, first_seen))) * 24.0
                        WHEN sold_at IS NOT NULL
                        THEN (julianday(sold_at) - julianday(COALESCE(published_at, first_seen))) * 24.0
                    END
                )                                                    AS avg_hours_active,
                AVG(sold_window_hours)                               AS avg_sold_window_hours
            FROM ads
            WHERE sold_at IS NOT NULL
        """).fetchone()

        fam = conn.execute("""
            SELECT
                console_family,
                COUNT(*)                                                         AS count,
                AVG(CASE WHEN last_price > 0 THEN last_price END)               AS avg_price,
                MIN(CASE WHEN last_price > 0 THEN last_price END)               AS min_price,
                MAX(CASE WHEN last_price > 0 THEN last_price END)               AS max_price,
                AVG(
                    CASE
                        WHEN sold_at_estimated IS NOT NULL
                        THEN (julianday(sold_at_estimated) - julianday(COALESCE(published_at, first_seen))) * 24.0
                        WHEN sold_at IS NOT NULL
                        THEN (julianday(sold_at) - julianday(COALESCE(published_at, first_seen))) * 24.0
                    END
                )                                                                AS avg_hours_active,
                AVG(sold_window_hours)                                           AS avg_sold_window_hours
            FROM ads
            WHERE sold_at IS NOT NULL
            GROUP BY console_family
            ORDER BY count DESC
        """).fetchall()

        weekday = conn.execute("""
            SELECT
                CAST(strftime('%w', COALESCE(sold_at_estimated, sold_at)) AS INTEGER) AS weekday_idx,
                COUNT(*)                                                     AS count
            FROM ads
            WHERE sold_at IS NOT NULL
            GROUP BY weekday_idx
            ORDER BY count DESC, weekday_idx ASC
        """).fetchall()

        hour = conn.execute("""
            SELECT
                CAST(strftime('%H', COALESCE(sold_at_estimated, sold_at)) AS INTEGER) AS hour,
                COUNT(*)                                                   AS count
            FROM ads
            WHERE sold_at IS NOT NULL
            GROUP BY hour
            ORDER BY count DESC, hour ASC
        """).fetchall()

        period = conn.execute("""
            SELECT
                CASE
                    WHEN CAST(strftime('%H', COALESCE(sold_at_estimated, sold_at)) AS INTEGER) BETWEEN 0 AND 5 THEN 'night'
                    WHEN CAST(strftime('%H', COALESCE(sold_at_estimated, sold_at)) AS INTEGER) BETWEEN 6 AND 11 THEN 'morning'
                    WHEN CAST(strftime('%H', COALESCE(sold_at_estimated, sold_at)) AS INTEGER) BETWEEN 12 AND 17 THEN 'afternoon'
                    ELSE 'evening'
                END                                                        AS period,
                COUNT(*)                                                   AS count
            FROM ads
            WHERE sold_at IS NOT NULL
            GROUP BY period
            ORDER BY count DESC
        """).fetchall()

    return {
        "global":    dict(g) if g else {},
        "by_family": [dict(r) for r in fam],
        "by_weekday": [dict(r) for r in weekday],
        "by_hour": [dict(r) for r in hour],
        "by_period": [dict(r) for r in period],
    }
