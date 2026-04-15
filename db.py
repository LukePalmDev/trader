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
from model_rules import (
    classify_title, detect_family, standardize_title,
    extract_sub_model, extract_edition_name, extract_color_str, extract_kinect,
)

log = logging.getLogger(__name__)

DB_PATH = Path(__file__).parent / "tracker.db"

# Regex per estrarre dimensione archiviazione dal nome prodotto
_STORAGE_RE = re.compile(r'(\d[\d.]*)\s*(GB|TB)', re.IGNORECASE)

_FAMILY_TO_CATEGORY = {
    'series-x': 'Xbox Series',
    'series-s': 'Xbox Series',
    'one-x':    'Xbox One',
    'one-s':    'Xbox One',
    'one':      'Xbox One',
    '360':      'Xbox 360',
    'original': 'Xbox Original',
}

_SOURCE_PREFIX = {
    "cex": 1,
    "gamelife": 2,
    "gamepeople": 3,
    "gameshock": 4,
    "rebuy": 5,
    "jollyrogerbay": 6,
    "subito": 7,
    "ebay": 8,
}

_CONDITION_RANK = {
    "Nuovo": 0,
    "Usato": 1,
}

_GAMELIFE_BLOCKED_URLS = {
    "https://www.gamelife.it/hwxx0023-rog-ally-xbox-512gb-bianco",
    "https://www.gamelife.it/hwxx0022-rog-ally-xbox-1tb-nero",
}

_SEPARATE_DB_SOURCES = {"subito", "ebay"}

_SOURCE_CONDITION_DEFAULT = {
    "cex": "Usato",
    "rebuy": "Usato",
    "gamepeople": "Nuovo",
}

# Rebuy può esporre più varianti (grado/qualità) sullo stesso URL.
# Non deduplicare per URL, altrimenti collassa varianti distinte.
_DEDUPE_URL_SOURCES = {"cex", "gameshock", "gamepeople", "jollyrogerbay"}

_GAMESHOCK_USED_RE = re.compile(r"\busata?\b|\bused\b", re.IGNORECASE)
_REBUY_GRADE_SUFFIX_RE = re.compile(r"\[(Eccellente|Molto buono|Buono|Accettabile)\]\s*$", re.IGNORECASE)
_REBUY_CONSOLE_FAMILY_RE = re.compile(
    r"\bxbox\s*(?:series\s*[xs]|one(?:\s*[xs])?|360|original)\b",
    re.IGNORECASE,
)
_REBUY_CONSOLE_HINT_RE = re.compile(
    r"\bconsole\b|\b\d+\s*(?:gb|tb)\b|\ball-digital\b|\bkinect\b|\bedition\b|\bedizione\b",
    re.IGNORECASE,
)
_REBUY_ACCESSORY_RE = re.compile(
    r"\b(controller|gamepad|joypad|joystick|headset|cuffie|alimentatore|cavo|charger|caricatore|dock|batteria|battery)\b",
    re.IGNORECASE,
)

_CEX_PACK_NON_RE = re.compile(r"\bnon[-\s]*imballat", re.IGNORECASE)
_CEX_PACK_SCONTATA_RE = re.compile(r"\bscontat", re.IGNORECASE)
_CEX_PACK_IMBALLATA_RE = re.compile(r"\bimballat|\bimbalatt", re.IGNORECASE)


def _normalize_url(url: str | None) -> str:
    return (url or "").strip().lower().rstrip("/")


def _infer_packaging_state(source: str, name: str, grade: str = "") -> str:
    src = (source or "").strip().lower()
    if src != "cex":
        return "Imballata"

    text = f"{name or ''} {grade or ''}"
    if _CEX_PACK_NON_RE.search(text):
        return "Non Imballata"
    if _CEX_PACK_SCONTATA_RE.search(text):
        return "Scontata"
    if _CEX_PACK_IMBALLATA_RE.search(text):
        return "Imballata"
    return "Imballata"


def _rebuild_display_ids(conn: sqlite3.Connection) -> None:
    rows = conn.execute(
        "SELECT id, source, name, condition FROM products ORDER BY id"
    ).fetchall()
    by_source: dict[str, list[tuple[int, str, str, str]]] = {}
    for row in rows:
        rid = int(row[0])
        source = str(row[1] or "").strip().lower()
        name = str(row[2] or "").strip()
        condition = str(row[3] or "").strip()
        by_source.setdefault(source, []).append((rid, source, name, condition))

    updates: list[tuple[int, int]] = []

    for source, source_rows in sorted(by_source.items(), key=lambda x: x[0]):
        prefix = _SOURCE_PREFIX.get(source, 9)
        ordered_rows: list[tuple[int, str, str, str]] = []

        if source == "gamelife":
            grouped: dict[str, list[tuple[int, str, str, str]]] = {}
            for row in source_rows:
                key = row[2].lower().strip()
                grouped.setdefault(key, []).append(row)

            for group_key in sorted(grouped.keys()):
                bucket = grouped[group_key]
                bucket.sort(
                    key=lambda r: (
                        _CONDITION_RANK.get(r[3], 9),
                        r[3].lower(),
                        r[0],
                    )
                )
                ordered_rows.extend(bucket)
        else:
            ordered_rows = sorted(
                source_rows,
                key=lambda r: (
                    r[2].lower(),
                    _CONDITION_RANK.get(r[3], 9),
                    r[3].lower(),
                    r[0],
                ),
            )

        for seq, row in enumerate(ordered_rows, start=1):
            rid = row[0]
            display_id = (prefix * 1000) + seq
            updates.append((display_id, rid))

    if updates:
        # Evita collisioni transitorie sull'indice UNIQUE durante la riscrittura completa.
        conn.execute("UPDATE products SET display_id = NULL")
        conn.executemany(
            "UPDATE products SET display_id = ? WHERE id = ?",
            updates,
        )


def _table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {str(row[1]) for row in rows}


def _add_column_if_missing(conn: sqlite3.Connection, table_name: str, definition: str) -> None:
    column_name = definition.split()[0]
    if column_name not in _table_columns(conn, table_name):
        conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {definition}")


def _migration_v3_segment_models(conn: sqlite3.Connection) -> None:
    _add_column_if_missing(conn, "products", "model_segment TEXT NOT NULL DEFAULT 'unknown'")
    _add_column_if_missing(conn, "products", "edition_class TEXT NOT NULL DEFAULT 'standard'")
    _add_column_if_missing(conn, "products", "canonical_model TEXT")
    _add_column_if_missing(conn, "products", "classify_confidence REAL")
    _add_column_if_missing(conn, "products", "classify_method TEXT")
    _add_column_if_missing(conn, "products", "is_base_model_auto INTEGER NOT NULL DEFAULT 0")

    rows = conn.execute(
        "SELECT id, name, console_family FROM products"
    ).fetchall()
    payload = []
    for row in rows:
        row_id = int(row[0])
        name = str(row[1] or "")
        family_hint = row[2]
        classified = classify_title(name, family_hint=family_hint)
        is_base_auto = int(
            classified.model_segment == "base" and classified.edition_class == "standard"
        )
        payload.append(
            (
                classified.console_family,
                classified.model_segment,
                classified.edition_class,
                classified.canonical_model,
                classified.classify_confidence,
                classified.classify_method,
                is_base_auto,
                row_id,
            )
        )

    if payload:
        conn.executemany(
            """
            UPDATE products
            SET console_family = ?,
                model_segment = ?,
                edition_class = ?,
                canonical_model = ?,
                classify_confidence = ?,
                classify_method = ?,
                is_base_model_auto = ?
            WHERE id = ?
            """,
            payload,
        )

    conn.execute("CREATE INDEX IF NOT EXISTS idx_products_segment ON products(model_segment)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_products_canonical ON products(canonical_model)")


def _migration_v4_display_ids(conn: sqlite3.Connection) -> None:
    _add_column_if_missing(conn, "products", "display_id INTEGER")
    _rebuild_display_ids(conn)
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_products_display_id ON products(display_id)")


def _migration_v5_drop_gamelife_blocked_urls(conn: sqlite3.Connection) -> None:
    rows = conn.execute(
        "SELECT id, url FROM products WHERE source = 'gamelife'"
    ).fetchall()
    blocked_ids = [
        int(row[0])
        for row in rows
        if _normalize_url(row[1]) in _GAMELIFE_BLOCKED_URLS
    ]
    if blocked_ids:
        placeholders = ",".join("?" for _ in blocked_ids)
        conn.execute(
            f"DELETE FROM state_changes WHERE product_id IN ({placeholders})",
            blocked_ids,
        )
        conn.execute(
            f"DELETE FROM products WHERE id IN ({placeholders})",
            blocked_ids,
        )
    _rebuild_display_ids(conn)


def _merge_products(conn: sqlite3.Connection, keep_id: int, drop_id: int) -> None:
    if keep_id == drop_id:
        return

    keep = conn.execute("SELECT * FROM products WHERE id = ?", (keep_id,)).fetchone()
    drop = conn.execute("SELECT * FROM products WHERE id = ?", (drop_id,)).fetchone()
    if not keep or not drop:
        return

    latest = keep
    if (drop["last_seen"] or "") > (keep["last_seen"] or ""):
        latest = drop

    first_seen_values = [v for v in (keep["first_seen"], drop["first_seen"]) if v]
    first_seen = min(first_seen_values) if first_seen_values else (latest["first_seen"] or "")
    last_seen = max(keep["last_seen"] or "", drop["last_seen"] or "")

    def _coalesce_latest(field: str):
        return latest[field] if latest[field] is not None else keep[field]

    conn.execute(
        """
        UPDATE products
        SET sku = COALESCE(?, sku),
            category_id = COALESCE(?, category_id),
            storage_id = COALESCE(?, storage_id),
            console_family = COALESCE(?, console_family),
            model_segment = COALESCE(?, model_segment),
            edition_class = COALESCE(?, edition_class),
            canonical_model = COALESCE(?, canonical_model),
            classify_confidence = COALESCE(?, classify_confidence),
            classify_method = COALESCE(?, classify_method),
            standard_name = COALESCE(?, standard_name),
            standard_key = COALESCE(?, standard_key),
            packaging_state = COALESCE(?, packaging_state),
            url = COALESCE(?, url),
            image_url = COALESCE(?, image_url),
            is_base_model = ?,
            is_base_model_auto = ?,
            first_seen = ?,
            last_seen = ?,
            last_price = ?,
            last_available = ?
        WHERE id = ?
        """,
        (
            _coalesce_latest("sku"),
            _coalesce_latest("category_id"),
            _coalesce_latest("storage_id"),
            _coalesce_latest("console_family"),
            _coalesce_latest("model_segment"),
            _coalesce_latest("edition_class"),
            _coalesce_latest("canonical_model"),
            _coalesce_latest("classify_confidence"),
            _coalesce_latest("classify_method"),
            _coalesce_latest("standard_name"),
            _coalesce_latest("standard_key"),
            _coalesce_latest("packaging_state"),
            _coalesce_latest("url"),
            _coalesce_latest("image_url"),
            1 if (keep["is_base_model"] or drop["is_base_model"]) else 0,
            1 if (keep["is_base_model_auto"] or drop["is_base_model_auto"]) else 0,
            first_seen,
            last_seen,
            _coalesce_latest("last_price"),
            _coalesce_latest("last_available"),
            keep_id,
        ),
    )

    conn.execute(
        "UPDATE state_changes SET product_id = ? WHERE product_id = ?",
        (keep_id, drop_id),
    )
    conn.execute("DELETE FROM products WHERE id = ?", (drop_id,))


def _delete_products_by_ids(conn: sqlite3.Connection, ids: list[int]) -> int:
    if not ids:
        return 0
    placeholders = ",".join("?" for _ in ids)
    conn.execute(
        f"DELETE FROM state_changes WHERE product_id IN ({placeholders})",
        ids,
    )
    conn.execute(
        f"DELETE FROM products WHERE id IN ({placeholders})",
        ids,
    )
    return len(ids)


def _dedupe_source_by_url(conn: sqlite3.Connection, source: str) -> int:
    rows = conn.execute(
        "SELECT * FROM products WHERE source = ? ORDER BY id",
        (source,),
    ).fetchall()
    by_url: dict[str, list[sqlite3.Row]] = {}
    for row in rows:
        key = _normalize_url(row["url"])
        if not key:
            continue
        by_url.setdefault(key, []).append(row)

    merged = 0
    for _, group in by_url.items():
        if len(group) <= 1:
            continue

        keep = max(
            group,
            key=lambda r: (
                len(str(r["name"] or "")),
                str(r["last_seen"] or ""),
                int(r["id"]),
            ),
        )
        keep_id = int(keep["id"])
        for row in group:
            rid = int(row["id"])
            if rid == keep_id:
                continue
            _merge_products(conn, keep_id, rid)
            merged += 1
    return merged


def _normalize_source_conditions(conn: sqlite3.Connection) -> int:
    changed = 0
    for source, desired in _SOURCE_CONDITION_DEFAULT.items():
        rows = conn.execute(
            "SELECT * FROM products WHERE source = ? AND condition != ? ORDER BY id",
            (source, desired),
        ).fetchall()
        for row in rows:
            rid = int(row["id"])
            existing = conn.execute(
                """
                SELECT id FROM products
                WHERE source = ?
                  AND name = ?
                  AND condition = ?
                  AND id != ?
                ORDER BY id
                LIMIT 1
                """,
                (source, row["name"], desired, rid),
            ).fetchone()
            if existing:
                _merge_products(conn, int(existing["id"]), rid)
            else:
                conn.execute(
                    "UPDATE products SET condition = ? WHERE id = ?",
                    (desired, rid),
                )
            changed += 1
    return changed


def _migration_v6_gameshock_cleanup(conn: sqlite3.Connection) -> None:
    conn.row_factory = sqlite3.Row
    _normalize_gameshock_records(conn)


def _migration_v7_gameshock_condition_rules(conn: sqlite3.Connection) -> None:
    conn.row_factory = sqlite3.Row
    _normalize_gameshock_records(conn)


def _migration_v8_rebuy_variants_cleanup(conn: sqlite3.Connection) -> None:
    conn.row_factory = sqlite3.Row
    _normalize_rebuy_records(conn)


def _migration_v9_standardized_names(conn: sqlite3.Connection) -> None:
    conn.row_factory = sqlite3.Row
    _add_column_if_missing(conn, "products", "standard_name TEXT")
    _add_column_if_missing(conn, "products", "standard_key TEXT")

    rows = conn.execute(
        "SELECT id, name, console_family FROM products"
    ).fetchall()
    payload: list[tuple[str, str, int]] = []
    for row in rows:
        pid = int(row["id"])
        name = str(row["name"] or "")
        family_hint = row["console_family"]
        classified = classify_title(name, family_hint=family_hint)
        standardized = standardize_title(
            name,
            classification=classified,
            family_hint=family_hint,
        )
        payload.append((standardized.standard_name, standardized.standard_key, pid))

    if payload:
        conn.executemany(
            "UPDATE products SET standard_name = ?, standard_key = ? WHERE id = ?",
            payload,
        )

    conn.execute("CREATE INDEX IF NOT EXISTS idx_products_standard_key ON products(standard_key)")


def _reclassify_products(conn: sqlite3.Connection) -> int:
    rows = conn.execute(
        "SELECT id, source, name FROM products ORDER BY id"
    ).fetchall()
    payload: list[tuple[str, str, str, str, float, str, str, str, str, int, int]] = []
    for row in rows:
        pid = int(row["id"])
        source = str(row["source"] or "")
        name = str(row["name"] or "")

        classified = classify_title(name, family_hint=None)
        standardized = standardize_title(name, classification=classified, family_hint=None)
        packaging_state = _infer_packaging_state(source, name)
        is_base_auto = int(
            classified.model_segment == "base" and classified.edition_class == "standard"
        )

        payload.append(
            (
                classified.console_family,
                classified.model_segment,
                classified.edition_class,
                classified.canonical_model,
                classified.classify_confidence,
                classified.classify_method,
                standardized.standard_name,
                standardized.standard_key,
                packaging_state,
                is_base_auto,
                pid,
            )
        )

    if payload:
        conn.executemany(
            """
            UPDATE products
            SET console_family = ?,
                model_segment = ?,
                edition_class = ?,
                canonical_model = ?,
                classify_confidence = ?,
                classify_method = ?,
                standard_name = ?,
                standard_key = ?,
                packaging_state = ?,
                is_base_model_auto = ?
            WHERE id = ?
            """,
            payload,
        )
    return len(payload)


def _migration_v10_packaging_and_reclassify(conn: sqlite3.Connection) -> None:
    conn.row_factory = sqlite3.Row
    _add_column_if_missing(conn, "products", "standard_name TEXT")
    _add_column_if_missing(conn, "products", "standard_key TEXT")
    _add_column_if_missing(conn, "products", "packaging_state TEXT")
    _reclassify_products(conn)


def _migration_v11_search_attributes(conn: sqlite3.Connection) -> None:
    """Aggiunge e popola i campi strutturati per la tab Ricerca."""
    conn.row_factory = sqlite3.Row
    _add_column_if_missing(conn, "products", "sub_model TEXT")
    _add_column_if_missing(conn, "products", "edition_name TEXT")
    _add_column_if_missing(conn, "products", "color TEXT")
    _add_column_if_missing(conn, "products", "has_kinect INTEGER")

    rows = conn.execute("SELECT id, name, console_family FROM products").fetchall()
    payload = []
    for row in rows:
        name   = str(row["name"] or "")
        family = str(row["console_family"] or "other")
        payload.append((
            extract_sub_model(name, family),
            extract_edition_name(name),
            extract_color_str(name),
            extract_kinect(name),
            int(row["id"]),
        ))
    if payload:
        conn.executemany(
            "UPDATE products SET sub_model=?, edition_name=?, color=?, has_kinect=? WHERE id=?",
            payload,
        )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_products_sub_model ON products(sub_model)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_products_edition_name ON products(edition_name)")
    # Riclassifica anche standard_name/standard_key per applicare le nuove regole (es. Elite 360)
    _reclassify_products(conn)


def _normalize_gameshock_records(conn: sqlite3.Connection) -> int:
    merged = 0

    # 1) dedupe righe Gameshock per URL normalizzato.
    rows = conn.execute(
        "SELECT * FROM products WHERE source = 'gameshock' ORDER BY id"
    ).fetchall()
    by_url: dict[str, list[sqlite3.Row]] = {}
    for row in rows:
        key = _normalize_url(row["url"])
        if not key:
            continue
        by_url.setdefault(key, []).append(row)

    for _, group in by_url.items():
        if len(group) <= 1:
            continue

        # Preferisci nome più completo, poi record più recente.
        keep = max(
            group,
            key=lambda r: (
                len(str(r["name"] or "")),
                str(r["last_seen"] or ""),
                int(r["id"]),
            ),
        )
        keep_id = int(keep["id"])

        for row in group:
            rid = int(row["id"])
            if rid == keep_id:
                continue
            _merge_products(conn, keep_id, rid)
            merged += 1

    # 2) riallinea condizione: Usato solo con marker espliciti, altrimenti Nuovo.
    rows = conn.execute(
        "SELECT * FROM products WHERE source = 'gameshock' ORDER BY id"
    ).fetchall()
    for row in rows:
        rid = int(row["id"])
        name = str(row["name"] or "")
        url = str(row["url"] or "")
        desired = "Usato" if (_GAMESHOCK_USED_RE.search(name) or _GAMESHOCK_USED_RE.search(url)) else "Nuovo"

        if row["condition"] == desired:
            continue

        same_used = conn.execute(
            """
            SELECT id FROM products
            WHERE source = 'gameshock'
              AND name = ?
              AND condition = ?
              AND id != ?
            ORDER BY id
            LIMIT 1
            """,
            (row["name"], desired, rid),
        ).fetchone()
        if same_used:
            _merge_products(conn, int(same_used["id"]), rid)
            merged += 1
        else:
            conn.execute(
                "UPDATE products SET condition = ? WHERE id = ?",
                (desired, rid),
            )

    _rebuild_display_ids(conn)
    return merged


def _is_rebuy_console_name(name: str) -> bool:
    if not _REBUY_CONSOLE_FAMILY_RE.search(name):
        return False
    has_hint = bool(_REBUY_CONSOLE_HINT_RE.search(name))
    has_accessory = bool(_REBUY_ACCESSORY_RE.search(name))
    if has_hint:
        return True
    if has_accessory:
        return False
    return False


def _normalize_rebuy_records(conn: sqlite3.Connection) -> int:
    """Riallinea Rebuy quando esistono vecchi record 1:1 URL + nuove varianti per grade."""
    rows = conn.execute(
        "SELECT * FROM products WHERE source = 'rebuy' ORDER BY id"
    ).fetchall()
    dropped = _delete_products_by_ids(
        conn,
        [
            int(row["id"])
            for row in rows
            if not _is_rebuy_console_name(str(row["name"] or ""))
        ],
    )

    rows = conn.execute(
        "SELECT * FROM products WHERE source = 'rebuy' ORDER BY id"
    ).fetchall()
    by_url: dict[str, list[sqlite3.Row]] = {}
    for row in rows:
        key = _normalize_url(row["url"])
        if not key:
            continue
        by_url.setdefault(key, []).append(row)

    merged = 0
    for _, group in by_url.items():
        if len(group) < 2:
            continue

        variants = [
            row for row in group
            if _REBUY_GRADE_SUFFIX_RE.search(str(row["name"] or ""))
        ]
        legacy = [
            row for row in group
            if not _REBUY_GRADE_SUFFIX_RE.search(str(row["name"] or ""))
        ]
        if not variants or not legacy:
            continue

        for old in legacy:
            old_price = old["last_price"]
            candidates = [r for r in variants if r["last_price"] is not None and old_price is not None]
            if candidates:
                keep = min(
                    candidates,
                    key=lambda r: abs(float(r["last_price"]) - float(old_price)),
                )
            else:
                keep = variants[0]
            _merge_products(conn, int(keep["id"]), int(old["id"]))
            merged += 1

    if dropped or merged:
        _rebuild_display_ids(conn)
    return dropped + merged


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
    Migration(
        3,
        "segment-models",
        callback=_migration_v3_segment_models,
    ),
    Migration(
        4,
        "display-ids",
        callback=_migration_v4_display_ids,
    ),
    Migration(
        5,
        "drop-gamelife-blocked-urls",
        callback=_migration_v5_drop_gamelife_blocked_urls,
    ),
    Migration(
        6,
        "gameshock-cleanup-usato",
        callback=_migration_v6_gameshock_cleanup,
    ),
    Migration(
        7,
        "gameshock-condition-used-marker",
        callback=_migration_v7_gameshock_condition_rules,
    ),
    Migration(
        8,
        "rebuy-variants-cleanup",
        callback=_migration_v8_rebuy_variants_cleanup,
    ),
    Migration(
        9,
        "standardized-names",
        callback=_migration_v9_standardized_names,
    ),
    Migration(
        10,
        "packaging-and-reclassify",
        callback=_migration_v10_packaging_and_reclassify,
    ),
    Migration(
        11,
        "search-attributes",
        callback=_migration_v11_search_attributes,
    ),
    Migration(
        12,
        "add-check-triggers",
        (
            """
            CREATE TRIGGER IF NOT EXISTS chk_products_last_price_insert
            BEFORE INSERT ON products
            WHEN NEW.last_price < 0
            BEGIN
                SELECT RAISE(ABORT, 'last_price must be >= 0');
            END;
            """,
            """
            CREATE TRIGGER IF NOT EXISTS chk_products_last_price_update
            BEFORE UPDATE ON products
            WHEN NEW.last_price < 0
            BEGIN
                SELECT RAISE(ABORT, 'last_price must be >= 0');
            END;
            """,
            """
            CREATE TRIGGER IF NOT EXISTS chk_products_last_available_insert
            BEFORE INSERT ON products
            WHEN NEW.last_available NOT IN (0, 1)
            BEGIN
                SELECT RAISE(ABORT, 'last_available must be 0 or 1');
            END;
            """,
            """
            CREATE TRIGGER IF NOT EXISTS chk_products_last_available_update
            BEFORE UPDATE ON products
            WHEN NEW.last_available NOT IN (0, 1)
            BEGIN
                SELECT RAISE(ABORT, 'last_available must be 0 or 1');
            END;
            """,
        ),
    ),
)


# ---------------------------------------------------------------------------
# Helpers interni
# ---------------------------------------------------------------------------

def _detect_family(name: str) -> str:
    return detect_family(name)


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
    conn = sqlite3.connect(str(db_path), timeout=30.0)
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
            display_id     INTEGER,
            name           TEXT    NOT NULL,
            standard_name  TEXT,
            standard_key   TEXT,
            source         TEXT    NOT NULL,
            condition      TEXT    NOT NULL,
            sku            TEXT,
            category_id    INTEGER REFERENCES categories(id),
            storage_id     INTEGER REFERENCES storage_sizes(id),
            console_family TEXT,
            url            TEXT,
            image_url      TEXT,
            is_base_model  INTEGER NOT NULL DEFAULT 0,
            is_base_model_auto INTEGER NOT NULL DEFAULT 0,
            model_segment  TEXT    NOT NULL DEFAULT 'unknown',
            edition_class  TEXT    NOT NULL DEFAULT 'standard',
            canonical_model TEXT,
            classify_confidence REAL,
            classify_method TEXT,
            packaging_state TEXT,
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
    applied = run_migrations(db_path, _MIGRATIONS, namespace="products")
    if applied:
        log.info("Migrazioni products applicate: %s", applied)
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
    sources_seen = set()
    conn = _connect(db_path)
    conn.isolation_level = None
    conn.execute("BEGIN IMMEDIATE")
    try:
        for p in products:
            name      = (p.get("name") or "").strip()
            source    = (p.get("source") or "").strip()
            condition = (p.get("condition") or "N/D").strip()
            price     = p.get("price")
            available = int(bool(p.get("available", True)))
            sku       = p.get("sku") or None
            grade     = (p.get("grade") or "").strip()
            url       = p.get("url") or ""
            image_url = p.get("image_url") or ""

            if not name or not source:
                continue
            sources_seen.add(source.lower())

            initial_family = _detect_family(name)
            classified   = classify_title(name, family_hint=initial_family)
            family       = classified.console_family
            model_segment = classified.model_segment
            edition_class = classified.edition_class
            canonical_model = classified.canonical_model
            classify_confidence = classified.classify_confidence
            classify_method = classified.classify_method
            standardized = standardize_title(
                name,
                classification=classified,
                family_hint=initial_family,
            )
            standard_name = standardized.standard_name
            standard_key = standardized.standard_key
            sub_model     = extract_sub_model(name, family)
            edition_name  = extract_edition_name(name)
            color         = extract_color_str(name)
            has_kinect    = extract_kinect(name)
            packaging_state = _infer_packaging_state(source, name, grade)
            is_base_auto = int(model_segment == "base" and edition_class == "standard")
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
                         console_family, model_segment, edition_class, canonical_model,
                         classify_confidence, classify_method, standard_name, standard_key,
                         sub_model, edition_name, color, has_kinect,
                         packaging_state, is_base_model_auto,
                         url, image_url,
                         first_seen, last_seen, last_price, last_available)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (name, source, condition, sku, category_id, storage_id,
                      family, model_segment, edition_class, canonical_model,
                      classify_confidence, classify_method, standard_name, standard_key,
                      sub_model, edition_name, color, has_kinect,
                      packaging_state, is_base_auto,
                      url, image_url,
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
                        model_segment  = ?,
                        edition_class  = ?,
                        canonical_model = ?,
                        classify_confidence = ?,
                        classify_method = ?,
                        standard_name = ?,
                        standard_key = ?,
                        sub_model = ?,
                        edition_name = ?,
                        color = ?,
                        has_kinect = ?,
                        packaging_state = ?,
                        is_base_model_auto = ?,
                        last_price     = ?,
                        last_available = ?
                    WHERE id = ?
                """, (now, url, image_url, sku, category_id, storage_id,
                      family, model_segment, edition_class, canonical_model,
                      classify_confidence, classify_method, standard_name, standard_key,
                      sub_model, edition_name, color, has_kinect,
                      packaging_state,
                      is_base_auto,
                      price, available, pid))

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

        if "gameshock" in sources_seen:
            _normalize_gameshock_records(conn)
        if "rebuy" in sources_seen:
            _normalize_rebuy_records(conn)
        _rebuild_display_ids(conn)

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    return stats


# ---------------------------------------------------------------------------
# Manutenzione / pulizia DB
# ---------------------------------------------------------------------------

def clean_db(db_path: Path = DB_PATH) -> dict[str, int]:
    """Normalizza e pulisce il DB principale prodotti.

    Operazioni:
      - rimuove record legacy di fonti con DB separato (subito/ebay)
      - rimuove record invalidi (name/source/condition vuoti)
      - normalizza condition per fonti note (es. CEX/Rebuy = Usato)
      - deduplica per URL normalizzato nelle fonti previste
      - applica pulizia specifica GameShock (Usato solo con marker espliciti)
      - elimina state_changes orfane
      - rigenera display_id
    """
    summary = {
        "removed_separate_db_sources": 0,
        "removed_invalid_rows": 0,
        "conditions_normalized": 0,
        "url_merged_rows": 0,
        "orphan_changes_removed": 0,
    }

    conn = _connect(db_path)
    conn.isolation_level = None
    conn.execute("BEGIN IMMEDIATE")
    try:
        conn.row_factory = sqlite3.Row

        # 1) Rimuove residui legacy di fonti gestite in DB dedicati.
        src_rows = conn.execute(
            "SELECT id FROM products WHERE source IN (?, ?)",
            tuple(sorted(_SEPARATE_DB_SOURCES)),
        ).fetchall()
        summary["removed_separate_db_sources"] = _delete_products_by_ids(
            conn, [int(r["id"]) for r in src_rows]
        )

        # 2) Rimuove record invalidi.
        bad_rows = conn.execute(
            """
            SELECT id FROM products
            WHERE name IS NULL OR TRIM(name) = ''
               OR source IS NULL OR TRIM(source) = ''
               OR condition IS NULL OR TRIM(condition) = ''
            """
        ).fetchall()
        summary["removed_invalid_rows"] = _delete_products_by_ids(
            conn, [int(r["id"]) for r in bad_rows]
        )

        # 3) Normalizza condizioni per fonte.
        summary["conditions_normalized"] = _normalize_source_conditions(conn)

        # 3b) Ricalcola classificazione/nomi standard/imballo con regole correnti.
        _reclassify_products(conn)

        # 4) Deduplica URL (fonti dove l'URL identifica il prodotto).
        merged = 0
        for source in sorted(_DEDUPE_URL_SOURCES):
            merged += _dedupe_source_by_url(conn, source)
        summary["url_merged_rows"] = merged

        # 5) Pulizia specifica Gameshock.
        summary["url_merged_rows"] += _normalize_gameshock_records(conn)

        # 5b) Pulizia specifica Rebuy (legacy -> varianti).
        summary["url_merged_rows"] += _normalize_rebuy_records(conn)

        # 6) Elimina cambi orfani.
        before = conn.execute("SELECT COUNT(*) FROM state_changes").fetchone()[0]
        conn.execute(
            """
            DELETE FROM state_changes
            WHERE product_id NOT IN (SELECT id FROM products)
            """
        )
        after = conn.execute("SELECT COUNT(*) FROM state_changes").fetchone()[0]
        summary["orphan_changes_removed"] = int(before - after)

        _rebuild_display_ids(conn)

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    return summary


# ---------------------------------------------------------------------------
# Lettura dati (usata dalle API del viewer)
# ---------------------------------------------------------------------------

def get_all_products(db_path: Path = DB_PATH) -> list[dict]:
    """Tutti i prodotti con metadati categoria e storage."""
    with _connect(db_path) as conn:
        rows = conn.execute("""
            SELECT
                p.id, p.display_id, p.name, p.source, p.condition, p.sku,
                p.console_family, p.url, p.image_url,
                p.is_base_model,
                p.is_base_model_auto, p.model_segment, p.edition_class,
                p.canonical_model, p.classify_confidence, p.classify_method,
                p.standard_name, p.standard_key,
                p.packaging_state,
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
                p.id, p.display_id, p.name, p.source, p.condition, p.sku,
                p.console_family, p.url, p.image_url,
                p.is_base_model, p.is_base_model_auto,
                p.model_segment, p.edition_class, p.canonical_model,
                p.classify_confidence, p.classify_method,
                p.standard_name, p.standard_key,
                p.packaging_state,
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


def get_standard_groups(db_path: Path = DB_PATH) -> list[dict]:
    """Raggruppa i prodotti per nome standard con elenco dei nomi originali."""
    with _connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT
                id, source, name, condition,
                standard_name, standard_key, packaging_state
            FROM products
            ORDER BY source, name, condition, id
            """
        ).fetchall()

    groups: dict[str, dict] = {}
    for row in rows:
        standard_name = str(row["standard_name"] or row["name"] or "").strip() or "N/D"
        standard_key = str(row["standard_key"] or "").strip() or f"name:{standard_name.lower()}"

        group = groups.get(standard_key)
        if group is None:
            group = {
                "standard_name": standard_name,
                "standard_key": standard_key,
                "total_products": 0,
                "sources": set(),
                "packaging_states": set(),
                "conditions": set(),
                "items": [],
                "_seen": set(),
            }
            groups[standard_key] = group

        source = str(row["source"] or "").strip()
        packaging = str(row["packaging_state"] or "Imballata").strip() or "Imballata"
        condition = str(row["condition"] or "").strip()
        original_name = str(row["name"] or "").strip()

        group["total_products"] += 1
        if source:
            group["sources"].add(source)
        if packaging:
            group["packaging_states"].add(packaging)
        if condition:
            group["conditions"].add(condition)

        fingerprint = (source, original_name, packaging, condition)
        if fingerprint not in group["_seen"]:
            group["_seen"].add(fingerprint)
            group["items"].append(
                {
                    "source": source,
                    "name": original_name,
                    "packaging_state": packaging,
                    "condition": condition,
                }
            )

    out: list[dict] = []
    for group in groups.values():
        items = sorted(
            group["items"],
            key=lambda x: (x["source"], x["name"], x["condition"]),
        )
        out.append(
            {
                "standard_name": group["standard_name"],
                "standard_key": group["standard_key"],
                "total_products": group["total_products"],
                "sources": sorted(group["sources"]),
                "packaging_states": sorted(group["packaging_states"]),
                "conditions": sorted(group["conditions"]),
                "items": items,
            }
        )

    out.sort(key=lambda x: (x["standard_name"].lower(), -x["total_products"]))
    return out


def search_products(
    base_family: str | None = None,
    sub_model: str | None = None,
    edition_name: str | None = None,
    color: str | None = None,
    storage_label: str | None = None,
    has_kinect: int | None = None,
    available_only: bool = False,
    db_path: Path = DB_PATH,
) -> list[dict]:
    """Ricerca prodotti per attributi strutturati. Ritorna righe per store."""
    # Mappa base_family → lista console_family
    _family_map: dict[str, list[str]] = {
        "Original": ["original"],
        "360":      ["360"],
        "One":      ["one", "one-s", "one-x"],
        "Series":   ["series-x", "series-s"],
    }

    conditions: list[str] = []
    params: list[object] = []

    if base_family and base_family in _family_map:
        placeholders = ",".join("?" * len(_family_map[base_family]))
        conditions.append(f"p.console_family IN ({placeholders})")
        params.extend(_family_map[base_family])

    if sub_model:
        conditions.append("p.sub_model = ?")
        params.append(sub_model)

    if edition_name:
        conditions.append("p.edition_name = ?")
        params.append(edition_name)

    if color:
        conditions.append("(p.color LIKE ? OR p.color LIKE ? OR p.color LIKE ?)")
        params.extend([color, f"{color}/%", f"%/{color}"])

    if storage_label:
        conditions.append("s.label = ?")
        params.append(storage_label)

    if has_kinect is not None:
        conditions.append("p.has_kinect = ?")
        params.append(has_kinect)

    if available_only:
        conditions.append("p.last_available = 1")

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

    sql = f"""
        SELECT
            p.id, p.display_id, p.name, p.source, p.condition,
            p.console_family, p.sub_model, p.edition_name, p.color,
            p.has_kinect, p.standard_name, p.standard_key,
            p.canonical_model, p.packaging_state,
            p.last_price, p.last_available,
            p.url, p.image_url,
            s.label AS storage_label
        FROM products p
        LEFT JOIN storage_sizes s ON s.id = p.storage_id
        {where}
        ORDER BY p.standard_name, p.last_price
    """
    with _connect(db_path) as conn:
        rows = conn.execute(sql, params).fetchall()
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

def get_price_history(db_path: Path = DB_PATH) -> list[dict]:
    """Recupera l'intero storico limitato a id, date e prezzo per i grafici Javascript (Stat 3, 4, 5)."""
    with _connect(db_path) as conn:
        rows = conn.execute("""
            SELECT
                sc.product_id, sc.changed_at, sc.price_new, sc.available_new
            FROM state_changes sc
            WHERE sc.change_type IN ('new', 'price', 'availability')
            ORDER BY sc.changed_at ASC
        """).fetchall()
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
