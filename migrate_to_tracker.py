#!/usr/bin/env python3
"""
Migrazione una tantum: unifica trader.db + subito.db + ebay.db → tracker.db

Esegui una sola volta:
  python3 migrate_to_tracker.py

Se tracker.db esiste già con dati, lo script salta senza sovrascrivere.
I vecchi file .db rimangono come backup (puoi eliminarli manualmente dopo verifica).
"""

import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).parent
TRACKER = ROOT / "tracker.db"
OLD_DBS = {
    "trader": ROOT / "trader.db",
    "subito": ROOT / "subito.db",
    "ebay": ROOT / "ebay.db",
}


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?",
        (name,),
    ).fetchone()
    return bool(row and row[0])


def _copy_table(src_conn: sqlite3.Connection, dst_conn: sqlite3.Connection, table: str) -> int:
    """Copia tutti i record di una tabella da src a dst. Restituisce il conteggio."""
    rows = src_conn.execute(f"SELECT * FROM {table}").fetchall()  # noqa: S608
    if not rows:
        return 0

    cols = [desc[0] for desc in src_conn.execute(f"SELECT * FROM {table} LIMIT 0").description]  # noqa: S608
    placeholders = ", ".join(["?"] * len(cols))
    col_list = ", ".join(cols)

    dst_conn.executemany(
        f"INSERT OR IGNORE INTO {table}({col_list}) VALUES ({placeholders})",
        rows,
    )
    return len(rows)


def migrate():
    # Verifica che almeno uno dei vecchi DB esista
    existing = {k: v for k, v in OLD_DBS.items() if v.exists()}
    if not existing:
        print("Nessun vecchio DB trovato (trader.db, subito.db, ebay.db). Niente da migrare.")
        return

    # Se tracker.db ha già le tabelle products + ads + sold_items, saltiamo
    if TRACKER.exists():
        with sqlite3.connect(str(TRACKER)) as conn:
            has_products = _table_exists(conn, "products")
            has_ads = _table_exists(conn, "ads")
            has_sold = _table_exists(conn, "sold_items")
            if has_products and has_ads and has_sold:
                # Verifica che non siano vuote
                p = conn.execute("SELECT COUNT(*) FROM products").fetchone()[0]
                a = conn.execute("SELECT COUNT(*) FROM ads").fetchone()[0]
                s = conn.execute("SELECT COUNT(*) FROM sold_items").fetchone()[0]
                if p > 0 or a > 0 or s > 0:
                    print(f"tracker.db esiste già con dati (products={p}, ads={a}, sold_items={s}). Migrazione non necessaria.")
                    return

    # Step 1: Inizializza tracker.db con tutti gli schema
    print("Inizializzazione schema tracker.db...")
    import db
    import db_subito
    import db_ebay
    db.init_db(TRACKER)
    db_subito.init_db(TRACKER)
    db_ebay.init_db(TRACKER)
    print("  Schema creato con tutte le tabelle e migrazioni.")

    dst = sqlite3.connect(str(TRACKER))
    dst.execute("PRAGMA journal_mode=WAL")

    # Step 2: Copia dati da trader.db
    if "trader" in existing:
        print(f"\nMigrazione da {existing['trader']}...")
        src = sqlite3.connect(str(existing["trader"]))
        for table in ("categories", "storage_sizes", "products", "state_changes"):
            if _table_exists(src, table):
                n = _copy_table(src, dst, table)
                print(f"  {table}: {n} record copiati")
        # Copia le vecchie migrazioni con namespace vuoto
        if _table_exists(src, "schema_migrations"):
            old_migs = src.execute("SELECT version, name, applied_at FROM schema_migrations").fetchall()
            for v, name, applied in old_migs:
                dst.execute(
                    "INSERT OR IGNORE INTO schema_migrations(namespace, version, name, applied_at) VALUES (?, ?, ?, ?)",
                    ("products", v, name, applied),
                )
        dst.commit()
        src.close()

    # Step 3: Copia dati da subito.db
    if "subito" in existing:
        print(f"\nMigrazione da {existing['subito']}...")
        src = sqlite3.connect(str(existing["subito"]))
        for table in ("ads", "ad_changes"):
            if _table_exists(src, table):
                n = _copy_table(src, dst, table)
                print(f"  {table}: {n} record copiati")
        if _table_exists(src, "schema_migrations"):
            old_migs = src.execute("SELECT version, name, applied_at FROM schema_migrations").fetchall()
            for v, name, applied in old_migs:
                dst.execute(
                    "INSERT OR IGNORE INTO schema_migrations(namespace, version, name, applied_at) VALUES (?, ?, ?, ?)",
                    ("ads", v, name, applied),
                )
        dst.commit()
        src.close()

    # Step 4: Copia dati da ebay.db
    if "ebay" in existing:
        print(f"\nMigrazione da {existing['ebay']}...")
        src = sqlite3.connect(str(existing["ebay"]))
        for table in ("sold_items", "sold_changes"):
            if _table_exists(src, table):
                n = _copy_table(src, dst, table)
                print(f"  {table}: {n} record copiati")
        if _table_exists(src, "schema_migrations"):
            old_migs = src.execute("SELECT version, name, applied_at FROM schema_migrations").fetchall()
            for v, name, applied in old_migs:
                dst.execute(
                    "INSERT OR IGNORE INTO schema_migrations(namespace, version, name, applied_at) VALUES (?, ?, ?, ?)",
                    ("ebay", v, name, applied),
                )
        dst.commit()
        src.close()

    # Step 5: VACUUM finale
    dst.execute("VACUUM")
    dst.close()

    # Verifica
    with sqlite3.connect(str(TRACKER)) as conn:
        p = conn.execute("SELECT COUNT(*) FROM products").fetchone()[0]
        a = conn.execute("SELECT COUNT(*) FROM ads").fetchone()[0]
        s = conn.execute("SELECT COUNT(*) FROM sold_items").fetchone()[0]

    print(f"\n✅ Migrazione completata: tracker.db")
    print(f"   products:   {p} record")
    print(f"   ads:        {a} record")
    print(f"   sold_items: {s} record")
    print(f"\n   I vecchi DB (trader.db, subito.db, ebay.db) sono stati mantenuti come backup.")
    print(f"   Dopo aver verificato, puoi eliminarli manualmente.")


if __name__ == "__main__":
    migrate()
