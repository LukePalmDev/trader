#!/usr/bin/env python3
"""Merge additivo di un DB locale dentro il DB del server.

Inserisce SOLO le righe mancanti (per chiave naturale) nelle tabelle padre
(ads, sold_items, products) e, per le sole righe nuove, migra anche lo storico
delle tabelle figlie (*_changes) rimappando la foreign key interna.

Non sovrascrive MAI righe esistenti sul server: le righe già presenti (stessa
chiave naturale) restano quelle del server, considerate più aggiornate.

Uso:
    python3 merge_local_into_server.py SERVER_DB INCOMING_LOCAL_DB [--apply]

Senza --apply esegue un dry-run (rollback finale) mostrando i conteggi.
"""
from __future__ import annotations

import argparse
import sqlite3
import sys

# (tabella_padre, chiavi_naturali, tabella_figlia, fk_figlia_verso_padre.id)
GROUPS = [
    ("ads", ["urn_id"], "ad_changes", "ad_id"),
    ("sold_items", ["item_id"], "sold_changes", "item_id"),
    ("products", ["source", "name", "condition"], "state_changes", "product_id"),
]


def _columns(con: sqlite3.Connection, schema: str, table: str) -> list[str]:
    return [r[1] for r in con.execute(f"PRAGMA {schema}.table_info({table})")]


def _common_columns(con: sqlite3.Connection, table: str) -> list[str]:
    """Colonne presenti sia su main (server) sia su loc (incoming), esclusa 'id'."""
    main_cols = _columns(con, "main", table)
    loc_cols = set(_columns(con, "loc", table))
    return [c for c in main_cols if c in loc_cols and c != "id"]


def merge(server_db: str, incoming_db: str, apply: bool) -> int:
    con = sqlite3.connect(server_db, timeout=60)
    con.execute("PRAGMA busy_timeout = 60000")
    con.execute("PRAGMA foreign_keys = OFF")
    con.execute("ATTACH DATABASE ? AS loc", (incoming_db,))
    con.execute("BEGIN IMMEDIATE")
    totals: dict[str, dict[str, int]] = {}
    try:
        for parent, natkeys, child, child_fk in GROUPS:
            pcols = _common_columns(con, parent)
            key_expr = ", ".join(natkeys)

            # Chiavi naturali già presenti sul server.
            existing = {
                tuple(r) for r in con.execute(
                    f"SELECT {key_expr} FROM main.{parent}"
                )
            }
            # Righe locali candidate.
            loc_rows = con.execute(
                f"SELECT id, {key_expr}, {', '.join(pcols)} FROM loc.{parent}"
            ).fetchall()

            id_map: dict[int, int] = {}
            inserted = 0
            placeholders = ", ".join(["?"] * len(pcols))
            insert_sql = (
                f"INSERT INTO main.{parent} ({', '.join(pcols)}) "
                f"VALUES ({placeholders})"
            )
            nk = len(natkeys)
            for row in loc_rows:
                loc_id = row[0]
                key = tuple(row[1 : 1 + nk])
                if key in existing:
                    continue
                values = row[1 + nk :]
                cur = con.execute(insert_sql, values)
                id_map[loc_id] = cur.lastrowid
                existing.add(key)  # evita duplicati intra-batch
                inserted += 1

            # Storico figlio: solo per i padri appena inseriti (FK rimappata).
            ccols = [c for c in _common_columns(con, child) if c != child_fk]
            child_inserted = 0
            if id_map and ccols:
                cph = ", ".join(["?"] * (len(ccols) + 1))
                cinsert = (
                    f"INSERT INTO main.{child} ({child_fk}, {', '.join(ccols)}) "
                    f"VALUES ({cph})"
                )
                child_rows = con.execute(
                    f"SELECT {child_fk}, {', '.join(ccols)} FROM loc.{child}"
                ).fetchall()
                for crow in child_rows:
                    old_fk = crow[0]
                    if old_fk not in id_map:
                        continue
                    con.execute(cinsert, (id_map[old_fk], *crow[1:]))
                    child_inserted += 1

            totals[parent] = {
                "parent_inserted": inserted,
                "child_inserted": child_inserted,
            }

        # Conteggi finali (entro la transazione).
        finals = {
            t: con.execute(f"SELECT COUNT(*) FROM main.{t}").fetchone()[0]
            for t, *_ in GROUPS
        }

        if apply:
            con.commit()
            mode = "APPLICATO"
        else:
            con.rollback()
            mode = "DRY-RUN (nessuna modifica scritta)"

        print(f"=== Merge {mode} ===")
        for parent, natkeys, child, _fk in GROUPS:
            t = totals[parent]
            print(
                f"  {parent}: +{t['parent_inserted']} righe | "
                f"{child}: +{t['child_inserted']} storici | "
                f"totale {parent} ora: {finals[parent]}"
            )
        return 0
    except Exception as exc:  # noqa: BLE001
        con.rollback()
        print(f"ERRORE merge (rollback eseguito): {exc}", file=sys.stderr)
        return 1
    finally:
        con.close()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("server_db")
    ap.add_argument("incoming_db")
    ap.add_argument("--apply", action="store_true", help="Scrive le modifiche (default: dry-run)")
    args = ap.parse_args()
    return merge(args.server_db, args.incoming_db, args.apply)


if __name__ == "__main__":
    raise SystemExit(main())
