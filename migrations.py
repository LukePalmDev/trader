from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterable


MigrationCallable = Callable[[sqlite3.Connection], None]


@dataclass(frozen=True)
class Migration:
    version: int
    name: str
    statements: tuple[str, ...] = ()
    callback: MigrationCallable | None = None


def _ensure_meta(conn: sqlite3.Connection) -> None:
    # Aggiornamento schema: aggiunge colonna 'namespace' se mancante.
    # Questo permette a più moduli (products, ads, sold_items) di coesistere
    # nello stesso DB senza conflitti sui version number.
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            namespace   TEXT NOT NULL DEFAULT '',
            version     INTEGER NOT NULL,
            name        TEXT NOT NULL,
            applied_at  TEXT NOT NULL,
            PRIMARY KEY (namespace, version)
        )
        """
    )
    # Se la tabella esiste già senza namespace (vecchio schema), la aggiorna
    cols = {row[1] for row in conn.execute("PRAGMA table_info(schema_migrations)").fetchall()}
    if "namespace" not in cols:
        conn.execute("ALTER TABLE schema_migrations ADD COLUMN namespace TEXT NOT NULL DEFAULT ''")


def _applied_versions(conn: sqlite3.Connection, namespace: str = "") -> set[int]:
    rows = conn.execute(
        "SELECT version FROM schema_migrations WHERE namespace = ?",
        (namespace,),
    ).fetchall()
    return {int(row[0]) for row in rows}


def run_migrations(
    db_path: Path,
    migrations: Iterable[Migration],
    *,
    namespace: str = "",
) -> list[int]:
    """Applica migrazioni idempotenti e restituisce le versioni applicate.

    Args:
        db_path: percorso del file SQLite.
        migrations: sequenza di Migration da applicare.
        namespace: prefisso logico per evitare collisioni (es. "products", "ads", "ebay").
    """
    ordered = sorted(migrations, key=lambda m: m.version)
    applied_now: list[int] = []

    with sqlite3.connect(str(db_path), timeout=30.0) as conn:
        _ensure_meta(conn)
        done = _applied_versions(conn, namespace)

        for migration in ordered:
            if migration.version in done:
                continue

            with conn:
                for statement in migration.statements:
                    conn.execute(statement)
                if migration.callback:
                    migration.callback(conn)
                conn.execute(
                    "INSERT INTO schema_migrations(namespace, version, name, applied_at) VALUES (?, ?, ?, ?)",
                    (
                        namespace,
                        migration.version,
                        migration.name,
                        datetime.now(timezone.utc).isoformat(),
                    ),
                )

            applied_now.append(migration.version)

    return applied_now
