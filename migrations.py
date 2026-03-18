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
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version     INTEGER PRIMARY KEY,
            name        TEXT NOT NULL,
            applied_at  TEXT NOT NULL
        )
        """
    )


def _applied_versions(conn: sqlite3.Connection) -> set[int]:
    rows = conn.execute("SELECT version FROM schema_migrations").fetchall()
    return {int(row[0]) for row in rows}


def run_migrations(db_path: Path, migrations: Iterable[Migration]) -> list[int]:
    """Applica migrazioni idempotenti e restituisce le versioni applicate."""
    ordered = sorted(migrations, key=lambda m: m.version)
    applied_now: list[int] = []

    with sqlite3.connect(str(db_path)) as conn:
        _ensure_meta(conn)
        done = _applied_versions(conn)

        for migration in ordered:
            if migration.version in done:
                continue

            with conn:
                for statement in migration.statements:
                    conn.execute(statement)
                if migration.callback:
                    migration.callback(conn)
                conn.execute(
                    "INSERT INTO schema_migrations(version, name, applied_at) VALUES (?, ?, ?)",
                    (
                        migration.version,
                        migration.name,
                        datetime.now(timezone.utc).isoformat(),
                    ),
                )

            applied_now.append(migration.version)

    return applied_now
