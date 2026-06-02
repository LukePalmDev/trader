#!/usr/bin/env bash
set -euo pipefail

BACKUP_DIR="${BACKUP_DIR:-/var/backups/trader}"
RETENTION_DAYS="${RETENTION_DAYS:-14}"

cd /

if [ -f /etc/trader/trader.env ]; then
  set -a
  # shellcheck disable=SC1091
  . /etc/trader/trader.env
  set +a
fi

DB_PATH="${TRADER_DB_PATH:-/var/lib/trader/tracker.db}"
mkdir -p "$BACKUP_DIR"

if [ ! -f "$DB_PATH" ]; then
  echo "Database not found: $DB_PATH" >&2
  exit 66
fi

stamp="$(date -u +%Y%m%dT%H%M%SZ)"
sqlite3 "$DB_PATH" ".backup '$BACKUP_DIR/tracker-$stamp.db'"
find "$BACKUP_DIR" -name 'tracker-*.db' -mtime +"$RETENTION_DAYS" -delete
