#!/usr/bin/env bash
set -euo pipefail

BACKUP_DIR="${BACKUP_DIR:-/var/backups/trader}"
RETENTION_DAYS="${RETENTION_DAYS:-14}"
LOG_DIR="${LOG_DIR:-/var/log/trader}"

cd /

if [ -f /etc/trader/trader.env ]; then
  set -a
  # shellcheck disable=SC1091
  . /etc/trader/trader.env
  set +a
fi

DB_PATH="${TRADER_DB_PATH:-/var/lib/trader/tracker.db}"
mkdir -p "$BACKUP_DIR"

# Log dedicato (best-effort) così la pagina /log mostra lo stato del backup.
LOG_FILE="$LOG_DIR/backup.log"
mkdir -p "$LOG_DIR" 2>/dev/null || true
log_line() { echo "$(date -u +%FT%TZ) $*" >>"$LOG_FILE" 2>/dev/null || true; }

APP="$(cd "$(dirname "$0")/.." && pwd)"
PY="${TRADER_PYTHON:-/opt/trader/venv/bin/python}"
[ -x "$PY" ] || PY="python3"
rec() { (cd "$APP" && "$PY" job_runs.py record backup "$1" ${2:+--error "$2"}) 2>/dev/null || true; }
trap 'rc=$?; if [ $rc -ne 0 ]; then log_line "[backup] ERROR exit code $rc"; rec error "exit code $rc"; fi' EXIT

log_line "[backup] start"

if [ ! -f "$DB_PATH" ]; then
  echo "Database not found: $DB_PATH" >&2
  log_line "[backup] ERROR database non trovato: $DB_PATH"
  exit 66
fi

stamp="$(date -u +%Y%m%dT%H%M%SZ)"
sqlite3 "$DB_PATH" ".backup '$BACKUP_DIR/tracker-$stamp.db'"
deleted="$(find "$BACKUP_DIR" -name 'tracker-*.db' -mtime +"$RETENTION_DAYS" -print -delete | wc -l | tr -d ' ')"
size="$(stat -c %s "$BACKUP_DIR/tracker-$stamp.db" 2>/dev/null || echo 0)"
log_line "[backup] OK tracker-$stamp.db ($size byte) — vecchi rimossi: $deleted"
rec ok
