#!/usr/bin/env bash
# Job trader sul Mac (launchd, IP residenziale). Sostituisce server_job.sh +
# residential_scrape.sh: qui scrape e DB stanno sulla stessa macchina.
# Uso: deploy/mac_job.sh {scrape-fonti|scrape-rebuy|scrape-subito|scrape-ebay|ai-cascade|verify-sold|backup|watchdog}
set -uo pipefail

APP_DIR="${APP_DIR:-$(cd "$(dirname "$0")/.." && pwd)}"
cd "$APP_DIR"
export PATH="$APP_DIR/.venv/bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"

# .env (stesso file letto da settings.py) anche per bash, senza sovrascrivere l'ambiente.
if [ -f "$APP_DIR/.env" ]; then
  while IFS= read -r line || [ -n "$line" ]; do
    case "$line" in ''|'#'*) continue ;; esac
    line="${line#export }"
    key="${line%%=*}"; val="${line#*=}"
    [ "$key" = "$line" ] && continue
    [ -n "${!key+x}" ] && continue
    val="${val%\"}"; val="${val#\"}"; val="${val%\'}"; val="${val#\'}"
    export "$key=$val"
  done < "$APP_DIR/.env"
fi

export TRADER_HOST=mac
export TRADER_LOG_DIR="${TRADER_LOG_DIR:-$APP_DIR/logs}"
export TRADER_VIEWER_OPEN_BROWSER=false
export TRADER_PLAYWRIGHT_CHANNEL="${TRADER_PLAYWRIGHT_CHANNEL:-chromium}"
PY="$APP_DIR/.venv/bin/python"
BACKUP_DIR="${BACKUP_DIR:-$APP_DIR/backups}"
RETENTION_DAYS="${RETENTION_DAYS:-14}"
LOCK_FILE="$APP_DIR/runtime/trader-job.lock"
mkdir -p "$TRADER_LOG_DIR" "$APP_DIR/runtime"

job="${1:-}"
LOG="$TRADER_LOG_DIR/${job:-mac_job}.log"
exec >>"$LOG" 2>&1
echo "[trader] job $job started at $(date -u +%Y%m%dT%H%M%SZ) (mac)"

db_path() {
  "$PY" -c 'from paths import DB_PATH; print(DB_PATH)'
}

do_backup() {
  local db stamp size deleted
  db="$(db_path)"
  if [ ! -f "$db" ]; then
    echo "[backup] ERROR database non trovato: $db"
    "$PY" job_runs.py record backup error --error "database non trovato" || true
    return 66
  fi
  mkdir -p "$BACKUP_DIR"
  stamp="$(date -u +%Y%m%dT%H%M%SZ)"
  if sqlite3 "$db" ".backup '$BACKUP_DIR/tracker-$stamp.db'"; then
    deleted="$(find "$BACKUP_DIR" -name 'tracker-*.db' -mtime +"$RETENTION_DAYS" -print -delete | wc -l | tr -d ' ')"
    size="$(stat -f %z "$BACKUP_DIR/tracker-$stamp.db" 2>/dev/null || echo 0)"
    echo "$(date -u +%FT%TZ) [backup] OK tracker-$stamp.db ($size byte) — vecchi rimossi: $deleted"
    "$PY" job_runs.py record backup ok || true
  else
    echo "[backup] ERROR sqlite3 .backup fallito"
    "$PY" job_runs.py record backup error --error "sqlite3 .backup fallito" || true
    return 1
  fi
}

run_case() {
  case "$job" in
    scrape-fonti)
      "$PY" run.py --source gamepeople,gameshock,cex,gamelife,rebuy
      "$PY" run.py --cleanup
      ;;
    scrape-rebuy)
      "$PY" run.py --source rebuy
      ;;
    scrape-subito)
      "$PY" run.py --source subito,gamelife
      "$PY" run.py --cleanup
      ;;
    scrape-ebay)
      "$PY" run.py --source ebay
      "$PY" run.py --cleanup
      ;;
    ai-cascade)
      if [ -z "${OPENAI_API_KEY:-}" ]; then
        echo "[trader] OPENAI_API_KEY non configurata nel .env: ai-cascade saltato"
        return 0
      fi
      "$PY" run.py --ai-cascade-classify --ai-cascade-limit "${AI_CASCADE_LIMIT:-1000}"
      ;;
    verify-sold)
      "$PY" verify_sold.py \
        --all --no-tiered-selection --max-runtime-minutes 50 \
        --concurrency 8 --cffi-concurrency 6 --chunk-size 200 \
        --browser-restart-every 2 --nav-timeout-ms 7000 \
        --max-http403-ratio 0.40 --min-coverage-ratio 0.40 \
        --fail-fast-min-attempts 400 --fail-fast-blocked-ratio 0.85 \
        --fail-fast-403-ratio 0.60
      ;;
    *)
      echo "Uso: $0 {scrape-fonti|scrape-rebuy|scrape-subito|scrape-ebay|ai-cascade|verify-sold|backup|watchdog}" >&2
      return 64
      ;;
  esac
}

case "$job" in
  backup) do_backup; exit $? ;;
  watchdog) "$PY" job_runs.py check; exit $? ;;
esac

# Un solo job pesante alla volta (come il flock del server).
if ! shlock -f "$LOCK_FILE" -p $$; then
  echo "[trader] un altro job è in corso ($(cat "$LOCK_FILE" 2>/dev/null)): salto $job"
  exit 0
fi
trap 'rm -f "$LOCK_FILE"' EXIT
run_case
rc=$?
echo "[trader] job $job finito con exit $rc at $(date -u +%Y%m%dT%H%M%SZ)"
exit $rc
