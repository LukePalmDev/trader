#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${APP_DIR:-/opt/trader/app}"
VENV_DIR="${VENV_DIR:-/opt/trader/venv}"
LOG_DIR="${LOG_DIR:-/var/log/trader}"
LOCK_FILE="${LOCK_FILE:-/var/lock/trader-job.lock}"

job="${1:-}"
mkdir -p "$LOG_DIR"

cd "$APP_DIR"

if [ -f /etc/trader/trader.env ]; then
  set -a
  # shellcheck disable=SC1091
  . /etc/trader/trader.env
  set +a
fi

export TRADER_VIEWER_OPEN_BROWSER="${TRADER_VIEWER_OPEN_BROWSER:-false}"
export TRADER_PLAYWRIGHT_CHANNEL="${TRADER_PLAYWRIGHT_CHANNEL:-chromium}"

run_python() {
  "$VENV_DIR/bin/python" "$@"
}

run_case() {
  case "$job" in
    scrape-fonti)
      # rebuy (GitHub) e gamelife (Mac residenziale) esclusi: bloccano l'IP del
      # server. Qui restano solo le fonti che funzionano da datacenter.
      run_python run.py --source gamepeople,gameshock,cex
      run_python run.py --cleanup
      ;;
    scrape-subito)
      run_python run.py --source subito
      run_python run.py --cleanup
      ;;
    scrape-ebay)
      run_python run.py --source ebay
      run_python ai_classifier.py --source ebay
      run_python run.py --cleanup
      ;;
    ai-classify)
      run_python ai_classifier.py
      ;;
    verify-sold)
      run_python verify_sold.py \
        --all \
        --no-tiered-selection \
        --max-runtime-minutes 50 \
        --concurrency 8 \
        --cffi-concurrency 6 \
        --chunk-size 200 \
        --browser-restart-every 2 \
        --nav-timeout-ms 7000 \
        --max-http403-ratio 0.40 \
        --min-coverage-ratio 0.40 \
        --fail-fast-min-attempts 400 \
        --fail-fast-blocked-ratio 0.85 \
        --fail-fast-403-ratio 0.60
      ;;
    *)
      echo "Usage: $0 {scrape-fonti|scrape-subito|scrape-ebay|ai-classify|verify-sold}" >&2
      exit 64
      ;;
  esac
}

timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
{
  flock -n 9 || {
    echo "[trader] another job is already running"
    exit 75
  }
  echo "[trader] job $job started at $timestamp"
  run_case
} 9>"$LOCK_FILE" 2>&1 | tee -a "$LOG_DIR/${job}.log"
