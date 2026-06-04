#!/usr/bin/env bash
# Scrape da una macchina RESIDENZIALE (es. il Mac) delle fonti che bloccano gli
# IP datacenter (subito), poi spedisce gli snapshot al server che li ingerisce.
# Uso: residential_scrape.sh [sorgenti]   (default: subito)
set -euo pipefail

REPO="${TRADER_REPO_DIR:-$(cd "$(dirname "$0")/.." && pwd)}"
SSH_KEY="${TRADER_DEPLOY_KEY:-$HOME/.ssh/trader_deploy}"
SRV_USER="${TRADER_SRV_USER:-trader}"
SRV_HOST="${TRADER_SRV_HOST:-206.189.61.30}"
SOURCES="${1:-subito}"
PY="${TRADER_PYTHON:-python3}"

cd "$REPO"
tmpdb="$(mktemp -t traderscrape).db"
trap 'rm -f "$tmpdb"' EXIT

# Scrape su DB temporaneo (non tocca dati locali): ci interessa solo lo snapshot.
TRADER_DB_PATH="$tmpdb" TRADER_VIEWER_OPEN_BROWSER=false "$PY" run.py --source "$SOURCES" || true

remote=""
IFS=',' read -ra SRCS <<< "$SOURCES"
for s in "${SRCS[@]}"; do
  f=$(ls -t "data/${s}"_*.json 2>/dev/null | head -1 || true)
  [ -z "$f" ] && continue
  tot=$("$PY" -c "import json;print(json.load(open('$f')).get('total',0))")
  if [ "$tot" -gt 0 ]; then
    scp -i "$SSH_KEY" -o StrictHostKeyChecking=accept-new "$f" "$SRV_USER@$SRV_HOST:/var/lib/trader/data/"
    remote="$remote /var/lib/trader/data/$(basename "$f")"
    echo "[residential] spedito $f ($tot)"
  else
    echo "[residential] $s: 0 prodotti, salto"
  fi
done

if [ -n "$remote" ]; then
  E="TRADER_DB_PATH=/var/lib/trader/tracker.db TRADER_OUTPUT_DIR=/var/lib/trader/data"
  ssh -i "$SSH_KEY" "$SRV_USER@$SRV_HOST" \
    "$E /opt/trader/venv/bin/python /opt/trader/app/run.py --ingest-snapshot$remote && \
     $E /opt/trader/venv/bin/python /opt/trader/app/run.py --cleanup"
  echo "[residential] ingest completato"
else
  echo "[residential] niente da ingerire"
fi
