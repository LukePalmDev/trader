#!/usr/bin/env bash
# Auto-deploy del codice sul server: allinea /opt/trader/app a origin/main.
# Idempotente: se non ci sono novità non fa nulla. I file runtime non tracciati
# (tracker.db vive in /var/lib/trader, ebay.db, logs/, backups/, ...) non vengono
# toccati da 'git reset --hard' (agisce solo sui file tracciati).
set -euo pipefail

APP_DIR="${APP_DIR:-/opt/trader/app}"
VENV_DIR="${VENV_DIR:-/opt/trader/venv}"
BRANCH="${DEPLOY_BRANCH:-main}"

cd "$APP_DIR"

# Lo script gira come root su una dir di proprietà 'trader': autorizza git.
git config --global --add safe.directory "$APP_DIR" 2>/dev/null || true

git fetch --quiet origin "$BRANCH"
local_sha="$(git rev-parse HEAD)"
remote_sha="$(git rev-parse "origin/$BRANCH")"

if [ "$local_sha" = "$remote_sha" ]; then
  echo "[deploy] già aggiornato ($local_sha)"
  exit 0
fi

echo "[deploy] aggiornamento ${local_sha:0:8} -> ${remote_sha:0:8}"

# Reinstalla le dipendenze solo se requirements.lock è cambiato.
reqs_changed=false
if ! git diff --quiet "$local_sha" "$remote_sha" -- requirements.lock; then
  reqs_changed=true
fi
systemd_changed=false
if ! git diff --quiet "$local_sha" "$remote_sha" -- deploy/systemd deploy/server_job.sh deploy/backup_db.sh; then
  systemd_changed=true
fi

git reset --hard "origin/$BRANCH"
chown -R trader:trader "$APP_DIR" 2>/dev/null || true
chmod +x "$APP_DIR/deploy/server_job.sh" "$APP_DIR/deploy/backup_db.sh" 2>/dev/null || true

if [ "$reqs_changed" = true ] && [ -x "$VENV_DIR/bin/pip" ]; then
  echo "[deploy] requirements.lock cambiato: aggiorno dipendenze"
  "$VENV_DIR/bin/pip" install -q -r requirements.lock || true
fi

if [ "$systemd_changed" = true ]; then
  echo "[deploy] unità systemd/deploy cambiate: aggiorno servizi e timer"
  install -m 0644 "$APP_DIR/deploy/systemd/"*.service /etc/systemd/system/
  install -m 0644 "$APP_DIR/deploy/systemd/"*.timer /etc/systemd/system/
  systemctl daemon-reload
  systemctl enable --now trader-ai-cascade.timer
fi

systemctl restart trader-viewer.service
echo "[deploy] completato a $(date -u +%FT%TZ) -> ${remote_sha:0:8}"
