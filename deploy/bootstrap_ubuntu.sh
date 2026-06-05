#!/usr/bin/env bash
set -euo pipefail

REPO_URL="${1:-https://github.com/LukePalmDev/trader.git}"
APP_USER="${APP_USER:-trader}"
APP_ROOT="${APP_ROOT:-/opt/trader}"
APP_DIR="$APP_ROOT/app"
VENV_DIR="$APP_ROOT/venv"
PYTHON_BIN="${PYTHON_BIN:-python3}"

if [ "$(id -u)" -ne 0 ]; then
  echo "Run as root: sudo bash deploy/bootstrap_ubuntu.sh [repo-url]" >&2
  exit 1
fi

apt-get update
apt-get install -y \
  git python3 python3-venv python3-pip \
  curl ca-certificates sqlite3 rsync unzip jq \
  build-essential libsqlite3-dev sudo openssl

if ! id "$APP_USER" >/dev/null 2>&1; then
  useradd --system --create-home --shell /bin/bash "$APP_USER"
fi

mkdir -p "$APP_ROOT" /var/lib/trader/data /var/log/trader /etc/trader /var/backups/trader
chown -R "$APP_USER:$APP_USER" "$APP_ROOT" /var/lib/trader /var/log/trader /var/backups/trader

if [ ! -d "$APP_DIR/.git" ]; then
  sudo -u "$APP_USER" git clone "$REPO_URL" "$APP_DIR"
else
  sudo -u "$APP_USER" git -C "$APP_DIR" pull --rebase
fi

"$PYTHON_BIN" -m venv "$VENV_DIR"
"$VENV_DIR/bin/pip" install --upgrade pip
"$VENV_DIR/bin/pip" install -r "$APP_DIR/requirements.lock"
"$VENV_DIR/bin/python" -m playwright install chromium
"$VENV_DIR/bin/python" -m playwright install-deps chromium
"$VENV_DIR/bin/python" -m patchright install chromium || true

if [ ! -f /etc/trader/trader.env ]; then
  cp "$APP_DIR/deploy/trader.env.example" /etc/trader/trader.env
  token="$(openssl rand -hex 32)"
  sed -i "s/^TRADER_API_TOKEN=.*/TRADER_API_TOKEN=$token/" /etc/trader/trader.env
  chmod 600 /etc/trader/trader.env
fi

install -m 0644 "$APP_DIR/deploy/systemd/"*.service /etc/systemd/system/
install -m 0644 "$APP_DIR/deploy/systemd/"*.timer /etc/systemd/system/
chmod +x "$APP_DIR/deploy/server_job.sh" "$APP_DIR/deploy/backup_db.sh"

if [ -f "$APP_DIR/tracker.db" ] && [ ! -f /var/lib/trader/tracker.db ]; then
  cp "$APP_DIR/tracker.db" /var/lib/trader/tracker.db
  chown "$APP_USER:$APP_USER" /var/lib/trader/tracker.db
fi

systemctl daemon-reload
systemctl enable --now trader-viewer.service
systemctl enable --now \
  trader-scrape-fonti.timer \
  trader-scrape-subito.timer \
  trader-scrape-ebay.timer \
  trader-ai-classify.timer \
  trader-ai-cascade.timer \
  trader-verify-sold.timer \
  trader-backup.timer

echo "Bootstrap complete."
echo "Edit secrets with: sudo nano /etc/trader/trader.env"
echo "Viewer tunnel: ssh -L 8080:127.0.0.1:8080 <server>"
