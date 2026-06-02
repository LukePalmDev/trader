#!/usr/bin/env bash
set -euo pipefail

APP_USER="${APP_USER:-trader}"
KEY_PATH="/home/$APP_USER/.ssh/github_deploy_key"

if [ "$(id -u)" -ne 0 ]; then
  echo "Run as root: sudo bash deploy/prepare_deploy_key.sh" >&2
  exit 1
fi

install -d -m 700 -o "$APP_USER" -g "$APP_USER" "/home/$APP_USER/.ssh"

if [ ! -f "$KEY_PATH" ]; then
  sudo -u "$APP_USER" ssh-keygen -t ed25519 -N "" -C "trader-digitalocean" -f "$KEY_PATH"
fi

sudo -u "$APP_USER" touch "/home/$APP_USER/.ssh/config"
if ! grep -q "Host github.com" "/home/$APP_USER/.ssh/config"; then
  cat >> "/home/$APP_USER/.ssh/config" <<EOF
Host github.com
  IdentityFile $KEY_PATH
  IdentitiesOnly yes
EOF
fi
chown "$APP_USER:$APP_USER" "/home/$APP_USER/.ssh/config"
chmod 600 "/home/$APP_USER/.ssh/config"

echo
echo "Add this public key in GitHub:"
echo "Repo > Settings > Deploy keys > Add deploy key > Allow write access: OFF"
echo
cat "$KEY_PATH.pub"
