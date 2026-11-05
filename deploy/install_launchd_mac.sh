#!/usr/bin/env bash
# Installa/aggiorna i LaunchAgent di trader sul Mac (utente corrente).
# Uso: deploy/install_launchd_mac.sh [--uninstall]
# Orari in ora locale del Mac. launchd recupera i job persi durante lo sleep.
set -euo pipefail

APP_DIR="$(cd "$(dirname "$0")/.." && pwd)"
LA="$HOME/Library/LaunchAgents"
PREFIX="org.byluke.trader"
UID_N="$(id -u)"
mkdir -p "$LA" "$APP_DIR/logs"

# nome | job | orari "HH:MM ..." oppure "every:SECONDI"
JOBS=(
  "scrape-fonti|scrape-fonti|07:00"
  "scrape-rebuy|scrape-rebuy|18:00"
  "scrape-subito|scrape-subito|04:00 10:00 16:00 22:00"
  "scrape-ebay|scrape-ebay|00:30"
  "ai-cascade|ai-cascade|05:10 11:10 17:10 23:10"
  "verify-sold|verify-sold|11:00 14:00"
  "backup|backup|03:15"
  "watchdog|watchdog|every:10800"
)

unload() {
  local label="$1"
  launchctl bootout "gui/$UID_N/$label" 2>/dev/null || true
}

load() {
  local plist="$1"
  plutil -lint "$plist" >/dev/null
  launchctl bootstrap "gui/$UID_N" "$plist"
}

if [ "${1:-}" = "--uninstall" ]; then
  for spec in "${JOBS[@]}" "viewer||"; do
    name="${spec%%|*}"
    unload "$PREFIX.$name"
    rm -f "$LA/$PREFIX.$name.plist"
  done
  echo "LaunchAgent trader rimossi."
  exit 0
fi

for spec in "${JOBS[@]}"; do
  IFS='|' read -r name job sched <<<"$spec"
  label="$PREFIX.$name"
  plist="$LA/$label.plist"
  if [[ "$sched" == every:* ]]; then
    trigger="  <key>StartInterval</key><integer>${sched#every:}</integer>"
  else
    trigger="  <key>StartCalendarInterval</key>
  <array>"
    for t in $sched; do
      h=$((10#${t%%:*})); m=$((10#${t##*:}))
      trigger="$trigger
    <dict><key>Hour</key><integer>$h</integer><key>Minute</key><integer>$m</integer></dict>"
    done
    trigger="$trigger
  </array>"
  fi
  cat >"$plist" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>$label</string>
  <key>ProgramArguments</key>
  <array>
    <string>/bin/bash</string>
    <string>$APP_DIR/deploy/mac_job.sh</string>
    <string>$job</string>
  </array>
  <key>WorkingDirectory</key><string>$APP_DIR</string>
$trigger
  <key>StandardOutPath</key><string>$APP_DIR/logs/launchd-$name.out.log</string>
  <key>StandardErrorPath</key><string>$APP_DIR/logs/launchd-$name.err.log</string>
  <key>ProcessType</key><string>Background</string>
</dict>
</plist>
PLIST
  unload "$label"
  load "$plist"
  echo "OK $label ($sched)"
done

# Viewer/API sempre attivo su 127.0.0.1 (porta da .env, default 8080).
label="$PREFIX.viewer"
plist="$LA/$label.plist"
cat >"$plist" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>$label</string>
  <key>ProgramArguments</key>
  <array>
    <string>$APP_DIR/.venv/bin/python</string>
    <string>$APP_DIR/run.py</string>
    <string>--view</string>
  </array>
  <key>WorkingDirectory</key><string>$APP_DIR</string>
  <key>EnvironmentVariables</key>
  <dict>
    <key>TRADER_HOST</key><string>mac</string>
    <key>TRADER_LOG_DIR</key><string>$APP_DIR/logs</string>
    <key>TRADER_VIEWER_OPEN_BROWSER</key><string>false</string>
  </dict>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><true/>
  <key>ThrottleInterval</key><integer>10</integer>
  <key>StandardOutPath</key><string>$APP_DIR/logs/viewer.log</string>
  <key>StandardErrorPath</key><string>$APP_DIR/logs/viewer.log</string>
</dict>
</plist>
PLIST
unload "$label"
load "$plist"
echo "OK $label (sempre attivo)"
