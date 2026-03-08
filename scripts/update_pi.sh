#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
APP_ROOT="${APP_ROOT:-/opt/threads-github-bot}"
APP_DIR="${APP_DIR:-$APP_ROOT/app}"
VENV_DIR="${VENV_DIR:-$APP_ROOT/venv}"
SYSTEMD_DIR="${SYSTEMD_DIR:-/etc/systemd/system}"
SERVICE_NAME="${SERVICE_NAME:-threads-github-bot}"

sudo rsync -a --delete \
  --exclude '.git/' \
  --exclude '.venv/' \
  --exclude '__pycache__/' \
  --exclude '.pytest_cache/' \
  --exclude '.ruff_cache/' \
  "$PROJECT_ROOT"/ "$APP_DIR"/

sudo "$VENV_DIR/bin/pip" install -r "$APP_DIR/requirements.txt"
for unit_file in "$APP_DIR"/deployment/systemd/*.service "$APP_DIR"/deployment/systemd/*.timer; do
  sudo install -m 644 "$unit_file" "$SYSTEMD_DIR/$(basename "$unit_file")"
done
sudo systemctl daemon-reload
sudo systemctl restart "$SERVICE_NAME.service"
sudo systemctl restart "$SERVICE_NAME.timer"

printf 'Updated %s from %s\n' "$SERVICE_NAME" "$PROJECT_ROOT"
