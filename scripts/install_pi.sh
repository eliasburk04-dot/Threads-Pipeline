#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
APP_ROOT="${APP_ROOT:-/opt/threads-github-bot}"
APP_DIR="${APP_DIR:-$APP_ROOT/app}"
VENV_DIR="${VENV_DIR:-$APP_ROOT/venv}"
CONFIG_DIR="${CONFIG_DIR:-/etc/threads-github-bot}"
ENV_FILE="${ENV_FILE:-$CONFIG_DIR/threads-github-bot.env}"
STATE_DIR="${STATE_DIR:-/var/lib/threads-github-bot}"
LOG_DIR="${LOG_DIR:-/var/log/threads-github-bot}"
SERVICE_NAME="${SERVICE_NAME:-threads-github-bot}"
SERVICE_USER="${SERVICE_USER:-threadsbot}"
SYSTEMD_DIR="${SYSTEMD_DIR:-/etc/systemd/system}"

ensure_python() {
  if command -v python3.11 >/dev/null 2>&1; then
    printf '%s\n' "$(command -v python3.11)"
    return
  fi

  if python3 - <<'PY' >/dev/null 2>&1
import sys
raise SystemExit(0 if sys.version_info >= (3, 11) else 1)
PY
  then
    printf '%s\n' "$(command -v python3)"
    return
  fi

  sudo apt-get update
  sudo apt-get install -y python3.11 python3.11-venv python3-pip rsync
  printf '%s\n' "$(command -v python3.11)"
}

PYTHON_BIN="${PYTHON_BIN:-$(ensure_python)}"

if ! id -u "$SERVICE_USER" >/dev/null 2>&1; then
  sudo useradd --system --home "$APP_ROOT" --shell /usr/sbin/nologin "$SERVICE_USER"
fi

sudo install -d -m 750 -o "$SERVICE_USER" -g "$SERVICE_USER" "$APP_ROOT" "$APP_DIR" "$STATE_DIR" "$LOG_DIR" "$CONFIG_DIR"
sudo rsync -a --delete \
  --exclude '.git/' \
  --exclude '.venv/' \
  --exclude '__pycache__/' \
  --exclude '.pytest_cache/' \
  --exclude '.ruff_cache/' \
  "$PROJECT_ROOT"/ "$APP_DIR"/

sudo "$PYTHON_BIN" -m venv "$VENV_DIR"
sudo "$VENV_DIR/bin/pip" install --upgrade pip
sudo "$VENV_DIR/bin/pip" install -r "$APP_DIR/requirements.txt"

if [[ "${INSTALL_DEV_DEPS:-0}" == "1" ]]; then
  sudo "$VENV_DIR/bin/pip" install -r "$APP_DIR/requirements-dev.txt"
fi

if [[ ! -f "$ENV_FILE" ]]; then
  sudo install -m 640 -o root -g "$SERVICE_USER" "$APP_DIR/.env.example" "$ENV_FILE"
fi

for unit_file in "$APP_DIR"/deployment/systemd/*.service "$APP_DIR"/deployment/systemd/*.timer; do
  sudo install -m 644 "$unit_file" "$SYSTEMD_DIR/$(basename "$unit_file")"
done
sudo chown -R "$SERVICE_USER:$SERVICE_USER" "$APP_ROOT" "$STATE_DIR" "$LOG_DIR"
sudo chmod 640 "$ENV_FILE"
sudo systemctl daemon-reload
sudo systemctl enable --now "$SERVICE_NAME.timer"

printf '\nInstalled %s\n' "$SERVICE_NAME"
printf 'App dir: %s\n' "$APP_DIR"
printf 'Venv: %s\n' "$VENV_DIR"
printf 'Env file: %s\n' "$ENV_FILE"
printf 'Timer status: systemctl status %s.timer --no-pager\n' "$SERVICE_NAME"
printf 'Immediate run: systemctl start %s-run-now.service\n' "$SERVICE_NAME"
