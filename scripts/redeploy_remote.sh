#!/usr/bin/env bash
set -euo pipefail

REMOTE_HOST="${REMOTE_HOST:-milkathedog@100.69.69.19}"
REMOTE_STAGING="${REMOTE_STAGING:-/tmp/threads-github-bot-src}"
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

rsync -az --delete \
  --exclude '.git/' \
  --exclude '.venv/' \
  --exclude '__pycache__/' \
  --exclude '.pytest_cache/' \
  --exclude '.ruff_cache/' \
  "$PROJECT_ROOT"/ "$REMOTE_HOST:$REMOTE_STAGING/"

ssh "$REMOTE_HOST" "cd '$REMOTE_STAGING' && sudo bash scripts/update_pi.sh"
