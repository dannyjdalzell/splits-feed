#!/bin/bash
set -euo pipefail
cd "$HOME/splits-feed"

ts() { /bin/date "+%Y-%m-%dT%H:%M:%S%z"; }
log() { echo "[$(ts)] $*"; }

log "[setup] ensuring Playwright + Chromium…"
/usr/bin/python3 -m pip install --user --quiet --upgrade pip playwright || true
/usr/bin/python3 -m playwright install chromium || true

# Always base a fresh run branch on remote main to avoid conflicts
RUNBR="auto/pregame-$("/bin/date" +%s)"
log "[git] syncing local from origin/main"
git fetch origin
# Older Git: use -C instead of -B
git switch -C main origin/main 2>/dev/null || git checkout -B main origin/main

log "[scraper] running…"
/usr/bin/python3 scripts/pregame_scraper.py || { log "[scraper] failed"; exit 1; }

# New branch per run; commit and push images found this run
git switch -c "$RUNBR" 2>/dev/null || git checkout -b "$RUNBR"
git add images/pregame_*.png 2>/dev/null || true

if git diff --cached --quiet; then
  log "[git] nothing new to commit"
  log "[done] no images -> nothing to push"
  exit 0
fi

git -c user.name="splits-bot" -c user.email="bot@local" \
  commit -m "[scraper] Pregame Most Action screenshot"
log "[git] pushing branch $RUNBR"
git push -u origin "$RUNBR"
log "[done] pushed to $RUNBR — check GitHub → Actions → OCR Splits"
