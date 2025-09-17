#!/bin/bash
set -euo pipefail
cd "$HOME/splits-feed"

ts(){ /bin/date "+%Y-%m-%dT%H:%M:%S%z"; }
log(){ echo "[$(ts)] $*"; }

# 0) Sync local main to remote
log "[git] fetch + reset to origin/main"
git fetch origin
git checkout -B main origin/main

# 1) Run scraper (creates images/pregame_*.png)
log "[scraper] running pregame"
/usr/bin/python3 scripts/pregame_scraper.py || log "[scraper] warning: run failed (will still try to push any files)"

# 2) Stage any new files
git add images/pregame_*.png 2>/dev/null || true
if git diff --cached --quiet; then
  log "[git] nothing new to commit"; exit 0
fi

git -c user.name="splits-bot" -c user.email="bot@local" commit -m "[scraper] Pregame Most Action screenshot"

# 3) Push with rebase/retry, last-resort with-lease
tries=0
while [ $tries -lt 3 ]; do
  if git pull --rebase origin main && git push origin HEAD:main; then
    log "[git] push to main OK"; exit 0
  fi
  tries=$((tries+1))
  log "[git] retry $tries…"
  sleep 2
done
log "[git] final attempt: force-with-lease"
git push --force-with-lease origin HEAD:main
