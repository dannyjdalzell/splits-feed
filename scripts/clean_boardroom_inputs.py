name: pipeline

on:
  workflow_dispatch: {}
  schedule:
    - cron: "*/15 * * * *"   # every 15 minutes
  push:
    branches: [ main ]

permissions:
  contents: write

env:
  TZ: America/Chicago
  PYTHONUNBUFFERED: "1"

jobs:
  run:
    runs-on: ubuntu-latest

    steps:
      # ---------- Repo checkout ----------
      - name: Checkout
        uses: actions/checkout@v4
        with:
          fetch-depth: 2

      - name: Setup Python 3.11
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      # ---------- Sheets → CSV (always fresh) ----------
      - name: Sheets → Twitter ingest (always-fresh Export → workspace)
        shell: bash
        run: |
          set -euo pipefail
          mkdir -p sources/sheets/twitter
          curl -Lsf "https://docs.google.com/spreadsheets/d/e/2PACX-1vT39ngJbPzNRjcnKVG-Oehiy4qzyrghIvCI0FQbaBj2jc9LYGLbMUZaCQDGN8Ck_8Q465hqsR4AYz3k/pub?gid=77061416&single=true&output=csv" \
            > sources/sheets/twitter/tweets.csv
          echo "[sheets] wrote $(wc -l < sources/sheets/twitter/tweets.csv) rows → sources/sheets/twitter/tweets.csv"

      # ---------- Twitter text analysis → signals CSV ----------
      - name: Twitter text analysis (signals)
        shell: bash
        run: |
          set -euo pipefail
          if [ -f scripts/analyze_twitter_text.py ]; then
            python scripts/analyze_twitter_text.py
          else
            echo "[warn] scripts/analyze_twitter_text.py not found; skipping"
          fi

      # ---------- Normalize + merge sources ----------
      - name: Normalize + merge sources
        shell: bash
        run: |
          set -euo pipefail
          if [ -f scripts/normalize_and_merge.py ]; then
            python scripts/normalize_and_merge.py
            echo "[ok] wrote: $GITHUB_WORKSPACE/audit_out/boardroom_inputs.csv"
          else
            echo "[warn] scripts/normalize_and_merge.py not found; skipping"
          fi

      # ---------- NEW: Clean boardroom_inputs before promote ----------
      - name: Clean boardroom_inputs (drop junk/unresolved before promote)
        shell: bash
        run: |
          set -euo pipefail
          python scripts/clean_boardroom_inputs.py

      # ---------- Promote staged → splits.csv (fallback to boardroom_inputs) ----------
      - name: Promote staged → splits.csv (with fallback)
        shell: bash
        run: |
          set -euo pipefail
          python - <<'PY'
import csv, sys
from pathlib import Path

root = Path(__file__).resolve().parents[2]
staged = root/"audit_out"/"splits_staged.csv"
board  = root/"audit_out"/"boardroom_inputs.csv"
out    = root/"splits.csv"

def read_rows(p):
    if not p.exists(): return []
    with p.open() as f:
        return list(csv.DictReader(f))

rows = read_rows(staged)
if rows:
    print(f"PROMOTE_OK (staged) rows: {len(rows)}")
else:
    print("staged present but no valid rows; falling back")
    rows = read_rows(board)
    print(f"PROMOTE_OK (fallback) rows: {len(rows)}")

if rows:
    with out.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=rows[0].keys())
        w.writeheader(); w.writerows(rows)
else:
    print("[warn] nothing to promote; leaving splits.csv unchanged")
PY

      # ---------- Live delta analysis (writes reports/) ----------
      - name: Live delta analysis (rolling; stop 15m pre-start)
        shell: bash
        run: |
          set -euo pipefail
          if [ -f scripts/live_delta_analysis.py ]; then
            python scripts/live_delta_analysis.py || echo "[live-delta] script returned non-zero"
          else
            echo "[live-delta] scripts/live_delta_analysis.py not found; skipping"
          fi

      # ---------- Commit outputs (rebase-safe) ----------
      - name: Commit outputs
        shell: bash
        run: |
          set -euo pipefail
          git config user.name  "splits-bot"
          git config user.email "actions@users.noreply.github.com"
          git add sources/sheets/twitter/tweets.csv || true
          git add audit_out/twitter_text_signals.csv audit_out/boardroom_inputs.csv || true
          git add splits.csv || true
          test -d reports && git add -A reports || true

          git diff --cached --quiet && { echo "no changes"; exit 0; }

          git fetch origin
          git pull --rebase origin main || git rebase --strategy-option=theirs origin/main || true

          git commit -m "ci: refresh + promote + live-delta (auto)" || true
          git push || echo "[push] remote moved; will land next cycle"
