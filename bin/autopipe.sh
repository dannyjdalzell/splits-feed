#!/usr/bin/env bash
set -euo pipefail

# --- config ---
ROOT="${HOME}/splits-feed"
ENV_FILE="${ROOT}/.autopipe.env"
[ -f "$ENV_FILE" ] && source "$ENV_FILE"

SHEET_CSV="${STATE_DIR}/tweets.csv"
SEEN_URLS="${STATE_DIR}/seen_urls.txt"
LOG="${STATE_DIR}/last_run.log"
IMG_DIR="${IMG_DIR:-$ROOT/images}"
REPO="${REPO_DIR:-$ROOT}"

mkdir -p "$STATE_DIR" "$IMG_DIR"

log(){ printf "%s | %s\n" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*" | tee -a "$LOG" ; }

# 1) Pull latest tweet CSV from your published Google Sheet
log "fetching sheet…"
curl -fsSL "${SHEETS_URL}&nocache=$(date +%s)" -o "$SHEET_CSV" || { log "sheet fetch FAILED"; exit 0; }

# 2) Extract new tweet URLs (header includes 'Link')
log "extracting URLs…"
urls=$(awk -F',' 'NR>1 {print $0}' "$SHEET_CSV" \
  | sed $'s/\r$//' \
  | awk -F',' '{print $0}' \
  | grep -Eo 'https?://(x|twitter)\.com/[^, ]+' | sort -u)

touch "$SEEN_URLS"
new=0
while read -r u; do
  [ -z "$u" ] && continue
  if ! grep -qxF "$u" "$SEEN_URLS"; then
    echo "$u" >> "$SEEN_URLS.new"
    echo "$u"
    new=$((new+1))
  fi
done <<< "$urls" > "${STATE_DIR}/new_urls.txt" || true
[ -f "$SEEN_URLS.new" ] && cat "$SEEN_URLS" "$SEEN_URLS.new" | sort -u > "$SEEN_URLS.tmp" && mv "$SEEN_URLS.tmp" "$SEEN_URLS" && rm -f "$SEEN_URLS.new"

log "new URLs: $new"

# 3) Download images for new URLs (idempotent; "/Users/danieldalzell/splits-feed/bin/_gallery_dl.sh" dedupes by filename)
if [ -s "${STATE_DIR}/new_urls.txt" ]; then
  log "downloading images…"
  while read -r url; do
    [ -z "$url" ] && continue
    "/Users/danieldalzell/splits-feed/bin/_gallery_dl.sh" -q -D "$IMG_DIR" "$url" || true
  done < "${STATE_DIR}/new_urls.txt"
else
  log "no new URLs"
fi

# 4) OCR → CSV (your existing script)
if [ -x "${HOME}/Desktop/run_ocr_and_diff.command" ]; then
  log "running OCR…"
  "${HOME}/Desktop/run_ocr_and_diff.command" || true
else
  # direct fallback if command missing
  python3 "${REPO}/scripts/splits_ocr.py" "${IMG_DIR}" > "${HOME}/Desktop/splits_ocr_new.csv" || true
fi

# 5) Merge & guard (use your latest merge guard if present)
if [ -x "${HOME}/Desktop/validate_and_merge_v3.command" ]; then
  log "merging…"
  "${HOME}/Desktop/validate_and_merge_v3.command" || true
fi

# 6) Build clean + audit (relaxed)
[ -x "${HOME}/Desktop/build_splits_clean_relaxed.command" ] && "${HOME}/Desktop/build_splits_clean_relaxed.command" || true
[ -x "${HOME}/Desktop/audit_splits.command" ] && "${HOME}/Desktop/audit_splits.command" || true

# 7) Commit + push (safe-noop if unchanged)
cd "$REPO"
git add splits.csv splits_clean.csv splits_garbage.csv .state/last_run.log || true
if ! git diff --staged --quiet; then
  git commit -m "autopipe: ingest + ocr + clean ($(date -u +%Y-%m-%dT%H:%M:%SZ))" || true
  git push || true
  log "pushed changes."
else
  log "no repo changes to commit."
fi

# 8) Status drop to Desktop
{
  echo "AUTOPIPE RUN @ $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "New URLs this run: $new"
  echo "Images dir: $IMG_DIR (files: $(find "$IMG_DIR" -type f | wc -l))"
  echo "splits.csv lines: $(wc -l < "$REPO/splits.csv" 2>/dev/null || echo 0)"
  echo "Clean: $(wc -l < "$REPO/splits_clean.csv" 2>/dev/null || echo 0)"
  echo "Garbage: $(wc -l < "$REPO/splits_garbage.csv" 2>/dev/null || echo 0)"
} > "${HOME}/Desktop/autopipe_status.txt"

log "done."
