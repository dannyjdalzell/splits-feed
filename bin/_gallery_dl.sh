#!/usr/bin/env bash
# robust wrapper for gallery-dl
set -euo pipefail
# Try common locations
for c in "$(command -v gallery-dl 2>/dev/null || true)" \
         "$HOME/Library/Python/3.9/bin/gallery-dl" \
         "$HOME/Library/Python/3.10/bin/gallery-dl" \
         "$HOME/Library/Python/3.11/bin/gallery-dl" \
         "$HOME/.local/bin/gallery-dl" \
         "/opt/homebrew/bin/gallery-dl" \
         "/usr/local/bin/gallery-dl"
do
  if [ -n "$c" ] && [ -x "$c" ]; then exec "$c" "$@"; fi
done
# Fallback to module execution (works even if no script on PATH)
exec python3 -m gallery_dl "$@"
