name: OCR Splits

on:
  push:
    paths:
      - "images/**"
      - "scripts/**"
      - ".github/workflows/ocr-splits.yml"
  workflow_dispatch:

permissions:
  contents: write

concurrency:
  group: ocr-splits-main
  cancel-in-progress: true

jobs:
  ocr:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout
        uses: actions/checkout@v4
        with:
          fetch-depth: 0

      - name: Install Tesseract
        run: |
          sudo apt-get update
          sudo apt-get install -y tesseract-ocr

      - name: Setup Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install Python deps
        run: |
          python -m pip install --upgrade pip
          pip install pillow pytesseract

      - name: Run OCR script
        run: python scripts/splits_ocr.py

      - name: Commit splits.csv (if changed)
        run: |
          git config user.name  "splits-bot"
          git config user.email "splits-bot@users.noreply.github.com"

          if ! git diff --quiet -- splits.csv; then
            git add splits.csv
            git commit -m "update splits.csv [ci]"
            git fetch origin main
            git pull --rebase origin main
            git push origin HEAD:main || git push --force-with-lease origin HEAD:main
          else
            echo "No changes in splits.csv — skipping commit."
          fi
