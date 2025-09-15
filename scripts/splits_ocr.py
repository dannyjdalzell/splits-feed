import os, csv, re
import pytesseract
from PIL import Image

OUT_FILE = "splits.csv"
IN_DIR = "images"

FIELDNAMES = [
    "timestamp", "league", "away_team", "home_team",
    "market", "tickets_pct", "handle_pct", "line", "source"
]

def now_ts():
    from datetime import datetime
    return datetime.utcnow().isoformat()

def preprocess(img):
    return img.convert("L")

def ocr_best(img):
    texts = []
    for psm in (6, 11, 4):
        cfg = f"--oem 3 --psm {psm}"
        txt = pytesseract.image_to_string(img, lang="eng", config=cfg)
        score = len(re.findall(r"\d{2,3}%|Bets|Handle", txt, flags=re.I))
        texts.append((score, psm, txt))
    texts.sort(reverse=True)
    return texts[0]  # (score, psm, txt)

def parse_blocks(txt):
    rows = []
    # ⚠️ This is just a stub: you’ll want to tune regex for your screenshot style
    for line in txt.splitlines():
        if "%" in line:
            rows.append({
                "timestamp": now_ts(),
                "league": "Unknown",
                "away_team": "",
                "home_team": "",
                "market": "Spread",
                "tickets_pct": 0,
                "handle_pct": 0,
                "line": "",
                "source": "OCR"
            })
    return rows

def main():
    all_rows = []
    os.makedirs(IN_DIR, exist_ok=True)

    for fname in os.listdir(IN_DIR):
        path = os.path.join(IN_DIR, fname)
        try:
            img = Image.open(path)
        except Exception:
            print(f"[ERR] Cannot open: {fname}")
            continue
        pim = preprocess(img)
        score, psm, txt = ocr_best(pim)
        rows = parse_blocks(txt)
        if rows:
            print(f"[OK] {fname}: {len(rows)} row(s) (psm={psm}, score={score})")
            all_rows += rows
        else:
            print(f"[WARN] No rows parsed from: {fname}")

    if all_rows:
        exists = os.path.exists(OUT_FILE)
        with open(OUT_FILE, "a", newline="") as f:
            w = csv.DictWriter(f, fieldnames=FIELDNAMES)
            if not exists:
                w.writeheader()
            for r in all_rows:
                w.writerow(r)
        print(f"Appended {len(all_rows)} row(s) → {OUT_FILE}")
    else:
        print("No valid rows parsed from any image.")

if __name__ == "__main__":
    main()
