# scripts/splits_ocr.py
# End-to-end OCR → CSV for sportsbook screenshots (BetMGM gold + Covers grid).
# Drop images into repo /images, GitHub Actions runs this and appends to splits.csv.

import os, re, csv, sys
from datetime import datetime, timezone
from typing import List, Dict

import pytesseract
from PIL import Image, ImageOps, ImageFilter

# ---------- Paths & schema ----------
ROOT = os.path.dirname(os.path.abspath(__file__))  # .../scripts
REPO = os.path.dirname(ROOT)
IMG_DIR = os.path.join(REPO, "images")
DEBUG_DIR = os.path.join(REPO, "out")
OUT_FILE = os.path.join(REPO, "splits.csv")

FIELDNAMES = [
    "timestamp", "league", "away_team", "home_team",
    "market", "tickets_pct", "handle_pct", "line", "source"
]

# ---------- Utilities ----------
def now_ts() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

def ensure_dirs():
    os.makedirs(DEBUG_DIR, exist_ok=True)

def clean_pct(x: str) -> int:
    try:
        return int(re.sub(r"[^\d]", "", x))
    except Exception:
        return 0

def norm_team(s: str) -> str:
    # normalize weird dashes, zero-width, stray symbols
    s = re.sub(r"[\u200b\u2010-\u2015]", "-", s)
    s = re.sub(r"[^A-Za-z0-9 .'\-]", " ", s)
    s = re.sub(r"\s+", " ", s).strip(" .-")
    return s

def preprocess(img: Image.Image) -> Image.Image:
    """Aggressive but safe preprocessing to help Tesseract on screenshots."""
    g = ImageOps.grayscale(img)
    # upscale small shots
    w, h = g.size
    scale = 2.0 if max(w, h) < 1600 else 1.3
    g = g.resize((int(w*scale), int(h*scale)), Image.LANCZOS)
    g = ImageOps.autocontrast(g)
    # light denoise / sharpen
    g = g.filter(ImageFilter.MedianFilter(3)).filter(ImageFilter.UnsharpMask(radius=2, percent=150))
    return g

def ocr_best(img: Image.Image):
    """Try multiple PSMs; pick the text with most tokens that look like %, bets, handle, numbers."""
    candidates = []
    for psm in (6, 4, 11):
        cfg = f'--oem 3 --psm {psm}'
        txt = pytesseract.image_to_string(img, lang="eng", config=cfg)
        score = len(re.findall(r"(?:bets?|handle|%|\+|-|\d{2,})", txt, flags=re.I))
        candidates.append((score, psm, txt))
    candidates.sort(key=lambda x: x[0], reverse=True)
    return candidates[0]  # (score, psm, txt)

def dump_debug(basename: str, pim: Image.Image, txt: str, psm: int):
    try:
        pim.save(os.path.join(DEBUG_DIR, f"DEBUG_{basename}_psm{psm}.png"))
    except Exception:
        pass
    try:
        with open(os.path.join(DEBUG_DIR, f"{basename}.txt"), "w", encoding="utf-8") as f:
            f.write(txt)
    except Exception:
        pass

# ---------- Parsers for known formats ----------
def parse_mgm_gold(txt: str) -> List[Dict]:
    """
    BetMGM gold table. We mine lines that look like team rows containing percentages.
    Pair lines in order (top=away, next=home).
    """
    rows = []
    # Keep only lines with letters and at least one percentage
    lines = [l for l in txt.splitlines() if re.search(r"[A-Za-z]", l) and re.search(r"\d{1,3}\s*%", l)]
    if not lines:
        return rows

    recs = []
    for line in lines:
        # team-ish text before the numeric chaos
        lead = re.split(r"\d{1,3}\s*%", line, maxsplit=1)[0]
        team = norm_team(lead)
        if not team or len(team) < 3:
            # fallback: first chunk of letters
            m = re.search(r"([A-Za-z .'\-]{3,})", line)
            team = norm_team(m.group(1)) if m else "Unknown"

        # gather all percents on row; many sheets order as %Bets then %Handle
        pcts = re.findall(r"(\d{1,3})\s*%", line)
        bets = clean_pct(pcts[0]) if len(pcts) >= 1 else 0
        handle = clean_pct(pcts[1]) if len(pcts) >= 2 else 0

        # try moneyline and spread numbers
        m_ml = re.search(r"(^|[^\d])([+-]\d{2,3})(?!\d)", line)
        ml = m_ml.group(2) if m_ml else ""

        m_sp = re.search(r"([+-]\d+(?:\.\d+)?)", line)
        sp = m_sp.group(1) if m_sp else ""

        recs.append({"team": team, "bets": bets, "handle": handle, "ml": ml, "sp": sp})

    # Pair adjacent lines as away/home
    for i in range(0, len(recs) - 1, 2):
        away = recs[i]
        home = recs[i + 1]
        ts = now_ts()
        league = "Unknown"

        if away["ml"] or home["ml"]:
            rows.append({
                "timestamp": ts, "league": league,
                "away_team": away["team"], "home_team": home["team"],
                "market": "ML",
                "tickets_pct": home["bets"], "handle_pct": home["handle"],
                "line": home["ml"] if home["ml"] else away["ml"],
                "source": "MGM_FAM"
            })

        if away["sp"] or home["sp"]:
            rows.append({
                "timestamp": ts, "league": league,
                "away_team": away["team"], "home_team": home["team"],
                "market": "Spread",
                "tickets_pct": home["bets"], "handle_pct": home["handle"],
                "line": home["sp"] if home["sp"] else away["sp"],
                "source": "MGM_FAM"
            })
    return rows

def parse_covers_grid(txt: str) -> List[Dict]:
    """
    Covers-style white/blue grid with columns Spread/Total/Money and Handle/Bets pairs.
    We pick the rightmost Handle/Bets pair on each team row (often Moneyline).
    """
    rows = []
    lines = [l for l in txt.splitlines() if re.search(r"[A-Za-z].*\d", l)]
    team_lines = []
    for ln in lines:
        if re.search(r"\d{1,3}\s*%", ln):
            m = re.match(r"\s*([A-Za-z .'\-]{3,})\s+", ln)
            if m:
                team_lines.append(ln)

    recs = []
    for ln in team_lines:
        t = norm_team(re.match(r"\s*([A-Za-z .'\-]{3,})\s+", ln).group(1))
        pcts = re.findall(r"(\d{1,3})\s*%", ln)
        bets = handle = 0
        if len(pcts) >= 2:
            # grab the last pair on the row (often the Money columns)
            bets = clean_pct(pcts[-1])
            handle = clean_pct(pcts[-2])
        recs.append({"team": t, "bets": bets, "handle": handle})

    for i in range(0, len(recs) - 1, 2):
        away = recs[i]
        home = recs[i + 1]
        ts = now_ts()
        league = "Unknown"
        for market in ("ML", "Spread", "Total"):
            rows.append({
                "timestamp": ts, "league": league,
                "away_team": away["team"], "home_team": home["team"],
                "market": market,
                "tickets_pct": home["bets"], "handle_pct": home["handle"],
                "line": "", "source": "COVERS_FAM"
            })
    return rows

def parse_blocks(txt: str) -> List[Dict]:
    """Try all known parsers and dedupe results."""
    out: List[Dict] = []
    out.extend(parse_mgm_gold(txt))
    out.extend(parse_covers_grid(txt))

    # de-dupe identical rows
    seen = set()
    deduped = []
    for r in out:
        key = (
            r.get("away_team",""),
            r.get("home_team",""),
            r.get("market",""),
            r.get("tickets_pct",0),
            r.get("handle_pct",0),
            r.get("line",""),
            r.get("source",""),
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(r)
    return deduped

# ---------- Main ----------
def main():
    ensure_dirs()

    if not os.path.isdir(IMG_DIR):
        print(f"[ERR] images/ folder not found at {IMG_DIR}")
        sys.exit(1)

    image_exts = {".png", ".jpg", ".jpeg", ".webp"}
    files = [f for f in os.listdir(IMG_DIR) if os.path.splitext(f.lower())[1] in image_exts]
    if not files:
        print("No images found in images/ — nothing to do.")
        return

    all_rows: List[Dict] = []
    for fname in sorted(files):
        path = os.path.join(IMG_DIR, fname)
        try:
            img = Image.open(path)
        except Exception:
            print(f"[ERR] Cannot open: {fname}")
            continue

        pim = preprocess(img)
        score, psm, txt = ocr_best(pim)

        base = os.path.splitext(os.path.basename(fname))[0]
        dump_debug(base, pim, txt, psm)

        rows = parse_blocks(txt)
        if rows:
            print(f"[OK] {fname}: {len(rows)} row(s) (psm={psm}, score={score})")
            all_rows.extend(rows)
        else:
            print(f"[WARN] No rows parsed from: {fname} (see out/{base}.txt & DEBUG_{base}_psm{psm}.png)")

    if all_rows:
        exists = os.path.exists(OUT_FILE)
        with open(OUT_FILE, "a", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=FIELDNAMES)
            if not exists:
                w.writeheader()
            for r in all_rows:
                # coerce types / blanks
                r.setdefault("league","Unknown")
                r.setdefault("line","")
                r["tickets_pct"] = int(r.get("tickets_pct", 0) or 0)
                r["handle_pct"]  = int(r.get("handle_pct", 0) or 0)
                w.writerow(r)
        print(f"Appended {len(all_rows)} row(s) → {OUT_FILE}")
    else:
        print("No valid rows parsed from any image.")

if __name__ == "__main__":
    main()
