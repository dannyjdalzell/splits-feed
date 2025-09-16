# scripts/splits_ocr.py
# OCR → CSV for sportsbook screenshots (BetMGM gold + Covers grid).
# Drop images into /images; GitHub Actions runs this and appends only CLEAN rows to splits.csv.

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

ALLOWED_MARKETS = {"ML", "Moneyline", "Spread", "Total"}

# ---------- Utilities ----------
def now_ts() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

def ensure_dirs():
    os.makedirs(DEBUG_DIR, exist_ok=True)

def clean_pct(x: str) -> int:
    try:
        return int(re.sub(r"[^\d]", "", str(x)))
    except Exception:
        return 0

def norm_team(s: str) -> str:
    if not s: return ""
    s = re.sub(r"[\u200b\u2010-\u2015]", "-", s)          # weird dashes/zero-width
    s = re.sub(r"[^A-Za-z0-9 .'\-]", " ", s)              # strip junk
    s = re.sub(r"\s+", " ", s).strip(" .-")               # collapse spaces/trim
    return s

def preprocess(img: Image.Image) -> Image.Image:
    g = ImageOps.grayscale(img)
    w, h = g.size
    scale = 2.0 if max(w, h) < 1600 else 1.3
    g = g.resize((int(w*scale), int(h*scale)), Image.LANCZOS)
    g = ImageOps.autocontrast(g)
    g = g.filter(ImageFilter.MedianFilter(3)).filter(ImageFilter.UnsharpMask(radius=2, percent=150))
    return g

def ocr_best(img: Image.Image):
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

# ---------- Clean/validate helpers ----------
def looks_like_team(name: str) -> bool:
    if not name: return False
    name = name.strip()
    return bool(re.search(r"[A-Za-z]{3,}", name)) and ((" " in name) or len(name) >= 5)

def pct_ok(x) -> bool:
    try:
        v = int(x)
        return 0 <= v <= 100
    except:
        return False

def is_valid_row(r: dict) -> bool:
    if r.get("market") not in ALLOWED_MARKETS: return False
    if not looks_like_team(r.get("away_team", "")): return False
    if not looks_like_team(r.get("home_team", "")): return False
    t, h = r.get("tickets_pct", 0), r.get("handle_pct", 0)
    if not (pct_ok(t) and pct_ok(h)): return False
    if int(t) == 0 and int(h) == 0: return False
    if r["away_team"].lower() == r["home_team"].lower(): return False
    return True

def canonical_key(r: dict) -> tuple:
    return (
        r.get("away_team","").lower().strip(),
        r.get("home_team","").lower().strip(),
        r.get("market","").upper().strip(),
        str(r.get("line","")).strip(),
        int(r.get("tickets_pct",0)),
        int(r.get("handle_pct",0)),
        r.get("source","").upper().strip(),
    )

def write_csv(path: str, rows: list, header: list):
    exists = os.path.exists(path)
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=header)
        if not exists:
            w.writeheader()
        for r in rows:
            r.setdefault("league","Unknown")
            r.setdefault("line","")
            r["tickets_pct"] = int(r.get("tickets_pct", 0) or 0)
            r["handle_pct"]  = int(r.get("handle_pct", 0) or 0)
            w.writerow(r)

# ---------- Parsers ----------
def parse_mgm_gold(txt: str) -> List[Dict]:
    rows = []
    lines = [l for l in txt.splitlines() if re.search(r"[A-Za-z]", l) and re.search(r"\d{1,3}\s*%", l)]
    if not lines:
        return rows

    recs = []
    for line in lines:
        lead = re.split(r"\d{1,3}\s*%", line, maxsplit=1)[0]
        team = norm_team(lead)
        if not team or len(team) < 3:
            m = re.search(r"([A-Za-z .'\-]{3,})", line)
            team = norm_team(m.group(1)) if m else "Unknown"

        pcts = re.findall(r"(\d{1,3})\s*%", line)
        bets = clean_pct(pcts[0]) if len(pcts) >= 1 else 0
        handle = clean_pct(pcts[1]) if len(pcts) >= 2 else 0

        m_ml = re.search(r"(^|[^\d])([+-]\d{2,3})(?!\d)", line)
        ml = m_ml.group(2) if m_ml else ""

        m_sp = re.search(r"([+-]\d+(?:\.\d+)?)", line)
        sp = m_sp.group(1) if m_sp else ""

        recs.append({"team": team, "bets": bets, "handle": handle, "ml": ml, "sp": sp})

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
        mteam = re.match(r"\s*([A-Za-z .'\-]{3,})\s+", ln)
        if not mteam:
            continue
        t = norm_team(mteam.group(1))
        pcts = re.findall(r"(\d{1,3})\s*%", ln)
        bets = handle = 0
        if len(pcts) >= 2:
            bets = clean_pct(pcts[-1])   # rightmost pair on row
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
        # --- this block was missing indent last time ---
        goods, rejects = [], []
        seen = set()
        for r in all_rows:
            if is_valid_row(r):
                key = canonical_key(r)
                if key not in seen:
                    seen.add(key)
                    goods.append(r)
            else:
                rejects.append(r)

        if goods:
            write_csv(OUT_FILE, goods, FIELDNAMES)
            print(f"[CLEAN] Appended {len(goods)} good row(s) → {OUT_FILE}")
        else:
            print("[CLEAN] No valid rows to append.")

        if rejects:
            rej_path = os.path.join(DEBUG_DIR, "rejects.csv")
            write_csv(rej_path, rejects, FIELDNAMES)
            print(f"[CLEAN] Wrote {len(rejects)} rejected row(s) → {rej_path}")
    else:
        print("No valid rows parsed from any image.")

if __name__ == "__main__":
    main()
