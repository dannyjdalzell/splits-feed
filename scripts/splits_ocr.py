#!/usr/bin/env python3
import os, sys, re, csv, datetime
from pathlib import Path

try:
    from PIL import Image
    import pytesseract
except Exception as e:
    print(f"[OCR] Missing deps: {e}", file=sys.stderr)
    sys.exit(0)  # don't fail the workflow; just exit cleanly

IMAGES_DIR = Path("images")
CSV_PATH   = Path("splits.csv")

# ---- output schema (9 cols, keep this EXACT) ----
CSV_FIELDS = [
    "timestamp",      # ISO
    "league",         # MLB/NFL/NBA/Unknown
    "away_team",
    "home_team",
    "market",         # ML/Spread/Total/Unknown
    "tickets_pct",    # 0-100 or ''
    "handle_pct",     # 0-100 or ''
    "line",           # e.g., -135 / +7.5 / 221.5
    "source",         # e.g., Pregame
]

# ---- team dictionaries (minimal but effective) ----
MLB = [
    "Yankees","Red Sox","Blue Jays","Rays","Orioles","Guardians","Tigers","Royals","Twins","White Sox",
    "Astros","Mariners","Athletics","Rangers","Angels","Braves","Mets","Phillies","Marlins","Nationals",
    "Cubs","Cardinals","Pirates","Brewers","Reds","Dodgers","Giants","Padres","Rockies","Diamondbacks"
]
NFL = [
    "Patriots","Jets","Bills","Dolphins","Chiefs","Chargers","Raiders","Broncos","Cowboys","Eagles","Giants","Commanders",
    "Packers","Bears","Vikings","Lions","Saints","Falcons","Panthers","Buccaneers","Rams","49ers","Seahawks","Cardinals",
    "Ravens","Steelers","Browns","Bengals","Titans","Colts","Jaguars","Texans"
]
NBA = [
    "Lakers","Warriors","Celtics","Knicks","Nets","Bulls","Heat","76ers","Bucks","Raptors","Hawks","Cavaliers","Pistons",
    "Pacers","Magic","Wizards","Hornets","Mavericks","Rockets","Spurs","Grizzlies","Pelicans","Nuggets","Timberwolves",
    "Thunder","Trail Blazers","Jazz","Kings","Clippers","Suns"
]
TEAM_MAP = {t:"MLB" for t in MLB} | {t:"NFL" for t in NFL} | {t:"NBA" for t in NBA}

def infer_league(text:str)->str:
    txt = text.lower()
    hits = [TEAM_MAP[t] for t in TEAM_MAP if t.lower() in txt]
    if not hits: return "Unknown"
    return max(set(hits), key=hits.count)

def find_two_teams(text:str):
    seen = []
    for t in TEAM_MAP:
        if re.search(rf"\b{re.escape(t)}\b", text, flags=re.I):
            if t not in seen:
                seen.append(t)
            if len(seen)==2: break
    if len(seen)<2:
        m = re.search(r"([A-Za-z .'-]{2,})\s+(?:vs|at)\s+([A-Za-z .'-]{2,})", text, flags=re.I)
        if m:
            return m.group(1).strip(), m.group(2).strip()
        return None, None
    return seen[0], seen[1]

def pct_or_blank(s:str):
    if s is None: return ""
    m = re.search(r"(\d{1,3})(?:\s*%)?", s)
    if not m: return ""
    val = int(m.group(1))
    val = max(0, min(100, val))
    return str(val)

def extract_line(text:str):
    m = re.search(r"([+-]?\d{1,3}(?:\.\d)?)\s*(?:ML|Moneyline)?", text)
    return m.group(1) if m else ""

def detect_source(text:str)->str:
    return "Pregame" if "pregame" in text.lower() else "Unknown"

def detect_market(text:str)->str:
    t = text.lower()
    if "ml" in t or "moneyline" in t: return "ML"
    if "spread" in t or re.search(r"[+-]\d+(\.\d)?", t): return "Spread"
    if "total" in t or re.search(r"\b\d{3}\.?\d?\b", t): return "Total"
    return "Unknown"

def ocr_text(image_path:Path)->str:
    try:
        img = Image.open(image_path)
    except Exception as e:
        print(f"[OCR] Cannot open {image_path}: {e}", file=sys.stderr)
        return ""
    try:
        return pytesseract.image_to_string(img)
    except Exception as e:
        print(f"[OCR] Tesseract failed on {image_path}: {e}", file=sys.stderr)
        return ""

def parse_pregame(text:str):
    away, home = find_two_teams(text)
    if not away or not home:
        return None
    pcts = re.findall(r"(\d{1,3})\s*%", text)
    tickets = pct_or_blank(pcts[0]) if len(pcts)>=1 else ""
    handle  = pct_or_blank(pcts[1]) if len(pcts)>=2 else ""
    league = infer_league(text)
    market = detect_market(text)
    line   = extract_line(text)
    return {
        "timestamp":  datetime.datetime.utcnow().replace(microsecond=0).isoformat()+"+00:00",
        "league":     league,
        "away_team":  away,
        "home_team":  home,
        "market":     market,
        "tickets_pct":tickets,
        "handle_pct": handle,
        "line":       line,
        "source":     "Pregame",
    }

def parse_generic(text:str):
    away, home = find_two_teams(text)
    if not away or not home:
        return None
    league = infer_league(text)
    return {
        "timestamp":  datetime.datetime.utcnow().replace(microsecond=0).isoformat()+"+00:00",
        "league":     league,
        "away_team":  away,
        "home_team":  home,
        "market":     "Unknown",
        "tickets_pct":"",
        "handle_pct": "",
        "line":       "",
        "source":     detect_source(text),
    }

def ensure_csv(path:Path):
    if not path.exists():
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=CSV_FIELDS)
            w.writeheader()

def append_row(row:dict):
    ensure_csv(CSV_PATH)
    reduced = {k: row.get(k, "") for k in CSV_FIELDS}
    with open(CSV_PATH, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        w.writerow(reduced)

def handle_image(p:Path):
    txt = ocr_text(p)
    if not txt.strip():
        print(f"[SKIP] Empty OCR for {p.name}")
        return
    row = None
    if "pregame" in txt.lower():
        row = parse_pregame(txt)
    if row is None:
        row = parse_pregame(txt)
    if row is None:
        row = parse_generic(txt)
    if row is None:
        print(f"[SKIP] Unrecognized layout: {p.name}")
        return
    append_row(row)
    print(f"[OK] {p.name} -> {row['league']} {row['away_team']} @ {row['home_team']} ({row['market']})")

def main():
    if not IMAGES_DIR.exists():
        print(f"[INFO] No images dir at {IMAGES_DIR.resolve()}; nothing to do.")
        return
    imgs = [p for p in IMAGES_DIR.iterdir() if p.suffix.lower() in (".png",".jpg",".jpeg",".webp")]
    if not imgs:
        print("[INFO] No images to process.")
        return
    for p in sorted(imgs):
        try:
            handle_image(p)
        except Exception as e:
            print(f"[ERROR] {p.name}: {e}", file=sys.stderr)

if __name__ == "__main__":
    main()
