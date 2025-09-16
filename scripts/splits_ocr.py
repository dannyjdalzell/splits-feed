# scripts/splits_ocr.py
# ------------------------------------------------------------
# OCR → parse tweet screenshots (grid & BetMGM gold tables)
# Writes/updates splits.csv at repo root, with LEAGUE inferred.
# ------------------------------------------------------------

import os, re, csv
from datetime import datetime
from pathlib import Path
import pytesseract
from PIL import Image, ImageOps, ImageFilter

# ---------- IO & schema ----------
REPO_ROOT = Path(__file__).resolve().parents[1]
IN_DIR    = REPO_ROOT / "images"
OUT_FILE  = REPO_ROOT / "splits.csv"

FIELDNAMES = [
    "timestamp", "league", "away_team", "home_team",
    "market", "tickets_pct", "handle_pct", "line", "source"
]

# ---------- small utils ----------
def now_ts() -> str:
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")

def preprocess(img):
    g = ImageOps.grayscale(img)
    g = ImageOps.autocontrast(g)
    g = g.filter(ImageFilter.SHARPEN)
    return g

def ocr_best(img):
    cands = []
    for psm in (6, 4, 11):
        cfg = f'--oem 3 --psm {psm}'
        txt = pytesseract.image_to_string(img, lang="eng", config=cfg)
        score = len(re.findall(r"(bets?|handle|%|[+-]\d{2,3}|\d{1,2}\.5|\d{1,3}\s?%)", txt, flags=re.I))
        cands.append((score, psm, txt))
    cands.sort(reverse=True)
    return cands[0]  # (score, psm, text)

# ---------- league inference ----------
NFL_WORDS  = {"nfl","week","spread","total","moneyline","chargers","raiders","cowboys","patriots","packers","chiefs","steelers","jets","giants","vikings","browns","bears","eagles","saints","lions"}
MLB_WORDS  = {"mlb","mlb games","moneyline","run line","yankees","dodgers","braves","rays","cardinals","cubs","reds","astros","giants","phillies","nationals","blue jays","rays","orioles"}
NCAAF_WORDS= {"college football","cfb","ncaaf","week","alabama","georgia","clemson","ohio state","texas","usc","notre dame","tennessee","oregon","michigan","florida state"}

def infer_league(text: str, filename: str) -> str:
    t = f"{filename.lower()} {text.lower()}"
    # strong headers
    if re.search(r"\bMLB\b|\bMLB Games\b", t): return "MLB"
    if re.search(r"\bCollege Football\b|\bCFB\b|\bNCAAF\b", t): return "NCAAF"
    if re.search(r"\bNFL\b|\bNFL Week\b", t): return "NFL"
    # keyword hits
    if sum(1 for w in MLB_WORDS   if w in t) >= 2: return "MLB"
    if sum(1 for w in NCAAF_WORDS if w in t) >= 2: return "NCAAF"
    if sum(1 for w in NFL_WORDS   if w in t) >= 2: return "NFL"
    # folder hint (optional: images/nfl, images/mlb, images/cfb)
    p = Path(filename).as_posix().lower()
    if "/nfl/" in p: return "NFL"
    if "/mlb/" in p: return "MLB"
    if "/cfb/" in p or "/ncaaf/" in p or "/college/" in p: return "NCAAF"
    return "Unknown"

# ---------- team-name sanitizer ----------
TEAM_JUNK = re.compile(r"\s*(?:[-+]?(\d+(?:\.\d+)?)\b|O|U)\s*")

def clean_team_name(s: str) -> str:
    out = TEAM_JUNK.sub(" ", s or "").strip()
    out = re.sub(r"\s{2,}", " ", out)
    out = re.sub(r"\bat\s*$", "", out, flags=re.I).strip()
    return out

# ---------- parsers ----------
TEAM_LINE = re.compile(
    r"""^\s*([A-Za-z0-9\.\'\-\&\s]+?)\s+(?:(?:[+\-]?\d+(?:\.\d+)?)\s*)?
        (?:(\d{1,3})\s*%.*?(\d{1,3})\s*%)""",
    re.I | re.X,
)

ML_SIG  = re.compile(r"([+-]\d{2,3})")
SP_SIG  = re.compile(r"([+-]\d+(?:\.\d+)?)")
TOT_SIG = re.compile(r"\b(\d{1,2}\.5|\d{1,2})\b")

def parse_grid_blocks(text: str, league_hint: str):
    rows = []
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    recs = []
    for ln in lines:
        m = TEAM_LINE.search(ln)
        if not m: 
            continue
        team = m.group(1).strip()
        perc = re.findall(r"(\d{1,3})\s*%", ln)
        bets = handle = None
        if len(perc) >= 2:
            bets, handle = int(perc[0]), int(perc[1])
        ml  = ML_SIG.search(ln)
        sp  = SP_SIG.search(ln)
        tot = TOT_SIG.search(ln)
        recs.append({
            "team": team,
            "bets": bets, "handle": handle,
            "ml": ml.group(1) if ml else "",
            "sp": sp.group(1) if sp else "",
            "tot": tot.group(1) if tot else "",
        })

    for i in range(0, len(recs) - 1, 2):
        away, home = recs[i], recs[i+1]
        ts = now_ts()
        league = league_hint or "Unknown"
        src = "GRID_OCR"

        def addrow(market, line):
            if away["bets"] is None or home["bets"] is None: 
                return
            rows.append({
                "timestamp": ts, "league": league,
                "away_team": away["team"], "home_team": home["team"],
                "market": market,
                "tickets_pct": home["bets"],
                "handle_pct": home["handle"] if home["handle"] is not None else 0,
                "line": line or "",
                "source": src,
            })

        addrow("ML",     home["ml"]  or away["ml"])
        addrow("Spread", home["sp"]  or away["sp"])
        addrow("Total",  home["tot"] or away["tot"])

    return rows

def parse_mgm_gold(text: str, league_hint: str):
    rows = []
    tlines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    league = league_hint or ("MLB" if re.search(r"\bMLB\b", text, re.I) else
                             "NCAAF" if re.search(r"\bCollege Football\b|\bCFB\b", text, re.I) else
                             "NFL" if re.search(r"\bNFL\b", text, re.I) else
                             "Unknown")

    for idx, ln in enumerate(tlines):
        m = re.search(r"([A-Za-z0-9\.\'\-\&\s]+)\s+at\s+([A-Za-z0-9\.\'\-\&\s]+)", ln, re.I)
        if not m: 
            continue
        away_name, home_name = m.group(1).strip(), m.group(2).strip()
        window = "\n".join(tlines[idx: idx+4])
        pc = re.findall(r"(\d{1,3})\s*%", window)
        bets = handle = None
        if len(pc) >= 2:
            bets, handle = int(pc[0]), int(pc[1])
        ml  = ML_SIG.search(window)
        sp  = SP_SIG.search(window)
        tot = TOT_SIG.search(window)

        ts = now_ts()
        src = "MGM_FAM"

        def addrow(market, line):
            rows.append({
                "timestamp": ts, "league": league,
                "away_team": away_name, "home_team": home_name,
                "market": market,
                "tickets_pct": bets if bets is not None else 0,
                "handle_pct": handle if handle is not None else 0,
                "line": line or "",
                "source": src,
            })

        addrow("ML",     ml.group(1)  if ml  else "")
        addrow("Spread", sp.group(1)  if sp  else "")
        addrow("Total",  tot.group(1) if tot else "")

    return rows

def parse_blocks(text: str, filename: str):
    league = infer_league(text, filename)
    out = []
    out += parse_grid_blocks(text, league)
    out += parse_mgm_gold(text, league)

    # Dedup + clean team names + drop obvious junk
    seen, uniq = set(), []
    for r in out:
        r["away_team"] = clean_team_name(r.get("away_team", ""))
        r["home_team"] = clean_team_name(r.get("home_team", ""))
        if not r["away_team"] or not r["home_team"]:
            continue
        if re.search(r"\d", r["away_team"]) or re.search(r"\d", r["home_team"]):
            continue
        sig = (r["league"], r["away_team"], r["home_team"], r["market"], r["line"], r["source"])
        if sig in seen: 
            continue
        seen.add(sig)
        uniq.append(r)
    return uniq

# ---------- main ----------
def main():
    all_rows = []
    if not IN_DIR.exists():
        print(f"[WARN] images/ not found at: {IN_DIR}")
        return

    img_files = []
    for ext in (".png", ".jpg", ".jpeg", ".webp"):
        img_files.extend(sorted(IN_DIR.glob(f"*{ext}")))
    if not img_files:
        print("[INFO] No images to process.")
        return

    for path in img_files:
        try:
            img = Image.open(path)
        except Exception:
            print(f"[ERR] Cannot open: {path.name}")
            continue

        pim = preprocess(img)
        score, psm, txt = ocr_best(pim)
        rows = parse_blocks(txt, path.name)

        if rows:
            print(f"[OK] {path.name}: {len(rows)} row(s) (psm={psm}, score={score})")
            all_rows += rows
        else:
            print(f"[WARN] No rows parsed from: {path.name}")

    if not all_rows:
        print("No valid rows parsed from any image.")
        return

    exists = OUT_FILE.exists()
    with OUT_FILE.open("a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDNAMES)
        if not exists:
            w.writeheader()
        for r in all_rows:
            w.writerow(r)
    print(f"Appended {len(all_rows)} row(s) → {OUT_FILE}")

if __name__ == "__main__":
    main()
