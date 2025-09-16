# scripts/splits_ocr.py
# ------------------------------------------------------------
# OCR → parse tweet screenshots (grid & BetMGM gold tables)
# Writes/updates splits.csv at repo root.
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
    # ISO without TZ (GitHub runners are UTC)
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")

def preprocess(img: Image.Image) -> Image.Image:
    # robust-ish prep for screenshots
    g = ImageOps.grayscale(img)
    g = ImageOps.autocontrast(g)
    g = g.filter(ImageFilter.SHARPEN)
    return g

def ocr_best(img: Image.Image):
    # Try several PSMs; pick the one with most "bets/handle/%/+/-/digits"
    cands = []
    for psm in (6, 4, 11):
        cfg = f'--oem 3 --psm {psm}'
        txt = pytesseract.image_to_string(img, lang="eng", config=cfg)
        score = len(re.findall(r"(bets?|handle|%|[+-]\d{2,3}|\d{1,3}\s?%)", txt, flags=re.I))
        cands.append((score, psm, txt))
    cands.sort(reverse=True)
    return cands[0]  # (score, psm, text)

# ---------- team-name sanitizer ----------
TEAM_JUNK = re.compile(r"\s*(?:[-+]?(\d+(?:\.\d+)?)\b|O|U)\s*")

def clean_team_name(s: str) -> str:
    out = TEAM_JUNK.sub(" ", s or "").strip()
    out = re.sub(r"\s{2,}", " ", out)
    out = re.sub(r"\bat\s*$", "", out, flags=re.I).strip()
    return out

# ---------- parsers ----------
TEAM_LINE = re.compile(
    r"""  # A line that likely contains a team + some numeric cells
    ^\s*([A-Za-z0-9\.\'\-\&\s]+?)\s+           # team-ish text
    (?:(?:[+\-]?\d+(?:\.\d+)?)\s*)?            # optional spread/total token nearby
    (?:
        (\d{1,3})\s*%.*?(\d{1,3})\s*%          # two % figures (bets / handle OR handle / bets)
    )
    .*?$""",
    re.I | re.X,
)

ML_SIG = re.compile(r"([+-]\d{2,3})")
SP_SIG = re.compile(r"([+-]\d+(?:\.\d+)?)")
TOT_SIG = re.compile(r"\b(\d{1,2}\.5|\d{1,2})\b")

def parse_grid_blocks(text: str):
    """
    Generic 'grid' (Covers/Action/ESPN) two-line row pairs.
    Returns list of row dicts for ML/Spread/Total where we have %s.
    """
    rows = []
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

    # Heuristic: build records with team + bets%/handle% and nearby line tokens per row,
    # then pair (away, home) sequentially.
    recs = []
    for ln in lines:
        m = TEAM_LINE.search(ln)
        if not m:
            continue
        team = m.group(1).strip()
        # try to find which order the percents appeared; we’ll normalize later
        percents = re.findall(r"(\d{1,3})\s*%", ln)
        bets = handle = None
        if len(percents) >= 2:
            # many grids arrange: Handle … Bets (or vice versa). We cannot be sure.
            # Use the first two as (bets, handle) but later we’ll flip if “bets > handle and label implies Handle”
            # Since we don’t have labels per-line, keep the simple assumption.
            p1, p2 = percents[0], percents[1]
            bets = int(p1)
            handle = int(p2)

        # Line signals (moneyline/spread/total) – might or might not appear on the same row
        ml = ML_SIG.search(ln)
        sp = SP_SIG.search(ln)
        tot = TOT_SIG.search(ln)

        recs.append({
            "team": team,
            "bets": bets, "handle": handle,
            "ml": ml.group(1) if ml else "",
            "sp": sp.group(1) if sp else "",
            "tot": tot.group(1) if tot else "",
        })

    # Pair as (away, home)
    for i in range(0, len(recs) - 1, 2):
        away = recs[i]
        home = recs[i+1]
        ts = now_ts()
        league = "Unknown"  # we can refine separately if needed
        src = "GRID_OCR"

        # Moneyline
        if away["bets"] is not None and home["bets"] is not None:
            # Prefer to take the %s from the favorite row for ML (but grids vary),
            # keep it simple: use home’s %s (consistently on many feeds).
            rows.append({
                "timestamp": ts, "league": league,
                "away_team": away["team"], "home_team": home["team"],
                "market": "ML",
                "tickets_pct": home["bets"] if home["bets"] is not None else 0,
                "handle_pct": home["handle"] if home["handle"] is not None else 0,
                "line": home["ml"] or away["ml"],
                "source": src,
            })

        # Spread
        if away["bets"] is not None and home["bets"] is not None:
            rows.append({
                "timestamp": ts, "league": league,
                "away_team": away["team"], "home_team": home["team"],
                "market": "Spread",
                "tickets_pct": home["bets"] if home["bets"] is not None else 0,
                "handle_pct": home["handle"] if home["handle"] is not None else 0,
                "line": home["sp"] or away["sp"],
                "source": src,
            })

        # Total
        if away["bets"] is not None and home["bets"] is not None:
            ln = home["tot"] or away["tot"]
            rows.append({
                "timestamp": ts, "league": league,
                "away_team": away["team"], "home_team": home["team"],
                "market": "Total",
                "tickets_pct": home["bets"] if home["bets"] is not None else 0,
                "handle_pct": home["handle"] if home["handle"] is not None else 0,
                "line": ln,
                "source": src,
            })

    return rows

def parse_mgm_gold(text: str):
    """
    BetMGM gold table (MLB Games / College Football Week …).
    We scan for lines that look like 'Team A at Team B' and then
    fish % and line tokens that follow on the same or nearby lines.
    """
    rows = []
    tlines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    league = "Unknown"
    if re.search(r"\bMLB\b", text, re.I): league = "MLB"
    if re.search(r"\bCollege Football\b|\bCFB\b", text, re.I): league = "NCAAF"
    if re.search(r"\bNFL\b", text, re.I): league = "NFL"

    # Find matchups "X at Y"
    for idx, ln in enumerate(tlines):
        m = re.search(r"([A-Za-z0-9\.\'\-\&\s]+)\s+at\s+([A-Za-z0-9\.\'\-\&\s]+)", ln, re.I)
        if not m:
            continue
        away_name = m.group(1).strip()
        home_name = m.group(2).strip()

        window = "\n".join(tlines[idx: idx+4])  # small context window
        # Pull bets/handle for ML/Spread/Total if present
        # These tables usually show three columns of %s (we’ll reuse the same %s across markets if needed).
        pc = re.findall(r"(\d{1,3})\s*%", window)
        bets = handle = None
        if len(pc) >= 2:
            # BetMGM layout typically shows %Bets then %Handle together
            bets = int(pc[0]); handle = int(pc[1])

        ml = ML_SIG.search(window)
        sp = SP_SIG.search(window)
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

        addrow("ML", ml.group(1) if ml else "")
        addrow("Spread", sp.group(1) if sp else "")
        addrow("Total", tot.group(1) if tot else "")

    return rows

def parse_blocks(text: str):
    # try both patterns; union unique rows (by tuple)
    out = []
    for block in (parse_grid_blocks(text), parse_mgm_gold(text)):
        out.extend(block)

    # Deduplicate by signature
    seen = set()
    uniq = []
    for r in out:
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
        img_files.extend(sorted(p for p in IN_DIR.glob(f"*{ext}")))
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
        rows = parse_blocks(txt)

        # Clean & filter before accumulating
        cleaned = []
        for r in rows:
            r["away_team"] = clean_team_name(r.get("away_team", ""))
            r["home_team"] = clean_team_name(r.get("home_team", ""))
            # drop if team fields still have digits (clearly not a name)
            if re.search(r"\d", r["away_team"]) or re.search(r"\d", r["home_team"]):
                continue
            cleaned.append(r)

        if cleaned:
            print(f"[OK] {path.name}: {len(cleaned)} row(s)")
            all_rows += cleaned
        else:
            print(f"[WARN] No rows parsed from: {path.name}")

    if not all_rows:
        print("No valid rows parsed from any image.")
        return

    # Append to CSV (create header if missing)
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
