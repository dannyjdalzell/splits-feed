#!/usr/bin/env python3
import os, re, sys, csv, subprocess, pathlib, datetime
from zoneinfo import ZoneInfo

REPO = pathlib.Path(__file__).resolve().parent.parent
IMAGES = REPO / "images"
OUT_CSV = REPO / "splits.csv"
TZ = ZoneInfo("America/Chicago"); UTC = ZoneInfo("UTC")

PERCENT_RX = re.compile(r'\b(100|\d{1,2})%\b')
ODDS_RX    = re.compile(r'(?<!\d)[-+]\d{3,4}(?!\d)')
SPREAD_RX  = re.compile(r'(?<!\d)[-+]\d(?:\.\d)?(?!\d)')
TEAM_RX    = re.compile(r'([A-Za-z .&-]{2,})\s+(?:@|vs\.?|at)\s+([A-Za-z .&-]{2,})', re.I)

FAMS = {
  "DK_FAM": ["draftkings","draft kings","bets %","handle %"],
  "FD_FAM": ["fanduel","fd sportsbook","fan duel","bets %","handle %"],
  "CIRCA_FAM": ["circa sports","expanded splits","circa"],
  "MGM_FAM": ["betmgm","%bets","%handle","opening","current","mlb games"],
  "CAESARS_FAM": ["caesars sportsbook","caesars"],
  "SUPERBOOK_FAM": ["superbook","westgate"],
  "BRACCO_FAM": ["betting splits","bracco"],
  "BOL_FAM": ["betonline","dave mason","bol"],
  "COVERS_FAM": ["covers","covers.com","consensus"],
}

def to_utc_iso(ts):
    return datetime.datetime.fromtimestamp(ts, TZ).astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S+00:00")

def skip(name:str)->bool:
    n=name.lower()
    if n.startswith(("pregame_","smoke_")): return True
    return not n.endswith((".png",".jpg",".jpeg",".webp",".tif",".tiff",".bmp",".gif",".heic"))

def ocr(path: pathlib.Path)->str:
    try:
        out = subprocess.check_output(["tesseract", str(path), "stdout"], stderr=subprocess.STDOUT, timeout=60)
        return out.decode("utf-8", errors="ignore")
    except subprocess.CalledProcessError as e:
        return e.output.decode("utf-8", errors="ignore")
    except Exception:
        return ""

def fam_of(text:str)->str:
    t=text.lower()
    for fam, keys in FAMS.items():
        for k in keys:
            if k in t: return fam
    return "TW_OTHER_FAM"

def market_of(text:str)->str:
    t=text.lower()
    if "over/under" in t or "o/u" in t or ("over" in t and "under" in t): return "Total"
    if "spread" in t or "runline" in t or "puck line" in t or "handicap" in t: return "Spread"
    if "moneyline" in t or "money line" in t or " ml " in t: return "ML"
    if SPREAD_RX.search(t): return "Spread"
    if ODDS_RX.search(t): return "ML"
    return "Unknown"

def percents(text:str):
    vals = PERCENT_RX.findall(text)
    if len(vals)>=2: return vals[0], vals[1]
    if len(vals)==1: return vals[0], ""
    return "", ""

def line_of(text:str):
    m=SPREAD_RX.search(text); 
    if m: return m.group(0)
    m=ODDS_RX.search(text); 
    return m.group(0) if m else ""

def matchup(text:str):
    m=TEAM_RX.search(text)
    if m:
        a=m.group(1).strip(); h=m.group(2).strip()
        return re.sub(r'\s+',' ',a)[:64], re.sub(r'\s+',' ',h)[:64]
    lines=[l.strip() for l in text.splitlines() if l.strip()]
    return (lines[0][:64], lines[1][:64]) if len(lines)>=2 else ("","")

def likely_split(text:str)->bool:
    # accept if 2+ % tokens, or (% and odds/spread)
    pct = len(PERCENT_RX.findall(text))
    return pct>=2 or (pct>=1 and (ODDS_RX.search(text) or SPREAD_RX.search(text)))

def league_guess(text:str)->str:
    t=text.lower()
    if "mlb" in t: return "MLB"
    if "nfl" in t: return "NFL"
    if "nba" in t: return "NBA"
    if "nhl" in t: return "NHL"
    return "Unknown"

def main():
    imgs = sorted([p for p in IMAGES.iterdir() if p.is_file() and not skip(p.name)],
                  key=lambda p: p.stat().st_mtime)
    if not imgs:
        print("[INFO] No images.")
        return
    header = ["timestamp","league","away_team","home_team","market","tickets_pct","handle_pct","line","source"]
    new=[]
    for img in imgs:
        text = ocr(img)
        if not text.strip(): 
            continue
        if not likely_split(text):
            continue
        fam = fam_of(text)
        market = market_of(text)
        tix,hnd = percents(text)
        line = line_of(text)
        away,home = matchup(text)
        ts_iso = to_utc_iso(img.stat().st_mtime)
        L = league_guess(text)
        new.append({
            "timestamp": ts_iso, "league": L,
            "away_team": away, "home_team": home,
            "market": market, "tickets_pct": tix,
            "handle_pct": hnd, "line": line, "source": fam
        })
    if not new:
        print("[INFO] No OCR rows extracted.")
        return
    exists = OUT_CSV.exists()
    with open(OUT_CSV, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=header)
        if not exists: w.writeheader()
        for r in new: w.writerow(r)
    print(f"[OK] OCR appended {len(new)} rows to {OUT_CSV.name}")
if __name__ == "__main__":
    main()
