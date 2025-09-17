#!/usr/bin/env python3
import os, io, re, sys, csv, json, glob, time, math, pathlib, datetime
from dataclasses import dataclass
from typing import Dict, List, Optional
from zoneinfo import ZoneInfo

# --- minimal OCR stack ---
try:
    import pytesseract
    from PIL import Image, ImageOps
except Exception as e:
    print(f"[WARN] OCR deps missing: {e}", file=sys.stderr)

REPO = pathlib.Path(__file__).resolve().parent.parent
IMAGES = REPO / "images"
OUT_CSV = REPO / "splits.csv"
TZ = ZoneInfo("America/Chicago")
UTC = ZoneInfo("UTC")

# Load profiles
PROFILES_PATH = os.environ.get("OCR_PROFILES_FILE", str(REPO / "scripts" / "ocr_profiles.yaml"))

def load_yaml(path:str) -> dict:
    # no external yaml dep; tiny parser for the structures we wrote
    data = {"common":{}, "families":{}}
    cur = []
    last_indent = 0
    stack = [data]
    with open(path, "r", encoding="utf-8") as f:
        for raw in f:
            line = raw.rstrip("\n")
            if not line.strip() or line.strip().startswith("#"): 
                continue
            indent = len(line) - len(line.lstrip())
            keyval = line.strip()
            while indent < last_indent:
                stack.pop()
                last_indent -= 2
            if keyval.endswith(":"):
                key = keyval[:-1]
                if key in ("common","families"):
                    stack[-1][key] = {}
                    stack.append(stack[-1][key])
                else:
                    stack[-1][key] = {}
                    stack.append(stack[-1][key])
                last_indent = indent + 2
            else:
                if ":" in keyval:
                    k,v = keyval.split(":",1)
                    k = k.strip(); v = v.strip()
                    if v.startswith("[") and v.endswith("]"):
                        # parse list of comma-separated items possibly quoted
                        items = []
                        buf = v[1:-1].strip()
                        if buf:
                            for part in buf.split(","):
                                items.append(part.strip().strip('"').strip("'"))
                        stack[-1][k] = items
                    elif v.startswith("{") and v.endswith("}"):
                        # tiny dict parse: {"a": "...", "b": "..."} not used here
                        stack[-1][k] = json.loads(v.replace("'",'"'))
                    elif v:
                        stack[-1][k] = v.strip().strip('"').strip("'")
                    else:
                        stack[-1][k] = ""
    return data

CFG = load_yaml(PROFILES_PATH)
COMMON = CFG.get("common",{})
FAMS   = CFG.get("families",{})

PCT_TOKENS = set([t.lower() for t in COMMON.get("percent_tokens",[])])
MKT = {k: [s.lower() for s in v] for k,v in COMMON.get("market_tokens",{}).items()}

PERCENT_RX = re.compile(r'\b(100|\d{1,2})%\b')
ODDS_RX = re.compile(r'(?<!\d)[-+]\d{3,4}(?!\d)')
TEAM_LINE_RX = re.compile(r'([A-Za-z .&-]{2,})\s+(?:@|vs\.?|at)\s+([A-Za-z .&-]{2,})', re.IGNORECASE)

@dataclass
class Row:
    timestamp: str
    league: str
    away_team: str
    home_team: str
    market: str
    tickets_pct: str
    handle_pct: str
    line: str
    source: str

def to_utc_iso(ts_local: float) -> str:
    dt_local = datetime.datetime.fromtimestamp(ts_local, TZ)
    return dt_local.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S+00:00")

def detect_family(text: str) -> Optional[str]:
    t = text.lower()
    for fam, spec in FAMS.items():
        hits = spec.get("detect_any",[])
        for h in hits:
            if h.lower() in t:
                return fam
    return None

def detect_market(text: str) -> str:
    t = text.lower()
    for mk, tokens in MKT.items():
        for tok in tokens:
            if tok in t:
                if mk == "moneyline": return "ML"
                if mk == "spread":    return "Spread"
                if mk == "total":     return "Total"
    # fallback: infer by presence
    if "over" in t or "under" in t or "o/u" in t: return "Total"
    return "Unknown"

def parse_percents(text: str) -> (str,str):
    # crude: take first two percent tokens as tickets, handle
    vals = PERCENT_RX.findall(text)
    if len(vals) >= 2:
        return vals[0], vals[1]
    if len(vals) == 1:
        return vals[0], ""
    return "", ""

def parse_line(text: str) -> str:
    # prefer spread like +3.5 / -2.5; else first odds token
    sp = re.search(r'(?<!\d)[-+]\d(?:\.\d)?(?!\d)', text)
    if sp: return sp.group(0)
    ml = ODDS_RX.search(text)
    return ml.group(0) if ml else ""

def parse_matchup(text: str) -> (str,str):
    m = TEAM_LINE_RX.search(text)
    if m:
        away = m.group(1).strip()
        home = m.group(2).strip()
        # trim weird dots
        return re.sub(r'\s+',' ',away), re.sub(r'\s+',' ',home)
    # fallback: split by newline heuristics
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    if len(lines) >= 2:
        return lines[0][:32], lines[1][:32]
    return "", ""

def ocr_image(path: pathlib.Path) -> str:
    try:
        img = Image.open(path).convert("L")
        img = ImageOps.autocontrast(img)
        return pytesseract.image_to_string(img)
    except Exception as e:
        return ""

def should_skip(name: str) -> bool:
    n = name.lower()
    if n.startswith(("pregame_","smoke_")): return True
    if not n.endswith((".png",".jpg",".jpeg",".webp",".tif",".tiff",".bmp",".gif",".heic")): return True
    return False

def main():
    images = sorted([p for p in IMAGES.iterdir() if p.is_file() and not should_skip(p.name)],
                    key=lambda p: p.stat().st_mtime)
    if not images:
        print("[INFO] No images to OCR.")
        return

    rows_out: List[Row] = []
    for p in images:
        text = ocr_image(p)
        if not text.strip():
            continue

        fam = detect_family(text) or "TW_OTHER_FAM"
        market = detect_market(text)
        tix, hnd = parse_percents(text)
        ln = parse_line(text)
        away, home = parse_matchup(text)

        # Only keep if we actually saw split-like content
        has_signal = any([tix, hnd, ln]) or ("%" in text)
        if not has_signal:
            continue

        ts_iso = to_utc_iso(p.stat().st_mtime)
        # league best-effort: detect MLB/NFL/NBA/NHL keywords
        L = "Unknown"
        tl = text.lower()
        if "mlb" in tl: L="MLB"
        elif "nfl" in tl: L="NFL"
        elif "nba" in tl: L="NBA"
        elif "nhl" in tl: L="NHL"

        rows_out.append(Row(ts_iso, L, away, home, market, tix, hnd, ln, fam))

    if not rows_out:
        print("[INFO] No OCR rows extracted.")
        return

    # ensure header exists; append safely
    header = ["timestamp","league","away_team","home_team","market","tickets_pct","handle_pct","line","source"]
    exists = OUT_CSV.exists()
    if not exists:
        with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(header)

    with open(OUT_CSV, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=header)
        for r in rows_out:
            w.writerow(r.__dict__)

    print(f"[OK] OCR appended {len(rows_out)} rows to {OUT_CSV.name}")

if __name__ == "__main__":
    main()
