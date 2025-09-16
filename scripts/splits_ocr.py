# scripts/splits_ocr.py
# v12-multibrand — parses MGM, FanDuel, DraftKings, CircaSports, and generic grids
import os, csv, re, sys
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Tuple
from PIL import Image, ImageOps, ImageFilter, ImageEnhance
import pytesseract

# -------- repo paths --------
REPO_ROOT = Path(__file__).resolve().parents[1]
IN_DIR    = REPO_ROOT / "images"
OUT_FILE  = REPO_ROOT / "splits.csv"

FIELDNAMES = ["away_team","home_team","market","tickets_pct","handle_pct","line","source"]

# -------- utils --------
def now_ts() -> str:
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")

def preprocess(img: Image.Image) -> Image.Image:
    g = ImageOps.grayscale(img)
    g = g.filter(ImageFilter.MedianFilter(3))
    g = ImageEnhance.Contrast(g).enhance(1.5)
    return g

def ocr_best(img: Image.Image) -> Tuple[int,int,str]:
    """
    Try several PSMs; keep the one with most 'table-ish' tokens.
    """
    best = (0, 6, "")
    for psm in (6, 11, 4):
        cfg = f'--oem 3 --psm {psm}'
        txt = pytesseract.image_to_string(img, lang="eng", config=cfg)
        score = len(re.findall(r"(?:%|bets?|handle|[+-]\d{2,3}|O/?U|ML|Spread|Total)", txt, flags=re.I))
        if score > best[0]:
            best = (score, psm, txt)
    return best

def looks_like_splits(text: str) -> bool:
    if not text:
        return False
    # require some %s and the words that show it’s a table of markets
    has_pct   = text.count("%") >= 2
    has_words = re.search(r"(bets?|handle|spread|total|ml|moneyline|o/?u)", text, re.I) is not None
    has_rows  = text.count("\n") > 3
    return has_pct and has_words and has_rows

TEAM_RE = r"[A-Z][A-Za-z\.\s&'\-]{2,}"  # permissive team token
def _norm(s:str) -> str:
    return re.sub(r"\s+"," ", s.replace("—","-").replace("–","-").replace("−","-")).strip()

def _teams_from(line: str) -> Tuple[str,str]:
    m = re.search(rf"({TEAM_RE})\s+at\s+({TEAM_RE})", line, re.I)
    if m:
        return _norm(m.group(1)), _norm(m.group(2))
    return "",""

def _hunt_block(block_lines: List[str]) -> Dict[str,Dict]:
    """
    Pull ML/Spread/Total %bets, %handle, and best guess line from a small window.
    Returns dict like:
      {"ML": {"b":int,"h":int,"line":str}, "Spread": {...}, "Total": {...}}
    """
    blk = " | ".join(block_lines)
    out = {"ML":{"b":0,"h":0,"line":""},
           "Spread":{"b":0,"h":0,"line":""},
           "Total":{"b":0,"h":0,"line":""}}

    # Generic % finders (accepts 'Bets' or 'Tickets', 'Handle' or 'Money')
    def pct(key:str) -> int:
        m = re.search(key, blk, re.I)
        return int(m.group(1)) if m else 0

    bets_any   = pct(r"(?:Bets|Tickets)\D{0,6}(\d{1,3})\s*%?")
    handle_any = pct(r"(?:Handle|Money)\D{0,6}(\d{1,3})\s*%?")

    # Market targeting
    # ML
    ml_line = ""
    m_ml = re.search(r"(?:ML|Moneyline)\D{0,8}([+-]?\d{2,3})", blk, re.I)
    if m_ml: ml_line = _norm(m_ml.group(1))
    out["ML"] = {"b":bets_any, "h":handle_any, "line":ml_line}

    # Spread (includes RL/PL/puck/run line)
    sp_line = ""
    m_sp = re.search(r"(?:Spread|RL|PL|Run\s*Line|Puck\s*Line)\D{0,8}([+-]\d+(?:\.\d+)?)\D{0,6}([+-]?\d{2,3})?", blk, re.I)
    if m_sp:
        sp_line = _norm(f"{m_sp.group(1)} {(m_sp.group(2) or '').strip()}")
    out["Spread"] = {"b":bets_any, "h":handle_any, "line":sp_line.strip()}

    # Total (O/U)
    to_line = ""
    m_to = re.search(r"(?:Total|O/?U)\D{0,6}([OU]?)\s*(\d+(?:\.\d+)?)\D{0,6}([+-]?\d{2,3})?", blk, re.I)
    if m_to:
        # Keep points + optional price; drop the leading O/U letter to keep CSV simple
        to_line = _norm(f"{m_to.group(2)} {(m_to.group(3) or '').strip()}")
    out["Total"] = {"b":bets_any, "h":handle_any, "line":to_line.strip()}

    return out

# ---------------- brand parsers ----------------
def parse_pairs_by_windows(lines: List[str], win:int=6) -> List[Tuple[str,str,Dict]]:
    """Find '<Away> at <Home>' lines and return (away,home,markets) for each."""
    pairs = []
    idxs = []
    for i, ln in enumerate(lines):
        a,h = _teams_from(ln)
        if a and h:
            idxs.append((i,a,h))
    for i,a,h in idxs:
        lo, hi = max(0,i-win), min(len(lines), i+win+1)
        markets = _hunt_block(lines[lo:hi])
        pairs.append((a,h,markets))
    return pairs

def parse_mgm(text:str) -> List[Dict]:
    lines = [x for x in (l.strip() for l in text.splitlines()) if x]
    rows = []
    for a,h,mk in parse_pairs_by_windows(lines, win=6):
        for k in ("ML","Spread","Total"):
            b,hp = mk[k]["b"], mk[k]["h"]
            ln   = mk[k]["line"]
            if b or hp or ln:
                rows.append({"away_team":a,"home_team":h,"market":k,
                             "tickets_pct":b,"handle_pct":hp,"line":ln,"source":"MGM_FAM"})
    return rows

def parse_fanduel(text:str) -> List[Dict]:
    # FanDuel grids still look like ML/Spread/Total + % Bets/Handle; reuse core
    rows = parse_mgm(text)
    for r in rows: r["source"] = "FANDUEL_GRID"
    return rows

def parse_dk(text:str) -> List[Dict]:
    # DraftKings tables: similar tokens, sometimes 'TIX'/'HNDL' in caps
    t = re.sub(r"TIX","Bets", text, flags=re.I)
    t = re.sub(r"HNDL","Handle", t, flags=re.I)
    rows = parse_mgm(t)
    for r in rows: r["source"] = "DRAFTKINGS_GRID"
    return rows

def parse_circa(text:str) -> List[Dict]:
    # Circa Sports: often “Circa Sports” header; markets are the same
    rows = parse_mgm(text)
    for r in rows: r["source"] = "CIRCA_GRID"
    return rows

def parse_generic_grid(text:str) -> List[Dict]:
    # Fallback: try the same windowed pair scan
    rows = parse_mgm(text)
    for r in rows:
        if r.get("source") in (None, "MGM_FAM"):
            r["source"] = "GRID_OCR"
    return rows

def detect_brand(text:str, fname:str) -> str:
    low = (text or "").lower()
    fn  = fname.lower()
    if "betmgm" in low or "mgm" in low or "betmgm" in fn or "mgm" in fn:
        return "MGM"
    if "fanduel" in low or "fanduel" in fn or "fd_" in fn:
        return "FANDUEL"
    if "draftkings" in low or "draft kings" in low or "dk" in fn or "draftkings" in fn:
        return "DRAFTKINGS"
    if "circa" in low or "circasports" in low or "circa" in fn:
        return "CIRCA"
    # allow custom typos: "braco" -> treat as generic grid for now
    if "braco" in low or "bracosports" in low or "braco" in fn:
        return "GENERIC"
    return "GENERIC"

def parse_text_by_brand(text:str, fname:str) -> List[Dict]:
    brand = detect_brand(text, fname)
    if brand == "MGM":        return parse_mgm(text)
    if brand == "FANDUEL":    return parse_fanduel(text)
    if brand == "DRAFTKINGS": return parse_dk(text)
    if brand == "CIRCA":      return parse_circa(text)
    return parse_generic_grid(text)

# ---------------- main ----------------
def main():
    all_rows: List[Dict] = []

    if not IN_DIR.exists():
        print(f"[ERR] images/ folder not found at {IN_DIR}")
        sys.exit(1)

    img_paths = sorted([p for p in IN_DIR.glob("*") if p.suffix.lower() in (".png",".jpg",".jpeg",".webp")])
    if not img_paths:
        print("[INFO] No images to process.")
        return

    for p in img_paths:
        try:
            img = Image.open(p)
        except Exception:
            print(f"[ERR] Cannot open: {p.name}")
            continue

        pim = preprocess(img)
        score, psm, text = ocr_best(pim)

        if not looks_like_splits(text):
            print(f"[SKIP] {p.name} — not a splits table (psm={psm}, score={score})")
            continue

        rows = parse_text_by_brand(text, p.name)

        # sanitize + enforce ints
        clean_rows = []
        for r in rows:
            r["away_team"]  = _norm(r.get("away_team",""))
            r["home_team"]  = _norm(r.get("home_team",""))
            r["market"]     = r.get("market","")
            r["tickets_pct"] = int(r.get("tickets_pct") or 0)
            r["handle_pct"]  = int(r.get("handle_pct") or 0)
            r["line"]        = _norm(r.get("line",""))
            r["source"]      = r.get("source","GRID_OCR")
            # require at least a market + one of (% or line) otherwise skip
            if r["market"] and (r["tickets_pct"] or r["handle_pct"] or r["line"]):
                clean_rows.append(r)

        if clean_rows:
            print(f"[OK] {p.name}: {len(clean_rows)} row(s)")
            all_rows.extend(clean_rows)
        else:
            print(f"[WARN] {p.name}: passed pre-check but no rows parsed")

    if not all_rows:
        print("No valid rows parsed from any image.")
        return

    exists = OUT_FILE.exists()
    with open(OUT_FILE, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDNAMES)
        if not exists:
            w.writeheader()
        for r in all_rows:
            w.writerow({
                "away_team":  r["away_team"],
                "home_team":  r["home_team"],
                "market":     r["market"],
                "tickets_pct":r["tickets_pct"],
                "handle_pct": r["handle_pct"],
                "line":       r["line"],
                "source":     r["source"],
            })

    print(f"Appended {len(all_rows)} row(s) → {OUT_FILE}")

if __name__ == "__main__":
    main()
