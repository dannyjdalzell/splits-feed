# scripts/splits_ocr.py
# v12-precheck — skips non-splits images before parsing
import os, csv, re, time, sys
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Tuple
from PIL import Image, ImageOps, ImageFilter, ImageEnhance
import pytesseract

# -------- paths / output --------
REPO_ROOT = Path(__file__).resolve().parents[1]
IN_DIR    = REPO_ROOT / "images"
OUT_FILE  = REPO_ROOT / "splits.csv"

FIELDNAMES = ["away_team","home_team","market","tickets_pct","handle_pct","line","source"]

# -------- helpers --------
def now_ts() -> str:
    # ISO-ish, no timezone conversion (runner is UTC)
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")

def preprocess(img: Image.Image) -> Image.Image:
    # gentle denoise + contrast; keep robust (don’t oversharpen)
    g = ImageOps.grayscale(img)
    g = g.filter(ImageFilter.MedianFilter(3))
    g = ImageEnhance.Contrast(g).enhance(1.5)
    return g

def ocr_best(img: Image.Image) -> Tuple[int,int,str]:
    """
    Try several PSMs and pick the text with the most tokens that look like
    tables: %, 'bets/handle', numbers, +/-, O/U, etc.
    """
    candidates = []
    for psm in (6, 11, 4):
        cfg = f'--oem 3 --psm {psm}'
        txt = pytesseract.image_to_string(img, lang="eng", config=cfg)
        score = len(re.findall(r"(?:%|bets?|handle|[+-]\d{2,3}|O/?U|ML|Spread|Total)", txt, flags=re.I))
        candidates.append((score, psm, txt))
    candidates.sort(reverse=True, key=lambda x: x[0])
    return candidates[0]

# ---------- NEW: quick pre-check so we skip promos/posters ----------
def looks_like_splits(text: str) -> bool:
    """
    Heuristic: require (a) at least two % signs, (b) table'y words,
    and (c) more than a couple lines of OCR text.
    """
    if text is None:
        return False
    has_pct   = text.count("%") >= 2
    has_words = re.search(r"(bets?|handle|spread|total|ml|moneyline|run line|rl)", text, re.I) is not None
    has_rows  = text.count("\n") > 3
    return has_pct and has_words and has_rows

# ---------- parsing ----------
TEAM_RE = r"[A-Z][A-Za-z\.\s&'-]{2,}"  # loose, but works ok on OCR text

def _clean_pct(x: str) -> int:
    m = re.search(r"(\d{1,3})\s*%?", x)
    return int(m.group(1)) if m else 0

def _clean_line(x: str) -> str:
    # keep things like +115, -120, 8.5 -115, etc.
    x = x.strip()
    # Normalize weird OCR dashes
    x = x.replace("−", "-").replace("—","-").replace("–","-")
    return x

def _norm_space(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()

def _maybe_teams_from_line(line: str) -> Tuple[str,str]:
    """
    Try to pull "<Away> at <Home>" from a line. If not, return ("","").
    """
    m = re.search(rf"({TEAM_RE})\s+at\s+({TEAM_RE})", line, re.I)
    if m:
        away = _norm_space(m.group(1))
        home = _norm_space(m.group(2))
        return away, home
    return "",""

def parse_mgm_block(text: str) -> List[Dict]:
    """
    Very tolerant parser for MGM black/gold tables and FanDuel/Action “grid” style.
    Strategy:
      1) Find likely team–pair lines (“X at Y”).
      2) For each pair, scan nearby lines for % Bets / % Handle for 3 markets:
         ML, Spread, Total. We accept partial data (0 if missing).
    """
    rows: List[Dict] = []
    lines = [l for l in (x.strip() for x in text.splitlines()) if l]
    if not lines:
        return rows

    # index team lines
    team_idx = []
    for i, line in enumerate(lines):
        a, h = _maybe_teams_from_line(line)
        if a and h:
            team_idx.append((i, a, h))

    if not team_idx:
        return rows

    # window around a match to hunt for markets
    WIN = 6

    def hunt(block: List[str], label_keywords, line_hint=None):
        """
        Find 'bets' and 'handle' near keywords.
        """
        bets = handle = 0
        line_str = ""
        blk = " | ".join(block)

        # % Bets / % Handle
        mb = re.search(r"Bets?\D{0,6}(\d{1,3})\s*%?", blk, re.I)
        mh = re.search(r"Handle\D{0,6}(\d{1,3})\s*%?", blk, re.I)
        if mb: bets = int(mb.group(1))
        if mh: handle = int(mh.group(1))

        # line (odds / spread / total)
        mline = re.search(r"([+-]?\d{2,3})(?!\d)", blk)
        mtot  = re.search(r"\b(?:O|U)\s*?(\d+(?:\.\d+)?)\s*([+-]?\d{2,3})?", blk, re.I)
        msp   = re.search(r"([+-]\d+(?:\.\d+)?)\s*([+-]?\d{2,3})?", blk)

        if any(k in blk.lower() for k in label_keywords):
            if mtot:
                line_str = _clean_line(f"{mtot.group(1)} {mtot.group(2) or ''}".strip())
            elif msp:
                line_str = _clean_line(" ".join([msp.group(1), (msp.group(2) or "")]).strip())
            elif mline:
                line_str = _clean_line(mline.group(1))

        # If we didn’t find keywords, still allow lines that show strong % signals
        if not line_str and (bets or handle):
            if mtot:
                line_str = _clean_line(f"{mtot.group(1)} {mtot.group(2) or ''}".strip())
            elif msp:
                line_str = _clean_line(" ".join([msp.group(1), (msp.group(2) or "")]).strip())
            elif mline:
                line_str = _clean_line(mline.group(1))

        return bets, handle, line_str

    for idx, away, home in team_idx:
        # window of text around the pair
        lo = max(0, idx - WIN)
        hi = min(len(lines), idx + WIN + 1)
        block = lines[lo:hi]

        # Three “passes” targeting ML / Spread / Total
        # (keywords broadened to catch slightly different templates)
        ml_b, ml_h, ml_line = hunt(block, ["ml","moneyline"])
        sp_b, sp_h, sp_line = hunt(block, ["spread","run line","rl","-1.5","+1.5"])
        to_b, to_h, to_line = hunt(block, ["total","o ","u "])  # space to avoid 'totally'

        if ml_b or ml_h or ml_line:
            rows.append({"away_team":away,"home_team":home,"market":"ML",
                         "tickets_pct":ml_b,"handle_pct":ml_h,"line":ml_line,"source":"MGM_FAM"})
        if sp_b or sp_h or sp_line:
            rows.append({"away_team":away,"home_team":home,"market":"Spread",
                         "tickets_pct":sp_b,"handle_pct":sp_h,"line":sp_line,"source":"MGM_FAM"})
        if to_b or to_h or to_line:
            rows.append({"away_team":away,"home_team":home,"market":"Total",
                         "tickets_pct":to_b,"handle_pct":to_h,"line":to_line,"source":"MGM_FAM"})

    return rows

def parse_text(text: str) -> List[Dict]:
    """Top-level parse (we can add more brand-specific parsers later)."""
    return parse_mgm_block(text)

# -------- main --------
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

        # ---------- pre-check here ----------
        if not looks_like_splits(text):
            print(f"[SKIP] {p.name} — not a splits table (psm={psm}, score={score})")
            continue

        rows = parse_text(text)

        # attach timestamp here to keep CSV narrow & stable
        ts = now_ts()
        for r in rows:
            r.setdefault("source", "MGM_FAM")
            # if we failed to grab % earlier, keep them explicit zeros
            r["tickets_pct"] = int(r.get("tickets_pct") or 0)
            r["handle_pct"]  = int(r.get("handle_pct") or 0)

        if rows:
            print(f"[OK] {p.name}: {len(rows)} row(s)")
            all_rows.extend(rows)
        else:
            print(f"[WARN] {p.name}: passed pre-check but parser found 0 rows")

    if not all_rows:
        print("No valid rows parsed from any image.")
        return

    # Write/append CSV (no timestamp column in this version)
    exists = OUT_FILE.exists()
    with open(OUT_FILE, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDNAMES)
        if not exists:
            w.writeheader()
        for r in all_rows:
            w.writerow({
                "away_team":  r.get("away_team",""),
                "home_team":  r.get("home_team",""),
                "market":     r.get("market",""),
                "tickets_pct":int(r.get("tickets_pct",0)),
                "handle_pct": int(r.get("handle_pct",0)),
                "line":       r.get("line",""),
                "source":     r.get("source","MGM_FAM"),
            })

    print(f"Appended {len(all_rows)} row(s) → {OUT_FILE}")

if __name__ == "__main__":
    main()
