# scripts/splits_ocr.py
# ------------------------------------------------------------
# OCR tweet screenshots → splits.csv
# - Per-account league/layout lock
# - Detects Grid vs BetMGM gold
# - Writes audit summary for sanity checks
# ------------------------------------------------------------

import os, re, csv
from datetime import datetime
from pathlib import Path
from collections import defaultdict, Counter
import pytesseract
from PIL import Image, ImageOps, ImageFilter

# ---------- IO ----------
REPO_ROOT = Path(__file__).resolve().parents[1]
IN_DIR    = REPO_ROOT / "images"
OUT_FILE  = REPO_ROOT / "splits.csv"

FIELDNAMES = [
    "timestamp", "league", "away_team", "home_team",
    "market", "tickets_pct", "handle_pct", "line", "source"
]

# ---------- Account policy (edit me) ----------
# layout: "GRID", "MGM", or "AUTO"
# league: "NFL" | "MLB" | "NCAAF" | "AUTO"
ACCOUNT_RULES = {
    "covers":           {"layout": "GRID", "league": "AUTO"},
    "betmgm":           {"layout": "MGM",  "league": "AUTO"},
    "betmgmnews":       {"layout": "MGM",  "league": "AUTO"},
    "vsinlive":         {"layout": "GRID", "league": "AUTO"},
    "actionnetworkhq":  {"layout": "GRID", "league": "AUTO"},
    # add more handles here as needed
}

# ---------- utils ----------
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
        score = len(re.findall(r"(bets?|handle|%|[+-]\d{2,3}|\d{1,2}\.5|\bO\b|\bU\b)", txt, re.I))
        cands.append((score, psm, txt))
    cands.sort(reverse=True)
    return cands[0]  # score, psm, text

# ---------- league/layout inference ----------
NFL_WORDS   = {"nfl","chargers","raiders","cowboys","patriots","chiefs","steelers","jets","giants","vikings","browns","bears","eagles","saints","lions"}
MLB_WORDS   = {"mlb","yankees","dodgers","braves","rays","cardinals","cubs","reds","astros","giants","phillies","nationals","blue jays","orioles"}
NCAAF_WORDS = {"college football","cfb","ncaaf","alabama","georgia","clemson","ohio state","texas","usc","tennessee","oregon","michigan","florida state","notre dame"}

def infer_league_auto(text: str) -> str:
    t = text.lower()
    if re.search(r"\bmlb\b|\bmlb games\b", t): return "MLB"
    if re.search(r"\bnfl\b|\bnfl week\b", t): return "NFL"
    if re.search(r"\bcollege football\b|\bcfb\b|\bncaaf\b", t): return "NCAAF"
    if sum(w in t for w in MLB_WORDS)   >= 2: return "MLB"
    if sum(w in t for w in NCAAF_WORDS) >= 2: return "NCAAF"
    if sum(w in t for w in NFL_WORDS)   >= 2: return "NFL"
    return "Unknown"

def handle_from_filename(name: str) -> str:
    # Expect filenames like 'covers_abcdef.jpg'
    base = Path(name).name
    m = re.match(r"([a-z0-9_]+)[\-_]", base.lower())
    return m.group(1) if m else ""

def locked_layout_for(handle: str) -> str:
    cfg = ACCOUNT_RULES.get(handle, {})
    return (cfg.get("layout") or "AUTO").upper()

def locked_league_for(handle: str) -> str:
    cfg = ACCOUNT_RULES.get(handle, {})
    return (cfg.get("league") or "AUTO").upper()

# ---------- team-name cleaning ----------
TEAM_JUNK = re.compile(r"\s*(?:[-+]?(\d+(?:\.\d+)?)\b|O|U)\s*")

def clean_team_name(s: str) -> str:
    out = TEAM_JUNK.sub(" ", s or "").strip()
    out = re.sub(r"\s{2,}", " ", out)
    out = re.sub(r"\bat\s*$", "", out, flags=re.I).strip()
    return out

# ---------- common extractors ----------
ML_SIG  = re.compile(r"([+-]\d{2,3})")
SP_SIG  = re.compile(r"([+-]\d+(?:\.\d+)?)")
TOT_SIG = re.compile(r"\b(\d{1,2}(?:\.5)?)\b")
PCT2    = re.compile(r"(\d{1,3})\s*%.*?(\d{1,3})\s*%")

# Grid row: Team ... Bets xx%  Handle yy%  (plus maybe ML/Spread/Total bits in same line)
TEAM_LINE = re.compile(
    r"""^\s*([A-Za-z0-9\.\'\-\&\s]+?)\s+(?:(?:[+\-]?\d+(?:\.\d+)?)\s*)?
        (?:(\d{1,3})\s*%.*?(\d{1,3})\s*%)""",
    re.I | re.X
)

def parse_grid(text: str, league: str):
    rows = []
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    recs = []
    for ln in lines:
        m = TEAM_LINE.search(ln)
        if not m: 
            continue
        team = m.group(1).strip()
        bets = int(m.group(2)) if m.group(2) else None
        handle = int(m.group(3)) if m.group(3) else None
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

    ts = now_ts()
    for i in range(0, len(recs) - 1, 2):
        away, home = recs[i], recs[i+1]
        def add(market, line):
            if away["bets"] is None or home["bets"] is None: return
            rows.append({
                "timestamp": ts, "league": league,
                "away_team": away["team"], "home_team": home["team"],
                "market": market,
                "tickets_pct": home["bets"],
                "handle_pct": home["handle"] if home["handle"] is not None else 0,
                "line": line or "",
                "source": "GRID_OCR",
            })
        add("ML",     home["ml"]  or away["ml"])
        add("Spread", home["sp"]  or away["sp"])
        add("Total",  home["tot"] or away["tot"])
    return rows

def parse_mgm(text: str, league: str):
    rows = []
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    ts = now_ts()
    for i, ln in enumerate(lines):
        m = re.search(r"([A-Za-z0-9\.\'\-\&\s]+)\s+at\s+([A-Za-z0-9\.\'\-\&\s]+)", ln, re.I)
        if not m: 
            continue
        away = m.group(1).strip()
        home = m.group(2).strip()
        win = "\n".join(lines[i:i+4])
        pc  = PCT2.search(win)
        bets = int(pc.group(1)) if pc else 0
        handle = int(pc.group(2)) if pc else 0
        ml  = ML_SIG.search(win)
        sp  = SP_SIG.search(win)
        tot = TOT_SIG.search(win)
        def add(market, sig, source="MGM_FAM"):
            line = sig.group(1) if sig else ""
            rows.append({
                "timestamp": ts, "league": league,
                "away_team": away, "home_team": home,
                "market": market, "tickets_pct": bets, "handle_pct": handle,
                "line": line, "source": source,
            })
        add("ML",     ml)
        add("Spread", sp)
        add("Total",  tot)
    return rows

def detect_layout(text: str) -> str:
    gold = bool(re.search(r"\bBETMGM\b|\bMLB Games\b|\bCollege Football Week\b", text, re.I))
    grid = bool(re.search(r"\bHandle\b.*\bBets\b", text, re.I))
    if gold and not grid: return "MGM"
    if grid and not gold: return "GRID"
    if gold and grid:     return "MGM"  # prefer gold if both patterns hit
    return "AUTO"

def parse_blocks(text: str, filename: str, acct_lock_layout: str, acct_lock_league: str, audit):
    # league
    inferred_league = infer_league_auto(text)
    league = inferred_league if acct_lock_league == "AUTO" else acct_lock_league

    # layout
    inferred_layout = detect_layout(text)
    layout = inferred_layout if acct_lock_layout == "AUTO" else acct_lock_layout

    if acct_lock_league != "AUTO" and inferred_league not in ("Unknown", acct_lock_league):
        audit["league_mismatch"] += 1
        audit["league_mismatch_details"].append((filename, acct_lock_league, inferred_league))

    if acct_lock_layout != "AUTO" and inferred_layout not in ("AUTO", acct_lock_layout):
        audit["layout_mismatch"] += 1
        audit["layout_mismatch_details"].append((filename, acct_lock_layout, inferred_layout))

    # parse
    if layout == "MGM":
        rows = parse_mgm(text, league)
    elif layout == "GRID":
        rows = parse_grid(text, league)
    else:
        # fallback: try both
        rows = parse_mgm(text, league)
        if not rows:
            rows = parse_grid(text, league)

    # clean + dedup
    seen, out = set(), []
    for r in rows:
        r["away_team"] = clean_team_name(r["away_team"])
        r["home_team"] = clean_team_name(r["home_team"])
        if not r["away_team"] or not r["home_team"]:
            audit["dropped_blank_teams"] += 1
            continue
        if re.search(r"\d", r["away_team"]) or re.search(r"\d", r["home_team"]):
            audit["dropped_numeric_teams"] += 1
            continue
        sig = (r["league"], r["away_team"], r["home_team"], r["market"], r["line"], r["source"])
        if sig in seen: 
            continue
        seen.add(sig)
        out.append(r)
    return out, league, layout, inferred_league, inferred_layout

# ---------- main ----------
def main():
    if not IN_DIR.exists():
        print(f"[WARN] images/ not found: {IN_DIR}")
        return

    img_files = []
    for ext in (".png", ".jpg", ".jpeg", ".webp"):
        img_files.extend(sorted(IN_DIR.glob(f"*{ext}")))

    if not img_files:
        print("[INFO] No images to process.")
        return

    all_rows = []
    audit_global = Counter()
    audit_by_acct = defaultdict(lambda: Counter())
    layout_count  = Counter()
    league_count  = Counter()

    for path in img_files:
        handle = handle_from_filename(path.name)
        acct_cfg_layout = locked_layout_for(handle)
        acct_cfg_league = locked_league_for(handle)

        # local audit counters per image
        audit_local = Counter(dropped_blank_teams=0, dropped_numeric_teams=0,
                              league_mismatch=0, layout_mismatch=0)
        audit_local["league_mismatch_details"] = []
        audit_local["layout_mismatch_details"] = []

        try:
            img = Image.open(path)
        except Exception:
            print(f"[ERR] cannot open: {path.name}")
            continue

        pim = preprocess(img)
        score, psm, txt = ocr_best(pim)
        rows, league, layout, inf_league, inf_layout = parse_blocks(
            txt, path.name, acct_cfg_layout, acct_cfg_league, audit_local
        )

        if rows:
            print(f"[OK] {path.name} ({handle or 'unknown'}): {len(rows)} row(s) | layout={layout} (inf={inf_layout}) league={league} (inf={inf_league}) psm={psm} score={score}")
            all_rows += rows
            layout_count[layout] += 1
            league_count[league] += 1
        else:
            print(f"[WARN] No rows from: {path.name} ({handle or 'unknown'}) | layout={layout} (inf={inf_layout}) league={league} (inf={inf_league})")

        # roll up audits
        for k, v in audit_local.items():
            if isinstance(v, int):
                audit_global[k] += v
                audit_by_acct[handle][k] += v

    if not all_rows:
        print("No valid rows parsed from any image.")
        print("--- AUDIT SUMMARY ---")
        print(f"Layouts seen: {dict(layout_count)}")
        print(f"Leagues seen: {dict(league_count)}")
        return

    exists = OUT_FILE.exists()
    with OUT_FILE.open("a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDNAMES)
        if not exists: w.writeheader()
        for r in all_rows: w.writerow(r)

    print(f"Appended {len(all_rows)} row(s) → {OUT_FILE}")

    # -------- AUDIT SUMMARY --------
    print("\n--- AUDIT SUMMARY ---")
    print(f"Layouts used: {dict(layout_count)}")
    print(f"Leagues used: {dict(league_count)}")
    print(f"Dropped (blank teams): {audit_global['dropped_blank_teams']}")
    print(f"Dropped (numeric teams): {audit_global['dropped_numeric_teams']}")
    print(f"Account/league mismatches: {audit_global['league_mismatch']}")
    print(f"Account/layout mismatches: {audit_global['layout_mismatch']}")
    for acct, cnt in audit_by_acct.items():
        if acct:
            print(f"  - @{acct}: {dict(cnt)}")

if __name__ == "__main__":
    main()
