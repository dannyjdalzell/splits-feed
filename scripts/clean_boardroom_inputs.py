#!/usr/bin/env python3
import csv, json, re, sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
inp  = ROOT / "audit_out" / "boardroom_inputs.csv"
outp = ROOT / "audit_out" / "boardroom_inputs.csv"   # in-place clean
rej  = ROOT / "audit_out" / "unresolved_from_boardroom.txt"

LEAGUES  = {"NFL","NCAAF","NBA","NCAAB","MLB","NHL","UFC/MMA"}
MARKETS  = {"ML","Spread","Total"}
BAD_PAT  = re.compile(r"(Estimating|SPORTSBOOK|Expanded Splits|Total Bets|CFB -)", re.I)
ALPHAOK  = re.compile(r"[A-Za-z]")

# load team dictionaries (strict resolve required)
dict_dir = ROOT / "dictionaries"
team_maps = []
for f in ["nfl.json","ncaaf_fbs_seed.json","nba.json","ncaab.json","mlb.json","nhl.json"]:
    p = dict_dir / f
    if p.exists():
        try:
            team_maps.append(json.loads(p.read_text()))
        except Exception:
            pass

def resolvable(name: str) -> bool:
    if not name or not ALPHAOK.search(name): return False
    n = name.strip().lower()
    for m in team_maps:
        if n in (k.lower() for k in m.keys()): return True
        if n in (str(v).lower() for v in m.values()): return True
    return False

if not inp.exists():
    print("[clean] no boardroom_inputs.csv found; nothing to do")
    sys.exit(0)

rows = list(csv.DictReader(inp.open()))
keep, drop = [], []

for r in rows:
    lg  = (r.get("league") or "").strip()
    aw  = (r.get("away_team") or "").strip()
    hm  = (r.get("home_team") or "").strip()
    mkt = (r.get("market") or "").strip()
    tp  = (r.get("tickets_pct") or "").strip()
    hp  = (r.get("handle_pct") or "").strip()

    # hard filters
    if lg not in LEAGUES:                          drop.append((r,"league"));   continue
    if mkt not in MARKETS:                         drop.append((r,"market"));   continue
    if BAD_PAT.search(aw) or BAD_PAT.search(hm):   drop.append((r,"badstr"));   continue
    if not resolvable(aw) or not resolvable(hm):   drop.append((r,"resolve"));  continue
    if not tp and not hp:                          drop.append((r,"empty%"));   continue

    keep.append(r)

# write back (in-place overwrite)
if keep:
    with outp.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=keep[0].keys())
        w.writeheader()
        w.writerows(keep)

# log rejections
if drop:
    rej.parent.mkdir(parents=True, exist_ok=True)
    with rej.open("w") as f:
        for r, why in drop:
            f.write(f"[{why}] {r}\n")

print(f"[clean] kept={len(keep)} dropped={len(drop)} → {outp}")
if drop:
    print(f"[clean] details → {rej}")
