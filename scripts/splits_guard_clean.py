#!/usr/bin/env python3
# splits_guard_clean.py — drop OCR junk, keep resolvable teams, promote → splits.csv
import os, re, json, sys
from datetime import datetime
import pandas as pd

TEAM_JSON = "scripts/team_dictionary.json"
SRC_CANDIDATES = [
    "audit_out/splits_staged.csv",
    "splits_staged.csv",
    "splits.csv",
]
OUT_CLEAN = "splits_clean.csv"
OUT_FLAG  = "splits_flagged.csv"
PROMOTE   = "splits.csv"

NOISE_RX = re.compile("|".join([
    r"Estimating resolution as \d+",
    r"Betting Splits Expanded Splits",
    r"^Spread$", r"^Total Bets?$", r"^Handle$", r"^Money$",
    r"^EF s?rr?ss?ro?soe?", r"^BRACCO", r"^SPORTSBOOK", r"^rs ",
    r"^Kansas City Chiefs ad$", r"^UC Davis Aggies$"
]), re.I)

def load_df(path):
    try:
        if os.path.exists(path):
            return pd.read_csv(path)
    except Exception:
        pass
    return None

def load_team_map():
    if not os.path.exists(TEAM_JSON):
        return {}, set()
    data = json.load(open(TEAM_JSON, "r", encoding="utf-8"))
    alias_to_canon = {}
    canon_set = set()

    def add(canon_key, aliases):
        ck = str(canon_key or "").strip().upper()
        if not ck:
            return
        canon_set.add(ck)
        for a in aliases:
            s = str(a or "").strip().upper()
            if s:
                alias_to_canon.setdefault(s, ck)

    # flat dict: {"KC":[...]}
    if isinstance(data, dict) and all(isinstance(v, list) for v in data.values()):
        for k, vs in data.items():
            add(k, [k] + vs)
    # list of dicts with abr/city/name/mascot
    elif isinstance(data, list):
        for row in data:
            if not isinstance(row, dict): continue
            abr  = row.get("abr") or row.get("abbrev")
            city = row.get("city") or row.get("team") or row.get("school")
            name = row.get("name") or row.get("mascot")
            aliases = [x for x in [abr, city, name, f"{city} {name}" if city and name else None] if x]
            add(abr or (city or name), aliases)
    # nested conferences map
    else:
        for _, teams in data.items():
            if not isinstance(teams, dict): continue
            for team, meta in teams.items():
                if not isinstance(meta, dict): continue
                abr = meta.get("abbrev") or meta.get("abr") or team
                add(abr, [abr, team])

    return alias_to_canon, canon_set

def resolve_team(text, alias_to_canon):
    t = str(text or "")
    for alias, canon in alias_to_canon.items():
        if re.search(rf"\b{re.escape(alias)}\b", t, re.I):
            return canon
    return None

def main():
    src = next((c for c in SRC_CANDIDATES if os.path.exists(c)), None)
    if not src:
        print("[guard] no splits source found"); sys.exit(0)

    df = load_df(src)
    if df is None or df.empty:
        print("[guard] source empty"); sys.exit(0)

    alias_to_canon, canon_set = load_team_map()

    def row_text(row):
        return " ".join("" if pd.isna(v) else str(v) for v in row.to_dict().values())

    keep, flagged = [], []
    for _, r in df.iterrows():
        txt = row_text(r)
        if NOISE_RX.search(txt):
            flagged.append((txt, "noise")); continue

        away = str(r.get("away_team", "") or "")
        home = str(r.get("home_team", "") or "")
        away_c = resolve_team(f"{away} {txt}", alias_to_canon)
        home_c = resolve_team(f"{home} {txt}", alias_to_canon)

        if not away_c and not home_c:
            flagged.append((txt, "no_team")); continue

        ro = dict(r)
        ro["away_team"] = away_c or away
        ro["home_team"] = home_c or home

        if ro["away_team"] and ro["away_team"] == ro["home_team"]:
            flagged.append((txt, "same_team")); continue

        keep.append(ro)

    clean = pd.DataFrame(keep)
    flags = pd.DataFrame(flagged, columns=["raw_text", "reason"])

    if not clean.empty:
        base = [c for c in ["timestamp","league","away_team","home_team","book"] if c in clean.columns]
        other = [c for c in clean.columns if c not in base]
        clean = clean[base + other]
        clean.to_csv(OUT_CLEAN, index=False)
        clean.to_csv(PROMOTE, index=False)
        print(f"[guard] wrote {OUT_CLEAN} ({len(clean)} rows) and promoted -> {PROMOTE}")
    else:
        open(OUT_CLEAN, "w").write("")
        print("[guard] no clean rows; left splits.csv untouched")

    if not flags.empty:
        flags.to_csv(OUT_FLAG, index=False)
        print(f"[guard] flagged {len(flags)} garbage rows -> {OUT_FLAG}")
    else:
        open(OUT_FLAG, "w").write("raw_text,reason\n")

if __name__ == "__main__":
    main()
