#!/usr/bin/env python3
import json, re, sys, argparse
from datetime import datetime
import pandas as pd

BAD_PATTERNS = [
    r"Estimating resolution",
    r"Betting Splits Expanded Splits",
    r"Handle Bets Total Handle Bets",
    r"Money dle Bets",
    r"Spread\s*$",  # as a team name
    r"MLB - [A-Za-z]{3,9},?Sep",  # calendar garbage
    r"Chiefs ad",  # OCR ad tails
]

MARKETS = {"Spread","ML","Moneyline","Total","O/U","OU"}

def _norm(s:str)->str:
    s = re.sub(r"[^A-Za-z0-9& +./'-]", " ", str(s or ""))
    s = re.sub(r"\s+", " ", s).strip()
    return s

def load_aliases(path:str):
    """
    Accepts multiple shapes:
    - list of {abr/abbrev, city, name, mascot}
    - nested dicts like conferences → schools → {abbrev, mascot}
    - {"aliases": {"KC":["KC","Kansas City","Chiefs",...] , ...}}
    Returns: alias_map (lower alias -> CANON), league_map (CANON -> league guess)
    """
    raw = json.load(open(path, "r", encoding="utf-8"))
    alias_map = {}
    league_map = {}
    def add(team, canon, league=None):
        t = _norm(team).lower()
        if not t: return
        alias_map[t] = canon
        if league: league_map[canon]=league

    def add_pack(city=None,name=None,abr=None,abbrev=None,mascot=None,league=None):
        parts = []
        if city: parts.append(city)
        if name: parts.append(name)
        city_name = " ".join(parts)
        canon = _norm((abr or abbrev or city_name or mascot or "")).upper() or _norm(city_name).upper()
        for a in filter(None,[abr,abbrev,city,name,mascot,city_name,
                              (city or "")+" "+(mascot or ""),
                              (city or "")+" "+(name or "")]):
            add(a, canon, league)

    if isinstance(raw, dict) and "aliases" in raw:
        for canon, arr in raw["aliases"].items():
            for a in arr: add(a, canon, None)
    elif isinstance(raw, dict):
        # treat nested conferences/leagues
        for maybe_league, teams in raw.items():
            if isinstance(teams, dict):
                for team, meta in teams.items():
                    if isinstance(meta, dict):
                        add_pack(city=team, name=meta.get("name"), abr=meta.get("abr") or meta.get("abbrev"),
                                 mascot=meta.get("mascot"), league=maybe_league)
                    else:
                        add_pack(city=team, league=maybe_league)
    elif isinstance(raw, list):
        for meta in raw:
            if isinstance(meta, dict):
                add_pack(city=meta.get("city"), name=meta.get("name"),
                         abr=meta.get("abr") or meta.get("abbrev"), mascot=meta.get("mascot"),
                         league=meta.get("league") or meta.get("conf"))
    return alias_map, league_map

def resolve_team(txt:str, alias_map:dict):
    s = _norm(txt).lower()
    if not s: return None
    # Exact
    if s in alias_map: return alias_map[s]
    # Suffix (cropped left by OCR, e.g. "rs Los Angeles Chargers")
    for alias in alias_map.keys():
        if s.endswith(alias): return alias_map[alias]
    # Substring
    for alias in alias_map.keys():
        if f" {alias} " in f" {s} ":
            return alias_map[alias]
    return None

def looks_bad_team(s:str)->bool:
    s = _norm(s)
    for pat in BAD_PATTERNS:
        if re.search(pat, s, flags=re.I): return True
    # Heuristic: teams should have at least 2 letters and not be generic words
    if len(re.sub(r"[^A-Za-z]", "", s)) < 3: return True
    return False

def coerce_pct(x):
    try:
        if x is None or str(x).strip()=="":
            return None
        v = float(str(x).replace("%","").strip())
        if 0 <= v <= 100: return v
    except: pass
    return None

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="src", default="splits.csv")
    ap.add_argument("--out", dest="out", default="splits_clean.csv")
    ap.add_argument("--teams", dest="teams", default="scripts/team_dictionary.json")
    ap.add_argument("--promote", action="store_true", help="also overwrite splits.csv with the clean file")
    return ap.parse_args()

def main():
    args = parse_args()
    try:
        df = pd.read_csv(args.src)
    except FileNotFoundError:
        print(f"[guard] no input {args.src}; exiting cleanly.")
        sys.exit(0)

    alias_map, league_map = load_aliases(args.teams)

    N0 = len(df)
    df = df.rename(columns={c:_norm(c).lower().replace(" ","_") for c in df.columns})

    # Basic required columns
    need = {"timestamp","away_team","home_team","market","source"}
    missing = need - set(df.columns)
    if missing:
        print(f"[guard] missing cols {missing}; exiting.")
        sys.exit(0)

    # Strip obvious garbage rows via patterns
    mask_bad = False
    for col in ["away_team","home_team"]:
        bad_here = df[col].astype(str).fillna("").str.contains("|".join(BAD_PATTERNS), case=False, regex=True)
        mask_bad = bad_here if mask_bad is False else (mask_bad | bad_here)
    df = df[~mask_bad].copy()

    # Normalize % and market
    df["tickets_pct"] = df.get("tickets_pct")
    df["handle_pct"]  = df.get("handle_pct")
    df["tickets_pct"] = df["tickets_pct"].map(coerce_pct)
    df["handle_pct"]  = df["handle_pct"].map(coerce_pct)

    df["market"] = df["market"].astype(str).str.replace(r"Money Line","ML", regex=False)
    df["market"] = df["market"].apply(lambda s: "ML" if s.strip().upper() in {"ML","MONEYLINE","MONEY LINE"} else s)
    df["market"] = df["market"].apply(lambda s: "Total" if s.strip().lower() in {"total","o/u","ou"} else s)
    df = df[df["market"].isin(list(MARKETS))]

    # Resolve teams
    df["away"] = df["away_team"].apply(lambda s: resolve_team(s, alias_map))
    df["home"] = df["home_team"].apply(lambda s: resolve_team(s, alias_map))
    # Drop obvious bad team strings
    bad_rows = df["away_team"].apply(looks_bad_team) | df["home_team"].apply(looks_bad_team)
    df = df[~bad_rows]
    # Keep only rows with both sides resolved and different
    df = df[df["away"].notna() & df["home"].notna() & (df["away"]!=df["home"])].copy()

    # Clean timestamp parse
    def to_ts(x):
        try: return pd.to_datetime(x, utc=True)
        except: return pd.NaT
    df["timestamp"] = df["timestamp"].map(to_ts)
    df = df[df["timestamp"].notna()]

    # Arrange / minimal columns
    keep_cols = ["timestamp","away","home","market","tickets_pct","handle_pct","line","source"]
    for k in keep_cols:
        if k not in df.columns: df[k] = None
    df = df[keep_cols].sort_values("timestamp")

    df.to_csv(args.out, index=False)
    print(f"[guard] input {N0} → kept {len(df)} rows; wrote {args.out}")

    if args.promote:
        df.to_csv("splits.csv", index=False)
        print("[guard] promoted splits_clean.csv → splits.csv")

if __name__ == "__main__":
    main()
