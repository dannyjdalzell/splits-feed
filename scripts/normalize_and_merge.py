#!/usr/bin/env python3
"""
normalize_and_merge.py
Unifies pipeline fuels and emits a boardroom-ready CSV with Twitter text weights applied to each matchup.

Inputs (all optional except splits.csv):
  - ./splits.csv                                   # base matchup rows (home_team, away_team, market, etc.)
  - ./audit_out/twitter_text_signals.csv           # graded LOW/MED/HIGH text signals (no league clustering)
  - ./dictionaries/*.json                          # team dictionaries to canonicalize names

Output:
  - ./audit_out/boardroom_inputs.csv               # enriched matchups with twitter weights (home/away), totals

Weighting:
  HIGH = 2,  MED = 1,  LOW = 0
  We aggregate per team across the file; if a 'date' column exists we favor recent (today +/- 2 days).

Usage:
  python scripts/normalize_and_merge.py
"""

import os, re, json, math, sys
from datetime import datetime, timedelta, timezone
import pandas as pd

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

# ---------- helpers ----------
def read_csv(path, required=False):
    if not os.path.exists(path):
        if required:
            raise SystemExit(f"[ERR] missing required file: {path}")
        return pd.DataFrame()
    try:
        return pd.read_csv(path, dtype=str, keep_default_na=False).fillna("")
    except Exception as e:
        if required:
            raise
        print(f"[WARN] failed to read {path}: {e}")
        return pd.DataFrame()

def load_dictionaries(droot):
    alias_to_team = {}
    team_to_canon = {}
    files = ["nfl.json","mlb.json","nba.json","nhl.json","ncaaf_fbs_seed.json"]
    for f in files:
        p = os.path.join(droot, f)
        if not os.path.isfile(p): 
            continue
        data = json.load(open(p, "r"))
        for canonical, aliases in data.items():
            team_to_canon[canonical.upper()] = canonical
            parts = canonical.split()
            seeds = [canonical]
            if len(parts) >= 2:
                seeds.append(" ".join(parts[:-1]))   # city
                seeds.append(parts[-1])              # nickname
            for a in list(set(seeds + list(aliases))):
                alias_to_team[a.strip().upper()] = canonical
    # pragmatic common shorthands (bias to common betting references)
    extras = {
        "DAL": "Dallas Cowboys", "BUF": "Buffalo Bills", "KC": "Kansas City Chiefs",
        "TB": "Tampa Bay Buccaneers", "RAYS": "Tampa Bay Rays",
        "LIGHTNING": "Tampa Bay Lightning", "RANGERS": "Texas Rangers",
        "KINGS": "Los Angeles Kings", "LAKERS": "Los Angeles Lakers",
        "CLIPPERS": "LA Clippers", "RAMS": "Los Angeles Rams",
        "CHARGERS": "Los Angeles Chargers", "SEAHAWKS": "Seattle Seahawks",
        "MARINERS": "Seattle Mariners", "WARRIORS": "Golden State Warriors",
        "ORIOLES": "Baltimore Orioles", "ASTROS": "Houston Astros",
        "PIRATES": "Pittsburgh Pirates",
    }
    for a,t in extras.items():
        alias_to_team[a] = t
    return alias_to_team, team_to_canon

def to_canonical(name, alias_map, canon_map):
    s = (name or "").strip()
    if not s:
        return ""
    u = s.upper()
    if u in canon_map:
        return canon_map[u]
    # try full-phrase alias, then token scan (longest alias first would require precomputed list)
    if u in alias_map:
        return alias_map[u]
    # last-resort: collapse whitespace
    u2 = re.sub(r"\s+", " ", u)
    return alias_map.get(u2, s)

def parse_date(s):
    if not s: return None
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%m/%d/%Y", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            continue
    return None

# ---------- load inputs ----------
splits_path = os.path.join(ROOT, "splits.csv")
tweets_path = os.path.join(ROOT, "audit_out", "twitter_text_signals.csv")
dict_dir    = os.path.join(ROOT, "dictionaries")

splits = read_csv(splits_path, required=True)
tweets = read_csv(tweets_path, required=False)
alias_map, canon_map = load_dictionaries(dict_dir)

# ---------- canonicalize split teams ----------
for col in ["home_team","away_team"]:
    if col in splits.columns:
        splits[col] = splits[col].apply(lambda x: to_canonical(x, alias_map, canon_map))
    else:
        splits[col] = ""

# ---------- build twitter team weights ----------
# Map signal strength to weight
W = {"HIGH": 2, "MED": 1, "LOW": 0}

team_weight = {}

if not tweets.empty:
    # Optional date decay: keep today±2 days full weight, otherwise half weight
    today = datetime.now(timezone.utc).date()
    def recent_factor(ds):
        d = parse_date(ds)
        if not d: 
            return 1.0
        dd = abs((d.date() - today).days)
        return 1.0 if dd <= 2 else 0.5

    for _, r in tweets.iterrows():
        txt = r.get("text","")
        teams_field = r.get("teams","")
        strength = r.get("signal_strength","").upper()
        base = W.get(strength, 0)

        if not teams_field: 
            continue
        # teams are " | "-separated canonical names from analyzer (we still pass through to_canonical defensively)
        teams = [t.strip() for t in teams_field.split("|") if t.strip()]
        if not teams: 
            continue

        factor = recent_factor(r.get("date",""))
        weight = base * factor

        for t in teams:
            canon = to_canonical(t, alias_map, canon_map)
            team_weight[canon] = team_weight.get(canon, 0.0) + weight

# ---------- apply weights to matchups ----------
# We leave all original split columns intact, and add:
#  - twitter_weight_home
#  - twitter_weight_away
#  - twitter_weight_total
out = splits.copy()

def w_of(team):
    return float(team_weight.get(team, 0.0))

out["twitter_weight_home"]  = out["home_team"].apply(w_of)
out["twitter_weight_away"]  = out["away_team"].apply(w_of)
out["twitter_weight_total"] = out["twitter_weight_home"] + out["twitter_weight_away"]

# ---------- write output ----------
os.makedirs(os.path.join(ROOT, "audit_out"), exist_ok=True)
out_path = os.path.join(ROOT, "audit_out", "boardroom_inputs.csv")
out.to_csv(out_path, index=False)

# ---------- log ----------
print(f"[ok] splits rows: {len(splits)}  twitter rows: {len(tweets)}  teams with weight: {len(team_weight)}")
print(f"[ok] wrote: {out_path}")
top_sig = sorted(team_weight.items(), key=lambda kv: kv[1], reverse=True)[:10]
if top_sig:
    print("[top twitter-weighted teams]")
    for t,v in top_sig:
        print(f"  {t:30s}  {v:.2f}")
