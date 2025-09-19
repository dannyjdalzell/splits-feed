#!/usr/bin/env python3
"""
Analyze Twitter text (no league clustering). Grade each tweet as LOW/MED/HIGH signal and
extract team mentions using your dictionaries. Output is a flat CSV for the pipeline.

Usage (local file or published CSV URL both work):
  python scripts/analyze_twitter_text.py \
    --csv  ./sources/sheets/twitter/tweets.csv \
    --dict ./dictionaries \
    --out  ./audit_out/twitter_text_signals.csv
"""

import argparse, os, re, json, pandas as pd
from datetime import datetime

# ----------------------------- args -----------------------------
ap = argparse.ArgumentParser()
ap.add_argument("--csv",  required=True, help="Tweets CSV (local path or HTTPS published CSV URL)")
ap.add_argument("--dict", required=True, help="Directory with dictionaries/*.json")
ap.add_argument("--out",  required=True, help="Output CSV path")
args = ap.parse_args()

# ------------------------- load dictionaries --------------------
def load_dicts(droot: str):
    files = ["nfl.json","mlb.json","nba.json","nhl.json","ncaaf_fbs_seed.json"]
    alias_to_team = {}
    team_set = set()

    def add_alias(alias, team):
        alias = alias.strip().upper()
        if alias and alias not in alias_to_team:
            alias_to_team[alias] = team

    def seed_aliases(team):
        parts = team.split()
        if len(parts) >= 2:
            city = " ".join(parts[:-1]); nick = parts[-1]
            return [team, city, nick]
        return [team]

    for f in files:
        p = os.path.join(droot, f)
        if not os.path.isfile(p): 
            continue
        data = json.load(open(p, "r"))
        for team, aliases in data.items():
            team_set.add(team)
            for a in seed_aliases(team): add_alias(a, team)
            for a in aliases: add_alias(a, team)

    # common short-hands (biased toward most common betting usage)
    extras = {
        "DAL": "Dallas Cowboys", "BUF": "Buffalo Bills", "KC": "Kansas City Chiefs",
        "TB": "Tampa Bay Buccaneers", "RAYS": "Tampa Bay Rays", "LIGHTNING": "Tampa Bay Lightning",
        "RANGERS": "Texas Rangers", "KINGS": "Los Angeles Kings", "LAKERS": "Los Angeles Lakers",
        "CLIPPERS": "LA Clippers", "RAMS": "Los Angeles Rams", "CHARGERS": "Los Angeles Chargers",
        "SEAHAWKS": "Seattle Seahawks", "MARINERS": "Seattle Mariners", "WARRIORS": "Golden State Warriors",
        "ORIOLES": "Baltimore Orioles", "ASTROS": "Houston Astros", "PIRATES": "Pittsburgh Pirates",
    }
    for a, t in extras.items():
        if t in team_set:
            add_alias(a, t)

    # pre-build regex list (longest alias first)
    aliases_sorted = sorted(alias_to_team.keys(), key=len, reverse=True)
    patterns = [(a, re.compile(rf'(?<![A-Z0-9]){re.escape(a)}(?![A-Z0-9])', re.I)) for a in aliases_sorted]
    return alias_to_team, patterns

ALIAS_TO, ALIAS_PATTERNS = load_dicts(args.dict)

def detect_teams(text: str):
    T = (text or "").upper()
    hits = []
    for alias, rx in ALIAS_PATTERNS:
        if rx.search(T):
            hits.append(ALIAS_TO[alias])
    # dedup preserve order
    seen = set(); out = []
    for t in hits:
        if t not in seen:
            seen.add(t); out.append(t)
    return out

# ---------------------------- load tweets -----------------------
def load_tweets(path: str) -> pd.DataFrame:
    # Expect a CSV with at least a text column. We’ll pick the best text-like column if multiple.
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    if df.empty:
        return df

    # pick text column heuristically
    def text_col(d: pd.DataFrame):
        best, best_c = -1, None
        for c in d.columns:
            vals = d[c].astype(str)
            score = vals.str.len().median() + 0.5*vals.str.len().quantile(0.9)
            if any(k in c.lower() for k in ["text","tweet","body","content","message"]):
                score += 200
            if score > best:
                best, best_c = score, c
        return best_c

    tc = text_col(df)
    df = df.rename(columns={tc: "__text__"}) if tc and "__text__" not in df.columns else df
    if "__text__" not in df.columns:
        # fallback: use the longest column
        tc = max(df.columns, key=lambda c: df[c].astype(str).str.len().median())
        df = df.rename(columns={tc: "__text__"})

    # standardize optional fields if present
    for c in ["timestamp","tweet_id","handle"]:
        if c not in df.columns:
            df[c] = ""

    return df

try:
    tweets = load_tweets(args.csv)
except Exception as e:
    raise SystemExit(f"[ERR] Could not read CSV: {e}")

# --------------------------- grading rules ----------------------
# HIGH: explicit betting-relevant cues
HIGH_RX = [
    r"\bmost\s+bet\b", r"\bmost\s+wagered\b", r"\btop\s*(?:\d+|five|ten|3|5|10)\b",
    r"\bhandle\b", r"\btickets?\b", r"\bconsensus\b", r"\bpublic\b",
    r"\bsteam(ed)?\b", r"\bmovement\b", r"\bline\s*move(d)?\b",
    r"\b%(\s|$)", r"\d{1,3}\s*%",
]
# MED: softer narrative but still betting-ish
MED_RX = [
    r"\bsharp\b", r"\bpros\b", r"\bpublic\s+side\b", r"\bsquare\b",
    r"\bfade\b", r"\bheavy\b", r"\bpopular\b",
]
# LOW: generic hype/no info (we mark LOW by absence of above)
HIGH = re.compile("|".join(HIGH_RX), re.I)
MED  = re.compile("|".join(MED_RX),  re.I)

def grade_signal(text: str) -> str:
    t = text or ""
    if HIGH.search(t): return "HIGH"
    if MED.search(t):  return "MED"
    return "LOW"

# ----------------------------- run ------------------------------
rows = []
for i, r in tweets.iterrows():
    txt = r["__text__"]
    strength = grade_signal(txt)
    teams = detect_teams(txt)
    # keep everything; grading lets you filter downstream
    rows.append({
        "date": (r["timestamp"][:10] if r["timestamp"] else datetime.utcnow().date().isoformat()),
        "tweet_id": r.get("tweet_id",""),
        "handle": r.get("handle",""),
        "text": txt,
        "teams": " | ".join(teams),   # no league clustering here; just names
        "signal_strength": strength,
        "notes": ""  # placeholder; we can fill later with specific tags like "most_bet", "handle", etc.
    })

out = pd.DataFrame(rows)
os.makedirs(os.path.dirname(args.out), exist_ok=True)
out.to_csv(args.out, index=False)
print(f"Wrote: {args.out}  rows={len(out)}  (HIGH={sum(out.signal_strength=='HIGH')}, MED={sum(out.signal_strength=='MED')}, LOW={sum(out.signal_strength=='LOW')})")

