#!/usr/bin/env python3
# ingest_twitter_csv.py
#
# Usage:
#   python3 ingest_twitter_csv.py \
#     --csv ~/splits-feed/sources/sheets/twitter/tweets.csv \
#     --dict ~/splits-feed/dictionaries \
#     --out  ~/splits-feed/audit_out/twitter_resolved.csv
#
# Output columns:
#   timestamp, league, sport, team1, team2, source, text, tweet_id, handle, image_id, image_url, notes, resolution
#
# Notes:
# - Strict: emits a row only when it finds >=2 teams from the SAME league.
# - If multiple leagues appear, it picks the tweet's dominant league (by count).
# - If still mixed (tie w/ cross-league), it drops the tweet unless --keep-mixed.

import argparse, os, re, json
import pandas as pd
from collections import defaultdict, Counter

def load_json(path):
    with open(path, "r") as f:
        return json.load(f)

def load_dictionaries(droot: str):
    files = {
        "NFL": os.path.join(droot, "nfl.json"),
        "MLB": os.path.join(droot, "mlb.json"),
        "NBA": os.path.join(droot, "nba.json"),
        "NHL": os.path.join(droot, "nhl.json"),
        "NCAAF": os.path.join(droot, "ncaaf_fbs_seed.json"),
    }
    team_to_league = {}
    alias_to_teams = defaultdict(set)
    for lg, p in files.items():
        if not os.path.isfile(p): 
            continue
        d = load_json(p)
        for team, aliases in d.items():
            team_to_league[team] = lg
            for a in aliases + [team]:
                norm = re.sub(r"\s+", " ", str(a).strip()).upper()
                if norm:
                    alias_to_teams[norm].add(team)
    # compile regex patterns for aliases
    patterns = []
    for alias, teams in alias_to_teams.items():
        toks = [re.escape(t) for t in alias.split() if t]
        if not toks:
            continue
        rx = re.compile(r"(?i)(?<![A-Za-z])" + r"\s+".join(toks) + r"(?![A-Za-z])")
        patterns.append((rx, alias, list(teams)))
    return team_to_league, patterns

def detect_teams(text: str, patterns, team_to_league):
    U = str(text or "").upper()
    hits = []  # (team, league, pos)
    for rx, alias, teams in patterns:
        for m in rx.finditer(U):
            pos = m.start()
            for t in teams:
                hits.append((t, team_to_league.get(t), pos))
    if not hits:
        return []
    # earliest position per team
    earliest = {}
    for t, lg, pos in hits:
        if t not in earliest or pos < earliest[t][1]:
            earliest[t] = (lg, pos)
    out = [(t, v[0], v[1]) for t, v in earliest.items()]
    # order by position (left→right)
    out.sort(key=lambda x: x[2])
    return out

def choose_pair_from_hits(hits, league_hint=None):
    """
    hits: list[(team, league, pos)]
    league_hint: optional string like 'NFL' to bias selection
    strategy:
      1) if league_hint present and >=2 teams of that league exist → use that league’s first 2.
      2) otherwise pick dominant league by count; use first 2 teams of that league.
      3) if a clear pair still not possible (tie across leagues), return None (drop) to avoid cross-sport garbage.
    """
    if not hits or len(hits) < 2:
        return None

    by_league = defaultdict(list)
    for t, lg, pos in hits:
        by_league[lg].append((t, pos))

    if league_hint and league_hint in by_league and len(by_league[league_hint]) >= 2:
        arr = sorted(by_league[league_hint], key=lambda x: x[1])[:2]
        return arr[0][0], arr[1][0], league_hint, "HINT_DOMINANT"

    counts = {lg: len(arr) for lg, arr in by_league.items()}
    if not counts:
        return None

    # dominant league by count; tie-breaker = earliest second-team position
    dominant, _ = max(counts.items(), key=lambda kv: kv[1])
    # tie handling
    tied = [lg for lg, c in counts.items() if c == counts[dominant]]
    if len(tied) > 1:
        # try to break ties by earliest positions of the second team
        def second_pos(lg):
            arr = sorted(by_league[lg], key=lambda x: x[1])
            if len(arr) >= 2:
                return arr[1][1]
            return 10**9
        dominant = min(tied, key=second_pos)

    if len(by_league[dominant]) >= 2:
        arr = sorted(by_league[dominant], key=lambda x: x[1])[:2]
        return arr[0][0], arr[1][0], dominant, "DOMINANT_LEAGUE"
    return None

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True, help="tweets.csv exported to disk (from Google Sheets)")
    ap.add_argument("--dict", required=True, help="path to dictionaries folder (nfl.json, mlb.json, etc.)")
    ap.add_argument("--out", required=True, help="output csv path (twitter_resolved.csv)")
    ap.add_argument("--keep-mixed", action="store_true", help="keep mixed/ambiguous tweets (will mark pair_league=MIXED)")
    args = ap.parse_args()

    team_to_league, patterns = load_dictionaries(args.dict)

    # Read CSV with permissive columns
    df = pd.read_csv(args.csv, dtype=str, keep_default_na=False).fillna("")
    # Standardize expected columns; allow extras to pass through
    for col in ["timestamp","tweet_id","handle","text","league_hint","image_id","image_url","notes"]:
        if col not in df.columns: df[col] = ""

# Map alternate text columns if 'text' is empty
if (df.get("text","") == "").all():
    for cand in ["Tweet", "tweet", "body", "message", "content"]:
        if cand in df.columns:
            df["text"] = df[cand].astype(str)
            break
# Map alternate hint if present
if (df.get("league_hint","") == "").all():
    for cand in ["league", "sport_hint", "leagueHint"]:
        if cand in df.columns:
            df["league_hint"] = df[cand].astype(str)
            break


    rows = []
    dropped = 0
    for _, r in df.iterrows():
        text = r["text"]
        league_hint = r["league_hint"].strip().upper() or None
        hits = detect_teams(text, patterns, team_to_league)
        res = choose_pair_from_hits(hits, league_hint=league_hint)
        if not res:
            dropped += 1
            if args.keep_mixed:
                rows.append({
                    "timestamp": r["timestamp"],
                    "league": "MIXED",
                    "sport": "MIXED",
                    "team1": "",
                    "team2": "",
                    "source": "TWITTER",
                    "text": text,
                    "tweet_id": r["tweet_id"],
                    "handle": r["handle"],
                    "image_id": r["image_id"],
                    "image_url": r["image_url"],
                    "notes": r["notes"],
                    "resolution": "NO_PAIR"
                })
            continue

        t1, t2, lg, how = res
        rows.append({
            "timestamp": r["timestamp"],
            "league": lg,
            "sport": lg,
            "team1": t1,            # text has no visual ordering; home/away left blank downstream
            "team2": t2,
            "source": "TWITTER",
            "text": text,
            "tweet_id": r["tweet_id"],
            "handle": r["handle"],
            "image_id": r["image_id"],
            "image_url": r["image_url"],
            "notes": r["notes"],
            "resolution": how
        })

    out_df = pd.DataFrame(rows, columns=[
        "timestamp","league","sport","team1","team2","source","text",
        "tweet_id","handle","image_id","image_url","notes","resolution"
    ])
    os.makedirs(os.path.dirname(os.path.expanduser(args.out)), exist_ok=True)
    out_df.to_csv(os.path.expanduser(args.out), index=False)

    print(f"Wrote: {os.path.expanduser(args.out)}")
    print(f"Rows kept: {len(out_df)} | Dropped (no same-league pair): {dropped}")

if __name__ == "__main__":
    main()

