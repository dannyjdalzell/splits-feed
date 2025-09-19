# ingest_twitter_csv.py — robust, CI-safe
# Reads a published Google Sheets CSV of tweets, detects teams with dictionaries,
# keeps ONLY same-league pairs, and writes twitter_resolved.csv.

import argparse, os, re, json
import pandas as pd
from collections import defaultdict

# ---------- Utilities ----------
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
    patterns = []

    for lg, fn in files.items():
        if not os.path.isfile(fn):
            continue
        d = load_json(fn)
        for team, aliases in d.items():
            team_to_league[team] = lg
            for a in aliases + [team]:
                norm = re.sub(r"\s+", " ", a.strip()).upper()
                alias_to_teams[norm].add(team)

    for alias, teams in alias_to_teams.items():
        toks = re.escape(alias)
        rx = re.compile(rf"(?i)(?<![A-Za-z]){toks}(?![A-Za-z])")
        patterns.append((rx, alias, list(teams)))

    return team_to_league, patterns

def detect_teams(text, patterns, team_to_league):
    textU = str(text or "").upper()
    hits = []
    for rx, alias, teams in patterns:
        for m in rx.finditer(textU):
            pos = m.start()
            for t in teams:
                hits.append((t, team_to_league.get(t), pos))
    if not hits:
        return []
    hits.sort(key=lambda x: x[2])
    return hits

# ---------- Main ----------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True, help="input tweets.csv (from Sheets)")
    ap.add_argument("--dict", required=True, help="dictionaries dir")
    ap.add_argument("--out", required=True, help="output resolved CSV")
    args = ap.parse_args()

    if not os.path.isfile(args.csv):
        raise SystemExit(f"[ERR] CSV not found: {args.csv}")
    if not os.path.isdir(args.dict):
        raise SystemExit(f"[ERR] Dict dir not found: {args.dict}")

    team_to_league, patterns = load_dictionaries(args.dict)

    df = pd.read_csv(args.csv, dtype=str).fillna("")
    out_rows = []
    dropped = 0

    for _, r in df.iterrows():
        text = r.get("text", "")
        hits = detect_teams(text, patterns, team_to_league)

        if len(hits) < 2:
            dropped += 1
            continue

        # take first two different-league hits
        uniq = {}
        for t, lg, pos in hits:
            if t not in uniq:
                uniq[t] = (lg, pos)
        if len(uniq) < 2:
            dropped += 1
            continue

        items = sorted(uniq.items(), key=lambda kv: kv[1][1])
        (team1, (lg1, _)), (team2, (lg2, _)) = items[:2]

        if lg1 != lg2:
            dropped += 1
            continue

        out_rows.append({
            "timestamp": r.get("timestamp", ""),
            "league": lg1,
            "sport": "",
            "team1": team1,
            "team2": team2,
            "source": "twitter_text",
            "text": text,
            "tweet_id": r.get("tweet_id", ""),
            "handle": r.get("handle", ""),
            "image_id": r.get("image_id", ""),
            "image_url": r.get("image_url", ""),
            "notes": "",
            "resolution": "twitter_csv"
        })

    out_df = pd.DataFrame(out_rows)
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    out_df.to_csv(args.out, index=False)

    print(f"Wrote: {args.out}")
    print(f"Rows kept: {len(out_df)} | Dropped (no same-league pair): {dropped}")

if __name__ == "__main__":
    main()

