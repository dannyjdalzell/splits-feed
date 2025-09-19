cat > ~/splits-feed/scripts/ingest_twitter_csv.py <<'PY'
# ingest_twitter_csv.py — robust, CI-safe
# Reads a published Google Sheets CSV of tweets, detects teams with dictionaries,
# keeps ONLY same-league pairs, and writes twitter_resolved.csv.
#
# Usage:
#   python scripts/ingest_twitter_csv.py \
#     --csv  ./sources/sheets/twitter/tweets.csv \
#     --dict ./dictionaries \
#     --out  ./audit_out/twitter_resolved.csv

import argparse, os, re, json
import pandas as pd
from collections import defaultdict

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
    out.sort(key=lambda x: x[2])  # by position
    return out

def choose_pair_from_hits(hits, league_hint=None):
    if not hits or len(hits) < 2:
        return None
    # group by league
    by_lg = {}
    for t, lg, pos in hits:
        by_lg.setdefault(lg, []).append((t, pos))
    # league_hint first
    if league_hint and league_hint in by_lg and len(by_lg[league_hint]) >= 2:
        arr = sorted(by_lg[league_hint], key=lambda x: x[1])[:2]
        return arr[0][0], arr[1][0], league_hint, "HINT_DOMINANT"
    # dominant league by count; tiebreak = earliest 2nd-team position
    counts = {lg: len(v) for lg, v in by_lg.items()}
    if not counts:
        return None
    best_lg = None
    best_score = None
    for lg, c in counts.items():
        arr = sorted(by_lg[lg], key=lambda x: x[1])
        sec = arr[1][1] if len(arr) >= 2 else 10**9
        score = (c, -sec)  # more teams, earlier second
        if best_score is None or score > best_score:
            best_score = score
            best_lg = lg
    if best_lg and len(by_lg[best_lg]) >= 2:
        arr = sorted(by_lg[best_lg], key=lambda x: x[1])[:2]
        return arr[0][0], arr[1][0], best_lg, "DOMINANT_LEAGUE"
    return None

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True)
    ap.add_argument("--dict", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--keep-mixed", action="store_true")
    args = ap.parse_args()

    # Load CSV safely
    if not os.path.exists(args.csv):
        os.makedirs(os.path.dirname(args.out), exist_ok=True)
        pd.DataFrame(columns=[
            "timestamp","league","sport","team1","team2","source","text",
            "tweet_id","handle","image_id","image_url","notes","resolution"
        ]).to_csv(args.out, index=False)
        print(f"[warn] tweets.csv missing: {args.csv}")
        print(f"Wrote empty: {args.out}")
        return

    df = pd.read_csv(args.csv, dtype=str, keep_default_na=False).fillna("")

    # Standardize expected columns; allow extras to pass through
    for col in ["timestamp","tweet_id","handle","text","league_hint","image_id","image_url","notes"]:
        if col not in df.columns:
            df[col] = ""

    # Map alternate text/hint columns if needed
    if (df["text"].astype(str).str.strip() == "").all():
        for cand in ["Tweet","tweet","body","message","content","Text","TEXT"]:
            if cand in df.columns:
                df["text"] = df[cand].astype(str)
                break
    if (df["league_hint"].astype(str).str.strip() == "").all():
        for cand in ["league","sport_hint","leagueHint","League","SPORT"]:
            if cand in df.columns:
                df["league_hint"] = df[cand].astype(str)
                break

    team_to_league, patterns = load_dictionaries(args.dict)

    rows = []
    dropped = 0
    for _, r in df.iterrows():
        text = str(r["text"])
        league_hint = str(r["league_hint"]).strip().upper() or None

        hits = detect_teams(text, patterns, team_to_league)
        res = choose_pair_from_hits(hits, league_hint=league_hint)
        if not res:
            dropped += 1
            if args.keep-mixed:
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
            "team1": t1,
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
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    out_df.to_csv(args.out, index=False)

    print(f"Wrote: {args.out}")
    print(f"Rows kept: {len(out_df)} | Dropped (no same-league pair): {dropped}")

if __name__ == "__main__":
    main()
PY
