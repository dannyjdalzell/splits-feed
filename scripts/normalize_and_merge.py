# normalize_and_merge.py
# Merge resolver output (images) + optional opponent_fills + Twitter text into a
# single staged CSV in the splits schema. Drops MIXED, enforces away=top, home=bottom.

import argparse, os
import pandas as pd

SPLITS_COLS = [
    'timestamp','league','sport','event_date','event_time','source',
    'home_team','away_team','market','side','odds','line','line_num','total','total_num',
    'bets_pct','bets_pct_num','handle_pct','handle_pct_num','ticket#','$ bet',
    'image_id','filename','notes'
]

def read_csv_safe(path):
    if not path or not os.path.exists(path): return None
    try:
        return pd.read_csv(path)
    except Exception:
        return None

def to_splits_from_resolver(df, label):
    # Accept v2 columns if present (away/home explicit), else fall back to team1/team2 = away/home
    if df is None or len(df) == 0:
        return pd.DataFrame(columns=SPLITS_COLS), 0
    df = df.copy()

    # Pair league (drop MIXED)
    if 'pair_league' in df.columns:
        df = df[df['pair_league'] != 'MIXED']

    # Determine away/home
    if {'away_team','home_team'}.issubset(df.columns):
        away = df['away_team']
        home = df['home_team']
    else:
        away = df.get('team1', '')
        home = df.get('team2', '')

    out = pd.DataFrame(columns=SPLITS_COLS)
    out['timestamp']   = ''                         # unknown from screenshots
    out['league']      = df.get('pair_league', df.get('league',''))
    out['sport']       = out['league']
    out['event_date']  = ''
    out['event_time']  = ''
    out['source']      = label                      # e.g., RESOLVER_V2
    out['home_team']   = home
    out['away_team']   = away
    out['market']      = 'UNKNOWN'
    out['side']        = ''
    out['odds']        = ''
    out['line']        = ''
    out['line_num']    = ''
    out['total']       = ''
    out['total_num']   = ''
    out['bets_pct']    = ''
    out['bets_pct_num']= ''
    out['handle_pct']  = ''
    out['handle_pct_num']=''
    out['ticket#']     = ''
    out['$ bet']       = ''
    out['image_id']    = df.get('image_id','')
    out['filename']    = df.get('filename_base','')
    out['notes']       = label
    return out, len(out)

def to_splits_from_twitter(df):
    # Twitter text has no visual ordering; keep teams, mark note.
    if df is None or len(df) == 0:
        return pd.DataFrame(columns=SPLITS_COLS), 0
    df = df.copy()

    out = pd.DataFrame(columns=SPLITS_COLS)
    out['timestamp']   = df.get('timestamp','')
    out['league']      = df.get('league','')
    out['sport']       = df.get('sport', out['league'])
    out['event_date']  = ''
    out['event_time']  = ''
    out['source']      = 'TWITTER'
    # Keep order as-is; downstream can decide home/away when schedule context exists
    out['home_team']   = df.get('team2','')   # optional convention
    out['away_team']   = df.get('team1','')
    out['market']      = 'UNKNOWN'
    out['side']        = ''
    out['odds']        = ''
    out['line']        = ''
    out['line_num']    = ''
    out['total']       = ''
    out['total_num']   = ''
    out['bets_pct']    = ''
    out['bets_pct_num']= ''
    out['handle_pct']  = ''
    out['handle_pct_num']=''
    out['ticket#']     = ''
    out['$ bet']       = ''
    out['image_id']    = df.get('image_id','')
    out['filename']    = df.get('image_url','')
    out['notes']       = 'TWITTER_TEXT'
    # strict: only keep same-league tweets (ingest script already does that)
    good = out[out['league'].astype(str).str.len() > 0].copy()
    return good, len(good)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--resolver', required=False, help='resolved_games_final_v2.csv')
    ap.add_argument('--opponent_fills', required=False, help='opponent_filled.csv (optional)')
    ap.add_argument('--twitter', required=False, help='twitter_resolved.csv (optional)')
    ap.add_argument('--stage', required=True, help='output staged CSV path')
    ap.add_argument('--report', required=True, help='output QA markdown')
    args = ap.parse_args()

    resolver = read_csv_safe(os.path.expanduser(args.resolver)) if args.resolver else None
    fills    = read_csv_safe(os.path.expanduser(args.opponent_fills)) if args.opponent_fills else None
    twitter  = read_csv_safe(os.path.expanduser(args.twitter)) if args.twitter else None

    # Build resolver table
    res_tbl, n_res = to_splits_from_resolver(resolver, 'RESOLVER_V2')
    # Promote high-confidence fills (if already merged upstream, this will be small/no-op)
    if fills is not None and len(fills):
        same_lg = fills[(fills['known_league'] == fills['opponent_league']) & (fills['confidence_pct'] >= 80.0)].copy()
        if len(same_lg):
            same_lg.rename(columns={'known_team':'team1','inferred_opponent':'team2','known_league':'pair_league',
                                    'filename_base':'filename_base','image_id':'image_id'}, inplace=True)
            filled_tbl, n_filled = to_splits_from_resolver(same_lg, 'OPPONENT_FILL')
            res_tbl = pd.concat([res_tbl, filled_tbl], ignore_index=True)

    tw_tbl, n_tw = to_splits_from_twitter(twitter)

    # Combine & dedupe
    stage = pd.concat([res_tbl, tw_tbl], ignore_index=True)

    # Simple dedupe key: league + home + away + filename (if present)
    def key(r):
        return '|'.join([
            str(r.get('league','')).lower(),
            str(r.get('home_team','')).lower(),
            str(r.get('away_team','')).lower(),
            str(r.get('filename','')).lower(),
            str(r.get('image_id','')).lower()
        ])
    if len(stage):
        stage['_k'] = stage.apply(key, axis=1)
        before = len(stage)
        stage = stage.drop_duplicates(subset=['_k']).drop(columns=['_k'])
        after = len(stage)
    else:
        before = after = 0

    # Write staged file
    os.makedirs(os.path.dirname(os.path.expanduser(args.stage)), exist_ok=True)
    stage.to_csv(os.path.expanduser(args.stage), index=False)

    # QA report
    lines = []
    lines.append("# splits staged QA")
    lines.append(f"- resolver rows: {n_res}")
    lines.append(f"- twitter rows:  {n_tw}")
    lines.append(f"- staged rows:   {after} (deduped from {before})")
    if len(stage):
        by_lg = stage['league'].value_counts(dropna=False).to_dict()
        by_src = stage['source'].value_counts(dropna=False).to_dict()
        lines.append(f"- by league: {by_lg}")
        lines.append(f"- by source: {by_src}")
    os.makedirs(os.path.dirname(os.path.expanduser(args.report)), exist_ok=True)
    with open(os.path.expanduser(args.report), 'w') as f:
        f.write('\n'.join(lines) + '\n')

    print("Wrote:", os.path.expanduser(args.stage))
    print("QA:", os.path.expanduser(args.report))
    print(f"Rows staged: {after}")

if __name__ == '__main__':
    main()
