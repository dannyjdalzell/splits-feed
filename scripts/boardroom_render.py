#!/usr/bin/env python3
import argparse, math, pandas as pd
from datetime import datetime, timezone

def decay_weight(ts, now, tau_hours=24):
    if pd.isna(ts): return 0.0
    age_h = (now - ts).total_seconds() / 3600.0
    return math.exp(-max(0.0, age_h)/tau_hours)

def safe_ts(x):
    try: return pd.to_datetime(x, utc=True)
    except: return pd.NaT

def load_signals(path, hours):
    df = pd.read_csv(path)
    cols = {c.lower(): c for c in df.columns}
    # expected: timestamp, entity, w24/w6 or score; sample text column
    tscol = cols.get("timestamp") or cols.get("time") or list(df.columns)[0]
    entcol = cols.get("entity") or cols.get("team") or "entity"
    txtcol = cols.get("text") or cols.get("sample_text") or "text"

    df["__ts"] = df[tscol].map(safe_ts)
    cutoff = pd.Timestamp.utcnow() - pd.Timedelta(hours=hours)
    df = df[df["__ts"] >= cutoff].copy()

    # base weight guess
    w = 0.0
    for cand in ["decayed","score","w24","w6","weight"]:
        if cand in cols:
            w = w + df[cols[cand]].fillna(0).astype(float)
    if w is 0.0 or (isinstance(w, float) and w == 0.0):
        w = 1.0

    now = pd.Timestamp.utcnow()
    df["__decay"] = df["__ts"].map(lambda t: decay_weight(t, now))
    df["__w"] = (w if isinstance(w, pd.Series) else pd.Series([w]*len(df))) * df["__decay"]

    out = (df.groupby(df[entcol].fillna("UNKNOWN"))
             .agg(score=("__w","sum"),
                  signals=("__w","size"),
                  last_seen=("__ts","max"),
                  sample_text=(txtcol,"first"))
             .reset_index()
             .rename(columns={entcol:"entity"}))
    out["score"] = out["score"].round(2)
    return out.sort_values(["score","signals"], ascending=[False,False])

def load_splits(path, hours):
    try:
        df = pd.read_csv(path)
    except FileNotFoundError:
        return None
    df = df.rename(columns={c:c.lower() for c in df.columns})
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
        cutoff = pd.Timestamp.utcnow() - pd.Timedelta(hours=hours)
        df = df[df["timestamp"] >= cutoff].copy()
    # build simple move by game_key+market
    if "game_key" not in df.columns:
        # fall back: order-agnostic key from raw strings if present
        a = df.get("away_team","").astype(str).str.upper().str.strip()
        h = df.get("home_team","").astype(str).str.upper().str.strip()
        df["game_key"] = a.where(a<=h, h + "|" + a).where(a<=h, a + "|" + h)
        df["game_key"] = df["game_key"].where(df["game_key"].str.contains("|", regex=False), a + "|" + h)
    if "line" not in df.columns:
        df["line"] = None
    return df

def clv_rlm_boost(entity_row, splits):
    """Dictionary-less: only boost when we can trivially match entity substring into game_key."""
    if splits is None or len(splits)==0: return 0.0
    ent = str(entity_row["entity"] or "").upper()
    if len(ent) < 2: return 0.0
    # candidate rows that mention the token
    sub = splits[splits["game_key"].astype(str).str.contains(ent, case=False, regex=False)]
    if sub.empty: return 0.0
    # compute net move by game_key+market (latest - earliest)
    sub = sub.sort_values("timestamp")
    bump = 0.0
    for (g,m), grp in sub.groupby([sub["game_key"], sub.get("market","?")]):
        L = pd.to_numeric(grp["line"], errors="coerce").dropna()
        if len(L) < 2: 
            continue
        move = L.iloc[-1] - L.iloc[0]
        # if entity appears as first token in key, assume it's the "away" side, else "home";
        # sign heuristic: a move *toward* the entity (more negative for fav, higher ML) → +1
        # We can’t tell fav/dog reliably here; keep very small, conservative.
        bump += 0.25 * (1 if move!=0 else 0)
    return round(bump, 2)

def write_md(rows, star5, star4, md_path):
    lines = []
    lines.append("# Boardroom Picks\n")
    lines.append(f"Lookback: last 72h. Thresholds: 5★≥{star5}, 4★≥{star4}.\n")
    for tier, title in [(5,"5★ plays"), (4,"4★ plays")]:
        lines.append(f"## {title}\n")
        found = False
        for _,r in rows.iterrows():
            if (tier==5 and r["score"]>=star5) or (tier==4 and star4<=r["score"]<star5):
                found = True
                when = r['last_seen'].strftime("%Y-%m-%d %H:%M UTC") if pd.notna(r['last_seen']) else "—"
                lines.append(f"**{r['entity']}** — {tier}★ (score {r['score']}, signals {int(r['signals'])})  \n")
                if isinstance(r.get("sample_text"), str) and r["sample_text"].strip():
                    lines.append(f"> {r['sample_text'].strip()}\n")
                lines.append(f"_last seen: {when}_\n")
                lines.append("")
        if not found:
            lines.append("_None at this time._\n")
    open(md_path,"w",encoding="utf-8").write("\n".join(lines))

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--signals", required=True)
    ap.add_argument("--splits", default="splits.csv")
    ap.add_argument("--hours", type=int, default=72)
    ap.add_argument("--min_signals", type=int, default=1)
    ap.add_argument("--star5", type=float, default=6.0)
    ap.add_argument("--star4", type=float, default=3.5)
    ap.add_argument("--allow_unknown", action="store_true", default=True)
    ap.add_argument("--out_csv", default="boardroom/boardroom_picks.csv")
    ap.add_argument("--out_md",  default="boardroom/boardroom_picks.md")
    args = ap.parse_args()

    picks = load_signals(args.signals, args.hours)
    if not args.allow_unknown:
        picks = picks[picks["entity"].str.upper()!="UNKNOWN"]
    picks = picks[picks["signals"]>=args.min_signals].copy()

    # optional tiny CLV/RLM bump (safe, conservative) with dictionary-less splits
    splits = load_splits(args.splits, args.hours)
    if splits is not None and not picks.empty:
        picks["clv_boost"] = picks.apply(lambda r: clv_rlm_boost(r, splits), axis=1)
        picks["score"] = (picks["score"].fillna(0) + picks["clv_boost"].fillna(0)).round(2)
    else:
        picks["clv_boost"] = 0.0

    picks = picks.sort_values(["score","signals"], ascending=[False,False])

    # write artifacts
    picks.rename(columns={"score":"score"}).to_csv(args.out_csv, index=False)
    write_md(picks, args.star5, args.star4, args.out_md)
    print(f"[boardroom] wrote: {args.out_csv}")
    print(f"[boardroom] wrote: {args.out_md}")

if __name__ == "__main__":
    main()
