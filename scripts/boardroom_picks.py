#!/usr/bin/env python3
# scripts/boardroom_picks.py
import argparse, os, sys, re
from datetime import datetime, timedelta, timezone
import pandas as pd

DEFAULT_SIGNALS = "audit_out/twitter_text_signals.csv"
DEFAULT_SPLITS  = "splits.csv"
DEFAULT_OUT_CSV = "boardroom/boardroom_picks.csv"
DEFAULT_OUT_MD  = "boardroom/boardroom_picks.md"

# === CONFIG YOU APPROVED ===
LOOKBACK_HOURS   = 72
FIVE_STAR_MIN    = 6.0
FOUR_STAR_MIN    = 3.5
TOP_SAMPLE_ROWS  = 3

# Only used if the upstream file lacks a numeric "score" column.
KEYWORD_WEIGHTS = {
    "reverse line movement": 3.0,
    "steam": 2.5,
    "sharp": 2.0,
    "contrarian": 1.5,
    "buyback": 1.5,
}

ABBREV_HINTS = set((
    "ATL","ARI","BAL","BOS","BUF","CAR","CHI","CIN","CLE","DAL","DEN","DET","GSW","FLA","HOU","IND",
    "JAX","KC","LAC","LAD","LAL","LAR","LV","LVR","MIA","MIL","MIN","NYG","NYY","NYM","NYJ","NYK",
    "NO","NOP","OKC","ORL","PHI","PHL","PHX","PHO","PIT","POR","SEA","SFG","SF","TB","TEN","TOR",
    "UTA","VAN","WAS","WSH","UCLA","USC","UGA","BAMA","TEX"
))
ABBREV_RE = re.compile(r'\b([A-Z]{2,4})\b')
PCT_RE = re.compile(r'(\b\d{1,3})\s*%')

def parse_args():
    ap = argparse.ArgumentParser(description="Make Boardroom 5★/4★ picks from signals (and optional splits).")
    ap.add_argument("--signals", default=DEFAULT_SIGNALS, help="Path to twitter_text_signals.csv")
    ap.add_argument("--splits", default=DEFAULT_SPLITS, help="Optional splits.csv (ignored if missing)")
    ap.add_argument("--out-csv", default=DEFAULT_OUT_CSV, help="Output CSV path")
    ap.add_argument("--out-md",  default=DEFAULT_OUT_MD,  help="Output Markdown summary")
    return ap.parse_args()

def load_signals(path):
    if not os.path.isfile(path):
        sys.exit(f"[boardroom] missing signals file: {path}")
    df = pd.read_csv(path)

    # normalize columns
    for col in ["timestamp","handle","text","url","keyword","entity","score"]:
        if col not in df.columns:
            df[col] = None

    # timestamps (UTC if possible)
    if df["timestamp"].notna().any():
        try:
            df["ts"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)
        except Exception:
            df["ts"] = pd.NaT
    else:
        df["ts"] = pd.NaT

    df["text"] = df["text"].fillna("").astype(str)
    df["text_lc"] = df["text"].str.lower()
    df["handle"] = df["handle"].fillna("").astype(str)
    return df

def infer_entity(row):
    ent = str(row.get("entity") or "").strip()
    if ent and ent.upper() != "UNKNOWN":
        return ent
    uppers = ABBREV_RE.findall(row["text"].upper())
    for tok in uppers:
        if tok in ABBREV_HINTS:
            return tok
    m = re.search(r'\b([A-Z]{2,4})\s*[+-]\d+(?:\.\d+)?\b', row["text"].upper())
    if m:
        return m.group(1)
    return "UNKNOWN"

def score_from_text(text_lc: str) -> float:
    score = 0.0
    for k, w in KEYWORD_WEIGHTS.items():
        if k in text_lc:
            score += w
    if "handle" in text_lc and "ticket" in text_lc:
        nums = [int(x) for x in PCT_RE.findall(text_lc) if x.isdigit()]
        if len(nums) >= 2 and abs(nums[0]-nums[1]) >= 10:
            score += 1.0
        else:
            score += 0.5
    return score

def attach_scores(df):
    df["entity"] = df.apply(infer_entity, axis=1)
    if "score" in df.columns and pd.api.types.is_numeric_dtype(df["score"]):
        df["row_score"] = df["score"].fillna(0.0)
    else:
        df["row_score"] = df["text_lc"].apply(score_from_text)
    return df

def filter_lookback(df, hours):
    if df["ts"].isna().all():
        return df
    cutoff = pd.Timestamp.now(tz=timezone.utc) - timedelta(hours=hours)
    return df[df["ts"] >= cutoff]

def aggregate(df):
    grp = df.groupby("entity", dropna=False).agg(
        total_score = ("row_score","sum"),
        signals     = ("row_score","size"),
        last_seen   = ("ts","max"),
        sample_text = ("text", lambda s: " | ".join(s.head(TOP_SAMPLE_ROWS)))
    ).reset_index()
    grp = grp.sort_values(["total_score","signals"], ascending=[False, False])
    return grp

def star_rating(score):
    if score >= FIVE_STAR_MIN: return 5
    if score >= FOUR_STAR_MIN: return 4
    return 0

def add_stars(df):
    df["stars"] = df["total_score"].apply(star_rating)
    return df

def merge_splits_if_present(df, splits_path):
    if not os.path.isfile(splits_path):
        return df
    try:
        sp = pd.read_csv(splits_path)
        key = None
        for c in ["team","abbr","entity","side","Team","TEAM"]:
            if c in sp.columns:
                key = c; break
        if key:
            sp["_key"] = sp[key].fillna("").astype(str).str.upper()
            df["_key"] = df["entity"].fillna("").astype(str).str.upper()
            merged = df.merge(sp, on="_key", how="left", suffixes=("","_splits"))
            merged.drop(columns=["_key"], inplace=True)
            return merged
    except Exception:
        pass
    return df

def write_outputs(df, out_csv, out_md):
    os.makedirs(os.path.dirname(out_csv), exist_ok=True)
    os.makedirs(os.path.dirname(out_md),  exist_ok=True)

    df.to_csv(out_csv, index=False)

    five = df[df["stars"]==5].copy()
    four = df[df["stars"]==4].copy()

    lines = []
    lines.append("# Boardroom Picks\n")
    lines.append(f"_Lookback: last {LOOKBACK_HOURS}h. Thresholds: 5★≥{FIVE_STAR_MIN}, 4★≥{FOUR_STAR_MIN}._\n")

    def block(title, part):
        lines.append(f"## {title}\n")
        if part.empty:
            lines.append("- (none)\n")
            return
        for _, r in part.iterrows():
            last = r["last_seen"]
            last_str = "" if pd.isna(last) else f" — last: {last}"
            lines.append(f"**{r['entity']}** — {int(r['stars'])}★ (score {r['total_score']:.1f}, signals {int(r['signals'])}){last_str}\n")
            if r.get("sample_text"):
                lines.append(f"> {r['sample_text']}\n")

    block("5★ plays", five)
    block("4★ plays", four)

    with open(out_md, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

def main():
    args = parse_args()
    sigs = load_signals(args.signals)
    sigs = attach_scores(sigs)
    sigs = filter_lookback(sigs, LOOKBACK_HOURS)
    agg  = aggregate(sigs)
    agg  = add_stars(agg)
    agg  = merge_splits_if_present(agg, args.splits)
    write_outputs(agg, args.out_csv, args.out_md)
    print(f"[boardroom] wrote: {args.out_csv}")
    print(f"[boardroom] wrote: {args.out_md}")
    if not ((agg["stars"]==5) | (agg["stars"]==4)).any():
        # 78 = "no data/meaningful output" but not an error in CI
        sys.exit(78)

if __name__ == "__main__":
    main()
