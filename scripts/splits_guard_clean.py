#!/usr/bin/env python3
import re, sys, argparse
from datetime import datetime, timezone
import pandas as pd

# OCR garbage phrases we always drop
BAD_PATTERNS = [
    r"Estimating resolution",
    r"Betting Splits Expanded Splits",
    r"Handle Bets Total Handle Bets",
    r"Money dle Bets",
    r"\bChiefs ad\b",
]

MARKETS = {"Spread","ML","Moneyline","Total","O/U","OU"}

def norm(s:str)->str:
    s = re.sub(r"[^A-Za-z0-9&@ +./'-]", " ", str(s or ""))
    s = re.sub(r"\s+", " ", s).strip()
    return s

def clean_team(s:str)->str:
    """Heuristics only; no dictionary."""
    s = norm(s)
    # strip common OCR tails
    s = re.sub(r"MLB - .*", "", s).strip()
    s = re.sub(r"\bSPORTSBOOK\b.*", "", s).strip()
    s = re.sub(r"\bEF\b.*", "", s).strip()
    s = re.sub(r"^\brs\b\s*", "", s)   # 'rs Los Angeles Chargers' → 'Los Angeles Chargers'
    s = re.sub(r"^\bs\b\s*", "", s)    # 's Total Bets' → 'Total Bets' (will be dropped later)
    # keep letters, spaces & a few symbols
    s = re.sub(r"[^A-Za-z0-9& +./'-]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def bad_team(s:str)->bool:
    s = s or ""
    if not s: return True
    for pat in BAD_PATTERNS:
        if re.search(pat, s, flags=re.I): return True
    # too short or generic
    letters = re.sub(r"[^A-Za-z]", "", s)
    if len(letters) < 3: return True
    if s.lower() in {"spread","ml","total","o/u","ou","fu"}: return True
    return False

def coerce_pct(x):
    try:
        if x is None or str(x).strip()=="":
            return None
        v = float(str(x).replace("%","").strip())
        if 0 <= v <= 100: return v
    except: pass
    return None

def to_ts(x):
    try:
        return pd.to_datetime(x, utc=True)
    except:
        return pd.NaT

def game_key(a:str, b:str)->str:
    a, b = norm(a).upper(), norm(b).upper()
    if a <= b: 
        return f"{a}|{b}"
    return f"{b}|{a}"

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="src", default="splits.csv")
    ap.add_argument("--out", dest="out", default="splits_clean.csv")
    ap.add_argument("--promote", action="store_true")
    args = ap.parse_args()

    try:
        df = pd.read_csv(args.src)
    except FileNotFoundError:
        print(f"[guard] no input {args.src}; exiting.")
        sys.exit(0)

    orig = len(df)
    # normalize headers
    df = df.rename(columns={c: norm(c).lower().replace(" ","_") for c in df.columns})

    need = {"timestamp","away_team","home_team","market"}
    if not need.issubset(df.columns):
        print(f"[guard] missing cols {need - set(df.columns)}; exiting.")
        sys.exit(0)

    # scrub
    df["away_team"] = df["away_team"].map(clean_team)
    df["home_team"] = df["home_team"].map(clean_team)

    # drop obvious garbage by substring
    bad_mask = False
    for col in ["away_team","home_team"]:
        m = df[col].astype(str).str.contains("|".join(BAD_PATTERNS), case=False, regex=True)
        bad_mask = m if bad_mask is False else (bad_mask | m)
    df = df[~bad_mask].copy()

    # coerce %
    if "tickets_pct" in df.columns: df["tickets_pct"] = df["tickets_pct"].map(coerce_pct)
    else: df["tickets_pct"] = None
    if "handle_pct" in df.columns:  df["handle_pct"]  = df["handle_pct"].map(coerce_pct)
    else: df["handle_pct"] = None

    # market normalize
    df["market"] = df["market"].astype(str).str.strip()
    df["market"] = df["market"].replace({"Moneyline":"ML","Money Line":"ML","O/U":"Total","OU":"Total"})
    df = df[df["market"].isin(MARKETS)]

    # drop rows with bad teams
    good = (~df["away_team"].map(bad_team)) & (~df["home_team"].map(bad_team)) & (df["away_team"]!=df["home_team"])
    df = df[good].copy()

    # timestamp
    df["timestamp"] = df["timestamp"].map(to_ts)
    df = df[df["timestamp"].notna()]

    # construct game key (order agnostic)
    df["game_key"] = [game_key(a,b) for a,b in zip(df["away_team"], df["home_team"])]

    # keep minimal useful columns
    keep = ["timestamp","game_key","away_team","home_team","market","tickets_pct","handle_pct","line"]
    for k in keep:
        if k not in df.columns: df[k] = None
    df = df[keep].sort_values("timestamp")

    df.to_csv(args.out, index=False)
    print(f"[guard] input {orig} → kept {len(df)}; wrote {args.out}")
    if args.promote:
        df.to_csv("splits.csv", index=False)
        print("[guard] promoted splits_clean.csv → splits.csv")

if __name__ == "__main__":
    main()
