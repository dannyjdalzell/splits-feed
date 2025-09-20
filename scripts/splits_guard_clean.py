# scripts/splits_guard_clean.py
import pandas as pd, re, os, sys
from datetime import datetime, timezone

SRC = "splits.csv"
OUT = "splits_clean.csv"

if not os.path.exists(SRC):
    print(f"[guard] {SRC} missing; nothing to do.")
    sys.exit(0)

df = pd.read_csv(SRC)

# Require these columns to exist; otherwise bail safely
need = {"timestamp","league","away_team","home_team","market","tickets_pct","handle_pct","line","source"}
missing = [c for c in need if c not in df.columns]
if missing:
    print(f"[guard] missing columns: {missing} -> writing empty {OUT} and exiting.")
    pd.DataFrame(columns=list(need)).to_csv(OUT, index=False)
    # Do NOT overwrite splits.csv if shape is wrong
    sys.exit(0)

# Basic sanitation
def bad_team(s):
    if not isinstance(s,str): return True
    s2 = s.strip()
    if len(s2) < 2 or len(s2) > 40: return True
    junk = (
        "Estimating resolution", "SPORTSBOOK", "Betting Splits", "Expanded Splits",
        "Money Handle", "Total Handle", "Bets RL", "Spread", "ad", "EF s", "El S"
    )
    if any(k.lower() in s2.lower() for k in junk): return True
    # must contain letters, not only punctuation/numbers
    if not re.search(r"[A-Za-z]", s2): return True
    return False

df = df.copy()

# Drop rows that aren’t real games
mask_good = (
    df["league"].astype(str).str.upper().isin([
        "NFL","NCAAF","NBA","NCAAB","MLB","NHL","WNBA","MLS","UFC"
    ])
)
mask_good &= ~df["away_team"].apply(bad_team)
mask_good &= ~df["home_team"].apply(bad_team)
mask_good &= df["market"].astype(str).str.upper().isin(["SPREAD","ML","TOTAL","OU","O/U"])

# Numeric sanity
def to_num(x):
    try:
        return float(str(x).strip().replace("%",""))
    except: return float("nan")

df["tickets_pct"] = df["tickets_pct"].map(to_num)
df["handle_pct"]  = df["handle_pct"].map(to_num)
df["line"] = pd.to_numeric(df["line"], errors="coerce")

mask_good &= df["tickets_pct"].between(0,100, inclusive="both")
mask_good &= df["handle_pct"].between(0,100, inclusive="both")
mask_good &= df["line"].abs() < 60  # kill wild OCR

clean = df[mask_good].copy()

# De-dup: keep most recent per (league, away, home, market)
def to_ts(x):
    try:
        return pd.to_datetime(x, utc=True)
    except:
        return pd.NaT

clean["ts"] = clean["timestamp"].map(to_ts)
clean = clean.dropna(subset=["ts"])
clean = (clean.sort_values("ts")
              .drop_duplicates(subset=["league","away_team","home_team","market"], keep="last")
              .drop(columns=["ts"]))

# Write clean view
clean.to_csv(OUT, index=False)
print(f"[guard] kept {len(clean)} / {len(df)} rows -> {OUT}")

# If we kept *something*, promote to splits.csv. Otherwise, leave the previous file untouched.
if len(clean) >= 25:
    clean.to_csv(SRC, index=False)
    print(f"[guard] promoted {OUT} -> {SRC}")
else:
    print("[guard] not enough clean rows to promote; leaving splits.csv as-is")
