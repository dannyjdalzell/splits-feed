#!/usr/bin/env python3
from typing import Optional
from pathlib import Path
import sys, csv
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
IN_FILE = ROOT / "splits.csv"
OUT_CLEAN = ROOT / "splits_clean.csv"
OUT_FLAGGED = ROOT / "splits_flagged.csv"

EXPECTED = ["timestamp","league","away_team","home_team","market","tickets_pct","handle_pct","line","source"]

KNOWN_LEAGUES = {
    "NFL": ["Patriots","Raiders","Giants","Vikings","Falcons","Dolphins","Bills","Broncos","Chargers","Cardinals","Ravens","Chiefs","Cowboys","Packers","Bears","Browns","Jets"],
    "MLB": ["Yankees","Blue Jays","Giants","Diamondbacks","Reds","Cardinals","Brewers","Dodgers","Red Sox","Guardians","Padres","Marlins","Orioles","Mets","Phillies","Nationals","Cubs","Rays"],
    "NCAAF": ["Oklahoma State","Oregon","Northwestern","Missouri","Louisiana","LSU","Michigan","Ohio State","Alabama","Clemson"],
}

def load_df(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    cols_norm = [c.strip().lower() for c in df.columns]
    if set(EXPECTED).issubset(set(cols_norm)):
        df.columns = cols_norm
    else:
        maybe_header = [str(x).strip().lower() for x in df.iloc[0].tolist()]
        if set(EXPECTED).issubset(set(maybe_header)):
            df.columns = maybe_header
            df = df.drop(index=0).reset_index(drop=True)
        else:
            df.columns = EXPECTED[:len(df.columns)] + [f"extra_{i}" for i in range(len(df.columns)-len(EXPECTED))]
    for c in EXPECTED:
        if c not in df.columns: df[c] = ""
        df[c] = df[c].astype(str).str.strip()
    return df[EXPECTED]

def guess_league(away: str, home: str) -> Optional[str]:
    a = (away or "").lower(); h = (home or "").lower()
    for league, teams in KNOWN_LEAGUES.items():
        for t in teams:
            tl = t.lower()
            if tl in a or tl in h:
                return league
    return None

def looks_gibberish(s: str) -> bool:
    if not s: return True
    good = sum(ch.isalnum() or ch in " .,-:/@%+()" for ch in s)
    return (len(s)-good)/max(1,len(s)) > 0.8  # very strict: only extreme junk is "gibberish"

def main():
    if not IN_FILE.exists():
        print(f"[ERROR] {IN_FILE} not found"); sys.exit(1)

    df = load_df(IN_FILE)

    cleaned = []
    flagged = []

    for _, r in df.iterrows():
        rec = {c: (r[c] or "") for c in EXPECTED}

        # Try to fix league if Unknown/empty
        if not rec["league"] or rec["league"].lower() == "unknown":
            g = guess_league(rec["away_team"], rec["home_team"])
            if g: rec["league"] = g

        # KEEP EVERY ROW in clean file
        cleaned.append(rec)

        # Tag junk (only extreme cases: both teams empty OR obvious gibberish)
        reason = None
        if not rec["away_team"] and not rec["home_team"]:
            reason = "no_teams"
        elif looks_gibberish((rec["away_team"]+" "+rec["home_team"]).strip()):
            reason = "gibberish"

        # Tag weak (soft issues that you may want to review)
        if reason is None:
            weak = (
                (not rec["league"] or rec["league"].lower()=="unknown") and
                (not rec["market"] or rec["market"].lower()=="unknown") and
                (not rec["tickets_pct"]) and (not rec["handle_pct"]) and (not rec["line"])
            )
            if weak: reason = "weak"

        if reason:
            flagged.append({**rec, "_flag_reason": reason})

    pd.DataFrame(cleaned, columns=EXPECTED).to_csv(OUT_CLEAN, index=False, quoting=csv.QUOTE_MINIMAL)
    pd.DataFrame(flagged, columns=(EXPECTED+["_flag_reason"]) if flagged else EXPECTED).to_csv(OUT_FLAGGED, index=False, quoting=csv.QUOTE_MINIMAL)

    total = len(df); kept = len(cleaned); fl = len(flagged)
    kp = (kept/total*100) if total else 0; fp = (fl/total*100) if total else 0
    print(f"[audit] total rows: {total}")
    print(f"[audit] kept (clean): {kept} ({kp:.1f}%)")
    print(f"[audit] flagged for review: {fl} ({fp:.1f}%)")
    print(f"[audit] wrote {OUT_CLEAN.name} and {OUT_FLAGGED.name}")
if __name__ == "__main__":
    main()
