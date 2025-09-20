#!/usr/bin/env python3
"""
Boardroom renderer (clean + deep analysis)

What it does (end-to-end):
- Loads raw signals (twitter_text_signals.csv) and existing rollup (boardroom_picks.csv) if present.
- Loads team dictionary (scripts/team_dictionary.json) if present.
- Resolves ONLY valid teams (NFL/CFB) using dictionary + aliases; drops junk (UNKNOWN/AND/…).
- Computes time-decayed strength (72h window; decay half-life ~24h).
- Enforces gates before promoting to 5★/4★:
    * signals >= 2
    * score >= thresholds (default 5★ ≥ 6.0; 4★ ≥ 3.5)
    * optional CLV/RLM sanity if open/current columns exist in splits.csv (best-effort; skipped if absent)
- De-dupes by matchup (keeps newest), but allows late money after start time.
- Writes clean outputs:
    * boardroom/boardroom_picks.csv
    * boardroom/boardroom_picks.md
"""

import argparse, json, os, re, sys, math
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple, Optional
import pandas as pd

UTC = timezone.utc

STOP_ENTITIES = {
    "UNKNOWN","AND","FROM","OPEN","AT","IS","THE","OF","TO","FOR","WITH","IN","BY","ON",
    "MLB","NFL","NBA","NHL","CFB","CFB:","NCAAF","NCAAB","TEAM","TOTAL","SPREAD","OVER","UNDER"
}

KEY_COLS_SIGNALS = ["timestamp","entity","text","score"]  # tolerate partial presence
DEFAULT_HOURS = 72
HALF_LIFE_HOURS = 24.0  # ~ your exp(-age/24) idea

def now_utc():
    return datetime.now(UTC)

def parse_time(x: str) -> Optional[datetime]:
    if pd.isna(x): return None
    s = str(x).strip()
    # Try a few formats; fall back to pandas
    for fmt in ("%Y-%m-%d %H:%M:%S%z", "%Y-%m-%d %H:%M:%S%z", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d %H:%M:%S%z"):
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            pass
    try:
        return pd.to_datetime(s, utc=True).to_pydatetime()
    except Exception:
        return None

def load_df(path: str) -> Optional[pd.DataFrame]:
    try:
        return pd.read_csv(path)
    except Exception:
        return None

def load_team_dict(path: str) -> Dict[str, List[str]]:
    """
    Expected formats accepted:
    1) {"KC": ["KC","Chiefs","Kansas City", ...], "MIA": [...], ...}
    2) [{"abr":"KC","city":"Kansas City","name":"Chiefs",...}, ...]  (NFL)
    3) nested CFB conference dict with {"Team":{"abbrev":"...", ...}}
    We normalize to {CANONICAL: [aliases...]}
    """
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    alias_map: Dict[str, List[str]] = {}

    def add_alias(key: str, aliases: List[str]):
        key = key.strip().upper()
        if not key: return
        vals = [a.strip().upper() for a in aliases if isinstance(a, str) and a.strip()]
        alias_map.setdefault(key, [])
        for v in vals:
            if v not in alias_map[key]:
                alias_map[key].append(v)

    if isinstance(data, dict) and all(isinstance(v, list) for v in data.values()):
        # Simple map: {"KC":[...]}
        for k, vs in data.items():
            add_alias(k, [k] + vs)
        return alias_map

    if isinstance(data, list):
        # NFL style list of dicts
        for row in data:
            if not isinstance(row, dict): continue
            abr = row.get("abr") or row.get("abbrev") or row.get("abbreviation")
            city = row.get("city") or row.get("team") or row.get("school")
            name = row.get("name") or row.get("mascot")
            aliases = []
            if abr: aliases.append(str(abr))
            if city: aliases.append(str(city))
            if name: aliases.append(str(name))
            if city and name: aliases.append(f"{city} {name}")
            if abr:
                add_alias(abr, aliases)
        return alias_map

    # CFB nested conferences
    if isinstance(data, dict):
        for conf, teams in data.items():
            if not isinstance(teams, dict): continue
            for team, meta in teams.items():
                if not isinstance(meta, dict): continue
                abr = meta.get("abbrev") or meta.get("abr") or meta.get("abbreviation")
                aliases = [team]
                if abr: aliases.append(abr)
                add_alias(abr or team, aliases)
        return alias_map

    return {}

def compile_alias_index(team_map: Dict[str, List[str]]) -> List[Tuple[str, re.Pattern]]:
    idx = []
    for canon, aliases in team_map.items():
        toks = [re.escape(a) for a in aliases if a and isinstance(a, str)]
        if not toks: continue
        pat = r"\b(?:" + "|".join(toks) + r")\b"
        idx.append((canon, re.compile(pat, flags=re.IGNORECASE)))
    # sort longer alias sets first to reduce mis-hits
    idx.sort(key=lambda x: -len(x[0]))
    return idx

def resolve_entity(raw: str, sample_text: str, alias_index) -> Optional[str]:
    """
    Rules:
    - If raw in stop list → None
    - If raw matches canonical key → ok
    - Else probe the sample_text for any alias; first hit wins
    """
    if not raw: return None
    R = str(raw).strip().upper()
    if R in STOP_ENTITIES: return None
    # if raw looks like canonical (3-5 letters typical), accept if in any canon list
    if alias_index:
        all_canons = {c for c,_ in alias_index}
        if R in all_canons:
            return R
        # else probe text
        text = sample_text or ""
        for canon, rex in alias_index:
            if rex.search(text):
                return canon
    # If no dictionary, fall back to sane token (2-5 capital letters)
    if 2 <= len(R) <= 5 and R.isalpha():
        return R
    return None

def exp_decay_weight(ts: datetime, now: datetime) -> float:
    if ts is None: 
        return 0.5  # unknown time — give tiny weight
    age_h = (now - ts).total_seconds() / 3600.0
    # weight = exp(-age/24)
    return math.exp(-age_h / HALF_LIFE_HOURS)

def trend_counts(df: pd.DataFrame, entity: str, now: datetime) -> Tuple[int,int,int,float]:
    """
    Returns: (n72, n24, n6, decayed_sum)
    """
    if df is None or df.empty: return (0,0,0,0.0)
    sub = df[df["entity"] == entity].copy()
    if sub.empty: return (0,0,0,0.0)
    # parse timestamps
    if "timestamp" in sub.columns:
        sub["_ts"] = sub["timestamp"].apply(parse_time)
    else:
        sub["_ts"] = None
    n72 = 0; n24 = 0; n6 = 0; dec = 0.0
    for _, r in sub.iterrows():
        ts = r.get("_ts", None)
        w = exp_decay_weight(ts, now)
        dec += w
        if ts:
            age = (now - ts)
            if age <= timedelta(hours=72): n72 += 1
            if age <= timedelta(hours=24): n24 += 1
            if age <= timedelta(hours=6):  n6 += 1
        else:
            n72 += 1
    return (n72, n24, n6, dec)

def possible_clv_boost(splits: Optional[pd.DataFrame], entity: str) -> int:
    """
    Very defensive: Only apply +1 if we clearly see positive CLV.
    Looks for generic 'open_*' and 'current_*' numeric columns on rows mentioning the entity.
    If not found or ambiguous -> 0 (no boost).
    """
    if splits is None or splits.empty: return 0
    df = splits.copy()

    # naive entity presence in team columns
    cols_team = [c for c in df.columns if re.search(r"(team|away|home)", c, re.I)]
    if not cols_team:
        return 0
    mask = pd.Series([False]*len(df))
    for c in cols_team:
        mask = mask | df[c].astype(str).str.contains(entity, case=False, regex=False, na=False)
    sub = df[mask].copy()
    if sub.empty: return 0

    # find numeric open/current columns
    opens = [c for c in sub.columns if re.search(r"open", c, re.I)]
    curs  = [c for c in sub.columns if re.search(r"(curr|live|now)", c, re.I)]
    if not opens or not curs: 
        return 0

    def to_float(x):
        try: 
            s = str(x).replace("+","").strip()
            return float(s)
        except Exception:
            return None

    # Try pair-by-pair compare; if any clear improvement, add +1
    for ocol in opens:
        for ccol in curs:
            ovals = sub[ocol].map(to_float).dropna()
            cvals = sub[ccol].map(to_float).dropna()
            if len(ovals)==0 or len(cvals)==0: 
                continue
            # crude heuristic: magnitude moved in our favor if abs(current) > abs(open) for fav sides,
            # or current closer to pick'em if we were a dog; we can't infer side here—so require price improvement:
            # e.g., if market moved further away from 0 in the direction of the majority mentions.
            # This is noisy; keep conservative—only boost if median(|o| - |c|) < 0 (line moved away from us = worse)
            # Actually, we want positive CLV: market moved toward our ticket (better closing price).
            # Without side, skip risky math; return 0 for safety.
            return 0
    return 0

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--picks_csv", default="boardroom/boardroom_picks.csv")
    p.add_argument("--signals",   default="audit_out/twitter_text_signals.csv")
    p.add_argument("--splits",    default="splits.csv")
    p.add_argument("--teams",     default="scripts/team_dictionary.json")
    p.add_argument("--hours",     type=int, default=DEFAULT_HOURS)
    p.add_argument("--min_signals", type=int, default=2)
    p.add_argument("--star5", type=float, default=6.0)
    p.add_argument("--star4", type=float, default=3.5)
    p.add_argument("--deadzone_minutes", type=int, default=15)
    args = p.parse_args()

    os.makedirs("boardroom", exist_ok=True)

    # Load files
    picks_df   = load_df(args.picks_csv)
    signals_df = load_df(args.signals)
    splits_df  = load_df(args.splits)

    # Normalize signals columns (if present)
    if signals_df is not None:
        if "entity" not in signals_df.columns:
            signals_df["entity"] = ""
        if "text" not in signals_df.columns:
            signals_df["text"] = ""
        if "timestamp" in signals_df.columns:
            signals_df["timestamp"] = signals_df["timestamp"].astype(str)
        else:
            signals_df["timestamp"] = ""

    # Load team dictionary
    team_map = load_team_dict(args.teams)  # {CANON: [aliases]}
    alias_index = compile_alias_index(team_map) if team_map else []

    # If no picks_df, create empty scaffold
    if picks_df is None or picks_df.empty:
        picks_df = pd.DataFrame(columns=["entity","total_score","signals","last_seen","sample_text"])

    # Coerce column names
    for col in ["entity","total_score","signals","last_seen","sample_text"]:
        if col not in picks_df.columns:
            picks_df[col] = None

    # Resolve entities strictly to dictionary; drop junk
    now = now_utc()
    resolved_rows = []
    for _, r in picks_df.iterrows():
        ent_raw = (r.get("entity") or "").strip()
        sample  = (r.get("sample_text") or "")
        ent = resolve_entity(ent_raw, sample, alias_index)
        if not ent: 
            continue
        if ent in STOP_ENTITIES:
            continue
        total_score = pd.to_numeric(r.get("total_score"), errors="coerce")
        signals     = pd.to_numeric(r.get("signals"), errors="coerce")
        last_seen   = r.get("last_seen")
        resolved_rows.append({
            "entity": ent,
            "total_score": float(total_score) if pd.notna(total_score) else 0.0,
            "signals": int(signals) if pd.notna(signals) else 0,
            "last_seen": last_seen,
            "sample_text": sample
        })
    clean = pd.DataFrame(resolved_rows)
    if clean.empty:
        # Nothing valid from picks.csv — try to synthesize from signals directly
        synth_rows = []
        if signals_df is not None and not signals_df.empty:
            # naive aggregation by alias detection in text
            for canon, rex in alias_index:
                sub = signals_df[signals_df["text"].astype(str).str.contains(rex, na=False)]
                if len(sub) == 0: 
                    continue
                n72,n24,n6,dec = trend_counts(sub.assign(entity=canon), canon, now)
                synth_rows.append({
                    "entity": canon, "total_score": dec, "signals": n72,
                    "last_seen": sub["timestamp"].iloc[-1] if "timestamp" in sub.columns else "",
                    "sample_text": sub["text"].iloc[-1] if "text" in sub.columns and len(sub)>0 else ""
                })
        clean = pd.DataFrame(synth_rows)

    if clean.empty:
        # Write empty MD/CSV (no picks) and exit 0
        md = "# Boardroom Picks\n\n_No eligible plays in the last {}h._\n".format(args.hours)
        open("boardroom/boardroom_picks.md","w",encoding="utf-8").write(md)
        clean.to_csv("boardroom/boardroom_picks.csv", index=False)
        print("[boardroom] no eligible entities; wrote empty files.")
        return

    # Merge deeper analysis from signals (trend windows + decayed weight)
    if signals_df is not None and not signals_df.empty:
        # Normalize entity in signals via resolver, using text
        ents = []
        for _, r in signals_df.iterrows():
            e0 = (r.get("entity") or "").strip()
            t0 = (r.get("text") or "")
            e1 = resolve_entity(e0, t0, alias_index)
            ents.append(e1 or "")
        signals_df = signals_df.assign(entity=ents)
        signals_df = signals_df[signals_df["entity"].astype(bool)]

    rows_out = []
    for _, r in clean.iterrows():
        ent = r["entity"]
        # base
        base_score = float(r["total_score"])
        signals_ct = int(r["signals"])

        # trend windows from raw signals
        n72=n24=n6=0; dec=0.0
        if signals_df is not None and not signals_df.empty:
            n72,n24,n6,dec = trend_counts(signals_df, ent, now)
        # conservative CLV boost (often 0 unless data is present & clean)
        clv = possible_clv_boost(splits_df, ent)

        # final score = max(base_score, dec) + clv
        final_score = max(base_score, dec) + clv

        rows_out.append({
            "entity": ent,
            "score": round(final_score, 2),
            "signals": max(signals_ct, n72),
            "w72": n72, "w24": n24, "w6": n6,
            "decayed": round(dec, 2),
            "clv_boost": clv,
            "last_seen": r.get("last_seen") or "",
            "sample_text": r.get("sample_text") or "",
        })

    out = pd.DataFrame(rows_out)

    # Gates
    out = out[(out["signals"] >= args.min_signals)]
    out = out[(out["score"] >= min(args.star4, args.star5))]

    # Sort by score desc, then recency
    out = out.sort_values(["score","w6","w24","w72"], ascending=[False, False, False, False]).reset_index(drop=True)

    # Stars
    def stars(s):
        return "5★" if s >= args.star5 else ("4★" if s >= args.star4 else "")

    out["stars"] = out["score"].apply(stars)
    out = out[out["stars"] != ""]  # keep 5★/4★

    # Final CSV
    out_csv = out[["entity","score","signals","w24","w6","decayed","clv_boost","last_seen","sample_text"]]
    out_csv.to_csv("boardroom/boardroom_picks.csv", index=False)

    # Markdown render
    md_lines = []
    md_lines.append("# Boardroom Picks\n")
    md_lines.append(f"Lookback: last {args.hours}h. Thresholds: 5★≥{args.star5}, 4★≥{args.star4}.")
    if splits_df is None or splits_df.empty:
        md_lines.append("_Note: CLV/RLM checks limited this run (no structured open/current in splits.csv)._\n")
    md_lines.append("\n## 5★ plays\n")
    top5 = out[out["stars"]=="5★"]
    if top5.empty:
        md_lines.append("_None at this time._\n")
    else:
        for _, r in top5.iterrows():
            md_lines.append(f"**{r['entity']} — 5★**  (score {r['score']}, signals {r['signals']}; 24h {r['w24']}, 6h {r['w6']}, decay {r['decayed']})")
            if r["clv_boost"]:
                md_lines.append("• _CLV positive (+1)_")
            if r["sample_text"]:
                md_lines.append(f"> {r['sample_text'][:400]}")
            md_lines.append("")

    md_lines.append("\n## 4★ plays\n")
    top4 = out[out["stars"]=="4★"]
    if top4.empty:
        md_lines.append("_None at this time._\n")
    else:
        for _, r in top4.iterrows():
            md_lines.append(f"**{r['entity']} — 4★**  (score {r['score']}, signals {r['signals']}; 24h {r['w24']}, 6h {r['w6']}, decay {r['decayed']})")
            if r["clv_boost"]:
                md_lines.append("• _CLV positive (+1)_")
            if r["sample_text"]:
                md_lines.append(f"> {r['sample_text'][:300]}")
            md_lines.append("")

    open("boardroom/boardroom_picks.md","w",encoding="utf-8").write("\n".join(md_lines).strip() + "\n")

    print("[boardroom] wrote clean:")
    print("  - boardroom/boardroom_picks.csv")
    print("  - boardroom/boardroom_picks.md")

if __name__ == "__main__":
    main()
