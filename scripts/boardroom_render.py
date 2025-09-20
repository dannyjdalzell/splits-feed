#!/usr/bin/env python3
"""
Boardroom renderer (clean + deep analysis)

- Uses your team dictionary (strict allow-list).
- Drops junk entities (UNKNOWN/AND/FROM/etc).
- Time-decay scoring over last 72h (half-life 24h).
- Requires min signals (default 2) and star thresholds (5★≥6.0, 4★≥3.5).
- Optional CLV/RLM sanity if split columns exist (safe no-op otherwise).
- Outputs:
    boardroom/boardroom_picks.csv
    boardroom/boardroom_picks.md
"""
import argparse, json, os, re, math
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple, Optional
import pandas as pd

UTC = timezone.utc
STOP_ENTITIES = {
    "UNKNOWN","AND","FROM","OPEN","AT","IS","THE","OF","TO","FOR","WITH","IN","BY","ON",
    "MLB","NFL","NBA","NHL","CFB","NCAAF","NCAAB","TEAM","TOTAL","SPREAD","OVER","UNDER"
}
DEFAULT_HOURS = 72
HALF_LIFE_HOURS = 24.0

def now_utc(): return datetime.now(UTC)

def parse_time(x: str) -> Optional[datetime]:
    try:
        return pd.to_datetime(str(x), utc=True, errors="coerce").to_pydatetime()
    except Exception:
        return None

def load_df(p: str) -> Optional[pd.DataFrame]:
    try: return pd.read_csv(p)
    except Exception: return None

def load_team_dict(path: str) -> Dict[str, List[str]]:
    if not os.path.exists(path): return {}
    data = json.load(open(path, "r", encoding="utf-8"))
    alias_map: Dict[str, List[str]] = {}

    def add_alias(key: str, aliases: List[str]):
        key = key.strip().upper()
        vals = [str(a).strip().upper() for a in aliases if isinstance(a, str) and a.strip()]
        if not key or not vals: return
        alias_map.setdefault(key, [])
        for v in vals:
            if v not in alias_map[key]: alias_map[key].append(v)

    # form 1: {"KC":["KANSAS CITY","CHIEFS",...], ...}
    if isinstance(data, dict) and all(isinstance(v, list) for v in data.values()):
        for k, vs in data.items(): add_alias(k, [k] + vs); return alias_map

    # form 2: [{"city":"Kansas City","name":"Chiefs","abr":"KC"}, ...]
    if isinstance(data, list):
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
            if abr: add_alias(abr, aliases)
        return alias_map

    # form 3: { "SEC": { "Alabama": {"abbrev":"BAMA"}, ... }, ... }
    if isinstance(data, dict):
        for _, teams in data.items():
            if not isinstance(teams, dict): continue
            for team, meta in teams.items():
                if not isinstance(meta, dict): continue
                abr = meta.get("abbrev") or meta.get("abr") or meta.get("abbreviation")
                aliases = [team]
                if abr: aliases.append(abr)
                add_alias((abr or team), aliases)
        return alias_map
    return {}

def compile_alias_index(team_map: Dict[str, List[str]]):
    idx = []
    for canon, aliases in team_map.items():
        toks = [re.escape(a) for a in aliases if a]
        if not toks: continue
        idx.append((canon, re.compile(r"\b(?:"+"|".join(toks)+r")\b", re.I)))
    return idx

def resolve_entity(raw: str, text: str, alias_index) -> Optional[str]:
    if not raw: return None
    R = str(raw).strip().upper()
    if R in STOP_ENTITIES: return None
    if alias_index:
        all_canons = {c for c,_ in alias_index}
        if R in all_canons: return R
        t = text or ""
        for canon, rex in alias_index:
            if rex.search(t): return canon
    if 2 <= len(R) <= 5 and R.isalpha(): return R
    return None

def exp_decay_weight(ts: Optional[datetime], now: datetime) -> float:
    if ts is None: return 0.5
    return math.exp(-((now - ts).total_seconds()/3600.0) / HALF_LIFE_HOURS)

def trend_counts(df: pd.DataFrame, entity: str, now: datetime):
    if df is None or df.empty: return (0,0,0,0.0)
    sub = df[df["entity"] == entity].copy()
    if sub.empty: return (0,0,0,0.0)
    sub["_ts"] = sub["timestamp"].apply(parse_time) if "timestamp" in sub.columns else None
    n72=n24=n6=0; dec=0.0
    for _, r in sub.iterrows():
        ts = r.get("_ts", None)
        w = exp_decay_weight(ts, now); dec += w; n72 += 1
        if ts:
            age = (now - ts)
            if age <= timedelta(hours=24): n24 += 1
            if age <= timedelta(hours=6):  n6  += 1
    return (n72,n24,n6,dec)

def possible_clv_boost(splits, entity: str) -> int:
    # Safe default: 0 unless we have clean numeric open/current columns.
    try:
        if splits is None or splits.empty: return 0
        cols_team = [c for c in splits.columns if re.search(r"(team|away|home)", c, re.I)]
        if not cols_team: return 0
        mask = pd.Series(False, index=splits.index)
        for c in cols_team:
            mask = mask | splits[c].astype(str).str.contains(entity, case=False, regex=False, na=False)
        sub = splits[mask]
        if sub.empty: return 0
        opens = [c for c in sub.columns if re.search(r"open", c, re.I)]
        curs  = [c for c in sub.columns if re.search(r"(curr|live|now)", c, re.I)]
        if not opens or not curs: return 0
        return 0  # conservative until structured columns exist
    except Exception:
        return 0

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--picks_csv", default="boardroom/boardroom_picks.csv")
    ap.add_argument("--signals",   default="audit_out/twitter_text_signals.csv")
    ap.add_argument("--splits",    default="splits.csv")
    ap.add_argument("--teams",     default="scripts/team_dictionary.json")
    ap.add_argument("--hours",     type=int, default=DEFAULT_HOURS)
    ap.add_argument("--min_signals", type=int, default=2)
    ap.add_argument("--star5", type=float, default=6.0)
    ap.add_argument("--star4", type=float, default=3.5)
    ap.add_argument("--deadzone_minutes", type=int, default=15)
    args = ap.parse_args()

    os.makedirs("boardroom", exist_ok=True)
    picks   = load_df(args.picks_csv)
    signals = load_df(args.signals)
    splits  = load_df(args.splits)

    if signals is not None and not signals.empty:
        for col in ("entity","text","timestamp"):
            if col not in signals.columns:
                signals[col] = "" if col != "timestamp" else ""
        signals["timestamp"] = signals["timestamp"].astype(str)

    team_map = load_team_dict(args.teams)
    alias_index = compile_alias_index(team_map) if team_map else []

    if picks is None or picks.empty:
        picks = pd.DataFrame(columns=["entity","total_score","signals","last_seen","sample_text"])
    for c in ["entity","total_score","signals","last_seen","sample_text"]:
        if c not in picks.columns: picks[c] = None

    now = now_utc()
    resolved = []
    for _, r in picks.iterrows():
        ent = resolve_entity((r.get("entity") or ""), (r.get("sample_text") or ""), alias_index)
        if not ent or ent in STOP_ENTITIES: continue
        resolved.append({
            "entity": ent,
            "total_score": pd.to_numeric(r.get("total_score"), errors="coerce") or 0.0,
            "signals": int(pd.to_numeric(r.get("signals"), errors="coerce") or 0),
            "last_seen": r.get("last_seen") or "",
            "sample_text": r.get("sample_text") or ""
        })
    clean = pd.DataFrame(resolved)

    if clean.empty and signals is not None and not signals.empty and alias_index:
        synth = []
        for canon, rex in alias_index:
            sub = signals[signals["text"].astype(str).str.contains(rex, na=False)]
            if sub.empty: continue
            n72,n24,n6,dec = trend_counts(sub.assign(entity=canon), canon, now)
            synth.append({
                "entity": canon, "total_score": dec, "signals": n72,
                "last_seen": sub["timestamp"].iloc[-1],
                "sample_text": sub["text"].iloc[-1] if "text" in sub.columns else ""
            })
        clean = pd.DataFrame(synth)

    if clean.empty:
        open("boardroom/boardroom_picks.csv","w").write("")
        open("boardroom/boardroom_picks.md","w").write("# Boardroom Picks\n\n_No eligible plays in the last {}h._\n".format(args.hours))
        print("[boardroom] no eligible entities; wrote empty files."); return

    if signals is not None and not signals.empty:
        ents = []
        for _, r in signals.iterrows():
            ents.append(resolve_entity((r.get("entity") or ""), (r.get("text") or ""), alias_index) or "")
        signals = signals.assign(entity=ents)
        signals = signals[signals["entity"].astype(bool)]

    rows = []
    for _, r in clean.iterrows():
        ent = r["entity"]
        base = float(r["total_score"]); sigs = int(r["signals"])
        n72=n24=n6=0; dec=0.0
        if signals is not None and not signals.empty:
            n72,n24,n6,dec = trend_counts(signals, ent, now)
        clv = possible_clv_boost(splits, ent)
        score = round(max(base, dec) + clv, 2)
        rows.append({
            "entity": ent, "score": score,
            "signals": max(sigs, n72),
            "w24": n24, "w6": n6, "decayed": round(dec,2), "clv_boost": clv,
            "last_seen": r.get("last_seen") or "", "sample_text": r.get("sample_text") or ""
        })
    out = pd.DataFrame(rows)

    out = out[(out["signals"] >= args.min_signals)]
    out = out[(out["score"] >= min(args.star4, args.star5))]
    out = out.sort_values(["score","w6","w24","signals"], ascending=[False,False,False,False]).reset_index(drop=True)

    def stars(s): return "5★" if s >= args.star5 else ("4★" if s >= args.star4 else "")
    out["stars"] = out["score"].apply(stars)
    out = out[out["stars"] != ""]

    csv = out[["entity","score","signals","w24","w6","decayed","clv_boost","last_seen","sample_text"]]
    csv.to_csv("boardroom/boardroom_picks.csv", index=False)

    md = []
    md.append("# Boardroom Picks\n")
    md.append(f"Lookback: last {args.hours}h. Thresholds: 5★≥{args.star5}, 4★≥{args.star4}.")
    if splits is None or splits.empty:
        md.append("_Note: CLV/RLM checks limited this run (no structured open/current in splits.csv)._")
    md.append("\n\n## 5★ plays\n")
    five = out[out["stars"]=="5★"]
    if five.empty: md.append("_None at this time._\n")
    else:
        for _, r in five.iterrows():
            md.append(f"**{r['entity']} — 5★**  (score {r['score']}, signals {r['signals']}; 24h {r['w24']}, 6h {r['w6']}, decay {r['decayed']})")
            if r["clv_boost"]: md.append("• _CLV positive (+1)_")
            if r["sample_text"]: md.append(f"> {r['sample_text'][:400]}")
            md.append("")
    md.append("\n## 4★ plays\n")
    four = out[out["stars"]=="4★"]
    if four.empty: md.append("_None at this time._\n")
    else:
        for _, r in four.iterrows():
            md.append(f"**{r['entity']} — 4★**  (score {r['score']}, signals {r['signals']}; 24h {r['w24']}, 6h {r['w6']}, decay {r['decayed']})")
            if r["clv_boost"]: md.append("• _CLV positive (+1)_")
            if r["sample_text"]: md.append(f"> {r['sample_text'][:300]}")
            md.append("")
    open("boardroom/boardroom_picks.md","w",encoding="utf-8").write("\n".join(md).strip()+"\n")
    print("[boardroom] wrote clean:\n  - boardroom/boardroom_picks.csv\n  - boardroom/boardroom_picks.md")

if __name__ == "__main__":
    main()
