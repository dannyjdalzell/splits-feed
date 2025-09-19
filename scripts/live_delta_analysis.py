#!/usr/bin/env python3
import os, sys, csv, json, math, shutil, datetime as dt
from collections import defaultdict, deque
import pandas as pd

ROOT = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
AOUT = os.path.join(ROOT, "audit_out")
REPORTS = os.path.join(ROOT, "reports")
TODAY = dt.datetime.utcnow().date().isoformat()

# Inputs produced earlier in the pipeline
SPLITS = os.path.join(ROOT, "splits.csv")                          # canonical rolling feed (OCR + fallback text)
BOARDROOM = os.path.join(ROOT, "audit_out", "boardroom_inputs.csv")# normalized aggregator (staged builder)
TWITTER = os.path.join(ROOT, "audit_out", "twitter_text_signals.csv")

# Outputs
DAY_DIR = os.path.join(REPORTS, TODAY)
SNAP_CSV = os.path.join(DAY_DIR, "game_snapshots.csv")
LATEST_MD = os.path.join(DAY_DIR, "analysis_latest.md")
TIMELINE_MD = os.path.join(DAY_DIR, "analysis_timeline.md")

os.makedirs(DAY_DIR, exist_ok=True)

def _read_csv(path, cols_required=None):
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return pd.DataFrame()
    df = pd.read_csv(path, dtype=str, keep_default_na=False).fillna("")
    if cols_required and not set(cols_required).issubset(df.columns):
        return pd.DataFrame()
    return df

def parse_ts(s):
    # Try ISO; fallback to now
    try:
        return dt.datetime.fromisoformat(s.replace("Z","+00:00")).astimezone(dt.timezone.utc)
    except Exception:
        return dt.datetime.now(dt.timezone.utc)

def pct_float(x):
    try:
        return float(x)
    except Exception:
        return None

def game_key(row):
    # league + canonical matchup id (away@home)
    league = row.get("league","Unknown")
    a = row.get("away_team","").strip()
    h = row.get("home_team","").strip()
    return f"{league}::{a} @ {h}"

def cutoff_ok(row, now_utc):
    """
    Honor the 15-minute pre-start cutoff IF we have event_date/event_time in the data.
    Otherwise, allow (we can't know the window).
    """
    evd = row.get("event_date","").strip()
    evt = row.get("event_time","").strip()
    if not evd or not evt:
        return True
    try:
        # Accept forms like '2025-09-19' and '18:05' (assume UTC if no tz)
        dt_str = f"{evd.strip()} {evt.strip()}"
        # Attempt a few common formats
        for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S"):
            try:
                gamedt = dt.datetime.strptime(dt_str, fmt).replace(tzinfo=dt.timezone.utc)
                break
            except ValueError:
                gamedt = None
        if gamedt is None:
            return True
        return now_utc <= (gamedt - dt.timedelta(minutes=15))
    except Exception:
        return True

def load_frame():
    # Prefer canonical splits.csv; if empty, fall back to boardroom inputs
    df = _read_csv(SPLITS, cols_required=["league","away_team","home_team"])
    if df.empty:
        df = _read_csv(BOARDROOM)
        if "team_a" in df.columns and "team_b" in df.columns:
            df = df.rename(columns={"team_a":"away_team","team_b":"home_team"})
    # normalize columns
    for c in ["timestamp","league","away_team","home_team","market","tickets_pct","handle_pct","line","source","event_date","event_time"]:
        if c not in df.columns:
            df[c] = ""
    # dedupe on identity to avoid log growth
    df = df.drop_duplicates(subset=["league","away_team","home_team","market","source","line","tickets_pct","handle_pct"]).reset_index(drop=True)
    return df

def enrich_with_twitter_weights(df):
    tw = _read_csv(TWITTER)
    if tw.empty:
        df["twitter_weight"] = 0.0
        return df
    # explode team list; count high/med occurrences per team
    weights = defaultdict(float)
    for _, r in tw.iterrows():
        teams = [t.strip() for t in str(r.get("teams","")).split("|") if t.strip()]
        lvl = r.get("signal_strength","LOW").upper()
        w = 2.0 if lvl == "HIGH" else (1.0 if lvl == "MED" else 0.25)
        for t in teams:
            weights[t] += w
    # map both away/home
    df["twitter_weight"] = df.apply(lambda r: weights.get(r.get("away_team",""),0.0) + weights.get(r.get("home_team",""),0.0), axis=1)
    return df

def compute_snapshot(df):
    """
    Build a per-game snapshot with:
      - last seen tickets/handle/line by market
      - intraday delta from earliest to latest observation today
      - twitter_weight rollup
    """
    now_utc = dt.datetime.now(dt.timezone.utc)
    rows = []
    grouped = defaultdict(list)
    for _, r in df.iterrows():
        if not cutoff_ok(r, now_utc):
            continue
        grouped[game_key(r)].append(r)

    for gk, items in grouped.items():
        # order by timestamp if present
        items_sorted = sorted(items, key=lambda r: parse_ts(str(r.get("timestamp",""))))
        first, last = items_sorted[0], items_sorted[-1]

        def snap_metric(name):
            f = pct_float(first.get(name,""))
            l = pct_float(last.get(name,""))
            d = (l - f) if (f is not None and l is not None) else None
            return l, d

        last_tix, d_tix   = snap_metric("tickets_pct")
        last_handle, d_h  = snap_metric("handle_pct")
        last_line, d_line = snap_metric("line")

        league = last.get("league","")
        away   = last.get("away_team","")
        home   = last.get("home_team","")
        market = last.get("market","") or "UNKNOWN"
        srcs   = sorted({str(r.get("source","")) for r in items_sorted if str(r.get("source",""))})

        rows.append({
            "date": TODAY,
            "league": league,
            "game": gk,
            "away_team": away,
            "home_team": home,
            "market": market,
            "last_tickets_pct": last_tix if last_tix is not None else "",
            "delta_tickets_pct": round(d_tix,2) if d_tix is not None else "",
            "last_handle_pct":  last_handle if last_handle is not None else "",
            "delta_handle_pct": round(d_h,2) if d_h is not None else "",
            "last_line":        round(last_line,2) if last_line is not None else "",
            "delta_line":       round(d_line,2) if d_line is not None else "",
            "observations":     len(items_sorted),
            "sources":          "|".join(srcs),
            "twitter_weight":   max([pct_float(r.get("twitter_weight","0")) or 0 for r in items_sorted]),
            "first_seen":       parse_ts(str(first.get("timestamp",""))).isoformat(),
            "last_seen":        parse_ts(str(last.get("timestamp",""))).isoformat(),
        })
    snap = pd.DataFrame(rows)
    if not snap.empty:
        snap = snap.sort_values(["league","game","market","last_seen"]).reset_index(drop=True)
    return snap

def write_markdown(snap: pd.DataFrame):
    if snap.empty:
        with open(LATEST_MD, "w") as f:
            f.write("# Live Analysis (latest)\n\n_No promotable games right now._\n")
        with open(TIMELINE_MD, "w") as f:
            f.write("# Live Timeline\n\n_No history today yet._\n")
        return

    # Top movers & strongest signals
    def fmt_row(r):
        parts = []
        if r["delta_handle_pct"] != "":
            parts.append(f"handle {r['delta_handle_pct']:+}")
        if r["delta_tickets_pct"] != "":
            parts.append(f"tickets {r['delta_tickets_pct']:+}")
        if r["delta_line"] != "":
            parts.append(f"line {r['delta_line']:+}")
        delta_txt = ", ".join(parts) if parts else "steady"
        tw = float(r.get("twitter_weight") or 0)
        tw_tag = " (TW⚡)" if tw >= 5 else (" (TW)" if tw >= 2 else "")
        return f"- **{r['league']}** {r['away_team']} @ {r['home_team']} — {r['market']} | last: tix={r['last_tickets_pct']} hdl={r['last_handle_pct']} line={r['last_line']} | Δ {delta_txt}{tw_tag} — src: {r['sources']}"

    # Latest view (by last_seen desc)
    latest = snap.sort_values("last_seen", ascending=False).head(40)
    with open(LATEST_MD, "w") as f:
        f.write("# Live Analysis (latest)\n\n")
        for _, r in latest.iterrows():
            f.write(fmt_row(r) + "\n")

    # Timeline view (group by game, show earliest → latest)
    with open(TIMELINE_MD, "w") as f:
        f.write("# Live Timeline\n\n")
        for (lg, game), grp in snap.groupby(["league","game"]):
            f.write(f"## {lg} — {game}\n")
            g2 = grp.sort_values("last_seen")
            for _, r in g2.iterrows():
                f.write(f"- {r['last_seen']}: tix={r['last_tickets_pct']} (Δ{r['delta_tickets_pct']}), "
                        f"hdl={r['last_handle_pct']} (Δ{r['delta_handle_pct']}), "
                        f"line={r['last_line']} (Δ{r['delta_line']}) — {r['market']} — src:{r['sources']}\n")
            f.write("\n")

def main():
    df = load_frame()
    if df.empty:
        # still lay down empty artifacts for the day
        pd.DataFrame(columns=[
            "date","league","game","away_team","home_team","market",
            "last_tickets_pct","delta_tickets_pct","last_handle_pct","delta_handle_pct",
            "last_line","delta_line","observations","sources","twitter_weight",
            "first_seen","last_seen"
        ]).to_csv(SNAP_CSV, index=False)
        write_markdown(pd.DataFrame())
        print("[live-delta] no rows to analyze; wrote empty artifacts")
        return

    df = enrich_with_twitter_weights(df)
    snap = compute_snapshot(df)

    # append to (or create) per-day snapshot CSV
    if os.path.exists(SNAP_CSV) and os.path.getsize(SNAP_CSV)>0:
        prior = pd.read_csv(SNAP_CSV, dtype=str).fillna("")
        # keep newest measurement per (league, game, market, last_seen)
        combined = pd.concat([prior, snap], ignore_index=True)
        combined = combined.drop_duplicates(subset=["league","game","market","last_seen"]).reset_index(drop=True)
        combined.to_csv(SNAP_CSV, index=False)
    else:
        snap.to_csv(SNAP_CSV, index=False)

    write_markdown(snap)
    print(f"[live-delta] wrote snapshots → {SNAP_CSV}")
    print(f"[live-delta] wrote latest → {LATEST_MD}")
    print(f"[live-delta] wrote timeline → {TIMELINE_MD}")

if __name__ == "__main__":
    main()
