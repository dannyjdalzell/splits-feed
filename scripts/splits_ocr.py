import re
from datetime import datetime

def clean_pct(x: str) -> int:
    try:
        return int(re.sub(r"[^\d]", "", x))  # strip % and junk
    except:
        return 0

def norm_team(s: str) -> str:
    s = re.sub(r"[\u200b\u2010-\u2015]", "-", s)  # weird dashes
    s = re.sub(r"[^A-Za-z0-9 .'-]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    # kill common OCR garbage
    return s.replace(" O ", " ").replace(" U ", " ").strip(" .-")

def now_ts() -> str:
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"

def parse_mgm_gold(txt: str):
    """
    BetMGM gold sheet. We mine pairs of team rows around '% Bets' and '% Handle' columns.
    We ALSO try to capture a moneyline or spread/total number and attach it to the home side.
    """
    rows = []
    # Narrow to the table region heuristically
    block = txt
    # Grab candidate lines that look like: Team Name ... % Bets xx ... % Handle yy
    cand = []
    for line in block.splitlines():
        if "% Bets" in line or "% Handle" in line:
            continue  # headers
        # keep lines that have a team-ish token and at least one percentage
        if re.search(r"\b[A-Za-z]{3,}\b", line) and re.search(r"\d{1,3}\s*%", line):
            cand.append(line)

    # Build records from each line
    recs = []
    for line in cand:
        # extract team name before columns; often "Chicago Cubs at", "Texas Rangers", etc.
        mteam = re.match(r"\s*([A-Za-z .'-]{3,}?)(?:\s+at\s+[A-Za-z].*)?$", line)
        team = None
        if mteam:
            team = norm_team(mteam.group(1))
        else:
            # fallback: take leading words until first % appears
            team = norm_team(re.split(r"\d{1,3}\s*%", line)[0])

        # percentages near '% Bets' and '% Handle'
        # try “% Bets xx % Handle yy” or any two % in the row
        pcts = re.findall(r"(\d{1,3})\s*%", line)
        bets = handle = 0
        if len(pcts) >= 2:
            # many sheets order: %Bets, %Handle
            bets, handle = clean_pct(pcts[0]), clean_pct(pcts[1])
        elif len(pcts) == 1:
            bets = clean_pct(pcts[0])

        # try to find a moneyline like -145 or +120 on the row
        m_ml = re.search(r"(^|[^\d])([+-]\d{2,3})(?!\d)", line)
        ml = m_ml.group(2) if m_ml else ""

        # try to find a spread like -1.5 / +1.5 (optionally with price after)
        m_sp = re.search(r"([+-]\d+(?:\.\d+)?)", line)
        sp = m_sp.group(1) if m_sp else ""

        recs.append({"team": team, "bets": bets, "handle": handle, "ml": ml, "sp": sp})

    # Pair: top=away, next=home
    for i in range(0, len(recs) - 1, 2):
        away = recs[i]
        home = recs[i + 1]
        league = "Unknown"  # we can infer later if you want by context
        ts = now_ts()

        # Moneyline record (if any ML in either line)
        if away["ml"] or home["ml"]:
            rows.append({
                "timestamp": ts, "league": league,
                "away_team": away["team"], "home_team": home["team"],
                "market": "ML",
                "tickets_pct": home["bets"],     # home side percentages
                "handle_pct": home["handle"],
                "line": home["ml"] or away["ml"],  # prefer home ml
                "source": "MGM_FAM"
            })

        # Spread record (if any +/- spread found)
        if away["sp"] or home["sp"]:
            rows.append({
                "timestamp": ts, "league": league,
                "away_team": away["team"], "home_team": home["team"],
                "market": "Spread",
                "tickets_pct": home["bets"],
                "handle_pct": home["handle"],
                "line": home["sp"] or away["sp"],
                "source": "MGM_FAM"
            })

    return rows

def parse_covers_grid(txt: str):
    """
    Covers/white-blue grid: columns like
      Spread | Handle | Bets | Total | Handle | Bets | Money | Handle | Bets
    We mine rows like:
       Chicago Cubs ... Spread ... Handle 82% ... Bets 78% ... Money ... Handle 79% Bets 61%
    """
    rows = []
    lines = [l for l in txt.splitlines() if re.search(r"[A-Za-z].*\d", l)]
    # collect team names in order and % pairs
    # Heuristic: team names on left margin; percentages appear as '(\d{1,3})%'
    team_lines = []
    for ln in lines:
        # left-most team token (strip emojis if OCR caught any)
        m = re.match(r"\s*([A-Za-z .'-]{3,})\s+", ln)
        if m and re.search(r"\d{1,3}\s*%", ln):
            team_lines.append(ln)

    # Build records per line
    recs = []
    for ln in team_lines:
        t = norm_team(re.match(r"\s*([A-Za-z .'-]{3,})\s+", ln).group(1))
        pcts = re.findall(r"(\d{1,3})\s*%", ln)
        # we normally see triples across Money/Total/Spread; we’ll pick Money side %
s if present
        bets = handle = 0
        if len(pcts) >= 2:
            # use the *rightmost* pair on the row (often Moneyline columns)
            bets = clean_pct(pcts[-1])
            handle = clean_pct(pcts[-2])
        recs.append({"team": t, "bets": bets, "handle": handle})

    # Pair as away/home in order
    for i in range(0, len(recs) - 1, 2):
        away = recs[i]
        home = recs[i + 1]
        rows.append({
            "timestamp": now_ts(), "league": "Unknown",
            "away_team": away["team"], "home_team": home["team"],
            "market": "ML",
            "tickets_pct": home["bets"],
            "handle_pct": home["handle"],
            "line": "", "source": "COVERS_FAM"
        })
        rows.append({
            "timestamp": now_ts(), "league": "Unknown",
            "away_team": away["team"], "home_team": home["team"],
            "market": "Spread",
            "tickets_pct": home["bets"], "handle_pct": home["handle"],
            "line": "", "source": "COVERS_FAM"
        })
        rows.append({
            "timestamp": now_ts(), "league": "Unknown",
            "away_team": away["team"], "home_team": home["team"],
            "market": "Total",
            "tickets_pct": home["bets"], "handle_pct": home["handle"],
            "line": "", "source": "COVERS_FAM"
        })
    return rows

def parse_blocks(txt: str):
    """
    Master parser: try both format-specific parsers and dedupe.
    """
    out = []
    out.extend(parse_mgm_gold(txt))
    out.extend(parse_covers_grid(txt))

    # dedupe identical rows produced by overlapping regex
    seen = set()
    deduped = []
    for r in out:
        key = (r["away_team"], r["home_team"], r["market"], r["tickets_pct"], r["handle_pct"], r["line"], r["source"])
        if key in seen:
            continue
        seen.add(key)
        deduped.append(r)
    return deduped
