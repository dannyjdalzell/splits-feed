#!/usr/bin/env python3
import time
from pathlib import Path
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

SAVE_DIR = Path.home() / "splits-feed" / "images"
SAVE_DIR.mkdir(parents=True, exist_ok=True)

URL = "https://www.pregame.com/game-center/consensus"
TS = int(time.time())
OUTFILE = SAVE_DIR / f"pregame_most_action_{TS}.png"

UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/126.0.0.0 Safari/537.36"
)

MOST_ACTION_TAB_SELECTORS = [
    "role=tab[name=/Most Action/i]",
    "role=tab[name=/Most Bet/i]",
    "text=/Most Action/i",
    "text=/Most Bet/i",
]

TABLE_KEYWORDS = ["Most Action","Most Bet","Cash %","Ticket %","Tickets %","Handle %","Consensus"]
TABLE_SELECTORS = ["table","[role='table']","section","div"]

def click_first(page, selectors, timeout=4000):
    for sel in selectors:
        try:
            loc = page.locator(sel)
            if loc.count() > 0:
                loc.first.click(timeout=timeout); return sel
        except Exception: pass
    return None

def element_contains_keywords(el, keywords)->bool:
    try: text = el.inner_text(timeout=1000)
    except Exception: return False
    tl = text.lower()
    return any(k.lower() in tl for k in keywords)

def find_best_table(page):
    cands=[]
    for sel in TABLE_SELECTORS:
        try:
            loc=page.locator(sel); n=loc.count()
            for i in range(min(n,20)): cands.append(loc.nth(i))
        except Exception: pass
    scored=[]
    for el in cands:
        try:
            if not element_contains_keywords(el, TABLE_KEYWORDS): continue
            try: rows = el.locator("tr").count()
            except Exception: rows=0
            try: role = el.get_attribute("role") or ""
            except Exception: role=""
            score = (2 if role=="table" else 0) + min(rows,50)
            scored.append((score, rows, role=="table", el))
        except Exception: pass
    if not scored: return None, []
    scored.sort(key=lambda t:t[0], reverse=True)
    return scored[0][3], scored

def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox"])
        ctx = browser.new_context(viewport={"width":1600,"height":1200}, user_agent=UA, java_script_enabled=True)
        page = ctx.new_page()
        try: page.goto(URL, timeout=60000, wait_until="domcontentloaded")
        except PWTimeout: page.goto(URL, timeout=90000)
        page.wait_for_timeout(3000)

        click_first(page, ["button:has-text('Accept')","button:has-text('I Accept')","button:has-text('Agree')","[aria-label='accept']"], timeout=2000)

        clicked = click_first(page, MOST_ACTION_TAB_SELECTORS, timeout=4000)
        if clicked: print(f"[scraper] clicked tab: {clicked}"); page.wait_for_timeout(2000)

        try: page.mouse.wheel(0,600); page.wait_for_timeout(800)
        except Exception: pass

        best, diag = find_best_table(page)
        if diag:
            top = diag[0]
            print(f"[scraper] candidates={len(diag)} best_score={top[0]} rows={top[1]} aria_table={top[2]}")

        if best:
            best.screenshot(path=str(OUTFILE)); print(f"[OK] Saved MOST ACTION -> {OUTFILE}")
        else:
            page.screenshot(path=str(OUTFILE), full_page=True); print(f"[WARN] No table; saved full page -> {OUTFILE}")
        browser.close()

if __name__ == "__main__":
    main()
