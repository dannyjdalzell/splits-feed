#!/usr/bin/env python3
import time
from pathlib import Path
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

SAVE_DIR = Path.home() / "splits-feed" / "images"
SAVE_DIR.mkdir(parents=True, exist_ok=True)

URL = "https://www.pregame.com/game-center/consensus"
TS = int(time.time())
OUTFILE = SAVE_DIR / f"pregame_most_action_{TS}.png"

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) "
      "AppleWebKit/537.36 (KHTML, like Gecko) "
      "Chrome/126.0.0.0 Safari/537.36")

TAB_SEL = [
    "role=tab[name=/Most Action/i]", "role=tab[name=/Most Bet/i]",
    "text=/Most Action/i", "text=/Most Bet/i",
]

KEYWORDS = ["Most Action","Most Bet","Cash %","Ticket %","Tickets %","Handle %","Consensus"]
SEL = ["table","[role='table']","section","div"]

def click_first(page, selectors, timeout=4000):
    for s in selectors:
        try:
            loc = page.locator(s)
            if loc.count() > 0:
                loc.first.click(timeout=timeout); return True
        except Exception: pass
    return False

def contains_any(el, words):
    try: t = el.inner_text(timeout=800).lower()
    except Exception: return False
    return any(w.lower() in t for w in words)

def best_table(page):
    cands = []
    for s in SEL:
        try:
            loc = page.locator(s); n = loc.count()
            for i in range(min(n, 20)): cands.append(loc.nth(i))
        except Exception: pass
    scored = []
    for el in cands:
        try:
            if not contains_any(el, KEYWORDS): continue
            try: rows = el.locator("tr").count()
            except Exception: rows = 0
            try: role = el.get_attribute("role") or ""
            except Exception: role = ""
            score = (2 if role == "table" else 0) + min(rows, 50)
            scored.append((score, rows, role=="table", el))
        except Exception: pass
    if not scored: return None
    scored.sort(key=lambda t:t[0], reverse=True)
    return scored[0][3]

def main():
    with sync_playwright() as p:
        b = p.chromium.launch(headless=True, args=["--no-sandbox"])
        ctx = b.new_context(viewport={"width":1600,"height":1200}, user_agent=UA, java_script_enabled=True)
        page = ctx.new_page()
        try: page.goto(URL, timeout=60000, wait_until="domcontentloaded")
        except PWTimeout: page.goto(URL, timeout=90000)
        page.wait_for_timeout(2500)

        # cookie banners (best-effort)
        click_first(page, ["button:has-text('Accept')","button:has-text('I Accept')","button:has-text('Agree')","[aria-label='accept']"], 2000)

        # Most Action tab
        if click_first(page, TAB_SEL, 4000):
            page.wait_for_timeout(1500)

        # small scroll helps lazy content
        try: page.mouse.wheel(0,600); page.wait_for_timeout(600)
        except Exception: pass

        el = best_table(page)
        if el:
            el.screenshot(path=str(OUTFILE)); print(f"[OK] MOST ACTION -> {OUTFILE}")
        else:
            page.screenshot(path=str(OUTFILE), full_page=True); print(f"[WARN] fallback full page -> {OUTFILE}")
        b.close()

if __name__ == "__main__":
    main()
