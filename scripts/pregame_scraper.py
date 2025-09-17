#!/usr/bin/env python3
import time
from pathlib import Path
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

SAVE_DIR = Path.home() / "splits-feed" / "images"
SAVE_DIR.mkdir(parents=True, exist_ok=True)

URL = "https://www.pregame.com/game-center/consensus"
TS = int(time.time())
OUTFILE = SAVE_DIR / f"pregame_consensus_{TS}.png"

UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/126.0.0.0 Safari/537.36"
)

def click_first(page, selectors, timeout=3000):
    for sel in selectors:
        try:
            el = page.locator(sel)
            if el.count() > 0:
                el.first.click(timeout=timeout)
                return True
        except Exception:
            pass
    return False

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True, args=["--no-sandbox"])
    context = browser.new_context(
        viewport={"width": 1600, "height": 1200},
        user_agent=UA,
        java_script_enabled=True,
    )
    page = context.new_page()
    try:
        page.goto(URL, timeout=60_000, wait_until="domcontentloaded")
    except PWTimeout:
        page.goto(URL, timeout=90_000)
    page.wait_for_timeout(4000)

    click_first(page, [
        "button:has-text('Accept')",
        "button:has-text('I Accept')",
        "button:has-text('I Agree')",
        "button:has-text('Agree')",
        "[aria-label='accept']",
    ])

    candidates = ["table","section:has-text('Consensus')","div:has-text('Consensus')","[role='table']"]
    target = None
    for _ in range(5):
        for sel in candidates:
            loc = page.locator(sel)
            if loc.count() > 0:
                target = loc.first
                break
        if target:
            break
        page.wait_for_timeout(1000)

    if target:
        try:
            target.screenshot(path=str(OUTFILE))
            print(f"[OK] Saved cropped consensus to {OUTFILE}")
        except Exception as e:
            print(f"[WARN] Crop failed ({e}); saving full page")
            page.screenshot(path=str(OUTFILE), full_page=True)
            print(f"[OK] Saved full page to {OUTFILE}")
    else:
        page.screenshot(path=str(OUTFILE), full_page=True)
        print(f"[WARN] Consensus selector not found; saved full page to {OUTFILE}")

    browser.close()
