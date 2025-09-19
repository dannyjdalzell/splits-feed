import os, sys, argparse, csv, re
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import cv2
import numpy as np
import pytesseract

# ---- keyword -> source mapping (add as needed) ----
KEYMAP = [
    (r"\bCIRCA\b",                 "CIRCA_FAM"),
    (r"\bBRACCO\b",                "BRACCO_FAM"),
    (r"\bFAN ?DUEL\b",             "FANDUEL_FAM"),
    (r"\bFD SPORTSBOOK\b",         "FANDUEL_FAM"),
    (r"\bDRAFT ?KINGS\b",          "GRID_OCR"),
    (r"\bDK\b",                     "GRID_OCR"),
    (r"\bBET ?MGM\b",              "MGM_FAM"),
    (r"\bMGM\b",                   "MGM_FAM"),
    (r"\bCOVERS\b",                "COVERS_FAM"),
    (r"\bPREGAME\b",               "Pregame"),
]

def detect_source_from_corner(img_bgr):
    """Crop top-left corner, enhance, OCR, and map to a source."""
    h, w = img_bgr.shape[:2]
    # Heuristic crops to catch that label zone. Try a couple of rectangles.
    crops = [
        (0, 0, int(0.55*w), int(0.25*h)),  # wider top-left
        (0, 0, int(0.40*w), int(0.18*h)),  # tight top-left
        (int(0.02*w), int(0.02*h), int(0.45*w), int(0.20*h)),  # offset a bit
    ]
    texts = []

    for (x0,y0,x1,y1) in crops:
        roi = img_bgr[y0:y1, x0:x1]
        if roi.size == 0:
            continue
        gray = cv2.cvtColor(roi, cv2.COLOR_BGR2GRAY)
        # Upscale for sharper text
        scale = 2
        gray = cv2.resize(gray, (gray.shape[1]*scale, gray.shape[0]*scale), interpolation=cv2.INTER_CUBIC)
        # Normalize + threshold several ways and OCR each; stop on first confident hit
        candidates = []

        # 1) CLAHE + Otsu
        clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8,8)).apply(gray)
        _, th_otsu = cv2.threshold(clahe, 0, 255, cv2.THRESH_BINARY+cv2.THRESH_OTSU)
        candidates.append(th_otsu)

        # 2) Adaptive threshold
        th_ad = cv2.adaptiveThreshold(gray, 255, cv2.ADAPTIVE_THRESH_MEAN_C,
                                      cv2.THRESH_BINARY, 31, 10)
        candidates.append(th_ad)

        # 3) Plain high-contrast
        norm = cv2.normalize(gray, None, 0, 255, cv2.NORM_MINMAX)
        candidates.append(norm)

        for cand in candidates:
            config = "--oem 3 --psm 6"
            txt = pytesseract.image_to_string(cand, config=config)
            txtU = re.sub(r"[^A-Z0-9 _-]", " ", txt.upper())
            texts.append(txtU)
            for pat, label in KEYMAP:
                if re.search(pat, txtU):
                    return label, txtU

    # Nothing matched; return aggregated text for debugging
    combined = " | ".join(texts)
    return "UNKNOWN", combined

def scan_images(root, since_hours):
    # List files under images/ newer than cutoff (or all if since_hours<=0)
    cutoff = None
    if since_hours > 0:
        cutoff = datetime.now(ZoneInfo("America/Chicago")) - timedelta(hours=since_hours)

    files = []
    for base, _, names in os.walk(root):
        for n in names:
            p = os.path.join(base, n)
            try:
                # skip non-images by extension
                if not re.search(r"\.(png|jpe?g|webp)$", n, re.I):
                    continue
                if cutoff:
                    ts = datetime.fromtimestamp(os.path.getmtime(p), tz=ZoneInfo("America/Chicago"))
                    if ts < cutoff:
                        continue
                files.append(p)
            except Exception:
                pass
    files.sort()
    return files

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--images", default="images", help="images folder")
    ap.add_argument("--since", type=int, default=72, help="hours back to scan (use 0 for all)")
    ap.add_argument("--out", default="audit/source_detect_top_left.csv", help="output csv")
    args = ap.parse_args()

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    files = scan_images(args.images, args.since)

    counts = {}
    rows = []
    for fp in files:
        img = cv2.imread(fp)
        if img is None:
            continue
        label, saw = detect_source_from_corner(img)
        counts[label] = counts.get(label, 0) + 1
        rows.append({"file": fp, "detected_source": label, "corner_text_sample": saw[:200]})

    with open(args.out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["file","detected_source","corner_text_sample"])
        w.writeheader()
        for r in rows:
            w.writerow(r)

    print("=== Detected sources (top-left OCR) ===")
    for k in sorted(counts):
        print(f"  {k:12} {counts[k]}")
    print(f"\nWrote: {args.out}")

if __name__ == "__main__":
    main()
