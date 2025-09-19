import cv2, pytesseract, os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

# Folders
imgdir = "images"
outcsv = "audit/logo_detect_results.csv"
os.makedirs("audit", exist_ok=True)

# Keywords to detect (case-insensitive)
FAMILIES = {
    "CIRCA_FAM": ["circa"],
    "BRACCO_FAM": ["bracco"],
    "FANDUEL_FAM": ["sportsbook", "fanduel"],  # logo says "SPORTSBOOK"
}

# Only last 72h
cutoff = datetime.now(ZoneInfo("America/Chicago")) - timedelta(hours=72)

rows = []
for fn in sorted(os.listdir(imgdir)):
    if not fn.lower().endswith((".jpg",".jpeg",".png")): 
        continue
    path = os.path.join(imgdir, fn)
    ts = datetime.fromtimestamp(os.path.getmtime(path), ZoneInfo("America/Chicago"))
    if ts < cutoff: 
        continue

    # Crop top 100px for logo area
    try:
        img = cv2.imread(path)
        if img is None: continue
        crop = img[0:100, 0:300]   # top-left
        text = pytesseract.image_to_string(crop).lower()
    except Exception as e:
        text = ""

    fam = "UNKNOWN"
    for k,keys in FAMILIES.items():
        if any(x in text for x in keys):
            fam = k
            break

    rows.append((fn, ts.isoformat(), fam))

# Write results
with open(outcsv,"w",encoding="utf-8") as f:
    f.write("file,timestamp,family\n")
    for r in rows:
        f.write(",".join(r)+"\n")

print(f"Wrote {len(rows)} rows -> {outcsv}")
