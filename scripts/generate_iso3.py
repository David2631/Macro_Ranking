import pycountry
import csv
import os

os.makedirs("data", exist_ok=True)
out_path = os.path.join("data", "countries_iso3.csv")
with open(out_path, "w", encoding="utf-8", newline="") as f:
    w = csv.writer(f)
    w.writerow(["alpha_3", "name"])
    seen = set()
    for c in sorted(pycountry.countries, key=lambda x: getattr(x, "alpha_3", "")):
        code = getattr(c, "alpha_3", None)
        name = getattr(c, "name", None)
        if not code:
            continue
        if code in seen:
            continue
        seen.add(code)
        w.writerow([code, name])

print(f"Wrote {len(seen)} codes to {out_path}")
