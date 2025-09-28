"""Run pipeline in batches to handle large country lists reliably.

Splits the provided country list into batches (default 10), runs the pipeline for each
batch via `src.main.main`, collects per-run ranking sheets, merges them into a single
combined ranking (one row per country — keeping the latest score), and writes a final
Excel file and manifest.
"""
import os
import sys
from datetime import datetime
ROOT = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0, ROOT)

import time
import pandas as pd
import pycountry
from src.main import main
from src.io.excel import export_to_excel
from src.io.artifacts import write_manifest

# Pick first 70 countries
countries = [c.alpha_3 for c in list(pycountry.countries)[:70]]
BATCH_SIZE = 10
OUT_DIR = os.path.join(ROOT, "output")
COMBINED_PATH = os.path.join(OUT_DIR, "macro_ranking_70.xlsx")

os.makedirs(OUT_DIR, exist_ok=True)

def chunk(it, size):
    for i in range(0, len(it), size):
        yield it[i : i + size]

collected = []
for i, batch in enumerate(chunk(countries, BATCH_SIZE), start=1):
    cs = ','.join(batch)
    print(f"Running batch {i} with {len(batch)} countries...")
    try:
        main(["--countries", cs])
    except Exception as e:
        print(f"Batch {i} failed: {e}")
        continue
    # find the newest excel in output folder
    files = [os.path.join(OUT_DIR, f) for f in os.listdir(OUT_DIR) if f.endswith('.xlsx')]
    if not files:
        print("No excel produced for batch", i)
        continue
    latest = max(files, key=os.path.getmtime)
    print(" -> produced", latest)
    try:
        df = pd.read_excel(latest, sheet_name=0)
        collected.append(df)
    except Exception as e:
        print("Failed to read produced excel:", e)
    # small delay to be courteous to API
    time.sleep(1)

if not collected:
    print("No data collected from batches.")
    sys.exit(1)

combined = pd.concat(collected, ignore_index=True)
# keep latest score per country: assume later batches should overwrite earlier
combined = combined.sort_values('score', ascending=False).drop_duplicates(subset=['country'], keep='first')
combined = combined.reset_index(drop=True)

# write final excel
export_to_excel(COMBINED_PATH, combined, pd.DataFrame(), pd.DataFrame(), {"excel": {"path": COMBINED_PATH}})
print('Wrote combined Excel to', COMBINED_PATH)

# write manifest
manifest = {
    'timestamp': datetime.utcnow().isoformat(),
    'fetch_summary': {'batches': len(collected)},
    'fetches': [],
    'n_rows': int(combined.shape[0]),
    'config_snapshot': {'countries': countries}
}
write_manifest(manifest, outputs={'excel': COMBINED_PATH})
print('Wrote manifest to data/_artifacts (see latest)')
