import json
import os
import pandas as pd

ROOT = os.path.dirname(os.path.dirname(__file__))
OUT_DIR = os.path.join(ROOT, "output")
ART_DIR = os.path.join(ROOT, "data", "_artifacts")
CSV = os.path.join(OUT_DIR, "macro_ranking_170.csv")
XLSX = os.path.join(OUT_DIR, "macro_ranking_170.xlsx")

print("Looking for output files...")
for p in (CSV, XLSX):
    print("  ", p, "->", "EXISTS" if os.path.exists(p) else "MISSING")

if os.path.exists(CSV):
    df = pd.read_csv(CSV)
    print("\nCSV shape:", df.shape)
    if "country" in df.columns:
        print("Unique countries:", df["country"].nunique())
        print("Duplicate country rows:", df.duplicated(subset=["country"]).sum())
    else:
        print("Note: no `country` column in CSV")
    print("\nTop 10 rows by score (if present):")
    if "score" in df.columns:
        print(df.sort_values("score", ascending=False).head(10).to_string(index=False))
    else:
        print(df.head(10).to_string(index=False))

if os.path.exists(XLSX):
    df2 = pd.read_excel(XLSX, sheet_name=0)
    print("\nXLSX shape:", df2.shape)

# list manifests
print("\nArtifacts directory:", ART_DIR)
if os.path.isdir(ART_DIR):
    mfiles = sorted(
        [os.path.join(ART_DIR, f) for f in os.listdir(ART_DIR) if f.endswith(".json")]
    )
    print("Found manifests:", len(mfiles))
    if mfiles:
        latest = mfiles[-1]
        print("Latest manifest:", latest)
        try:
            with open(latest, "r", encoding="utf-8") as fh:
                mf = json.load(fh)
            keys = list(mf.keys())
            print("Manifest keys:", keys)
            print(
                "Manifest snapshot: n_rows=",
                mf.get("n_rows"),
                "outputs=",
                mf.get("outputs"),
            )
        except Exception as e:
            print("Failed reading manifest:", e)
else:
    print("No artifacts directory found")
