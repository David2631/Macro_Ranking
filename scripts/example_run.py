"""Example runner: create synthetic data, run minimal pipeline, write Excel and manifest.

Produces outputs in ./output and manifest in data/_artifacts for quick inspection.
"""
import os
from datetime import datetime
import pandas as pd
import numpy as np

# ensure repo root
ROOT = os.path.dirname(os.path.dirname(__file__))
import sys
sys.path.insert(0, ROOT)

from src.io.artifacts import write_manifest
from src.io.excel import export_to_excel
from src.processing.scoring import compute_composite

# synthetic config-like dict
cfg = {
    "countries": ["USA", "DEU", "FRA", "JPN", "BRA"],
    "indicators": [
        # indicator id, good_direction is up by default
        type("I", (), {"id": "gdp_growth", "transform": "none", "good_direction": "up"}),
        type("I", (), {"id": "inflation", "transform": "none", "good_direction": "down"}),
        type("I", (), {"id": "unemployment", "transform": "none", "good_direction": "down"}),
    ],
    "scoring": {
        "weights": {"gdp_growth": 0.5, "inflation": 0.25, "unemployment": 0.25},
        "min_coverage_ratio": 0.0,
        "apply_coverage_penalty": True,
        "coverage_k": 1.0,
        "bootstrap": {"enabled": False},
        "smoothing": {"method": "none"},
        "standardization": {"method": "zscore"},
    },
    "excel": {"path": os.path.join(ROOT, "output", "example_ranking.xlsx")},
}

# synthesize latest standardized values per country per indicator
np.random.seed(0)
rows = []
for ind in cfg["indicators"]:
    for c in cfg["countries"]:
        val = np.random.randn() + (0.5 if ind.id == "gdp_growth" else 0.0)
        # inflation: higher is bad, unemployment higher is bad
        rows.append({"country": c, "indicator": ind.id, "date": datetime.utcnow().date().isoformat(), "value": float(val)})

raw_df = pd.DataFrame(rows)
# apply a simple "standardize" placeholder
indicators = []
for ind in cfg["indicators"]:
    df = raw_df[raw_df["indicator"] == ind.id].copy()
    df["value_std"] = (df["value"] - df["value"].mean()) / (df["value"].std(ddof=0) or 1.0)
    df.loc[df["indicator"] == ind.id, "indicator"] = ind.id
    indicators.append(df[["country", "indicator", "date", "value", "value_std"]])
indicators_df = pd.concat(indicators, ignore_index=True)

pivot = indicators_df.pivot(index="country", columns="indicator", values="value_std")

# compute composite with coverage penalty
scores = compute_composite(pivot, cfg["scoring"]["weights"], apply_coverage_penalty=True, coverage_k=1.0)

# rank
ranked = scores.sort_values(ascending=False).to_frame(name="score")
ranked["rank"] = ranked["score"].rank(ascending=False, method="min")
ranked["coverage_ratio"] = pivot.notna().sum(axis=1) / pivot.shape[1]

# Ensure output dirs
os.makedirs(os.path.join(ROOT, "output"), exist_ok=True)

# Write excel
export_to_excel(cfg["excel"]["path"], ranked, indicators_df, raw_df, cfg)

# Write manifest
manifest = {
    "timestamp": datetime.utcnow().isoformat(),
    "fetch_summary": {"synthetic": len(rows)},
    "fetches": [],
    "n_rows": len(rows),
    "config_snapshot": cfg,
}
write_manifest(manifest, outputs={"excel": cfg["excel"]["path"]})

print("Wrote example Excel and manifest. Check output/ and data/_artifacts/ for files.")
