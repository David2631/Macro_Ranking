"""Batch-run the macro-ranking pipeline and combine results into a single table.

This script splits the first N ISO3 countries into batches, runs the pipeline for
each batch via `src.main.main`, collects the produced per-batch Excel ranking sheet
and concatenates them into a single flat table (one row per country, keeping the
highest score). It writes a final combined Excel and CSV under `output/` and a
deterministic manifest via `src.io.artifacts.write_manifest`.

Usage:
  python scripts/run_170.py           # dry-run report (no work)
  python scripts/run_170.py --run     # actually execute batches
  python scripts/run_170.py --run --batch-size 20 --n 170
"""

import os
import sys
from datetime import datetime
import time
import argparse
import pandas as pd
import pycountry

ROOT = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0, ROOT)

from src.main import main
from src.io.artifacts import write_manifest


def chunk(it, size):
    for i in range(0, len(it), size):
        yield it[i : i + size]


def list_countries(n=170):
    # Pick first n countries from pycountry (may include territories)
    return [c.alpha_3 for c in list(pycountry.countries)[:n]]


def run_batches(countries, batch_size=20, run=False, cleanup=True, out_dir=None):
    out_dir = out_dir or os.path.join(ROOT, "output")
    os.makedirs(out_dir, exist_ok=True)

    combined_xlsx = os.path.join(out_dir, f"macro_ranking_{len(countries)}.xlsx")
    combined_csv = os.path.join(out_dir, f"macro_ranking_{len(countries)}.csv")

    existing_files = set([f for f in os.listdir(out_dir) if f.endswith(".xlsx")])
    collected = []

    for i, batch in enumerate(chunk(countries, batch_size), start=1):
        cs = ",".join(batch)
        print(
            f"Batch {i}/{(len(countries)+batch_size-1)//batch_size}: {len(batch)} countries"
        )
        if not run:
            print("  Dry-run: would call main with countries:", cs)
            continue

        try:
            main(["--countries", cs])
        except Exception as e:
            print(f"  Batch {i} failed: {e}")
            continue

        # detect any new xlsx files produced by the run
        files = [f for f in os.listdir(out_dir) if f.endswith(".xlsx")]
        new_files = [os.path.join(out_dir, f) for f in files if f not in existing_files]
        if not new_files:
            print("  Warning: no new .xlsx produced for this batch")
            continue
        latest = max(new_files, key=os.path.getmtime)
        print("  -> produced", latest)

        try:
            df = pd.read_excel(latest, sheet_name=0)
            collected.append(df)
        except Exception as e:
            print("  Failed to read produced excel:", e)

        if cleanup:
            for nf in new_files:
                try:
                    os.remove(nf)
                except Exception:
                    pass

        # polite pause
        time.sleep(1)

    if not run:
        print("Dry-run complete. Rerun with --run to execute batches.")
        return

    if not collected:
        print("No data collected from batches. Exiting with error.")
        sys.exit(1)

    combined = pd.concat(collected, ignore_index=True)
    # keep highest score per country if available
    if "score" in combined.columns and "country" in combined.columns:
        combined = combined.sort_values("score", ascending=False).drop_duplicates(
            subset=["country"], keep="first"
        )
    combined = combined.reset_index(drop=True)

    try:
        combined.to_excel(combined_xlsx, index=False)
        combined.to_csv(combined_csv, index=False)
        print("Wrote combined Excel to", combined_xlsx)
        print("Wrote combined CSV to", combined_csv)
    except Exception as e:
        print("Failed to write combined outputs:", e)

    manifest = {
        "timestamp": datetime.utcnow().isoformat(),
        "fetch_summary": {"batches": len(collected)},
        "fetches": [],
        "n_rows": int(combined.shape[0]),
        "config_snapshot": {"countries": countries},
    }
    try:
        write_manifest(manifest, outputs={"excel": combined_xlsx, "csv": combined_csv})
        print("Wrote manifest to data/_artifacts (see latest)")
    except Exception as e:
        print("Failed to write manifest:", e)


def parse_args_and_run():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--run", action="store_true", help="Execute batches (default is dry-run)"
    )
    parser.add_argument(
        "--batch-size", type=int, default=20, help="Number of countries per batch"
    )
    parser.add_argument(
        "--no-cleanup", action="store_true", help="Do not delete per-batch output files"
    )
    parser.add_argument(
        "--n",
        type=int,
        default=170,
        help="Number of countries to include (first N from pycountry)",
    )
    args = parser.parse_args()

    countries = list_countries(n=args.n)
    run_batches(
        countries, batch_size=args.batch_size, run=args.run, cleanup=not args.no_cleanup
    )


if __name__ == "__main__":
    parse_args_and_run()
