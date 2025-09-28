"""Small helper used in CI to run the pipeline against local SDMX fixtures when available.

Behavior:
- If `tests/fixtures/sdmx/wb/` exists, monkeypatch `WorldBankFetcher.fetch` to load JSON files named `{indicator}_{country}.json` and return (DataFrame, [fetch_log])
- Otherwise, it falls back to calling `src.main.main` with the provided config (or default example-config.yaml)

This script is intentionally conservative: it doesn't change library code and only monkeypatches behavior at runtime.
"""
import os
import sys
import json
import pandas as pd
import logging
from datetime import datetime

from importlib import import_module

# ensure repo root is importable as top-level package when run as a script
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

FIX_DIR = os.path.join(os.getcwd(), "tests", "fixtures", "sdmx", "wb")


def load_fixture(indicator, country):
    fname = f"{indicator}_{country}.json"
    path = os.path.join(FIX_DIR, fname)
    if not os.path.exists(path):
        return None
    with open(path, encoding="utf-8") as fh:
        j = json.load(fh)
    # Expect fixture JSON to be a list of records with date and value
    try:
        df = pd.DataFrame(j)
    except Exception:
        df = pd.DataFrame(j.get("data", []))
    # simple fetch log with richer provenance fields
    log = {
        "request_url": f"file://{path}",
        "params": {"fixture": True},
        "http_status": 200,
        "response_time_ms": 0,
        "rows": len(df),
        "sha256_normalized": None,
        "sha256_raw": None,
        "fetch_timestamp": datetime.now().isoformat(),
        "indicator": indicator,
        "country": country,
        "no_backfill": False,
    }
    return df, [log]


def main(config_path: str = None):
    # If fixtures not present, run main normally
    if not os.path.isdir(FIX_DIR):
        print("No SDMX fixtures found; running normal pipeline")
        from src.main import main as pipeline_main

        pipeline_main(["--config", config_path] if config_path else None)
        return

    # Monkeypatch WorldBankFetcher.fetch
    try:
        mod = import_module("src.fetchers.worldbank")
        if hasattr(mod, "WorldBankFetcher"):
            orig_cls = mod.WorldBankFetcher

            class FixtureWB(orig_cls):
                def fetch(self, countries, indicators, start, end, freq):
                    # indicators is list of dicts or similar where .get('id') might be indicator
                    all_df = []
                    logs = []
                    for ind in indicators:
                        ind_id = ind.get("id") if isinstance(ind, dict) else getattr(ind, "id", None)
                        code = ind.get("code") if isinstance(ind, dict) else getattr(ind, "code", None)
                        for c in countries:
                            got = load_fixture(code or ind_id, c)
                            if got is None:
                                continue
                            df, lg = got
                            if df is None or df.empty:
                                continue
                            # ensure columns match expected shape
                            df = df.copy()
                            if "date" in df.columns:
                                df["date"] = pd.to_datetime(df["date"])
                            df["country"] = c
                            df["indicator"] = ind_id
                            all_df.append(df)
                            logs.extend(lg)
                    if not all_df:
                        return pd.DataFrame(columns=["source", "indicator", "country", "date", "value"]), logs
                    return pd.concat(all_df, ignore_index=True), logs

            mod.WorldBankFetcher = FixtureWB
            print("Patched WorldBankFetcher.fetch to use local SDMX fixtures")
    except Exception as e:
        logging.warning(f"Could not patch fetcher for fixtures: {e}")

    # run pipeline with provided config or default example-config.yaml
    from src.main import main as pipeline_main

    cfg_arg = ["--config", config_path] if config_path else None
    pipeline_main(cfg_arg)


if __name__ == "__main__":
    import sys

    cfg = sys.argv[1] if len(sys.argv) > 1 else None
    main(cfg)
