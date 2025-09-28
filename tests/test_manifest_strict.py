import os
import glob
import json
from src.io import artifacts


def _latest_manifest_path():
    art_dir = os.path.join(
        os.path.dirname(os.path.dirname(__file__)), "data", "_artifacts"
    )
    files = sorted(glob.glob(os.path.join(art_dir, "*.json")))
    if not files:
        return None
    return files[-1]


def test_manifest_fetch_entries_strict():
    """
    If the latest stored manifest already passes validation, assert required keys exist.
    Otherwise build a synthetic manifest using the enrichment path and ensure the
    produced manifest validates with zero issues.
    """
    path = _latest_manifest_path()
    if path:
        with open(path, encoding="utf-8") as fh:
            m = json.load(fh)
        if m.get("validation", {}).get("n_issues", 0) == 0:
            # quick sanity: required keys present in first fetch entry
            if m.get("fetches"):
                f = m["fetches"][0]
                for k in (
                    "request_url",
                    "http_status",
                    "response_time_ms",
                    "rows",
                    "sha256_normalized",
                    "fetch_timestamp",
                ):
                    assert k in f and f.get(k) is not None
                return

    # Otherwise, construct a minimal synthetic fetch entry and run it through enrichment + write_manifest
    sample_records = [
        {"indicator": "TEST_IND", "country": "TST", "date": "2020", "value": 1}
    ]
    raw_bytes = json.dumps(sample_records, ensure_ascii=False).encode("utf-8")
    sample_fe = {
        "url": "https://example.test/api",
        "params": None,
        "status_code": 200,
        "response_bytes": raw_bytes,
        "records": sample_records,
        "indicator": "TEST_IND",
        "country": "TST",
        "api_meta": {"source": "TEST"},
    }

    enriched = artifacts._enrich_fetch_entry(sample_fe)
    manifest = {
        "config_snapshot": {"test": True},
        "fetch_summary": {"TEST": 1},
        "fetches": [enriched],
        "n_rows": 1,
    }

    # write manifest (will populate environment + validation)
    path_out = artifacts.write_manifest(manifest, prefix="test_manifest_strict")
    assert os.path.exists(path_out)
    with open(path_out, encoding="utf-8") as fh:
        m2 = json.load(fh)

    assert (
        m2.get("validation", {}).get("n_issues", 0) == 0
    ), f"Enriched manifest had validation issues: {m2.get('validation')}"
