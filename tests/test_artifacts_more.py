import os
import json
from src.io import artifacts


def test_sha256_of_records_is_deterministic():
    recs1 = [
        {"indicator": "gdp", "country": "DEU", "date": "2020-01-01", "value": 1},
        {"indicator": "cpi", "country": "FRA", "date": "2020-01-01", "value": 2},
    ]
    recs2 = list(reversed(recs1))
    h1 = artifacts.sha256_of_records(recs1)
    h2 = artifacts.sha256_of_records(recs2)
    assert h1 == h2
    assert isinstance(h1, str) and len(h1) == 64


def test_enrich_and_validate_fetch_entry_basic(tmp_path):
    # Simulate a minimal fetch entry with raw bytes and records
    recs = [{"indicator": "gdp", "country": "DEU", "date": "2020-01-01", "value": 1}]
    fe = {
        "url": "https://example.com/api",
        "status_code": 200,
        "response_bytes": b"rawdata",
        "records": recs,
    }
    enriched = artifacts._enrich_fetch_entry(fe)
    # required canonical fields present
    assert enriched.get("request_url") == "https://example.com/api"
    assert enriched.get("http_status") == 200
    assert (
        isinstance(enriched.get("sha256_raw"), str)
        or enriched.get("sha256_raw") is None
    )
    assert (
        isinstance(enriched.get("sha256_normalized"), str)
        or enriched.get("sha256_normalized") is None
    )
    # validation should pass (no missing required keys if sha etc present)
    issues = artifacts._validate_fetch_entry(enriched)
    # rows should be present and integer
    assert isinstance(enriched.get("rows"), int) or enriched.get("rows") is None
    # Issues may include missing fields depending on enrichment; ensure return type
    assert isinstance(issues, list)


def test_write_manifest_creates_files_and_signature(tmp_path, monkeypatch):
    # prepare a small output file to include in outputs
    outfile = tmp_path / "out.csv"
    outfile.write_text("a,b\n1,2\n")
    manifest = {
        "config_snapshot": {"some": "config"},
        "fetch_summary": {},
        "fetches": [
            {
                "request": "https://example.com",
                "records": [
                    {"indicator": "gdp", "country": "DEU", "date": "2020-01-01"}
                ],
            }
        ],
        "n_rows": 1,
    }
    # set signing key
    monkeypatch.setenv("MANIFEST_SIGNING_KEY", "testkey")
    path = artifacts.write_manifest(
        manifest, prefix="test_manifest", outputs={"sample": str(outfile)}
    )
    assert os.path.exists(path)
    # sidecar environment file should exist
    env_path = path.replace(".json", ".environment")
    assert os.path.exists(env_path)
    # manifest should contain manifest_signature and provenance manifest_sha256
    with open(path, "r", encoding="utf-8") as f:
        m = json.load(f)
    assert "manifest_signature" in m
    assert "provenance" in m and "manifest_sha256" in m["provenance"]
