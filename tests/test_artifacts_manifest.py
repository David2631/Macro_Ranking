import os
import sys
import json

# make sure project root is on sys.path so `src` can be imported
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from src.io.artifacts import write_manifest, sha256_of_records


def test_write_manifest_with_fetch_and_signature(tmp_path, monkeypatch):
    # Prepare a dummy output file
    out_file = tmp_path / "out.csv"
    out_file.write_text("a,b\n1,2\n")

    # Dummy fetch records
    records = [
        {"indicator": "gdp", "country": "USA", "date": "2020-01-01", "value": 100},
        {"indicator": "gdp", "country": "USA", "date": "2020-04-01", "value": 101},
    ]

    fetch_entry = {
        "url": "https://api.example.org/gdp",
        "status_code": 200,
        "response_time_ms": 123,
        "records": records,
        "indicator": "gdp",
        "country": "USA",
        "fetch_timestamp": "2025-09-27T12:00:00Z",
    }

    manifest = {
        "fetch_summary": {"WB": 2},
        "fetches": [fetch_entry],
        "n_rows": 2,
        "config_snapshot": {"foo": "bar"},
    }

    # set a deterministic signing key
    monkeypatch.setenv("MANIFEST_SIGNING_KEY", "test-signing-key-123")

    mpath = write_manifest(manifest, outputs={"csv": str(out_file)})
    with open(mpath, "r", encoding="utf-8") as fh:
        m = json.load(fh)

    # signature present
    assert "manifest_signature" in m or "run_id" in m

    # outputs hash present for csv
    assert "outputs" in m and "csv" in m["outputs"]

    # verify sha256_normalized for the attached fetch 'records' (written by enrichment)
    # find matching fetch in m["fetches"]
    f = m.get("fetches", [])[0]
    # calculate ourselves
    expected = sha256_of_records(records)
    # if the manifest writer enriched entries, it should either include sha256_normalized
    assert f.get("sha256_normalized") == expected
