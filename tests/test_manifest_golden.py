import json
import os
from src.io.artifacts import write_manifest


def test_manifest_golden(tmp_path):
    # prepare a small manifest with one fetch entry (with records)
    records = [
        {"indicator": "GDP", "country": "DEU", "date": "2020-01-01", "value": 1.2},
        {"indicator": "GDP", "country": "DEU", "date": "2020-04-01", "value": 1.3},
    ]

    fetch = {
        "request_url": "https://example.org/api/gdp",
        "params": {"country": "DEU"},
        "http_status": 200,
        "response_time_ms": 120,
        "records": records,
        "fetch_timestamp": "2025-09-28T00:00:00Z",
        "as_of": "2020-04-01",
    }

    manifest = {
        "config_snapshot": {"countries": ["DEU"]},
        "fetch_summary": {"batches": 1},
        "fetches": [fetch],
        "n_rows": 2,
    }

    out = write_manifest(manifest, prefix="test_manifest", outputs={})
    assert os.path.exists(out)

    with open(out, "r", encoding="utf-8") as fh:
        m = json.load(fh)

    # env hash must be present and stable-ish
    env = m.get("environment")
    assert env is not None
    assert "env_hash" in env

    # fetch entry should have sha256_normalized computed
    fetches = m.get("fetches") or []
    assert len(fetches) == 1
    fe = fetches[0]
    assert fe.get("sha256_normalized") is not None
    assert fe.get("fetch_timestamp") == "2025-09-28T00:00:00Z"


import sys
import hashlib
import hmac
from pathlib import Path

# ensure local src is importable
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)


def canonical_payload(payload: dict) -> bytes:
    return json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")


def test_manifest_golden_signature(tmp_path, monkeypatch):
    # Load fixture records
    fixture = Path(__file__).parent / "fixtures" / "manifest_golden_records.json"
    fixture_path = str(fixture)
    records = json.loads(open(fixture_path, encoding="utf-8").read())

    fetch_entry = {
        "url": "https://api.example.org/gdp",
        "status_code": 200,
        "response_time_ms": 42,
        "records": records,
        "indicator": "gdp",
        "country": "USA",
        "fetch_timestamp": "2025-09-27T12:34:56Z",
    }

    manifest = {
        "fetch_summary": {"WB": len(records)},
        "fetches": [fetch_entry],
        "n_rows": len(records),
        "config_snapshot": {"scenario": "S1"},
    }

    # deterministic env
    monkeypatch.setenv("MANIFEST_SIGNING_KEY", "golden-test-key-xyz")
    monkeypatch.setenv("RUN_ID", "golden-run-0001")

    mpath = write_manifest(manifest, outputs={})
    m = json.loads(open(mpath, encoding="utf-8").read())

    # Build stable payload as write_manifest does (config_snapshot, fetch_summary, fetches, n_rows, outputs)
    stable_keys = ["config_snapshot", "fetch_summary", "fetches", "n_rows", "outputs"]
    stable_payload = {k: m.get(k) for k in stable_keys if k in m}

    # compute expected signature using same key and canonical payload
    key = os.environ.get("MANIFEST_SIGNING_KEY").encode("utf-8")
    canonical = canonical_payload(stable_payload)
    expected_sig = hmac.new(key, canonical, hashlib.sha256).hexdigest()

    assert m.get("manifest_signature") == expected_sig


import sys

ROOT2 = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT2))


def test_manifest_hmac_stable(tmp_path, monkeypatch):
    # prepare a deterministic manifest payload
    manifest = {
        "config_snapshot": {"a": 1, "b": 2},
        "fetch_summary": {"WB": 3},
        "fetches": [
            {
                "url": "https://example",
                "rows": 10,
                "sha256_raw": "aaa",
                "sha256_normalized": "bbb",
            }
        ],
        "n_rows": 10,
    }
    outputs = {"excel": "./output/test.xlsx"}
    key = "test-signing-key"
    monkeypatch.setenv("MANIFEST_SIGNING_KEY", key)
    # call write_manifest which will write file and use HMAC
    mpath = write_manifest(manifest, outputs=outputs)
    # read back file and check manifest_signature is present and equals run_id
    with open(mpath, "r", encoding="utf-8") as f:
        doc = json.load(f)
    assert "manifest_signature" in doc
    assert doc["manifest_signature"] == doc["run_id"]
    # sanity: signature is hex string length 64
    assert (
        isinstance(doc["manifest_signature"], str)
        and len(doc["manifest_signature"]) == 64
    )
