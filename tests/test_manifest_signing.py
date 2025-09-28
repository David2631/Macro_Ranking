import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.io.artifacts import write_manifest  # noqa: E402


def test_manifest_signing_hmac(tmp_path, monkeypatch):
    # set a deterministic signing key
    monkeypatch.setenv("MANIFEST_SIGNING_KEY", "test-secret")
    manifest = {
        "config_snapshot": {"dummy": True},
        "fetch_summary": {"WB": 1},
        "fetches": [{"url": "https://example.org", "rows": 1}],
        "n_rows": 1,
    }
    path = write_manifest(manifest, prefix="test_signing", outputs=None)
    data = json.loads(open(path, "r", encoding="utf-8").read())
    # when signing key is present, run_id should equal manifest_signature
    assert "manifest_signature" in data
    assert data["run_id"] == data["manifest_signature"]
