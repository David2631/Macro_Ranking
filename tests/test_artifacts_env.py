import sys
import json
import hashlib
import hmac
from pathlib import Path

# ensure project root is importable when tests run in isolation
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.io.artifacts import write_manifest, sha256_of_file


def test_manifest_signing_and_output_hash(tmp_path, monkeypatch):
    # create a dummy output file
    out_file = tmp_path / "out.csv"
    out_file.write_text("a,b,c\n1,2,3\n")

    # set a deterministic signing key
    monkeypatch.setenv("MANIFEST_SIGNING_KEY", "test-signing-key")

    manifest = {
        "config_snapshot": {"a": 1},
        "fetch_summary": {},
        "fetches": [],
        "n_rows": 0,
    }

    path = write_manifest(
        manifest, prefix="test_manifest", outputs={"csv": str(out_file)}
    )
    assert Path(path).exists()

    m = json.loads(Path(path).read_text(encoding="utf-8"))
    # signature must be present
    assert "manifest_signature" in m
    sig = m["manifest_signature"]

    # recompute expected signature from stable payload
    stable_keys = ["config_snapshot", "fetch_summary", "fetches", "n_rows", "outputs"]
    stable_payload = {k: m.get(k) for k in stable_keys if k in m}
    canonical = json.dumps(
        stable_payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")
    expected = hmac.new(b"test-signing-key", canonical, hashlib.sha256).hexdigest()
    assert sig == expected

    # outputs hash was included and matches file
    assert "outputs" in m
    out_entry = m["outputs"].get("csv")
    assert out_entry and out_entry["sha256"] == sha256_of_file(str(out_file))
