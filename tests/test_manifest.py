import os

# ruff: noqa
import json
import sys
from pathlib import Path

# ensure project root is on sys.path so `src` package is importable when running pytest
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.io.artifacts import (
    write_manifest,
    ensure_artifact_dir,
    sha256_of_file,
)  # noqa: E402


def test_write_manifest_creates_file(tmp_path):
    ensure_artifact_dir()
    # create a small dummy output file
    out = tmp_path / "dummy.txt"
    out.write_text("hello world", encoding="utf-8")
    manifest = {"test": True}
    path = write_manifest(manifest, prefix="test_manifest", outputs={"dummy": str(out)})
    assert os.path.exists(path)
    data = json.loads(open(path, "r", encoding="utf-8").read())
    assert "environment" in data
    assert "outputs" in data
    assert "dummy" in data["outputs"]
    # verify sha matches
    expected = sha256_of_file(str(out))
    assert data["outputs"]["dummy"]["sha256"] == expected
