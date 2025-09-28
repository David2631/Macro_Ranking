import json
from pathlib import Path
import runpy


def test_fixture_run_produces_outputs(tmp_path):
    # Run the fixture-mode runner by executing the script file directly
    out_dir = Path("output")
    if out_dir.exists():
        for f in out_dir.iterdir():
            try:
                f.unlink()
            except Exception:
                pass

    # Execute the runner script; it exposes a `main()` in its globals
    module_globals = runpy.run_path(str(Path("scripts") / "ci_fixture_run.py"))
    fixture_main = module_globals.get("main")
    assert fixture_main is not None, "ci_fixture_run.py should expose a main() function"
    # Provide an explicit config path so argparse inside src.main doesn't pick up pytest argv
    fixture_main("example-configs/example-config-s1.yaml")

    # Assert expected files created
    assert out_dir.exists(), "output directory should exist"
    xs = list(out_dir.glob("*.xlsx"))
    assert len(xs) >= 1, "At least one Excel output should be created by fixture run"
    alloc = out_dir / "allocations.csv"
    assert alloc.exists(), "allocations.csv should be created"

    # manifest exists
    m = Path("data/_artifacts")
    assert m.exists(), "artifact manifest dir should exist"
    man = list(m.glob("manifest_*.json"))
    assert len(man) >= 1, "manifest file should be written"

    # Basic manifest schema check: accept either detailed fetch logs or a fetch_summary + n_rows
    js = json.loads(man[0].read_text())
    if "fetches" in js and isinstance(js["fetches"], list) and len(js["fetches"]) > 0:
        fe = js["fetches"][0]
        assert "sha256_normalized" in fe
    else:
        # fallback: ensure fetch_summary exists and rows were written
        assert "fetch_summary" in js
        assert js.get("n_rows", 0) > 0
