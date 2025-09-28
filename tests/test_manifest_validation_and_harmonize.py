import os
import sys
import json
import pandas as pd

# ensure src importable
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from src.io.artifacts import write_manifest
from src.processing.harmonize import frequency_pipeline


def test_manifest_validation_includes_fetch_issues(tmp_path, monkeypatch):
    # create a fetch entry missing rows and status (to trigger validation issues)
    fetch_entry = {
        "url": "https://api.example.org/x",
        "records": [{"a": 1}],
        "indicator": "x",
        "country": "USA",
        # intentionally omit status_code and rows
    }
    manifest = {
        "fetch_summary": {"X": 1},
        "fetches": [fetch_entry],
        "n_rows": 1,
        "config_snapshot": {},
    }
    monkeypatch.delenv("MANIFEST_SIGNING_KEY", raising=False)
    mpath = write_manifest(manifest, outputs={})
    m = json.loads(open(mpath, encoding="utf-8").read())
    assert "validation" in m and "fetch_issues" in m["validation"]


def test_frequency_pipeline_monthly_to_quarter_mean():
    dates = pd.date_range("2020-01-31", periods=6, freq="ME")
    s = pd.Series([1, 2, 3, 4, 5, 6], index=dates)
    q = frequency_pipeline(s, "M", "Q", rule="mean")
    # Expect two quarters covered (Q1: Jan-Mar mean=2, Q2: Apr-Jun mean=5)
    assert len(q) >= 2
    assert abs(q.iloc[0] - 2.0) < 1e-6


def test_frequency_pipeline_annual_to_quarter_pad():
    dates = pd.to_datetime(["2020-12-31", "2021-12-31"])  # year end
    s = pd.Series([100, 200], index=dates)
    q = frequency_pipeline(s, "A", "Q")
    # After padding, Q4 should have the annual value
    # pandas may report 'QE-DEC' or similar; accept any quarter-end frequency
    freqstr = q.index.freqstr or str(q.index.freq)
    assert "Q" in freqstr.upper()
    # Q4 2020 should equal 100
    q2020_q4 = q[q.index.year == 2020].iloc[-1]
    assert abs(q2020_q4 - 100) < 1e-6
