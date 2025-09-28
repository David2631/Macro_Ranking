import sys
from pathlib import Path

# ensure project root is on sys.path so `src` package is importable when running pytest
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.io.artifacts import sha256_of_records, sha256_of_bytes  # noqa: E402


def test_sha256_of_records_is_order_independent():
    recs1 = [
        {"indicator": "gdp", "country": "DEU", "date": "2020", "value": 1.0},
        {"indicator": "gdp", "country": "FRA", "date": "2020", "value": 0.5},
    ]
    recs2 = list(reversed(recs1))
    h1 = sha256_of_records(recs1)
    h2 = sha256_of_records(recs2)
    assert h1 == h2
    # different content -> different hash
    recs3 = [{"indicator": "gdp", "country": "DEU", "date": "2020", "value": 1.0001}]
    h3 = sha256_of_records(recs3)
    assert h3 != h1


def test_sha256_of_records_vs_raw_bytes():
    recs = [
        {"indicator": "gdp", "country": "DEU", "date": "2020", "value": 1.0},
    ]
    canonical = sha256_of_records(recs)
    raw = str(recs).encode("utf-8")
    raw_hash = sha256_of_bytes(raw)
    assert canonical != raw_hash
