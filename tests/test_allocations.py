import pandas as pd
import sys
from pathlib import Path

# ensure project root is on sys.path so `src` package is importable when running pytest
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from src.portfolio.allocations import score_to_weights  # noqa: E402


def test_score_to_weights_basic():
    s = pd.Series({"USA": 1.0, "DEU": 0.5, "FRA": 0.25})
    w = score_to_weights(s)
    assert abs(w.sum() - 1.0) < 1e-8
    assert all(w >= 0)
    # top_n
    w2 = score_to_weights(s, top_n=2)
    assert set(w2.index) == set(["USA", "DEU"])
    assert abs(w2.sum() - 1.0) < 1e-8


def test_min_alloc_flooring_and_max_cap():
    s = pd.Series({"A": 0.7, "B": 0.2, "C": 0.1})
    # force minimum allocation floor
    w = score_to_weights(s, min_alloc=0.15)
    assert all(w >= 0.15 - 1e-8)
    assert abs(w.sum() - 1.0) < 1e-8

    # force maximum cap
    w2 = score_to_weights(s, max_alloc=0.5)
    assert all(w2 <= 0.5 + 1e-8)
    assert abs(w2.sum() - 1.0) < 1e-8


def test_top_n_and_degenerate_scores():
    s = pd.Series({"X": 0.0, "Y": 0.0, "Z": 0.0})
    # all-zero scores -> equal weights among top_n or among all if top_n None
    w = score_to_weights(s)
    assert abs(w.sum() - 1.0) < 1e-8
    assert len(w) == 3

    w_top2 = score_to_weights(s, top_n=2)
    assert len(w_top2) == 2
    assert abs(w_top2.sum() - 1.0) < 1e-8

    # negative scores: should be handled (shifted) and still sum to 1
    s2 = pd.Series({"A": -1.0, "B": -0.5, "C": 0.0})
    w3 = score_to_weights(s2)
    assert abs(w3.sum() - 1.0) < 1e-8
    assert all(w3 >= 0)
