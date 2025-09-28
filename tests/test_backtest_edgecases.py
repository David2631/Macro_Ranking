import pandas as pd
import pytest


def test_score_to_weights_infeasible_min_alloc():
    from src.portfolio.allocations import score_to_weights

    s = pd.Series({"A": 1.0, "B": 2.0, "C": 3.0})
    # min_alloc too large
    with pytest.raises(ValueError):
        score_to_weights(s, min_alloc=0.5)  # 3 assets * 0.5 > 1.0


def test_score_to_weights_zero_scores_fallback_equal():
    from src.portfolio.allocations import score_to_weights

    s = pd.Series({"A": 0.0, "B": 0.0, "C": 0.0})
    w = score_to_weights(s)
    assert pytest.approx(w.sum(), rel=1e-9) == 1.0
    assert all(w > 0)


def test_compute_rebalanced_weights_fallback_equal_when_empty_signal():
    from src.backtest.simple import compute_rebalanced_weights

    signals = pd.DataFrame(columns=["A", "B"])  # empty
    res = compute_rebalanced_weights(signals)
    # should be an empty dict
    assert isinstance(res, dict)
    assert len(res) == 0
