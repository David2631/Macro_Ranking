import pandas as pd

from src.portfolio.rebalance import compute_target_weights, apply_turnover_costs


def test_compute_target_weights_basic():
    scores = pd.Series({"USA": 1.0, "DEU": 0.5, "FRA": 0.2})
    w = compute_target_weights(scores, threshold=0.0, power=1.0)
    assert isinstance(w, pd.Series)
    assert w.sum() == 1.0
    assert "USA" in w.index


def test_apply_turnover_costs():
    old = pd.Series({"USA": 0.5, "DEU": 0.5})
    new = pd.Series({"USA": 0.6, "DEU": 0.4})
    c = apply_turnover_costs(old, new, cost_per_unit=0.1)
    # turnover = |0.1| + | -0.1| = 0.2 => cost = 0.2 * 0.1 = 0.02
    assert abs(c - 0.02) < 1e-12
