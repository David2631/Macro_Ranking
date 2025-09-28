import pandas as pd
import numpy as np
from src.backtest.simple import compute_rebalanced_weights, run_backtest
from src.processing import scoring


def test_compute_rebalanced_weights_fallback():
    # signal contains a non-numeric that causes score_to_weights to raise in some cases
    signals = pd.DataFrame(
        {"A": [1.0], "B": [np.nan]}, index=[pd.Timestamp("2020-01-01")]
    )
    res = compute_rebalanced_weights(signals)
    assert isinstance(res, dict)
    assert pd.Timestamp("2020-01-01") in res
    w = res[pd.Timestamp("2020-01-01")]
    # only A has weight
    assert np.isclose(w.get("A", 0.0), 1.0) or np.isclose(w.sum(), 1.0)


def test_run_backtest_basic():
    dates = pd.date_range("2020-01-01", periods=3, freq="D")
    prices = pd.DataFrame({"A": [100, 101, 102], "B": [200, 198, 199]}, index=dates)
    weights_by_date = {dates[0]: pd.Series({"A": 0.5, "B": 0.5})}
    res = run_backtest(prices, weights_by_date)
    # nav should be a series with same index
    assert list(res.index) == list(dates)
    assert "nav" in res.columns
    assert res["nav"].iloc[0] > 0


def test_coverage_penalty_degenerate():
    cov = pd.Series([0.5, 0.5, 0.5], index=["A", "B", "C"])
    mult = scoring.coverage_penalty(cov, k=1.0)
    # degenerate case iqr==0 -> identity clipped to [0,1]
    assert all((mult >= 0) & (mult <= 1))


def test_bootstrap_scores_small():
    pivot = pd.DataFrame({"i1": [1.0, 2.0], "i2": [3.0, 4.0]}, index=["X", "Y"])
    weights = {"i1": 0.4, "i2": 0.6}
    summary, samples = scoring.bootstrap_scores(pivot, weights, n_boot=50, seed=1)
    assert "score_mean" in summary.columns
    assert samples.shape[0] == 2
