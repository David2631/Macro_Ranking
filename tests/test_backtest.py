import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime

# ensure project root is on sys.path so `src` package can be imported in tests
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)


def make_prices(dates, symbols):
    # simple random walk prices seeded for determinism
    rng = np.random.RandomState(0)
    data = {}
    for s in symbols:
        steps = rng.normal(loc=0.0005, scale=0.01, size=len(dates))
        prices = 100 * (1 + steps).cumprod()
        data[s] = prices
    return pd.DataFrame(data, index=dates)


def test_backtest_basic_run():
    from src.backtest.simple import compute_rebalanced_weights, run_backtest

    # dates: 20 business days
    start = datetime(2020, 1, 1)
    dates = pd.bdate_range(start, periods=20)
    symbols = ["AAA", "BBB", "CCC"]
    prices = make_prices(dates, symbols)

    # synthetic signals: increase score for AAA over time
    signals = pd.DataFrame(index=dates, columns=symbols)
    for i, d in enumerate(dates):
        signals.loc[d] = [1.0 + 0.01 * i, 0.5, 0.2]

    weights_by_date = compute_rebalanced_weights(signals, top_n=2, min_alloc=0.0, max_alloc=0.8)
    # ensure we have weights for each date
    assert len(weights_by_date) == len(dates)

    res = run_backtest(prices, weights_by_date)
    # nav should be present and increasing length
    assert "nav" in res.columns
    assert len(res) == len(dates)
    # turnover should be present (NaN for non-rebalance days is acceptable)
    assert "turnover" in res.columns
    # nav should be finite
    assert res["nav"].dropna().apply(lambda x: np.isfinite(x)).all()
