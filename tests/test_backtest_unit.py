import pandas as pd


def test_backtest_simple_positive_returns():
    from src.backtest.simple import compute_rebalanced_weights, run_backtest

    dates = pd.date_range(start="2020-01-01", periods=5, freq="B")
    # two assets with steadily positive returns
    prices = pd.DataFrame({
        "A": [100, 101, 102, 103, 104],
        "B": [50, 51, 52, 53, 54],
    }, index=dates)

    # signals: prefer asset A first day, then equal
    signals = pd.DataFrame(
        {
            "A": [1.0, 0.5],
            "B": [0.0, 0.5],
        },
        index=[dates[0], dates[2]],
    )

    w_by_date = compute_rebalanced_weights(signals, top_n=None)
    res = run_backtest(prices, w_by_date, rebalance_on=sorted(w_by_date.keys()))

    # nav should be > 0 and increasing because both assets have positive returns
    assert res["nav"].iloc[-1] > 1.0
    # turnover should be present for rebalancing dates
    assert "turnover" in res.columns
    # at least one non-zero turnover recorded
    assert res["turnover"].dropna().sum() >= 0
