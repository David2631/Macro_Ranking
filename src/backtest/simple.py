import pandas as pd
from typing import Dict, Iterable, Optional


def compute_rebalanced_weights(signals: pd.DataFrame, top_n: Optional[int] = None, min_alloc: float = 0.0, max_alloc: float = 1.0) -> Dict[pd.Timestamp, pd.Series]:
    """
    Given a DataFrame `signals` indexed by date with columns as assets and values as scores,
    compute target weights at each date by converting scores to weights via proportional allocation.
    Returns dict mapping timestamp -> pd.Series(weights).
    """
    out = {}
    from src.portfolio.allocations import score_to_weights

    for dt, row in signals.iterrows():
        try:
            weights = score_to_weights(row, min_alloc=min_alloc, max_alloc=max_alloc, top_n=top_n)
        except Exception:
            # fallback: equal weights among non-nulls
            vals = row.dropna()
            if vals.empty:
                w = pd.Series(dtype=float)
            else:
                n = len(vals)
                w = pd.Series(1.0 / n, index=vals.index)
            out[pd.Timestamp(dt)] = w
            continue
        out[pd.Timestamp(dt)] = weights
    return out


def run_backtest(prices: pd.DataFrame, weights_by_date: Dict[pd.Timestamp, pd.Series], rebalance_on: Optional[Iterable[pd.Timestamp]] = None) -> pd.DataFrame:
    """
    Simple backtest: compute daily returns from prices (forward returns between rebalances) and apply target weights.
    - `prices` is a DataFrame indexed by date with asset columns
    - `weights_by_date` maps rebalancing dates to weight Series
    Returns a DataFrame with portfolio value and turnover metrics.
    """
    prices = prices.sort_index()
    # compute simple returns
    returns = prices.pct_change().fillna(0)

    # ensure rebalance dates present
    rebalance_dates = sorted(weights_by_date.keys()) if rebalance_on is None else sorted(rebalance_on)

    pv = pd.Series(index=prices.index, dtype=float)
    turnover = pd.Series(0.0, index=rebalance_dates)

    nav = 1.0
    current_w = pd.Series(dtype=float)

    for dt in prices.index:
        if dt in rebalance_dates:
            target = weights_by_date.get(dt, pd.Series(dtype=float)).reindex(prices.columns).fillna(0.0)
            # compute turnover as sum of absolute differences
            turnover.loc[dt] = (current_w.reindex(target.index).fillna(0.0) - target).abs().sum()
            current_w = target
        # apply daily returns
        r = (current_w * returns.loc[dt]).sum()
        nav = nav * (1.0 + r)
        pv.loc[dt] = nav

    res = pd.DataFrame({"nav": pv})
    res = res.join(turnover.rename("turnover"), how="left")
    return res
