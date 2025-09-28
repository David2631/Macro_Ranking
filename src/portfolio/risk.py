import pandas as pd


def compute_turnover_costs(
    weights_old: pd.Series, weights_new: pd.Series, cost_per_unit: float = 0.001
) -> float:
    """Compute estimated turnover cost fraction between two weight vectors.

    weights_old/new: pd.Series indexed by country codes. cost_per_unit is fraction per unit turnover.
    """
    if weights_old is None or weights_old.empty:
        return 0.0
    idx = sorted(set(weights_old.index).union(set(weights_new.index)))
    w_old = weights_old.reindex(idx).fillna(0.0)
    w_new = weights_new.reindex(idx).fillna(0.0)
    turnover = (w_old - w_new).abs().sum()
    return float(turnover * float(cost_per_unit))
