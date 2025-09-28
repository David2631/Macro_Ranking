from typing import Dict, Optional, Any
import pandas as pd
from .weights import threshold_power_weights


def compute_target_weights(
    scores: pd.Series,
    mapping: Optional[Dict[str, Dict[str, Any]]] = None,
    threshold: float = 0.0,
    power: float = 1.0,
    min_alloc: float = 0.0,
    max_alloc: float = 1.0,
    top_n: Optional[int] = None,
) -> pd.Series:
    """High-level wrapper: convert scores -> portfolio weights using threshold_power_weights.

    mapping is not strictly required for weight computation but passed through for future extensions.
    """
    w = threshold_power_weights(
        scores,
        threshold=threshold,
        power=power,
        min_alloc=min_alloc,
        max_alloc=max_alloc,
        top_n=top_n,
    )
    return w


def apply_turnover_costs(
    weights_old: pd.Series, weights_new: pd.Series, cost_per_unit: float = 0.001
) -> float:
    """Compute turnover cost given previous and new weights; returns total cost fraction.

    cost_per_unit is cost per absolute weight change (e.g., 0.001 = 10 bps per 100% turnover)
    """
    if weights_old is None or weights_old.empty:
        return 0.0
    # align indices
    idx = sorted(set(weights_old.index).union(set(weights_new.index)))
    w_old = weights_old.reindex(idx).fillna(0.0)
    w_new = weights_new.reindex(idx).fillna(0.0)
    turnover = (w_old - w_new).abs().sum()
    return float(turnover * cost_per_unit)
