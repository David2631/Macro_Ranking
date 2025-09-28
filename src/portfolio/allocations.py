import pandas as pd
import numpy as np
from typing import Optional


def score_to_weights(
    scores: pd.Series,
    min_alloc: float = 0.0,
    max_alloc: float = 1.0,
    top_n: Optional[int] = None,
) -> pd.Series:
    """Convert a score series into portfolio weights.

    - Keep only positive scores by default (if scores can be negative, shift to positive part)
    - If top_n is provided, only allocate to top_n countries by score
    - Normalize to sum to 1.0
    - Clamp per-country allocations between min_alloc and max_alloc
    Returns a pandas Series indexed by country with weight values.
    """
    s = scores.dropna().astype(float).copy()
    if s.empty:
        return pd.Series(dtype=float)

    # If negatives exist, shift so minimum is zero
    min_val = s.min()
    if min_val < 0:
        s = s - min_val

    if top_n is not None and len(s) > top_n:
        s = s.nlargest(top_n)

    names = list(s.index)
    n = len(names)

    # feasibility checks
    if min_alloc * n > 1.0 + 1e-12:
        raise ValueError("min_alloc too large for number of assets")
    if max_alloc * n < 1.0 - 1e-12:
        raise ValueError("max_alloc too small for number of assets")

    scores_arr = s.values.astype(float)
    # if all scores are zero, set equal scores for proportional allocation
    if scores_arr.sum() == 0:
        scores_arr = np.ones_like(scores_arr)

    remaining_idx = list(range(n))
    fixed = {}
    remaining_budget = 1.0
    sum_scores_remaining = float(scores_arr.sum())

    # iterative redistribution respecting bounds
    for _ in range(n * 3):
        if not remaining_idx:
            break
        # provisional weights for remaining assets
        provisional = {
            i: (scores_arr[i] / sum_scores_remaining) * remaining_budget
            for i in remaining_idx
        }
        to_fix_low = [i for i, w in provisional.items() if w < min_alloc - 1e-12]
        to_fix_high = [i for i, w in provisional.items() if w > max_alloc + 1e-12]

        if not to_fix_low and not to_fix_high:
            # assign provisional to remaining and finish
            for i, w in provisional.items():
                fixed[i] = w
            remaining_idx = []
            break

        # first fix lows
        if to_fix_low:
            for i in to_fix_low:
                fixed[i] = min_alloc
                remaining_idx.remove(i)
                remaining_budget -= min_alloc
                sum_scores_remaining -= scores_arr[i]
            if remaining_budget < -1e-12:
                raise ValueError("Infeasible allocation after applying min_allocs")

        # then fix highs
        if to_fix_high:
            for i in to_fix_high:
                # if already fixed as low in this iteration, skip
                if i not in remaining_idx:
                    continue
                fixed[i] = max_alloc
                remaining_idx.remove(i)
                remaining_budget -= max_alloc
                sum_scores_remaining -= scores_arr[i]
            if remaining_budget < -1e-12:
                raise ValueError("Infeasible allocation after applying max_allocs")

    # assemble final weights
    weights = pd.Series(dtype=float)
    for idx, name in enumerate(names):
        if idx in fixed:
            weights.at[name] = float(fixed[idx])
        else:
            # any remaining ones get proportional share (may be zero)
            if (
                sum_scores_remaining > 0
                and remaining_budget > 0
                and idx in remaining_idx
            ):
                weights.at[name] = float(
                    (scores_arr[idx] / sum_scores_remaining) * remaining_budget
                )
            else:
                weights.at[name] = 0.0

    # final normalization to mitigate tiny numerical drift
    total_w = weights.sum()
    if total_w <= 0:
        # equal fallback
        weights = pd.Series(1.0 / n, index=names)
    else:
        weights = weights / total_w
    return weights


def write_allocations(path: str, weights: pd.Series):
    df = weights.reset_index()
    df.columns = ["country", "weight"]
    df.to_csv(path, index=False)
