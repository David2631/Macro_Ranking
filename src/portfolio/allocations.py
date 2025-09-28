import pandas as pd
import numpy as np
from typing import Optional


def score_to_weights(
    scores: pd.Series,
    min_alloc: float = 0.0,
    max_alloc: float = 1.0,
    top_n: Optional[int] = None,
    region_map: Optional[dict] = None,
    max_region_alloc: Optional[float] = None,
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

    # Apply region-level cap if requested. region_map should map country -> region id.
    # When max_region_alloc is set, reduce weights proportionally within regions that exceed the cap
    if region_map and max_region_alloc is not None:
        # compute region sums
        region_series = pd.Series(region_map)
        # map the weights to regions; countries not in region_map get key None
        reg_of = {
            k: region_series.get(k) if k in region_series.index else None
            for k in weights.index
        }
        from typing import Dict

        reg_idx: Dict[str, list] = {}
        for c, r in reg_of.items():
            reg_idx.setdefault(r, []).append(c)

        # iterative adjustment: for regions exceeding cap, scale down proportionally and redistribute the freed budget
        freed = 0.0
        adjusted = weights.copy()
        for r, members in reg_idx.items():
            if r is None:
                continue
            s = adjusted.loc[members].sum()
            if s > max_region_alloc + 1e-12:
                # scale factor to reduce to cap
                factor = max_region_alloc / s
                adjusted.loc[members] = adjusted.loc[members] * factor
                freed += s - adjusted.loc[members].sum()

        # redistribute freed budget proportionally across unconstrained members (regions that are below cap and countries without region)
        if freed > 1e-12:
            # eligible recipients: countries whose region is None or region sum < max_region_alloc
            eligible = []
            region_sums = {
                r: adjusted.loc[members].sum()
                for r, members in reg_idx.items()
                if r is not None
            }
            for c in adjusted.index:
                r = reg_of.get(c)
                if r is None or region_sums.get(r, 0.0) < max_region_alloc - 1e-12:
                    eligible.append(c)
            if eligible:
                # distribute proportional to current weight (or equal if zero)
                cur = adjusted.loc[eligible]
                if cur.sum() == 0:
                    add = pd.Series(1.0 / len(eligible), index=eligible) * freed
                else:
                    add = (cur / cur.sum()) * freed
                adjusted.loc[eligible] = adjusted.loc[eligible] + add

        # final renormalize to sum to 1
        if adjusted.sum() > 0:
            adjusted = adjusted / adjusted.sum()
        weights = adjusted
    return weights


def write_allocations(path: str, weights: pd.Series):
    df = weights.reset_index()
    df.columns = ["country", "weight"]
    df.to_csv(path, index=False)
