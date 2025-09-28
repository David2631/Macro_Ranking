from typing import Optional
import pandas as pd
import numpy as np
from .allocations import score_to_weights


def threshold_power_weights(
    scores: pd.Series,
    threshold: float = 0.0,
    power: float = 1.0,
    min_alloc: float = 0.0,
    max_alloc: float = 1.0,
    top_n: Optional[int] = None,
) -> pd.Series:
    """Convert scores to weights using w_i ∝ max(score_i - threshold, 0)^power

    - scores: pd.Series indexed by asset (ISO3)
    - threshold: minimum score to start allocating
    - power: exponent to tilt allocation (power>1 concentrates on top scores)
    - min_alloc/max_alloc/top_n forwarded to `score_to_weights` for clamping

    Returns pd.Series of normalized weights summing to 1.
    """
    s = scores.dropna().astype(float).copy()
    if s.empty:
        return pd.Series(dtype=float)

    # compute positive, thresholded values
    trimmed = (s - float(threshold)).clip(lower=0.0)
    if trimmed.sum() == 0:
        # nothing above threshold -> return empty weights (or equal shares?)
        return pd.Series(dtype=float)

    tilted = np.power(trimmed.values, float(power))
    tilted_series = pd.Series(tilted, index=trimmed.index)

    # delegate the normalization and clamping to existing helper
    weights = score_to_weights(tilted_series, min_alloc=min_alloc, max_alloc=max_alloc, top_n=top_n)
    # drop zero (or near-zero) weights so returned series only contains active holdings
    if weights.empty:
        return weights
    eps = 1e-12
    weights = weights[weights > eps].copy()
    if weights.empty:
        return pd.Series(dtype=float)
    # renormalize to sum to 1.0 to avoid tiny numerical drift
    weights = weights / float(weights.sum())
    return weights
