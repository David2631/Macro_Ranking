import numpy as np
import pandas as pd
from src.transforms import standardize


def test_robust_zscore_center_scale():
    arr = [1, 2, 3, 100]
    z = standardize.robust_zscore(arr)
    assert z.shape[0] == 4
    # median is 2.5, so median of [1,2,3,100] -> 2.5 -> first value negative
    assert z[0] < 0 < z[2]


def test_winsorize_clip():
    arr = [1, 2, 3, 100]
    w = standardize.winsorize(arr, lower_pct=0.0, upper_pct=0.75)
    # values should be clipped to the quantile bounds
    import numpy as np

    a = np.asarray(arr, dtype=float)
    high = np.nanquantile(a, 0.75)
    low = np.nanquantile(a, 0.0)
    assert max(w) <= high + 1e-8
    assert min(w) >= low - 1e-8


def test_rank_norm_monotonic():
    arr = [10, 20, 30, 40]
    r = standardize.rank_norm(arr)
    # increasing input should produce increasing ranks (normal quantiles also increase)
    assert all(np.diff(r) > 0)


def test_rolling_baseline():
    s = pd.Series(range(1, 25))
    baseline, deviation = standardize.rolling_baseline(s, window=4, min_periods=2)
    assert baseline.isna().sum() < len(s)
    assert deviation.shape == s.shape
