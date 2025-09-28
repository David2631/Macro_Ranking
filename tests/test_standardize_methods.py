import numpy as np
import math
from src.processing.features import (
    winsorize,
    robust_zscore,
    rank_norm,
    standardize_series,
)


def test_winsorize_basic():
    a = [0, 1, 2, 100]
    w = winsorize(a, lower_pct=0.0, upper_pct=0.75)
    # 100 should be capped to the 75th percentile value
    import numpy as _np

    expected_hi = _np.nanpercentile(_np.asarray(a, dtype=float), 75)
    assert _np.isclose(w[-1], expected_hi)


def test_winsorize_all_nan():
    a = [math.nan, math.nan]
    w = winsorize(a)
    assert np.all(np.isnan(w))


def test_robust_zscore_constant_series():
    a = [1, 1, 1, 1]
    rz = robust_zscore(a)
    # fallback to std-based zscore may produce nan (std=0), ensure no crash
    assert len(rz) == 4


def test_robust_zscore_rolling():
    a = [1, 2, 3, 100, 5, 6]
    rz = robust_zscore(a, window=3)
    assert len(rz) == len(a)


def test_rank_norm_basic():
    a = [10, 20, 30, 40]
    rn = rank_norm(a)
    # mean approximately 0
    assert abs(np.nanmean(rn)) < 1e-6
    # monotonicity preserved
    assert rn[0] < rn[1] < rn[2] < rn[3]


def test_standardize_series_methods():
    a = [1, 2, 3, 4, 5]
    for m in ["zscore", "winsorized_zscore", "robust_zscore", "rank"]:
        s = standardize_series(a, method=m)
        assert len(s) == len(a)


import pandas as pd
from src.transforms.pipeline import apply_standardization


def make_df(values):
    return pd.DataFrame(
        {
            "indicator": ["t1"] * len(values),
            "country": ["AAA"] * len(values),
            "date": pd.date_range("2020-01-01", periods=len(values), freq="ME"),
            "value": values,
        }
    )


def test_winsorized_and_robust_zscore_differ():
    df = make_df([1, 2, 3, 1000, 5, 6, 7])

    out_robust = apply_standardization(
        df.copy(),
        config={
            "winsor_lower": 0.01,
            "winsor_upper": 0.99,
            "rolling_window": 3,
            "rolling_min_periods": 1,
        },
        method="robust_zscore",
    )
    out_wins = apply_standardization(
        df.copy(),
        config={
            "winsor_lower": 0.10,
            "winsor_upper": 0.90,
            "rolling_window": 3,
            "rolling_min_periods": 1,
        },
        method="winsorized_zscore",
    )

    a = out_robust["std_value"]
    b = out_wins["std_value"]

    # With stronger winsorization, at least one element should differ
    assert not a.equals(b)


def test_auto_sign_check_flips_when_needed():
    # construct a dev series where higher raw value is bad but good_direction='up'
    df = make_df([10, 9, 8, 7, 6, 5, 4])
    # here, higher raw value indicates worse outcome, so if good_direction='up' we expect flip
    out = apply_standardization(
        df.copy(),
        config={
            "winsor_lower": 0.01,
            "winsor_upper": 0.99,
            "rolling_window": 3,
            "rolling_min_periods": 1,
        },
        method="robust_zscore",
        good_direction="up",
        auto_sign_check=True,
    )

    # compute spearman between dev and std — should be positive when good_direction matches, else flipped
    s = out.set_index("date")
    dev = s["value"] - s["value"].rolling(window=3, min_periods=1).median()
    corr = dev.corr(s["std_value"], method="spearman")

    # since we forced good_direction='up' but actual dev is negatively sloped, we expect corr>0 after auto-flip
    assert corr is not None
    assert corr > 0
