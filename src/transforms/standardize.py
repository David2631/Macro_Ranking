from typing import Iterable, Optional
import numpy as np
import pandas as pd


def robust_zscore(arr: Iterable[float], center: Optional[float] = None, scale: Optional[float] = None):
    a = np.asarray(list(arr), dtype=float)
    if center is None:
        center = np.nanmedian(a)
    # scale may be a scalar or an array matching a
    if scale is None:
        # use MAD scaled to be consistent with std for normal dist
        mad = np.nanmedian(np.abs(a - center))
        s = mad * 1.4826 if mad > 0 else np.nanstd(a)
        scale = s
    scale_arr = scale if np.ndim(scale) else np.full_like(a, float(scale))
    # avoid division by zero
    scale_arr = np.where((scale_arr == 0) | np.isnan(scale_arr), np.nan, scale_arr)
    out = (a - center) / scale_arr
    # replace NaN results (due to zero scale) with zeros
    out = np.where(np.isnan(out), 0.0, out)
    return out


def rolling_mad(series: pd.Series, window: int = 12, min_periods: int = 3):
    """Return rolling MAD scaled to be comparable to std (1.4826 factor)."""
    med = series.rolling(window=window, min_periods=min_periods).median()
    mad = series.subtract(med).abs().rolling(window=window, min_periods=min_periods).median()
    return mad * 1.4826


def winsorize(arr: Iterable[float], lower_pct: float = 0.01, upper_pct: float = 0.99):
    a = np.asarray(list(arr), dtype=float)
    low = np.nanquantile(a, lower_pct)
    high = np.nanquantile(a, upper_pct)
    return np.clip(a, low, high)


def rank_norm(arr: Iterable[float]):
    a = np.asarray(list(arr), dtype=float)
    # rank, convert to uniform [0,1], then to standard normal via inverse CDF
    s = pd.Series(a)
    ranks = s.rank(method='average', na_option='keep')
    uniform = (ranks - 0.5) / ranks.count()
    # avoid exact 0/1
    eps = np.finfo(float).eps
    uniform = np.clip(uniform, eps, 1 - eps)
    from scipy import stats

    return stats.norm.ppf(uniform.values)


def rolling_baseline(series: pd.Series, window: int = 12, min_periods: int = 3):
    """Compute rolling median baseline and return (baseline, deviation)

    baseline: rolling median
    deviation: series - baseline
    """
    baseline = series.rolling(window=window, min_periods=min_periods).median()
    deviation = series - baseline
    return baseline, deviation
