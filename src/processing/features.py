from typing import Iterable, Optional
import numpy as np
import pandas as pd


def winsorize(
    arr: Iterable[float], lower_pct: float = 0.01, upper_pct: float = 0.99
) -> np.ndarray:
    """Winsorize values in array at given percentiles.

    Args:
        arr: iterable of numbers (may contain nan)
        lower_pct: lower percentile (0..1)
        upper_pct: upper percentile (0..1)

    Returns:
        np.ndarray with winsorized values
    """
    a = np.asarray(arr, dtype="float64")
    if a.size == 0:
        return a
    valid = ~np.isnan(a)
    if not valid.any():
        return a
    lo = np.nanpercentile(a, lower_pct * 100)
    hi = np.nanpercentile(a, upper_pct * 100)
    res = a.copy()
    res[valid & (res < lo)] = lo
    res[valid & (res > hi)] = hi
    return res


def robust_zscore(arr: Iterable[float], window: Optional[int] = None) -> np.ndarray:
    """Compute robust z-score using median and MAD.

    If window is provided, compute rolling median/MAD; otherwise compute global.
    MAD is scaled by 1.4826 to be comparable to std for normal data.
    """
    s = pd.Series(arr, dtype="float64")
    if window is None or window >= len(s.dropna()):
        med = s.median()
        mad = (s - med).abs().median()
        if mad == 0 or pd.isna(mad):
            # fallback to std zscore
            return ((s - s.mean()) / s.std()).to_numpy()
        scale = mad * 1.4826
        return ((s - med) / scale).to_numpy()
    else:
        # rolling median and MAD
        rm = s.rolling(window=window, min_periods=1).median()

        # rolling mad: median absolute deviation of window
        def _mad(x):
            x = pd.Series(x)
            return (x - x.median()).abs().median()

        rmad = s.rolling(window=window, min_periods=1).apply(_mad, raw=False)
        scale = rmad * 1.4826
        out = (s - rm) / scale
        # where scale is zero or nan, fallback to standard zscore in that window
        fallback = (scale == 0) | scale.isna()
        if fallback.any():
            # compute rolling mean/std
            rmean = s.rolling(window=window, min_periods=1).mean()
            rstd = s.rolling(window=window, min_periods=1).std()
            out.loc[fallback] = (s.loc[fallback] - rmean.loc[fallback]) / rstd.loc[
                fallback
            ]
        return out.to_numpy()


def rank_norm(arr: Iterable[float]) -> np.ndarray:
    """Rank-based normalization to approximate normal scores (van der Waerden).

    Returns values approximately N(0,1).
    """
    s = pd.Series(arr, dtype="float64")
    n = s.rank(method="average", na_option="keep")
    # Convert rank to quantile in (0,1)
    denom = n.max()
    if denom == 0 or pd.isna(denom):
        return s.to_numpy()
    q = (n - 0.5) / denom
    # inverse normal
    import scipy.stats as st

    res = q.apply(lambda v: st.norm.ppf(v) if not pd.isna(v) else np.nan)
    return res.to_numpy()


def standardize_series(
    arr: Iterable[float],
    method: str = "zscore",
    winsor_p: float = 0.01,
    window: Optional[int] = None,
) -> np.ndarray:
    """Unified API to standardize a numeric series.

    Supported methods: 'zscore' (mean/std), 'robust_zscore' (median/MAD), 'rank' (rank-normalization)
    """
    a = np.asarray(arr, dtype="float64")
    if method == "zscore":
        s = pd.Series(a)
        return ((s - s.mean()) / s.std()).to_numpy()
    if method == "winsorized_zscore":
        w = winsorize(a, lower_pct=winsor_p, upper_pct=1 - winsor_p)
        s = pd.Series(w)
        return ((s - s.mean()) / s.std()).to_numpy()
    if method == "robust_zscore":
        return robust_zscore(a, window=window)
    if method == "rank":
        return rank_norm(a)
    raise ValueError(f"Unknown standardization method: {method}")


def apply_transform(
    df: pd.DataFrame, transform: str, indicator_id: str
) -> pd.DataFrame:
    df = df.copy()
    if transform == "none":
        return df
    if transform == "pct_change_yoy":
        df = df.sort_values("date")
        df["value"] = df["value"].pct_change(periods=12) * 100
        return df
    if transform == "pct_change_qoq":
        df = df.sort_values("date")
        df["value"] = df["value"].pct_change(periods=1) * 100
        return df
    if transform == "diff":
        df = df.sort_values("date")
        df["value"] = df["value"].diff()
        return df
    return df


def smooth(df: pd.DataFrame, window: int) -> pd.DataFrame:
    if window <= 1:
        return df
    df = df.copy()
    df = df.sort_values("date")
    df["value"] = df["value"].rolling(window=window, min_periods=1).mean()
    return df


def standardize(df: pd.DataFrame, method: str) -> pd.DataFrame:
    df = df.copy()
    if method == "zscore":
        mu = df["value"].mean()
        sigma = df["value"].std(ddof=0)
        df["value_std"] = (df["value"] - mu) / (sigma if sigma != 0 else 1.0)
        return df
    if method == "robust_zscore":
        # median / MAD based z-score (MAD scaled by 1.4826 to be comparable to std)
        med = df["value"].median()
        mad = (df["value"] - med).abs().median()
        mad_scaled = mad * 1.4826
        denom = mad_scaled if mad_scaled != 0 else 1.0
        df["value_std"] = (df["value"] - med) / denom
        return df
    if method.startswith("robust_zscore("):
        # format: robust_zscore(window=40) or robust_zscore(window=40,p=0.99)
        # simple parser
        try:
            inside = method[len("robust_zscore(") : -1]
            parts = {
                k: float(v) for k, v in [p.split("=") for p in inside.split(",") if p]
            }
            window = int(parts.get("window", 0))
        except Exception:
            window = 0
        if window <= 1:
            return standardize(df, "robust_zscore")
        # rolling median and MAD
        s = df.sort_values("date")
        med = s["value"].rolling(window=window, min_periods=1).median()
        mad = (
            s["value"]
            .rolling(window=window, min_periods=1)
            .apply(lambda x: (x - x.median()).abs().median())
        )
        mad_scaled = mad * 1.4826
        denom = mad_scaled.replace(0, 1.0)
        s["value_std"] = (s["value"] - med) / denom
        return s
    if method == "winsorized_zscore":
        # winsorize at 1%/99% then zscore
        low = df["value"].quantile(0.01)
        high = df["value"].quantile(0.99)
        tmp = df.copy()
        tmp["value"] = tmp["value"].clip(lower=low, upper=high)
        mu = tmp["value"].mean()
        sigma = tmp["value"].std(ddof=0)
        tmp["value_std"] = (tmp["value"] - mu) / (sigma if sigma != 0 else 1.0)
        return tmp
    if method == "rank_normalization":
        # map ranks to quantiles of standard normal (Blom approximation)
        df = df.copy()
        n = len(df)
        if n <= 1:
            df["value_std"] = 0.0
            return df
        ranks = df["value"].rank(method="average")
        # avoid 0/1 endpoints
        p = (ranks - 0.375) / (n + 0.25)

        def _ppf_standard_normal(pvals):
            """Inverse CDF (ppf) for standard normal using Acklam's approximation.

            Works on numpy arrays or scalars. Falls back to 0.0 for invalid p (<=0 or >=1).
            """
            p_arr = np.asarray(pvals, dtype=float)
            out = np.zeros_like(p_arr, dtype=float)
            # Mask valid probabilities
            mask = (p_arr > 0.0) & (p_arr < 1.0)
            if not mask.any():
                return out

            # Coefficients in rational approximations
            a = [
                -3.969683028665376e01,
                2.209460984245205e02,
                -2.759285104469687e02,
                1.383577518672690e02,
                -3.066479806614716e01,
                2.506628277459239e00,
            ]
            b = [
                -5.447609879822406e01,
                1.615858368580409e02,
                -1.556989798598866e02,
                6.680131188771972e01,
                -1.328068155288572e01,
            ]
            c = [
                -7.784894002430293e-03,
                -3.223964580411365e-01,
                -2.400758277161838e00,
                -2.549732539343734e00,
                4.374664141464968e00,
                2.938163982698783e00,
            ]
            d = [
                7.784695709041462e-03,
                3.224671290700398e-01,
                2.445134137142996e00,
                3.754408661907416e00,
            ]

            plow = 0.02425
            phigh = 1 - plow

            # Allocate result array
            res = np.empty_like(p_arr, dtype=float)

            # Lower region
            mask_low = mask & (p_arr < plow)
            if mask_low.any():
                q = np.sqrt(-2 * np.log(p_arr[mask_low]))
                res[mask_low] = (
                    ((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]
                ) / ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1)

            # Central region
            mask_central = mask & (p_arr >= plow) & (p_arr <= phigh)
            if mask_central.any():
                q = p_arr[mask_central] - 0.5
                r = q * q
                res[mask_central] = (
                    (
                        ((((a[0] * r + a[1]) * r + a[2]) * r + a[3]) * r + a[4]) * r
                        + a[5]
                    )
                    * q
                    / (((((b[0] * r + b[1]) * r + b[2]) * r + b[3]) * r + b[4]) * r + 1)
                )

            # Upper region
            mask_high = mask & (p_arr > phigh)
            if mask_high.any():
                q = np.sqrt(-2 * np.log(1 - p_arr[mask_high]))
                res[mask_high] = -(
                    ((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]
                ) / ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1)

            # For invalid p (<=0 or >=1), fill with 0.0
            res[~mask] = 0.0
            return res

        # compute ppf for each p
        p_vals = p.values if hasattr(p, "values") else np.asarray(p)
        q = _ppf_standard_normal(p_vals)
        df["value_std"] = q
        return df
    if method == "minmax":
        mn = df["value"].min()
        mx = df["value"].max()
        denom = (mx - mn) if (mx - mn) != 0 else 1.0
        df["value_std"] = (df["value"] - mn) / denom
        return df
    if method == "winsorize":
        # simple winsorization at 5th/95th percentiles
        low = df["value"].quantile(0.05)
        high = df["value"].quantile(0.95)
        df = df.copy()
        df["value"] = df["value"].clip(lower=low, upper=high)
        # return as-is (no further scaling)
        df["value_std"] = df["value"]
        return df
    # default noop
    df["value_std"] = df["value"]
    return df
