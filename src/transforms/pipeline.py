from typing import Optional, Dict
import pandas as pd
import numpy as np
from src.transforms import standardize
from src.config import DEFAULT_STD_CONFIG, StandardizeConfig
from scipy import stats


def apply_standardization(
    df: pd.DataFrame,
    config: Optional[Dict] = None,
    method: str = "robust_zscore",
    group_keys: Optional[list] = None,
    invert: bool = False,
    good_direction: Optional[str] = None,
    auto_sign_check: bool = True,
):
    """Apply standardization per group (indicator, country) to a DataFrame.

    Expects df with columns: ['indicator', 'country', 'date', 'value']
    Returns df with an added 'std_value' column.
    """
    # accept either dict or pydantic config
    if config is None:
        cfg: StandardizeConfig = DEFAULT_STD_CONFIG
    else:
        try:
            cfg = DEFAULT_STD_CONFIG.copy(update=config)
        except Exception:
            # if config is already a StandardizeConfig or similar, use as-is
            cfg = config  # type: ignore[assignment]
    gk = group_keys or ["indicator", "country"]

    out = df.copy()
    out["std_value"] = np.nan

    # sort by date for rolling operations
    if "date" in out.columns:
        out = out.sort_values("date")

    grouped = out.groupby(gk)

    results = []
    for name, group in grouped:
        s = group.set_index("date")["value"].astype(float)
        # compute rolling baseline and deviation
        baseline, deviation = standardize.rolling_baseline(
            s, window=cfg.rolling_window, min_periods=cfg.rolling_min_periods
        )
        # deviation may have NaNs for early windows; dropna for stats
        dev = deviation.fillna(0).values

        # optionally use rolling MAD as scale for robust zscore
        rolling_mad = None
        if getattr(cfg, 'rolling_mad', False):
            try:
                rm = standardize.rolling_mad(s, window=cfg.rolling_window, min_periods=cfg.rolling_min_periods)
                # align to dev index and fillna (use bfill/ffill to avoid deprecated fillna(method=...))
                rolling_mad = rm.bfill().ffill().values
            except Exception:
                rolling_mad = None

        # winsorize if requested
        if method in ("winsorized_zscore", "robust_zscore", "zscore"):
            w = standardize.winsorize(dev, lower_pct=cfg.winsor_lower, upper_pct=cfg.winsor_upper)
        else:
            w = dev

        # standardize methods
        if method == "robust_zscore":
            z = standardize.robust_zscore(w, scale=rolling_mad if rolling_mad is not None else None)
        elif method == "zscore":
            # classic zscore: mean/std
            a = np.asarray(w, dtype=float)
            mu = np.nanmean(a)
            sd = np.nanstd(a)
            sd = sd if sd > 0 else np.nan
            z = (a - mu) / (sd if sd and not np.isnan(sd) else 1.0)
        elif method == "winsorized_zscore":
            # winsorize then classic zscore
            a = np.asarray(w, dtype=float)
            mu = np.nanmean(a)
            sd = np.nanstd(a)
            sd = sd if sd > 0 else np.nan
            z = (a - mu) / (sd if sd and not np.isnan(sd) else 1.0)
        elif method == "rank_norm":
            z = standardize.rank_norm(w)
        else:
            raise ValueError(f"unknown standardization method: {method}")

        # apply invert if requested (useful for indicators where lower is better)
        if invert:
            z = -1.0 * z

        # automatic sign check: ensure that sign of relationship between raw value and
        # standardized score matches declared good_direction. If mismatch and auto_sign_check
        # is enabled, flip the sign.
        if auto_sign_check and good_direction in ("up", "down"):
            try:
                # compute Spearman correlation between raw dev and z
                valid = (~np.isnan(dev)) & (~np.isnan(z))
                if valid.sum() >= 2:
                    corr, _ = stats.spearmanr(dev[valid], z[valid])
                    if not np.isnan(corr):
                        if good_direction == "up" and corr < 0:
                            z = -1.0 * z
                        if good_direction == "down" and corr > 0:
                            z = -1.0 * z
            except Exception:
                # on any failure, keep z as-is
                pass

        # map back to group order
        grp_out = group.copy()
        grp_out = grp_out.assign(std_value=list(z))
        results.append(grp_out)

    if results:
        df_out = pd.concat(results, ignore_index=True)
    else:
        df_out = out

    # restore original ordering by index if date existed
    return df_out.sort_index()


def simple_score(df_std: pd.DataFrame, by: str = "country") -> pd.DataFrame:
    """Simple scoring: mean of std_value per group (country)."""
    s = df_std.groupby(by)["std_value"].mean().reset_index()
    s = s.rename(columns={"std_value": "score"})
    return s
