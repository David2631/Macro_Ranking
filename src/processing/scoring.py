import pandas as pd
from typing import Dict
import numpy as np
from typing import Tuple


def compute_coverage(pivot_df: pd.DataFrame) -> pd.Series:
    # pivot_df: countries x indicators with value_std (or raw) columns
    return pivot_df.notna().sum(axis=1) / pivot_df.shape[1]


def compute_composite(
    pivot_df: pd.DataFrame,
    weights: Dict[str, float],
    apply_coverage_penalty: bool = False,
    coverage_series: pd.Series = None,
    coverage_k: float = 1.0,
) -> pd.Series:
    """Compute weighted composite score per row (country).

    If apply_coverage_penalty is True, a coverage multiplier is computed using
    `coverage_penalty` and applied to the final composite score (per-country).
    """
    # pivot_df columns should match weights keys
    df = pivot_df.copy()
    # Align weights
    w = pd.Series(weights)
    # Ensure missing columns are treated as NaN
    for col in w.index:
        if col not in df.columns:
            df[col] = pd.NA
    # Coerce to numeric (turn pd.NA -> NaN) and compute weighted avg.
    df_num = (
        df.apply(lambda s: pd.to_numeric(s, errors="coerce"))
        .astype(float)
        .multiply(w, axis=1)
    )
    numerator = df_num.sum(axis=1, skipna=True)
    # denominator: for each cell, include weight if value is notna
    denom = (df_num.notna().multiply(w, axis=1)).sum(axis=1)
    # Avoid division by zero
    score = numerator.divide(denom).where(denom != 0)

    # Apply coverage penalty multiplier if requested
    if apply_coverage_penalty:
        # derive coverage if not provided
        if coverage_series is None:
            cov = pivot_df.notna().sum(axis=1) / pivot_df.shape[1]
        else:
            cov = coverage_series.reindex(score.index)
        try:
            mult = coverage_penalty(cov, k=coverage_k).reindex(score.index).fillna(0.0)
            score = score * mult
        except Exception:
            # on any error, fallback to unmodified score
            pass

    return score


def rank_scores(scores: pd.Series) -> pd.DataFrame:
    out = pd.DataFrame({"score": scores})
    # rank only non-NaN scores; NaNs stay NaN
    out["rank"] = out["score"].rank(ascending=False, method="min")
    # sort with NaNs last so countries without score appear at the bottom
    out = out.sort_values(by="score", ascending=False, na_position="last")
    return out


def coverage_penalty(coverage: pd.Series, k: float = 1.0) -> pd.Series:
    """Apply a penalty factor to scores based on coverage.

    Penalty = median_i - k * IQR_i is a conceptual note in the spec; here we
    return a multiplier between 0 and 1 where lower coverage reduces the multiplier.
    """
    cov = coverage.copy().astype(float)
    # compute median and IQR (75th - 25th)
    med = float(np.nanmedian(cov.values))
    q75 = float(np.nanpercentile(cov.values, 75))
    q25 = float(np.nanpercentile(cov.values, 25))
    iqr = q75 - q25

    # threshold below which multiplier becomes 0
    threshold = med - k * iqr
    # clamp threshold to [0,1]
    threshold = max(0.0, min(1.0, threshold))

    # prepare output series filled with NaN -> will set to 0 for NaNs later
    mult = pd.Series(index=cov.index, dtype=float)

    # degenerate case: no dispersion (iqr == 0) or med == threshold
    if np.isclose(iqr, 0.0) or np.isclose(med, threshold):
        # fallback: use identity clipped to [0,1]
        mult = cov.clip(0.0, 1.0)
        mult = mult.fillna(0.0)
        return mult

    # linear interpolation: threshold -> 0, median -> 1, above median -> 1
    for idx, val in cov.items():
        if np.isnan(val):
            mult.loc[idx] = 0.0
            continue
        if val >= med:
            mult.loc[idx] = 1.0
        elif val <= threshold:
            mult.loc[idx] = 0.0
        else:
            mult.loc[idx] = (val - threshold) / (med - threshold)

    # ensure bounds
    mult = mult.clip(0.0, 1.0)
    return mult


def bootstrap_scores(
    pivot_df: pd.DataFrame, weights: Dict[str, float], n_boot: int = 1000, seed: int = 0
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Bootstrap the composite scores by resampling indicators with replacement.

    Returns a tuple: (summary_df, samples_df)
    - summary_df: index countries, columns [score_mean, score_ci_low, score_ci_high]
    - samples_df: raw bootstrap samples (countries x n_boot)
    """
    rng = np.random.RandomState(seed)
    indicators = list(pivot_df.columns)
    if not indicators:
        # no indicators: return empty frames
        return pd.DataFrame(), pd.DataFrame()
    samples = []
    for i in range(n_boot):
        # resample indicators with replacement
        sampled = rng.choice(indicators, size=len(indicators), replace=True)
        df_samp = pivot_df.loc[:, sampled].copy()
        # align weights for sampled indicators (allow duplicates)
        w_vals = np.array([weights.get(ind, 0.0) for ind in sampled], dtype=float)
        vals = (
            df_samp.apply(lambda s: pd.to_numeric(s, errors="coerce"))
            .astype(float)
            .to_numpy()
        )
        # numerator: sum over columns of value * weight
        numer = (np.nan_to_num(vals, nan=0.0) * w_vals).sum(axis=1)
        # denom: sum of weights where value is not NA
        mask = ~np.isnan(vals)
        denom = (mask * w_vals).sum(axis=1)
        with np.errstate(divide="ignore", invalid="ignore"):
            score_arr = np.where(denom != 0, numer / denom, np.nan)
        score = pd.Series(score_arr, index=df_samp.index)
        samples.append(score)
    samples_df = pd.concat(samples, axis=1)
    mean = samples_df.mean(axis=1)
    ci_low = samples_df.quantile(0.025, axis=1)
    ci_high = samples_df.quantile(0.975, axis=1)
    summary = pd.DataFrame(
        {"score_mean": mean, "score_ci_low": ci_low, "score_ci_high": ci_high}
    )
    return summary, samples_df


def rank_stability(samples_df: pd.DataFrame, baseline_scores: pd.Series) -> pd.Series:
    """Compute a simple rank stability metric: fraction of bootstrap samples where rank equals baseline rank.
    Returns stability ∈ [0,1] per country.
    """
    baseline_rank = baseline_scores.rank(ascending=False, method="min")
    stab = pd.Series(index=baseline_scores.index, dtype=float)
    for country in baseline_scores.index:
        # compute rank across samples
        ranks = samples_df.rank(ascending=False, axis=0, method="min")
        matches = (ranks.loc[country] == baseline_rank.loc[country]).sum()
        stab.loc[country] = matches / samples_df.shape[1]
    return stab
