from typing import Tuple, Dict, Any
import pandas as pd
import numpy as np


def _infer_freq(series: pd.Series) -> str:
    # Try pandas infer_freq, fallback to median delta heuristic
    try:
        f = pd.infer_freq(series.index)
        if f is not None:
            # normalize some common codes
            if f.startswith("A") or f.startswith("Y"):
                return "A"
            if f.startswith("Q"):
                return "Q"
            if f.startswith("M"):
                return "M"
    except Exception:
        pass
    # median delta days heuristic
    if len(series.index) < 2:
        return "M"
    deltas = np.diff(series.index.astype("int64") // 10**9)  # seconds
    median_days = np.median(deltas) / (24 * 3600)
    if median_days > 300:
        return "A"
    if median_days > 40:
        return "Q"
    return "M"


def _quarter_index_range(start, end):
    # use QE (quarter end) to be compatible with pandas new tokens
    return pd.date_range(start=start, end=end, freq="QE")


def harmonize_indicator(
    df: pd.DataFrame,
    target_freq: str = "Q",
    aggregation: str = "mean",
    sa_flag: bool = False,
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """Harmonize a single indicator/country time series to target frequency.

    Expects df columns: ['date', 'value'] with date as datetime64.
    Returns (harmonized_df, report)
    """
    if df.empty:
        return df.copy(), {
            "n_in": 0,
            "n_out": 0,
            "source_freq": None,
            "target_freq": target_freq,
        }

    s = df.sort_values("date").set_index("date")["value"].astype(float)
    src_freq = _infer_freq(s)

    report = {
        "source_freq": src_freq,
        "target_freq": target_freq,
        "aggregation": aggregation,
        "sa_flag": sa_flag,
        "n_in": len(s),
    }

    if src_freq == target_freq:
        out = s.reset_index().rename(columns={"value": "value"})
        report["n_out"] = len(out)
        return out, report

    # Annual to Quarterly: pad annual values forward to quarter ends
    if src_freq == "A" and target_freq == "Q":
        start = s.index.min()
        end = s.index.max()
        qidx = _quarter_index_range(start=start, end=end + pd.offsets.QuarterEnd(0))
        # reindex quarters and forward fill by aligning annual dates to quarter index
        # create series reindexed to quarter index then ffill
        # first reindex original series to its year-end to align
        annual = s.copy()
        # ensure annual series indexed at year-end
        # normalize to year-end timestamps using 'Y' token
        annual.index = annual.index.to_period("Y").to_timestamp("Y")
        # if seasonal-adjusted flag is set and an sa_value column exists, prefer it
        if sa_flag and "sa_value" in df.columns:
            annual = df.set_index("date")["sa_value"].astype(float)
            annual.index = annual.index.to_period("Y").to_timestamp("Y")
        qseries = annual.reindex(qidx, method="ffill")
        out = pd.DataFrame({"date": qseries.index, "value": qseries.values})
        report["n_out"] = len(out)
        return out, report

    # Monthly to Quarterly: aggregate months into quarter using aggregation rule
    if src_freq == "M" and target_freq == "Q":
        # use quarter-end resampling token
        r = s.resample("QE")
        if aggregation == "mean":
            q = r.mean()
        elif aggregation == "median":
            q = r.median()
        elif aggregation == "last":
            q = r.last()
        else:
            # fallback to mean
            q = r.mean()
        out = pd.DataFrame({"date": q.index, "value": q.values})
        report["n_out"] = len(out)
        return out, report

    # Fallback: attempt resample with asfreq or ffill
    try:
        if target_freq == "Q":
            qidx = _quarter_index_range(start=s.index.min(), end=s.index.max())
            out_series = s.reindex(qidx, method="ffill")
            out = pd.DataFrame({"date": out_series.index, "value": out_series.values})
            report["n_out"] = len(out)
            return out, report
    except Exception:
        pass

    # If nothing else, return original
    report["n_out"] = len(s)
    return (
        s.reset_index().rename(columns={"value": "value"}).reset_index(drop=True),
        report,
    )


def harmonize_df(
    df: pd.DataFrame,
    target_freq: str = "Q",
    aggregation: str = "mean",
    sa_flag: bool = False,
):
    """Apply harmonization to a full DataFrame with columns ['indicator','country','date','value'].

    Returns (harmonized_df, report_df)
    """
    results = []
    reports = []
    gb = df.groupby(["indicator", "country"])
    for (ind, c), group in gb:
        hdf, rep = harmonize_indicator(
            group[["date", "value"]],
            target_freq=target_freq,
            aggregation=aggregation,
            sa_flag=sa_flag,
        )
        hdf["indicator"] = ind
        hdf["country"] = c
        results.append(hdf[["indicator", "country", "date", "value"]])
        rep.update({"indicator": ind, "country": c})
        reports.append(rep)

    if results:
        out_df = pd.concat(results, ignore_index=True)
    else:
        out_df = df.copy()

    report_df = pd.DataFrame(reports)
    return out_df, report_df


import pandas as pd
import pycountry


def iso_to_iso3(code: str) -> str:
    # Accept already ISO3 or ISO2; attempt conversion
    if not code:
        return code
    code = code.strip().upper()
    if len(code) == 3:
        return code
    try:
        c = pycountry.countries.get(alpha_2=code)
        if c:
            return c.alpha_3
    except Exception:
        pass
    try:
        c = pycountry.countries.lookup(code)
        if c:
            return c.alpha_3
    except Exception:
        pass
    return code


def harmonize_countries(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["country"] = df["country"].apply(lambda x: iso_to_iso3(x))
    return df


def parse_dates(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    return df


def frequency_pipeline(
    series: pd.Series, from_freq: str, to_freq: str, rule: str = "mean"
) -> pd.Series:
    """
    Resample a time series from `from_freq` (e.g., 'A','Q','M') to `to_freq`.
    Supports:
      - A->Q: asfreq('Q', method='pad') treating annual as end-of-year values
      - M->Q: aggregate monthly to quarter-end via mean/last/median

    Returns resampled Series indexed by the target period end dates.
    """
    s = series.dropna().sort_index()
    if from_freq == to_freq:
        return s
    # normalise freq tokens
    f = from_freq.upper()
    t = to_freq.upper()
    if f == "A" and t == "Q":
        # treat annual value as available at year-end -> forward-fill to quarterly
        # convert index to period end if needed
        idx = pd.to_datetime(s.index)
        s.index = idx
        # resample by quarter end, pad annual values forward within same year
        q = s.resample("QE").ffill()
        return q
    if f == "M" and t == "Q":
        if rule == "mean":
            return s.resample("QE").mean()
        if rule == "last":
            return s.resample("QE").last()
        if rule == "median":
            return s.resample("QE").median()
    # default fallback: try pandas resample with mean
    try:
        # normalize target freq tokens for pandas deprecation: 'Q'->'QE', 'M'->'ME'
        tf = to_freq.upper()
        if tf == "Q":
            tf = "QE"
        if tf == "M":
            tf = "ME"
        return s.resample(tf).mean()
    except Exception:
        return s


def diagnostic_report(
    indicator_name: str, original_freq: str, target_freq: str, rule: str = "mean"
) -> dict:
    return {
        "indicator": indicator_name,
        "from_freq": original_freq,
        "to_freq": target_freq,
        "rule": rule,
        "notes": f"Resample {original_freq}->{target_freq} using {rule}",
    }
