import pandas as pd
import numpy as np
from src.processing.harmonize import (
    frequency_pipeline,
    harmonize_indicator,
    harmonize_df,
    diagnostic_report,
)


def make_annual_series(values, start_year=2018):
    dates = [
        pd.Timestamp(f"{y}-12-31") for y in range(start_year, start_year + len(values))
    ]
    return pd.Series(values, index=pd.DatetimeIndex(dates))


def make_monthly_series(values, start="2020-01-31"):
    idx = pd.date_range(start=start, periods=len(values), freq="M")
    return pd.Series(values, index=idx)


def test_frequency_pipeline_annual_to_quarter():
    s = make_annual_series([10, 20, 30], start_year=2018)
    q = frequency_pipeline(s, from_freq="A", to_freq="Q")
    # for 3 annual observations we expect 9 resulting quarters (2018Q4..2020Q4)
    assert len(q) == 9
    # values should be forward-filled so last quarter of 2018 equals 10
    assert q.iloc[2] == 10


def test_frequency_pipeline_monthly_to_quarter_mean():
    m = make_monthly_series([1, 2, 3, 4, 5, 6], start="2020-01-31")
    q = frequency_pipeline(m, from_freq="M", to_freq="Q", rule="mean")
    # expect 2 quarters (6 months)
    assert len(q) == 2
    # first quarter is mean of 1,2,3 == 2
    assert np.isclose(q.iloc[0], 2.0)


def test_harmonize_indicator_annual():
    df = pd.DataFrame(
        {
            "date": pd.to_datetime(["2018-12-31", "2019-12-31"]),
            "value": [100, 200],
        }
    )
    out, rep = harmonize_indicator(df, target_freq="Q", aggregation="mean")
    assert rep["source_freq"] == "A"
    assert rep["target_freq"] == "Q"
    assert len(out) >= 4


def test_harmonize_df_grouping():
    df = pd.DataFrame(
        {
            "indicator": ["i1"] * 3 + ["i2"] * 3,
            "country": ["AAA"] * 3 + ["BBB"] * 3,
            "date": pd.date_range("2020-01-01", periods=3, freq="M").tolist()
            + pd.date_range("2020-01-01", periods=3, freq="M").tolist(),
            "value": [1, 2, 3, 4, 5, 6],
        }
    )
    out_df, report_df = harmonize_df(df, target_freq="Q", aggregation="mean")
    # Should produce rows for 2 indicators
    assert "indicator" in out_df.columns
    assert len(report_df) == 2


def test_diagnostic_report():
    r = diagnostic_report("gdp", "M", "Q", "mean")
    assert r["from_freq"] == "M"
    assert "notes" in r
