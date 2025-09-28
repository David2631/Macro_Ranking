import pandas as pd
from src.processing.harmonize import harmonize_indicator, harmonize_df


def test_annual_to_quarter_padding():
    df = pd.DataFrame(
        {
            "date": pd.to_datetime(["2018-12-31", "2019-12-31", "2020-12-31"]),
            "value": [10.0, 20.0, 30.0],
        }
    )
    out, rep = harmonize_indicator(df, target_freq="Q", aggregation="mean")
    # expect quarterly rows covering years
    assert rep["source_freq"] == "A"
    assert rep["target_freq"] == "Q"
    # 2018-12-31 -> 2018Q4 through 2020Q4 inclusive = 9 quarters
    assert len(out) == 9
    # every quarter value should be one of the annual values
    assert set(out["value"].unique()).issubset({10.0, 20.0, 30.0})


def test_monthly_to_quarter_aggregation():
    dates = pd.date_range("2020-01-01", periods=6, freq="ME")
    df = pd.DataFrame({"date": dates, "value": [1, 2, 3, 4, 5, 6]})
    out, rep = harmonize_indicator(df, target_freq="Q", aggregation="mean")
    assert rep["source_freq"] == "M"
    assert len(out) == 2  # six months -> 2 quarters
    # first quarter mean = (1+2+3)/3 = 2
    assert out.iloc[0]["value"] == 2.0


def test_harmonize_df_integration():
    df = pd.DataFrame(
        {
            "indicator": ["i1"] * 6,
            "country": ["AAA"] * 6,
            "date": pd.date_range("2020-01-01", periods=6, freq="ME"),
            "value": [1, 2, 3, 4, 5, 6],
        }
    )
    out_df, rep_df = harmonize_df(df, target_freq="Q", aggregation="mean")
    assert "indicator" in out_df.columns and "country" in out_df.columns
    assert not rep_df.empty
