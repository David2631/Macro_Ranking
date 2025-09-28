import pandas as pd
from src.transforms.pipeline import apply_standardization, simple_score


def make_sample_df():
    rows = []
    # two indicators, two countries, monthly dates
    dates = [f"2020-{m:02d}" for m in range(1, 13)]
    for ind in ["IND1", "IND2"]:
        for c in ["A", "B"]:
            for i, d in enumerate(dates):
                val = i + (0 if c == "A" else 5) + (0 if ind == "IND1" else 2)
                rows.append({"indicator": ind, "country": c, "date": d, "value": val})
    return pd.DataFrame(rows)


def test_pipeline_standardize_and_score():
    df = make_sample_df()
    df_std = apply_standardization(df, method="robust_zscore")
    assert "std_value" in df_std.columns
    # basic sanity: output rows == input rows
    assert df_std.shape[0] == df.shape[0]
    scores = simple_score(df_std, by="country")
    assert set(scores["country"]) == {"A", "B"}
    # scores should be finite
    assert scores["score"].notnull().all()


def test_pipeline_invert_and_rolling_mad():
    df = make_sample_df()
    # test invert: for country B values are larger, so invert should flip sign
    df_std_norm = apply_standardization(df, method="robust_zscore", invert=False)
    df_std_inv = apply_standardization(df, method="robust_zscore", invert=True)
    # pick first row for country A and same position for inverted run
    a_norm = df_std_norm[
        (df_std_norm["country"] == "A") & (df_std_norm["indicator"] == "IND1")
    ].iloc[0]["std_value"]
    a_inv = df_std_inv[
        (df_std_inv["country"] == "A") & (df_std_inv["indicator"] == "IND1")
    ].iloc[0]["std_value"]
    assert a_norm == -a_inv

    # test rolling_mad on (should run without error)
    df_std_rm = apply_standardization(
        df, config={"rolling_mad": True}, method="robust_zscore"
    )
    assert "std_value" in df_std_rm.columns
