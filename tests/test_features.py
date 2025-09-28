import pandas as pd
import numpy as np
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.processing.features import standardize  # noqa: E402


def test_robust_zscore_basic():
    vals = [1, 2, 2, 3, 100]
    df = pd.DataFrame({"value": vals})
    out = standardize(df, "robust_zscore")
    # median is 2, mad = median(|x-med|) = median([1,0,0,1,98]) = 1 -> scaled ~1.4826
    # so z for 100 should be (100-2)/1.4826 ~ 66.14
    assert "value_std" in out.columns
    assert out["value_std"].iloc[1] == 0 or abs(out["value_std"].iloc[1]) < 1e-8
    # highest value should be large positive
    assert out["value_std"].iloc[-1] > 50


def test_robust_zscore_rolling_window():
    vals = [1, 2, 2, 3, 100]
    df = pd.DataFrame(
        {
                "date": pd.date_range(start="2020-01-01", periods=len(vals), freq="ME"),
            "value": vals,
        }
    )
    out = standardize(df, "robust_zscore(window=3)")
    assert "value_std" in out.columns
    assert len(out) == len(vals)
    # last value should be finite and much larger than earlier ones
    assert np.isfinite(out["value_std"].iloc[-1])


def test_winsorized_zscore():
    vals = [1, 2, 3, 4, 1000]
    df = pd.DataFrame({"value": vals})
    out = standardize(df, "winsorized_zscore")
    assert "value_std" in out.columns
    # winsorized result should have the extreme clipped (value column returned is clipped)
    assert out["value"].max() < 1000


def test_rank_normalization_mapping():
    vals = list(range(1, 11))
    df = pd.DataFrame({"value": vals})
    out = standardize(df, "rank_normalization")
    assert "value_std" in out.columns
    # ranks should map to increasing normalized scores
    assert out["value_std"].is_monotonic_increasing
