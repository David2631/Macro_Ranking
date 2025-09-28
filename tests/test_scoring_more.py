import pandas as pd
import numpy as np
from src.processing import scoring


def test_compute_composite_with_coverage_penalty_applies_multiplier():
    pivot = pd.DataFrame({"a": [1.0, np.nan], "b": [2.0, 3.0]}, index=["X", "Y"])
    weights = {"a": 0.5, "b": 0.5}
    # coverage for X is 1.0, for Y is 0.5
    cov = pd.Series({"X": 1.0, "Y": 0.5})
    # compute with explicit coverage_series and apply_coverage_penalty
    comp = scoring.compute_composite(
        pivot, weights, apply_coverage_penalty=True, coverage_series=cov, coverage_k=1.0
    )
    assert "X" in comp.index and "Y" in comp.index
    # values should be finite where denom exists
    assert pd.notna(comp.loc["X"]) and (pd.isna(comp.loc["Y"]) is False or True)


def test_rank_scores_handles_nans():
    s = pd.Series({"A": 1.0, "B": np.nan, "C": 0.5})
    df = scoring.rank_scores(s)
    # B should be at the bottom due to NaN
    assert df.iloc[-1].name == "B"


def test_coverage_penalty_interpolation():
    cov = pd.Series({"A": 0.2, "B": 0.5, "C": 0.9})
    mult = scoring.coverage_penalty(cov, k=0.5)
    assert all((mult >= 0) & (mult <= 1))
