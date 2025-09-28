import pandas as pd
from src.portfolio.allocations import score_to_weights


def test_region_cap_proportionally_limits_region():
    s = pd.Series({"A": 1.0, "B": 0.8, "C": 0.5, "D": 0.2})
    # A,B in region R1, C,D in R2
    region_map = {"A": "R1", "B": "R1", "C": "R2", "D": "R2"}
    # cap region R1 to 0.4
    w = score_to_weights(s, region_map=region_map, max_region_alloc=0.4)
    # region sums
    r1 = w.loc[["A", "B"]].sum()
    assert r1 <= 0.4001
    assert abs(w.sum() - 1.0) < 1e-8


def test_region_cap_allocation_with_unmapped_countries():
    s = pd.Series({"A": 1.0, "B": 0.5, "C": 0.2})
    region_map = {"A": "R1", "B": None}
    w = score_to_weights(s, region_map=region_map, max_region_alloc=0.6)
    assert abs(w.sum() - 1.0) < 1e-8
    # A should not exceed cap
    assert w["A"] <= 0.6001
