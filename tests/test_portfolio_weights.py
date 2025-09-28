import pandas as pd
import numpy as np

from src.portfolio.weights import threshold_power_weights


def test_threshold_power_basic():
    scores = pd.Series({"USA": 0.9, "DEU": 0.5, "FRA": 0.2, "ESP": -0.1})
    # threshold 0.1 removes ESP and reduces FRA
    w = threshold_power_weights(scores, threshold=0.1, power=1.0)
    assert "ESP" not in w.index
    # weights sum to 1
    assert np.isclose(w.sum(), 1.0)
    # USA should have largest weight
    assert w.loc["USA"] > w.loc["DEU"]


def test_threshold_power_power_tilt():
    scores = pd.Series({"A": 1.0, "B": 0.8, "C": 0.5})
    w1 = threshold_power_weights(scores, threshold=0.0, power=1.0)
    w2 = threshold_power_weights(scores, threshold=0.0, power=2.0)
    # with higher power, the largest should get relatively more weight
    assert w2.loc["A"] / w2.loc["B"] > w1.loc["A"] / w1.loc["B"]
