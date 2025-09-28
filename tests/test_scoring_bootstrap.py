import sys
import os
import pandas as pd

# ensure src importable
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from src.processing.scoring import compute_composite, bootstrap_scores, rank_stability


def test_bootstrap_scores_basic():
    # create pivot with 3 countries and 3 indicators
    data = {
        "ind1": [1.0, 0.5, -0.2],
        "ind2": [0.8, 0.4, -0.1],
        "ind3": [1.2, 0.6, 0.0],
    }
    idx = ["A", "B", "C"]
    pivot = pd.DataFrame(data, index=idx)
    weights = {"ind1": 0.4, "ind2": 0.3, "ind3": 0.3}
    summary, samples = bootstrap_scores(pivot, weights, n_boot=200, seed=42)
    assert "score_mean" in summary.columns
    assert samples.shape[1] == 200

    # baseline composite
    baseline = compute_composite(pivot, weights)
    stab = rank_stability(samples, baseline)
    assert all((stab >= 0) & (stab <= 1))
