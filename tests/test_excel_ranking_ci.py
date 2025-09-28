import os
import sys
import pandas as pd

# add project root
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from src.io.excel import export_to_excel


def test_export_ranking_with_ci(tmp_path):
    # build a simple ranked df with CI and stability
    ranked = pd.DataFrame({
        "score": {"A": 1.0, "B": 0.5},
        "score_ci_low": {"A": 0.9, "B": 0.4},
        "score_ci_high": {"A": 1.1, "B": 0.6},
        "rank_stability": {"A": 0.95, "B": 0.6},
    })
    indicators_df = pd.DataFrame()
    raw_df = pd.DataFrame()
    out = tmp_path / "test_ranking.xlsx"
    # should not raise
    path = export_to_excel(str(out), ranked, indicators_df, raw_df, {})
    assert os.path.exists(path)