import unittest
import pandas as pd
from src.processing.features import standardize


class SmokeTests(unittest.TestCase):
    def test_standardize_zscore(self):
        df = pd.DataFrame({"value": [1, 2, 3, 4, 5]})
        out = standardize(df, "zscore")
        self.assertIn("value_std", out.columns)


if __name__ == "__main__":
    unittest.main()
