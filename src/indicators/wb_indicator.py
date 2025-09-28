from typing import List, Dict
import pandas as pd
from .base import IndicatorPlugin
from ..fetchers.worldbank import WorldBankFetcher


class WBIndicator(IndicatorPlugin):
    def __init__(self, config: Dict, indicator_id: str, source_code: str):
        super().__init__(config)
        self.id = indicator_id
        self.code = source_code
        self.fetcher = WorldBankFetcher(config.get("runtime"))

    def fetch(
        self, countries: List[str], start: str, end: str, freq: str
    ) -> pd.DataFrame:
        df, logs = self.fetcher.fetch(
            countries, [{"id": self.id, "code": self.code}], start, end, freq
        )
        return df, logs

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        # simple passthrough for now
        return df

    def standardize(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        mu = df["value"].mean()
        sigma = df["value"].std(ddof=0)
        df["value_std"] = (df["value"] - mu) / (sigma if sigma != 0 else 1.0)
        return df

    def direction(self) -> str:
        return "up"
