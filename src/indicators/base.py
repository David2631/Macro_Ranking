from __future__ import annotations
from typing import List, Dict
import pandas as pd


class IndicatorPlugin:
    """Base class for indicator plugins. Implementers should provide fetch, transform, std and direction metadata."""

    id: str

    def __init__(self, config: Dict):
        self.config = config

    def fetch(
        self, countries: List[str], start: str, end: str, freq: str
    ) -> pd.DataFrame:
        raise NotImplementedError()

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        raise NotImplementedError()

    def standardize(self, df: pd.DataFrame) -> pd.DataFrame:
        raise NotImplementedError()

    def direction(self) -> str:
        return "up"
