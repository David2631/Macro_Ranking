from abc import ABC, abstractmethod
from typing import List, Dict, Optional, Any, Tuple
import pandas as pd


class AbstractFetcher(ABC):
    """Abstract fetcher interface. Implementations must return a DataFrame with columns
    [source, indicator, country, date, value]
    """

    source: str

    def __init__(self, runtime_config: Optional[Dict[str, Any]] = None):
        self.runtime: Dict[str, Any] = runtime_config or {}

    @abstractmethod
    def fetch(
        self,
        countries: List[str],
        indicators: List[Dict],
        start: str,
        end: str,
        freq: str,
    ) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
        """Fetch returns a tuple (DataFrame, fetch_logs)

        Implementations should return (df, fetch_logs) where fetch_logs is a list
        of dicts with canonical fetch metadata.
        """
        pass
