import types
from unittest.mock import patch
import pandas as pd


def make_fake_series():
    class Obs:
        def __init__(self, index, value):
            self.index = index
            self.value = value

    class SeriesObj:
        def __init__(self, dims, observations):
            self.dimensions = dims
            self.observations = observations

    class Data:
        def __init__(self, series_list):
            self.series = series_list

    s = SeriesObj({"REF_AREA": "XYZ"}, [Obs("2010", 10), Obs("2011", 11)])
    r = types.SimpleNamespace(data=Data([s]))
    return r


def test_oecd_fetcher_basic(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    fake = types.SimpleNamespace()
    fake.data = make_fake_series().data

    class FakeClient:
        def data(self, resource_id=None, key=None, startPeriod=None, endPeriod=None):
            return fake

    with patch.dict("sys.modules"):
        import sys

        sys.modules["pandasdmx"] = types.SimpleNamespace(Request=lambda _: FakeClient())

        from src.fetchers.oecd import OECDFetcher

        f = OECDFetcher()
        df, logs = f.fetch(
            countries=["XYZ"],
            indicators=[{"id": "IND1", "code": "XYZ.IND1", "resource": "MEI"}],
            start="2010-01-01",
            end="2011-12-31",
            freq="A",
        )

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
        assert any(entry.get("indicator") == "IND1" for entry in logs)

        # Second call should use cache
        df2, logs2 = f.fetch(
            countries=["XYZ"],
            indicators=[{"id": "IND1", "code": "XYZ.IND1", "resource": "MEI"}],
            start="2010-01-01",
            end="2011-12-31",
            freq="A",
        )
        assert len(df2) == 2
