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

    s = SeriesObj({"REF_AREA": "EMU"}, [Obs("2015", 100), Obs("2016", 110)])
    r = types.SimpleNamespace(data=Data([s]))
    return r


def test_ecb_fetcher_basic(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    fake = types.SimpleNamespace()
    fake.data = make_fake_series().data

    class FakeClient:
        def data(self, resource_id=None, key=None, startPeriod=None, endPeriod=None):
            return fake

    with patch.dict("sys.modules"):
        import sys

        sys.modules["pandasdmx"] = types.SimpleNamespace(Request=lambda _: FakeClient())

        from src.fetchers.ecb import ECBFetcher

        f = ECBFetcher()
        df, logs = f.fetch(
            countries=["EMU"],
            indicators=[{"id": "EXR", "code": "EMU.EXR.1", "resource": "EXR"}],
            start="2015-01-01",
            end="2016-12-31",
            freq="A",
        )

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
        assert any(entry.get("indicator") == "EXR" for entry in logs)

        # cache hit on second call
        df2, logs2 = f.fetch(
            countries=["EMU"],
            indicators=[{"id": "EXR", "code": "EMU.EXR.1", "resource": "EXR"}],
            start="2015-01-01",
            end="2016-12-31",
            freq="A",
        )
        assert len(df2) == 2
