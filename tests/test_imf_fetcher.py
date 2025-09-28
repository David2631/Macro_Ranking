import types
from unittest.mock import patch
import pandas as pd


def make_fake_series():
    # create a minimal fake response object similar to pandasdmx.data.Response
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

    # Build a single series with two observations
    s = SeriesObj({'REF_AREA': 'ABC'}, [Obs('2000', 1.23), Obs('2001', 2.34)])
    r = types.SimpleNamespace(data=Data([s]))
    return r


def test_imf_fetcher_basic(tmp_path, monkeypatch):
    # Ensure cache is isolated to tmpdir
    monkeypatch.chdir(tmp_path)

    # Mock pandasdmx.Request to return an object with data.series
    fake = types.SimpleNamespace()
    fake.data = make_fake_series().data

    class FakeClient:
        def data(self, resource_id=None, key=None, startPeriod=None, endPeriod=None):
            return fake

    with patch.dict('sys.modules'):
        # create a fake pandasdmx module with Request
        import sys

        mod = types.SimpleNamespace(Request=lambda source: FakeClient())
        sys.modules['pandasdmx'] = mod

        from src.fetchers.imf import IMFFetcher

        f = IMFFetcher()
        df, logs = f.fetch(countries=['ABC'], indicators=[{'id':'GDP','code':'ABC.GDP.1'}], start='2000-01-01', end='2001-12-31', freq='A')

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
        assert any(entry.get('indicator') == 'GDP' for entry in logs)

        # second call should hit cache (no exception) and return same shape
        df2, logs2 = f.fetch(countries=['ABC'], indicators=[{'id':'GDP','code':'ABC.GDP.1'}], start='2000-01-01', end='2001-12-31', freq='A')
        assert len(df2) == 2
