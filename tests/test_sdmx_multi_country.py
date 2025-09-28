import json
import pandas as pd
import pytest
import sys
import types
from types import SimpleNamespace


def _load_fixture(name):
    p = f"tests/fixtures/{name}.json"
    with open(p, "r", encoding="utf-8") as f:
        return json.load(f)


class FakeSeries:
    def __init__(self, country, observations):
        self.dimensions = {"REF_AREA": country}
        self.observations = [SimpleNamespace(index=d, value=v) for d, v in observations]


class FakeResponse:
    def __init__(self, series_list):
        self.data = SimpleNamespace(series=series_list)


class FakeClient:
    def __init__(self, fixture):
        self.fixture = fixture

    def data(self, resource_id=None, key=None, startPeriod=None, endPeriod=None):
        series_list = []
        for s in self.fixture.get("series", []):
            series_list.append(FakeSeries(s.get("country"), s.get("observations")))
        return FakeResponse(series_list)


@pytest.fixture(autouse=True)
def inject_fake_pandasdmx():
    def fake_request(source):
        if source == "IMF":
            data = _load_fixture("imf_multi")
        elif source == "OECD":
            data = _load_fixture("oecd_multi")
        else:
            data = _load_fixture("ecb_multi")
        return FakeClient(data)

    fake_mod = types.ModuleType("pandasdmx")
    setattr(fake_mod, "Request", fake_request)
    sys.modules["pandasdmx"] = fake_mod
    yield
    try:
        del sys.modules["pandasdmx"]
    except Exception:
        pass


from src.fetchers.imf import IMFFetcher  # noqa: E402
from src.fetchers.oecd import OECDFetcher  # noqa: E402
from src.fetchers.ecb import ECBFetcher  # noqa: E402


def test_imf_multi():
    f = IMFFetcher({})
    df, logs = f.fetch(["DEU", "FRA"], [{"id": "i", "code": "c"}], "2019-01-01", "2024-01-01", "A")
    assert isinstance(df, pd.DataFrame)
    # should have rows for DEU and FRA
    assert set(df["country"]) >= {"DEU", "FRA"}
    assert isinstance(logs, list)


def test_oecd_multi():
    f = OECDFetcher({})
    df, logs = f.fetch(["DEU", "ITA"], [{"id": "o", "code": "c"}], "2019-01-01", "2022-01-01", "A")
    assert isinstance(df, pd.DataFrame)
    assert set(df["country"]) >= {"DEU", "ITA"}
    assert isinstance(logs, list)


def test_ecb_multi():
    f = ECBFetcher({})
    df, logs = f.fetch(["DEU", "ESP"], [{"id": "e", "code": "c"}], "2018-01-01", "2021-01-01", "A")
    assert isinstance(df, pd.DataFrame)
    assert set(df["country"]) >= {"DEU", "ESP"}
    assert isinstance(logs, list)
