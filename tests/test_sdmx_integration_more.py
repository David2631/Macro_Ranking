import json
import pandas as pd
import pytest
import sys
import types
from types import SimpleNamespace

# reuse fixture loader


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
        # rotate fixtures for sources
        if source == "IMF":
            data = _load_fixture("imf_sample")
        elif source == "OECD":
            data = _load_fixture("oecd_sample")
        else:
            data = _load_fixture("ecb_sample")
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


def test_sdmx_fetchers_multi_country():
    # Each fetcher should return a DataFrame and a fetch log list
    f_imf = IMFFetcher({})
    df_imf, logs_imf = f_imf.fetch(
        ["DEU"], [{"id": "x", "code": "c"}], "2020-01-01", "2024-01-01", "A"
    )
    assert isinstance(df_imf, pd.DataFrame)
    assert isinstance(logs_imf, list)
    # logs should have normalized sha and timestamp
    if logs_imf:
        for ent in logs_imf:
            assert "sha256_normalized" in ent
            assert "fetch_timestamp" in ent

    f_oecd = OECDFetcher({})
    df_oecd, logs_oecd = f_oecd.fetch(
        ["DEU"], [{"id": "y", "code": "c2"}], "2020-01-01", "2024-01-01", "A"
    )
    assert isinstance(df_oecd, pd.DataFrame)
    assert isinstance(logs_oecd, list)
    if logs_oecd:
        for ent in logs_oecd:
            assert "sha256_normalized" in ent
            assert "fetch_timestamp" in ent

    f_ecb = ECBFetcher({})
    df_ecb, logs_ecb = f_ecb.fetch(
        ["DEU"], [{"id": "z", "code": "c3"}], "2020-01-01", "2024-01-01", "A"
    )
    assert isinstance(df_ecb, pd.DataFrame)
    assert isinstance(logs_ecb, list)
    if logs_ecb:
        for ent in logs_ecb:
            assert "sha256_normalized" in ent
            assert "fetch_timestamp" in ent
