import json
import pandas as pd
import pytest
from types import SimpleNamespace
import sys
import os
import types

# ensure project src is importable when pytest runs this file directly
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# This test provides offline fixtures for pandasdmx-like responses and verifies
# that IMF/OECD/ECB fetchers can process them without making live calls.

# fetchers will be imported after the fake pandasdmx injection below


def _load_fixture(name):
    p = f"tests/fixtures/{name}.json"
    with open(p, "r", encoding="utf-8") as f:
        return json.load(f)


class FakeSeries:
    def __init__(self, country, observations):
        self.dimensions = {"REF_AREA": country}
        # observations: list of (date, value)
        self.observations = [SimpleNamespace(index=d, value=v) for d, v in observations]


class FakeResponse:
    def __init__(self, series_list):
        self.data = SimpleNamespace(series=series_list)


class FakeClient:
    def __init__(self, fixture):
        self.fixture = fixture

    def data(self, resource_id=None, key=None, startPeriod=None, endPeriod=None):
        # translate fixture into FakeResponse
        series_list = []
        for s in self.fixture.get("series", []):
            series_list.append(FakeSeries(s.get("country"), s.get("observations")))
        return FakeResponse(series_list)


@pytest.fixture(autouse=True)
def inject_fake_pandasdmx():
    # Insert a minimal fake pandasdmx module into sys.modules to avoid importing
    # the real pandasdmx (which pulls pydantic and causes issues in test env).
    def fake_request(source):
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
    # cleanup
    try:
        del sys.modules["pandasdmx"]
    except Exception:
        pass


# import fetchers after fake module injection
from src.fetchers.imf import IMFFetcher  # noqa: E402
from src.fetchers.oecd import OECDFetcher  # noqa: E402
from src.fetchers.ecb import ECBFetcher  # noqa: E402


def test_imf_fetch_offline():
    f = IMFFetcher({})
    df, logs = f.fetch(
        ["DEU"], [{"id": "IND1", "code": "CODE1"}], "2020-01-01", "2024-01-01", "A"
    )
    assert isinstance(df, pd.DataFrame)
    assert isinstance(logs, list)


def test_oecd_fetch_offline():
    f = OECDFetcher({})
    df, logs = f.fetch(
        ["DEU"], [{"id": "IND2", "code": "CODE2"}], "2020-01-01", "2024-01-01", "A"
    )
    assert isinstance(df, pd.DataFrame)
    assert isinstance(logs, list)


def test_ecb_fetch_offline():
    f = ECBFetcher({})
    df, logs = f.fetch(
        ["DEU"], [{"id": "IND3", "code": "CODE3"}], "2020-01-01", "2024-01-01", "A"
    )
    assert isinstance(df, pd.DataFrame)
    assert isinstance(logs, list)
