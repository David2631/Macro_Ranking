import sys
import os
import types
import json
import pytest
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


@pytest.fixture
def inject_fake_pandasdmx():
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
    try:
        del sys.modules["pandasdmx"]
    except Exception:
        pass


def test_sdmx_pipeline_end_to_end(tmp_path, inject_fake_pandasdmx, monkeypatch):
    # Run the main pipeline with the tmp SDMX-only config; ensures no network calls
    from src.main import main

    cfg = os.path.abspath("tmp_config_sdmx.yaml")
    # ensure output folders
    outdir = os.path.join(os.getcwd(), "output")
    os.makedirs(outdir, exist_ok=True)

    # run main with the config; it should write a manifest and the excel/csv outputs
    main(["--config", cfg])

    # find the latest manifest in data/_artifacts
    artdir = os.path.join(os.getcwd(), "data", "_artifacts")
    manifests = [os.path.join(artdir, p) for p in os.listdir(artdir) if p.endswith('.json')]
    assert manifests, "No manifest files written"
    # pick the most recently modified manifest
    mpath = max(manifests, key=os.path.getmtime)
    with open(mpath, "r", encoding="utf-8") as fh:
        m = json.load(fh)
    # manifest should include series_as_of and outputs
    assert "series_as_of" in m
    assert "outputs" in m
    # outputs should reference the excel we configured
    assert m["outputs"].get("excel") is not None
