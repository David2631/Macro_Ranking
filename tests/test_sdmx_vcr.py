import json
import types
from unittest.mock import patch


def _make_response_from_fixture(fixture_path):
    # Resolve fixture path relative to this test file so tests work when cwd changes
    import os
    base = os.path.dirname(__file__)
    # Try several candidate locations and pick the first that exists.
    candidates = []
    if os.path.isabs(fixture_path):
        candidates.append(fixture_path)
    else:
        # as given (relative to cwd)
        candidates.append(fixture_path)
        # relative to this test file
        candidates.append(os.path.join(base, fixture_path))
        # if given as tests/... strip leading 'tests/' or 'tests\\' then join
        rel = fixture_path
        if rel.startswith('tests/') or rel.startswith('tests\\'):
            try:
                rel2 = rel.split('/', 1)[1]
            except Exception:
                rel2 = rel.split('\\', 1)[1]
            candidates.append(os.path.join(base, rel2))

    full = None
    for c in candidates:
        if os.path.exists(c):
            full = c
            break
    if full is None:
        # fallback to joining base and fixture_path (will raise the original error)
        full = os.path.join(base, fixture_path)
    with open(full, 'r', encoding='utf-8') as f:
        payload = json.load(f)

    # Build an object with .data.series where each series has .dimensions and .observations
    class Obs:
        def __init__(self, index, value):
            self.index = index
            self.value = value

    class SeriesObj:
        def __init__(self, dims, observations):
            self.dimensions = dims
            # create obs objects with index/value
            self.observations = [Obs(o[0], o[1]) for o in observations]

    class Data:
        def __init__(self, series_list):
            self.series = series_list

    series_list = []
    for s in payload.get('series', []):
        dims = s.get('dimensions', {})
        obs = s.get('observations', [])
        series_list.append(SeriesObj(dims, obs))

    return types.SimpleNamespace(data=Data(series_list))


def test_sdmx_vcr_imf_oecd_ecb(tmp_path, monkeypatch):
    import sys
    import os
    # ensure project root is importable even after we change cwd
    base = os.path.dirname(__file__)
    project_root = os.path.dirname(base)
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

    # ensure tests run in isolated tmp dir
    monkeypatch.chdir(tmp_path)

    imf_resp = _make_response_from_fixture('tests/fixtures/imf_fixture.json')
    oecd_resp = _make_response_from_fixture('tests/fixtures/oecd_fixture.json')
    ecb_resp = _make_response_from_fixture('tests/fixtures/ecb_fixture.json')

    class FakeIMF:
        def data(self, **kwargs):
            return imf_resp

    class FakeOECD:
        def data(self, **kwargs):
            return oecd_resp

    class FakeECB:
        def data(self, **kwargs):
            return ecb_resp

    with patch.dict('sys.modules'):
        import sys
        sys.modules['pandasdmx'] = types.SimpleNamespace(Request=lambda source: FakeIMF() if source=='IMF' else (FakeOECD() if source=='OECD' else FakeECB()))

        # import fetchers and call them to ensure they can process fixture payloads
        from src.fetchers.imf import IMFFetcher
        from src.fetchers.oecd import OECDFetcher
        from src.fetchers.ecb import ECBFetcher

        imf = IMFFetcher()
        df_i, logs_i = imf.fetch(countries=['ABC'], indicators=[{'id':'GDP','code':'ABC.GDP.1'}], start='2000-01-01', end='2001-12-31', freq='A')
        assert len(df_i) == 2

        oecd = OECDFetcher()
        df_o, logs_o = oecd.fetch(countries=['XYZ'], indicators=[{'id':'IND1','code':'XYZ.IND1','resource':'MEI'}], start='2010-01-01', end='2011-12-31', freq='A')
        assert len(df_o) == 2

        ecb = ECBFetcher()
        df_e, logs_e = ecb.fetch(countries=['EMU'], indicators=[{'id':'EXR','code':'EMU.EXR.1','resource':'EXR'}], start='2015-01-01', end='2016-12-31', freq='A')
        assert len(df_e) == 2
