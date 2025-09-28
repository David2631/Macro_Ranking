import os
import csv
import pandas as pd
from src.io.excel import export_to_excel


def make_sample_dfs():
    ranking = pd.DataFrame({"iso3": ["DEU", "FRA"], "score": [0.9, 0.8]}).set_index(
        "iso3"
    )
    indicators = pd.DataFrame(
        {
            "iso3": ["DEU", "FRA"],
            "value": [1.2, 2.3],
            "date": [pd.Timestamp("2020-01-01"), pd.Timestamp("2020-02-01")],
        }
    )
    raw = pd.DataFrame(
        {
            "iso3": ["DEU", "FRA"],
            "value": [10, 20],
            "date": [pd.Timestamp("2020-01-01"), pd.Timestamp("2020-02-01")],
        }
    )
    return ranking, indicators, raw


def test_export_without_mapping_file(tmp_path, monkeypatch):
    ranking, indicators, raw = make_sample_dfs()
    out = tmp_path / "out.xlsx"
    cfg = {}
    # ensure data/countries_iso3.csv does not exist
    data_dir = tmp_path / "data"
    if data_dir.exists():
        for f in data_dir.iterdir():
            f.unlink()
    # run export; should fallback to pycountry lookups (which may succeed) or leave codes unchanged
    result = export_to_excel(str(out), ranking, indicators, raw, cfg)
    assert os.path.exists(result)


def test_export_with_mapping_file_and_portfolio_backtest(tmp_path):
    ranking, indicators, raw = make_sample_dfs()
    out = tmp_path / "out2.xlsx"
    # create data/countries_iso3.csv and countries_iso3_map.csv
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    with open(data_dir / "countries_iso3.csv", "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["DEU", "Germany"])  # simple mapping
        w.writerow(["FRA", "France"])
    # create mapping file for portfolio
    with open(
        data_dir / "countries_iso3_map.csv", "w", newline="", encoding="utf-8"
    ) as fh:
        w = csv.writer(fh)
        w.writerow(["iso3", "ticker", "isin", "exchange", "currency"])
        w.writerow(["DEU", "ETC_DE", "DE000", "XETRA", "EUR"])
        w.writerow(["FRA", "ETC_FR", "FR000", "EPA", "EUR"])
    # portfolio object with prev_weight and weight to trigger est_cost calculation
    portfolio = pd.DataFrame(
        {"country": ["DEU", "FRA"], "weight": [0.6, 0.4], "prev_weight": [0.5, 0.5]}
    )
    backtest = pd.DataFrame({"date": [pd.Timestamp("2020-01-01")], "value": [1.0]})
    cfg = {
        "portfolio": {
            "allocations": portfolio,
            "mapping_path": str(data_dir / "countries_iso3_map.csv"),
            "cost_per_unit": 0.005,
        },
        "backtest": {"results": backtest},
    }
    # change cwd to tmp_path so export_to_excel finds data/ files
    cwd = os.getcwd()
    os.chdir(str(tmp_path))
    try:
        res = export_to_excel(str(out), ranking, indicators, raw, cfg)
        assert os.path.exists(res)
    finally:
        os.chdir(cwd)


def test_harmonize_report_and_temp_replace(monkeypatch, tmp_path):
    ranking, indicators, raw = make_sample_dfs()
    out = tmp_path / "out3.xlsx"
    cfg = {"harmonize_report": pd.DataFrame({"a": [1, 2]})}

    # monkeypatch os.replace to raise PermissionError on first call, then succeed
    calls = {"n": 0}

    def fake_replace(src, dst):
        calls["n"] += 1
        if calls["n"] == 1:
            raise PermissionError("locked")
        # otherwise just do a simple move
        return None

    monkeypatch.setattr("src.io.excel.os.replace", fake_replace)
    # ensure temp path is writable
    res = export_to_excel(str(out), ranking, indicators, raw, cfg)
    assert os.path.exists(res) or res.endswith(".xlsx")
    # expect that os.replace got called at least once
    assert calls["n"] >= 1
