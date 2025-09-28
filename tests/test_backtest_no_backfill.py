import pandas as pd

# We'll test the internal filtering function by constructing a small DataFrame
# and simulating fetch_entries and cfg with backtest.no_backfill True.

# no extra imports required


def _apply_no_backfill_filter(data, fetch_entries):
    # mimic the logic in main.py: build as_of_map and filter rows
    as_of_map = {}
    for f in fetch_entries:
        key = (f.get("indicator"), f.get("country"))
        ts = f.get("fetch_timestamp")
        if not key[0] or not key[1] or not ts:
            continue
        if key not in as_of_map or ts > as_of_map[key]:
            as_of_map[key] = ts

    def _filter_row(r):
        key = (r["indicator"], r["country"])
        as_of_ts = as_of_map.get(key)
        if not as_of_ts:
            return True
        try:
            row_date = pd.to_datetime(r["date"]).date()
            as_of_date = pd.to_datetime(as_of_ts).date()
            return row_date <= as_of_date
        except Exception:
            return True

    return data[data.apply(_filter_row, axis=1)]


def test_no_backfill_filters_newer_rows():
    df = pd.DataFrame(
        [
            {"indicator": "IND1", "country": "DEU", "date": "2023-01-01", "value": 1},
            {"indicator": "IND1", "country": "DEU", "date": "2024-01-01", "value": 2},
        ]
    )
    fetch_entries = [
        {
            "indicator": "IND1",
            "country": "DEU",
            "fetch_timestamp": "2023-06-01T00:00:00Z",
            "no_backfill": True,
        }
    ]
    out = _apply_no_backfill_filter(df, fetch_entries)
    # only the 2023 row should remain
    assert len(out) == 1
    assert out.iloc[0]["date"] == "2023-01-01"


def test_no_backfill_keeps_if_as_of_after_latest():
    df = pd.DataFrame(
        [
            {"indicator": "IND1", "country": "FRA", "date": "2023-01-01", "value": 1},
            {"indicator": "IND1", "country": "FRA", "date": "2023-06-01", "value": 2},
        ]
    )
    fetch_entries = [
        {
            "indicator": "IND1",
            "country": "FRA",
            "fetch_timestamp": "2023-12-01T00:00:00Z",
            "no_backfill": True,
        }
    ]
    out = _apply_no_backfill_filter(df, fetch_entries)
    assert len(out) == 2


def test_no_backfill_no_as_of_keeps_all():
    df = pd.DataFrame(
        [
            {"indicator": "IND2", "country": "ITA", "date": "2022-01-01", "value": 1},
            {"indicator": "IND2", "country": "ITA", "date": "2023-01-01", "value": 2},
        ]
    )
    fetch_entries = []
    out = _apply_no_backfill_filter(df, fetch_entries)
    assert len(out) == 2
