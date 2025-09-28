import os
import json
import time
from src.io import cache


def test_cache_series_as_of_computed(tmp_path):
    old = os.getcwd()
    os.chdir(str(tmp_path))
    try:
        key = "k1"
        payload = {
            "records": [],
            "fetch_logs": [
                {
                    "indicator": "gdp",
                    "country": "DEU",
                    "fetch_timestamp": "2020-01-02T00:00:00Z",
                }
            ],
        }
        path = os.path.join(cache.CACHE_DIR, f"{key}.json")
        os.makedirs(cache.CACHE_DIR, exist_ok=True)
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(payload, fh)
        res = cache.cache_get(key, ttl_hours=1)
        assert isinstance(res, dict)
        assert "series_as_of" in res
    finally:
        os.chdir(old)


def test_cache_ttl_expired_removes_file(tmp_path):
    old = os.getcwd()
    os.chdir(str(tmp_path))
    try:
        key = "k2"
        data = [{"a": 1}]
        os.makedirs(cache.CACHE_DIR, exist_ok=True)
        path = os.path.join(cache.CACHE_DIR, f"{key}.json")
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(data, fh)
        # set mtime to 2 days ago
        old_mtime = time.time() - 2 * 24 * 3600
        os.utime(path, (old_mtime, old_mtime))
        res = cache.cache_get(key, ttl_hours=1)
        assert res is None
        # file should have been removed or ignored
        assert not os.path.exists(path)
    finally:
        os.chdir(old)
