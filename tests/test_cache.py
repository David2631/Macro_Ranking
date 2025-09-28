import os
from src.io import cache


def test_cache_set_and_get(tmp_path):
    key = "testkey"
    data = [{"a": 1}, {"a": 2}]
    # ensure cache dir is local
    old = os.getcwd()
    os.chdir(str(tmp_path))
    try:
        cache.cache_set(key, data, ttl_hours=1)
        res = cache.cache_get(key, ttl_hours=1)
        assert isinstance(res, dict)
        assert "records" in res
    finally:
        os.chdir(old)
