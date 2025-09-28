import os
import json
from datetime import datetime, timedelta
import threading

CACHE_DIR = ".cache"


def ensure_cache_dir():
    if not os.path.exists(CACHE_DIR):
        os.makedirs(CACHE_DIR, exist_ok=True)


# simple lock to serialize cache writes (file system)
_cache_lock = threading.Lock()


from typing import Optional, Dict, Any


def cache_get(key: str, ttl_hours: int = 24) -> Optional[Dict[str, Any]]:
    ensure_cache_dir()
    path = os.path.join(CACHE_DIR, f"{key}.json")
    if not os.path.exists(path):
        return None
    stat = os.path.getmtime(path)
    mtime = datetime.fromtimestamp(stat)
    if datetime.now() - mtime > timedelta(hours=ttl_hours):
        try:
            os.remove(path)
        except Exception:
            pass
        return None
    try:
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
            # expected cache payload can be either a list of records (legacy) or
            # a dict with {'records': [...], 'fetch_logs': [...]}.
            if isinstance(data, dict) and "records" in data:
                # ensure fetch_logs key exists
                if "fetch_logs" not in data:
                    data["fetch_logs"] = []
                # ensure series_as_of exists (may be computed from fetch_logs)
                if "series_as_of" not in data:
                    try:
                        series_map: Dict[tuple[str, str], str] = {}
                        for entry in data.get("fetch_logs", []):
                            ind = entry.get("indicator")
                            c = entry.get("country")
                            ts = entry.get("fetch_timestamp")
                            if not ind or not c or not ts:
                                continue
                            keyt = (ind, c)
                            if keyt not in series_map or ts > series_map[keyt]:
                                series_map[keyt] = ts
                        data["series_as_of"] = {f"{k[0]}::{k[1]}": v for k, v in series_map.items()}
                    except Exception:
                        data["series_as_of"] = {}
                return data
            else:
                return {"records": data, "fetch_logs": []}
    except Exception:
        return None


def cache_set(key: str, data: Any, ttl_hours: int = 24) -> None:
    ensure_cache_dir()
    path = os.path.join(CACHE_DIR, f"{key}.json")
    try:
        # Accept either a DataFrame-like records list, or a dict with 'records'
        payload = data
        if isinstance(data, list):
            payload = {"records": data}
        # Ensure fetch_logs exists for consumers
        if isinstance(payload, dict) and "fetch_logs" not in payload:
            payload["fetch_logs"] = []
        # If fetch_logs exist, compute per-series as_of mapping for convenience
        if isinstance(payload, dict) and payload.get("fetch_logs"):
            try:
                series_map: Dict[tuple[str, str], str] = {}
                for f in payload.get("fetch_logs", []):
                    ind = f.get("indicator")
                    c = f.get("country")
                    ts = f.get("fetch_timestamp")
                    if not ind or not c or not ts:
                        continue
                    keyt = (ind, c)
                    if keyt not in series_map or ts > series_map[keyt]:
                        series_map[keyt] = ts
                payload["series_as_of"] = {
                    f"{k[0]}::{k[1]}": v for k, v in series_map.items()
                }
            except Exception:
                payload["series_as_of"] = {}
        # write atomically under lock to avoid concurrent writes
        with _cache_lock:
            with open(path, "w", encoding="utf-8") as fh:
                json.dump(payload, fh, default=str, ensure_ascii=False)
    except Exception:
        pass
