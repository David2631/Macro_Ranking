import requests  # type: ignore
import pandas as pd
from .base import AbstractFetcher
import logging
import hashlib
import time as _time

try:
    from tenacity import retry  # type: ignore
except Exception:
    retry = None  # type: ignore
from typing import List, Dict, Optional
from src.io.artifacts import sha256_of_records

WB_BASE = "https://api.worldbank.org/v2"


class WorldBankFetcher(AbstractFetcher):
    source = "WB"

    def __init__(self, runtime_config: Optional[Dict] = None):
        super().__init__(runtime_config)
        self.timeout = self.runtime.get("request_timeout_sec", 20)
        self.retry_max = self.runtime.get("retry_max", 3)

    def _get(self, url, params):
        # If tenacity is available it would decorate this; otherwise simple retry
        attempts = 0
        while True:
            try:
                start = _time.time()
                r = requests.get(url, params=params, timeout=self.timeout)
                elapsed = (_time.time() - start) * 1000.0
                r.raise_for_status()
                # compute sha256 of raw content
                try:
                    raw = r.content if hasattr(r, "content") else r.text.encode("utf-8")
                    sha: Optional[str] = hashlib.sha256(raw).hexdigest()
                except Exception:
                    sha = None
                http_meta = {
                    "url": r.url,
                    "params": params,
                    "status_code": r.status_code,
                    "response_time_ms": int(elapsed),
                    "headers": dict(r.headers or {}),
                    "sha256_raw": sha,
                }
                return r, http_meta
            except Exception:
                attempts += 1
                if attempts >= (self.retry_max or 3):
                    raise
                sleep = min(10, 2**attempts)
                _time.sleep(sleep)

    def fetch(
        self,
        countries: List[str],
        indicators: List[Dict],
        start: str,
        end: str,
        freq: str,
    ) -> pd.DataFrame:
        rows: list[dict] = []
        logger = logging.getLogger(__name__)
        fetch_logs: list[dict] = []
        # defensive: if indicators is None or empty, return empty structures
        if not indicators:
            return (
                pd.DataFrame(
                    columns=["source", "indicator", "country", "date", "value"]
                ),
                fetch_logs,
            )
        for ind in indicators:
            code = ind["code"]
            for country in countries:
                page = 1
                while True:
                    url = f"{WB_BASE}/country/{country}/indicator/{code}"
                    # World Bank API expects year ranges for many series (e.g. 2020:2021)
                    if start and "-" in start:
                        date_param = f"{start[:4]}:{end[:4]}"
                    else:
                        date_param = f"{start}:{end}"
                    params = {
                        "format": "json",
                        "date": date_param,
                        "per_page": 100,
                        "page": page,
                    }
                    try:
                        r, meta = self._get(url, params)
                    except Exception as e:
                        # log and append a canonical error entry then break
                        logger.warning(f"WB fetch error for {country}/{code}: {e}")
                        from datetime import datetime, timezone

                        fetch_logs.append(
                            {
                                "request_url": url,
                                "params": params,
                                "http_status": None,
                                "response_time_ms": 0,
                                "rows": 0,
                                "sha256_raw": None,
                                "sha256_normalized": None,
                                "fetch_timestamp": datetime.now(
                                    timezone.utc
                                ).isoformat(),
                                "indicator": ind.get("id"),
                                "country": country,
                                "api_meta": None,
                                "no_backfill": False,
                                "error": str(e),
                            }
                        )
                        break
                    try:
                        data = r.json()
                    except Exception:
                        data = None
                    # attach rows count if possible
                    rows_count = 0
                    # defensive: data may be a list-like but with None in place of records
                    if not data or len(data) < 2:
                        # still record the fetch attempt
                        meta.update({"rows": 0})
                        fetch_logs.append(meta)
                        break
                    api_meta, records = data[0], data[1]
                    if records is None or not isinstance(records, list):
                        # no records returned for this page
                        meta.update({"rows": 0})
                        fetch_logs.append(meta)
                        break
                    for rec in records:
                        if rec.get("value") is None:
                            continue
                        rows.append(
                            {
                                "source": "WB",
                                "indicator": ind["id"],
                                "country": country,
                                "date": rec.get("date"),
                                "value": rec.get("value"),
                            }
                        )
                    rows_count = len(records)
                    # record fetch metadata including rows returned and any header fields
                    try:
                        # build records for this response to compute normalized hash
                        recs_for_hash = []
                        for rec in records:
                            if rec.get("value") is None:
                                continue
                            recs_for_hash.append(
                                {
                                    "indicator": ind["id"],
                                    "country": country,
                                    "date": rec.get("date"),
                                    "value": rec.get("value"),
                                }
                            )
                        sha_norm = sha256_of_records(recs_for_hash)
                        meta_entry = {
                            "request_url": r.url,
                            "params": params,
                            "http_status": r.status_code,
                            "response_time_ms": (
                                meta.get("response_time_ms")
                                if isinstance(meta, dict)
                                else None
                            ),
                            "rows": rows_count,
                            "etag": r.headers.get("ETag"),
                            "last_modified": r.headers.get("Last-Modified"),
                            "sha256_raw": (
                                meta.get("sha256_raw")
                                if isinstance(meta, dict)
                                else None
                            ),
                            "sha256_normalized": sha_norm,
                            "fetch_timestamp": datetime.now(timezone.utc).isoformat(),
                            "api_meta": api_meta,
                            "indicator": ind["id"],
                            "country": country,
                            "no_backfill": False,
                        }
                    except Exception:
                        meta_entry = {
                            "url": url,
                            "params": params,
                            "rows": rows_count,
                            "api_meta": api_meta,
                        }
                    fetch_logs.append(meta_entry)
                    # pagination
                    total = int(api_meta.get("total", 0))
                    per_page = int(api_meta.get("per_page", 100))
                    if page * per_page >= total:
                        break
                    page += 1
                    _time.sleep(0.1)
        if not rows:
            return (
                pd.DataFrame(
                    columns=["source", "indicator", "country", "date", "value"]
                ),
                fetch_logs,
            )
        df = pd.DataFrame(rows)
        return df, fetch_logs
