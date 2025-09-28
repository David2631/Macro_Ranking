"""IMF fetcher using pandasdmx (SDMX REST). If pandasdmx is not available, returns empty df."""

import logging
from typing import List, Dict
import pandas as pd
from .base import AbstractFetcher
from src.io import cache as io_cache
from ._utils import simple_backoff_retry, cache_key_for_sdmx, time_ms


class IMFFetcher(AbstractFetcher):
    source = "IMF"

    def fetch(
        self,
        countries: List[str],
        indicators: List[Dict],
        start: str,
        end: str,
        freq: str,
    ):
        """Return (DataFrame, fetch_logs).

        Behaviour:
        - Uses pandasdmx when available; otherwise returns empty df and canonical empty fetch_logs.
        - Uses file cache (24h) to avoid repeated network calls. Cache key is stable based on source/resource/key/start/end.
        - Performs simple backoff retry for transient errors.
        """

        logger = logging.getLogger(__name__)
        fetch_logs: list[dict] = []
        rows: list[dict] = []

        # Try to import pandasdmx; if not present, return empty but well-formed logs
        try:
            import pandasdmx as sdmx
        except Exception:
            logger.debug("pandasdmx not available; IMF fetcher skipped")
            # create canonical empty fetch entries for each indicator
            from datetime import datetime, timezone

            for ind in indicators:
                fetch_logs.append(
                    {
                        "request_url": f"sdmx://IMF/IFS?series={ind.get('code')}",
                        "params": {"key": ind.get("code"), "start": start, "end": end},
                        "http_status": None,
                        "response_time_ms": 0,
                        "rows": 0,
                        "sha256_raw": None,
                        "sha256_normalized": None,
                        "fetch_timestamp": datetime.now(timezone.utc).isoformat(),
                        "indicator": ind.get("id"),
                        "country": None,
                        "api_meta": {"resource": "IFS"},
                        "no_backfill": False,
                        "error": "pandasdmx_missing",
                    }
                )
            return pd.DataFrame(columns=["source", "indicator", "country", "date", "value"]), fetch_logs

        # create client lazily with retry
        try:
            client = simple_backoff_retry(lambda: sdmx.Request("IMF"), attempts=2, base_delay=0.2)
        except Exception as e:
            logger.debug(f"pandasdmx client creation failed: {e}")
            from datetime import datetime, timezone

            for ind in indicators:
                fetch_logs.append(
                    {
                        "request_url": f"sdmx://IMF/IFS?series={ind.get('code')}",
                        "params": {"key": ind.get("code"), "start": start, "end": end},
                        "http_status": None,
                        "response_time_ms": 0,
                        "rows": 0,
                        "sha256_raw": None,
                        "sha256_normalized": None,
                        "fetch_timestamp": datetime.now(timezone.utc).isoformat(),
                        "indicator": ind.get("id"),
                        "country": None,
                        "api_meta": {"resource": "IFS"},
                        "no_backfill": False,
                        "error": "client_creation_failed",
                    }
                )
            return pd.DataFrame(columns=["source", "indicator", "country", "date", "value"]), fetch_logs

        # Process each indicator: check cache -> fetch -> emit logs
        from src.io.artifacts import sha256_of_records
        from datetime import datetime, timezone
        # try to load canonical series mapping (optional)
        try:
            from src.fetchers.mapping import load_series_mapping, lookup_indicator
            mapping = load_series_mapping("data/series_mapping.csv")
        except Exception:
            mapping = {}

        for ind in indicators:
            code = ind.get("code")
            key = cache_key_for_sdmx("IMF", "IFS", code or "", start[:4], end[:4])
            cached = io_cache.cache_get(key, ttl_hours=24)
            if cached:
                # cached is dict with 'records' and 'fetch_logs'
                recs = cached.get("records") or []
                rows.extend(recs)
                # merge fetch_logs (cached may contain provenance)
                fetch_logs.extend(cached.get("fetch_logs") or [])
                continue

            # not cached -> perform network fetch
            start_ms = time_ms()
            pulled = []
            try:
                def _do_fetch():
                    return client.data(resource_id="IFS", key=code, startPeriod=start[:4], endPeriod=end[:4])

                res = simple_backoff_retry(_do_fetch, attempts=3, base_delay=0.3)

                if res and getattr(res, "data", None):
                    for series in res.data.series:
                        country = None
                        try:
                            # series.dimensions may be a dict-like mapping
                            country = series.dimensions.get("REF_AREA") or series.dimensions.get("COUNTRY")
                        except Exception:
                            country = None
                        for obs in series.observations:
                            try:
                                val = obs.value
                                dt = obs.index
                            except Exception:
                                continue
                            rec = {"source": "IMF", "indicator": ind.get("id"), "country": country, "date": dt, "value": val}
                            rows.append(rec)
                            pulled.append(rec)

                response_time = time_ms() - start_ms
                sha_raw = None
                try:
                    import json as _json
                    import hashlib as _hashlib

                    canonical = _json.dumps(pulled, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
                    sha_raw = _hashlib.sha256(canonical).hexdigest()
                except Exception:
                    sha_raw = None

                try:
                    sha_norm = sha256_of_records(pulled)
                except Exception:
                    sha_norm = None

                # resolved indicator id via mapping if available
                mapped_ind = None
                try:
                    mapped_ind = lookup_indicator(mapping, 'IMF', 'IFS', str(code or "")) if mapping else None
                except Exception:
                    mapped_ind = None

                entry = {
                    "request_url": f"sdmx://IMF/IFS?series={code}",
                    "params": {"key": code, "start": start, "end": end},
                    "http_status": 200,
                    "response_time_ms": int(response_time),
                    "rows": len(pulled),
                    "sha256_raw": sha_raw,
                    "sha256_normalized": sha_norm,
                    "fetch_timestamp": datetime.now(timezone.utc).isoformat(),
                    "indicator": mapped_ind or ind.get("id"),
                    "country": pulled[0].get("country") if pulled else None,
                    "api_meta": {"resource": "IFS"},
                    "no_backfill": False,
                }
                fetch_logs.append(entry)

                # cache the pulled records and fetch_logs
                try:
                    io_cache.cache_set(key, {"records": pulled, "fetch_logs": [entry]}, ttl_hours=24)
                except Exception:
                    logger.debug("cache_set failed, continuing")

            except Exception as e:
                logger.debug(f"IMF fetch error for {ind}: {e}")
                response_time = time_ms() - start_ms
                fetch_logs.append(
                    {
                        "request_url": f"sdmx://IMF/IFS?series={code}",
                        "params": {"key": code, "start": start, "end": end},
                        "http_status": None,
                        "response_time_ms": int(response_time),
                        "rows": 0,
                        "sha256_raw": None,
                        "sha256_normalized": None,
                        "fetch_timestamp": datetime.now(timezone.utc).isoformat(),
                        "indicator": ind.get("id"),
                        "country": None,
                        "api_meta": {"resource": "IFS"},
                        "no_backfill": False,
                        "error": str(e),
                    }
                )

        if not rows:
            return pd.DataFrame(columns=["source", "indicator", "country", "date", "value"]), fetch_logs
        return pd.DataFrame(rows), fetch_logs
