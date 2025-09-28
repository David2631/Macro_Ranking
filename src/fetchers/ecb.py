"""ECB SDMX fetcher (MVP).

Uses pandasdmx when available. Returns an empty DataFrame and a fetch_log
when pandasdmx is not installed so tests can run in CI without network.
"""
import logging
from typing import List, Dict
import pandas as pd
from .base import AbstractFetcher


class ECBFetcher(AbstractFetcher):
    source = "ECB"

    def fetch(
        self,
        countries: List[str],
        indicators: List[Dict],
        start: str,
        end: str,
        freq: str,
    ):
        logger = logging.getLogger(__name__)
        fetch_logs: list[dict] = []
        # Try pandasdmx first
        try:
            import pandasdmx as sdmx
        except Exception:
            logger.warning(
                "pandasdmx not available, ECB fetcher returns empty DataFrame"
            )
            return (
                pd.DataFrame(
                    columns=["source", "indicator", "country", "date", "value"]
                ),
                fetch_logs,
            )

        rows: list[dict] = []
        client = sdmx.Request("ECB")
        from src.io.artifacts import sha256_of_records
        from datetime import datetime, timezone

        for ind in indicators:
            try:
                res = client.data(
                    resource_id=ind.get("code"),
                    startPeriod=start[:4],
                    endPeriod=end[:4],
                )
                pulled = []
                if res and getattr(res, "data", None):
                    for series in res.data.series:
                        country = None
                        try:
                            country = series.dimensions.get(
                                "REF_AREA"
                            ) or series.dimensions.get("COUNTRY")
                        except Exception:
                            country = None
                        for obs in series.observations:
                            try:
                                rec = {
                                    "source": "ECB",
                                    "indicator": ind["id"],
                                    "country": country,
                                    "date": obs.index,
                                    "value": obs.value,
                                }
                                rows.append(rec)
                                pulled.append(rec)
                            except Exception:
                                continue
                try:
                    sha_raw = None
                    try:
                        import json
                        import hashlib

                        canonical = json.dumps(
                            pulled,
                            sort_keys=True,
                            separators=(",", ":"),
                            ensure_ascii=False,
                        ).encode("utf-8")
                        sha_raw = hashlib.sha256(canonical).hexdigest()
                    except Exception:
                        sha_raw = None
                    sha_norm = sha256_of_records(pulled)
                    fetch_logs.append(
                        {
                            "request_url": f"sdmx://ECB/{ind.get('code')}",
                            "params": {
                                "code": ind.get("code"),
                                "start": start,
                                "end": end,
                            },
                            "http_status": 200,
                            "response_time_ms": 0,
                            "rows": len(pulled),
                            "sha256_raw": sha_raw,
                            "sha256_normalized": sha_norm,
                            "fetch_timestamp": datetime.now(timezone.utc).isoformat(),
                            "indicator": ind.get("id"),
                            "country": pulled[0].get("country") if pulled else None,
                            "api_meta": {"resource": ind.get("code")},
                            "no_backfill": False,
                        }
                    )
                except Exception:
                    # continue gracefully on metadata/hash failures
                    pass
            except Exception as e:
                logger.debug(f"ECB fetch error for {ind}: {e}")
                from datetime import datetime, timezone
                fetch_logs.append(
                    {
                        "request_url": f"sdmx://ECB/{ind.get('code')}",
                        "params": {"code": ind.get("code"), "start": start, "end": end},
                        "http_status": None,
                        "response_time_ms": 0,
                        "rows": 0,
                        "sha256_raw": None,
                        "sha256_normalized": None,
                        "fetch_timestamp": datetime.now(timezone.utc).isoformat(),
                        "indicator": ind.get("id"),
                        "country": None,
                        "api_meta": {"resource": ind.get('code')},
                        "no_backfill": False,
                        "error": str(e),
                    }
                )

        if not rows:
            return (
                pd.DataFrame(
                    columns=["source", "indicator", "country", "date", "value"]
                ),
                fetch_logs,
            )
        return pd.DataFrame(rows), fetch_logs
