"""Loader for canonical provider->series -> repo indicator mappings.

Supports CSV mapping (simple example in data/series_mapping.csv). Exposes
`load_series_mapping(path)` which returns a dict keyed by (provider,resource,series_code)
and `lookup_indicator(mapping, provider, resource, series_code)` helper.
"""
import csv
from typing import Dict, Tuple, Optional, Any


def load_series_mapping(path: str) -> Dict[Tuple[str, str, str], Dict[str, Any]]:
    mapping: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for r in reader:
                provider = (r.get("provider") or "").strip()
                resource = (r.get("resource") or "").strip()
                series = (r.get("series_code") or "").strip()
                indicator = (r.get("indicator_id") or "").strip()
                if not provider or not series:
                    continue
                mapping[(provider, resource, series)] = {"indicator_id": indicator, "notes": r.get("notes")}
    except Exception:
        # return empty mapping on error to avoid breaking pipeline consumers
        return {}
    return mapping


def lookup_indicator(mapping: Dict[Tuple[str, str, str], Dict[str, Any]], provider: str, resource: str, series_code: str) -> Optional[str]:
    key = (provider, resource or "", series_code)
    res = mapping.get(key)
    if res:
        return res.get("indicator_id")
    # try fallback: resource-agnostic key (provider, '', series)
    res2 = mapping.get((provider, "", series_code))
    if res2:
        return res2.get("indicator_id")
    return None
