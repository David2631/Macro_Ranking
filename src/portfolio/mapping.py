import csv
from typing import Dict, Optional


def load_country_mapping(path: str) -> Dict[str, Dict[str, str]]:
    """Load a simple CSV mapping file with columns: iso3,ticker,isin,exchange,currency

    Returns dict keyed by ISO3 uppercased to a dict of provided fields.
    """
    out = {}
    try:
        with open(path, newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                code = (row.get("iso3") or row.get("ISO3") or row.get("code") or "").strip().upper()
                if not code:
                    continue
                out[code] = {k: v for k, v in row.items() if v is not None}
    except FileNotFoundError:
        return {}
    except Exception:
        return {}
    return out


def get_mapping_for_country(mapping: Dict[str, Dict[str, str]], iso3: str) -> Optional[Dict[str, str]]:
    if not iso3:
        return None
    return mapping.get(str(iso3).upper())
