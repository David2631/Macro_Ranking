IMF Fetcher (SDMX) — Usage

Overview

The project includes an `IMFFetcher` that uses the SDMX REST API via `pandasdmx` to fetch IMF data (best-effort with the IFS dataset). The fetcher is wrapped by `IMFIndicator` and integrates with the pipeline so that per-request provenance (`fetch_logs`) is included in the output manifest.

Requirements

- pandasdmx (already present in `requirements.txt`)
- Internet access to query IMF SDMX endpoints

How to add an IMF indicator to `example-config.yaml`

Add an `indicators` entry where the `sources` array contains an object with `source: IMF` and the appropriate IMF series `code` (dataset key). Example:

indicators:
  - id: imf_gdp
    sources:
      - source: IMF
        code: "IFS/M..NGDP_RPOW"  # example key, adjust to real IMF series key
    good_direction: up
    transform: none

Notes

- IMF dataset keys can be tricky (often require dataset-specific keys). If a plain series code does not return data, try using the dataset/resource-specific key syntax or consult the IMF SDMX documentation.
- If `pandasdmx` is not installed or network access is unavailable, the fetcher will return an empty DataFrame and a warning will be logged. The pipeline is resilient to this.
- Each successful fetch produces `fetch_logs` entries in the manifest including a SHA256 of the canonical JSON of pulled records for provenance.

Troubleshooting

- If you get unexpected empty results, enable `--debug` when running the pipeline to see detailed HTTP/SDMX client logs.
- Consider testing a single IMF series using the interactive Python snippet below:

```python
from pathlib import Path
import sys
sys.path.insert(0, str(Path('.').resolve()))
from src.fetchers.imf import IMFFetcher
f = IMFFetcher({'request_timeout_sec': 20})
df, logs = f.fetch(['USA'], [{'id':'example','code':'IFS/...' }], '2015-01-01', '2025-01-01', 'A')
print(df.head())
print(logs)
```
