Examples — Makro Ranking System

This document contains runnable examples and common CLI usage.

1) Quick fixture-only run (deterministic)

This uses local SDMX JSON fixtures (place under `tests/fixtures/sdmx/<source>/`):

```powershell
python -m scripts.ci_fixture_run example-configs/example-config-s1.yaml
```

Output will be written to `output/` and manifests to `data/_artifacts/`.

2) Full network run with example config S2

```powershell
python -m src.main --config example-configs/example-config-s2.yaml
```

3) Override countries from CLI

```powershell
python -m src.main --config example-configs/example-config-s2.yaml --countries DEU,FRA,ESP
```

4) Run a one-off debug with higher concurrency and debug logs

```powershell
python -m src.main --config example-configs/example-config-s3.yaml --debug
```

5) Produce allocations CSV only

The pipeline always writes `output/allocations.csv` when allocations can be computed. You can read it into your portfolio system or map country codes with `docs/portfolio_mapping_template.csv`.

6) Adding SDMX fixtures for CI

- Put `{indicator}_{country}.json` under `tests/fixtures/sdmx/wb/` (or other source folder). The fixture JSON should be a list of objects with `date` and `value` keys.
- The CI workflow collects `tests/fixtures/sdmx` into artifacts and the `fixtures` job will copy them back into the repo before running the fixture-only pipeline.

7) Signed manifests (optional)

Set the `MANIFEST_SIGNING_KEY` secret in CI. When present the pipeline will compute a deterministic HMAC-SHA256 signature over a stable payload and add `manifest_signature` to the manifest; it will also set `run_id` to the signature.


If you want, I can also add example Jupyter notebooks demonstrating how to load the Excel output and perform simple portfolio analysis; tell me which format you prefer (notebook or plain script).