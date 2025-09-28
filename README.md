Makro-Ranking-System

Dieses Projekt implementiert ein Python-Tool, das frei zugängliche Makrodaten sammelt, Indikatoren berechnet und Länder nach einem gewichteten Composite-Score rankt.

Quickstart

1. Create a virtualenv (Python 3.10+ recommended) and install dependencies:

```powershell
python -m venv .venv; .\.venv\Scripts\Activate.ps1; pip install -r requirements.txt
```

2. Run the pipeline with the example config:

Developer notes — pre-commit
----------------------------

We use pre-commit to enforce formatting and basic checks locally. To run the hooks locally:

```powershell
python -m pip install --user pre-commit
python -m pre_commit install
python -m pre_commit run --all-files
```

If you modify dependencies, update `requirements-dev.txt` and run `pip install -r requirements-dev.txt`.

CHANGELOG
---------

Unreleased
- Added deterministic manifests with environment sidecar and manifest SHA provenance.
- Tightened CI: mypy and pip-audit are now blocking; pytest reports are uploaded as artifacts.

```powershell
python -m src.main --config ./example-configs/example-config-s1.yaml
```

Where outputs and manifests are written to `./output` and `./data/_artifacts` respectively.

Manifest and provenance
-----------------------
The pipeline writes a manifest JSON for each run. This contains per-fetch sha256 hashes (raw & normalized), an environment snapshot and `series_as_of` metadata used for no-backfill/backtest logic. See `docs/MANIFEST.md` for the format and verification steps.

Notes
- Caching is a simple JSON file cache in `.cache/` and preserves `fetch_logs` for provenance.
- Many SDMX fetchers include fixtures for offline tests; full SDMX integration is part of the sprint backlog.

What is included (implemented)
-------------------------------
- Deterministic manifests with canonical `sha256_normalized` and optional HMAC signing (env var `MANIFEST_SIGNING_KEY`).
- World Bank fetcher plugin and SDMX fixture mode (fixture files under `tests/fixtures/sdmx/wb/`).
- File-based cache with TTL and canonical fetch-log persistence.
- Frequency harmonization (A/M -> Q), transformations (yoy/qoq/diff), smoothing and standardization (zscore, robust_zscore, winsorized_zscore).
- Scoring pipeline with coverage checks, coverage-penalty, bootstrap confidence intervals and `rank_stability` diagnostics.
- Excel export with German number/date formats and multiple sheets: `Ranking`, `Indicators`, `Raw`, `Config`, optional `Portfolio` and `Backtest`, and `README`/method sheet.
- Backtest engine (rebalancing weights, synthetic price fallback) and allocation CSV export (`output/allocations.csv`).
- Fixture-driven CI helper script (`scripts/ci_fixture_run.py`) that patches the fetcher and replays SDMX fixtures for deterministic runs.
- GitHub Actions CI workflow that runs tests and a fixture-only pipeline job; artifact upload/download for debugging.
- Extensive unit and integration tests. Local test-suite: 45 passed (see `pytest` output locally).

Files and important paths
-------------------------
- Example configs: `example-configs/example-config-s1.yaml`, `...-s2.yaml`, `...-s3.yaml`
- Outputs: `output/*.xlsx`, `output/allocations.csv`
- Artifacts/manifests: `data/_artifacts/manifest_*.json`
- SDMX fixtures for CI/local: `tests/fixtures/sdmx/<source>/` (e.g. `wb`)
- Tests: `tests/` (unit + integration + fixture-driven tests)

How to run (recap)
------------------
1) Setup venv and install deps

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

2) Run deterministically from fixtures (fast, recommended for development)

```powershell
python -m scripts.ci_fixture_run example-configs/example-config-s1.yaml
```

3) Full network run (may take longer and requires internet):

```powershell
python -m src.main --config example-configs/example-config-s2.yaml
```

4) Run tests

```powershell
pytest -q
```

CI notes
--------
- The CI workflow runs tests in a Python matrix and then a `fixtures` job that will download any `sdmx` artifacts and run the fixture-only pipeline. If you add more fixtures, place them under `tests/fixtures/sdmx/<source>/` and CI will collect them as artifacts for downstream jobs.

Next steps (optional)
---------------------
- Push these changes and open a PR so GitHub Actions can run end-to-end and upload artifacts for inspection.
- Add more SDMX fixtures (additional indicators/countries) to broaden CI coverage.
- I can create a Dockerfile and a simple Makefile (make fetch, make rank, make test) if you'd like reproducible runs.

Contact
-------
If you want, I can prepare the PR text and push everything. Tell me which next step you want me to do.

Backtest usage
--------------
The pipeline supports an optional backtest mode that can be enabled in the configuration. The backtest uses the computed ranking signals to construct simple portfolio allocations and then simulates portfolio NAV and turnover using historical price series.

Key config options (example snippet):

```yaml
backtest:
	enabled: true
	top_n: 10            # number of top countries to include in allocation
	min_alloc: 0.01      # minimum per-country allocation
	max_alloc: 0.6       # maximum per-country allocation
	no_backfill: false   # if true, enforce point-in-time via manifest `series_as_of`
```

When backtest is enabled the Excel writer will include two additional sheets (if available): `Portfolio` and `Backtest`.

CI and SDMX fixtures
--------------------
We include a GitHub Actions workflow at `.github/workflows/ci.yml` that runs pytest and collects artifacts. For deterministic CI runs against SDMX sources, place offline SDMX fixtures under `tests/fixtures/sdmx/` — the CI job will collect them as artifacts for debugging. In CI you can also provide a `MANIFEST_SIGNING_KEY` secret to enable signed manifests for release runs.

If you want, I can add a small example pipeline-run that runs in CI using only the SDMX fixtures (next step).

Documentation

- Usage guide: `docs/USAGE.md`
- Portfolio mapping template: `docs/portfolio_mapping_template.csv`
 - Examples: `docs/EXAMPLES.md`

Portfolio mapping template
--------------------------
If you want to map country codes to portfolio asset labels (e.g., ETFs) use the provided CSV template at `docs/portfolio_mapping_template.csv`.
Format:

country,asset_label
DEU,DEU ETF
FRA,FRA ETF

The pipeline currently writes `output/allocations.csv` as the canonical per-run allocations; you can import this into your portfolio system or use the `docs/portfolio_mapping_template.csv` to map country codes to instrument identifiers.

Per‑Indicator Standardisierungs‑Overrides
----------------------------------------
Sie können globale Standardisierungs‑Einstellungen in der Config unter `scoring.standardization` setzen. Falls ein einzelner Indikator abweichende Einstellungen benötigt (z.B. engere Winsor‑Bounds wegen Ausreißern), legen Sie im Indikator‑Manifest oder in der Indikator‑Definition ein Feld `standardization` an, das die gleichen Optionen enthält und die globale Konfiguration überschreibt.

Beispiel (YAML‑Snippet für einen Indikator):

```yaml
indicators:
	- id: gdp_growth
		name: "BIP‑Wachstum"
		good_direction: up
		standardization:
			winsor_lower: 0.05
			winsor_upper: 0.95
			rolling_window: 5
			rolling_min_periods: 1
```

Das System merged die `standardization`‑Diktate: Per‑Indicator‑Werte haben Vorrang vor den globalen Einstellungen. Wenn das Feld nicht vorhanden ist, wird die globale `scoring.standardization` verwendet.
