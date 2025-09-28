## Contributing

Thanks for contributing to Macro_Ranking — we appreciate it!

Quick checklist:

- Open an issue for larger changes before starting work.
- Create a branch from `main` named `feature/...` or `fix/...`.
- Run tests locally and add unit tests for any new behaviour.
- Keep changes small and focused; CI enforces linters, types and an 85% coverage gate.

Recommended local setup (PowerShell):

```powershell
python -m venv .venv; .\.venv\Scripts\Activate.ps1; pip install -r requirements-dev.txt
pytest -q
```

If you add or change fetchers, include SDMX fixtures in `tests/fixtures/sdmx/<source>`.
Use `black`/`ruff` to format and lint; CI will run the full checks.

Thank you!
