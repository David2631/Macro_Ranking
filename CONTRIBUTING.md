Contributing

Thanks for contributing! A few guidelines:

- Run tests before opening a PR:

```powershell
python -m venv .venv; .\.venv\Scripts\Activate.ps1; pip install -r requirements.txt
pytest -q
```

- Add SDMX fixtures under `tests/fixtures/sdmx/<source>` when adding or changing fetchers.
- Keep changes small and add unit tests for new behavior.
- Use `black` or `flake8` if you'd like; CI currently runs pytest only.
