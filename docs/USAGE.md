# Makro-Ranking — Schnellstart (PowerShell)

Diese Datei sammelt nützliche Befehle für die Verwendung des aktuellen Teilprojekts unter Windows (PowerShell). Alle Befehle sind Copy/Paste-fähig.

Vorbedingungen
- Python (3.10+) installiert und im PATH
- Ein virtuelles Environment ist empfohlen (venv/conda)

1) Virtuelle Umgebung erstellen und aktivieren (empfohlen)

```powershell
# PowerShell (Windows)
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

2) Abhängigkeiten installieren

```powershell
# Installiere aus requirements.txt (falls vorhanden)
pip install -r requirements.txt
# Falls du pydantic, pycountry oder pandasdmx extra brauchst:
pip install pydantic pycountry pandasdmx
```

3) Konfiguration
- Standardkonfiguration liegt in `example-config.yaml`.
- Länder ändern: öffne `example-config.yaml` und editiere die Liste `countries:` (ISO3-Codes).

4) Länder per CLI überschreiben

```powershell
# Beispiel: nur Deutschland, USA, Japan
python -m src.main --config .\example-config.yaml --countries DEU,USA,JPN --debug
```

5) Pipeline ausführen (Standardlauf)

```powershell
python -m src.main --config .\example-config.yaml
```

6) Tests ausführen (sofern vorhanden)

```powershell
# Test-Runner (falls Tests eingerichtet sind)
python -m pytest -q
# oder das Projekt-spezifische runTests wrapper (falls vorhanden)
# (Beispiel: wenn du unsere runTests-Integration nutzt)
```

7) ISO3‑Länderliste generieren / anzeigen

```powershell
# Script zum Generieren
python .\scripts\generate_iso3.py
# Ausgabe: data\countries_iso3.csv
# Vorschau
Get-Content .\data\countries_iso3.csv -TotalCount 40
```

8) Ergebnisdateien / Artefakte
- Excel-Ergebnis (Standard): `output\macro_ranking.xlsx`
- Fetch/Provenance-Manifest: `data\_artifacts\manifest_YYYYMMDDTHHMMSSZ.json`

9) Häufige Fehler & Hinweise
- `pip install -r requirements.txt` schlägt fehl: versuche zuerst `pip install --upgrade pip` und prüfe, ob du in einem virtuellen Environment arbeitest.
- SDMX-Quellen (IMF/OECD/ECB): diese Fetcher sind als Best‑Effort vorhanden; du musst Dataset‑Keys oder Indikator‑Codes anpassen. Nutze die World Bank Codes aus `example-config.yaml` für sofortige Ergebnisse.

10) Weiterführende Aktionen
- Länder nach Name statt ISO unterstützen: eine kleine Erweiterung könnte `--countries "Germany,Japan"` erlauben und `pycountry` zur Auflösung nutzen.
- CI: Füge `pytest`, `mypy` und `flake8` zu einem GitHub Actions workflow hinzu.

Wenn du willst, kann ich diese Datei noch um:
- Beispiel `example-config.yaml` snippet erweitern (mit einer großen Länderliste),
- und einen PS1 wrapper erstellen, der standardmäßig die virtuelle Umgebung aktiviert und das Tool startet.

