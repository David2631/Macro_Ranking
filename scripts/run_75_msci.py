"""Run the pipeline for a curated list of ~75 countries that typically have equity markets / MSCI ETFs.

This script reuses the batching runner to execute the pipeline for a curated ISO3 list and
produces a combined Excel + CSV and manifest in output/ and data/_artifacts/.
"""

import os
import sys

ROOT = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0, ROOT)

from scripts.run_170 import run_batches

# Curated ~75 countries (ISO3) with equity markets / ETF coverage. This list is conservative
# and includes developed markets, large emerging markets and a selection of other markets.
COUNTRIES = [
    "USA",
    "CAN",
    "MEX",
    "BRA",
    "ARG",
    "COL",
    "CHL",
    "PER",
    "GBR",
    "IRL",
    "FRA",
    "DEU",
    "ITA",
    "ESP",
    "NLD",
    "BEL",
    "LUX",
    "SWE",
    "NOR",
    "DNK",
    "FIN",
    "AUT",
    "CHE",
    "PRT",
    "GRC",
    "POL",
    "CZE",
    "HUN",
    "ROU",
    "TUR",
    "KWT",
    "SAU",
    "QAT",
    "ARE",
    "ISR",
    "EGY",
    "ZAF",
    "KOR",
    "JPN",
    "TWN",
    "HKG",
    "SGP",
    "AUS",
    "NZL",
    "IND",
    "CHN",
    "IDN",
    "MYS",
    "THA",
    "PHL",
    "VNM",
    "KAZ",
    "LKA",
    "NGA",
    "KEN",
    "MAR",
    "EST",
    "LTU",
    "LVA",
    "SVN",
    "SVK",
    "HRV",
    "SRB",
    "BGR",
    "CYP",
    "MLT",
    "ISL",
    "URY",
    "ECU",
    "BOL",
    "PAN",
    "CRI",
    "DOM",
    "PRY",
    "JAM",
]

# dedupe and ensure we have 75 or fewer
COUNTRIES = list(dict.fromkeys(COUNTRIES))[:75]

if __name__ == "__main__":
    # run in 3 batches of 25
    run_batches(COUNTRIES, batch_size=25, run=True, cleanup=True)
