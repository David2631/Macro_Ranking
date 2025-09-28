"""Run the pipeline for 70 countries (selected from pycountry list).

This script prepends the repository root to sys.path so `src` can be imported
when run from the scripts folder.
"""
import os
import sys
ROOT = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0, ROOT)

import pycountry
from src.main import main

countries = [c.alpha_3 for c in list(pycountry.countries)[:70]]
print('Running pipeline for', len(countries), 'countries')
main(['--countries', ','.join(countries)])
