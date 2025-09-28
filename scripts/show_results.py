import pandas as pd
import json
import itertools

EXCEL = 'output/macro_ranking_20250927T143949Z.xlsx'
MANIFEST = 'data/_artifacts/manifest_20250927T143949Z.json'

print('Loading', EXCEL)
df = pd.read_excel(EXCEL, sheet_name=0)
print('\n--- Ranking (full) ---\n')
print(df.to_string(index=False))

print('\n--- Manifest Summary ---\n')
with open(MANIFEST, 'r', encoding='utf-8') as fh:
    m = json.load(fh)

print('manifest_path:', MANIFEST)
print('run_id:', m.get('run_id'))
print('manifest_signature:', m.get('manifest_signature'))
print('n_rows:', m.get('n_rows'))
print('fetch_summary:', m.get('fetch_summary'))
print('outputs:', m.get('outputs'))
print('fetches_count:', len(m.get('fetches', [])))
print('\nfirst 10 fetch entries:\n')
print(json.dumps(list(itertools.islice(m.get('fetches', []), 10)), indent=2, ensure_ascii=False))
