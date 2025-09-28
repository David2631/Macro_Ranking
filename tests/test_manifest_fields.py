import os
import json
import glob


def test_latest_manifest_has_env_and_fetch_fields():
    art_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', '_artifacts')
    files = sorted(glob.glob(os.path.join(art_dir, '*.json')))
    assert files, 'No manifest files found in data/_artifacts'
    latest = files[-1]
    with open(latest, encoding='utf-8') as fh:
        m = json.load(fh)

    # environment hash
    assert 'environment' in m
    assert 'env_hash' in m['environment']

    # fetch entries
    assert 'fetches' in m
    assert isinstance(m['fetches'], list)
    if m['fetches']:
        sample = m['fetches'][0]
        # required keys
        for k in ('request_url', 'http_status', 'response_time_ms', 'rows', 'sha256_normalized', 'fetch_timestamp'):
            assert k in sample, f'Missing key {k} in fetch entry'
