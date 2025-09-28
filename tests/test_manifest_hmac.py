import os
import json
import hmac
import hashlib
from src.io import artifacts


def test_manifest_hmac_signature(tmp_path, monkeypatch):
    # set a deterministic signing key
    monkeypatch.setenv('MANIFEST_SIGNING_KEY', 'testkey123')

    # construct a minimal stable manifest payload (only stable_keys will be signed)
    manifest = {
        'config_snapshot': {'k': 1},
        'fetch_summary': {'X': 1},
        'fetches': [
            {
                'request_url': 'https://example.test/api',
                'params': None,
                'http_status': 200,
                'response_time_ms': 10,
                'rows': 1,
                'sha256_normalized': 'deadbeef',
            }
        ],
        'n_rows': 1,
        'outputs': {},
    }

    out_path = artifacts.write_manifest(manifest, prefix='hmac_test')
    assert out_path and os.path.exists(out_path)

    with open(out_path, encoding='utf-8') as fh:
        m = json.load(fh)

    # manifest_signature must be present
    assert 'manifest_signature' in m

    # recompute expected signature over stable payload using same canonical JSON rules
    stable_keys = ['config_snapshot', 'fetch_summary', 'fetches', 'n_rows', 'outputs']
    stable_payload = {k: m.get(k) for k in stable_keys if k in m}
    canonical = json.dumps(stable_payload, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode('utf-8')
    expected = hmac.new(b'testkey123', canonical, hashlib.sha256).hexdigest()
    assert m['manifest_signature'] == expected
