"""Verification commands must not treat a saved certificate as fresh evidence."""
import importlib.util
import json
from pathlib import Path
import sys

import pytest


@pytest.mark.parametrize('readback_fails', [False, True])
def test_verify_only_rechecks_completed_case_without_loading(tmp_path, monkeypatch, readback_fails):
    script = Path(__file__).resolve().parents[1] / 'scripts' / 'run_multigb_acceptance.py'
    spec = importlib.util.spec_from_file_location('independent_acceptance_controller', script)
    controller = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(controller)

    folder = tmp_path / 'matrix'
    case = folder / 'bcp-128'
    source_dir = folder / 'source'
    case.mkdir(parents=True)
    source_dir.mkdir()
    source = {
        'source': str(source_dir / 'source.csv'),
        'source_bytes': 2 * 1024 ** 3 + 1,
        'sha256': 'fixture-source-hash',
        'column_names': ['code'],
        'rows': 1,
    }
    (source_dir / 'independent-source.json').write_text(json.dumps(source), encoding='utf-8')
    runtime = {'fingerprint_sha256': 'fixture-runtime'}
    cached = {'runtime_fingerprint_sha256': 'fixture-runtime', 'verification': {'marker': 'old'}}
    (case / 'verified.json').write_text(json.dumps(cached), encoding='utf-8')
    (case / 'execution.json').write_text(json.dumps({'exit_code': 3, 'supervisor_failure': None, 'elapsed_seconds': 1, 'sampled_peak_process_tree_rss_bytes': 1024}), encoding='utf-8')
    config = tmp_path / 'config.toml'
    config.write_text('[destination]\nserver="127.0.0.1,14339"\n', encoding='utf-8')
    credentials = tmp_path / 'credentials.json'
    credentials.write_text('{}', encoding='utf-8')
    monkeypatch.setattr(controller, 'runtime_fingerprint', lambda _: runtime)
    monkeypatch.setattr(controller, 'hash_file', lambda _: (source['source_bytes'], source['sha256']))
    monkeypatch.setattr(controller, 'CASES', [('bcp', 128, 8 * 1024 * 1024)])
    calls = []

    def fresh_readback(observed_folder, observed_source, observed_credentials, destination):
        calls.append(observed_folder)
        assert observed_folder == case
        assert observed_source == source
        if readback_fails:
            raise AssertionError('fresh SQL readback found changed values')
        return {'marker': 'fresh'}

    def forbidden_load(*args, **kwargs):
        pytest.fail('--verify-only must never launch a new import')

    monkeypatch.setattr(controller, 'verify', fresh_readback)
    monkeypatch.setattr(controller, 'supervise', forbidden_load)
    monkeypatch.setattr(sys, 'argv', [str(script), '--folder', str(folder),
        '--interpreter', sys.executable, '--config', str(config),
        '--credentials-json', str(credentials), '--bcp-path', str(tmp_path / 'unused-bcp.exe'),
        '--only', 'bcp-128', '--verify-only'])

    if readback_fails:
        with pytest.raises(AssertionError, match='fresh SQL readback found changed values'):
            controller.main()
        assert json.loads((case / 'verified.json').read_text(encoding='utf-8')) == cached
    else:
        assert controller.main() == 0
        result = json.loads((case / 'verified.json').read_text(encoding='utf-8'))
        assert result['verification'] == {'marker': 'fresh'}
    assert calls == [case]
