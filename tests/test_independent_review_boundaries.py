"""Independent boundary oracles for staging, naming, and retained capture evidence."""
import hashlib

import pytest

from db_ingest.capture import require_safe_capture_retry
from db_ingest.config import load_config
from db_ingest.errors import IngestError
from db_ingest.journal import Journal
from db_ingest.models import ColumnProfile, FieldValue, RecordEnvelope, SchemaPlan
from db_ingest.pipeline import prepare, recover_plan
from db_ingest.publication import _stage_records


def schema(width=1):
    return SchemaPlan('db', 'dbo', 'records', [
        ColumnProfile(i + 1, f'c{i}', f'c{i}', 'nvarchar(max)')
        for i in range(width)
    ], 'source', 'interpretation', 'mapping')


def row(ordinal, *values):
    return RecordEnvelope(f'r{ordinal}', ordinal, None, None, None, None,
                          [FieldValue('NULL' if v is None else 'VALUE', v) for v in values])


class Sink:
    def __init__(self):
        self.rows = []

    def add(self, *args):
        self.rows.append(args)


def independent_digest(rows):
    # A wire-format oracle; deliberately does not import canonical_value or receipts.
    framed = bytearray(b'db_ingest:typed-values:v1\0')
    tag = b'nvarchar(max)'
    for ordinal, values in rows:
        identity = f'r{ordinal}'.encode()
        framed += len(identity).to_bytes(4, 'big') + identity
        framed += ordinal.to_bytes(8, 'big') + len(values).to_bytes(4, 'big')
        for value in values:
            framed += len(tag).to_bytes(4, 'big') + tag
            if value is None:
                framed += b'N'
            else:
                payload = value.encode('utf-8')
                framed += b'V' + len(payload).to_bytes(8, 'big') + payload
    return hashlib.sha256(framed).hexdigest()


@pytest.mark.parametrize('transport', ['loader', 'writer'])
def test_stage_digest_and_null_framing_use_independent_oracle(transport):
    values = [(1, ['ab', 'c']), (3, ['a', 'bc']), (4, ['', None]),
              (9, ['\r\n"\\', '😀é'])]
    sink = Sink()
    count, digest = _stage_records({}, schema(2), (row(i, *v) for i, v in values),
                                  'intent', sink if transport == 'loader' else None,
                                  sink if transport == 'writer' else None)
    assert count == len(values)
    assert digest.hexdigest() == independent_digest(values)
    if transport == 'loader':
        assert [(r.ordinal, v) for r, v in sink.rows] == values
    else:
        assert sink.rows[2][0] == [0, '', 1, '', 'intent', 'r4', 4]
        assert sink.rows[-1][0] == [0, '\r\n"\\', 0, '😀é', 'intent', 'r9', 9]


@pytest.mark.parametrize('transport', ['loader', 'writer'])
@pytest.mark.parametrize('limit_key', ['field', 'record', 'batch'])
@pytest.mark.parametrize('allowed', [False, True])
def test_stage_exact_byte_boundaries(transport, limit_key, allowed):
    # Two fields each require 4 bytes under max(UTF-8, UTF-16), total 8.
    value = 4 if limit_key == 'field' else 8
    limit = value if allowed else value - 1
    config = {'limits': {}, 'load': {}}
    if limit_key == 'batch':
        config['load']['batch_bytes'] = limit
    else:
        config['limits'][f'max_in_memory_{limit_key}_bytes'] = limit
    sink = Sink()
    args = (config, schema(2), [row(1, 'éé', '😀')], 'intent',
            sink if transport == 'loader' else None,
            sink if transport == 'writer' else None)
    if allowed:
        assert _stage_records(*args)[0] == 1
        assert len(sink.rows) == 1
    else:
        with pytest.raises(IngestError) as caught:
            _stage_records(*args)
        assert caught.value.code == ('FIELD_BUDGET' if limit_key == 'field' else 'RECORD_BUDGET')
        assert not sink.rows


@pytest.mark.parametrize('bad_values', [[], ['a', 'b']])
def test_late_width_change_is_rejected_without_consuming_following_rows(bad_values):
    sink = Sink()
    def stream():
        yield row(1, 'valid')
        assert len(sink.rows) == 1, 'staging must consume and write incrementally'
        yield row(2, *bad_values)
        pytest.fail('staging read past the first invalid record')
    with pytest.raises(IngestError) as caught:
        _stage_records({}, schema(), stream, 'intent', sink, None)
    assert caught.value.code == 'ROW_WIDTH'
    assert len(sink.rows) == 1


@pytest.mark.parametrize('paths', [
    ('a_b/orders.csv', 'a/b_orders.csv'),
    ('north/orders.csv', 'north_orders.csv'),
    ('a-b/orders.csv', 'a b/orders.csv'),
    ('a/orders.csv', 'A_Orders.csv'),
])
def test_single_underscore_ambiguous_paths_block_inventory_and_resume(tmp_path, paths):
    for name in paths:
        source = tmp_path / name
        source.parent.mkdir(parents=True, exist_ok=True)
        source.write_bytes(b'id,value\n1,original\n')
    config = load_config(overrides={
        'source.root': str(tmp_path), 'mapping.name_collision': 'path_error',
        'parallel.enabled': False, 'parsing.header': 'present',
        'preservation.work_directory': str(tmp_path / 'work'),
        'preservation.minimum_free_disk_bytes': 0,
    }, environ={})
    result = prepare(config)
    assert result['exit_code'] == 2
    assert any(e['code'] == 'TABLE_NAME_COLLISION' for e in result['errors'])
    assert not result['files']
    resumed = recover_plan(config, result['run_id'])
    assert resumed['exit_code'] == 2
    assert not resumed['files']
    assert all((tmp_path / name).read_bytes() == b'id,value\n1,original\n' for name in paths)


@pytest.mark.parametrize('retained', ['source.partial', 'source.bin'])
def test_retry_checks_all_older_attempts_and_preserves_bytes(tmp_path, retained):
    journal = Journal(tmp_path / 'work')
    try:
        old_id, empty_id = 'a' * 32, 'b' * 32
        folder = journal.work / 'captures' / old_id
        folder.mkdir(parents=True)
        evidence = folder / retained
        evidence.write_bytes(b'captured-prefix')
        for capture_id in [old_id, empty_id]:
            journal.event('run', capture_id, 'CAPTURE_ATTEMPT_STARTED',
                          {'selection_id': 'selected', 'capture_id': capture_id})
        with pytest.raises(IngestError) as caught:
            require_safe_capture_retry(journal, 'run', 'selected', empty_id)
        assert caught.value.code == 'CAPTURE_RETRY_UNVERIFIED'
        assert caught.value.context['capture_id'] == old_id
        assert evidence.read_bytes() == b'captured-prefix'
    finally:
        journal.close()


@pytest.mark.parametrize('invalid_id', ['../outside', 'A' * 32, 'x' * 32, 'a' * 33])
def test_invalid_historical_capture_identity_fails_closed(tmp_path, invalid_id):
    journal = Journal(tmp_path / 'work')
    try:
        journal.event('run', invalid_id, 'CAPTURE_ATTEMPT_STARTED', {'selection_id': 'selected'})
        with pytest.raises(IngestError) as caught:
            require_safe_capture_retry(journal, 'run', 'selected', 'b' * 32)
        assert caught.value.code == 'CAPTURE_RETRY_EVIDENCE_INVALID'
    finally:
        journal.close()


def test_embedded_nul_is_rejected_before_any_transport_write():
    sink = Sink()
    with pytest.raises(IngestError) as caught:
        _stage_records({}, schema(), [row(1, 'before\0after')], 'intent', sink, None)
    assert caught.value.code == 'VALUE_INVALID'
    assert not sink.rows
