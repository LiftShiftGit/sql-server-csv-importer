"""Real Windows path and capture protection checks; optional SMB root supplied by lab."""
import os
from pathlib import Path
import pytest
from db_ingest.windows_paths import validate_path_spelling, validate_local_work
from db_ingest.capture import capture_source, _stable_reader
from db_ingest.errors import IngestError
from db_ingest.journal import Journal

pytestmark=pytest.mark.skipif(os.name!='nt',reason='Windows-only release checks')

@pytest.mark.parametrize('value',[r'D:exports',r'\exports',r'\\.\PhysicalDrive0'])
def test_ambiguous_or_device_path_refused(value):
    with pytest.raises(IngestError,match='filesystem|ambiguous'): validate_path_spelling(value,'source.root')

@pytest.mark.parametrize('value',[r'D:\exports',r'D:/exports',r'exports',r'.\exports',r'\\server\share\exports',r'\\?\D:\exports',r'\\?\UNC\server\share\exports'])
def test_accepted_path_spellings(value):
    validate_path_spelling(value,'source.root')

@pytest.mark.parametrize('value',[r'\\unavailable-server\share\work',r'\\?\UNC\unavailable-server\share\work'])
def test_network_journal_refused_before_io(value):
    with pytest.raises(IngestError) as e: Journal(value)
    assert e.value.code=='WORK_DIRECTORY_NETWORK'

def test_mapped_network_work_refused(monkeypatch,tmp_path):
    import db_ingest.windows_paths as paths
    original=paths._remote
    monkeypatch.setattr(paths,'_remote',lambda path: True)
    with pytest.raises(IngestError) as e: validate_local_work(tmp_path/'work')
    assert e.value.code=='WORK_DIRECTORY_NETWORK' and not (tmp_path/'work').exists()

@pytest.mark.parametrize('prefix',[False,True])
def test_long_local_capture_and_work(tmp_path,prefix):
    root=tmp_path/('a'*80)/('b'*80)/('c'*80);root.mkdir(parents=True)
    path=root/'space café.csv';raw=b'id,label\r\n001,"a,b"\r\n';path.write_bytes(raw)
    work=root/'work'
    if prefix:root=Path('\\\\?\\'+str(root));path=root/path.name;work=root/'work'
    validate_local_work(work)
    artifact=capture_source(path,root,work)
    assert Path(artifact.path).read_bytes()==raw and len(artifact.path)>260
    with _stable_reader(path):
        with pytest.raises(OSError): path.open('wb')
        with pytest.raises(OSError): path.unlink()
    assert path.read_bytes()==raw

def test_unc_capture_and_locking():
    value=os.environ.get('DB_INGEST_TEST_UNC_ROOT')
    if not value:pytest.skip('Set the disposable UNC source root for SMB checks')
    root=Path(value);path=root/'orders.csv';raw=path.read_bytes()
    import tempfile
    with tempfile.TemporaryDirectory() as work:
        artifact=capture_source(path,root,work)
        assert Path(artifact.path).read_bytes()==raw
        with _stable_reader(path):
            with pytest.raises(OSError):path.open('wb')
            with pytest.raises(OSError):path.unlink()
        with path.open('r+b'):
            with pytest.raises(IngestError) as e:capture_source(path,root,work)
            assert e.value.code=='CAPTURE_IO'
    assert path.read_bytes()==raw

@pytest.mark.parametrize('section,key,value,code',[
    ('source','root',r'D:exports','CONFIG_PATH'),
    ('source','path',r'D:exports','CONFIG_PATH'),
    ('preservation','work_directory',r'D:work','CONFIG_PATH'),
    ('preservation','work_directory',r'\\server\share\work','WORK_DIRECTORY_NETWORK'),
    ('preservation','work_directory',r'\\?\UNC\server\share\work','WORK_DIRECTORY_NETWORK'),
])
def test_configuration_entry_points_reject_unsafe_paths(section,key,value,code,tmp_path):
    import json
    from db_ingest.config import load_config
    p=tmp_path/'bad.toml'
    p.write_text('['+section+']\n'+key+'='+json.dumps(value)+'\n',encoding='utf-8')
    with pytest.raises(IngestError) as e: load_config(p,environ={})
    assert e.value.code==code
