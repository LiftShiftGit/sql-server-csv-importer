from pathlib import Path
import pytest
from db_ingest.config import load_config
from db_ingest.planning import map_destination
from db_ingest.pipeline import prepare,recover_plan
from db_ingest.errors import IngestError

@pytest.mark.parametrize('path,expected',[('orders.csv','orders'),('sales/orders.csv','orders'),('a-b.csv','a_b'),('123.csv','n_123'),('订单.csv','unnamed'),('a.b.csv','a_b'),('Orders.csv','Orders')])
def test_filename_mapping(path,expected):
    c=load_config(environ={})
    assert map_destination(c,path)['table']==expected
    assert map_destination(c,path)['database']==c['destination']['database']

def test_explicit_hash_option_and_no_silent_truncation():
    c=load_config(overrides={'mapping.name_collision':'stable_hash_suffix'},environ={});assert map_destination(c,'orders.csv')['table']=='orders__bef94e2e5e84a227'
    c['mapping']['name_collision']='filename_error'
    with pytest.raises(IngestError,match='128'):map_destination(c,'a'*129+'.csv')
    assert map_destination(c,'a'*128+'.csv')['table']=='a'*128

@pytest.mark.parametrize('key,value',[('database_strategy','per_file'),('database_strategy','per_directory'),('table_strategy','per_directory'),('existing_target_strategy','mapped_business')])
def test_filename_mode_rejects_unsupported_mapping_combinations(key,value):
    with pytest.raises(IngestError):load_config(overrides={'mapping.name_collision':'filename_error','mapping.'+key:value},environ={})

@pytest.mark.parametrize('paths',[('a/orders.csv','b/orders.csv'),('a/orders.csv','b/Orders.csv'),('a-b.csv','a b.csv'),('orders.csv','orders.txt'),('1.csv','n_1.csv')])
def test_inventory_collision_blocks_entire_selection_and_resume(tmp_path,paths):
    for name in paths:
        p=tmp_path/name;p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(b'id,name\n1,test\n')
    c=load_config(overrides={'source.root':str(tmp_path),'source.include':['**/*'],'mapping.name_collision':'filename_error','parsing.header':'present','preservation.work_directory':str(tmp_path/'work'),'preservation.minimum_free_disk_bytes':0,'parallel.enabled':False},environ={})
    report=prepare(c)
    assert report['exit_code']==2 and any(e['code']=='TABLE_NAME_COLLISION' for e in report['errors'])
    assert not report['files']
    again=recover_plan(c,report['run_id']);assert again['exit_code']==2
    for name in paths:assert (tmp_path/name).read_bytes()==b'id,name\n1,test\n'


def test_old_hashed_resume_requires_explicit_hash_option():
    from db_ingest.pipeline import _guard_resume_config
    old=load_config(overrides={'mapping.name_collision':'stable_hash_suffix'},environ={})
    current=load_config(environ={})
    with pytest.raises(IngestError) as caught:_guard_resume_config(current,old)
    assert caught.value.code=='RESUME_SEMANTIC_CHANGE'
    _guard_resume_config(load_config(overrides={'mapping.name_collision':'stable_hash_suffix'},environ={}),old)

@pytest.mark.parametrize('example',['ingest.example.toml','db_ingest/ingest.example.toml'])
def test_starter_templates_choose_filename_default(example):
    assert load_config(Path(__file__).resolve().parents[1]/example,environ={})['mapping']['name_collision']=='filename_error'


def test_explicit_and_omitted_filename_have_identical_semantics():
    from db_ingest.config import hashes
    a=load_config(environ={})
    b=load_config(overrides={'mapping.name_collision':'filename_error'},environ={})
    assert hashes(a)==hashes(b)


def test_filename_inventory_does_not_upgrade_a_stale_read_snapshot(tmp_path,monkeypatch):
    import sqlite3
    from db_ingest.journal import Journal
    source=tmp_path/'orders.csv';source.write_bytes(b'id,name\n1,test\n')
    c=load_config(overrides={'source.root':str(tmp_path),'parsing.header':'present','parsing.delimiter':',','parallel.enabled':False,'preservation.work_directory':str(tmp_path/'work'),'preservation.minimum_free_disk_bytes':0},environ={})
    original=Journal.add_selection;injected=[]
    def overlapping_write(journal,run_id,*args,**kwargs):
        if not injected:
            # A read followed by another connection's committed write reproduces
            # SQLITE_BUSY_SNAPSHOT if TEMP cache initialization left a TX open.
            journal.db.execute('SELECT state FROM runs WHERE run_id=?',(run_id,)).fetchone()
            other=sqlite3.connect(journal.work/'journal.sqlite3',timeout=1)
            try:
                other.execute('UPDATE runs SET updated=? WHERE run_id=?',('concurrent inventory fixture',run_id));other.commit()
            finally:other.close()
            injected.append(True)
        return original(journal,run_id,*args,**kwargs)
    monkeypatch.setattr(Journal,'add_selection',overlapping_write)
    report=prepare(c)
    assert injected and report['exit_code']==0
    assert report['files'][0]['plan']['table']=='orders'
    assert source.read_bytes()==b'id,name\n1,test\n'


def test_collision_closes_discovery_generator(tmp_path,monkeypatch):
    from db_ingest import discovery
    closed=[]
    def entries(config):
        try:
            for name in ['a/orders.csv','b/orders.csv']:
                yield {'path':str(tmp_path/name),'relative_path':name,'status':'selected','reason':'fixture'}
        finally:closed.append(True)
    monkeypatch.setattr(discovery,'inventory',entries)
    c=load_config(overrides={'source.root':str(tmp_path),'parallel.enabled':False,'preservation.work_directory':str(tmp_path/'work'),'preservation.minimum_free_disk_bytes':0},environ={})
    assert prepare(c)['exit_code']==2
    assert closed==[True]
