import pytest
from db_ingest.config import load_config
from db_ingest.planning import map_destination
from db_ingest.pipeline import prepare,recover_plan,_guard_resume_config
from db_ingest.errors import IngestError

@pytest.mark.parametrize('strategy,path,expected',[
 ('per_file','orders.csv','orders'),('per_file','north/orders.csv','north_orders'),
 ('per_file','north/2026/orders.csv','north_2026_orders'),('per_file','south/orders.csv','south_orders'),
 ('per_directory','north/part1.csv','north'),('per_directory','north/2026/part1.csv','north_2026'),
 ('per_directory','part1.csv','root_rows')])
def test_readable_mapping(strategy,path,expected):
 c=load_config(overrides={'mapping.name_collision':'path_error','mapping.table_strategy':strategy,'mapping.root_table':'root_rows'},environ={})
 assert map_destination(c,path)['table']==expected
 assert map_destination(c,path.replace('/','\\'))['table']==expected

@pytest.mark.parametrize('strategy,paths,blocked',[
 ('per_file',['north/orders.csv','south/orders.csv'],False),
 ('per_file',['north/orders.csv','north_orders.csv'],True),
 ('per_file',['North/orders.csv','north_orders.csv'],True),
 ('per_file',['north/a-b.csv','north/a b.csv'],True),
 ('per_directory',['north/a.csv','north/b.csv'],False),
 ('per_directory',['north/a.csv','south/b.csv'],False),
 ('per_directory',['a_b/part.csv','a/b/part.csv'],True),
 ('per_directory',['part.csv','root/part.csv'],True)])
def test_inventory_groups_and_collisions(tmp_path,strategy,paths,blocked):
 for name in paths:
  p=tmp_path/name;p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(b'id,name\n001,alpha\n')
 c=load_config(overrides={'source.root':str(tmp_path),'source.include':['**/*.csv'],'mapping.name_collision':'path_error','mapping.table_strategy':strategy,'mapping.root_table':'root','load.mode':'create_only','parsing.header':'present','parsing.delimiter':',','parallel.enabled':False,'preservation.minimum_free_disk_bytes':0,'preservation.work_directory':str(tmp_path/'work')},environ={})
 report=prepare(c)
 assert report['exit_code']==(2 if blocked else 0),report
 if blocked:
  assert any(e['code']=='TABLE_NAME_COLLISION' for e in report['errors'])
  assert not report['files']
  assert recover_plan(c,report['run_id'])['exit_code']==2

@pytest.mark.parametrize('overrides',[
 {'mapping.database_strategy':'per_directory','mapping.name_collision':'path_error'},
 {'mapping.existing_target_strategy':'mapped_business','mapping.name_collision':'path_error'},
 {'mapping.existing_target_strategy':'mapped_business','mapping.name_collision':'stable_hash_suffix','load.mode':'create_only'}])
def test_invalid_combinations(overrides):
 with pytest.raises(IngestError):load_config(overrides=overrides,environ={})

def test_no_truncation_or_resume_semantic_change():
 c=load_config(overrides={'mapping.name_collision':'path_error'},environ={})
 with pytest.raises(IngestError):map_destination(c,'north/'+('a'*124)+'.csv')
 with pytest.raises(IngestError):_guard_resume_config(c,load_config(environ={}))
 with pytest.raises(IngestError):_guard_resume_config(load_config(overrides={'load.mode':'create_only'},environ={}),load_config(environ={}))


def test_export_mapping_complete_and_no_overwrite(tmp_path):
 import csv
 from db_ingest.cli import main
 for folder in ['north','south']:
  p=tmp_path/folder/'orders.csv';p.parent.mkdir();p.write_bytes(b'id,name\n001,test\n')
 cfg=tmp_path/'ingest.toml';work=tmp_path/'work'
 cfg.write_text(f"[source]\nroot='{tmp_path}'\ninclude=['**/*.csv']\n[mapping]\nname_collision='path_error'\n[load]\nmode='create_only'\n[parsing]\nheader='present'\ndelimiter=','\n[preservation]\nwork_directory='{work}'\nminimum_free_disk_bytes=0\n[parallel]\nenabled=false\n",encoding='utf-8')
 report=prepare(load_config(cfg,environ={}));assert report['exit_code']==0
 out=tmp_path/'mapping.csv';args=['--config',str(cfg),'export-mapping','--run-id',report['run_id'],'--output',str(out)]
 assert main(args)==0
 with out.open(encoding='utf-8-sig',newline='') as stream:rows=list(csv.DictReader(stream))
 assert {x['table'] for x in rows}=={'north_orders','south_orders'}
 assert {x['load_mode'] for x in rows}=={'create_only'}
 before=out.read_bytes();assert main(args)==4;assert out.read_bytes()==before
