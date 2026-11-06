"""Root/path inheritance must not broaden explicit selections or resumed intents."""
import json
from pathlib import Path
import pytest
from db_ingest.config import load_config,effective_for_file,hashes
from db_ingest.discovery import discover
from db_ingest.pipeline import _guard_resume_config,prepare
from db_ingest.errors import IngestError

def write(tmp_path,root,path=None):
 p=tmp_path/'input.toml';p.write_text('[source]\nroot='+json.dumps(str(root))+ ('\npath='+json.dumps(path) if path is not None else ''),encoding='utf-8');return p

@pytest.mark.parametrize('path',[None,''])
@pytest.mark.parametrize('cli_root',[False,True])
def test_inherits_final_root_after_precedence(tmp_path,path,cli_root):
 p=write(tmp_path,tmp_path/'original',path)
 overrides={'source.root':str(tmp_path/'chosen')} if cli_root else {}
 c=load_config(p,overrides=overrides,environ={})
 expected=tmp_path/('chosen' if cli_root else 'original')
 assert c['source']['path']==str(expected)
 assert c['_provenance']['source.path']=='derived:source.root'
 assert effective_for_file(c,'a.csv')['source']['path']==c['source']['path']

@pytest.mark.parametrize('explicit',["D:\\Exports",'D:\\Exports\\one.csv','D:\\Exports\\branch'])
def test_explicit_path_survives_root_override(tmp_path,explicit):
 c=load_config(write(tmp_path,'D:\\Exports',explicit),overrides={'source.root':'D:\\Other'},environ={})
 assert c['source']['path']==explicit

@pytest.mark.parametrize('path',[None,'','explicit'])
def test_cli_source_precedence_and_unset_optional_argument(tmp_path,path):
 p=write(tmp_path,tmp_path,str(tmp_path/'sub'))
 overrides={'source.path':str(tmp_path/'one.csv') if path=='explicit' else path}
 c=load_config(p,overrides=overrides,environ={})
 expected=tmp_path/'sub' if path is None else tmp_path/'one.csv' if path=='explicit' else tmp_path
 assert c['source']['path']==str(expected)

@pytest.mark.parametrize('recursive',[False,True])
@pytest.mark.parametrize('path',[None,''])
def test_multiple_files_exact_selection_with_filters_and_work_exclusion(tmp_path,recursive,path):
 root=tmp_path/'exports';root.mkdir();(root/'sub').mkdir();(root/'work').mkdir()
 for name in ['a.csv','b.csv','skip.csv','other.txt','sub/c.csv','work/private.csv']:(root/name).write_text('a\n1\n')
 c=load_config(write(tmp_path,root,path),overrides={'source.recursive':recursive,'source.include':['**/*.csv'],'source.exclude':['skip.csv'],'preservation.work_directory':str(root/'work')},environ={})
 assert [p.relative_to(root).as_posix() for p in discover(c)]==(['a.csv','b.csv','sub/c.csv'] if recursive else ['a.csv','b.csv'])

@pytest.mark.parametrize('kind',['file','subfolder'])
def test_explicit_narrow_selection_remains_narrow(tmp_path,kind):
 root=tmp_path/'exports';root.mkdir();(root/'sub').mkdir();(root/'a.csv').write_text('a');(root/'sub/b.csv').write_text('b')
 path=root/('a.csv' if kind=='file' else 'sub')
 c=load_config(write(tmp_path,root,str(path)),environ={})
 assert list(discover(c))==[root/('a.csv' if kind=='file' else 'sub/b.csv')]

def test_bad_or_outside_path_never_falls_back_to_root(tmp_path):
 for path,code in [(tmp_path/'absent','SOURCE_MISSING'),(tmp_path.parent/'outside','SOURCE_OUTSIDE_ROOT')]:
  c=load_config(write(tmp_path,tmp_path,str(path)),environ={})
  with pytest.raises(IngestError) as e:list(discover(c))
  assert e.value.code==code

def test_implicit_and_explicit_same_root_have_same_identity_and_resume(tmp_path):
 implicit=load_config(write(tmp_path,tmp_path),environ={})
 explicit=load_config(write(tmp_path,tmp_path,str(tmp_path)),environ={})
 assert hashes(implicit)==hashes(explicit)
 _guard_resume_config(implicit,explicit);_guard_resume_config(explicit,implicit)

def test_legacy_saved_selection_cannot_silently_expand_on_resume(tmp_path):
 old=load_config(write(tmp_path,tmp_path,str(tmp_path/'sub')),environ={})
 new=load_config(write(tmp_path,tmp_path),environ={})
 with pytest.raises(IngestError) as e:_guard_resume_config(new,old)
 assert e.value.code=='RESUME_SELECTION_CHANGE'

@pytest.mark.parametrize('text',['path = null','path = false','path = []','path = 7'])
def test_invalid_toml_path_is_not_treated_as_unspecified(tmp_path,text):
 p=tmp_path/'bad.toml';p.write_text('[source]\n'+text)
 with pytest.raises(IngestError):load_config(p,environ={})

def test_root_only_plan_captures_each_file_without_sql_or_modifying_sources(tmp_path):
 root=tmp_path/'exports';root.mkdir();raw=b'id,text\n001,"quoted, text"\n'
 for name in ('a.csv','b.csv'):(root/name).write_bytes(raw)
 c=load_config(write(tmp_path,root),overrides={'parsing.header':'present','parsing.delimiter':',','parallel.enabled':False,'preservation.work_directory':str(tmp_path/'work')},environ={})
 r=prepare(c);assert r['exit_code']==0 and r['file_count']==2
 for item in r['files']:
  assert item['counts']['pending']==1 and not item.get('receipt')
  assert Path(item['artifact']['path']).read_bytes()==raw
 assert all(p.read_bytes()==raw for p in root.glob('*.csv'))

@pytest.mark.parametrize('root',['',' ','\t'])
def test_empty_root_cannot_turn_inheritance_into_current_directory_scan(tmp_path,root):
 with pytest.raises(IngestError) as e:load_config(write(tmp_path,root),environ={})
 assert e.value.code=='CONFIG_SOURCE'
