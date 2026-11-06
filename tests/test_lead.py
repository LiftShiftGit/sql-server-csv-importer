import json
from pathlib import Path
import subprocess
import sys
import pytest
from db_ingest.config import load_config,hashes,parse_bool,resolve_columns
from db_ingest.errors import IngestError
from db_ingest.journal import Journal
from db_ingest.models import SourceArtifact,RecordEnvelope,FieldValue
from db_ingest.planning import map_destination


def test_resume_hash_changes_semantics_not_credentials():
    a=load_config(environ={})
    b=load_config(environ={"DB_PASSWORD":"new;}$()password","DB_CONNECT_TIMEOUT":"99"})
    assert hashes(a)["interpretation"]==hashes(b)["interpretation"]
    assert hashes(a)["mapping"]==hashes(b)["mapping"]
    b["file_rules"]=[{"glob":"**/*.csv","header":"absent"}]
    assert hashes(a)["interpretation"]!=hashes(b)["interpretation"]


def test_strict_unknown_and_invariants(tmp_path):
    for content in ('[parsing]\nstrcit_decoding=true', '[schema]\ninfer_defaults=true', '[preservation]\ncapture_original_bytes=false'):
        p=tmp_path/"bad.toml";p.write_text(content)
        with pytest.raises(IngestError):load_config(p,environ={})


def test_mapping_selection_independent_all_strategies():
    c=load_config(overrides={"mapping.name_collision":"stable_hash_suffix"},environ={});names=["A/a.csv","a/A.csv","A/a.tsv","A/b.csv"]
    for strategy in ("single","per_top_level_directory","per_directory","per_file"):
        c["mapping"]["database_strategy"]=strategy
        expected=[map_destination(c,p) for p in names]
        assert [map_destination(c,p) for p in reversed(names)]==list(reversed(expected))
        assert len({(n["database"].lower(),n["table"].lower()) for n in expected})==4
        assert all(len(n["table"])<=128 for n in expected)


def test_journal_dispositions_survive_reopen(tmp_path):
    j=Journal(tmp_path);j.create_run("r",{})
    a=SourceArtifact("f","artifact","source","x.csv","abc",3)
    j.add_file("r",a,"intent")
    for i in (1,2):j.save_record("r","f",RecordEnvelope(str(i),i,0,3,1,1,[FieldValue("VALUE","same")]))
    j.flush();j.close();j=Journal(tmp_path)
    assert j.counts("r","f")["pending"]==2
    j.committed("r","f");j.committed("r","f")
    assert j.counts("r","f")["committed"]==2
    assert j.counts("r","f")["pending"]==0
    j.close()


def test_cli_entrypoints_and_init(tmp_path):
    run=subprocess.run([sys.executable,"-m","db_ingest","init-config","--output",str(tmp_path/"ingest.toml")],capture_output=True,text=True)
    assert run.returncode==0,run.stderr
    run=subprocess.run([sys.executable,"-m","db_ingest","init-config","--output",str(tmp_path/"ingest.toml")],capture_output=True,text=True)
    assert run.returncode==2 and "CONFIG_EXISTS" in run.stderr
    help_run=subprocess.run([sys.executable,"-m","db_ingest","--help"],capture_output=True,text=True)
    assert help_run.returncode==0 and "reimport" in help_run.stdout


def test_native_output_budget_is_independent_and_resume_operational(tmp_path):
    default=load_config(environ={})
    changed=load_config(overrides={"limits.output_buffer_bytes":65536},environ={})
    assert default['limits']['output_buffer_bytes']==32768
    assert hashes(default)['interpretation']==hashes(changed)['interpretation']
    assert hashes(default)['mapping']==hashes(changed)['mapping']
    assert hashes(default)['operational']!=hashes(changed)['operational']
    for value in (0,-1,1048577,True):
        with pytest.raises(IngestError) as error:load_config(overrides={"limits.output_buffer_bytes":value},environ={})
        assert error.value.code=='CONFIG_LIMIT'
