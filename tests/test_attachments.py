from pathlib import Path
from dataclasses import asdict
import base64
import pytest
from db_ingest.attachments import AttachmentContext,references,verify_references
from db_ingest.config import load_config
from db_ingest.journal import Journal
from db_ingest.models import SourceArtifact,ColumnProfile,FieldValue,RecordEnvelope


def case(tmp_path):
    config=load_config(environ={});config["preservation"].update(work_directory=str(tmp_path/"work"),minimum_free_disk_bytes=1)
    root=tmp_path/"sidecars";root.mkdir()
    j=Journal(tmp_path/"work");artifact=SourceArtifact("csv","unused","original.csv","data.csv","hash",10)
    j.create_run("run",{});j.add_file("run",artifact,"intent")
    col=ColumnProfile(1,"blob","blob","varbinary(max)",stats={"binary_encoding":"attachment_path","attachment_root":str(root)})
    return root,j,AttachmentContext(j,"run",artifact,[col],config)


def record(value,ordinal=1):return RecordEnvelope(str(ordinal),ordinal,0,1,1,1,[FieldValue("VALUE",value)])


def test_attachment_original_survives_deletion_and_replay(tmp_path):
    root,j,context=case(tmp_path);original=bytes(range(256));(root/"all.bin").write_bytes(original)
    first=record("all.bin");assert context.process(first)==[]
    assert base64.b64decode(first.fields[0].text)==original
    (root/"all.bin").unlink();root.rmdir()
    replay=record("all.bin",2);assert context.process(replay)==[]
    assert base64.b64decode(replay.fields[0].text)==original
    refs=references(j,"run","csv");assert refs["count"]==2
    assert verify_references(j,"run","csv")==1
    assert Path(refs["artifacts"][0]["artifact"]["path"]).read_bytes()==original
    j.close()


def test_attachment_missing_escape_and_null_are_distinct(tmp_path):
    root,j,context=case(tmp_path)
    assert context.process(record("missing.bin"))[0]["code"]=="ATTACHMENT_MISSING"
    assert context.process(record("../escape.bin"))[0]["code"]=="ATTACHMENT_ESCAPE"
    null=record("unused");null.fields[0]=FieldValue("NULL",None)
    assert context.process(null)==[] and null.fields[0].state=="NULL"
    assert context.process(record(""))[0]["code"]=="ATTACHMENT_MISSING"
    (root/"empty.bin").write_bytes(b"")
    empty=record("empty.bin");assert context.process(empty)==[] and empty.fields[0].state=="EMPTY"
    j.close()


def test_corrupted_retained_attachment_blocks_replay(tmp_path):
    root,j,context=case(tmp_path);(root/"a.bin").write_bytes(b"a")
    assert context.process(record("a.bin"))==[]
    asset=references(j,"run","csv")["artifacts"][0]["artifact"]
    Path(asset["path"]).write_bytes(b"b")
    from db_ingest.errors import IngestError
    with pytest.raises(IngestError) as exc:context.process(record("a.bin",2))
    assert exc.value.code=="ARCHIVE_MISMATCH" and exc.value.exit_code==5
    j.close()


def test_attachment_reference_reports_have_byte_bounds(tmp_path):
    root,j,context=case(tmp_path);(root/"a.bin").write_bytes(b"a")
    assert context.process(record("a.bin"))==[]
    report=references(j,"run","csv",max_bytes=1)
    assert report["count"]==1 and report["artifacts"]==[] and report["truncated"]
    too_long=context.process(record("x"*40000,2))
    assert too_long[0]["code"]=="ATTACHMENT_PATH_LIMIT"
    assert len(too_long[0]["original_path"])==256
    j.close()
