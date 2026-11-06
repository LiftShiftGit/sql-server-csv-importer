from dataclasses import asdict
import base64,copy,json
from pathlib import Path
import pytest
from db_ingest.models import ColumnProfile,FieldValue,RecordEnvelope,SourceArtifact
from db_ingest.journal import Journal
from db_ingest.projections import ProjectionContext,projected_value,summary,verify_references,export
from db_ingest.errors import IngestError


@pytest.mark.parametrize("kind,policy,value,expected",[
    ("nvarchar(3)",{"truncation":"right"},"A😀B","A😀"),
    ("nvarchar(2)",{"truncation":"right"},"A😀B","A"),
    ("varchar(5)",{"truncation":"right","text_encoding":"utf-8"},"A😀B","A😀"),
    ("varbinary(2)",{"truncation":"right","binary_encoding":"base64"},"AAECAw==","AAE="),
    ("decimal(5,2)",{"rounding":"half_even"},"1.235","1.24"),
    ("decimal(5,2)",{"rounding":"half_even"},"1.245","1.24"),
    ("decimal(5,2)",{"rounding":"half_even"},"1e-999999","0.00"),
])
def test_projection_policy_has_authored_exact_result(kind,policy,value,expected):
    assert projected_value(FieldValue("VALUE",value),ColumnProfile(1,"x","x",kind,True,policy))==expected


def test_original_projected_evidence_atomic_event_and_ledger_binding(tmp_path):
    journal=Journal(tmp_path/"work");journal.create_run("r",{})
    artifact=SourceArtifact("f",str(tmp_path/"source"),"original","x.csv","a"*64,10)
    journal.add_file("r",artifact,"i")
    column=ColumnProfile(1,"x","x","nvarchar(2)",True,{"truncation":"right"})
    record=RecordEnvelope("record",2,3,10,2,2,[FieldValue("VALUE","A😀B",True)])
    original=copy.deepcopy(record);journal.save_record("r","f",record)
    try:
        with pytest.raises(IngestError):ProjectionContext(journal,"r",artifact,[column],{"schema":{"allow_lossy_projection":False}})
        context=ProjectionContext(journal,"r",artifact,[column],{"schema":{"allow_lossy_projection":True}})
        assert context.process(record)==[] and context.changed
        journal.set_disposition("r","f",record.record_id,"pending",record)
        evidence=verify_references(journal,"r","f")
        assert evidence["applied_cells"]==evidence["affected_records"]==1
        assert context.process(original)==[] #Same crash/reparse identity, same result, no duplicate event.
        assert journal.db.execute("SELECT COUNT(*) FROM events WHERE category='LOSSY_PROJECTION'").fetchone()[0]==1
        row=journal.db.execute("SELECT original_field,projected_field FROM projection_events").fetchone()
        assert json.loads(row[0])=={"state":"VALUE","text":"A😀B","quoted":True}
        assert json.loads(row[1])=={"state":"VALUE","text":"A","quoted":True}
        output=tmp_path/"events.jsonl"
        # A unit-only ledger without an approved source summary cannot export originals.
        with pytest.raises(IngestError) as unsealed:export(journal,"r",output)
        assert unsealed.value.code=="PROJECTION_EVIDENCE_MISMATCH" and not output.exists()
        journal.db.execute("UPDATE projection_events SET projected_field='{}'");journal.db.commit()
        with pytest.raises(IngestError) as error:verify_references(journal,"r","f")
        assert error.value.code=="PROJECTION_EVIDENCE_MISMATCH"
    finally:journal.close()
