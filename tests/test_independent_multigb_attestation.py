"""Authored chunk-ledger evidence; no SQL connections or importer parser/converter."""
import hashlib,json,runpy
from pathlib import Path
import pytest
API=runpy.run_path(str(Path(__file__).resolve().parents[1]/'scripts/verify_multigb_attestation.py'))

def fixture(tmp_path):
 root=tmp_path/'work/transport/attempt';root.mkdir(parents=True)
 entries=[]
 for index,(data,rows) in enumerate([(b'first\nsecond\n',2),(b'third\n',1)],1):
  path=root/('chunk_'+str(index)+'.bcp');path.write_bytes(data)
  entry={'index':index,'path':str(path),'rows':rows,'bytes':len(data),'sha256':hashlib.sha256(data).hexdigest()}
  entries.extend([{**entry,'state':'SPOOLED'},{**entry,'state':'TYPED_STAGE_DECODED'}])
 ledger=root/'chunks.jsonl';raw=b''.join((json.dumps(e)+'\n').encode() for e in entries);ledger.write_bytes(raw)
 return ledger,entries,{'chunk_ledger':{'path':str(ledger),'events':4,'chunk_count':2,'sha256':hashlib.sha256(raw).hexdigest()},'chunks':[]}

def test_all_rows_once_and_each_chunk_content_hash(tmp_path):
 _,_,summary=fixture(tmp_path);result=API['attest_chunks'](tmp_path,summary,3,2,100)
 assert result['rows']==3 and result['chunk_count']==2 and result['events']==4 and result['bytes_hashed']==19

def test_modified_same_length_chunk_cannot_pass(tmp_path):
 _,entries,summary=fixture(tmp_path);Path(entries[2]['path']).write_bytes(b'WRONG\n')
 with pytest.raises(ValueError,match='CHUNK_BYTES_OR_HASH_MISMATCH'):API['attest_chunks'](tmp_path,summary,3,2,100)

def test_spooled_without_typed_verification_cannot_pass_even_with_new_ledger_hash(tmp_path):
 path,entries,summary=fixture(tmp_path);raw=b''.join((json.dumps(e)+'\n').encode() for e in entries[:-1]);path.write_bytes(raw);summary['chunk_ledger'].update(events=3,sha256=hashlib.sha256(raw).hexdigest())
 with pytest.raises(ValueError,match='LEDGER_EVENT_COUNT'):API['attest_chunks'](tmp_path,summary,3,2,100)

def test_wrong_authored_total_cannot_pass(tmp_path):
 _,_,summary=fixture(tmp_path)
 with pytest.raises(ValueError,match='LEDGER_ACCEPTED_ROW_TOTAL'):API['attest_chunks'](tmp_path,summary,4,2,100)


def test_authored_payload_uses_bigint_and_note_is_slightly_below_four_gib():
 manifest={'rows':131073,'padding':16384,'accepted_rows':131070,'rejected_indexes':[4095,65535,100000],'column_names':['code','mixed','amount','note'],'expected_types':['nvarchar(11)','nvarchar(11)','decimal(23,19)','nvarchar(max)']}
 size=API['authored_storage_bytes'](manifest)
 assert size['last_note_bytes']==80048
 assert size['note_bytes']==4294949040 and 2**31<size['note_bytes']<2**32
 assert size['ordered_text_column_bytes']==[2359260,498076,4294949040]
 assert size['total_text_business_payload_bytes']==4297806376>2**32
 assert size['decimal_storage_expected'] is None
