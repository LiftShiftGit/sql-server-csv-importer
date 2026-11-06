"""Destination boundary forwards transaction ownership and rejects unavailable engines."""
import pytest
from db_ingest.adapters.base import get_adapter,DestinationAdapter
from db_ingest.config import load_config
from db_ingest.errors import IngestError

def test_complete_mssql_contract_and_unavailable_adapters():
    config=load_config(environ={});adapter=get_adapter(config)
    assert isinstance(adapter,DestinationAdapter) and adapter.name=='mssql'
    assert adapter.quote_identifier('a]b')=='[a]]b]'
    assert str(adapter.map_type('numeric(38,7)'))=='decimal(38,7)'
    for kind in ('mysql','postgresql','sqlite','unknown'):
        config['destination']['type']=kind
        with pytest.raises(IngestError) as error:get_adapter(config)
        assert error.value.code=='DESTINATION_UNSUPPORTED'

def test_actual_publish_and_receipt_implementations_keep_the_same_arguments(monkeypatch):
    adapter=get_adapter(load_config(environ={}));seen=[]
    monkeypatch.setattr('db_ingest.publication.publish',lambda *args:seen.append(args) or 'receipt')
    monkeypatch.setattr('db_ingest.publication.find_receipt',lambda *args:seen.append(args) or None)
    assert adapter.stage_validate_publish('config','plan','records','intent','run','user','secret')=='receipt'
    assert seen[-1]==('config','plan','records','intent','run','user','secret')
    assert adapter.read_receipt('config','plan','intent','user','secret') is None
    assert seen[-1]==('config','plan','intent','user','secret')

def test_adapter_native_diagnostics_preserve_observed_provider_facts():
    adapter=get_adapter(load_config(environ={}))
    error=adapter.categorize_error(Exception('42000','secret log full (9002)'),'publish',('secret',))
    assert error.code=='LOG_CAPACITY' and error.context['native_code']==9002
    assert 'secret' not in str(error) and error.context['retryable'] is False
