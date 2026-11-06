"""SQL Server facade over tested transaction-owning implementations.

Staging and validation stay with publication so a portable interface cannot imply
that ODBC/bcp/sqlcmd sessions share a transaction. Methods import lazily to keep
connectivity and model modules independent of publication orchestration.
"""
class SqlServerAdapter:
    name='mssql'
    def quote_identifier(self,name):
        from .ddl import quote_identifier
        return quote_identifier(name)
    def capabilities(self,config,username=None,password=None):
        from .connect import test_connection
        return test_connection(config,username,password,False)
    def connect(self,config,username=None,password=None,database=None,autocommit=False):
        from .connect import connect
        return connect(config,username,password,database,autocommit)
    def inspect_schema(self,cursor,plan):
        from .evolution import inspection
        from .ddl import qualified
        return inspection(cursor,qualified(plan.schema,plan.table))
    def map_type(self,type_name):
        from .types import parse_sql_type
        return parse_sql_type(type_name)
    def bootstrap(self,config,database,username=None,password=None):
        from db_ingest.publication import _bootstrap_database,_bootstrap_metadata
        connection=_bootstrap_database(config,database,username,password)
        try:_bootstrap_metadata(connection,config)
        finally:connection.close()
    def stage_validate_publish(self,config,plan,records,intent_id,run_id,username=None,password=None):
        from db_ingest.publication import publish
        return publish(config,plan,records,intent_id,run_id,username,password)
    def read_receipt(self,config,plan,intent_id,username=None,password=None):
        from db_ingest.publication import find_receipt
        return find_receipt(config,plan,intent_id,username,password)
    def resolve_publication(self,config,plan,intent_id,known_receipts=(),username=None,password=None,current_receipt=None):
        from db_ingest.publication import resolve_publication
        return resolve_publication(config,plan,intent_id,known_receipts,username,password,current_receipt)
    def reconcile(self,config,plan,receipt,username=None,password=None):
        from db_ingest.reconciliation import verify_destination
        return verify_destination(config,plan,receipt,username,password)
    def categorize_error(self,error,operation,secrets=(),exit_code=4):
        from .diagnostics import provider_error
        return provider_error(error,operation,secrets,exit_code)
