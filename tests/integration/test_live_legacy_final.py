"""Final new-format scope, separately from historical fixture regression reports."""
import os,runpy
from pathlib import Path
import pytest
ROOT=Path(__file__).resolve().parents[2]
API=runpy.run_path(str(ROOT/'tests/integration/test_live_fixture_categories.py'))
pytestmark=pytest.mark.skipif(not os.environ.get('DB_INGEST_TEST_CONFIG'),reason='Disposable SQL required')
@pytest.mark.parametrize('engine',['odbc','bcp'])
@pytest.mark.parametrize('category',['csv_escaping','csv_dirty','csv_legacy_encodings','mainframe_ebcdic'])
def test_final_authored_export_values_bytes_schema_and_negative_outcomes(engine,category):
    API['test_generated_category_full_authored_schema_values_outcomes_and_sql_absence'](engine,category)
