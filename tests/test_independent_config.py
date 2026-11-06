"""Independent configuration policy tests authored from the acceptance contract."""
from pathlib import Path
import tempfile
import unittest
from db_ingest.config import load_config, hashes, parse_bool, resolve_columns
from db_ingest.errors import IngestError

class IndependentConfigTests(unittest.TestCase):
    def test_boolean_spellings_not_python_truthiness(self):
        for v in ['false','no','0',False]: self.assertIs(parse_bool(v),False)
        for v in ['true','yes','1',True]: self.assertIs(parse_bool(v),True)
        with self.assertRaises(IngestError):parse_bool('maybe')
    def test_environment_alias_conflict_and_cli_precedence(self):
        with self.assertRaises(IngestError) as cm: load_config(environ={'DB_TRUST_SERVICE_CERTIFICATE':'yes','DB_TRUST_SERVER_CERTIFICATE':'no'})
        self.assertEqual(cm.exception.code,'CONFIG_ALIAS_CONFLICT')
        config=load_config(overrides={'destination.encrypt':'yes'},environ={'DB_ENCRYPT':'no','DB_TRUST_SERVICE_CERTIFICATE':'false'})
        self.assertEqual(config['destination']['encrypt'],'yes')
        self.assertEqual(config['destination']['trust_server_certificate'],'no')
    def test_rotated_credentials_operational_changes_do_not_change_semantics(self):
        a=load_config(environ={'DB_PASSWORD':'first'})
        b=load_config(environ={'DB_PASSWORD':'second','DB_CONNECT_TIMEOUT':'99'},overrides={'parallel.max_workers':1})
        self.assertEqual(hashes(a)['interpretation'],hashes(b)['interpretation'])
        self.assertEqual(hashes(a)['mapping'],hashes(b)['mapping'])
        b['parsing']['null_tokens']=['NULL']
        self.assertNotEqual(hashes(a)['interpretation'],hashes(b)['interpretation'])
    def test_unknown_keys_and_invariant_rejected(self):
        with tempfile.TemporaryDirectory() as td:
            p=Path(td)/'config.toml'
            for body,code in [('[parsing]\nsilent_drop = true\n','CONFIG_UNKNOWN'),('[schema]\ninfer_defaults = true\n','CONFIG_INVARIANT')]:
                p.write_text(body,encoding='utf-8')
                with self.assertRaises(IngestError) as cm:load_config(p,environ={})
                self.assertEqual(cm.exception.code,code)
    def test_duplicate_header_requires_ordinal(self):
        c=load_config(environ={})
        c['columns']=[{'source_name':'a','target_type':'nvarchar(10)'}]
        with self.assertRaises(IngestError): resolve_columns(c,['a','a'])
        c['columns']=[{'source_name':'a','source_ordinal':2,'target_type':'nvarchar(10)'}]
        resolved=resolve_columns(c,['a','a'])
        self.assertEqual(resolved[0]['source_ordinal'],2)
        c['columns']=[{'source_name':'b','source_ordinal':2,'target_type':'nvarchar(10)'}]
        with self.assertRaises(IngestError):resolve_columns(c,['a','a'])
if __name__=='__main__':unittest.main()
