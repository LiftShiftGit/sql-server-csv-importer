"""Read-only installed-runtime and deployment preflight; exits nonzero on any failure.

Run with the core installed interpreter using -I. No test-library dependency.
The optional SQL probe observes connections/permissions; it does not publish data.
"""
import argparse,hashlib,json,os,platform,sys
from pathlib import Path


def main(argv=None):
 p=argparse.ArgumentParser(description=__doc__)
 p.add_argument('--config',required=True,type=Path)
 p.add_argument('--output',required=True,type=Path)
 p.add_argument('--source-root',type=Path,help='Require byte-for-byte installed .py/.toml parity with this release source')
 p.add_argument('--sql',action='store_true',help='Also observe configured SQL connections using DB_USERNAME/DB_PASSWORD or integrated auth')
 a=p.parse_args(argv)
 import db_ingest
 from db_ingest.config import load_config
 from db_ingest.adapters.mssql.connect import doctor,test_connection
 from db_ingest.errors import IngestError
 root=Path(db_ingest.__file__).resolve().parent
 checks=[];sql_executed=False
 def check(name,passed,**details):checks.append({'check':name,'passed':bool(passed),**details})
 check('isolated_interpreter',sys.flags.isolated==1)
 check('windows_64bit_supported_python',platform.system()=='Windows' and sys.maxsize>2**32 and sys.version_info>=(3,12))
 check('installed_package',root.parent.name=='site-packages',path=str(root))
 files={p.relative_to(root).as_posix():hashlib.sha256(p.read_bytes()).hexdigest() for p in root.rglob('*') if p.suffix in ('.py','.toml')}
 if a.source_root:
  source=a.source_root.resolve()/'db_ingest'
  expected={p.relative_to(source).as_posix():hashlib.sha256(p.read_bytes()).hexdigest() for p in source.rglob('*') if p.suffix in ('.py','.toml')}
  check('release_source_parity',files==expected,missing=sorted(set(expected)-set(files)),extra=sorted(set(files)-set(expected)),changed=sorted(k for k in files.keys()&expected.keys() if files[k]!=expected[k]))
 try:
  c=load_config(a.config)
  from db_ingest.pipeline import _preflight_features
  _preflight_features(c)
  check('configuration',True)
  d=doctor(c);check('driver_and_filesystem',d['exit_code']==0,details=d)
  if c['load']['engine']=='bcp':
   from db_ingest.transports.bcp import prerequisite
   # Selection must enforce credentials, executable and tested TLS support.
   ok,reason,capability=prerequisite(c)
   check('explicit_bcp',ok,reason=reason,capability=capability)
  if a.sql and all(x["passed"] for x in checks):
   sql_executed=True
   d=test_connection(c,c['destination'].get('username'),c['destination'].get('password'))
   check('sql_connections',d['exit_code']==0,details=d)
 except IngestError as e:check('configuration_or_probe',False,code=e.code)
 except Exception as e:check('preflight_exception',False,type=type(e).__name__)
 result={'state':'PASS' if all(x['passed'] for x in checks) else 'FAIL','python':sys.version,'runtime_files':files,'checks':checks,'sql_requested':a.sql,'sql_executed':sql_executed,'limits':['No load/recovery/throughput certification','Read-only filesystem metadata does not prove service-account write access','Domain auth and native Windows SQL hosting require their own environment acceptance']}
 # Never persist environment variables or raw exception messages.
 a.output.parent.mkdir(parents=True,exist_ok=True);a.output.write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')
 print(json.dumps({'state':result['state'],'report':str(a.output.resolve())}))
 return 0 if result['state']=='PASS' else 2

if __name__=='__main__':raise SystemExit(main())
