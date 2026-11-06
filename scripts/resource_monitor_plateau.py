"""Independent long-running Windows monitor allocation/handle plateau probe."""
import argparse,ctypes,gc,json,os,sys,time
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import psutil
from db_ingest.resources import process_tree_memory


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--samples',type=int,default=10000);parser.add_argument('--output',type=Path,required=True);args=parser.parse_args()
    if os.name!='nt':raise SystemExit('Windows monitor plateau is only certified on Windows')
    if not 1<=args.samples<=100000:raise SystemExit('Use1..100000 bounded samples')
    process=psutil.Process()
    for _ in range(32):process_tree_memory()
    gc.collect();before=process.memory_info().rss;handles=process.num_handles();cache=len(ctypes._pointer_type_cache)
    observations=[];started=time.perf_counter()
    for index in range(1,args.samples+1):
        snapshot=process_tree_memory()
        assert not snapshot.unavailable_pids
        assert snapshot.processes[os.getpid()]>0
        if index%1000==0 or index==args.samples:
            gc.collect();row={'samples':index,'rss_bytes':process.memory_info().rss,'handles':process.num_handles(),'pointer_cache':len(ctypes._pointer_type_cache)};observations.append(row)
            print(json.dumps(row),flush=True)
    result={'samples':args.samples,'warmup_samples':32,'pointer_cache_before':cache,'pointer_cache_after':len(ctypes._pointer_type_cache),'pointer_cache_growth':len(ctypes._pointer_type_cache)-cache,'rss_before':before,'rss_after':process.memory_info().rss,'rss_growth_bytes':process.memory_info().rss-before,'handles_before':handles,'handles_after':process.num_handles(),'seconds':time.perf_counter()-started,'observations':observations}
    result['passed']=result['pointer_cache_growth']==0 and max(r['rss_bytes'] for r in observations)-before<=8388608 and max(r['handles'] for r in observations)-handles<=4
    args.output.write_text(json.dumps(result,indent=2),encoding='utf-8');print(json.dumps(result),flush=True)
    return 0 if result['passed'] else 1

if __name__=='__main__':raise SystemExit(main())
