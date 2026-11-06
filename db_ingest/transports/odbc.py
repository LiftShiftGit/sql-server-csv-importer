"""Bounded row/byte ODBC batches and explicit native bcp selection policy."""
from db_ingest.errors import IngestError
from db_ingest.resources import checkpoint


def choose_transport(config):
    requested=config.get("load",{}).get("engine","auto")
    if requested not in ("auto","bcp","odbc"):raise IngestError("ENGINE_INVALID",f"Unknown loader {requested!r}")
    preferred=config.get("load",{}).get("preferred_engine","bcp")
    if requested=="bcp" or (requested=="auto" and preferred=="bcp"):
        from .bcp import prerequisite
        ok,reason,tool=prerequisite(config)
        if ok:return {"engine":"bcp","requested":requested,"reason":reason,"tool":tool,"fast_executemany":False,"batching":"encoded reversible raw spool chunks, server typed decode, complete stage/target readback"}
        if requested=="bcp":raise IngestError("BCP_PREREQUISITE",reason+"; no ODBC fallback executed",2,{"tool":tool})
        return {"engine":"odbc","requested":requested,"reason":"Auto selected ODBC: "+reason,"bcp_tool":tool,"fast_executemany":bool(config.get("load",{}).get("fast_executemany",False)),"batching":"bounded parameter groups"}
    return {"engine":"odbc","requested":requested,"reason":"ODBC explicitly requested or preferred","fast_executemany":bool(config.get("load",{}).get("fast_executemany",False)),"batching":"bounded parameter groups"}


def parameter_bytes(value):
    if isinstance(value,str):return max(2,len(value.encode("utf-16-le")))
    if isinstance(value,(bytes,bytearray,memoryview)):return max(1,len(value))
    return 8


class OdbcBatchWriter:
    """Bounded groups with measured-driver fast arrays for small values.

    Driver18 rejects long-data fast-array descriptors (HY104). LOB rows are isolated
    into ordinary bounded executemany groups; the report exposes every such fallback.
    Fast-array admission uses largest-per-parameter stride, never source-byte sums.
    """
    def __init__(self,cursor,sql,config):
        self.cursor,self.sql,self.config=cursor,sql,config
        self.limit_rows=int(config.get("load",{}).get("batch_rows",5000));self.limit_bytes=int(config.get("load",{}).get("batch_bytes",8388608))
        self.fast=bool(config.get("load",{}).get("fast_executemany",False))
        self.rows=[];self.bytes=0;self.maximum=[];self.native=False
        self.summary={"count":0,"max_rows":0,"max_bytes":0,"limit_rows":self.limit_rows,"limit_bytes":self.limit_bytes,"kind":"parameter bytes" if not self.fast else "guarded native stride or isolated LOB parameter bytes","fast_executemany":self.fast,"fast_array_batches":0,"fallback_batches":0,"fallback_rows":0,"max_native_array_bytes":0}
    def _eligible(self,parameters):
        return self.fast and all(not (isinstance(v,str) and len(v.encode("utf-16-le"))>8000) and not (isinstance(v,(bytes,bytearray,memoryview)) and len(v)>8000) for v in parameters)
    def _shape(self,parameters,native):
        sizes=[parameter_bytes(value) for value in parameters]
        maximum=[max(a,b) for a,b in zip(self.maximum,sizes)] if self.maximum else sizes
        measured=(len(self.rows)+1)*(sum(maximum)+64*len(maximum)) if native else self.bytes+sum(sizes)
        return maximum,measured
    def add(self,parameters):
        checkpoint()
        native=self._eligible(parameters)
        if self.rows and native!=self.native:self.flush()
        maximum,measured=self._shape(parameters,native)
        if self.rows and (len(self.rows)>=self.limit_rows or measured>self.limit_bytes):
            self.flush();maximum,measured=self._shape(parameters,native)
        if measured>self.limit_bytes:raise IngestError("ODBC_RECORD_BUDGET","One parameter row/native-array stride exceeds load.batch_bytes",2,{"required_bytes":measured,"limit_bytes":self.limit_bytes})
        checkpoint(measured)
        self.rows.append(tuple(parameters));self.maximum=maximum;self.bytes=measured;self.native=native
    def flush(self):
        if not self.rows:return
        checkpoint(self.bytes,force=True)
        self.cursor.fast_executemany=self.native
        if self.native:
            import pyodbc
            specifications=[]
            for index,maximum in enumerate(self.maximum):
                sample=self.rows[0][index]
                if isinstance(sample,str):specifications.append((pyodbc.SQL_WVARCHAR,max(1,maximum//2),0))
                elif isinstance(sample,(bytes,bytearray,memoryview)):specifications.append((pyodbc.SQL_VARBINARY,max(1,maximum),0))
                else:specifications.append((pyodbc.SQL_BIGINT,19,0))
            self.cursor.setinputsizes(specifications)
        else:self.cursor.setinputsizes(None)
        self.cursor.executemany(self.sql,self.rows)
        from .faults import test_fault
        test_fault(self.config,"STAGING_SENT_BEFORE_ACK")
        self.summary["count"]+=1;self.summary["max_rows"]=max(self.summary["max_rows"],len(self.rows));self.summary["max_bytes"]=max(self.summary["max_bytes"],self.bytes)
        if self.native:
            self.summary["fast_array_batches"]+=1;self.summary["max_native_array_bytes"]=max(self.summary["max_native_array_bytes"],self.bytes)
        elif self.fast:
            self.summary["fallback_batches"]+=1;self.summary["fallback_rows"]+=len(self.rows);self.summary["fallback_reason"]="Driver18 long-data fast-array descriptors are not supported; LOB records use bounded ordinary executemany"
        self.rows=[];self.bytes=0;self.maximum=[];self.native=False
    def finish(self):self.flush();return self.summary


from .bcp import BcpStageLoader

class OdbcCanonicalLoader(BcpStageLoader):
    """Native arrays over four simple canonical raw columns, then exact server decode.

    Driver descriptor behavior is not entrusted with heterogeneous business types.
    The same D3 spool/raw verification/server decode used by bcp protects every value.
    """
    def __init__(self,*args,**kwargs):
        super().__init__(*args,**kwargs)
        self.execution={"count":0,"max_rows":0,"max_bytes":0,"fast_array_batches":0,"fallback_batches":0,"fallback_rows":0,"max_native_array_bytes":0}
        self.summary.update(kind="canonical ODBC native stride or isolated LOB bytes",fast_executemany=True,wire="D3 ASCII JSON/Base64; binary raw parameter columns")
        self.summary.pop("bcp_commit_rows",None)
    def _run(self,path,chunk):
        sql=f"INSERT INTO {self.raw} (record_token,ordinal,payload,row_sha256) VALUES (CONVERT(varchar(1024),?),?,CONVERT(varchar(max),CONVERT(varbinary(max),?)),CONVERT(char(64),?))"
        writer=OdbcBatchWriter(self.conn.cursor(),sql,self.config)
        with path.open("rb") as source:
            for line in iter(lambda:source.readline(self.limit_bytes+1),b""):
                checkpoint()
                if len(line)>self.limit_bytes or not line.endswith(b"\n"):raise IngestError("ODBC_CANONICAL_FRAMING","Bounded canonical row framing mismatch")
                token,ordinal,payload,digest=line[:-1].split(b"\t")
                writer.add([token,int(ordinal),payload,digest])
        result=writer.finish();self.conn.commit()
        chunk["odbc_parameter_batches"]=result
        for key in ("count","fast_array_batches","fallback_batches","fallback_rows"):self.execution[key]+=result[key]
        for key in ("max_rows","max_bytes","max_native_array_bytes"):self.execution[key]=max(self.execution[key],result[key])
        chunk["loader"]="ODBC parameter arrays/explicit bounded LOB fallback"
        return 0
    def finish(self):
        result=super().finish();result["wire_chunk_count"]=result["count"];result["wire_max_chunk_bytes"]=result["max_bytes"]
        result.update(self.execution)
        if result["fallback_batches"]:result["fallback_reason"]="Long-data canonical rows use bounded ordinary executemany; native arrays are limited to <=8000-byte fields"
        self._save("TYPED_STAGE_COMPLETE")
        return result
