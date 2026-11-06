"""Sanitized provider diagnostics; no unsupported claims about hidden server reasons."""
import re
from datetime import datetime,timezone
from db_ingest.errors import IngestError

NATIVE = {
 701: ("SERVER_MEMORY", "SQL Server reported insufficient memory in its resource pool. Inspect active grants/compile pressure and reduce concurrent staging; do not assume a network/authentication problem or silently raise limits."),
 8645: ("SERVER_MEMORY_GRANT", "A query timed out waiting for a memory grant. Inspect actual grants and bounded staging admission before retry."),
 18456: ("AUTHENTICATION", "Check the login and server-side login failure log; the hidden reason is not inferred."),
 4060: ("DATABASE_ACCESS", "Check database name, online state and CONNECT permission; absence is not established."),
 229: ("PERMISSION", "Request the named operation permission from the database administrator."),
 262: ("PERMISSION", "Database or object creation permission is unavailable."),
 8152: ("LENGTH", "Compare the destination storage length with the retained source value."),
 2628: ("LENGTH", "Compare the reported column storage length with the retained source value."),
 245: ("TYPE", "Correct the explicit type policy or quarantine the retained source record."),
 8114: ("TYPE", "Check the exact declared type and lexical representation."),
 8115: ("RANGE", "Widen the exact type or retain as Unicode text under a new interpretation."),
 515: ("NULLABILITY", "A NULL reached a required destination column; do not invent a default."),
 2601: ("UNIQUE", "Resolve the unique-index conflict; duplicates remain distinct source records."),
 2627: ("UNIQUE", "Resolve the primary/unique-key conflict without dropping source records."),
 547: ("CONSTRAINT", "Inspect the named CHECK or foreign-key constraint."),
 1205: ("DEADLOCK", "Resolve the prior transaction before a bounded retry."),
 1222: ("LOCK_TIMEOUT", "Inspect the blocker and reconcile transaction state before retry."),
 9002: ("LOG_CAPACITY", "Ask the DBA to inspect log reuse, log space and recovery policy."),
 1105: ("STORAGE_CAPACITY", "Ask the DBA to inspect filegroup and volume capacity."),
}

def sanitize(text, secrets=()):
    result = str(text)
    for secret in sorted((str(x) for x in secrets if x), key=len, reverse=True):
        result = result.replace(secret, "<redacted>")
    return result

def _native_codes(args):
    """Read diagnostic suffixes, excluding quoted values and truncated quoted tails."""
    codes = []
    for arg in args:
        message = str(arg)
        # Providers may echo apostrophes literally (it's) or doubled (it''s).
        quoted = r"'(?:(?:'')|(?:(?<=\w)'(?=\w))|[^'])*(?:'|$)|\"(?:(?:\"\")|[^\"])*(?:\"|$)"
        outside_quotes = re.sub(quoted, lambda m: " " * len(m.group()), message)
        for match in re.finditer(
                r"\((-?\d+)\)(?:\s+\(SQL[A-Za-z0-9_]+\))?\s*(?=;|$)", message):
            if outside_quotes[match.start():match.start()+1] == "(":
                codes.append(int(match.group(1)))
    return codes


def provider_error(exc, operation, secrets=(), exit_code=4):
    args = getattr(exc, "args", ())
    raw = sanitize(" | ".join(str(x) for x in args) or str(exc), secrets)
    state = args[0] if args and re.fullmatch(r"[A-Z0-9]{5}", str(args[0])) else None
    nums = _native_codes(args)
    code, advice = "DATABASE", "Inspect the exact provider diagnostic, server reachability and permissions; TLS policy is unchanged."
    # ODBC can report the accompanying generic 18456 before the specific 4060.
    # An explicit inaccessible-database diagnostic is more informative than generic
    # login failure; it does not prove the database is absent.
    native = next((n for n in reversed(nums) if n in NATIVE), None)
    if native == 18456 and 4060 in nums and state == "28000":
        native = 4060
    if native is not None:
        code, advice = NATIVE[native]
    elif "certificate" in raw.lower() or "ssl provider" in raw.lower():
        code, advice = "TLS", "Check certificate chain, expiry and hostname; TLS policy has not been weakened."
    elif state in ("HYT00", "HYT01"):
        code, advice = "TIMEOUT", "Check endpoint and timeout; do not infer rollback from a timeout."
    return IngestError(code, f"{operation}: {raw}", exit_code, {"operation":operation,"sqlstate":state,"native_code":native,"provider":"pyodbc","diagnostic":raw,"stage":operation,"category":code.lower(),"exit_code":exit_code,"policy":{"commit_resolution":"required before replay","tls_policy":"unchanged; see effective configuration"},"tool":"Microsoft ODBC Driver 18 via pyodbc","expected":None,"original_name":None,"column_ordinal":None,"recorded_at_utc":datetime.now(timezone.utc).isoformat(),"provider_diagnostic_completeness":"unknown; provider may truncate diagnostics","observed":{"sqlstate":state,"native_code":native,"diagnostic":raw},"likely_cause":"Only the supplied provider facts are established","remediation":advice,"retryable":native in (1205,1222),"committed":"unknown" if exit_code == 5 else "not established"})
