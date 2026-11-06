-- Run in destination.database, NOT destination.control_database.
-- Replace [audit] if audit.control_schema is different.
-- This is a bounded read-only inspection, not a substitute for CLI reconcile.
SET NOCOUNT ON;
DECLARE @RunId nvarchar(64) = NULL; -- N'paste-run-id'; NULL previews receipts across runs.
SELECT TOP (100) intent_id, target, target_object_id, database_guid,
    JSON_VALUE(payload, '$.run_id') AS run_id,
    TRY_CONVERT(bigint, JSON_VALUE(payload, '$.committed_records')) AS committed_records,
    JSON_VALUE(payload, '$.assurance') AS assurance,
    JSON_VALUE(payload, '$.value_digest') AS value_digest,
    payload AS complete_receipt_json
FROM [audit].[__db_ingest_receipts]
WHERE @RunId IS NULL OR JSON_VALUE(payload, '$.run_id') = @RunId
ORDER BY intent_id;
