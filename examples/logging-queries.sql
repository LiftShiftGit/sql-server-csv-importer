-- Run in destination.control_database (SSMS database dropdown).
-- Replace every [audit] if audit.control_schema is different.
-- Read-only. Requires SELECT on these audit tables; JSON compatibility level >= 130.
SET NOCOUNT ON;
DECLARE @RunId nvarchar(64) = NULL; -- Paste a run ID in N'...' or leave NULL for latest.
DECLARE @Limit int = 100;          -- Preview limit, not a completeness assertion.

-- 1. Recent runs. updated_at_utc is a mirror update, NOT the run start/end time.
SELECT TOP (@Limit) run_id, state, updated_at_utc
FROM [audit].[Runs] ORDER BY updated_at_utc DESC, run_id;
IF @RunId IS NULL
    SELECT TOP (1) @RunId = run_id FROM [audit].[Runs] ORDER BY updated_at_utc DESC, run_id;
IF @RunId IS NULL OR NOT EXISTS (SELECT 1 FROM [audit].[Runs] WHERE run_id = @RunId)
    THROW 50001, 'No matching mirrored run. Use local CLI status/show-errors or check the database and run ID.', 1;

-- 2. Selected run totals. NULL means unknown/unobserved, not zero.
SELECT run_id, state, updated_at_utc,
    TRY_CONVERT(bigint, JSON_VALUE(summary_json, '$.files')) AS files,
    TRY_CONVERT(bigint, JSON_VALUE(summary_json, '$.source_bytes')) AS source_bytes,
    TRY_CONVERT(bigint, JSON_VALUE(summary_json, '$.physical_lines')) AS physical_lines,
    TRY_CONVERT(bigint, JSON_VALUE(summary_json, '$.logical_records')) AS logical_records,
    TRY_CONVERT(bigint, JSON_VALUE(summary_json, '$.counts.committed')) AS committed_records,
    TRY_CONVERT(bigint, JSON_VALUE(summary_json, '$.counts.rejected')) AS rejected_records,
    TRY_CONVERT(bigint, JSON_VALUE(summary_json, '$.counts.pending')) AS pending_records,
    TRY_CONVERT(bigint, JSON_VALUE(summary_json, '$.counts.unresolved')) AS unresolved_spans,
    TRY_CONVERT(bigint, JSON_VALUE(summary_json, '$.measures.record_error_events')) AS record_error_events,
    TRY_CONVERT(bigint, JSON_VALUE(summary_json, '$.execution_policy.sampled_peak_process_tree_rss_bytes')) AS sampled_run_peak_bytes
FROM [audit].[Runs] WHERE run_id = @RunId;

-- 3. Per-file statistics, destination, capture location and generated failure-table reference.
SELECT TOP (@Limit) file_id, source_path, source_hash, artifact_path,
    JSON_VALUE(summary_json, '$.state') AS file_state,
    JSON_VALUE(summary_json, '$.intent_id') AS intent_id,
    JSON_VALUE(summary_json, '$.destination.database') AS destination_database,
    JSON_VALUE(summary_json, '$.destination.schema') AS target_schema,
    JSON_VALUE(summary_json, '$.destination.table') AS target_table,
    TRY_CONVERT(bigint, JSON_VALUE(summary_json, '$.measures.source_bytes')) AS source_bytes,
    TRY_CONVERT(bigint, JSON_VALUE(summary_json, '$.measures.parsed_data_records')) AS parsed_records,
    TRY_CONVERT(bigint, JSON_VALUE(summary_json, '$.measures.committed')) AS committed_records,
    TRY_CONVERT(bigint, JSON_VALUE(summary_json, '$.measures.rejected')) AS rejected_records,
    TRY_CONVERT(bigint, JSON_VALUE(summary_json, '$.measures.pending')) AS pending_records,
    TRY_CONVERT(bigint, JSON_VALUE(summary_json, '$.measures.unresolved')) AS unresolved_spans,
    JSON_VALUE(summary_json, '$.publication_assurance') AS assurance,
    JSON_QUERY(summary_json, '$.failure_table') AS failure_table_reference
FROM [audit].[Files] WHERE run_id = @RunId ORDER BY file_id;

-- 4. Error categories: one record can have several error events/categories.
SELECT code, COUNT_BIG(*) AS error_events, COUNT_BIG(DISTINCT record_id) AS affected_records
FROM [audit].[RecordErrors] WHERE run_id = @RunId GROUP BY code ORDER BY error_events DESC, code;

-- 5. Rejected record details. Ordered field payloads may contain sensitive source values.
-- Multiple rows per record are intentional here: one per error.
SELECT TOP (@Limit) r.file_id, r.record_id, r.logical_ordinal, r.byte_start, r.byte_end,
    e.error_ordinal, e.code, e.column_ordinal, r.payload_json, e.diagnostic_json
FROM [audit].[RejectedRecords] AS r
LEFT JOIN [audit].[RecordErrors] AS e ON e.run_id = r.run_id AND e.record_id = r.record_id
WHERE r.run_id = @RunId ORDER BY r.file_id, r.logical_ordinal, r.record_id, e.error_ordinal;

-- 6. File/run incidents, including failures that are not attributable to one CSV row.
SELECT TOP (@Limit) incident_id, file_id, code, diagnostic_json
FROM [audit].[FileIncidents] WHERE run_id = @RunId ORDER BY incident_id;

-- 7. Latest events; details_json includes the full retained event detail.
SELECT TOP (@Limit) recorded_at_utc, category, event_id, details_json
FROM [audit].[Events] WHERE run_id = @RunId ORDER BY recorded_at_utc DESC, event_id;

-- 8. Phase timings include retries/failed attempts. Parallel phases overlap;
-- do NOT sum this column and call it elapsed wall-clock duration or compute run rows/sec from it.
SELECT TOP (@Limit) f.file_id, p.phase, p.elapsed_seconds_all_attempts, p.attempts, p.succeeded_attempts
FROM [audit].[Files] AS f
CROSS APPLY OPENJSON(f.summary_json, '$.phase_timings') WITH (
    phase nvarchar(128) '$.phase',
    elapsed_seconds_all_attempts float '$.elapsed_seconds_all_attempts',
    attempts int '$.attempts', succeeded_attempts int '$.succeeded_attempts'
) AS p
WHERE f.run_id = @RunId ORDER BY f.file_id, p.phase;

-- 9. Central receipt mirror: one row per publication intent (not one per file in a group).
-- committed_records is this publication's count, NOT the target's current total row count.
SELECT TOP (@Limit) intent_id, destination_database, target, committed_records,
    assurance, recorded_at_utc, source_hash, value_digest, receipt_json
FROM [audit].[PublicationReceipts] WHERE run_id = @RunId ORDER BY recorded_at_utc DESC, intent_id;

-- 10. Column types/profiles; retain interpretation_hash when comparing different interpretations.
SELECT TOP (@Limit) p.file_id, p.interpretation_hash, p.ordinal, p.original_name,
    p.target_name, p.type_name, p.profile_json
FROM [audit].[ColumnProfiles] AS p
JOIN [audit].[Files] AS f ON f.file_id = p.file_id AND f.run_id = @RunId
ORDER BY p.file_id, p.interpretation_hash, p.ordinal;
