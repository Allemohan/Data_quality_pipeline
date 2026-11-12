-- ============================================================================
-- DATA QUALITY & RECONCILIATION VERIFICATION QUERIES
-- ============================================================================

-- 1. Target Table Aggregate Count
SELECT COUNT(*) AS total_customers_in_target
FROM customers;

-- 2. Quarantine Status Breakdown
SELECT 
    status, 
    COUNT(*) AS record_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) AS percentage
FROM quarantine_records
GROUP BY status
ORDER BY record_count DESC;

-- 3. Top Rejection Error Codes Histogram
SELECT 
    error_code, 
    validation_stage,
    COUNT(*) AS occurrence_count
FROM quarantine_records
GROUP BY error_code, validation_stage
ORDER BY occurrence_count DESC;

-- 4. Target Uniqueness & Duplicate Key Verification (MUST BE 0 ROWS)
SELECT 
    customer_id, 
    COUNT(*) AS count_of_instances
FROM customers
GROUP BY customer_id
HAVING COUNT(*) > 1;

-- 5. Referential Integrity Check: Orphan Customer Types (MUST BE 0 ROWS)
SELECT 
    c.customer_id, 
    c.customer_name, 
    c.customer_type_id
FROM customers c
LEFT JOIN customer_types ct ON c.customer_type_id = ct.customer_type_id
WHERE ct.customer_type_id IS NULL;

-- 6. Pipeline Execution Audit & Mathematical Reconciliation
-- Verifies: input_count = valid_count + rejected_count + duplicate_count
SELECT 
    run_id,
    source_file,
    started_at,
    completed_at,
    status,
    input_count,
    valid_count,
    rejected_count,
    duplicate_count,
    (valid_count + rejected_count + duplicate_count) AS calculated_sum,
    CASE 
        WHEN input_count = (valid_count + rejected_count + duplicate_count) THEN 'PASS'
        ELSE 'FAIL_DISCREPANCY'
    END AS reconciliation_audit,
    processing_time_seconds,
    ROUND(input_count / NULLIF(processing_time_seconds, 0), 1) AS records_per_second
FROM pipeline_runs
ORDER BY started_at DESC;

-- 7. Controlled Replay Audit Summary
SELECT 
    r.status AS replay_status,
    COUNT(r.replay_run_id) AS replay_attempts,
    COUNT(DISTINCT r.quarantine_id) AS unique_quarantined_records_replayed,
    COUNT(r.target_record_id) AS loaded_to_target_count
FROM replay_runs r
GROUP BY r.status;

-- 8. Source-to-Target Lineage Tracing Check
-- Traces sample target records back to their exact source file and line
SELECT 
    customer_id,
    customer_name,
    email,
    source_file,
    source_row_number,
    run_id,
    created_at,
    updated_at
FROM customers
LIMIT 10;

