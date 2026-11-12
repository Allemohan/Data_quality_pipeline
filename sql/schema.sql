-- ============================================================================
-- DATA QUALITY QUARANTINE & REPLAY PIPELINE SCHEMA
-- ============================================================================

-- 1. Reference Table: customer_types
CREATE TABLE IF NOT EXISTS customer_types (
    customer_type_id INTEGER PRIMARY KEY,
    type_name VARCHAR(50) NOT NULL UNIQUE,
    description TEXT,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- 2. Target Table: customers
CREATE TABLE IF NOT EXISTS customers (
    customer_id INTEGER PRIMARY KEY,
    customer_name VARCHAR(100) NOT NULL,
    email VARCHAR(255) NOT NULL,
    phone VARCHAR(50),
    country_code VARCHAR(10),
    customer_type_id INTEGER NOT NULL REFERENCES customer_types(customer_type_id),
    registration_date DATE NOT NULL,
    status VARCHAR(20) NOT NULL,
    source_file VARCHAR(255) NOT NULL,
    source_row_number INTEGER NOT NULL,
    run_id VARCHAR(100) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Indexes on target table for lookups and reconciliation
CREATE INDEX IF NOT EXISTS idx_customers_email ON customers(email);
CREATE INDEX IF NOT EXISTS idx_customers_run_id ON customers(run_id);
CREATE INDEX IF NOT EXISTS idx_customers_status ON customers(status);

-- 3. Pipeline Runs Table (Audit & Observability)
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id VARCHAR(100) PRIMARY KEY,
    source_file VARCHAR(255) NOT NULL,
    started_at TIMESTAMP NOT NULL,
    completed_at TIMESTAMP,
    status VARCHAR(30) NOT NULL,
    input_count INTEGER NOT NULL DEFAULT 0,
    valid_count INTEGER NOT NULL DEFAULT 0,
    rejected_count INTEGER NOT NULL DEFAULT 0,
    duplicate_count INTEGER NOT NULL DEFAULT 0,
    processing_time_seconds NUMERIC(10, 4) DEFAULT 0.0,
    error_message TEXT
);

-- 4. Quarantine Records Table
CREATE TABLE IF NOT EXISTS quarantine_records (
    quarantine_id VARCHAR(36) PRIMARY KEY,
    source_record_id VARCHAR(100),
    source_file VARCHAR(255) NOT NULL,
    source_row_number INTEGER NOT NULL,
    run_id VARCHAR(100) NOT NULL,
    raw_payload TEXT NOT NULL,
    error_code VARCHAR(100) NOT NULL,
    error_message TEXT NOT NULL,
    validation_stage VARCHAR(50) NOT NULL,
    status VARCHAR(30) NOT NULL DEFAULT 'QUARANTINED',
    retry_count INTEGER NOT NULL DEFAULT 0,
    first_seen_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_seen_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    corrected_at TIMESTAMP,
    replayed_at TIMESTAMP
);

-- Indexes for quarantine queries and replay selection
CREATE INDEX IF NOT EXISTS idx_quarantine_status ON quarantine_records(status);
CREATE INDEX IF NOT EXISTS idx_quarantine_run_id ON quarantine_records(run_id);
CREATE INDEX IF NOT EXISTS idx_quarantine_error_code ON quarantine_records(error_code);
CREATE INDEX IF NOT EXISTS idx_quarantine_source_rec ON quarantine_records(source_record_id);

-- 5. Replay Runs Table (Replay Audit Trail)
CREATE TABLE IF NOT EXISTS replay_runs (
    replay_run_id VARCHAR(36) PRIMARY KEY,
    quarantine_id VARCHAR(36) NOT NULL REFERENCES quarantine_records(quarantine_id),
    original_run_id VARCHAR(100) NOT NULL,
    replay_started_at TIMESTAMP NOT NULL,
    replay_completed_at TIMESTAMP,
    status VARCHAR(30) NOT NULL,
    old_error VARCHAR(100),
    new_error VARCHAR(100),
    target_record_id VARCHAR(100)
);

CREATE INDEX IF NOT EXISTS idx_replay_quarantine_id ON replay_runs(quarantine_id);

