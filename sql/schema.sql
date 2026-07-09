-- ============================================================
-- Health Insurance Analytics DW - Schema
-- Layers: audit (observability) / bronze (raw) / silver (clean, SCD2)
--         / gold (star schema for Power BI)
-- All loads are idempotent: unique constraints + ON CONFLICT upserts.
-- ============================================================

CREATE SCHEMA IF NOT EXISTS audit;
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- ---------------- AUDIT ----------------
CREATE TABLE IF NOT EXISTS audit.pipeline_runs (
    run_id UUID PRIMARY KEY,
    pipeline_name TEXT NOT NULL,
    batch_date DATE NOT NULL,
    status TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS audit.pipeline_run_steps (
    id BIGSERIAL PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES audit.pipeline_runs(run_id),
    step_name TEXT NOT NULL,
    status TEXT NOT NULL,
    rows_in INT DEFAULT 0,
    rows_out INT DEFAULT 0,
    rows_rejected INT DEFAULT 0,
    duration_ms INT,
    finished_at TIMESTAMPTZ,
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS audit.quarantine (
    id BIGSERIAL PRIMARY KEY,
    run_id UUID NOT NULL,
    entity TEXT NOT NULL,
    raw_record JSONB NOT NULL,
    rejection_reason TEXT NOT NULL,
    quarantined_at TIMESTAMPTZ DEFAULT now()
);

-- Per-FILE tracking, separate from per-STEP tracking in pipeline_run_steps.
-- One row per source file per run: lets you answer "did claims.csv load
-- today, and if not, exactly why" without grepping container logs.
CREATE TABLE IF NOT EXISTS audit.file_ingestion_log (
    id BIGSERIAL PRIMARY KEY,
    run_id UUID NOT NULL,
    source_file TEXT NOT NULL,
    entity TEXT NOT NULL,
    status TEXT NOT NULL,               -- SUCCESS / FAILED / MISSING / PARTIAL
    file_size_bytes BIGINT,
    rows_read INT DEFAULT 0,
    rows_inserted INT DEFAULT 0,
    chunks_processed INT DEFAULT 0,
    error_message TEXT,
    started_at TIMESTAMPTZ DEFAULT now(),
    finished_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_file_ingestion_log_run ON audit.file_ingestion_log(run_id);
CREATE INDEX IF NOT EXISTS idx_file_ingestion_log_file ON audit.file_ingestion_log(source_file, started_at DESC);

-- ---------------- BRONZE (append-only, dedup by row hash) ----------------
CREATE TABLE IF NOT EXISTS bronze.patients_raw (
    row_hash TEXT PRIMARY KEY,          -- sha256 of raw row -> idempotent re-ingest
    source_file TEXT NOT NULL,
    batch_date DATE NOT NULL,
    run_id UUID NOT NULL,
    payload JSONB NOT NULL,
    ingested_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS bronze.claims_raw (
    row_hash TEXT PRIMARY KEY,
    source_file TEXT NOT NULL,
    batch_date DATE NOT NULL,
    run_id UUID NOT NULL,
    payload JSONB NOT NULL,
    ingested_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS bronze.encounters_raw (
    row_hash TEXT PRIMARY KEY,
    source_file TEXT NOT NULL,
    batch_date DATE NOT NULL,
    run_id UUID NOT NULL,
    payload JSONB NOT NULL,
    ingested_at TIMESTAMPTZ DEFAULT now()
);

-- ---------------- SILVER (conformed, SCD2 for patient dim) ----------------
CREATE TABLE IF NOT EXISTS silver.patients (
    surrogate_key BIGSERIAL PRIMARY KEY,
    patient_id TEXT NOT NULL,
    first_name TEXT,
    last_name TEXT,
    date_of_birth DATE,
    gender TEXT,
    plan_type TEXT,
    plan_start_date DATE,
    zip_code TEXT,
    chronic_conditions TEXT,
    effective_from TIMESTAMPTZ NOT NULL DEFAULT now(),
    effective_to TIMESTAMPTZ,
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    row_hash TEXT NOT NULL              -- hash of attributes -> detect real change vs re-ingest
);
CREATE INDEX IF NOT EXISTS idx_silver_patients_current
    ON silver.patients (patient_id) WHERE is_current;

CREATE TABLE IF NOT EXISTS silver.claims (
    claim_id TEXT PRIMARY KEY,          -- natural key -> upsert target (idempotent)
    patient_id TEXT NOT NULL,
    encounter_id TEXT,
    claim_date DATE NOT NULL,
    diagnosis_code TEXT,
    procedure_code TEXT,
    claim_amount NUMERIC(12,2),
    approved_amount NUMERIC(12,2),
    claim_status TEXT,
    provider_id TEXT,
    updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS silver.encounters (
    encounter_id TEXT PRIMARY KEY,
    patient_id TEXT NOT NULL,
    admit_date DATE NOT NULL,
    discharge_date DATE,
    encounter_type TEXT,
    facility_id TEXT,
    is_readmission BOOLEAN DEFAULT FALSE,
    length_of_stay_days INT,
    updated_at TIMESTAMPTZ DEFAULT now()
);

-- ---------------- GOLD (star schema) ----------------
CREATE TABLE IF NOT EXISTS gold.dim_patient (
    patient_key BIGSERIAL PRIMARY KEY,
    patient_id TEXT NOT NULL,
    full_name TEXT,
    age INT,
    age_band TEXT,
    gender TEXT,
    plan_type TEXT,
    zip_code TEXT,
    chronic_condition_count INT,
    is_chronic BOOLEAN,
    is_current BOOLEAN,
    UNIQUE (patient_id, is_current)
);

CREATE TABLE IF NOT EXISTS gold.dim_date (
    date_key DATE PRIMARY KEY,
    year INT, quarter INT, month INT, month_name TEXT, day INT,
    week_of_year INT, is_weekend BOOLEAN
);

CREATE TABLE IF NOT EXISTS gold.fact_claims (
    claim_id TEXT PRIMARY KEY,
    patient_id TEXT NOT NULL,
    claim_date DATE NOT NULL REFERENCES gold.dim_date(date_key),
    diagnosis_code TEXT,
    procedure_code TEXT,
    claim_amount NUMERIC(12,2),
    approved_amount NUMERIC(12,2),
    denied_amount NUMERIC(12,2),
    claim_status TEXT,
    provider_id TEXT
);
CREATE INDEX IF NOT EXISTS idx_fact_claims_patient ON gold.fact_claims(patient_id);
CREATE INDEX IF NOT EXISTS idx_fact_claims_date ON gold.fact_claims(claim_date);

CREATE TABLE IF NOT EXISTS gold.fact_encounters (
    encounter_id TEXT PRIMARY KEY,
    patient_id TEXT NOT NULL,
    admit_date DATE NOT NULL REFERENCES gold.dim_date(date_key),
    discharge_date DATE,
    encounter_type TEXT,
    facility_id TEXT,
    is_readmission BOOLEAN,
    length_of_stay_days INT
);
CREATE INDEX IF NOT EXISTS idx_fact_encounters_patient ON gold.fact_encounters(patient_id);
