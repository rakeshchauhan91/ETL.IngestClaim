-- ============================================================
-- Business-facing analytical views. Power BI connects to these directly
-- (DirectQuery or Import). Each view maps to a report page - see
-- powerbi/README.md for the report design and DAX.
-- ============================================================

-- 1) Monthly cost trend & PMPM (Per Member Per Month) - core CFO metric
CREATE OR REPLACE VIEW gold.vw_monthly_cost_trend AS
SELECT
    date_trunc('month', c.claim_date)::date AS month,
    COUNT(DISTINCT c.patient_id) AS members_with_claims,
    COUNT(*) AS claim_count,
    SUM(c.claim_amount) AS total_claimed,
    SUM(c.approved_amount) AS total_approved,
    SUM(c.approved_amount) / NULLIF(COUNT(DISTINCT c.patient_id), 0) AS pmpm_approved
FROM gold.fact_claims c
GROUP BY 1
ORDER BY 1;

-- 2) Chronic disease prevalence & cost burden - shows which conditions drive spend
CREATE OR REPLACE VIEW gold.vw_chronic_condition_cost AS
SELECT
    LEFT(c.diagnosis_code, 3) AS diagnosis_group,
    COUNT(DISTINCT c.patient_id) AS patients_affected,
    COUNT(*) AS claim_count,
    SUM(c.approved_amount) AS total_approved,
    ROUND(AVG(c.approved_amount), 2) AS avg_claim_value
FROM gold.fact_claims c
GROUP BY 1
ORDER BY total_approved DESC;

-- 3) Readmission rate by facility - quality/cost signal for network management
CREATE OR REPLACE VIEW gold.vw_readmission_rate_by_facility AS
SELECT
    e.facility_id,
    COUNT(*) AS total_encounters,
    SUM(CASE WHEN e.is_readmission THEN 1 ELSE 0 END) AS readmissions,
    ROUND(100.0 * SUM(CASE WHEN e.is_readmission THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 1)
        AS readmission_rate_pct,
    ROUND(AVG(e.length_of_stay_days), 1) AS avg_length_of_stay
FROM gold.fact_encounters e
GROUP BY 1
ORDER BY readmission_rate_pct DESC;

-- 4) High-cost member concentration (Pareto) - identifies care-management targets
CREATE OR REPLACE VIEW gold.vw_patient_cost_ranking AS
SELECT
    p.patient_id,
    p.full_name,
    p.age_band,
    p.plan_type,
    p.is_chronic,
    SUM(c.approved_amount) AS total_approved,
    RANK() OVER (ORDER BY SUM(c.approved_amount) DESC) AS cost_rank,
    ROUND(100.0 * SUM(SUM(c.approved_amount)) OVER (ORDER BY SUM(c.approved_amount) DESC)
        / NULLIF(SUM(SUM(c.approved_amount)) OVER (), 0), 1) AS cumulative_cost_pct
FROM gold.fact_claims c
JOIN gold.dim_patient p ON p.patient_id = c.patient_id AND p.is_current
GROUP BY p.patient_id, p.full_name, p.age_band, p.plan_type, p.is_chronic
ORDER BY total_approved DESC;

-- 5) Enrollment mix by plan type & age band - product/pricing decisions
CREATE OR REPLACE VIEW gold.vw_enrollment_mix AS
SELECT
    plan_type,
    age_band,
    gender,
    COUNT(*) AS member_count,
    SUM(CASE WHEN is_chronic THEN 1 ELSE 0 END) AS chronic_member_count
FROM gold.dim_patient
WHERE is_current
GROUP BY 1, 2, 3;

-- 6) Claim denial analysis - operational/revenue leakage
CREATE OR REPLACE VIEW gold.vw_denial_analysis AS
SELECT
    date_trunc('month', claim_date)::date AS month,
    provider_id,
    COUNT(*) FILTER (WHERE claim_status = 'DENIED') AS denied_claims,
    COUNT(*) AS total_claims,
    ROUND(100.0 * COUNT(*) FILTER (WHERE claim_status = 'DENIED') / NULLIF(COUNT(*), 0), 1)
        AS denial_rate_pct,
    SUM(claim_amount) FILTER (WHERE claim_status = 'DENIED') AS denied_amount
FROM gold.fact_claims
GROUP BY 1, 2;

-- ============================================================
-- PIPELINE HEALTH / OPERATIONS VIEWS
-- Point a "Pipeline Health" Power BI page (or any dashboard) at these so
-- a data steward / on-call engineer can see failures without touching SQL
-- or container logs at all.
-- ============================================================

-- 7) Run-level health - one row per pipeline execution
CREATE OR REPLACE VIEW audit.vw_run_health AS
SELECT
    run_id,
    pipeline_name,
    batch_date,
    status,
    started_at,
    finished_at,
    EXTRACT(EPOCH FROM (finished_at - started_at)) AS duration_seconds,
    error_message
FROM audit.pipeline_runs
ORDER BY started_at DESC;

-- 8) File-level health - answers "did file X load today, and if not, why"
CREATE OR REPLACE VIEW audit.vw_file_health AS
SELECT
    f.source_file,
    f.entity,
    f.status,
    f.file_size_bytes,
    ROUND(f.file_size_bytes / 1048576.0, 1) AS file_size_mb,
    f.rows_read,
    f.rows_inserted,
    f.chunks_processed,
    f.error_message,
    f.started_at,
    f.finished_at,
    r.batch_date,
    r.pipeline_name
FROM audit.file_ingestion_log f
JOIN audit.pipeline_runs r ON r.run_id = f.run_id
ORDER BY f.started_at DESC;

-- 9) Most recent status per file - "is today's data fresh" at a glance
CREATE OR REPLACE VIEW audit.vw_latest_file_status AS
SELECT DISTINCT ON (source_file)
    source_file, entity, status, rows_inserted, error_message, started_at
FROM audit.file_ingestion_log
ORDER BY source_file, started_at DESC;

-- 10) Step-level failure detail - which stage broke (extract/silver/gold)
CREATE OR REPLACE VIEW audit.vw_step_failures AS
SELECT
    s.run_id, r.batch_date, s.step_name, s.status, s.rows_in, s.rows_out,
    s.rows_rejected, s.duration_ms, s.error_message, s.finished_at
FROM audit.pipeline_run_steps s
JOIN audit.pipeline_runs r ON r.run_id = s.run_id
WHERE s.status = 'FAILED'
ORDER BY s.finished_at DESC;

-- 11) Quarantine summary - what kinds of bad data are we seeing, and how much
CREATE OR REPLACE VIEW audit.vw_quarantine_summary AS
SELECT
    entity,
    rejection_reason,
    COUNT(*) AS occurrence_count,
    MAX(quarantined_at) AS most_recent
FROM audit.quarantine
GROUP BY entity, rejection_reason
ORDER BY occurrence_count DESC;
