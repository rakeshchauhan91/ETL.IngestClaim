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
