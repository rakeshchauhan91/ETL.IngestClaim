"""
Transform silver -> gold: builds the star schema (dim_patient, dim_date,
fact_claims, fact_encounters) that Power BI reads directly.
Fully idempotent: dims are rebuilt via upsert, facts via natural-key upsert,
and dim_date is generated once and left as-is (it's a fixed calendar).
"""
from sqlalchemy import text
from src.observability import get_logger

log = get_logger("transform_gold")


def build_dim_date(engine, start_year: int = 2022, end_year: int = 2027):
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO gold.dim_date
                SELECT
                    d::date AS date_key,
                    EXTRACT(YEAR FROM d)::int,
                    EXTRACT(QUARTER FROM d)::int,
                    EXTRACT(MONTH FROM d)::int,
                    TO_CHAR(d, 'Month'),
                    EXTRACT(DAY FROM d)::int,
                    EXTRACT(WEEK FROM d)::int,
                    EXTRACT(ISODOW FROM d) IN (6, 7)
                FROM generate_series(
                    make_date(:start_year, 1, 1), make_date(:end_year, 12, 31), interval '1 day'
                ) d
                ON CONFLICT (date_key) DO NOTHING
            """),
            {"start_year": start_year, "end_year": end_year},
        )
    log.info("gold.dim_date ensured")


def build_dim_patient(engine):
    with engine.begin() as conn:
        result = conn.execute(text("""
            INSERT INTO gold.dim_patient
                (patient_id, full_name, age, age_band, gender, plan_type, zip_code,
                 chronic_condition_count, is_chronic, is_current)
            SELECT
                p.patient_id,
                p.first_name || ' ' || p.last_name,
                EXTRACT(YEAR FROM age(p.date_of_birth))::int AS age,
                CASE
                    WHEN EXTRACT(YEAR FROM age(p.date_of_birth)) < 18 THEN '0-17'
                    WHEN EXTRACT(YEAR FROM age(p.date_of_birth)) < 35 THEN '18-34'
                    WHEN EXTRACT(YEAR FROM age(p.date_of_birth)) < 50 THEN '35-49'
                    WHEN EXTRACT(YEAR FROM age(p.date_of_birth)) < 65 THEN '50-64'
                    ELSE '65+'
                END AS age_band,
                p.gender,
                p.plan_type,
                p.zip_code,
                CASE WHEN p.chronic_conditions IS NULL OR p.chronic_conditions = '' THEN 0
                     ELSE array_length(string_to_array(p.chronic_conditions, '|'), 1) END,
                CASE WHEN p.chronic_conditions IS NULL OR p.chronic_conditions = '' THEN FALSE
                     ELSE TRUE END,
                TRUE
            FROM silver.patients p
            WHERE p.is_current
            ON CONFLICT (patient_id, is_current) DO UPDATE SET
                full_name = EXCLUDED.full_name,
                age = EXCLUDED.age,
                age_band = EXCLUDED.age_band,
                gender = EXCLUDED.gender,
                plan_type = EXCLUDED.plan_type,
                zip_code = EXCLUDED.zip_code,
                chronic_condition_count = EXCLUDED.chronic_condition_count,
                is_chronic = EXCLUDED.is_chronic
        """))
    log.info(f"gold.dim_patient upserted, rows affected={result.rowcount}")
    return result.rowcount


def build_fact_claims(engine):
    with engine.begin() as conn:
        result = conn.execute(text("""
            INSERT INTO gold.fact_claims
                (claim_id, patient_id, claim_date, diagnosis_code, procedure_code,
                 claim_amount, approved_amount, denied_amount, claim_status, provider_id)
            SELECT
                c.claim_id, c.patient_id, c.claim_date, c.diagnosis_code, c.procedure_code,
                c.claim_amount, c.approved_amount,
                CASE WHEN c.claim_status = 'DENIED' THEN c.claim_amount ELSE 0 END,
                c.claim_status, c.provider_id
            FROM silver.claims c
            WHERE EXISTS (SELECT 1 FROM gold.dim_date d WHERE d.date_key = c.claim_date)
            ON CONFLICT (claim_id) DO UPDATE SET
                patient_id = EXCLUDED.patient_id,
                claim_date = EXCLUDED.claim_date,
                diagnosis_code = EXCLUDED.diagnosis_code,
                procedure_code = EXCLUDED.procedure_code,
                claim_amount = EXCLUDED.claim_amount,
                approved_amount = EXCLUDED.approved_amount,
                denied_amount = EXCLUDED.denied_amount,
                claim_status = EXCLUDED.claim_status,
                provider_id = EXCLUDED.provider_id
        """))
    log.info(f"gold.fact_claims upserted, rows affected={result.rowcount}")
    return result.rowcount


def build_fact_encounters(engine):
    with engine.begin() as conn:
        result = conn.execute(text("""
            INSERT INTO gold.fact_encounters
                (encounter_id, patient_id, admit_date, discharge_date, encounter_type,
                 facility_id, is_readmission, length_of_stay_days)
            SELECT
                e.encounter_id, e.patient_id, e.admit_date, e.discharge_date, e.encounter_type,
                e.facility_id, e.is_readmission, e.length_of_stay_days
            FROM silver.encounters e
            WHERE EXISTS (SELECT 1 FROM gold.dim_date d WHERE d.date_key = e.admit_date)
            ON CONFLICT (encounter_id) DO UPDATE SET
                patient_id = EXCLUDED.patient_id,
                admit_date = EXCLUDED.admit_date,
                discharge_date = EXCLUDED.discharge_date,
                encounter_type = EXCLUDED.encounter_type,
                facility_id = EXCLUDED.facility_id,
                is_readmission = EXCLUDED.is_readmission,
                length_of_stay_days = EXCLUDED.length_of_stay_days
        """))
    log.info(f"gold.fact_encounters upserted, rows affected={result.rowcount}")
    return result.rowcount
