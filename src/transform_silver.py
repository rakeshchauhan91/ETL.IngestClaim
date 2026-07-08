"""
Transform bronze -> silver:
  - validate rows (Pydantic) -> good / quarantine
  - dedupe within batch
  - patient dim uses SCD2 (attribute-change tracking) - idempotent because
    we only close/open a version when the *attribute hash* actually changes
  - claims/encounters use natural-key upserts (ON CONFLICT ... DO UPDATE) -
    re-running the same batch converges to the same state, not duplicates
"""
import hashlib
import json
import pandas as pd
from sqlalchemy import text

from src.models import PatientRecord, ClaimRecord, EncounterRecord
from src.validation import validate_dataframe
from src.observability import get_logger

log = get_logger("transform_silver")


def _attr_hash(row: dict, fields: list) -> str:
    payload = {k: row.get(k) for k in fields}
    return hashlib.sha256(json.dumps(payload, sort_keys=True, default=str).encode()).hexdigest()


def _quarantine(engine, run_id: str, entity: str, bad_df: pd.DataFrame):
    if bad_df.empty:
        return
    with engine.begin() as conn:
        for rec in bad_df.to_dict(orient="records"):
            reason = rec.pop("rejection_reason")
            rec.pop("entity", None)
            conn.execute(
                text("""
                    INSERT INTO audit.quarantine (run_id, entity, raw_record, rejection_reason)
                    VALUES (:run_id, :entity, CAST(:raw AS JSONB), :reason)
                """),
                {"run_id": run_id, "entity": entity, "raw": json.dumps(rec, default=str), "reason": reason},
            )


def transform_patients(engine, bronze_df: pd.DataFrame, run_id: str):
    if bronze_df.empty:
        return {"rows_in": 0, "rows_out": 0, "rows_rejected": 0}

    good, bad, rate = validate_dataframe(bronze_df, PatientRecord, "patient")
    _quarantine(engine, run_id, "patient", bad)
    good = good.drop_duplicates(subset=["patient_id"], keep="last")

    scd_fields = ["first_name", "last_name", "date_of_birth", "gender",
                  "plan_type", "plan_start_date", "zip_code", "chronic_conditions"]

    upserts = 0
    with engine.begin() as conn:
        for rec in good.to_dict(orient="records"):
            new_hash = _attr_hash(rec, scd_fields)
            current = conn.execute(
                text("""SELECT row_hash FROM silver.patients
                        WHERE patient_id = :pid AND is_current"""),
                {"pid": rec["patient_id"]},
            ).fetchone()

            if current is None:
                # brand new patient
                conn.execute(
                    text("""
                        INSERT INTO silver.patients
                            (patient_id, first_name, last_name, date_of_birth, gender, plan_type,
                             plan_start_date, zip_code, chronic_conditions, row_hash)
                        VALUES (:patient_id, :first_name, :last_name, :date_of_birth, :gender,
                                :plan_type, :plan_start_date, :zip_code, :chronic_conditions, :rh)
                    """),
                    {**rec, "rh": new_hash},
                )
                upserts += 1
            elif current[0] != new_hash:
                # attributes changed -> close old version, open new one (SCD2)
                conn.execute(
                    text("""UPDATE silver.patients SET is_current = FALSE, effective_to = now()
                            WHERE patient_id = :pid AND is_current"""),
                    {"pid": rec["patient_id"]},
                )
                conn.execute(
                    text("""
                        INSERT INTO silver.patients
                            (patient_id, first_name, last_name, date_of_birth, gender, plan_type,
                             plan_start_date, zip_code, chronic_conditions, row_hash)
                        VALUES (:patient_id, :first_name, :last_name, :date_of_birth, :gender,
                                :plan_type, :plan_start_date, :zip_code, :chronic_conditions, :rh)
                    """),
                    {**rec, "rh": new_hash},
                )
                upserts += 1
            # else: identical -> no-op, re-run is idempotent

    return {"rows_in": len(bronze_df), "rows_out": upserts, "rows_rejected": len(bad)}


def transform_claims(engine, bronze_df: pd.DataFrame, run_id: str):
    if bronze_df.empty:
        return {"rows_in": 0, "rows_out": 0, "rows_rejected": 0}

    good, bad, rate = validate_dataframe(bronze_df, ClaimRecord, "claim")
    _quarantine(engine, run_id, "claim", bad)
    good = good.drop_duplicates(subset=["claim_id"], keep="last")

    with engine.begin() as conn:
        for rec in good.to_dict(orient="records"):
            conn.execute(
                text("""
                    INSERT INTO silver.claims
                        (claim_id, patient_id, encounter_id, claim_date, diagnosis_code,
                         procedure_code, claim_amount, approved_amount, claim_status,
                         provider_id, updated_at)
                    VALUES (:claim_id, :patient_id, :encounter_id, :claim_date, :diagnosis_code,
                            :procedure_code, :claim_amount, :approved_amount, :claim_status,
                            :provider_id, now())
                    ON CONFLICT (claim_id) DO UPDATE SET
                        patient_id = EXCLUDED.patient_id,
                        encounter_id = EXCLUDED.encounter_id,
                        claim_date = EXCLUDED.claim_date,
                        diagnosis_code = EXCLUDED.diagnosis_code,
                        procedure_code = EXCLUDED.procedure_code,
                        claim_amount = EXCLUDED.claim_amount,
                        approved_amount = EXCLUDED.approved_amount,
                        claim_status = EXCLUDED.claim_status,
                        provider_id = EXCLUDED.provider_id,
                        updated_at = now()
                """),
                rec,
            )

    return {"rows_in": len(bronze_df), "rows_out": len(good), "rows_rejected": len(bad)}


def transform_encounters(engine, bronze_df: pd.DataFrame, run_id: str):
    if bronze_df.empty:
        return {"rows_in": 0, "rows_out": 0, "rows_rejected": 0}

    bronze_df = bronze_df.copy()
    if "discharge_date" in bronze_df.columns:
        bronze_df["discharge_date"] = bronze_df["discharge_date"].replace("", None)
    if "is_readmission" in bronze_df.columns:
        bronze_df["is_readmission"] = bronze_df["is_readmission"].astype(str).str.lower().isin(["true", "1"])

    good, bad, rate = validate_dataframe(bronze_df, EncounterRecord, "encounter")
    _quarantine(engine, run_id, "encounter", bad)
    good = good.drop_duplicates(subset=["encounter_id"], keep="last")

    with engine.begin() as conn:
        for rec in good.to_dict(orient="records"):
            admit = pd.to_datetime(rec["admit_date"])
            discharge = pd.to_datetime(rec["discharge_date"]) if rec.get("discharge_date") else None
            los = (discharge - admit).days if discharge is not None else None
            conn.execute(
                text("""
                    INSERT INTO silver.encounters
                        (encounter_id, patient_id, admit_date, discharge_date, encounter_type,
                         facility_id, is_readmission, length_of_stay_days, updated_at)
                    VALUES (:encounter_id, :patient_id, :admit_date, :discharge_date, :encounter_type,
                            :facility_id, :is_readmission, :los, now())
                    ON CONFLICT (encounter_id) DO UPDATE SET
                        patient_id = EXCLUDED.patient_id,
                        admit_date = EXCLUDED.admit_date,
                        discharge_date = EXCLUDED.discharge_date,
                        encounter_type = EXCLUDED.encounter_type,
                        facility_id = EXCLUDED.facility_id,
                        is_readmission = EXCLUDED.is_readmission,
                        length_of_stay_days = EXCLUDED.length_of_stay_days,
                        updated_at = now()
                """),
                {**rec, "los": los},
            )

    return {"rows_in": len(bronze_df), "rows_out": len(good), "rows_rejected": len(bad)}
