"""
Run with: pytest tests/ -v
These tests need no Azure/Postgres - pure logic tests for validation rules.
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pandas as pd
from datetime import date, timedelta
from src.validation import validate_dataframe
from src.models import PatientRecord, ClaimRecord, EncounterRecord

NOW = "2026-07-08T00:00:00+00:00"


def _base_patient(**overrides):
    row = {
        "patient_id": "PAT-1", "first_name": "Jane", "last_name": "Doe",
        "date_of_birth": "1990-01-01", "gender": "F", "plan_type": "HMO",
        "plan_start_date": "2023-01-01", "zip_code": "12345",
        "chronic_conditions": None, "source_file": "x.csv", "ingested_at": NOW,
    }
    row.update(overrides)
    return row


def test_valid_patient_passes():
    df = pd.DataFrame([_base_patient()])
    good, bad, rate = validate_dataframe(df, PatientRecord, "patient")
    assert len(good) == 1 and len(bad) == 0


def test_future_dob_rejected():
    future = (date.today() + timedelta(days=10)).isoformat()
    df = pd.DataFrame([_base_patient(date_of_birth=future)])
    good, bad, rate = validate_dataframe(df, PatientRecord, "patient")
    assert len(good) == 0 and len(bad) == 1
    assert "future" in bad.iloc[0]["rejection_reason"]


def test_invalid_gender_rejected():
    df = pd.DataFrame([_base_patient(gender="Z")])
    good, bad, rate = validate_dataframe(df, PatientRecord, "patient")
    assert len(bad) == 1


def test_claim_approved_exceeds_claim_amount_rejected():
    row = {
        "claim_id": "CLM-1", "patient_id": "PAT-1", "encounter_id": "ENC-1",
        "claim_date": "2026-01-01", "diagnosis_code": "E11.9", "procedure_code": "CPT-1",
        "claim_amount": 100.0, "approved_amount": 200.0, "claim_status": "APPROVED",
        "provider_id": "PROV-1", "source_file": "x.csv", "ingested_at": NOW,
    }
    df = pd.DataFrame([row])
    good, bad, rate = validate_dataframe(df, ClaimRecord, "claim")
    assert len(bad) == 1
    assert "exceeds" in bad.iloc[0]["rejection_reason"]


def test_encounter_discharge_before_admit_rejected():
    row = {
        "encounter_id": "ENC-1", "patient_id": "PAT-1", "admit_date": "2026-02-01",
        "discharge_date": "2026-01-01", "encounter_type": "INPATIENT",
        "facility_id": "FAC-001", "is_readmission": False,
        "source_file": "x.csv", "ingested_at": NOW,
    }
    df = pd.DataFrame([row])
    good, bad, rate = validate_dataframe(df, EncounterRecord, "encounter")
    assert len(bad) == 1


def test_nan_optional_field_normalized_to_none():
    """Empty CSV cell -> NaN -> must not crash Optional[str] validation."""
    df = pd.DataFrame([_base_patient()])
    df.loc[0, "chronic_conditions"] = float("nan")
    good, bad, rate = validate_dataframe(df, PatientRecord, "patient")
    assert len(good) == 1
    assert good.iloc[0]["chronic_conditions"] is None
