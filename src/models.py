"""
Domain schemas. Used to validate every incoming row before it is allowed
into Silver. Anything that fails goes to quarantine with a reason.
"""
from datetime import date
from typing import Optional
from pydantic import BaseModel, Field, field_validator

VALID_GENDERS = {"M", "F", "O"}
VALID_PLAN_TYPES = {"HMO", "PPO", "EPO", "POS"}
VALID_CLAIM_STATUS = {"SUBMITTED", "APPROVED", "DENIED", "PAID", "PENDING"}


class PatientRecord(BaseModel):
    patient_id: str = Field(min_length=1)
    first_name: str
    last_name: str
    date_of_birth: date
    gender: str
    plan_type: str
    plan_start_date: date
    zip_code: str = Field(min_length=5, max_length=10)
    chronic_conditions: Optional[str] = None  # pipe-delimited ICD-10 codes, nullable
    source_file: str
    ingested_at: str

    @field_validator("gender")
    @classmethod
    def gender_valid(cls, v):
        if v.upper() not in VALID_GENDERS:
            raise ValueError(f"invalid gender '{v}'")
        return v.upper()

    @field_validator("plan_type")
    @classmethod
    def plan_valid(cls, v):
        if v.upper() not in VALID_PLAN_TYPES:
            raise ValueError(f"invalid plan_type '{v}'")
        return v.upper()

    @field_validator("date_of_birth")
    @classmethod
    def dob_not_future(cls, v):
        if v > date.today():
            raise ValueError("date_of_birth is in the future")
        if v.year < 1900:
            raise ValueError("date_of_birth implausibly old")
        return v


class ClaimRecord(BaseModel):
    claim_id: str = Field(min_length=1)
    patient_id: str = Field(min_length=1)
    encounter_id: str
    claim_date: date
    diagnosis_code: str  # ICD-10 e.g. E11.9
    procedure_code: Optional[str] = None
    claim_amount: float = Field(ge=0)
    approved_amount: float = Field(ge=0)
    claim_status: str
    provider_id: str
    source_file: str
    ingested_at: str

    @field_validator("claim_status")
    @classmethod
    def status_valid(cls, v):
        if v.upper() not in VALID_CLAIM_STATUS:
            raise ValueError(f"invalid claim_status '{v}'")
        return v.upper()

    @field_validator("approved_amount")
    @classmethod
    def approved_not_exceed_claim(cls, v, info):
        claim_amount = info.data.get("claim_amount")
        if claim_amount is not None and v > claim_amount:
            raise ValueError("approved_amount exceeds claim_amount")
        return v


class EncounterRecord(BaseModel):
    encounter_id: str = Field(min_length=1)
    patient_id: str = Field(min_length=1)
    admit_date: date
    discharge_date: Optional[date] = None
    encounter_type: str  # INPATIENT / OUTPATIENT / ER
    facility_id: str
    is_readmission: bool = False
    source_file: str
    ingested_at: str

    @field_validator("discharge_date")
    @classmethod
    def discharge_after_admit(cls, v, info):
        admit = info.data.get("admit_date")
        if v is not None and admit is not None and v < admit:
            raise ValueError("discharge_date precedes admit_date")
        return v
