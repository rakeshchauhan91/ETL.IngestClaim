"""
Generates realistic-but-fake patient/claims/encounters CSVs into ./data/landing/
so the pipeline is runnable end-to-end without any real PHI.
Includes a small % of deliberately dirty rows to exercise validation/quarantine.
"""
import csv
import random
import uuid
from datetime import date, timedelta
from pathlib import Path
from faker import Faker

fake = Faker()
random.seed(42)
Faker.seed(42)

OUT_DIR = Path(__file__).parent / "landing"
OUT_DIR.mkdir(parents=True, exist_ok=True)

PLAN_TYPES = ["HMO", "PPO", "EPO", "POS"]
CHRONIC_ICD10 = ["E11.9", "I10", "J45.9", "N18.9", "F32.9"]  # diabetes, hypertension, asthma, CKD, depression
ACUTE_ICD10 = ["S93.4", "J06.9", "R10.9", "M54.5", "K21.9"]
FACILITIES = [f"FAC-{i:03d}" for i in range(1, 11)]
PROVIDERS = [f"PROV-{i:04d}" for i in range(1, 51)]


def gen_patients(n=2000):
    rows = []
    patient_ids = []
    for _ in range(n):
        pid = f"PAT-{uuid.uuid4().hex[:10].upper()}"
        patient_ids.append(pid)
        dob = fake.date_of_birth(minimum_age=0, maximum_age=95)
        conditions = random.sample(CHRONIC_ICD10, k=random.choice([0, 0, 0, 1, 1, 2]))
        rows.append({
            "patient_id": pid,
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "date_of_birth": dob.isoformat(),
            "gender": random.choice(["M", "F", "O"]),
            "plan_type": random.choice(PLAN_TYPES),
            "plan_start_date": fake.date_between(start_date="-3y", end_date="today").isoformat(),
            "zip_code": fake.zipcode(),
            "chronic_conditions": "|".join(conditions) if conditions else "",
        })

    # inject dirty rows (future DOB, bad gender, bad plan) - ~3%
    for _ in range(int(n * 0.03)):
        row = random.choice(rows).copy()
        row["patient_id"] = f"PAT-{uuid.uuid4().hex[:10].upper()}"
        bug = random.choice(["future_dob", "bad_gender", "bad_plan"])
        if bug == "future_dob":
            row["date_of_birth"] = (date.today() + timedelta(days=30)).isoformat()
        elif bug == "bad_gender":
            row["gender"] = "X"
        else:
            row["plan_type"] = "UNKNOWN"
        rows.append(row)

    with open(OUT_DIR / "patients.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    return patient_ids


def gen_encounters(patient_ids, n=3000):
    rows = []
    encounter_ids = []
    for _ in range(n):
        eid = f"ENC-{uuid.uuid4().hex[:10].upper()}"
        encounter_ids.append(eid)
        admit = fake.date_between(start_date="-2y", end_date="today")
        los = random.choice([0, 1, 2, 3, 5, 7, 14])
        discharge = admit + timedelta(days=los)
        rows.append({
            "encounter_id": eid,
            "patient_id": random.choice(patient_ids),
            "admit_date": admit.isoformat(),
            "discharge_date": discharge.isoformat() if discharge <= date.today() else "",
            "encounter_type": random.choice(["INPATIENT", "OUTPATIENT", "ER"]),
            "facility_id": random.choice(FACILITIES),
            "is_readmission": random.random() < 0.12,
        })

    with open(OUT_DIR / "encounters.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    return encounter_ids


def gen_claims(patient_ids, encounter_ids, n=8000):
    rows = []
    for _ in range(n):
        claim_amount = round(random.uniform(50, 25000), 2)
        status = random.choices(
            ["PAID", "APPROVED", "DENIED", "PENDING", "SUBMITTED"],
            weights=[0.5, 0.2, 0.1, 0.1, 0.1],
        )[0]
        approved = 0.0 if status == "DENIED" else round(claim_amount * random.uniform(0.6, 1.0), 2)
        diag = random.choice(CHRONIC_ICD10 + ACUTE_ICD10)
        rows.append({
            "claim_id": f"CLM-{uuid.uuid4().hex[:12].upper()}",
            "patient_id": random.choice(patient_ids),
            "encounter_id": random.choice(encounter_ids),
            "claim_date": fake.date_between(start_date="-2y", end_date="today").isoformat(),
            "diagnosis_code": diag,
            "procedure_code": f"CPT-{random.randint(10000, 99999)}",
            "claim_amount": claim_amount,
            "approved_amount": approved,
            "claim_status": status,
            "provider_id": random.choice(PROVIDERS),
        })

    # inject dirty rows - approved > claim amount (~2%)
    for _ in range(int(n * 0.02)):
        row = random.choice(rows).copy()
        row["claim_id"] = f"CLM-{uuid.uuid4().hex[:12].upper()}"
        row["approved_amount"] = row["claim_amount"] + 500
        rows.append(row)

    with open(OUT_DIR / "claims.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


if __name__ == "__main__":
    print("Generating synthetic patient data...")
    pids = gen_patients()
    eids = gen_encounters(pids)
    gen_claims(pids, eids)
    print(f"Done. Files written to {OUT_DIR}")
