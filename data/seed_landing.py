"""
One-shot local/demo helper: generates synthetic CSVs, then uploads them into
the Azurite (or real Azure Blob) 'landing' container so the pipeline has
something to ingest. In production this step is replaced by your real
source system (EHR export, claims clearinghouse feed, SFTP drop, etc.)
landing directly into Blob Storage.
"""
from data.generate_sample_data import gen_patients, gen_encounters, gen_claims, OUT_DIR
from src.ingestion import upload_local_csv_to_landing

if __name__ == "__main__":
    pids = gen_patients()
    eids = gen_encounters(pids)
    gen_claims(pids, eids)

    for fname in ["patients.csv", "encounters.csv", "claims.csv"]:
        upload_local_csv_to_landing(str(OUT_DIR / fname), fname)

    print("Landing zone seeded.")
