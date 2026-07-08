"""
Pipeline orchestrator - single entrypoint, runs Extract -> Bronze -> Silver -> Gold.
Runnable identically locally (docker compose run pipeline) and on Azure
(Container Apps Job with the same image and env vars).

Idempotency guarantee: re-running this script for the same batch_date, or
re-running it after a crash, converges to the same end state - never
duplicates rows. Safe to retry on failure (see @retry decorator).
"""
import sys
from datetime import date

from sqlalchemy import create_engine, text
from tenacity import retry, stop_after_attempt, wait_exponential

from src.config import settings
from src.observability import get_logger, RunTracker
from src.ingestion import read_landing_csv, load_to_bronze, read_bronze_as_df
from src.transform_silver import transform_patients, transform_claims, transform_encounters
from src.transform_gold import build_dim_date, build_dim_patient, build_fact_claims, build_fact_encounters

log = get_logger("pipeline")

LANDING_FILES = {
    "patients": "patients.csv",
    "claims": "claims.csv",
    "encounters": "encounters.csv",
}


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=2, max=30))
def get_engine():
    engine = create_engine(settings.pg_conn_str, pool_pre_ping=True)
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    return engine


def ensure_schema(engine):
    """Applies DDL idempotently (CREATE ... IF NOT EXISTS everywhere)."""
    import pathlib
    sql_dir = pathlib.Path(__file__).parent.parent / "sql"
    for fname in ["schema.sql", "gold_views.sql"]:
        sql_text = (sql_dir / fname).read_text()
        with engine.begin() as conn:
            conn.execute(text(sql_text))
    log.info("Schema/DDL applied (idempotent)")


def run_pipeline():
    batch_date = settings.batch_date or date.today().isoformat()
    engine = get_engine()
    ensure_schema(engine)

    tracker = RunTracker(engine, pipeline_name="health_insurance_etl", batch_date=batch_date)
    tracker.start_run()

    try:
        # ---------------- EXTRACT + BRONZE ----------------
        for entity, filename in LANDING_FILES.items():
            with tracker.step(f"extract_bronze_{entity}") as result:
                df = read_landing_csv(filename)
                inserted = load_to_bronze(engine, f"{entity}_raw", df, filename, batch_date, tracker.run_id)
                result["rows_in"] = len(df)
                result["rows_out"] = inserted

        # ---------------- SILVER ----------------
        quarantine_rates = {}

        with tracker.step("silver_patients") as result:
            bronze_df = read_bronze_as_df(engine, "patients_raw", batch_date)
            stats = transform_patients(engine, bronze_df, tracker.run_id)
            result.update(stats)
            quarantine_rates["patients"] = stats["rows_rejected"] / max(stats["rows_in"], 1)

        with tracker.step("silver_claims") as result:
            bronze_df = read_bronze_as_df(engine, "claims_raw", batch_date)
            stats = transform_claims(engine, bronze_df, tracker.run_id)
            result.update(stats)
            quarantine_rates["claims"] = stats["rows_rejected"] / max(stats["rows_in"], 1)

        with tracker.step("silver_encounters") as result:
            bronze_df = read_bronze_as_df(engine, "encounters_raw", batch_date)
            stats = transform_encounters(engine, bronze_df, tracker.run_id)
            result.update(stats)
            quarantine_rates["encounters"] = stats["rows_rejected"] / max(stats["rows_in"], 1)

        # ---------------- DATA QUALITY GATE ----------------
        for entity, rate in quarantine_rates.items():
            if rate > settings.max_quarantine_rate:
                raise RuntimeError(
                    f"Data quality gate failed for {entity}: "
                    f"{rate:.1%} rejected > threshold {settings.max_quarantine_rate:.1%}"
                )

        # ---------------- GOLD ----------------
        with tracker.step("gold_dim_date") as result:
            build_dim_date(engine)
            result["rows_out"] = 1

        with tracker.step("gold_dim_patient") as result:
            n = build_dim_patient(engine)
            result["rows_out"] = n

        with tracker.step("gold_fact_claims") as result:
            n = build_fact_claims(engine)
            result["rows_out"] = n

        with tracker.step("gold_fact_encounters") as result:
            n = build_fact_encounters(engine)
            result["rows_out"] = n

        tracker.finish_run("SUCCESS")
        log.info(f"Pipeline run {tracker.run_id} completed SUCCESSFULLY for batch_date={batch_date}")

    except Exception as e:
        tracker.finish_run("FAILED", error=str(e))
        log.error(f"Pipeline run {tracker.run_id} FAILED: {e}")
        raise


if __name__ == "__main__":
    try:
        run_pipeline()
    except Exception:
        sys.exit(1)
