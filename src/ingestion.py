"""
Extract: reads CSVs from the landing zone (Blob/Azurite) and loads into Bronze.
Idempotent because bronze.*_raw is keyed on row_hash (sha256 of the raw row) -
re-ingesting the same file/row is a harmless no-op (ON CONFLICT DO NOTHING).
"""
import hashlib
import json
import io
from datetime import datetime, timezone

import pandas as pd
from sqlalchemy import text
from azure.storage.blob import BlobServiceClient, ContainerClient

from src.config import settings
from src.observability import get_logger

log = get_logger("ingestion")


def _row_hash(row: dict) -> str:
    canonical = json.dumps(row, sort_keys=True, default=str, allow_nan=False)
    return hashlib.sha256(canonical.encode()).hexdigest()


def get_container_client(container_name: str) -> ContainerClient:
    svc = BlobServiceClient.from_connection_string(settings.azure_storage_conn_str)
    try:
        svc.create_container(container_name)
    except Exception:
        pass  # already exists - fine, idempotent
    return svc.get_container_client(container_name)


def read_landing_csv(blob_name: str) -> pd.DataFrame:
    container = get_container_client(settings.landing_container)
    blob_client = container.get_blob_client(blob_name)
    data = blob_client.download_blob().readall()
    df = pd.read_csv(io.BytesIO(data), dtype=str)
    # Empty CSV cells become NaN (float) even with dtype=str. json.dumps() will
    # happily emit a bare `NaN` token for that, which is NOT valid JSON and
    # Postgres's JSONB parser rejects it outright. Normalize to None (-> JSON null).
    return df.astype(object).where(pd.notnull(df), None)


def upload_local_csv_to_landing(local_path: str, blob_name: str):
    """Helper used by local run / demo to seed the landing zone."""
    container = get_container_client(settings.landing_container)
    with open(local_path, "rb") as f:
        container.upload_blob(name=blob_name, data=f, overwrite=True)
    log.info(f"Uploaded {local_path} -> landing/{blob_name}")


def load_to_bronze(engine, table: str, df: pd.DataFrame, source_file: str,
                    batch_date: str, run_id: str) -> int:
    """Append-only, idempotent load into a bronze.*_raw table."""
    now = datetime.now(timezone.utc).isoformat()
    records = df.to_dict(orient="records")
    rows_inserted = 0

    with engine.begin() as conn:
        for rec in records:
            rec["source_file"] = source_file
            rec["ingested_at"] = now
            rhash = _row_hash(rec)
            result = conn.execute(
                text(f"""
                    INSERT INTO bronze.{table} (row_hash, source_file, batch_date, run_id, payload)
                    VALUES (:row_hash, :source_file, :batch_date, :run_id, CAST(:payload AS JSONB))
                    ON CONFLICT (row_hash) DO NOTHING
                """),
                {
                    "row_hash": rhash,
                    "source_file": source_file,
                    "batch_date": batch_date,
                    "run_id": run_id,
                    "payload": json.dumps(rec, default=str, allow_nan=False),
                },
            )
            rows_inserted += result.rowcount

    log.info(f"bronze.{table}: {rows_inserted} new rows inserted (of {len(records)} seen)")
    return rows_inserted


def read_bronze_as_df(engine, table: str, batch_date: str) -> pd.DataFrame:
    with engine.begin() as conn:
        result = conn.execute(
            text(f"SELECT payload FROM bronze.{table} WHERE batch_date = :batch_date"),
            {"batch_date": batch_date},
        )
        rows = [r[0] for r in result.fetchall()]
    return pd.DataFrame(rows) if rows else pd.DataFrame()
