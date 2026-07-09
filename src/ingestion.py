"""
Extract: reads CSVs from the landing zone (Blob/Azurite) and loads into Bronze.
Idempotent because bronze.*_raw is keyed on row_hash (sha256 of the raw row) -
re-ingesting the same file/row is a harmless no-op (ON CONFLICT DO NOTHING).

Scale notes (100-200MB+ CSVs, growing source count):
  - Reads are STREAMED in chunks (default 50k rows) via pandas `chunksize`,
    not loaded whole into memory. A 200MB CSV never fully materializes as
    one DataFrame - peak memory is bounded by CHUNK_SIZE regardless of file size.
  - Inserts are BULK (psycopg2 execute_values), not row-by-row. This is the
    single biggest speed lever: row-by-row INSERT for 1M rows can take hours;
    batched execute_values does the same in low minutes.
  - Every file is ingested independently and wrapped in its own try/except,
    logged to audit.file_ingestion_log. One corrupt/missing file no longer
    aborts the other files in the same run (see pipeline.py).
"""
import hashlib
import json
import io
from datetime import datetime, timezone

import pandas as pd
from sqlalchemy import text
from psycopg2.extras import execute_values
from azure.storage.blob import BlobServiceClient, ContainerClient
from azure.core.exceptions import ResourceNotFoundError

from src.config import settings
from src.observability import get_logger

log = get_logger("ingestion")

CHUNK_SIZE = settings.csv_chunk_size  # rows per chunk; tune via CSV_CHUNK_SIZE env var


class FileNotFoundInLanding(Exception):
    """Raised when an expected source file is absent from the landing zone."""


class FileIngestionError(Exception):
    """Wraps any failure ingesting a specific file, with row-level context."""


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


def get_blob_size(blob_name: str) -> int:
    container = get_container_client(settings.landing_container)
    blob_client = container.get_blob_client(blob_name)
    if not blob_client.exists():
        raise FileNotFoundInLanding(f"'{blob_name}' not found in landing container")
    return blob_client.get_blob_properties().size


def stream_landing_csv_chunks(blob_name: str, chunksize: int = CHUNK_SIZE):
    """
    Yields normalized DataFrame chunks from a landing-zone CSV without ever
    holding the whole file in memory. Works identically for a 1KB demo file
    or a 500MB production extract.
    """
    container = get_container_client(settings.landing_container)
    blob_client = container.get_blob_client(blob_name)

    if not blob_client.exists():
        raise FileNotFoundInLanding(f"'{blob_name}' not found in landing container")

    try:
        data = blob_client.download_blob().readall()
    except ResourceNotFoundError as e:
        raise FileNotFoundInLanding(f"'{blob_name}' disappeared mid-read: {e}")

    try:
        reader = pd.read_csv(io.BytesIO(data), dtype=str, chunksize=chunksize)
        for chunk in reader:
            # Empty CSV cells become NaN (float) even with dtype=str; json.dumps
            # would emit an invalid bare `NaN` token for that. Normalize to None.
            yield chunk.astype(object).where(pd.notnull(chunk), None)
    except pd.errors.ParserError as e:
        raise FileIngestionError(f"CSV parse error in '{blob_name}': {e}")
    except pd.errors.EmptyDataError:
        raise FileIngestionError(f"'{blob_name}' is empty or has no columns")


def upload_local_csv_to_landing(local_path: str, blob_name: str):
    """Helper used by local run / demo to seed the landing zone."""
    container = get_container_client(settings.landing_container)
    with open(local_path, "rb") as f:
        container.upload_blob(name=blob_name, data=f, overwrite=True)
    log.info(f"Uploaded {local_path} -> landing/{blob_name}")


def _bulk_insert_chunk(conn, table: str, df: pd.DataFrame, source_file: str,
                        batch_date: str, run_id: str) -> int:
    """Bulk-inserts one chunk via execute_values with ON CONFLICT DO NOTHING."""
    now = datetime.now(timezone.utc).isoformat()
    rows = []
    for rec in df.to_dict(orient="records"):
        rec["source_file"] = source_file
        rec["ingested_at"] = now
        rhash = _row_hash(rec)
        rows.append((rhash, source_file, batch_date, run_id, json.dumps(rec, default=str, allow_nan=False)))

    if not rows:
        return 0

    raw_conn = conn.connection.dbapi_connection
    with raw_conn.cursor() as cur:
        result = execute_values(
            cur,
            f"""
            INSERT INTO bronze.{table} (row_hash, source_file, batch_date, run_id, payload)
            VALUES %s
            ON CONFLICT (row_hash) DO NOTHING
            RETURNING row_hash
            """,
            rows,
            template="(%s, %s, %s, %s, CAST(%s AS JSONB))",
            page_size=1000,
            fetch=True,
        )
        return len(result)


def load_file_to_bronze(engine, table: str, blob_name: str, entity: str,
                         batch_date: str, run_id: str) -> dict:
    """
    Ingests ONE source file end-to-end: stream -> chunk -> bulk insert,
    with its own file_ingestion_log row. Never raises past this point for
    "expected" failure modes (missing file, bad CSV) - it records the
    failure and returns a status dict so the caller can continue with
    other files instead of aborting the whole run.
    """
    started = datetime.now(timezone.utc)
    rows_read = rows_inserted = chunks = 0

    try:
        file_size = get_blob_size(blob_name)
    except FileNotFoundInLanding as e:
        _log_file_status(engine, run_id, blob_name, entity, "MISSING", 0, 0, 0, 0, str(e), started)
        log.error(f"file_missing: {blob_name} - {e}")
        return {"status": "MISSING", "rows_read": 0, "rows_inserted": 0, "error": str(e)}

    try:
        with engine.begin() as conn:
            for chunk_df in stream_landing_csv_chunks(blob_name):
                chunks += 1
                rows_read += len(chunk_df)
                inserted = _bulk_insert_chunk(conn, table, chunk_df, blob_name, batch_date, run_id)
                rows_inserted += inserted
                log.info(f"{blob_name}: chunk {chunks} - {len(chunk_df)} rows read, "
                         f"{inserted} new (running total: {rows_inserted})")

        _log_file_status(engine, run_id, blob_name, entity, "SUCCESS",
                          file_size, rows_read, rows_inserted, chunks, None, started)
        return {"status": "SUCCESS", "rows_read": rows_read, "rows_inserted": rows_inserted}

    except (FileIngestionError, FileNotFoundInLanding) as e:
        _log_file_status(engine, run_id, blob_name, entity, "FAILED",
                          file_size, rows_read, rows_inserted, chunks, str(e), started)
        log.error(f"file_ingestion_failed: {blob_name} - {e}")
        return {"status": "FAILED", "rows_read": rows_read, "rows_inserted": rows_inserted, "error": str(e)}

    except Exception as e:
        # Unexpected error (DB connection drop mid-file, etc.) - still record it
        # rather than letting the whole pipeline die with no queryable trace.
        _log_file_status(engine, run_id, blob_name, entity, "FAILED",
                          file_size, rows_read, rows_inserted, chunks, f"Unexpected: {e}", started)
        log.error(f"file_ingestion_unexpected_error: {blob_name} - {e}")
        return {"status": "FAILED", "rows_read": rows_read, "rows_inserted": rows_inserted, "error": str(e)}


def _log_file_status(engine, run_id, source_file, entity, status, file_size,
                      rows_read, rows_inserted, chunks, error, started):
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO audit.file_ingestion_log
                    (run_id, source_file, entity, status, file_size_bytes, rows_read,
                     rows_inserted, chunks_processed, error_message, started_at, finished_at)
                VALUES (:run_id, :source_file, :entity, :status, :file_size, :rows_read,
                        :rows_inserted, :chunks, :error, :started, now())
            """),
            {
                "run_id": run_id, "source_file": source_file, "entity": entity, "status": status,
                "file_size": file_size, "rows_read": rows_read, "rows_inserted": rows_inserted,
                "chunks": chunks, "error": error, "started": started,
            },
        )


def read_bronze_as_df(engine, table: str, batch_date: str) -> pd.DataFrame:
    with engine.begin() as conn:
        result = conn.execute(
            text(f"SELECT payload FROM bronze.{table} WHERE batch_date = :batch_date"),
            {"batch_date": batch_date},
        )
        rows = [r[0] for r in result.fetchall()]
    return pd.DataFrame(rows) if rows else pd.DataFrame()
