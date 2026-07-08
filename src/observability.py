"""
Observability: structured logging + run/step audit trail in Postgres + optional
Azure Monitor (Application Insights) export via OpenTelemetry.

Design goal: even with zero Azure wiring, `pipeline_runs` / `pipeline_run_steps`
tables give a fully queryable audit trail (durations, row counts, failures).
When APPLICATIONINSIGHTS_CONNECTION_STRING is set, the same events also flow
into Azure Monitor for dashboards/alerts.
"""
import json
import logging
import sys
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone

from sqlalchemy import text

from src.config import settings


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger
    logger.setLevel(settings.log_level)
    handler = logging.StreamHandler(sys.stdout)

    class JsonFormatter(logging.Formatter):
        def format(self, record):
            payload = {
                "ts": datetime.now(timezone.utc).isoformat(),
                "level": record.levelname,
                "logger": record.name,
                "message": record.getMessage(),
            }
            if hasattr(record, "extra_fields"):
                payload.update(record.extra_fields)
            return json.dumps(payload)

    handler.setFormatter(JsonFormatter())
    logger.addHandler(handler)

    if settings.app_insights_conn_str:
        try:
            from azure.monitor.opentelemetry import configure_azure_monitor
            configure_azure_monitor(connection_string=settings.app_insights_conn_str)
        except Exception as e:
            logger.warning(f"Azure Monitor not configured: {e}")

    return logger


log = get_logger("etl")


def log_extra(logger, msg, **fields):
    logger.info(msg, extra={"extra_fields": fields})


class RunTracker:
    """Writes pipeline run + step audit records. Idempotent-safe (run_id is a UUID per run)."""

    def __init__(self, engine, pipeline_name: str, batch_date: str):
        self.engine = engine
        self.run_id = str(uuid.uuid4())
        self.pipeline_name = pipeline_name
        self.batch_date = batch_date

    def start_run(self):
        with self.engine.begin() as conn:
            conn.execute(
                text("""
                    INSERT INTO audit.pipeline_runs
                        (run_id, pipeline_name, batch_date, status, started_at)
                    VALUES (:run_id, :pipeline_name, :batch_date, 'RUNNING', now())
                """),
                {"run_id": self.run_id, "pipeline_name": self.pipeline_name, "batch_date": self.batch_date},
            )
        log_extra(log, "pipeline_run_started", run_id=self.run_id, batch_date=self.batch_date)

    def finish_run(self, status: str, error: str = None):
        with self.engine.begin() as conn:
            conn.execute(
                text("""
                    UPDATE audit.pipeline_runs
                    SET status = :status, finished_at = now(), error_message = :error
                    WHERE run_id = :run_id
                """),
                {"status": status, "error": error, "run_id": self.run_id},
            )
        log_extra(log, "pipeline_run_finished", run_id=self.run_id, status=status)

    @contextmanager
    def step(self, step_name: str):
        start = time.time()
        rows_in = rows_out = rows_rejected = 0
        result = {}
        try:
            yield result
            duration_ms = int((time.time() - start) * 1000)
            rows_in = result.get("rows_in", 0)
            rows_out = result.get("rows_out", 0)
            rows_rejected = result.get("rows_rejected", 0)
            with self.engine.begin() as conn:
                conn.execute(
                    text("""
                        INSERT INTO audit.pipeline_run_steps
                            (run_id, step_name, status, rows_in, rows_out, rows_rejected,
                             duration_ms, finished_at)
                        VALUES (:run_id, :step, 'SUCCESS', :rin, :rout, :rrej, :dur, now())
                    """),
                    {"run_id": self.run_id, "step": step_name, "rin": rows_in,
                     "rout": rows_out, "rrej": rows_rejected, "dur": duration_ms},
                )
            log_extra(log, "step_success", run_id=self.run_id, step=step_name,
                      rows_in=rows_in, rows_out=rows_out, rows_rejected=rows_rejected,
                      duration_ms=duration_ms)
        except Exception as e:
            duration_ms = int((time.time() - start) * 1000)
            with self.engine.begin() as conn:
                conn.execute(
                    text("""
                        INSERT INTO audit.pipeline_run_steps
                            (run_id, step_name, status, duration_ms, finished_at, error_message)
                        VALUES (:run_id, :step, 'FAILED', :dur, now(), :err)
                    """),
                    {"run_id": self.run_id, "step": step_name, "dur": duration_ms, "err": str(e)},
                )
            log_extra(log, "step_failed", run_id=self.run_id, step=step_name, error=str(e))
            raise
