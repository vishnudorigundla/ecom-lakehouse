"""Writes one row per table per layer per run to control.pipeline_run_audit.

start()  inserts a STARTED row; finish() updates the same row to SUCCESS or FAILED.
A job that dies without reaching finish() leaves a STARTED row behind, which is the signal that it crashed.
Values go in as SQL parameters, so error text with quotes or newlines is safe.
"""
from __future__ import annotations

MAX_ERROR_CHARS = 4000


class Audit:
    def __init__(self, spark, catalog: str, run_id: str, job_name: str, bundle_target: str, triggered_by: str):
        self.spark = spark
        self.table = f"{catalog}.control.pipeline_run_audit"
        self.run_id = run_id
        self.job_name = job_name
        self.bundle_target = bundle_target
        self.triggered_by = triggered_by

    def start(self, layer: str, table_name: str) -> None:
        self.spark.sql(
            f"""INSERT INTO {self.table}
                (run_id, job_name, layer, table_name, status, start_ts, bundle_target, triggered_by)
                VALUES (:run_id, :job, :layer, :tbl, 'STARTED', current_timestamp(), :tgt, :by)""",
            args={"run_id": self.run_id, "job": self.job_name, "layer": layer, "tbl": table_name,
                  "tgt": self.bundle_target, "by": self.triggered_by},
        )

    def finish(self, layer: str, table_name: str, status: str, rows_read: int | None = None,
               rows_written: int | None = None, rows_rejected: int | None = None,
               target_version: int | None = None, error: str | None = None) -> None:
        self.spark.sql(
            f"""UPDATE {self.table} SET
                  status = :status,
                  end_ts = current_timestamp(),
                  duration_sec = CAST(current_timestamp() AS DOUBLE) - CAST(start_ts AS DOUBLE),
                  rows_read = CAST(:rr AS BIGINT),
                  rows_written = CAST(:rw AS BIGINT),
                  rows_rejected = CAST(:rj AS BIGINT),
                  target_version = CAST(:tv AS BIGINT),
                  error_message = CAST(:err AS STRING)
                WHERE run_id = :run_id AND layer = :layer AND table_name = :tbl AND status = 'STARTED'""",
            args={"status": status, "rr": rows_read, "rw": rows_written, "rj": rows_rejected,
                  "tv": target_version, "err": (error or None) and error[:MAX_ERROR_CHARS],
                  "run_id": self.run_id, "layer": layer, "tbl": table_name},
        )