"""Control tables that the code itself needs. Created on first use so no manual step is required."""
from __future__ import annotations

SCHEMA_DRIFT_DDL = """
CREATE TABLE IF NOT EXISTS {catalog}.control.schema_drift_log (
  run_id       STRING,
  table_name   STRING    NOT NULL,
  change_type  STRING    NOT NULL,
  column_name  STRING    NOT NULL,
  old_type     STRING,
  new_type     STRING,
  detected_at  TIMESTAMP NOT NULL
) COMMENT 'Schema changes detected during ingestion'
"""


def ensure_control_tables(spark, catalog: str) -> None:
    spark.sql(SCHEMA_DRIFT_DDL.format(catalog=catalog))