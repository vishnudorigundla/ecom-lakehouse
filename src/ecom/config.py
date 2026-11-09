"""Reads control.pipeline_config: one row per source table drives every pipeline."""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class TableConfig:
    source_table: str
    load_type: str            # incremental_file | snapshot
    partition_key: str        # ingest_date | snapshot_date
    file_format: str          # csv
    bronze_table: str         # schema.table, no catalog
    silver_table: str
    primary_keys: tuple[str, ...]
    order_by_col: str | None
    scd_type: int | None


def load_table_configs(spark, catalog: str, only: list[str] | None = None) -> list[TableConfig]:
    """Active config rows, sorted by name. `only` limits the run to some tables and fails on unknown names."""
    rows = spark.table(f"{catalog}.control.pipeline_config").where("is_active").collect()
    configs = [
        TableConfig(
            source_table=r["source_table"], load_type=r["load_type"], partition_key=r["partition_key"],
            file_format=r["file_format"], bronze_table=r["bronze_table"], silver_table=r["silver_table"],
            primary_keys=tuple(r["primary_keys"]), order_by_col=r["order_by_col"], scd_type=r["scd_type"],
        )
        for r in rows
    ]
    if only:
        wanted = set(only)
        unknown = wanted - {c.source_table for c in configs}
        if unknown:
            raise ValueError(f"Not found (or not active) in pipeline_config: {sorted(unknown)}")
        configs = [c for c in configs if c.source_table in wanted]
    return sorted(configs, key=lambda c: c.source_table)