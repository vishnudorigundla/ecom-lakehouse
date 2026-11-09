"""Bronze ingestion: landing volume -> Delta, driven by control.pipeline_config.

Per table:
  1. Auto Loader reads only NEW files (its checkpoint remembers the rest) and keeps every column as text.
  2. For each micro-batch we fingerprint the files (SHA-256 of the content). A file whose content was already
     loaded under another name (a re-delivery) is skipped and logged as SKIPPED_DUPLICATE.
  3. Rows of the remaining files are appended to the Bronze table with _source_file, _file_modified_ts,
     _ingest_ts and _run_id. Data is written BEFORE the log row, so a crash can only cause a repeat that Silver
     removes by key, never a lost file.
  4. After the stream, files that produced no rows (header only) are logged as SKIPPED_EMPTY.
  5. New columns that appeared are written to control.schema_drift_log.
"""
from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from ecom.audit import Audit
from ecom.config import TableConfig, load_table_configs
from ecom.control import ensure_control_tables

MAX_SCHEMA_RESTARTS = 3


@dataclass
class BronzeResult:
    rows_read: int = 0
    rows_written: int = 0
    rows_rejected: int = 0      # rows that sat in duplicate files and were skipped
    files_loaded: int = 0
    files_duplicate: int = 0
    files_empty: int = 0
    target_version: int | None = None


# ---------------------------------------------------------------- helpers
def landing_path(catalog: str, table: str) -> str:
    return f"/Volumes/{catalog}/bronze/landing/{table}"


def checkpoint_path(catalog: str, table: str) -> str:
    return f"/Volumes/{catalog}/bronze/checkpoints/{table}"


def _to_volume_path(path: str) -> str:
    """Auto Loader reports dbfs:/Volumes/...; readers want /Volumes/..."""
    return path.removeprefix("dbfs:")


def _rel_path(col: str, partition_key: str):
    """'.../orders/ingest_date=2025-06-10/orders_x.csv' -> 'ingest_date=2025-06-10/orders_x.csv'.

    Used to match the same file however its path prefix is written (dbfs:/Volumes vs /Volumes)."""
    return F.regexp_extract(F.col(col), rf"({partition_key}=.*)$", 1)


def _is_schema_change(exc: Exception) -> bool:
    text = f"{type(exc).__name__} {exc}".lower()
    return "unknown_field_exception" in text or "unknownfieldexception" in text


def _columns(spark, table: str) -> dict[str, str] | None:
    if not spark.catalog.tableExists(table):
        return None
    return {f.name: f.dataType.simpleString() for f in spark.table(table).schema.fields}


def _target_version(spark, table: str) -> int | None:
    if not spark.catalog.tableExists(table):
        return None
    return int(spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").first()["version"])




# ---------------------------------------------------------------- per micro-batch
def _make_batch_handler(spark, catalog: str, cfg: TableConfig, run_id: str):
    target = f"{catalog}.{cfg.bronze_table}"
    log_table = f"{catalog}.control.file_ingestion_log"
    view = f"_file_log_updates_{cfg.source_table}"

    def handle(batch_df, batch_id: int) -> None:
        if batch_df.isEmpty():
            return
        batch_df = batch_df.persist()

        files = (batch_df.groupBy("_source_file")
                 .agg(F.count(F.lit(1)).alias("row_count"), F.min("_file_modified_ts").alias("file_modified_ts"))
                 .withColumn("_k", _rel_path("_source_file", cfg.partition_key)))
        paths = [_to_volume_path(r["_source_file"]) for r in files.select("_source_file").collect()]

        info = (spark.read.format("binaryFile").load(paths)
                .select(_rel_path("path", cfg.partition_key).alias("_k"),
                        F.col("length").alias("file_size_bytes"),
                        F.sha2(F.col("content"), 256).alias("content_hash")))

        already_loaded = (spark.table(log_table)
                          .where((F.col("table_name") == cfg.source_table) & (F.col("status") == "LOADED"))
                          .select("content_hash").distinct().withColumn("_seen", F.lit(True)))

        # Same content twice in one batch: the alphabetically first path wins, so reruns pick the same file.
        first_of_hash = Window.partitionBy("content_hash").orderBy("_source_file")
        decided = (files.join(info, "_k", "left")
                   .join(already_loaded, "content_hash", "left")
                   .withColumn("_rn", F.row_number().over(first_of_hash))
                   .withColumn("status",
                               F.when(F.col("content_hash").isNotNull()
                                      & (F.col("_seen").isNotNull() | (F.col("_rn") > 1)),
                                      F.lit("SKIPPED_DUPLICATE")).otherwise(F.lit("LOADED")))
                   .persist())

        keep = decided.where(F.col("status") == "LOADED").select("_source_file")
        to_write = (batch_df.join(F.broadcast(keep), "_source_file", "left_semi")
                    .withColumn("_run_id", F.lit(run_id)))
        (to_write.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(target))

        (decided.select(F.lit(cfg.source_table).alias("table_name"),
                        F.col("_source_file").alias("file_path"), "file_size_bytes",
                        F.col("file_modified_ts"), "content_hash", "row_count", "status",
                        F.lit(run_id).alias("run_id"), F.current_timestamp().alias("ingested_at"))
         .createOrReplaceTempView(view))
        spark.sql(f"""MERGE INTO {log_table} t USING {view} s
                      ON t.table_name = s.table_name AND t.file_path = s.file_path
                      WHEN MATCHED THEN UPDATE SET *
                      WHEN NOT MATCHED THEN INSERT *""")

        

        decided.unpersist()
        batch_df.unpersist()

    return handle



# ---------------------------------------------------------------- header-only files
def register_empty_files(spark, catalog: str, cfg: TableConfig, run_id: str) -> int:
    """Log files that have no data rows. Auto Loader consumed them but they never appear in a batch.

    Only files that are unknown to the log AND really empty are logged. A non-empty unknown file (it landed
    while the stream was running) is left alone and will be loaded and logged by the next run."""
    log_table = f"{catalog}.control.file_ingestion_log"
    listing = (spark.read.format("binaryFile").option("recursiveFileLookup", "true")
               .option("pathGlobFilter", f"*.{cfg.file_format}").load(landing_path(catalog, cfg.source_table))
               .select("path", "length", "modificationTime")
               .withColumn("_k", _rel_path("path", cfg.partition_key)))
    logged = (spark.table(log_table).where(F.col("table_name") == cfg.source_table)
              .select(_rel_path("file_path", cfg.partition_key).alias("_k")).distinct())
    unknown = listing.join(logged, "_k", "left_anti")
    paths = [_to_volume_path(r["path"]) for r in unknown.select("path").collect()]
    if not paths:
        return 0

    text = F.decode(F.col("content"), "UTF-8")
    non_blank = F.size(F.filter(F.split(text, r"\r?\n"), lambda line: F.length(F.trim(line)) > 0))
    empties = (spark.read.format("binaryFile").load(paths)
               .withColumn("_lines", non_blank)
               .where(F.col("_lines") <= 1)          # at most a header line
               .select(F.lit(cfg.source_table).alias("table_name"), F.col("path").alias("file_path"),
                       F.col("length").alias("file_size_bytes"),
                       F.col("modificationTime").alias("file_modified_ts"),
                       F.sha2(F.col("content"), 256).alias("content_hash"),
                       F.lit(0).cast("bigint").alias("row_count"),
                       F.lit("SKIPPED_EMPTY").alias("status"),
                       F.lit(run_id).alias("run_id"), F.current_timestamp().alias("ingested_at")))
    n = empties.count()
    if n:
        empties.write.format("delta").mode("append").saveAsTable(log_table)
    return n


# ---------------------------------------------------------------- schema drift
def _log_schema_drift(spark, catalog: str, table: str, before: dict | None, after: dict | None, run_id: str) -> None:
    if before is None or after is None:      # first load creates the table: nothing to compare
        return
    for col, dtype in after.items():
        if col not in before and not col.startswith("_"):
            spark.sql(
                f"""INSERT INTO {catalog}.control.schema_drift_log
                    (run_id, table_name, change_type, column_name, old_type, new_type, detected_at)
                    VALUES (:run_id, :tbl, 'COLUMN_ADDED', :col, NULL, :new, current_timestamp())""",
                args={"run_id": run_id, "tbl": table, "col": col, "new": dtype})

def _summarize_run(spark, catalog: str, table: str, run_id: str) -> BronzeResult:
    """Counts for this run, read back from the file log. (The batch function runs in a separate process on
    serverless compute, so it cannot update Python variables in the notebook.)"""
    result = BronzeResult()
    summary = (spark.table(f"{catalog}.control.file_ingestion_log")
               .where((F.col("table_name") == table) & (F.col("run_id") == run_id))
               .groupBy("status").agg(F.sum("row_count").alias("rows"), F.count(F.lit(1)).alias("files")))
    for r in summary.collect():
        rows, files = int(r["rows"] or 0), int(r["files"])
        if r["status"] == "LOADED":
            result.rows_written += rows
            result.files_loaded += files
        elif r["status"] == "SKIPPED_DUPLICATE":
            result.rows_rejected += rows
            result.files_duplicate += files
        elif r["status"] == "SKIPPED_EMPTY":
            result.files_empty += files
    result.rows_read = result.rows_written + result.rows_rejected
    return result

# ---------------------------------------------------------------- one table
def ingest_table(spark, catalog: str, cfg: TableConfig, run_id: str) -> BronzeResult:
    target = f"{catalog}.{cfg.bronze_table}"
    chk = checkpoint_path(catalog, cfg.source_table)
    columns_before = _columns(spark, target)

    handler = _make_batch_handler(spark, catalog, cfg, run_id)

    for attempt in range(1, MAX_SCHEMA_RESTARTS + 2):
        reader = (spark.readStream.format("cloudFiles")
                  .option("cloudFiles.format", cfg.file_format)
                  .option("cloudFiles.inferColumnTypes", "false")          # Bronze stays raw text
                  .option("cloudFiles.schemaLocation", f"{chk}/schema")
                  .option("cloudFiles.schemaEvolutionMode", "addNewColumns"))
        if cfg.file_format == "csv":
            reader = reader.option("header", "true")
        stream = (reader.load(landing_path(catalog, cfg.source_table))
                  .withColumn("_source_file", F.col("_metadata.file_path"))
                  .withColumn("_file_modified_ts", F.col("_metadata.file_modification_time"))
                  .withColumn("_ingest_ts", F.current_timestamp()))
        try:
            (stream.writeStream.foreachBatch(handler)
             .option("checkpointLocation", f"{chk}/checkpoint")
             .trigger(availableNow=True).start().awaitTermination())
            break
        except Exception as exc:
            # Auto Loader stops on purpose when it finds a new column, saves the new schema, and expects a restart.
            if _is_schema_change(exc) and attempt <= MAX_SCHEMA_RESTARTS:
                print(f"[{cfg.source_table}] new columns detected, restarting stream (attempt {attempt})")
                continue
            raise

    register_empty_files(spark, catalog, cfg, run_id)
    _log_schema_drift(spark, catalog, cfg.bronze_table, columns_before, _columns(spark, target), run_id)
    result = _summarize_run(spark, catalog, cfg.source_table, run_id)
    result.target_version = _target_version(spark, target)
    return result

# ---------------------------------------------------------------- all tables
def run_bronze(spark, catalog: str, run_id: str, audit: Audit, only: list[str] | None = None) -> list[tuple]:
    """Load every active table in pipeline_config. One failed table does not stop the others;
    the job fails at the end if any table failed."""
    ensure_control_tables(spark, catalog)
    configs = load_table_configs(spark, catalog, only)
    failures: list[tuple] = []
    for cfg in configs:
        audit.start("bronze", cfg.bronze_table)
        try:
            r = ingest_table(spark, catalog, cfg, run_id)
            audit.finish("bronze", cfg.bronze_table, "SUCCESS", r.rows_read, r.rows_written,
                         r.rows_rejected, r.target_version)
            print(f"[{cfg.source_table}] read={r.rows_read} written={r.rows_written} skipped_rows={r.rows_rejected} "
                  f"files: loaded={r.files_loaded} duplicate={r.files_duplicate} empty={r.files_empty}")
        except Exception as exc:  # noqa: BLE001 - one failed table must not stop the others
            audit.finish("bronze", cfg.bronze_table, "FAILED", error=f"{type(exc).__name__}: {exc}")
            failures.append((cfg.source_table, str(exc)[:300]))
            print(f"[{cfg.source_table}] FAILED: {exc}")
    if failures:
        raise RuntimeError(f"Bronze failed for {len(failures)} table(s): {failures}")
    return failures