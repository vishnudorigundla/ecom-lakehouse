"""Silver: Bronze -> clean, validated, deduplicated Delta tables, driven by pipeline_config and silver_specs.

Per table:
  1. Read only the Bronze rows newer than the watermark (control.watermark, on _ingest_ts).
  2. Clean and type the columns with the table's spec.
  3. Check the rules in control.dq_rules; rows that fail a quarantine rule go to control.quarantine_records.
  4. Keep the latest version per business key and MERGE into the Silver table.
  5. Only after a successful write, move the watermark. A failed run leaves it where it was.
"""
from __future__ import annotations

from dataclasses import dataclass
from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from ecom.audit import Audit
from ecom.config import TableConfig, load_table_configs
from ecom.silver_specs import SPECS, SilverSpec, get_spec

WATERMARK_COLUMN = "_ingest_ts"
WATERMARK_FORMAT = "%Y-%m-%d %H:%M:%S.%f"


# ---------------------------------------------------------------- watermark
def read_watermark(spark, catalog: str, silver_table: str) -> str:
    row = (spark.table(f"{catalog}.control.watermark")
           .where(F.col("table_name") == silver_table)
           .select("last_watermark_value").first())
    if row is None:
        raise ValueError(f"No watermark row for '{silver_table}'. Seed it in control.watermark first.")
    return row["last_watermark_value"]


def set_watermark(spark, catalog: str, silver_table: str, value: str, run_id: str) -> None:
    spark.sql(
        f"""UPDATE {catalog}.control.watermark
            SET last_watermark_value = :v, last_run_id = :run_id,
                last_success_ts = current_timestamp(), updated_at = current_timestamp()
            WHERE table_name = :t""",
        args={"v": value, "run_id": run_id, "t": silver_table})


# ---------------------------------------------------------------- reading new Bronze rows
def read_new_bronze(spark, catalog: str, cfg: TableConfig, last_watermark: str) -> tuple[DataFrame | None, str | None]:
    """Bronze rows loaded after the watermark, plus the new watermark value (the newest _ingest_ts seen).

    Returns (None, None) when there is nothing new. The upper bound is fixed once, so rows that Bronze
    writes while Silver is running are left for the next run instead of being skipped."""
    bronze = spark.table(f"{catalog}.{cfg.bronze_table}")
    new_rows = bronze.where(F.col(WATERMARK_COLUMN) > F.lit(last_watermark).cast("timestamp"))
    high = new_rows.agg(F.max(WATERMARK_COLUMN).alias("high")).first()["high"]
    if high is None:
        return None, None
    new_rows = new_rows.where(F.col(WATERMARK_COLUMN) <= F.lit(high))
    return new_rows, high.strftime(WATERMARK_FORMAT)



# ---------------------------------------------------------------- cleaning
def clean(df: DataFrame, spec: SilverSpec, run_id: str) -> DataFrame:
    """Apply the spec's expressions. The untouched Bronze row is kept as JSON (_raw_json) so quarantine
    can show exactly what arrived, even when cleaning turned a bad value into NULL."""
    raw_json = F.to_json(F.struct(*[F.col(c) for c in df.columns]))
    cleaned = df.select(
        *[F.expr(expression).alias(column) for column, expression in spec.expressions.items()],
        F.col("_ingest_ts"), F.col("_source_file"), raw_json.alias("_raw_json"))
    return (cleaned.withColumn("_processed_ts", F.current_timestamp())
                   .withColumn("_run_id", F.lit(run_id)))


# ---------------------------------------------------------------- data quality rules
def load_rules(spark, catalog: str, table_name: str) -> list:
    return (spark.table(f"{catalog}.control.dq_rules")
            .where((F.col("table_name") == table_name) & F.col("is_active")).collect())


def evaluate_rules(cleaned: DataFrame, rules: list) -> DataFrame:
    """Adds one True/False column per rule, named _ok_<rule_id>. A rule that evaluates to NULL counts as failed."""
    for rule in rules:
        cleaned = cleaned.withColumn(f"_ok_{rule['rule_id']}", F.coalesce(F.expr(rule["rule_expr"]), F.lit(False)))
    return cleaned


def split_valid_invalid(checked: DataFrame, rules: list) -> tuple[DataFrame, DataFrame]:
    """Valid = passes every quarantine rule. 'warn' and 'fail' rules never move rows aside."""
    must_pass = [F.col(f"_ok_{r['rule_id']}") for r in rules if r["severity"] == "quarantine"]
    if not must_pass:
        return checked, checked.limit(0)
    all_ok = reduce(lambda a, b: a & b, must_pass)
    return checked.where(all_ok), checked.where(~all_ok)



# ---------------------------------------------------------------- quarantine
def write_quarantine(spark, catalog: str, invalid: DataFrame, rules: list, cfg: TableConfig, run_id: str) -> int:
    """One quarantine row per rejected record per failed rule. Returns how many rows were written."""
    quarantine_rules = [r for r in rules if r["severity"] == "quarantine"]
    if not quarantine_rules:
        return 0
    failed_names = F.array(*[F.when(~F.col(f"_ok_{r['rule_id']}"), F.lit(r["rule_name"]))
                             for r in quarantine_rules])
    joined_key = F.concat_ws("|", *[F.col(k).cast("string") for k in cfg.primary_keys])
    quarantined = (invalid
                   .withColumn("_rule_name", F.explode(F.filter(failed_names, lambda name: name.isNotNull())))
                   .select(F.lit(run_id).alias("run_id"),
                           F.lit(cfg.source_table).alias("table_name"),
                           F.col("_rule_name").alias("rule_name"),
                           F.when(joined_key != "", joined_key).alias("record_key"),
                           F.col("_raw_json").alias("record_json"),
                           F.col("_source_file").alias("source_file"),
                           F.current_timestamp().alias("rejected_at"))
                   .persist())
    count = quarantined.count()
    if count:
        quarantined.write.format("delta").mode("append").saveAsTable(f"{catalog}.control.quarantine_records")
    quarantined.unpersist()
    return count


# ---------------------------------------------------------------- rule statistics
def rule_stats(checked: DataFrame, rules: list) -> tuple[int, dict[str, int]]:
    """Total rows checked and, per rule, how many failed. One pass over the data."""
    aggregates = [F.count(F.lit(1)).alias("total")] + [
        F.sum(F.when(~F.col(f"_ok_{r['rule_id']}"), 1).otherwise(0)).alias(f"failed_{r['rule_id']}")
        for r in rules]
    row = checked.agg(*aggregates).first()
    total = int(row["total"] or 0)
    return total, {r["rule_id"]: int(row[f"failed_{r['rule_id']}"] or 0) for r in rules}


def write_dq_results(spark, catalog: str, cfg: TableConfig, run_id: str, rules: list,
                     total: int, failed_by_rule: dict[str, int]) -> None:
    rows = [(run_id, cfg.source_table, r["rule_id"], r["rule_name"], r["severity"], total,
             failed_by_rule[r["rule_id"]],
             (1.0 - failed_by_rule[r["rule_id"]] / total) if total else 1.0) for r in rules]
    if not rows:
        return
    (spark.createDataFrame(rows, schema="run_id string, table_name string, rule_id string, rule_name string, "
                                        "severity string, total_rows bigint, failed_rows bigint, pass_rate double")
     .withColumn("evaluated_at", F.current_timestamp())
     .write.format("delta").mode("append").saveAsTable(f"{catalog}.control.dq_results"))


def enforce_fail_rules(rules: list, failed_by_rule: dict[str, int], table: str) -> None:
    """A rule with severity 'fail' stops the run if even one row breaks it."""
    broken = [(r["rule_name"], failed_by_rule[r["rule_id"]]) for r in rules
              if r["severity"] == "fail" and failed_by_rule[r["rule_id"]] > 0]
    if broken:
        raise RuntimeError(f"Data quality 'fail' rule(s) broken for {table}: {broken}")



# ---------------------------------------------------------------- latest version per key
def latest_per_key(valid: DataFrame, cfg: TableConfig) -> DataFrame:
    """One row per business key: the newest version. Exact duplicates collapse here too.

    Newest = highest order_by_col; ties go to the later load (_ingest_ts), then to the later file name,
    so a rerun always picks the same row."""
    order_col = cfg.order_by_col or "_ingest_ts"
    newest_first = Window.partitionBy(*cfg.primary_keys).orderBy(
        F.col(order_col).desc_nulls_last(), F.col("_ingest_ts").desc(), F.col("_source_file").desc())
    return (valid.withColumn("_rn", F.row_number().over(newest_first))
            .where(F.col("_rn") == 1)
            .drop("_rn"))


# ---------------------------------------------------------------- upsert
def merge_into_silver(spark, catalog: str, cfg: TableConfig, latest: DataFrame) -> None:
    """Insert new keys; update an existing key only if the incoming version is at least as new.

    The 'at least as new' check stops a late-arriving old event from overwriting a newer state."""
    target = f"{catalog}.{cfg.silver_table}"
    view = f"_silver_updates_{cfg.source_table}"
    latest.select(*spark.table(target).columns).createOrReplaceTempView(view)

    order_col = cfg.order_by_col or "_ingest_ts"
    on_clause = " AND ".join(f"t.{key} = s.{key}" for key in cfg.primary_keys)
    spark.sql(f"""MERGE INTO {target} t USING {view} s
                  ON {on_clause}
                  WHEN MATCHED AND (s.{order_col} >= t.{order_col} OR t.{order_col} IS NULL) THEN UPDATE SET *
                  WHEN NOT MATCHED THEN INSERT *""")



# ---------------------------------------------------------------- one table
@dataclass
class SilverResult:
    rows_read: int = 0          # new Bronze rows checked
    rows_written: int = 0       # latest-per-key rows sent to the MERGE
    rows_rejected: int = 0      # records moved to quarantine
    target_version: int | None = None


def _version(spark, table: str) -> int | None:
    if not spark.catalog.tableExists(table):
        return None
    return int(spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").first()["version"])


def process_table(spark, catalog: str, cfg: TableConfig, run_id: str) -> SilverResult:
    spec = get_spec(cfg.source_table)
    target = f"{catalog}.{cfg.silver_table}"
    result = SilverResult()

    last_watermark = read_watermark(spark, catalog, cfg.silver_table)
    new_rows, new_watermark = read_new_bronze(spark, catalog, cfg, last_watermark)
    if new_rows is None:                      # nothing new: the watermark stays where it is
        result.target_version = _version(spark, target)
        return result

    rules = load_rules(spark, catalog, cfg.source_table)
    checked = evaluate_rules(clean(new_rows, spec, run_id), rules).persist()
    latest = None
    try:
        total, failed_by_rule = rule_stats(checked, rules)
        write_dq_results(spark, catalog, cfg, run_id, rules, total, failed_by_rule)
        enforce_fail_rules(rules, failed_by_rule, cfg.source_table)

        valid, invalid = split_valid_invalid(checked, rules)
        write_quarantine(spark, catalog, invalid, rules, cfg, run_id)

        latest = latest_per_key(valid, cfg).persist()
        merge_into_silver(spark, catalog, cfg, latest)

        result.rows_read = total
        result.rows_rejected = invalid.count()
        result.rows_written = latest.count()
        set_watermark(spark, catalog, cfg.silver_table, new_watermark, run_id)   # last step, only on success
    finally:
        checked.unpersist()
        if latest is not None:
            latest.unpersist()

    result.target_version = _version(spark, target)
    return result


# ---------------------------------------------------------------- all tables
def run_silver(spark, catalog: str, run_id: str, audit: Audit, only: list[str] | None = None) -> list[tuple]:
    """Process every active table that has a Silver spec. One failed table does not stop the others;
    the job fails at the end if any table failed."""
    configs = load_table_configs(spark, catalog, only)
    skipped = [c.source_table for c in configs if c.source_table not in SPECS]
    if skipped:
        print(f"No Silver spec yet, skipped: {skipped}")
    failures: list[tuple] = []
    for cfg in [c for c in configs if c.source_table in SPECS]:
        audit.start("silver", cfg.silver_table)
        try:
            r = process_table(spark, catalog, cfg, run_id)
            audit.finish("silver", cfg.silver_table, "SUCCESS", r.rows_read, r.rows_written,
                         r.rows_rejected, r.target_version)
            print(f"[{cfg.source_table}] read={r.rows_read} written={r.rows_written} quarantined={r.rows_rejected}")
        except Exception as exc:  # noqa: BLE001 - one failed table must not stop the others
            audit.finish("silver", cfg.silver_table, "FAILED", error=f"{type(exc).__name__}: {exc}")
            failures.append((cfg.source_table, str(exc)[:300]))
            print(f"[{cfg.source_table}] FAILED: {exc}")
    if failures:
        raise RuntimeError(f"Silver failed for {len(failures)} table(s): {failures}")
    return failures