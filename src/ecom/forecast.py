"""Forecast scoring: load the registered champion model, apply the interval calibration,
append the result to ml.sales_forecast."""
from __future__ import annotations

import mlflow
import mlflow.prophet
import pandas as pd
from mlflow import MlflowClient
from pyspark.sql import functions as F

HORIZON_DAYS = 90
MODEL_ALIAS = "champion"
TARGET = "ml.sales_forecast"
SERIES = "total_net_sales"


def load_champion(catalog: str):
    """The model that currently holds the 'champion' alias in Unity Catalog."""
    mlflow.set_registry_uri("databricks-uc")
    name = f"{catalog}.ml.sales_forecast_prophet"
    version = MlflowClient().get_model_version_by_alias(name, MODEL_ALIAS).version
    return name, str(version), mlflow.prophet.load_model(f"models:/{name}@{MODEL_ALIAS}")


def _bucket(k: int) -> str:
    return "1-7" if k <= 7 else "8-30" if k <= 30 else "31-90"


def build_rows(raw: pd.DataFrame, calib: pd.DataFrame) -> pd.DataFrame:
    """raw = ds + yhat (the model's own daily forecast). Returns day, week and month rows with a
    calibrated forecast (raw x q50) and an 80% range (raw x q10, raw x q90)."""
    raw = raw.reset_index(drop=True).copy()
    q = calib.set_index(["grain", "bucket"])[["q10", "q50", "q90"]]

    def scaled(grain: str, bucket: str, base: float):
        r = q.loc[(grain, bucket)]
        return base * r["q50"], base * r["q10"], base * r["q90"]

    out = []
    for k, (ds, yhat) in enumerate(zip(raw["ds"], raw["yhat"]), start=1):
        out.append(("day", ds.date(), k, *scaled("day", _bucket(k), yhat)))
    for grain, size in (("week", 7), ("month", 30)):
        for start in range(0, len(raw) - size + 1, size):          # full blocks only
            block = raw.iloc[start:start + size]
            out.append((grain, block["ds"].iloc[0].date(), start + 1,
                        *scaled(grain, "all", block["yhat"].sum())))
    return pd.DataFrame(out, columns=["grain", "forecast_date", "horizon_days",
                                      "yhat", "yhat_lower", "yhat_upper"])


def score_and_store(spark, catalog: str, run_id: str) -> tuple[int, int]:
    name, version, model = load_champion(catalog)
    future = model.make_future_dataframe(periods=HORIZON_DAYS, include_history=False)
    raw = model.predict(future)[["ds", "yhat"]]
    calib = spark.table(f"{catalog}.ml.forecast_calibration").toPandas()

    rows = build_rows(raw, calib)
    rows["forecast_run_id"] = run_id
    rows["generated_at"] = pd.Timestamp.now()
    rows["model_name"] = name
    rows["model_version"] = version
    rows["series"] = SERIES
    rows["interval_level"] = 0.8

    table = f"{catalog}.{TARGET}"
    spark.sql(f"DELETE FROM {table} WHERE forecast_run_id = :rid", args={"rid": run_id})   # rerun-safe
    (spark.createDataFrame(rows)
          .select("forecast_run_id", "generated_at", "model_name", "model_version", "series", "grain",
                  F.col("forecast_date").cast("date"), F.col("horizon_days").cast("int"),
                  F.col("yhat").cast("decimal(18,2)"), F.col("yhat_lower").cast("decimal(18,2)"),
                  F.col("yhat_upper").cast("decimal(18,2)"), F.col("interval_level").cast("double"))
          .write.mode("append").saveAsTable(table))
    return len(raw), len(rows)


def run_forecast_scoring(spark, catalog: str, run_id: str, audit) -> None:
    audit.start("ml", TARGET)
    try:
        read, written = score_and_store(spark, catalog, run_id)
        version = int(spark.sql(f"DESCRIBE HISTORY {catalog}.{TARGET} LIMIT 1").first()["version"])
        audit.finish("ml", TARGET, "SUCCESS", read, written, 0, version)
        print(f"[{TARGET}] days forecast={read} rows written={written}")
    except Exception as exc:  # noqa: BLE001 - record the failure, then fail the job
        audit.finish("ml", TARGET, "FAILED", error=f"{type(exc).__name__}: {exc}")
        raise