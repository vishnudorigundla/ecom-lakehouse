"""Forecast training: fit the Prophet model on all history, log it to MLflow, register a new version.
A new version becomes the 'champion' only when promote=True."""
from __future__ import annotations

import logging

import mlflow
import mlflow.prophet
import pandas as pd
from mlflow import MlflowClient
from mlflow.models import infer_signature
from prophet import Prophet

HORIZON_DAYS = 90
HOLIDAYS_PRIOR_SCALE = 1.0
INTERVAL_WIDTH = 0.8
MODEL_ALIAS = "champion"
MODEL_TABLE = "sales_forecast_prophet"


def load_series(spark, catalog: str) -> pd.Series:
    df = spark.table(f"{catalog}.reporting.sales_daily").select("full_date", "net_sales").toPandas()
    df["full_date"] = pd.to_datetime(df["full_date"])
    return df.set_index("full_date")["net_sales"].astype(float).sort_index()


def load_holidays(spark, catalog: str) -> pd.DataFrame:
    cal = spark.table(f"{catalog}.ml.event_calendar").toPandas()
    return pd.DataFrame([{"holiday": r.event_name, "ds": d}
                         for r in cal.itertuples() for d in pd.date_range(r.start_date, r.end_date)])


def fit_model(series: pd.Series, holidays: pd.DataFrame) -> Prophet:
    m = Prophet(growth="linear", seasonality_mode="multiplicative", yearly_seasonality=True,
                weekly_seasonality=True, daily_seasonality=False, interval_width=INTERVAL_WIDTH,
                holidays=holidays, holidays_prior_scale=HOLIDAYS_PRIOR_SCALE)
    m.fit(pd.DataFrame({"ds": series.index, "y": series.values}))
    return m


def train_and_register(spark, catalog: str, run_id: str, promote: bool) -> tuple[str, int, int]:
    logging.getLogger("cmdstanpy").setLevel(logging.WARNING)
    logging.getLogger("prophet").setLevel(logging.WARNING)

    series = load_series(spark, catalog)
    holidays = load_holidays(spark, catalog)

    mlflow.set_registry_uri("databricks-uc")
    user = spark.sql("SELECT current_user()").first()[0]
    mlflow.set_experiment(f"/Users/{user}/ecom_sales_forecast")
    name = f"{catalog}.ml.{MODEL_TABLE}"

    with mlflow.start_run(run_name=f"train_{run_id}"):
        model = fit_model(series, holidays)
        future = model.make_future_dataframe(periods=HORIZON_DAYS, include_history=False)
        sample = model.predict(future)

        mlflow.log_params({
            "growth": "linear", "seasonality_mode": "multiplicative",
            "holidays_prior_scale": HOLIDAYS_PRIOR_SCALE, "interval_width": INTERVAL_WIDTH,
            "train_start": str(series.index.min().date()), "train_end": str(series.index.max().date()),
            "train_days": len(series), "event_names": holidays["holiday"].nunique(),
            "pipeline_run_id": run_id, "promoted": promote,
        })
        calib = spark.table(f"{catalog}.ml.forecast_calibration").toPandas()
        mlflow.log_table(calib.assign(calibrated_at=calib["calibrated_at"].astype(str)), "calibration.json")
        mlflow.prophet.log_model(model, "model", signature=infer_signature(future, sample),
                                 registered_model_name=name)

    client = MlflowClient()
    version = max(int(v.version) for v in client.search_model_versions(f"name='{name}'"))
    if promote:
        client.set_registered_model_alias(name, MODEL_ALIAS, version)
    return name, version, len(series)


def run_forecast_training(spark, catalog: str, run_id: str, audit, promote: bool = False) -> None:
    audit.start("ml", MODEL_TABLE)
    try:
        name, version, rows = train_and_register(spark, catalog, run_id, promote)
        audit.finish("ml", MODEL_TABLE, "SUCCESS", rows, 0, 0, version)
        print(f"[{name}] trained on {rows} days -> version {version}"
              f" ({'promoted to champion' if promote else 'NOT promoted'})")
    except Exception as exc:  # noqa: BLE001 - record the failure, then fail the job
        audit.finish("ml", MODEL_TABLE, "FAILED", error=f"{type(exc).__name__}: {exc}")
        raise