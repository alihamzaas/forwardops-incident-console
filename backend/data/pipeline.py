from __future__ import annotations

import json
import shutil
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from backend.config import (
    BRONZE_DIR,
    GOLD_CUSTOMER_DIR,
    GOLD_INCIDENT_DIR,
    PIPELINE_STATE_PATH,
    RAW_DATASET_PATH,
    SILVER_DIR,
    SYSTEM_DIR,
)
from backend.data.seed import ensure_sample_data
from backend.models import PipelineArtifacts


ROOT_CAUSE_MAP = {
    "none": "Healthy pipeline execution",
    "executor_oom": "Memory pressure on wide Spark feature extraction stage",
    "skewed_shuffle": "Uneven partition distribution causing shuffle skew",
    "metadata_timeout": "Control-plane timeout during catalog or manifest access",
    "hot_partition": "A single hot partition dominated merge throughput",
    "queue_saturation": "Shared compute pool was saturated during production traffic",
    "late_input_arrival": "Upstream landing delay compressed the batch window",
}

RUNBOOK_MAP = {
    "none": "No action required; continue normal monitoring.",
    "executor_oom": "Increase executor memory for the hot stage, rebalance partitions, and re-run the failed slice.",
    "skewed_shuffle": "Salt the skewed key, raise shuffle partitions, and backfill the worst partition range.",
    "metadata_timeout": "Retry against a warm catalog replica and stagger metadata-heavy jobs away from the publish window.",
    "hot_partition": "Compact the hotspot partition, split the merge scope, and validate downstream reads before reopening traffic.",
    "queue_saturation": "Move the workload to a reserved pool and shift the low-priority jobs out of the same window.",
    "late_input_arrival": "Trigger the upstream ingest catch-up flow and run only the delayed partition before the full refresh.",
}

SEVERITY_RANK = {"high": 0, "medium": 1, "low": 2}


def run_feature_pipeline() -> PipelineArtifacts:
    started_at = utcnow()
    run_id = f"pipe-{uuid.uuid4().hex[:8]}"

    raw_frame = ensure_sample_data()
    bronze = build_bronze(raw_frame)
    silver = build_silver(bronze)
    customer_health = build_customer_health(silver)
    incidents = build_incident_queue(silver)

    write_parquet_dataset(bronze, BRONZE_DIR, partition_cols=["partition_date", "customer_id"])
    write_parquet_dataset(silver, SILVER_DIR, partition_cols=["severity", "customer_id"])
    write_parquet_dataset(customer_health, GOLD_CUSTOMER_DIR, partition_cols=["risk_band"])
    write_parquet_dataset(incidents, GOLD_INCIDENT_DIR, partition_cols=["severity"])

    artifacts = PipelineArtifacts(
        run_id=run_id,
        started_at=started_at,
        finished_at=utcnow(),
        raw_rows=int(raw_frame.shape[0]),
        feature_rows=int(silver.shape[0]),
        incident_count=int(incidents.shape[0]),
        customer_count=int(customer_health.shape[0]),
        output_paths={
            "raw": str(RAW_DATASET_PATH),
            "bronze": str(BRONZE_DIR),
            "silver": str(SILVER_DIR),
            "customer_health": str(GOLD_CUSTOMER_DIR),
            "incident_queue": str(GOLD_INCIDENT_DIR),
        },
    )
    persist_pipeline_state(artifacts.to_dict())
    return artifacts


def build_bronze(raw_frame: pd.DataFrame) -> pd.DataFrame:
    bronze = raw_frame.copy()
    bronze["event_ts"] = pd.to_datetime(bronze["event_ts"])
    bronze["partition_date"] = pd.to_datetime(bronze["partition_date"]).dt.strftime("%Y-%m-%d")
    bronze["ingested_at"] = utcnow()
    return bronze.sort_values("event_ts").reset_index(drop=True)


def build_silver(bronze: pd.DataFrame) -> pd.DataFrame:
    silver = bronze.copy()
    silver["incident_id"] = silver.apply(
        lambda row: f"inc-{row['customer_id'].split('-')[0]}-{pd.to_datetime(row['event_ts']).strftime('%m%d')}-{row.name:04d}",
        axis=1,
    )
    silver["latency_over_sla_min"] = (silver["duration_min"] - silver["sla_minutes"]).clip(lower=0)
    silver["memory_pressure_score"] = (
        ((silver["memory_utilization_pct"] - 68).clip(lower=0) * 0.8) + silver["spill_gb"] * 1.7
    ).round(1)
    silver["queue_pressure_score"] = ((silver["queue_depth"] / 20) * 100).clip(0, 100).round(1)
    silver["shuffle_pressure_score"] = ((silver["shuffle_gb"] / silver["input_rows_m"]).clip(lower=0.4) * 16).round(1)

    reliability_penalty = (
        silver["latency_over_sla_min"] * 0.65
        + silver["failed_tasks"] * 0.85
        + silver["retried_tasks"] * 0.28
        + silver["queue_depth"] * 1.1
        + silver["spill_gb"] * 0.95
        + (silver["memory_utilization_pct"] - 70).clip(lower=0) * 0.42
    )
    reliability_penalty += silver["status"].map({"success": 0, "degraded": 12, "failed": 28}).fillna(0)
    silver["reliability_score"] = (100 - reliability_penalty).clip(lower=8, upper=99).round(1)

    silver["severity"] = silver.apply(classify_severity, axis=1)
    silver["severity_rank"] = silver["severity"].map(SEVERITY_RANK).fillna(3).astype(int)
    silver["root_cause_guess"] = silver["failure_category"].map(ROOT_CAUSE_MAP)
    silver["recommended_action"] = silver["failure_category"].map(RUNBOOK_MAP)
    silver["customer_summary"] = silver.apply(
        lambda row: (
            f"{row['customer_name']} {row['workload_name']} is {row['status']} because of "
            f"{row['failure_category'].replace('_', ' ')}."
        ),
        axis=1,
    )
    return silver


def build_customer_health(silver: pd.DataFrame) -> pd.DataFrame:
    latest_event = pd.to_datetime(silver["event_ts"]).max()
    recent = silver.loc[pd.to_datetime(silver["event_ts"]) >= latest_event - pd.Timedelta(days=7)].copy()

    grouped = recent.groupby(["customer_id", "customer_name", "region", "account_tier", "cluster_name"], as_index=False).agg(
        jobs_last_7d=("job_id", "count"),
        failed_jobs=("status", lambda values: int((values == "failed").sum())),
        degraded_jobs=("status", lambda values: int((values == "degraded").sum())),
        open_incidents=("severity", lambda values: int((values == "high").sum())),
        avg_reliability_score=("reliability_score", "mean"),
        avg_latency_over_sla_min=("latency_over_sla_min", "mean"),
        avg_queue_depth=("queue_depth", "mean"),
        avg_duration_min=("duration_min", "mean"),
        success_rate_pct=("status", lambda values: round(float((values == "success").mean() * 100), 1)),
    )

    worst_workloads = (
        recent.sort_values("reliability_score")
        .groupby("customer_id", as_index=False)
        .first()[["customer_id", "workload_name", "failure_category"]]
        .rename(columns={"workload_name": "at_risk_workload", "failure_category": "dominant_failure"})
    )
    customer_health = grouped.merge(worst_workloads, on="customer_id", how="left")
    customer_health["customer_health_score"] = (
        100
        - customer_health["failed_jobs"] * 6
        - customer_health["degraded_jobs"] * 2.5
        - customer_health["open_incidents"] * 4
        - customer_health["avg_latency_over_sla_min"] * 0.4
        - (100 - customer_health["avg_reliability_score"]) * 0.35
    ).clip(lower=25, upper=98).round(1)
    customer_health["risk_band"] = customer_health["customer_health_score"].apply(classify_risk_band)
    customer_health["exec_summary"] = customer_health.apply(
        lambda row: (
            f"{row['customer_name']} is trending {row['risk_band']} with "
            f"{row['open_incidents']} high-severity incidents and {row['success_rate_pct']}% recent success."
        ),
        axis=1,
    )
    return customer_health.sort_values("customer_health_score").reset_index(drop=True)


def build_incident_queue(silver: pd.DataFrame) -> pd.DataFrame:
    incidents = silver.loc[
        (silver["severity"].isin(["high", "medium"])) | (silver["status"] != "success")
    ].copy()
    incidents["needs_customer_update"] = incidents["severity"].eq("high")
    incidents["summary_line"] = incidents.apply(
        lambda row: (
            f"{row['failure_category'].replace('_', ' ')} on {row['workload_name']} "
            f"for {row['customer_name']} with reliability {row['reliability_score']}."
        ),
        axis=1,
    )
    incidents = incidents.sort_values(["severity_rank", "event_ts"], ascending=[True, False]).head(16)
    return incidents.reset_index(drop=True)


def classify_severity(row: pd.Series) -> str:
    if (
        row["status"] == "failed"
        or row["reliability_score"] < 38
        or row["latency_over_sla_min"] > 40
        or row["queue_depth"] > 14
    ):
        return "high"
    if row["status"] == "degraded" or row["reliability_score"] < 68 or row["latency_over_sla_min"] > 15:
        return "medium"
    return "low"


def classify_risk_band(score: float) -> str:
    if score < 45:
        return "critical"
    if score < 72:
        return "watch"
    return "healthy"


def write_parquet_dataset(dataframe: pd.DataFrame, target_dir: Path, partition_cols: list[str]) -> None:
    target_dir.parent.mkdir(parents=True, exist_ok=True)
    temporary_dir = target_dir.parent / f"{target_dir.name}__tmp"
    if temporary_dir.exists():
        shutil.rmtree(temporary_dir)

    dataframe.to_parquet(temporary_dir, engine="pyarrow", index=False, partition_cols=partition_cols)

    # Swap directories atomically so the UI never reads half-written parquet data.
    if target_dir.exists():
        shutil.rmtree(target_dir)
    temporary_dir.rename(target_dir)


def read_parquet_dataset(target_dir: Path) -> pd.DataFrame:
    if not target_dir.exists():
        return pd.DataFrame()
    return pd.read_parquet(target_dir)


def persist_pipeline_state(payload: dict[str, object]) -> None:
    SYSTEM_DIR.mkdir(parents=True, exist_ok=True)
    PIPELINE_STATE_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def load_pipeline_state() -> dict[str, object]:
    if not PIPELINE_STATE_PATH.exists():
        return {}
    return json.loads(PIPELINE_STATE_PATH.read_text(encoding="utf-8"))


def utcnow() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()
