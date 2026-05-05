from __future__ import annotations

import random
from datetime import datetime, timedelta

import pandas as pd

from backend.config import RAW_DATASET_PATH


CUSTOMERS = [
    {
        "customer_id": "nova-retail",
        "customer_name": "Nova Retail",
        "region": "us-east-1",
        "account_tier": "platinum",
        "cluster_name": "spark-lakehouse-a",
        "owner_team": "growth-ml",
        "risk_bias": 0.34,
        "workloads": [
            {
                "workload_name": "recommendation_features",
                "stage_name": "feature_join",
                "baseline_rows": 94,
                "baseline_shuffle": 128,
                "baseline_duration": 74,
                "executor_count": 42,
                "sla_minutes": 95,
                "primary_failure": "executor_oom",
                "secondary_failure": "skewed_shuffle",
                "scheduled_hour": 2,
            },
            {
                "workload_name": "pricing_refresh",
                "stage_name": "delta_merge",
                "baseline_rows": 51,
                "baseline_shuffle": 86,
                "baseline_duration": 58,
                "executor_count": 28,
                "sla_minutes": 70,
                "primary_failure": "hot_partition",
                "secondary_failure": "metadata_timeout",
                "scheduled_hour": 5,
            },
            {
                "workload_name": "store_recommendation_serving",
                "stage_name": "parquet_publish",
                "baseline_rows": 16,
                "baseline_shuffle": 24,
                "baseline_duration": 22,
                "executor_count": 12,
                "sla_minutes": 35,
                "primary_failure": "queue_saturation",
                "secondary_failure": "late_input_arrival",
                "scheduled_hour": 8,
            },
        ],
    },
    {
        "customer_id": "atlas-health",
        "customer_name": "Atlas Health",
        "region": "us-central-1",
        "account_tier": "platinum",
        "cluster_name": "hipaa-feature-fabric",
        "owner_team": "clinical-ai",
        "risk_bias": 0.28,
        "workloads": [
            {
                "workload_name": "claims_feature_extract",
                "stage_name": "claim_line_enrichment",
                "baseline_rows": 67,
                "baseline_shuffle": 74,
                "baseline_duration": 66,
                "executor_count": 36,
                "sla_minutes": 80,
                "primary_failure": "metadata_timeout",
                "secondary_failure": "late_input_arrival",
                "scheduled_hour": 1,
            },
            {
                "workload_name": "member_risk_scoring",
                "stage_name": "feature_aggregation",
                "baseline_rows": 31,
                "baseline_shuffle": 41,
                "baseline_duration": 49,
                "executor_count": 18,
                "sla_minutes": 60,
                "primary_failure": "queue_saturation",
                "secondary_failure": "executor_oom",
                "scheduled_hour": 4,
            },
            {
                "workload_name": "prior_auth_agent_cache",
                "stage_name": "serving_snapshot",
                "baseline_rows": 8,
                "baseline_shuffle": 12,
                "baseline_duration": 18,
                "executor_count": 10,
                "sla_minutes": 25,
                "primary_failure": "metadata_timeout",
                "secondary_failure": "hot_partition",
                "scheduled_hour": 6,
            },
        ],
    },
    {
        "customer_id": "meridian-energy",
        "customer_name": "Meridian Energy",
        "region": "us-west-2",
        "account_tier": "strategic",
        "cluster_name": "forecast-hpc-grid",
        "owner_team": "grid-forecasting",
        "risk_bias": 0.31,
        "workloads": [
            {
                "workload_name": "load_forecast_training",
                "stage_name": "feature_window_builder",
                "baseline_rows": 112,
                "baseline_shuffle": 152,
                "baseline_duration": 96,
                "executor_count": 48,
                "sla_minutes": 120,
                "primary_failure": "skewed_shuffle",
                "secondary_failure": "executor_oom",
                "scheduled_hour": 3,
            },
            {
                "workload_name": "outage_probability_features",
                "stage_name": "weather_join",
                "baseline_rows": 44,
                "baseline_shuffle": 57,
                "baseline_duration": 53,
                "executor_count": 22,
                "sla_minutes": 70,
                "primary_failure": "late_input_arrival",
                "secondary_failure": "queue_saturation",
                "scheduled_hour": 5,
            },
            {
                "workload_name": "dispatch_agent_snapshot",
                "stage_name": "embedding_publish",
                "baseline_rows": 10,
                "baseline_shuffle": 15,
                "baseline_duration": 20,
                "executor_count": 8,
                "sla_minutes": 28,
                "primary_failure": "queue_saturation",
                "secondary_failure": "metadata_timeout",
                "scheduled_hour": 7,
            },
        ],
    },
    {
        "customer_id": "helios-logistics",
        "customer_name": "Helios Logistics",
        "region": "us-south-1",
        "account_tier": "growth",
        "cluster_name": "route-optimizer-farm",
        "owner_team": "routing-ai",
        "risk_bias": 0.24,
        "workloads": [
            {
                "workload_name": "route_optimizer_features",
                "stage_name": "network_join",
                "baseline_rows": 58,
                "baseline_shuffle": 66,
                "baseline_duration": 63,
                "executor_count": 26,
                "sla_minutes": 78,
                "primary_failure": "queue_saturation",
                "secondary_failure": "hot_partition",
                "scheduled_hour": 2,
            },
            {
                "workload_name": "parcel_eta_model_refresh",
                "stage_name": "parquet_compaction",
                "baseline_rows": 29,
                "baseline_shuffle": 32,
                "baseline_duration": 39,
                "executor_count": 14,
                "sla_minutes": 48,
                "primary_failure": "hot_partition",
                "secondary_failure": "metadata_timeout",
                "scheduled_hour": 4,
            },
            {
                "workload_name": "support_agent_retrieval_cache",
                "stage_name": "serving_publish",
                "baseline_rows": 7,
                "baseline_shuffle": 9,
                "baseline_duration": 14,
                "executor_count": 6,
                "sla_minutes": 20,
                "primary_failure": "late_input_arrival",
                "secondary_failure": "queue_saturation",
                "scheduled_hour": 9,
            },
        ],
    },
]


IMPACT_BY_FAILURE = {
    "none": "Healthy run; no customer intervention required.",
    "executor_oom": "Feature refresh delayed; agent recommendations are partially stale.",
    "skewed_shuffle": "Cluster runtime expanded and the SLA window is at risk.",
    "metadata_timeout": "Downstream publish paused while metastore retries cleared.",
    "hot_partition": "One dataset partition is dominating runtime and causing retries.",
    "queue_saturation": "Shared cluster is saturated; customer dashboards may lag.",
    "late_input_arrival": "Upstream ingest landed late and compressed the processing window.",
}


def ensure_sample_data() -> pd.DataFrame:
    RAW_DATASET_PATH.parent.mkdir(parents=True, exist_ok=True)
    if RAW_DATASET_PATH.exists():
        return pd.read_csv(RAW_DATASET_PATH)

    dataframe = generate_sample_dataframe()
    dataframe.to_csv(RAW_DATASET_PATH, index=False)
    return dataframe


def generate_sample_dataframe(days: int = 18, seed: int = 14) -> pd.DataFrame:
    rng = random.Random(seed)
    records: list[dict[str, object]] = []
    window_end = datetime(2026, 4, 24, 9, 0)

    for day_offset in range(days):
        current_day = window_end - timedelta(days=days - day_offset - 1)
        for customer in CUSTOMERS:
            for workload in customer["workloads"]:
                status, failure_category = choose_outcome(customer, workload, current_day, rng)
                metrics = build_metrics(workload, status, failure_category, rng)
                event_ts = current_day.replace(
                    hour=workload["scheduled_hour"] + rng.randint(0, 1),
                    minute=rng.randint(4, 54),
                )
                job_suffix = f"{current_day.strftime('%m%d')}-{rng.randint(100, 999)}"

                records.append(
                    {
                        "event_ts": event_ts.isoformat(timespec="minutes"),
                        "partition_date": event_ts.date().isoformat(),
                        "customer_id": customer["customer_id"],
                        "customer_name": customer["customer_name"],
                        "region": customer["region"],
                        "account_tier": customer["account_tier"],
                        "cluster_name": customer["cluster_name"],
                        "owner_team": customer["owner_team"],
                        "environment": "production",
                        "workload_name": workload["workload_name"],
                        "stage_name": workload["stage_name"],
                        "job_id": f"{customer['customer_id']}-{workload['workload_name']}-{job_suffix}",
                        "storage_format": "parquet",
                        "table_layout": "medallion",
                        "sla_minutes": workload["sla_minutes"],
                        "status": status,
                        "failure_category": failure_category,
                        "user_impact": IMPACT_BY_FAILURE[failure_category],
                        "ticket_hint": build_ticket_hint(customer["customer_name"], workload["workload_name"], failure_category),
                        **metrics,
                    }
                )

    dataframe = pd.DataFrame(records).sort_values("event_ts").reset_index(drop=True)
    return dataframe


def choose_outcome(customer: dict, workload: dict, current_day: datetime, rng: random.Random) -> tuple[str, str]:
    risk = customer["risk_bias"]
    if current_day.weekday() in (1, 2, 3):
        risk += 0.03
    if workload["primary_failure"] in {"executor_oom", "skewed_shuffle"}:
        risk += 0.02

    roll = rng.random()
    if roll < risk * 0.32:
        return "failed", workload["primary_failure"]
    if roll < risk:
        secondary = workload["secondary_failure"]
        chosen = secondary if rng.random() < 0.55 else workload["primary_failure"]
        return "degraded", chosen
    return "success", "none"


def build_metrics(workload: dict, status: str, failure_category: str, rng: random.Random) -> dict[str, float | int]:
    rows = workload["baseline_rows"] + rng.randint(-8, 10)
    shuffle = workload["baseline_shuffle"] + rng.randint(-12, 14)
    duration = workload["baseline_duration"] + rng.randint(-8, 9)
    cpu = rng.randint(48, 76)
    memory = rng.randint(52, 79)
    queue_depth = rng.randint(2, 7)
    spill = max(0, round(shuffle * 0.03 + rng.uniform(0.2, 2.4), 1))
    failed_tasks = rng.randint(0, 3)
    retried_tasks = rng.randint(0, 6)
    gpu_hours = round(workload["executor_count"] * duration / 60 * 0.14, 1)

    if failure_category == "executor_oom":
        memory = rng.randint(91, 99)
        spill = round(spill + rng.uniform(10, 24), 1)
        duration += rng.randint(26, 48)
        queue_depth += rng.randint(2, 5)
        failed_tasks = rng.randint(14, 36)
        retried_tasks = rng.randint(18, 52)
    elif failure_category == "skewed_shuffle":
        shuffle += rng.randint(78, 142)
        spill = round(spill + rng.uniform(8, 18), 1)
        duration += rng.randint(20, 42)
        failed_tasks = rng.randint(7, 20)
        retried_tasks = rng.randint(16, 40)
        cpu = rng.randint(72, 92)
    elif failure_category == "metadata_timeout":
        duration += rng.randint(14, 26)
        queue_depth += rng.randint(3, 7)
        failed_tasks = rng.randint(2, 9)
        retried_tasks = rng.randint(5, 15)
        cpu = rng.randint(42, 61)
    elif failure_category == "hot_partition":
        shuffle += rng.randint(46, 98)
        spill = round(spill + rng.uniform(5, 11), 1)
        duration += rng.randint(18, 32)
        failed_tasks = rng.randint(9, 22)
        retried_tasks = rng.randint(10, 24)
    elif failure_category == "queue_saturation":
        queue_depth = rng.randint(12, 20)
        duration += rng.randint(24, 58)
        cpu = rng.randint(84, 97)
        memory = rng.randint(67, 88)
        retried_tasks = rng.randint(8, 20)
    elif failure_category == "late_input_arrival":
        rows = max(4, rows - rng.randint(8, 16))
        duration += rng.randint(12, 24)
        queue_depth += rng.randint(2, 5)
        failed_tasks = rng.randint(1, 6)

    if status == "success":
        failed_tasks = 0
        retried_tasks = max(0, retried_tasks - 2)

    output_rows = max(1, rows - rng.randint(1, 5))
    return {
        "input_rows_m": rows,
        "output_rows_m": output_rows,
        "shuffle_gb": shuffle,
        "spill_gb": spill,
        "cpu_utilization_pct": cpu,
        "memory_utilization_pct": memory,
        "queue_depth": queue_depth,
        "failed_tasks": failed_tasks,
        "retried_tasks": retried_tasks,
        "duration_min": duration,
        "executor_count": workload["executor_count"],
        "gpu_hours": gpu_hours,
    }


def build_ticket_hint(customer_name: str, workload_name: str, failure_category: str) -> str:
    if failure_category == "none":
        return f"{customer_name} {workload_name} completed in-family."
    return f"{customer_name} flagged {workload_name} for follow-up because of {failure_category.replace('_', ' ')}."
