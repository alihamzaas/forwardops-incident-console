from __future__ import annotations

from typing import Any

import pandas as pd
from pandas.api.types import CategoricalDtype

from backend.config import GOLD_CUSTOMER_DIR, GOLD_INCIDENT_DIR, SILVER_DIR
from backend.data.pipeline import load_pipeline_state, read_parquet_dataset, run_feature_pipeline
from backend.database import init_db, list_agent_runs, list_pipeline_runs


class ControlPlaneRepository:
    def bootstrap(self) -> None:
        init_db()
        if not GOLD_CUSTOMER_DIR.exists() or not GOLD_INCIDENT_DIR.exists():
            run_feature_pipeline()

    def dashboard_payload(self, recent_activity: list[dict[str, Any]] | None = None) -> dict[str, Any]:
        customer_health = read_parquet_dataset(GOLD_CUSTOMER_DIR)
        incidents = read_parquet_dataset(GOLD_INCIDENT_DIR)
        job_features = read_parquet_dataset(SILVER_DIR)
        pipeline_state = load_pipeline_state()

        average_health = round(float(customer_health["customer_health_score"].mean()), 1) if not customer_health.empty else 0.0
        high_priority_incidents = int((incidents["severity"] == "high").sum()) if not incidents.empty else 0
        jobs_last_7d = int(customer_health["jobs_last_7d"].sum()) if not customer_health.empty else 0
        latest_event_at = None
        if not job_features.empty:
            latest_event_at = pd.to_datetime(job_features["event_ts"]).max().isoformat()

        return {
            "overview": {
                "customer_count": int(customer_health.shape[0]),
                "average_health_score": average_health,
                "high_priority_incidents": high_priority_incidents,
                "jobs_last_7d": jobs_last_7d,
                "latest_event_at": latest_event_at,
                "last_pipeline_run_at": pipeline_state.get("finished_at"),
            },
            "customers": frame_to_records(customer_health.sort_values("customer_health_score")),
            "incidents": frame_to_records(
                incidents.sort_values(["severity_rank", "event_ts"], ascending=[True, False]).head(14)
            ),
            "pipeline_runs": list_pipeline_runs(),
            "agent_runs": list_agent_runs(),
            "pipeline_state": pipeline_state,
            "recent_activity": recent_activity or [],
        }

    def load_incident(self, incident_id: str) -> dict[str, Any] | None:
        incidents = read_parquet_dataset(GOLD_INCIDENT_DIR)
        if incidents.empty:
            return None
        match = incidents.loc[incidents["incident_id"] == incident_id]
        if match.empty:
            return None
        return frame_to_records(match)[0]

    def load_customer_health(self, customer_id: str) -> dict[str, Any] | None:
        customer_health = read_parquet_dataset(GOLD_CUSTOMER_DIR)
        if customer_health.empty:
            return None
        match = customer_health.loc[customer_health["customer_id"] == customer_id]
        if match.empty:
            return None
        return frame_to_records(match)[0]

    def load_comparable_incidents(
        self,
        failure_category: str,
        exclude_incident_id: str,
        limit: int = 3,
    ) -> list[dict[str, Any]]:
        incidents = read_parquet_dataset(GOLD_INCIDENT_DIR)
        if incidents.empty:
            return []
        filtered = incidents.loc[
            (incidents["failure_category"] == failure_category) & (incidents["incident_id"] != exclude_incident_id)
        ]
        if filtered.empty:
            return []
        filtered = filtered.sort_values(["severity_rank", "event_ts"], ascending=[True, False]).head(limit)
        return frame_to_records(filtered)


def frame_to_records(dataframe: pd.DataFrame) -> list[dict[str, Any]]:
    if dataframe.empty:
        return []

    serializable = dataframe.copy()
    for column in serializable.columns:
        if pd.api.types.is_datetime64_any_dtype(serializable[column]):
            serializable[column] = serializable[column].dt.strftime("%Y-%m-%dT%H:%M:%S")
        elif isinstance(serializable[column].dtype, CategoricalDtype):
            serializable[column] = serializable[column].astype(str)
    return serializable.to_dict(orient="records")
