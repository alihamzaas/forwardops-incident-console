from __future__ import annotations

from typing import Any


RUNBOOK_LIBRARY = {
    "executor_oom": {
        "root_cause": "Executor memory was undersized for a wide join and the spill curve climbed sharply.",
        "actions": [
            "Pin the workload to a higher-memory executor profile for the next run.",
            "Increase shuffle partitions before the feature join to reduce per-task peak memory.",
            "Replay only the failed partition window instead of reprocessing the full batch.",
        ],
        "deployment": [
            "Update the workload config in staging with the higher-memory profile.",
            "Validate spill and retry metrics on one replay partition.",
            "Promote the config to production and keep the reserved queue warm for the next scheduled run.",
        ],
    },
    "skewed_shuffle": {
        "root_cause": "One or two keys are dominating partition sizes and forcing large shuffle retries.",
        "actions": [
            "Salt the hot key and raise shuffle partition count for this workload.",
            "Backfill the heaviest partition range first to unblock customer-facing freshness.",
            "Capture a skew snapshot so the customer team can see exactly which dimension exploded.",
        ],
        "deployment": [
            "Apply key salting behind a feature flag in staging.",
            "Run a targeted replay against the skewed date range.",
            "Promote the change once shuffle skew and task retry counts normalize.",
        ],
    },
    "metadata_timeout": {
        "root_cause": "The workload stalled on control-plane metadata calls during publish or manifest reads.",
        "actions": [
            "Retry against a warm metastore replica and move the publish off the busiest minute of the window.",
            "Cache the manifest lookup for repeated read-heavy stages.",
            "Add a customer-facing status note because the pipeline can recover without data loss.",
        ],
        "deployment": [
            "Validate catalog connectivity in staging.",
            "Shift the publish window by 10 minutes and rerun the affected stage.",
            "Deploy the retry configuration and confirm downstream readers reopen cleanly.",
        ],
    },
    "hot_partition": {
        "root_cause": "A single partition or merge key is absorbing disproportionate write pressure.",
        "actions": [
            "Compact the hot partition before the next merge.",
            "Split the write scope into smaller micro-batches for the overloaded partition.",
            "Run a targeted validation on downstream reads before widening the backfill.",
        ],
        "deployment": [
            "Apply the partition split in staging.",
            "Run a micro-batch backfill on the hotspot.",
            "Promote once merge latency and retry counts stabilize.",
        ],
    },
    "queue_saturation": {
        "root_cause": "The workload contended with other production jobs and never acquired enough reserved slots.",
        "actions": [
            "Move the workload to a reserved queue during the customer SLA window.",
            "Shift low-priority backfills out of the same time block.",
            "Keep the customer updated because latency is driven by shared-cluster pressure, not bad data.",
        ],
        "deployment": [
            "Reserve capacity for the workload in staging.",
            "Replay one representative batch to confirm end-to-end runtime.",
            "Promote the schedule and queue policy before the next customer window.",
        ],
    },
    "late_input_arrival": {
        "root_cause": "The upstream ingest landed late and compressed the downstream processing window.",
        "actions": [
            "Trigger the upstream catch-up flow and rerun only the delayed partitions.",
            "Add a freshness guard so the downstream publish waits for the complete landing set.",
            "Flag the upstream dependency in the customer update so ownership is clear.",
        ],
        "deployment": [
            "Patch the freshness guard in staging.",
            "Replay the delayed slice and validate the publish checkpoint.",
            "Roll out the guard before the next scheduled ingest window.",
        ],
    },
    "none": {
        "root_cause": "The pipeline is operating inside the normal performance envelope.",
        "actions": ["Keep monitoring the workload and compare next-run latency to the current baseline."],
        "deployment": ["No deployment change is required."],
    },
}


def load_incident_context(repository: Any, incident_id: str) -> dict[str, Any]:
    incident = repository.load_incident(incident_id)
    if not incident:
        raise ValueError(f"Incident {incident_id} was not found.")
    return incident


def load_customer_context(repository: Any, customer_id: str) -> dict[str, Any]:
    customer = repository.load_customer_health(customer_id)
    if not customer:
        raise ValueError(f"Customer {customer_id} was not found.")
    return customer


def load_comparable_failures(repository: Any, failure_category: str, incident_id: str) -> list[dict[str, Any]]:
    return repository.load_comparable_incidents(failure_category, incident_id, limit=3)


def build_runbook(incident: dict[str, Any], customer: dict[str, Any]) -> dict[str, Any]:
    template = RUNBOOK_LIBRARY[incident["failure_category"]]
    actions = list(template["actions"])
    deployment = list(template["deployment"])

    if incident["queue_depth"] >= 12:
        actions.append("Keep the workload pinned to reserved capacity until queue depth is back below 8.")
    if incident["memory_utilization_pct"] >= 90:
        actions.append("Add a memory profile check to the pre-flight validation for this workload.")
    if customer["open_incidents"] >= 2:
        deployment.append("Bundle this change with a short customer checkpoint so the recovery plan feels coordinated.")

    return {
        "root_cause": template["root_cause"],
        "actions": actions,
        "deployment": deployment,
    }


def draft_customer_update(
    incident: dict[str, Any],
    customer: dict[str, Any],
    runbook: dict[str, Any],
) -> str:
    return (
        f"We identified the main issue behind {incident['workload_name']} for {incident['customer_name']}: "
        f"{runbook['root_cause']} Current health for the account is {customer['customer_health_score']} and "
        f"the top remediation step is to {runbook['actions'][0].lower()}."
    )
