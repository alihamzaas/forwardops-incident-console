from __future__ import annotations

import time

from backend.models import AgentReport, AgentStep

from .tools import (
    build_runbook,
    draft_customer_update,
    load_comparable_failures,
    load_customer_context,
    load_incident_context,
)


def run_triage(run_id: str, incident_id: str, repository, publish=None, pause_seconds: float = 0.25) -> AgentReport:
    steps: list[AgentStep] = []

    def emit(stage: str, title: str, detail: str, metrics: dict | None = None) -> None:
        step = AgentStep(stage=stage, title=title, detail=detail, metrics=metrics or {})
        steps.append(step)
        if publish:
            publish(
                "agent_step",
                {
                    "run_id": run_id,
                    "incident_id": incident_id,
                    "step": step.to_dict(),
                },
            )
        time.sleep(pause_seconds)

    incident = load_incident_context(repository, incident_id)
    emit(
        "planner",
        "Framed the customer issue",
        (
            f"{incident['customer_name']} has a {incident['severity']} severity issue on "
            f"{incident['workload_name']} with reliability {incident['reliability_score']}."
        ),
        {
            "queue_depth": incident["queue_depth"],
            "latency_over_sla_min": incident["latency_over_sla_min"],
        },
    )

    customer = load_customer_context(repository, incident["customer_id"])
    emit(
        "blast-radius",
        "Measured customer blast radius",
        (
            f"Account health is {customer['customer_health_score']} with "
            f"{customer['open_incidents']} open high-priority incidents."
        ),
        {
            "customer_health_score": customer["customer_health_score"],
            "open_incidents": customer["open_incidents"],
        },
    )

    comparable_incidents = load_comparable_failures(repository, incident["failure_category"], incident_id)
    comparison_text = "Found no directly comparable incidents in the recent queue."
    if comparable_incidents:
        comparison_text = (
            f"Matched {len(comparable_incidents)} recent incidents with the same failure mode to guide remediation."
        )
    emit(
        "history",
        "Pulled comparable failures",
        comparison_text,
        {"matches": len(comparable_incidents)},
    )

    runbook = build_runbook(incident, customer)
    emit(
        "runbook",
        "Built the recovery plan",
        runbook["root_cause"],
        {"top_action": runbook["actions"][0]},
    )

    customer_update = draft_customer_update(incident, customer, runbook)
    emit(
        "customer-update",
        "Prepared the stakeholder update",
        customer_update,
    )

    report = AgentReport(
        run_id=run_id,
        incident_id=incident_id,
        customer_id=incident["customer_id"],
        customer_name=incident["customer_name"],
        workload_name=incident["workload_name"],
        severity=incident["severity"],
        root_cause=runbook["root_cause"],
        executive_summary=(
            f"{incident['customer_name']} needs a focused fix on {incident['workload_name']}. "
            f"The likely driver is {incident['failure_category'].replace('_', ' ')}, "
            f"and the fastest path is to {runbook['actions'][0].lower()}"
        ),
        evidence=[
            f"Reliability score is {incident['reliability_score']}.",
            f"Queue depth reached {incident['queue_depth']} and memory utilization reached {incident['memory_utilization_pct']}%.",
            f"Latency ran {incident['latency_over_sla_min']} minutes over SLA.",
            incident["user_impact"],
        ],
        next_actions=runbook["actions"],
        deployment_plan=runbook["deployment"],
        customer_update=customer_update,
        comparable_incidents=comparable_incidents,
        steps=steps,
    )

    if publish:
        publish(
            "agent_complete",
            {
                "run_id": run_id,
                "incident_id": incident_id,
                "report": report.to_dict(),
            },
        )

    return report
