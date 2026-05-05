from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass
class PipelineArtifacts:
    run_id: str
    started_at: str
    finished_at: str
    raw_rows: int
    feature_rows: int
    incident_count: int
    customer_count: int
    output_paths: dict[str, str]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class AgentStep:
    stage: str
    title: str
    detail: str
    metrics: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class AgentReport:
    run_id: str
    incident_id: str
    customer_id: str
    customer_name: str
    workload_name: str
    severity: str
    root_cause: str
    executive_summary: str
    evidence: list[str]
    next_actions: list[str]
    deployment_plan: list[str]
    customer_update: str
    comparable_incidents: list[dict[str, Any]]
    steps: list[AgentStep] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["steps"] = [step.to_dict() for step in self.steps]
        return payload
