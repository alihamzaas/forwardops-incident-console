from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass
class DatasetProfile:
    session_id: str
    filename: str
    rows: int
    columns: int
    column_names: list[str]
    dtypes: dict[str, str]
    numeric_columns: list[str]
    categorical_columns: list[str]
    missing_values: dict[str, int]
    sample_rows: list[dict[str, Any]]
    parquet_path: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class ExecutionTrace:
    step: str
    message: str
    detail: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class SandboxResult:
    ok: bool
    code: str
    stdout: str
    error: str | None
    chart_json: str | None
    traces: list[ExecutionTrace]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["traces"] = [trace.to_dict() for trace in self.traces]
        return payload


@dataclass
class CopilotState:
    session_id: str
    query: str
    profile: dict[str, Any]
    dataframe_records: list[dict[str, Any]]
    plan: str = ""
    findings: str = ""
    code: str = ""
    execution: dict[str, Any] = field(default_factory=dict)
    summary: str = ""
    chart_json: str | None = None
    events: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
