from __future__ import annotations

import json
import os
from typing import Any


CLAUDE_MODEL = os.getenv("CLAUDE_MODEL", "claude-3-5-sonnet-20241022")


class ClaudeToolUseClient:
    """Small Claude wrapper with a deterministic fallback for local demos.

    The API path uses Anthropic's Messages API and declares tools so Claude can
    reason with structured dataset metadata. If the SDK or API key is missing,
    the same methods return predictable outputs, keeping tests and demos usable.
    """

    def __init__(self) -> None:
        self.api_key = os.getenv("ANTHROPIC_API_KEY")
        self._client = None

    @property
    def available(self) -> bool:
        return bool(self.api_key)

    def plan(self, query: str, profile_context: str) -> str:
        fallback = (
            "1. Inspect dataset shape, columns, and missing values.\n"
            "2. Identify numeric trends and categorical breakdowns relevant to the question.\n"
            "3. Generate guarded pandas code for the analysis.\n"
            "4. Execute the code, validate output quality, repair if needed, and summarize results."
        )
        return self._ask_claude(
            system="You are the Planner node in a data-analysis agent graph. Return a concise numbered plan.",
            user=f"Question: {query}\n\nDataset context:\n{profile_context}",
            fallback=fallback,
            tools=[dataset_profile_tool()],
        )

    def analyze(self, query: str, profile_context: str) -> str:
        fallback = (
            "The dataset should be analyzed with pandas. Focus on numeric summaries, group-by comparisons, "
            "missingness, outliers, and a plot-ready table if the query asks for visual output."
        )
        return self._ask_claude(
            system="You are the Analyst node. Identify useful analysis steps and data checks.",
            user=f"Question: {query}\n\nDataset context:\n{profile_context}",
            fallback=fallback,
            tools=[dataset_profile_tool(), analysis_hint_tool()],
        )

    def write_code(self, query: str, profile: dict[str, Any], findings: str) -> str:
        numeric = profile.get("numeric_columns", [])
        categorical = profile.get("categorical_columns", [])
        fallback = deterministic_code(query, numeric, categorical)
        prompt = (
            "Write Python pandas code for a guarded sandbox. The dataframe is named df. "
            "Do not import modules, read files, write files, use network calls, or access the OS. "
            "Print concise findings. If useful, assign a Plotly figure to fig.\n\n"
            f"Question: {query}\nProfile: {json.dumps(profile, default=str)}\nFindings: {findings}"
        )
        return self._ask_claude(
            system="You are the Coder node. Return only executable Python code.",
            user=prompt,
            fallback=fallback,
            tools=[code_policy_tool()],
        )

    def summarize(self, query: str, execution: dict[str, Any]) -> str:
        stdout = execution.get("stdout") or ""
        error = execution.get("error") or ""
        fallback = (
            f"Analysis completed for: {query}\n\n"
            f"Key output:\n{stdout[:1200] if stdout else 'No printed output was produced.'}"
        )
        if error:
            fallback = f"The analysis code failed after guarded execution. Error: {error}"
        return self._ask_claude(
            system="You are the Summarizer node. Turn execution output into a readable summary.",
            user=f"Question: {query}\nExecution result: {json.dumps(execution, default=str)[:6000]}",
            fallback=fallback,
            tools=[summary_policy_tool()],
        )

    def repair_code(self, query: str, code: str, error: str, profile: dict[str, Any]) -> str:
        fallback = deterministic_code(query, profile.get("numeric_columns", []), profile.get("categorical_columns", []))
        return self._ask_claude(
            system="You repair pandas analysis code for a restricted sandbox. Return only code.",
            user=f"Question: {query}\nBroken code:\n{code}\nError:\n{error}\nProfile:{json.dumps(profile, default=str)}",
            fallback=fallback,
            tools=[code_policy_tool()],
        )

    def _ask_claude(self, system: str, user: str, fallback: str, tools: list[dict[str, Any]]) -> str:
        if not self.api_key:
            return fallback
        try:
            if self._client is None:
                from anthropic import Anthropic

                self._client = Anthropic(api_key=self.api_key)
            messages: list[dict[str, Any]] = [{"role": "user", "content": user}]
            response = self._client.messages.create(
                model=CLAUDE_MODEL,
                max_tokens=1800,
                system=system,
                tools=tools,
                messages=messages,
            )
            tool_uses = [block for block in response.content if getattr(block, "type", None) == "tool_use"]
            if not tool_uses:
                return response_text(response) or fallback

            messages.append({"role": "assistant", "content": serialize_response_blocks(response.content)})
            messages.append(
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "tool_result",
                            "tool_use_id": block.id,
                            "content": json.dumps(handle_tool_use(block.name, block.input), default=str),
                        }
                        for block in tool_uses
                    ],
                }
            )
            follow_up = self._client.messages.create(
                model=CLAUDE_MODEL,
                max_tokens=1800,
                system=system,
                tools=tools,
                messages=messages,
            )
            return response_text(follow_up) or response_text(response) or fallback
        except Exception as exc:
            return f"{fallback}\n\nClaude fallback reason: {exc}"


def response_text(response: Any) -> str:
    return "\n".join(block.text for block in response.content if getattr(block, "type", None) == "text").strip()


def serialize_response_blocks(blocks: list[Any]) -> list[dict[str, Any]]:
    serialized = []
    for block in blocks:
        block_type = getattr(block, "type", None)
        if block_type == "text":
            serialized.append({"type": "text", "text": block.text})
        elif block_type == "tool_use":
            serialized.append({"type": "tool_use", "id": block.id, "name": block.name, "input": block.input})
    return serialized


def handle_tool_use(name: str, tool_input: dict[str, Any]) -> dict[str, Any]:
    handlers = {
        "inspect_dataset_profile": inspect_dataset_profile,
        "suggest_analysis_checks": suggest_analysis_checks,
        "validate_code_policy": validate_code_policy,
        "format_summary": format_summary,
    }
    handler = handlers.get(name)
    if handler is None:
        return {"ok": False, "message": f"Unknown tool: {name}"}
    return handler(tool_input)


def inspect_dataset_profile(tool_input: dict[str, Any]) -> dict[str, Any]:
    return {
        "ok": True,
        "focus": tool_input.get("question_focus", "general analysis"),
        "note": "Dataset profile is already included in the prompt context; use it to choose pandas operations.",
    }


def suggest_analysis_checks(tool_input: dict[str, Any]) -> dict[str, Any]:
    checks = tool_input.get("checks") or []
    return {
        "ok": True,
        "accepted_checks": checks[:8],
        "guidance": "Prefer checks that can be expressed as dataframe summaries, group-bys, and Plotly figures.",
    }


def validate_code_policy(tool_input: dict[str, Any]) -> dict[str, Any]:
    return {
        "ok": bool(tool_input.get("uses_only_dataframe", False)),
        "requires_dataframe_only": True,
        "sandbox_notes": [
            "Do not import modules.",
            "Do not read or write files.",
            "Use df, pd, px, and go objects that the sandbox provides.",
        ],
        "model_explanation": tool_input.get("explanation", ""),
    }


def format_summary(tool_input: dict[str, Any]) -> dict[str, Any]:
    return {
        "ok": True,
        "audience": tool_input.get("audience", "technical reviewer"),
        "include_caveats": bool(tool_input.get("include_caveats", True)),
    }


def deterministic_code(query: str, numeric_columns: list[str], categorical_columns: list[str]) -> str:
    all_columns = set(numeric_columns) | set(categorical_columns)
    if {"account", "workload", "duration_minutes", "sla_minutes"}.issubset(all_columns):
        risk_terms = []
        if "duration_minutes" in all_columns and "sla_minutes" in all_columns:
            risk_terms.append('analysis["sla_overrun_minutes"]')
        if "error_count" in all_columns:
            risk_terms.append('analysis["error_count"] * 8')
        if "retry_count" in all_columns:
            risk_terms.append('analysis["retry_count"] * 3')
        if "shuffle_spill_gb" in all_columns:
            risk_terms.append('analysis["shuffle_spill_gb"] * 0.5')
        risk_expression = " + ".join(risk_terms) or "0"
        return f"""print("Analysis question: {query}")
print("Rows:", len(df))
analysis = df.copy()
analysis["sla_overrun_minutes"] = (analysis["duration_minutes"] - analysis["sla_minutes"]).clip(lower=0)
analysis["risk_score"] = {risk_expression}
summary = analysis.groupby(["account", "workload"]).agg(
    runs=("run_id", "count"),
    avg_duration_minutes=("duration_minutes", "mean"),
    avg_cost_usd=("cost_usd", "mean"),
    errors=("error_count", "sum"),
    retries=("retry_count", "sum"),
    avg_sla_overrun_minutes=("sla_overrun_minutes", "mean"),
    risk_score=("risk_score", "mean"),
).sort_values("risk_score", ascending=False).head(10)
print(summary.to_string())
top = summary.reset_index().iloc[0].to_dict()
print("Top risk:", top)
fig = px.bar(summary.reset_index(), x="account", y="risk_score", color="workload", title="Operational risk score by account and workload")
"""
    if numeric_columns and categorical_columns:
        category = preferred_column(categorical_columns, query, ["account", "customer", "workload", "region", "team"])
        metric = preferred_column(numeric_columns, query, ["duration", "cost", "error", "retry", "risk", "sla", "spill", "rows"])
        return f"""print("Analysis question: {query}")
print("Rows:", len(df))
print("Columns:", list(df.columns))
summary = df.groupby({category!r})[{metric!r}].agg(["count", "mean", "min", "max"]).sort_values("mean", ascending=False).head(10)
print(summary.to_string())
fig = px.bar(summary.reset_index(), x={category!r}, y="mean", title="Average {metric} by {category}")
"""
    if numeric_columns:
        metric = preferred_column(numeric_columns, query, ["duration", "cost", "error", "retry", "risk", "sla", "spill", "rows"])
        return f"""print("Analysis question: {query}")
print("Rows:", len(df))
print(df[{metric!r}].describe().to_string())
fig = px.histogram(df, x={metric!r}, title="Distribution of {metric}")
"""
    return f"""print("Analysis question: {query}")
print("Rows:", len(df))
print(df.head(10).to_string())
"""


def preferred_column(columns: list[str], query: str, preferences: list[str]) -> str:
    lowered_query = query.lower()
    for column in columns:
        if column.lower() in lowered_query:
            return column
    for preference in preferences:
        for column in columns:
            if preference in column.lower():
                return column
    return columns[0]


def dataset_profile_tool() -> dict[str, Any]:
    return {
        "name": "inspect_dataset_profile",
        "description": "Inspect row count, column names, data types, missingness, and sample rows.",
        "input_schema": {
            "type": "object",
            "properties": {
                "columns": {"type": "array", "items": {"type": "string"}},
                "question_focus": {"type": "string"},
            },
            "required": ["question_focus"],
        },
    }


def analysis_hint_tool() -> dict[str, Any]:
    return {
        "name": "suggest_analysis_checks",
        "description": "Suggest pandas analysis checks before code generation.",
        "input_schema": {
            "type": "object",
            "properties": {
                "checks": {"type": "array", "items": {"type": "string"}},
            },
            "required": ["checks"],
        },
    }


def code_policy_tool() -> dict[str, Any]:
    return {
        "name": "validate_code_policy",
        "description": "Validate that generated code stays inside the pandas/plotly sandbox policy.",
        "input_schema": {
            "type": "object",
            "properties": {
                "uses_only_dataframe": {"type": "boolean"},
                "creates_plotly_figure": {"type": "boolean"},
                "explanation": {"type": "string"},
            },
            "required": ["uses_only_dataframe", "explanation"],
        },
    }


def summary_policy_tool() -> dict[str, Any]:
    return {
        "name": "format_summary",
        "description": "Format analysis output into a concise readable summary.",
        "input_schema": {
            "type": "object",
            "properties": {
                "audience": {"type": "string"},
                "include_caveats": {"type": "boolean"},
            },
            "required": ["audience"],
        },
    }
