from __future__ import annotations

from collections.abc import Iterator
from typing import Any, TypedDict

import pandas as pd

from backend.copilot.claude_client import ClaudeToolUseClient
from backend.copilot.profiling import profile_to_prompt
from backend.copilot.sandbox import run_guarded_code


class AgentGraphState(TypedDict, total=False):
    session_id: str
    query: str
    profile: dict[str, Any]
    dataframe_records: list[dict[str, Any]]
    plan: str
    findings: str
    code: str
    execution: dict[str, Any]
    summary: str
    chart_json: str | None
    events: list[dict[str, Any]]


def planner_node(state: AgentGraphState) -> AgentGraphState:
    client = ClaudeToolUseClient()
    plan = client.plan(state["query"], profile_to_prompt(state["profile"]))
    return with_event(state, "planner", {"plan": plan})


def analyst_node(state: AgentGraphState) -> AgentGraphState:
    client = ClaudeToolUseClient()
    findings = client.analyze(state["query"], profile_to_prompt(state["profile"]))
    return with_event(state, "analyst", {"findings": findings})


def coder_node(state: AgentGraphState) -> AgentGraphState:
    client = ClaudeToolUseClient()
    code = client.write_code(state["query"], state["profile"], state.get("findings", ""))
    return with_event(state, "coder", {"code": strip_markdown_fence(code)})


def executor_node(state: AgentGraphState) -> AgentGraphState:
    dataframe = pd.DataFrame(state["dataframe_records"])
    first_result = run_guarded_code(state["code"], dataframe)
    execution = first_result.to_dict()
    code = state["code"]

    if not first_result.ok:
        client = ClaudeToolUseClient()
        repaired = strip_markdown_fence(client.repair_code(state["query"], state["code"], first_result.error or "", state["profile"]))
        second_result = run_guarded_code(repaired, dataframe)
        execution = second_result.to_dict()
        execution["retry"] = {
            "attempted": True,
            "original_error": first_result.error,
            "original_code": state["code"],
        }
        code = repaired

    return with_event(
        state,
        "executor",
        {
            "code": code,
            "execution": execution,
            "chart_json": execution.get("chart_json"),
        },
    )


def summarizer_node(state: AgentGraphState) -> AgentGraphState:
    client = ClaudeToolUseClient()
    summary = client.summarize(state["query"], state.get("execution", {}))
    return with_event(state, "summarizer", {"summary": summary})


def build_langgraph():
    from langgraph.graph import END, StateGraph

    graph = StateGraph(AgentGraphState)
    graph.add_node("planner", planner_node)
    graph.add_node("analyst", analyst_node)
    graph.add_node("coder", coder_node)
    graph.add_node("executor", executor_node)
    graph.add_node("summarizer", summarizer_node)
    graph.set_entry_point("planner")
    graph.add_edge("planner", "analyst")
    graph.add_edge("analyst", "coder")
    graph.add_edge("coder", "executor")
    graph.add_edge("executor", "summarizer")
    graph.add_edge("summarizer", END)
    return graph.compile()


def run_agent_graph(initial_state: AgentGraphState) -> Iterator[dict[str, Any]]:
    try:
        graph = build_langgraph()
        seen = 0
        for state in graph.stream(initial_state, stream_mode="values"):
            events = state.get("events", [])
            for event in events[seen:]:
                yield event
            seen = len(events)
        return
    except Exception as exc:
        yield {"stage": "runtime", "data": {"message": f"LangGraph unavailable, running sequential fallback: {exc}"}}

    state = dict(initial_state)
    for node in (planner_node, analyst_node, coder_node, executor_node, summarizer_node):
        before = len(state.get("events", []))
        state = node(state)
        for event in state.get("events", [])[before:]:
            yield event


def with_event(state: AgentGraphState, stage: str, updates: dict[str, Any]) -> AgentGraphState:
    next_state = dict(state)
    next_state.update(updates)
    event_payload = {
        "stage": stage,
        "data": {key: value for key, value in updates.items() if key != "chart_json"},
    }
    if updates.get("chart_json"):
        event_payload["chart_json"] = updates["chart_json"]
    next_state["events"] = [*state.get("events", []), event_payload]
    return next_state


def strip_markdown_fence(text: str) -> str:
    stripped = text.strip()
    if stripped.startswith("```"):
        lines = stripped.splitlines()
        if lines and lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        return "\n".join(lines).strip()
    return stripped
