from __future__ import annotations

import ast
import io
import multiprocessing as mp
import traceback
from contextlib import redirect_stdout
from typing import Any

import pandas as pd

from backend.copilot.models import ExecutionTrace, SandboxResult


FORBIDDEN_NAMES = {
    "__import__",
    "eval",
    "exec",
    "compile",
    "open",
    "input",
    "globals",
    "locals",
    "vars",
    "dir",
    "getattr",
    "setattr",
    "delattr",
    "breakpoint",
}
FORBIDDEN_MODULES = {"os", "sys", "subprocess", "socket", "pathlib", "shutil", "requests", "urllib"}


def validate_code(code: str) -> list[ExecutionTrace]:
    traces = [ExecutionTrace("validate", "Parsed generated Python code.")]
    tree = ast.parse(code)
    for node in ast.walk(tree):
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            module_names = []
            if isinstance(node, ast.Import):
                module_names = [alias.name.split(".")[0] for alias in node.names]
            elif node.module:
                module_names = [node.module.split(".")[0]]
            blocked = sorted(set(module_names) & FORBIDDEN_MODULES)
            if blocked or module_names:
                raise ValueError(f"Imports are disabled in the sandbox. Saw: {', '.join(module_names)}")
        if isinstance(node, ast.Call):
            name = call_name(node)
            if name in FORBIDDEN_NAMES:
                raise ValueError(f"Sandbox blocked unsafe call: {name}")
            if name and name.split(".")[0] in FORBIDDEN_MODULES:
                raise ValueError(f"Sandbox blocked unsafe module access: {name}")
        if isinstance(node, ast.Attribute) and node.attr.startswith("__"):
            raise ValueError("Dunder attribute access is blocked in the sandbox.")
    traces.append(ExecutionTrace("validate", "Code passed sandbox policy checks."))
    return traces


def run_guarded_code(code: str, dataframe: pd.DataFrame, timeout_seconds: int = 6) -> SandboxResult:
    traces: list[ExecutionTrace] = []
    try:
        traces.extend(validate_code(code))
    except Exception as exc:
        traces.append(ExecutionTrace("validate", "Code failed sandbox policy checks.", {"error": str(exc)}))
        return SandboxResult(False, code, "", str(exc), None, traces)

    queue: mp.Queue = mp.Queue()
    process = mp.Process(target=_worker, args=(code, dataframe, queue), daemon=True)
    traces.append(ExecutionTrace("execute", "Started isolated worker process.", {"timeout_seconds": timeout_seconds}))
    process.start()
    process.join(timeout_seconds)

    if process.is_alive():
        process.terminate()
        process.join(1)
        traces.append(ExecutionTrace("execute", "Execution timed out and worker was terminated."))
        return SandboxResult(False, code, "", "Execution timed out.", None, traces)

    payload = queue.get() if not queue.empty() else {"ok": False, "stdout": "", "error": "Worker exited without output.", "chart_json": None}
    traces.append(
        ExecutionTrace(
            "execute",
            "Worker finished execution.",
            {"ok": payload["ok"], "stdout_chars": len(payload.get("stdout") or "")},
        )
    )
    return SandboxResult(
        ok=bool(payload["ok"]),
        code=code,
        stdout=payload.get("stdout") or "",
        error=payload.get("error"),
        chart_json=payload.get("chart_json"),
        traces=traces,
    )


def _worker(code: str, dataframe: pd.DataFrame, queue: mp.Queue) -> None:
    try:
        import plotly.express as px
        import plotly.graph_objects as go
        import plotly.io as pio

        stdout = io.StringIO()
        safe_builtins = {
            "abs": abs,
            "bool": bool,
            "dict": dict,
            "enumerate": enumerate,
            "float": float,
            "int": int,
            "len": len,
            "list": list,
            "max": max,
            "min": min,
            "print": print,
            "range": range,
            "round": round,
            "set": set,
            "sorted": sorted,
            "str": str,
            "sum": sum,
            "tuple": tuple,
        }
        namespace: dict[str, Any] = {
            "df": dataframe.copy(),
            "pd": pd,
            "px": px,
            "go": go,
        }
        with redirect_stdout(stdout):
            exec(compile(code, "<copilot-sandbox>", "exec"), {"__builtins__": safe_builtins}, namespace)
        fig = namespace.get("fig")
        chart_json = pio.to_json(fig) if fig is not None else None
        queue.put({"ok": True, "stdout": stdout.getvalue(), "error": None, "chart_json": chart_json})
    except Exception:
        queue.put({"ok": False, "stdout": "", "error": traceback.format_exc(), "chart_json": None})


def call_name(node: ast.Call) -> str | None:
    if isinstance(node.func, ast.Name):
        return node.func.id
    if isinstance(node.func, ast.Attribute):
        parts = [node.func.attr]
        value = node.func.value
        while isinstance(value, ast.Attribute):
            parts.append(value.attr)
            value = value.value
        if isinstance(value, ast.Name):
            parts.append(value.id)
        return ".".join(reversed(parts))
    return None
