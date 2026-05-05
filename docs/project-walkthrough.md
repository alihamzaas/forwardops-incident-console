# Project Walkthrough

## What This Project Demonstrates

Agentic Data Copilot is a personal end-to-end AI/data application. It combines dataset upload, data profiling, a PySpark materialization path, Claude-assisted code generation, a LangGraph-style agent pipeline, guarded Python execution, SSE streaming, and a React dashboard.

The hosted GitHub Pages demo in `docs/` remains static and interactive for quick review. The full copilot runs through FastAPI locally or in Docker because it needs a backend process.

## Architecture

### 1. FastAPI intake and session storage

The backend in [`backend/fastapi_app.py`](../backend/fastapi_app.py) exposes dataset upload, sample loading, graph metadata, health checks, session listing, and SSE analysis endpoints.

Uploaded files are stored under `backend/data/copilot_uploads/`, profiled with pandas, and registered in SQLite through [`backend/copilot/storage.py`](../backend/copilot/storage.py). The React app shows the row count, column count, numeric columns, categorical columns, and materialization engine immediately after upload.

### 2. PySpark materialization path

[`backend/copilot/spark_pipeline.py`](../backend/copilot/spark_pipeline.py) attempts to create a local PySpark session and writes:

- Bronze parquet: the uploaded dataset plus ingestion metadata
- Silver parquet: column-level feature metadata such as data type, missing count, and row count

If Spark is not installed, the same function falls back to pandas and writes parquet outputs. That keeps the project runnable on small laptops while still preserving a real PySpark path for Docker or Spark-enabled environments.

### 3. LangGraph agent pipeline

[`backend/copilot/graph.py`](../backend/copilot/graph.py) defines the staged analysis flow:

- Planner: turns the user question and dataset profile into a concrete plan
- Analyst: chooses data checks and analysis strategy
- Coder: generates pandas/Plotly code
- Executor: validates and runs the code in the sandbox
- Summarizer: turns raw output into a readable answer

When LangGraph is installed, the backend compiles the graph with `StateGraph`. If LangGraph is unavailable during tests or lightweight demos, the code runs the same nodes sequentially and emits a runtime event explaining the fallback.

### 4. Claude API and tool-use integration

[`backend/copilot/claude_client.py`](../backend/copilot/claude_client.py) wraps Anthropic's Messages API. The client declares tools for dataset profile inspection, analysis check selection, sandbox policy validation, and summary formatting.

If `ANTHROPIC_API_KEY` is configured, Claude can use those tools before returning the plan, analysis notes, generated code, repair code, or summary. If the key is not configured, deterministic fallback logic keeps the demo functional and testable.

### 5. Guarded code-execution sandbox

[`backend/copilot/sandbox.py`](../backend/copilot/sandbox.py) makes generated code visible and controlled:

- Parses the generated code with `ast`
- Blocks imports, file access, network-style modules, unsafe builtins, and dunder attribute access
- Runs approved code in a separate multiprocessing worker
- Provides only `df`, `pd`, `px`, `go`, and safe builtins
- Captures stdout, errors, Plotly figure JSON, and execution traces
- Terminates the worker on timeout

If the first generated code attempt fails, the Executor node asks Claude for a repair when available, then retries with the same sandbox policy.

### 6. React dashboard and SSE streaming

[`web/src/App.jsx`](../web/src/App.jsx) is the full copilot UI. It shows:

- Upload/sample controls
- Runtime capability chips
- Dataset profile
- Planner/Analyst/Coder/Executor/Summarizer stage cards
- Generated Python code
- Sandbox validation and execution traces
- stdout and errors
- Plotly chart output
- Final summary

The analysis endpoint uses SSE. Because the request needs a JSON body, the React client uses `fetch()` with a streaming reader and parses SSE packets manually in [`web/src/api.js`](../web/src/api.js).

## How To Demo It

1. Run `docker compose up --build`.
2. Open `http://127.0.0.1:8000`.
3. Click `Load sample dataset`.
4. Ask the default question or upload a CSV/Excel file.
5. Click `Run analysis graph`.
6. Walk through the streamed events, generated code, sandbox traces, chart, and summary.

## Hosted Demo Mode

The public GitHub Pages link serves the static dashboard from `docs/`. It is intentionally lightweight so reviewers can click something immediately. The README explains that the full copilot requires local/Docker execution because GitHub Pages cannot run FastAPI, PySpark, SQLite writes, Claude API calls, or Python sandbox processes.
