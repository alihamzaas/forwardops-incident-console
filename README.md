# Agentic Data Copilot

[Live Demo](https://alihamzaas.github.io/forwardops-incident-console/) | [Project Walkthrough](./docs/project-walkthrough.md)

Agentic Data Copilot is a personal full-stack AI/data engineering project. Users can upload a dataset, ask an analysis question, and watch the system plan the work, generate Python analysis code, run it in a guarded sandbox, and stream the results back to a React dashboard.

The repository also includes the hosted ForwardOps dashboard in `docs/`. GitHub Pages can only host static files, so the live link shows the interactive static demo. The full copilot experience runs locally or with Docker Compose because it needs a Python backend, SSE streaming, SQLite, and optional Claude API access.

## What It Does

- Uploads CSV or Excel datasets through a FastAPI backend.
- Profiles the dataset and stores each session in SQLite.
- Materializes uploaded data through a PySpark bronze/silver pipeline, with a pandas fallback for small local environments.
- Runs a LangGraph-style pipeline with Planner, Analyst, Coder, Executor, and Summarizer stages.
- Uses Claude through the Anthropic Messages API with tool-use hooks when `ANTHROPIC_API_KEY` is configured.
- Falls back to deterministic local planning/code generation when no Claude key is present, so the demo remains runnable.
- Executes generated pandas/Plotly code in a guarded multiprocessing sandbox with AST validation, traces, timeout handling, and retry repair.
- Streams each stage to the React dashboard over Server-Sent Events.

## Stack Map

- `backend/fastapi_app.py`: FastAPI app, upload endpoint, SSE analysis endpoint, health endpoint, static React hosting.
- `backend/copilot/graph.py`: Planner -> Analyst -> Coder -> Executor -> Summarizer graph. Uses LangGraph when installed and keeps a sequential fallback for demos/tests.
- `backend/copilot/claude_client.py`: Claude API wrapper with declared tools for dataset inspection, analysis checks, code policy validation, and summary formatting.
- `backend/copilot/sandbox.py`: Visible code-execution sandbox with AST policy checks, isolated worker process, stdout capture, Plotly JSON capture, timeout, and trace metadata.
- `backend/copilot/spark_pipeline.py`: PySpark materialization path for uploaded data plus pandas fallback.
- `backend/copilot/storage.py`: SQLite session and run storage.
- `web/src/App.jsx`: React dashboard for upload, graph stages, generated code, sandbox traces, stdout, chart output, and final summary.
- `Dockerfile` and `docker-compose.yml`: One-command deployment for the full app.

## Run The Full App With Docker

```bash
git clone https://github.com/alihamzaas/forwardops-incident-console.git
cd forwardops-incident-console

# Optional: enables the Claude path. Without this, the app uses deterministic fallbacks.
export ANTHROPIC_API_KEY="your-api-key"

docker compose up --build
```

Open [`http://127.0.0.1:8000`](http://127.0.0.1:8000).

## Run Locally Without Docker

```bash
git clone https://github.com/alihamzaas/forwardops-incident-console.git
cd forwardops-incident-console

python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r backend/requirements.txt

cd web
npm install
npm run build
cd ..

# Optional: enables Claude instead of deterministic local fallbacks.
export ANTHROPIC_API_KEY="your-api-key"

chmod +x run_local.sh
./run_local.sh
```

Then open [`http://127.0.0.1:8000`](http://127.0.0.1:8000).

For the PySpark path, local machines also need Java available. The Docker image installs OpenJDK for this. If Java/Spark is unavailable locally, the backend records the reason and uses the pandas parquet fallback instead.

## Demo Flow

1. Click `Load sample dataset` or upload a CSV/Excel file.
2. Review the dataset profile and lake materialization engine.
3. Ask a question such as: `Which account and workload has the highest operational risk?`
4. Click `Run analysis graph`.
5. Inspect the streamed Planner, Analyst, Coder, Executor, and Summarizer stages.
6. Review the generated Python code, sandbox traces, stdout, Plotly chart, and final summary.

## API Endpoints

- `GET /api/copilot/health`
- `GET /api/copilot/graph`
- `POST /api/copilot/sample`
- `POST /api/copilot/upload`
- `GET /api/copilot/sessions`
- `POST /api/copilot/analyze`

The `POST /api/copilot/analyze` endpoint returns an SSE stream. The React client parses that stream with `fetch()` so it can use `POST` while still rendering events as they arrive.

## Run Checks

```bash
python -m unittest discover tests
node --check docs/app.js
```

## Hosted Demo Note

GitHub Pages serves the static site from `docs/`, so the public demo stays clickable without any server. The full Agentic Data Copilot requires the local/Docker backend because browser-only hosting cannot run FastAPI, PySpark, SQLite writes, Claude API calls, or the Python sandbox.

## Suggested Resume Framing

- Built a full-stack data-analysis copilot where users upload a dataset and the system plans an analysis, writes Python code, runs it in a guarded sandbox, and streams results back to the browser.
- Implemented Claude API/tool-use integration, a LangGraph Planner/Analyst/Coder/Executor/Summarizer pipeline, FastAPI SSE endpoints, a React dashboard, SQLite session storage, and Docker Compose deployment.
- Added a PySpark bronze/silver materialization path and a visible execution sandbox with validation traces, timeout protection, Plotly output capture, and retry repair.
