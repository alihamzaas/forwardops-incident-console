# Oracle FDE Project Walkthrough

## What This Project Demonstrates

This demo is intentionally shaped around the Oracle Forward Deployed Engineering role instead of being a generic AI chatbot:

- A local data engineering pipeline writes partitioned Parquet datasets across bronze, silver, and gold layers.
- The backend exposes real REST endpoints plus an SSE event stream for live operational updates.
- The frontend presents customer health, incident prioritization, and agentic triage in one workflow.
- The hosted site can also run in static demo mode for GitHub Pages.

## Architecture

### 1. Raw telemetry generation

The app seeds realistic customer telemetry for four fictional accounts:

- Nova Retail
- Atlas Health
- Meridian Energy
- Helios Logistics

Each customer has distinct workloads, recurring failure patterns, SLAs, and owner teams. That keeps the demo specific and gives you something concrete to talk through.

### 2. Medallion-style feature pipeline

The pipeline in [`backend/data/pipeline.py`](../backend/data/pipeline.py):

- Generates or reloads raw cluster telemetry
- Writes a bronze Parquet dataset partitioned by date and customer
- Builds a silver feature table with reliability, queue pressure, memory pressure, and SLA drift
- Builds gold tables for customer health and incident queueing

The write path uses a temp-directory swap so the UI does not read partially written parquet datasets.

### 3. Control-plane API

The server in [`backend/main.py`](../backend/main.py) uses Python’s built-in HTTP server so it runs without missing framework dependencies. It provides:

- `GET /api/dashboard`
- `GET /api/health`
- `GET /api/incidents/<id>`
- `POST /api/pipeline/run`
- `POST /api/agent/triage`
- `GET /api/events`

That gives you RESTful APIs plus SSE, which maps well to the job description.

### 4. Hosted demo mode

When the site is published via GitHub Pages, there is no Python backend available. The web client detects that and falls back to static demo data from [`docs/demo/dashboard.json`](./demo/dashboard.json), while still preserving the same dashboard and triage UX.

That means:

- GitHub visitors can see and interact with the project immediately
- Recruiters do not hit a broken page
- You still keep the full local backend for deeper demos and code review

### 5. Agentic incident triage

The triage flow in [`backend/agents/graph.py`](../backend/agents/graph.py) is structured as staged tool use:

- Frame the incident
- Measure customer blast radius
- Pull comparable failures
- Build a runbook
- Draft the customer-facing update

This is not pretending to be a hidden LLM system. It is a grounded, inspectable agent workflow that you can explain confidently.

## What To Say In An Interview

Use language like this:

> I built a forward-deployed incident console for multi-customer data workloads. It simulates production Spark-style telemetry, materializes bronze/silver/gold Parquet datasets, surfaces account health and prioritized incidents, and includes an agentic triage flow that streams a runbook and customer update back to the UI over SSE. I also added a GitHub Pages-safe demo mode so the project stays interactive when hosted statically.

## Resume Framing

Keep it honest and specific. For example:

- Built an end-to-end forward-deployed incident console for multi-customer data workloads using Python, Parquet, SQLite, REST APIs, and SSE streaming.
- Designed a medallion-style telemetry pipeline that transforms raw cluster signals into reliability features, customer health scoring, and incident prioritization.
- Implemented an agentic triage workflow and a GitHub Pages-compatible demo mode so recruiters can interact with the project without a local backend.
