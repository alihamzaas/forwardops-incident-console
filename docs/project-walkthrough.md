# Project Walkthrough

## What This Project Demonstrates

ForwardOps Incident Console is a personal full-stack data operations project. It focuses on realistic customer-style telemetry, reliability signals, incident prioritization, and an agent-assisted triage workflow.

- A local data engineering pipeline writes partitioned Parquet datasets across bronze, silver, and gold layers.
- The backend exposes REST endpoints plus an SSE event stream for live operational updates.
- The frontend presents account health, incident prioritization, and triage in one workflow.
- The hosted site can also run in static demo mode for GitHub Pages.

## Architecture

### 1. Raw telemetry generation

The app seeds realistic operational telemetry for four fictional accounts:

- Nova Retail
- Atlas Health
- Meridian Energy
- Helios Logistics

Each account has distinct workloads, recurring failure patterns, SLAs, and owner teams. That keeps the demo specific and gives the data model a more realistic shape.

### 2. Medallion-style feature pipeline

The pipeline in [`backend/data/pipeline.py`](../backend/data/pipeline.py):

- Generates or reloads raw cluster telemetry
- Writes a bronze Parquet dataset partitioned by date and account
- Builds a silver feature table with reliability, queue pressure, memory pressure, and SLA drift
- Builds gold tables for account health and incident queueing

The write path uses a temp-directory swap so the UI does not read partially written Parquet datasets.

### 3. Control-plane API

The server in [`backend/main.py`](../backend/main.py) uses Python's built-in HTTP server so it runs with a small dependency footprint. It provides:

- `GET /api/dashboard`
- `GET /api/health`
- `GET /api/incidents/<id>`
- `POST /api/pipeline/run`
- `POST /api/agent/triage`
- `GET /api/events`

That gives the project both REST APIs and a live event stream.

### 4. Hosted demo mode

When the site is published via GitHub Pages, there is no Python backend available. The web client detects that and falls back to static demo data from [`docs/demo/dashboard.json`](./demo/dashboard.json), while preserving the same dashboard and triage experience.

That means:

- Visitors can see and interact with the project immediately
- The hosted page stays functional without a running server
- The full local backend remains available for deeper demos and code review

### 5. Agent-assisted incident triage

The triage flow in [`backend/agents/graph.py`](../backend/agents/graph.py) is structured as staged tool use:

- Frame the incident
- Measure account blast radius
- Pull comparable failures
- Build a runbook
- Draft a stakeholder-facing update

This is a grounded, inspectable workflow rather than a vague text generator.

## Project Summary

Use language like this:

> I built a personal data operations console for multi-account workloads. It simulates production-style telemetry, materializes bronze/silver/gold Parquet datasets, surfaces account health and prioritized incidents, and includes an agent-assisted triage flow that streams a runbook and stakeholder update back to the UI over SSE. I also added a GitHub Pages-safe demo mode so the project stays interactive when hosted statically.

## Suggested Resume Framing

Keep it honest and specific:

- Built an end-to-end data operations console for multi-account workloads using Python, Parquet, SQLite, REST APIs, and SSE streaming.
- Designed a medallion-style telemetry pipeline that transforms raw cluster signals into reliability features, account health scoring, and incident prioritization.
- Implemented an agent-assisted triage workflow and a GitHub Pages-compatible demo mode so the project remains interactive without a local backend.
