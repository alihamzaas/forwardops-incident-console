# ForwardOps Incident Console

ForwardOps Incident Console is a personal full-stack data operations project. It simulates production-style workload telemetry, turns raw operational signals into account health and incident features, and provides an interactive dashboard for triage and recovery planning.

## What It Does

- Seeds realistic multi-account telemetry for production-style workloads
- Materializes bronze, silver, and gold Parquet datasets
- Computes reliability, queue pressure, memory pressure, and SLA drift features
- Exposes a local control-plane API and SSE stream
- Renders a browser dashboard with account health, incident queue, and live activity
- Runs an agent-assisted triage workflow that produces an operator plan, deployment checklist, and stakeholder update
- Falls back to static demo mode when hosted on GitHub Pages

## Tech Stack

- Backend: Python standard library HTTP server, SQLite, pandas, pyarrow
- Data layer: partitioned Parquet datasets with medallion-style modeling
- Frontend: static HTML, CSS, and JavaScript
- Streaming: Server-Sent Events (SSE)
- Hosting-ready mode: GitHub Pages static demo plus Docker-friendly full app

## Run Locally

From the project root:

```bash
cd agentic-data-copilot
chmod +x run_local.sh
./run_local.sh
```

Then open:

[`http://127.0.0.1:8000`](http://127.0.0.1:8000)

If you want the direct Python command:

```bash
cd agentic-data-copilot
/opt/anaconda3/bin/python3 -m backend.main
```

## Run The Checks

```bash
cd agentic-data-copilot
/opt/anaconda3/bin/python3 -m unittest discover tests
```

## Publish The Demo On GitHub Pages

The site assets live in `docs/`, and they automatically switch into static demo mode when no backend is available.

After you push the repo to GitHub:

1. Open the repository on GitHub.
2. Go to `Settings` -> `Pages`.
3. Under `Build and deployment`, choose `Deploy from a branch`.
4. Select your default branch and the `/docs` folder.
5. Save.

GitHub will publish a live demo page that works without the backend.

## Full Hosted App Option

If you want the full backend online, the repo also supports Docker-style deployment. For container hosting, make sure `LISTEN_HOST=0.0.0.0` is set, which is already handled in the Docker setup.

## Project Layout

```text
backend/
  agents/        grounded triage workflow and runbook tools
  data/          raw seed generation and parquet feature pipeline
  main.py        REST + SSE control-plane server
  repository.py  reads gold datasets for the dashboard and agent
docs/
  index.html     hosted site entrypoint
  app.js         frontend logic with live mode + demo mode
  styles.css     dashboard styling
  demo/          static GitHub Pages data
run_local.sh     interpreter-aware local launch script
```

## How To Demo It

1. Start the server locally.
2. Open the dashboard and review account health.
3. Click a high-severity incident from the queue.
4. Launch the triage agent.
5. Walk through the streamed reasoning, deployment plan, and stakeholder update.
6. Trigger “Rebuild Feature Pipeline” to show end-to-end data refresh behavior.

On GitHub Pages, the same flow works in static demo mode.

## Project Summary

- Built an end-to-end data operations console for workload telemetry using Python, Parquet, SQLite, REST APIs, and SSE.
- Designed a medallion-style telemetry pipeline that computes reliability features, account health scoring, and incident prioritization.
- Implemented an agent-assisted triage workflow and a hosted static demo mode so the project remains interactive from GitHub Pages.

More detail lives in [docs/project-walkthrough.md](./docs/project-walkthrough.md).
