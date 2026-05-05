# ForwardOps Incident Console

ForwardOps Incident Console is a portfolio-ready demo built to support an Oracle Forward Deployed Engineering application. It is intentionally grounded in the kinds of work the role calls out:

- Data engineering and distributed-system signals
- Customer-facing incident ownership
- Full-stack delivery with REST APIs and SSE
- Agentic AI orchestration with real workflow steps

Instead of a generic “upload a file and chat with AI” template, this app simulates a forward-deployed team operating production customer workloads across Spark-style pipelines and HPC-adjacent compute queues.

## What It Does

- Seeds realistic multi-customer telemetry for production workloads
- Materializes bronze, silver, and gold Parquet datasets
- Computes reliability, queue pressure, memory pressure, and SLA drift features
- Exposes a local control-plane API and SSE stream
- Renders a browser dashboard with customer health, incident queue, and live activity
- Runs an agentic triage workflow that produces an operator plan, deployment checklist, and customer update
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
2. Open the dashboard and review customer health.
3. Click a high-severity incident from the queue.
4. Launch the triage agent.
5. Walk through the streamed reasoning, deployment plan, and customer update.
6. Trigger “Rebuild Feature Pipeline” to show end-to-end data refresh behavior.

On GitHub Pages, the same flow works in static demo mode.

## How To Talk About It On Your Resume

Frame it as a personal or portfolio project. Good, honest wording:

- Built an end-to-end forward-deployed incident console for customer data workloads using Python, Parquet, SQLite, REST APIs, and SSE.
- Designed a medallion-style telemetry pipeline that computes reliability features, customer health scoring, and incident prioritization.
- Implemented an agentic triage workflow and a hosted static demo mode so recruiters can interact with the project directly from GitHub.

More detail lives in [docs/oracle-fde-project-walkthrough.md](./docs/oracle-fde-project-walkthrough.md).
