from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from backend.copilot.graph import AgentGraphState, run_agent_graph
from backend.copilot.profiling import profile_dataframe, read_dataset, save_upload_bytes
from backend.copilot.spark_pipeline import materialize_with_pyspark
from backend.copilot.storage import init_copilot_db, list_sessions, load_session, save_run, save_session


PROJECT_ROOT = Path(__file__).resolve().parent.parent
WEB_DIST = PROJECT_ROOT / "web" / "dist"
DOCS_DIR = PROJECT_ROOT / "docs"
SAMPLE_DATASET = PROJECT_ROOT / "samples" / "workload_metrics.csv"

app = FastAPI(title="Agentic Data Copilot", version="2.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


class AnalyzeRequest(BaseModel):
    session_id: str
    query: str


@app.on_event("startup")
def startup() -> None:
    init_copilot_db()


@app.get("/api/copilot/health")
def health() -> dict[str, Any]:
    return {
        "status": "ok",
        "features": {
            "fastapi": True,
            "sse": True,
            "langgraph": module_available("langgraph"),
            "claude_sdk": module_available("anthropic"),
            "claude_api_key_configured": bool(os.getenv("ANTHROPIC_API_KEY")),
            "pyspark": module_available("pyspark"),
        },
    }


@app.post("/api/copilot/upload")
async def upload_dataset(file: UploadFile = File(...)) -> dict[str, Any]:
    if not file.filename:
        raise HTTPException(status_code=400, detail="A CSV or Excel file is required.")
    if Path(file.filename).suffix.lower() not in {".csv", ".xlsx", ".xls"}:
        raise HTTPException(status_code=400, detail="Only CSV and Excel uploads are supported.")

    return persist_dataset(file.filename, await file.read())


@app.post("/api/copilot/sample")
def load_sample_dataset() -> dict[str, Any]:
    if not SAMPLE_DATASET.exists():
        raise HTTPException(status_code=404, detail="Sample dataset is missing from the repository.")
    return persist_dataset(SAMPLE_DATASET.name, SAMPLE_DATASET.read_bytes())


@app.get("/api/copilot/graph")
def graph_shape() -> dict[str, Any]:
    nodes = ["Planner", "Analyst", "Coder", "Executor", "Summarizer"]
    return {
        "nodes": nodes,
        "edges": [
            {"from": "Planner", "to": "Analyst"},
            {"from": "Analyst", "to": "Coder"},
            {"from": "Coder", "to": "Executor"},
            {"from": "Executor", "to": "Summarizer"},
        ],
        "notes": {
            "Planner": "Turns the user question and dataset profile into an explicit analysis plan.",
            "Analyst": "Chooses data checks and pandas/plotly strategies.",
            "Coder": "Uses Claude or the deterministic fallback to generate sandbox-safe Python.",
            "Executor": "Runs generated code in an isolated process with policy checks and retry repair.",
            "Summarizer": "Converts raw execution output into a readable result.",
        },
    }


def persist_dataset(filename: str, content: bytes) -> dict[str, Any]:
    session_id, upload_path = save_upload_bytes(filename, content)
    dataframe = read_dataset(upload_path)
    lake_result = materialize_with_pyspark(upload_path, session_id)
    profile = profile_dataframe(
        session_id=session_id,
        filename=filename,
        dataframe=dataframe,
        parquet_path=lake_result.get("bronze_path"),
    ).to_dict()
    profile["lake_result"] = lake_result
    save_session(session_id, filename, str(upload_path), profile)
    return {"session_id": session_id, "profile": profile}


@app.get("/api/copilot/sessions")
def sessions() -> list[dict[str, Any]]:
    return list_sessions()


@app.post("/api/copilot/analyze")
def analyze(request: AnalyzeRequest) -> StreamingResponse:
    session = load_session(request.session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found.")
    dataframe = read_dataset(Path(session["upload_path"]))
    initial_state: AgentGraphState = {
        "session_id": request.session_id,
        "query": request.query,
        "profile": session["profile"],
        "dataframe_records": dataframe.head(5000).to_dict(orient="records"),
        "events": [],
    }

    def stream_events():
        final_payload: dict[str, Any] = {}
        for event in run_agent_graph(initial_state):
            if event.get("stage") == "summarizer":
                final_payload = event.get("data", {})
            yield f"event: {event.get('stage', 'message')}\ndata: {json.dumps(event, default=str)}\n\n"
        save_run(request.session_id, request.query, final_payload)
        yield "event: done\ndata: {\"status\":\"complete\"}\n\n"

    return StreamingResponse(stream_events(), media_type="text/event-stream")


def module_available(module_name: str) -> bool:
    try:
        __import__(module_name)
        return True
    except Exception:
        return False


if WEB_DIST.exists():
    app.mount("/", StaticFiles(directory=WEB_DIST, html=True), name="web")
else:
    app.mount("/", StaticFiles(directory=DOCS_DIR, html=True), name="docs")
