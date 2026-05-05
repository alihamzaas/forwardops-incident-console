from __future__ import annotations

import json
import mimetypes
import queue
import threading
import traceback
import uuid
from collections import deque
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

from backend.agents.graph import run_triage
from backend.config import FRONTEND_DIR, HOST, PORT
from backend.data.pipeline import run_feature_pipeline
from backend.database import record_agent_run, record_pipeline_run
from backend.repository import ControlPlaneRepository


class EventBroker:
    def __init__(self) -> None:
        self._subscribers: set[queue.Queue] = set()
        self._lock = threading.Lock()

    def subscribe(self) -> queue.Queue:
        subscriber: queue.Queue = queue.Queue()
        with self._lock:
            self._subscribers.add(subscriber)
        return subscriber

    def unsubscribe(self, subscriber: queue.Queue) -> None:
        with self._lock:
            self._subscribers.discard(subscriber)

    def publish(self, event_name: str, payload: dict) -> None:
        with self._lock:
            subscribers = list(self._subscribers)
        for subscriber in subscribers:
            subscriber.put((event_name, payload))


class AppState:
    def __init__(self) -> None:
        self.repository = ControlPlaneRepository()
        self.repository.bootstrap()
        self.broker = EventBroker()
        self.pipeline_jobs: dict[str, dict] = {}
        self.agent_jobs: dict[str, dict] = {}
        self.report_cache: dict[str, dict] = {}
        self.activity: deque[dict] = deque(maxlen=40)
        self.lock = threading.Lock()
        self._append_activity(
            kind="bootstrap",
            title="Control plane ready",
            detail="Loaded sample telemetry, parquet tables, and the incident console.",
        )

    def dashboard(self) -> dict:
        with self.lock:
            activity = list(self.activity)
        return self.repository.dashboard_payload(recent_activity=activity)

    def start_pipeline(self) -> dict:
        run_id = f"pipe-{uuid.uuid4().hex[:8]}"
        started_at = utcnow()
        job = {"id": run_id, "status": "running", "started_at": started_at}
        with self.lock:
            self.pipeline_jobs[run_id] = job
        self._publish(
            "pipeline",
            {
                "run_id": run_id,
                "status": "running",
                "message": "Rebuilding bronze, silver, and gold parquet datasets.",
            },
            title="Pipeline refresh started",
            detail="Refreshing partitioned telemetry and customer health views.",
        )
        thread = threading.Thread(target=self._run_pipeline, args=(run_id, started_at), daemon=True)
        thread.start()
        return job

    def _run_pipeline(self, run_id: str, started_at: str) -> None:
        try:
            artifacts = run_feature_pipeline()
            with self.lock:
                self.pipeline_jobs[run_id] = {
                    "id": run_id,
                    "status": "completed",
                    "started_at": started_at,
                    "finished_at": artifacts.finished_at,
                    "summary": artifacts.to_dict(),
                }
            record_pipeline_run(
                run_id=run_id,
                status="completed",
                started_at=started_at,
                finished_at=artifacts.finished_at,
                summary=artifacts.to_dict(),
            )
            self._publish(
                "pipeline",
                {
                    "run_id": run_id,
                    "status": "completed",
                    "message": f"Feature pipeline finished with {artifacts.incident_count} prioritized incidents.",
                    "summary": artifacts.to_dict(),
                },
                title="Pipeline refresh completed",
                detail=f"Wrote {artifacts.feature_rows} feature rows across bronze, silver, and gold datasets.",
            )
        except Exception as exc:
            error_text = str(exc)
            with self.lock:
                self.pipeline_jobs[run_id] = {
                    "id": run_id,
                    "status": "failed",
                    "started_at": started_at,
                    "finished_at": utcnow(),
                    "error": error_text,
                }
            record_pipeline_run(
                run_id=run_id,
                status="failed",
                started_at=started_at,
                finished_at=utcnow(),
                error_text=error_text,
            )
            self._publish(
                "pipeline",
                {
                    "run_id": run_id,
                    "status": "failed",
                    "message": error_text,
                },
                title="Pipeline refresh failed",
                detail=error_text,
            )

    def start_triage(self, incident_id: str) -> dict:
        run_id = f"triage-{uuid.uuid4().hex[:8]}"
        started_at = utcnow()
        job = {"id": run_id, "incident_id": incident_id, "status": "running", "started_at": started_at}
        with self.lock:
            self.agent_jobs[run_id] = job

        self._publish(
            "agent_status",
            {
                "run_id": run_id,
                "incident_id": incident_id,
                "status": "running",
                "message": "Forward-deployed triage agent is collecting incident evidence.",
            },
            title="Agent triage started",
            detail=f"Investigating incident {incident_id}.",
        )

        thread = threading.Thread(target=self._run_triage, args=(run_id, incident_id, started_at), daemon=True)
        thread.start()
        return job

    def _run_triage(self, run_id: str, incident_id: str, started_at: str) -> None:
        def publish(event_name: str, payload: dict) -> None:
            self._publish(
                event_name,
                payload,
                title=payload.get("step", {}).get("title", "Agent update"),
                detail=payload.get("step", {}).get("detail", payload.get("message", "Agent emitted an update.")),
            )

        try:
            report = run_triage(run_id=run_id, incident_id=incident_id, repository=self.repository, publish=publish)
            finished_at = utcnow()
            with self.lock:
                self.agent_jobs[run_id] = {
                    "id": run_id,
                    "incident_id": incident_id,
                    "status": "completed",
                    "started_at": started_at,
                    "finished_at": finished_at,
                    "summary": report.to_dict(),
                }
                self.report_cache[run_id] = report.to_dict()
            record_agent_run(
                run_id=run_id,
                incident_id=incident_id,
                status="completed",
                started_at=started_at,
                finished_at=finished_at,
                summary=report.to_dict(),
            )
            self._append_activity(
                kind="agent_complete",
                title="Agent triage completed",
                detail=f"Prepared a customer-ready plan for {report.customer_name}.",
            )
        except Exception as exc:
            error_text = str(exc)
            finished_at = utcnow()
            with self.lock:
                self.agent_jobs[run_id] = {
                    "id": run_id,
                    "incident_id": incident_id,
                    "status": "failed",
                    "started_at": started_at,
                    "finished_at": finished_at,
                    "error": error_text,
                }
            record_agent_run(
                run_id=run_id,
                incident_id=incident_id,
                status="failed",
                started_at=started_at,
                finished_at=finished_at,
                error_text=error_text,
            )
            self._publish(
                "agent_error",
                {
                    "run_id": run_id,
                    "incident_id": incident_id,
                    "status": "failed",
                    "message": error_text,
                },
                title="Agent triage failed",
                detail=error_text,
            )

    def get_report(self, run_id: str) -> dict | None:
        with self.lock:
            return self.report_cache.get(run_id)

    def _publish(self, event_name: str, payload: dict, title: str, detail: str) -> None:
        enriched = {"timestamp": utcnow(), **payload}
        self._append_activity(kind=event_name, title=title, detail=detail)
        self.broker.publish(event_name, enriched)

    def _append_activity(self, kind: str, title: str, detail: str) -> None:
        with self.lock:
            self.activity.appendleft(
                {
                    "kind": kind,
                    "title": title,
                    "detail": detail,
                    "timestamp": utcnow(),
                }
            )


class RequestHandler(BaseHTTPRequestHandler):
    server_version = "ForwardOpsControlPlane/1.0"

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        path = parsed.path

        if path == "/api/health":
            self._send_json({"status": "ok"})
            return
        if path == "/api/dashboard":
            self._send_json(APP_STATE.dashboard())
            return
        if path.startswith("/api/incidents/"):
            incident_id = path.removeprefix("/api/incidents/")
            incident = APP_STATE.repository.load_incident(incident_id)
            if not incident:
                self._send_json({"error": "Incident not found."}, status=HTTPStatus.NOT_FOUND)
                return
            self._send_json(incident)
            return
        if path.startswith("/api/agent-runs/"):
            run_id = path.removeprefix("/api/agent-runs/")
            report = APP_STATE.get_report(run_id)
            if not report:
                self._send_json({"error": "Run not found."}, status=HTTPStatus.NOT_FOUND)
                return
            self._send_json(report)
            return
        if path == "/api/events":
            self._stream_events()
            return

        self._serve_static(path)

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        path = parsed.path

        if path == "/api/pipeline/run":
            self._send_json(APP_STATE.start_pipeline(), status=HTTPStatus.ACCEPTED)
            return
        if path == "/api/agent/triage":
            payload = self._read_json_body()
            incident_id = payload.get("incident_id")
            if not incident_id:
                self._send_json({"error": "incident_id is required."}, status=HTTPStatus.BAD_REQUEST)
                return
            self._send_json(APP_STATE.start_triage(incident_id), status=HTTPStatus.ACCEPTED)
            return

        self._send_json({"error": "Not found."}, status=HTTPStatus.NOT_FOUND)

    def log_message(self, format: str, *args) -> None:
        return

    def _read_json_body(self) -> dict:
        length = int(self.headers.get("Content-Length", "0"))
        if length <= 0:
            return {}
        raw = self.rfile.read(length)
        return json.loads(raw.decode("utf-8"))

    def _send_json(self, payload: dict, status: HTTPStatus = HTTPStatus.OK) -> None:
        encoded = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def _stream_events(self) -> None:
        subscriber = APP_STATE.broker.subscribe()
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "keep-alive")
        self.end_headers()

        try:
            self.wfile.write(b": connected\n\n")
            self.wfile.flush()
            while True:
                try:
                    event_name, payload = subscriber.get(timeout=12)
                except queue.Empty:
                    self.wfile.write(b": keep-alive\n\n")
                    self.wfile.flush()
                    continue

                packet = f"event: {event_name}\ndata: {json.dumps(payload)}\n\n"
                self.wfile.write(packet.encode("utf-8"))
                self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            pass
        finally:
            APP_STATE.broker.unsubscribe(subscriber)

    def _serve_static(self, path: str) -> None:
        if path in {"", "/"}:
            file_path = FRONTEND_DIR / "index.html"
        else:
            file_path = (FRONTEND_DIR / path.lstrip("/")).resolve()
            if not str(file_path).startswith(str(FRONTEND_DIR.resolve())):
                self._send_json({"error": "Forbidden."}, status=HTTPStatus.FORBIDDEN)
                return

        if not file_path.exists() or file_path.is_dir():
            self._send_json({"error": "Not found."}, status=HTTPStatus.NOT_FOUND)
            return

        mime_type, _ = mimetypes.guess_type(file_path.name)
        content = file_path.read_bytes()
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", mime_type or "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(content)))
        self.end_headers()
        self.wfile.write(content)


def utcnow() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


APP_STATE = AppState()


def run_server(host: str = HOST, port: int = PORT) -> None:
    server = ThreadingHTTPServer((host, port), RequestHandler)
    print(f"ForwardOps control plane running at http://{host}:{port}")
    server.serve_forever()


if __name__ == "__main__":
    run_server()
