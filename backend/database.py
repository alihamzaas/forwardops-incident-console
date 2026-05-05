from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from backend.config import DB_PATH


def get_connection() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(DB_PATH)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA journal_mode=WAL")
    return connection


def init_db() -> None:
    connection = get_connection()
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            id TEXT PRIMARY KEY,
            status TEXT NOT NULL,
            started_at TEXT NOT NULL,
            finished_at TEXT,
            summary_json TEXT,
            error_text TEXT
        );

        CREATE TABLE IF NOT EXISTS agent_runs (
            id TEXT PRIMARY KEY,
            incident_id TEXT NOT NULL,
            status TEXT NOT NULL,
            started_at TEXT NOT NULL,
            finished_at TEXT,
            summary_json TEXT,
            error_text TEXT
        );
        """
    )
    connection.commit()
    connection.close()


def record_pipeline_run(
    run_id: str,
    status: str,
    started_at: str,
    finished_at: str | None = None,
    summary: dict[str, Any] | None = None,
    error_text: str | None = None,
) -> None:
    _upsert(
        table_name="pipeline_runs",
        payload={
            "id": run_id,
            "status": status,
            "started_at": started_at,
            "finished_at": finished_at,
            "summary_json": json.dumps(summary) if summary is not None else None,
            "error_text": error_text,
        },
    )


def record_agent_run(
    run_id: str,
    incident_id: str,
    status: str,
    started_at: str,
    finished_at: str | None = None,
    summary: dict[str, Any] | None = None,
    error_text: str | None = None,
) -> None:
    _upsert(
        table_name="agent_runs",
        payload={
            "id": run_id,
            "incident_id": incident_id,
            "status": status,
            "started_at": started_at,
            "finished_at": finished_at,
            "summary_json": json.dumps(summary) if summary is not None else None,
            "error_text": error_text,
        },
    )


def list_pipeline_runs(limit: int = 6) -> list[dict[str, Any]]:
    connection = get_connection()
    rows = connection.execute(
        """
        SELECT id, status, started_at, finished_at, summary_json, error_text
        FROM pipeline_runs
        ORDER BY started_at DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    connection.close()
    return [_decode_row(dict(row)) for row in rows]


def list_agent_runs(limit: int = 8) -> list[dict[str, Any]]:
    connection = get_connection()
    rows = connection.execute(
        """
        SELECT id, incident_id, status, started_at, finished_at, summary_json, error_text
        FROM agent_runs
        ORDER BY started_at DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    connection.close()
    return [_decode_row(dict(row)) for row in rows]


def _upsert(table_name: str, payload: dict[str, Any]) -> None:
    connection = get_connection()
    keys = ", ".join(payload.keys())
    placeholders = ", ".join(["?"] * len(payload))
    updates = ", ".join([f"{key}=excluded.{key}" for key in payload.keys() if key != "id"])
    connection.execute(
        f"""
        INSERT INTO {table_name} ({keys})
        VALUES ({placeholders})
        ON CONFLICT(id) DO UPDATE SET {updates}
        """,
        tuple(payload.values()),
    )
    connection.commit()
    connection.close()


def _decode_row(row: dict[str, Any]) -> dict[str, Any]:
    if row.get("summary_json"):
        row["summary"] = json.loads(row["summary_json"])
    else:
        row["summary"] = None
    row.pop("summary_json", None)
    return row
