from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any


DB_PATH = Path(__file__).resolve().parents[1] / "data" / "copilot_sessions.sqlite3"


def get_connection() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(DB_PATH)
    connection.row_factory = sqlite3.Row
    return connection


def init_copilot_db() -> None:
    connection = get_connection()
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS copilot_sessions (
            id TEXT PRIMARY KEY,
            filename TEXT NOT NULL,
            upload_path TEXT NOT NULL,
            profile_json TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS copilot_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id TEXT NOT NULL,
            query TEXT NOT NULL,
            result_json TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        """
    )
    connection.commit()
    connection.close()


def save_session(session_id: str, filename: str, upload_path: str, profile: dict[str, Any]) -> None:
    init_copilot_db()
    connection = get_connection()
    connection.execute(
        """
        INSERT OR REPLACE INTO copilot_sessions (id, filename, upload_path, profile_json)
        VALUES (?, ?, ?, ?)
        """,
        (session_id, filename, upload_path, json.dumps(profile, default=str)),
    )
    connection.commit()
    connection.close()


def load_session(session_id: str) -> dict[str, Any] | None:
    init_copilot_db()
    connection = get_connection()
    row = connection.execute(
        "SELECT id, filename, upload_path, profile_json, created_at FROM copilot_sessions WHERE id = ?",
        (session_id,),
    ).fetchone()
    connection.close()
    if not row:
        return None
    payload = dict(row)
    payload["profile"] = json.loads(payload.pop("profile_json"))
    return payload


def list_sessions() -> list[dict[str, Any]]:
    init_copilot_db()
    connection = get_connection()
    rows = connection.execute(
        """
        SELECT id, filename, profile_json, created_at
        FROM copilot_sessions
        ORDER BY created_at DESC
        LIMIT 25
        """
    ).fetchall()
    connection.close()
    sessions = []
    for row in rows:
        payload = dict(row)
        profile = json.loads(payload.pop("profile_json"))
        payload["rows"] = profile.get("rows")
        payload["columns"] = profile.get("columns")
        payload["column_names"] = profile.get("column_names", [])
        sessions.append(payload)
    return sessions


def save_run(session_id: str, query: str, result: dict[str, Any]) -> None:
    init_copilot_db()
    connection = get_connection()
    connection.execute(
        "INSERT INTO copilot_runs (session_id, query, result_json) VALUES (?, ?, ?)",
        (session_id, query, json.dumps(result, default=str)),
    )
    connection.commit()
    connection.close()
