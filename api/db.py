from __future__ import annotations

import json
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from api.config import DB_PATH

_LOCK = threading.Lock()


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=60)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 60000")
    conn.execute("PRAGMA journal_mode = WAL")
    return conn


def init_db() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with _LOCK, connect() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS jobs (
                job_id TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                source TEXT NOT NULL,
                submitted_at TEXT NOT NULL,
                started_at TEXT,
                finished_at TEXT,
                output_dir TEXT NOT NULL,
                log_path TEXT NOT NULL,
                request_json TEXT NOT NULL,
                error TEXT
            );

            CREATE TABLE IF NOT EXISTS job_urls (
                job_id TEXT NOT NULL,
                url_index INTEGER NOT NULL,
                requested_url TEXT NOT NULL,
                status TEXT NOT NULL,
                started_at TEXT,
                finished_at TEXT,
                error TEXT,
                PRIMARY KEY (job_id, url_index),
                FOREIGN KEY (job_id) REFERENCES jobs(job_id)
            );
            """
        )


def create_job(job_id: str, source: str, urls: list[str], output_dir: Path, log_path: Path, request_payload: dict[str, Any]) -> None:
    now = utc_now()
    with _LOCK, connect() as conn:
        conn.execute(
            """
            INSERT INTO jobs (job_id, status, source, submitted_at, output_dir, log_path, request_json)
            VALUES (?, 'queued', ?, ?, ?, ?, ?)
            """,
            (job_id, source, now, str(output_dir), str(log_path), json.dumps(request_payload, sort_keys=True)),
        )
        conn.executemany(
            """
            INSERT INTO job_urls (job_id, url_index, requested_url, status)
            VALUES (?, ?, ?, 'queued')
            """,
            [(job_id, idx, url) for idx, url in enumerate(urls)],
        )


def update_job(job_id: str, status: str, *, error: str | None = None) -> None:
    now = utc_now()
    fields = ["status = ?"]
    values: list[Any] = [status]
    if status == "running":
        fields.append("started_at = COALESCE(started_at, ?)")
        values.append(now)
    if status in {"succeeded", "failed"}:
        fields.append("finished_at = COALESCE(finished_at, ?)")
        values.append(now)
    if error is not None:
        fields.append("error = ?")
        values.append(error)
    values.append(job_id)
    with _LOCK, connect() as conn:
        conn.execute(f"UPDATE jobs SET {', '.join(fields)} WHERE job_id = ?", values)


def update_url(job_id: str, url_index: int, status: str, *, error: str | None = None) -> None:
    now = utc_now()
    fields = ["status = ?"]
    values: list[Any] = [status]
    if status == "running":
        fields.append("started_at = COALESCE(started_at, ?)")
        values.append(now)
    if status in {"succeeded", "failed"}:
        fields.append("finished_at = COALESCE(finished_at, ?)")
        values.append(now)
    if error is not None:
        fields.append("error = ?")
        values.append(error)
    values.extend([job_id, url_index])
    with _LOCK, connect() as conn:
        conn.execute(
            f"UPDATE job_urls SET {', '.join(fields)} WHERE job_id = ? AND url_index = ?",
            values,
        )


def get_job(job_id: str) -> dict[str, Any] | None:
    with _LOCK, connect() as conn:
        row = conn.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,)).fetchone()
        if row is None:
            return None
        job = dict(row)
        urls = conn.execute(
            "SELECT * FROM job_urls WHERE job_id = ? ORDER BY url_index",
            (job_id,),
        ).fetchall()
        job["urls"] = [dict(url_row) for url_row in urls]
        return job


def list_jobs(status: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
    query = "SELECT * FROM jobs"
    params: tuple[Any, ...] = ()
    if status:
        query += " WHERE status = ?"
        params = (status,)
    query += " ORDER BY submitted_at DESC LIMIT ?"
    params = params + (limit,)
    with _LOCK, connect() as conn:
        rows = conn.execute(query, params).fetchall()
        return [dict(row) for row in rows]


def count_jobs(status: str) -> int:
    with _LOCK, connect() as conn:
        row = conn.execute("SELECT COUNT(*) AS n FROM jobs WHERE status = ?", (status,)).fetchone()
        return int(row["n"])


def mark_interrupted_running_jobs() -> None:
    now = utc_now()
    with _LOCK, connect() as conn:
        conn.execute(
            """
            UPDATE jobs
            SET status = 'failed', finished_at = ?, error = COALESCE(error, 'API restarted while job was running')
            WHERE status = 'running'
            """,
            (now,),
        )
        conn.execute(
            """
            UPDATE job_urls
            SET status = 'failed', finished_at = ?, error = COALESCE(error, 'API restarted while job was running')
            WHERE status = 'running'
            """,
            (now,),
        )
