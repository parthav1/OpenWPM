from __future__ import annotations

import json
import os
import queue
import subprocess
import threading
import uuid
from pathlib import Path
from typing import Any

from fastapi import Depends, FastAPI, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from api import db
from api.config import (
    API_TOKEN,
    HOST,
    JOBS_DIR,
    MAX_BATCH_URLS,
    OPENWPM_ROOT,
    PORT,
    PYTHON_BIN,
    USE_XVFB,
)
from api.schemas import CrawlJobCreated, CrawlJobStatus, CrawlRequest, HealthResponse

app = FastAPI(
    title="OpenWPM Crawler API",
    description="Internal API for submitting URL crawl jobs to the local OpenWPM crawler.",
    version="0.1.0",
)
security = HTTPBearer(auto_error=False)
job_queue: queue.Queue[str] = queue.Queue()
worker_started = False
worker_lock = threading.Lock()


def require_auth(credentials: HTTPAuthorizationCredentials | None = Depends(security)) -> None:
    if not API_TOKEN:
        return
    if credentials is None or credentials.scheme.lower() != "bearer" or credentials.credentials != API_TOKEN:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid or missing API token")


def make_job_id() -> str:
    return uuid.uuid4().hex[:16]


def run_subprocess(job_id: str) -> int:
    job_dir = JOBS_DIR / job_id
    log_path = job_dir / "runner.log"
    cmd = [PYTHON_BIN, "-m", "api.crawl_runner", "--job-id", job_id]
    if USE_XVFB:
        cmd = ["xvfb-run", "-a"] + cmd
    env = os.environ.copy()
    env["PYTHONPATH"] = str(OPENWPM_ROOT) + os.pathsep + env.get("PYTHONPATH", "")
    with log_path.open("ab") as log_file:
        log_file.write(("\n=== launching: " + " ".join(cmd) + " ===\n").encode())
        log_file.flush()
        proc = subprocess.Popen(
            cmd,
            cwd=OPENWPM_ROOT,
            stdout=log_file,
            stderr=log_file,
            env=env,
        )
        return proc.wait()


def worker_loop() -> None:
    while True:
        job_id = job_queue.get()
        try:
            job = db.get_job(job_id)
            if job is None:
                continue
            return_code = run_subprocess(job_id)
            refreshed = db.get_job(job_id)
            if refreshed and refreshed["status"] in {"queued", "running"}:
                if return_code == 0:
                    db.update_job(job_id, "succeeded")
                else:
                    db.update_job(job_id, "failed", error=f"crawler subprocess exited with code {return_code}")
        except Exception as exc:
            db.update_job(job_id, "failed", error=f"{type(exc).__name__}: {exc}")
        finally:
            job_queue.task_done()


def ensure_worker_started() -> None:
    global worker_started
    with worker_lock:
        if worker_started:
            return
        thread = threading.Thread(target=worker_loop, name="crawler-api-worker", daemon=True)
        thread.start()
        worker_started = True


@app.on_event("startup")
def startup() -> None:
    db.init_db()
    db.mark_interrupted_running_jobs()
    ensure_worker_started()
    for job in reversed(db.list_jobs(status="queued", limit=1000)):
        job_queue.put(job["job_id"])


@app.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    return HealthResponse(
        ok=True,
        service="openwpm-crawler-api",
        auth_required=bool(API_TOKEN),
        queued_jobs=db.count_jobs("queued"),
        running_jobs=db.count_jobs("running"),
    )


@app.post("/crawl", response_model=CrawlJobCreated, dependencies=[Depends(require_auth)])
def create_crawl(request: CrawlRequest) -> CrawlJobCreated:
    if len(request.urls) > MAX_BATCH_URLS:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Batch too large: max {MAX_BATCH_URLS} URLs per job",
        )

    job_id = make_job_id()
    job_dir = JOBS_DIR / job_id
    job_dir.mkdir(parents=True, exist_ok=False)
    payload: dict[str, Any] = request.model_dump(mode="json")
    urls = [str(url) for url in request.urls]
    request_path = job_dir / "request.json"
    log_path = job_dir / "runner.log"
    request_path.write_text(json.dumps(payload, indent=2, sort_keys=True))
    log_path.touch()

    db.create_job(
        job_id=job_id,
        source=request.source,
        urls=urls,
        output_dir=job_dir / "crawl_output",
        log_path=log_path,
        request_payload=payload,
    )
    job_queue.put(job_id)

    return CrawlJobCreated(
        job_id=job_id,
        status="queued",
        status_url=f"/jobs/{job_id}",
        results_url=f"/jobs/{job_id}/results",
    )


@app.get("/jobs/{job_id}", response_model=CrawlJobStatus, dependencies=[Depends(require_auth)])
def get_job(job_id: str) -> CrawlJobStatus:
    job = db.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Unknown job_id")
    return CrawlJobStatus(
        job_id=job["job_id"],
        status=job["status"],
        source=job["source"],
        submitted_at=job["submitted_at"],
        started_at=job["started_at"],
        finished_at=job["finished_at"],
        url_count=len(job["urls"]),
        output_dir=job["output_dir"],
        log_path=job["log_path"],
        error=job["error"],
    )


@app.get("/jobs/{job_id}/results", dependencies=[Depends(require_auth)])
def get_results(job_id: str) -> dict[str, Any]:
    job = db.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Unknown job_id")
    result_path = Path(job["output_dir"]).parent / "result.json"
    if result_path.exists():
        result = json.loads(result_path.read_text())
    else:
        result = {
            "job_id": job_id,
            "status": job["status"],
            "output_dir": job["output_dir"],
            "runner_log": job["log_path"],
            "urls": job["urls"],
            "message": "Results are not available until the crawl runner writes result.json.",
        }
    return result


@app.get("/jobs", dependencies=[Depends(require_auth)])
def list_jobs(limit: int = 100) -> list[dict[str, Any]]:
    return db.list_jobs(limit=max(1, min(limit, 500)))


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("api.server:app", host=HOST, port=PORT, reload=False)
