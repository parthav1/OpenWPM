from __future__ import annotations

import json
import os
import queue
import subprocess
import threading
import uuid
from pathlib import Path
from typing import Any

from fastapi import Depends, FastAPI, HTTPException, Response, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from api import db
from api.config import (
    API_TOKEN,
    HOST,
    JOBS_DIR,
    MAX_BATCH_URLS,
    MAX_QUEUE_SIZE,
    OPENWPM_ROOT,
    PORT,
    PYTHON_BIN,
    USE_XVFB,
    WORKER_COUNT,
)
from api.schemas import CrawlJobCreated, CrawlJobStatus, CrawlRequest, HealthResponse

app = FastAPI(
    title="OpenWPM Crawler API",
    description="Internal API for submitting URL crawl jobs to the local OpenWPM crawler.",
    version="0.1.0",
)
security = HTTPBearer(auto_error=False)
job_queue: queue.Queue[str] = queue.Queue(maxsize=MAX_QUEUE_SIZE)
worker_threads_started = 0
worker_lock = threading.Lock()


def require_auth(credentials: HTTPAuthorizationCredentials | None = Depends(security)) -> None:
    if not API_TOKEN:
        return
    if credentials is None or credentials.scheme.lower() != "bearer" or credentials.credentials != API_TOKEN:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid or missing API token")


def make_job_id() -> str:
    return uuid.uuid4().hex[:16]


def load_job_result(job_id: str) -> dict[str, Any]:
    job = db.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Unknown job_id")
    result_path = Path(job["output_dir"]).parent / "result.json"
    if result_path.exists():
        return json.loads(result_path.read_text())
    return {
        "job_id": job_id,
        "status": job["status"],
        "output_dir": job["output_dir"],
        "runner_log": job["log_path"],
        "urls": job["urls"],
        "html_files": [],
        "message": "Results are not available until the crawl runner writes result.json.",
    }


def html_file_for_job(job_id: str, index: int) -> Path:
    result = load_job_result(job_id)
    html_files = result.get("html_files") or []
    if index < 0 or index >= len(html_files):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No rendered HTML file at index {index}; found {len(html_files)} file(s)",
        )
    html_path = Path(html_files[index]).resolve()
    job_root = (JOBS_DIR / job_id).resolve()
    try:
        html_path.relative_to(job_root)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Resolved file is outside job directory") from exc
    if not html_path.exists() or not html_path.is_file():
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Rendered HTML file is missing")
    return html_path


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


def worker_loop(worker_index: int) -> None:
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
            db.update_job(job_id, "failed", error=f"worker {worker_index}: {type(exc).__name__}: {exc}")
        finally:
            job_queue.task_done()


def ensure_worker_started() -> None:
    global worker_threads_started
    with worker_lock:
        if worker_threads_started >= WORKER_COUNT:
            return
        for worker_index in range(worker_threads_started, WORKER_COUNT):
            thread = threading.Thread(
                target=worker_loop,
                args=(worker_index,),
                name=f"crawler-api-worker-{worker_index}",
                daemon=True,
            )
            thread.start()
        worker_threads_started = WORKER_COUNT


@app.on_event("startup")
def startup() -> None:
    db.init_db()
    db.mark_interrupted_running_jobs()
    ensure_worker_started()
    for job in reversed(db.list_jobs(status="queued", limit=MAX_QUEUE_SIZE)):
        try:
            job_queue.put_nowait(job["job_id"])
        except queue.Full:
            break


@app.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    return HealthResponse(
        ok=True,
        service="openwpm-crawler-api",
        auth_required=bool(API_TOKEN),
        queued_jobs=db.count_jobs("queued"),
        running_jobs=db.count_jobs("running"),
        worker_count=WORKER_COUNT,
        in_memory_queue_size=job_queue.qsize(),
        queue_capacity=MAX_QUEUE_SIZE,
    )


@app.post("/crawl", response_model=CrawlJobCreated, dependencies=[Depends(require_auth)])
def create_crawl(request: CrawlRequest) -> CrawlJobCreated:
    if len(request.urls) > MAX_BATCH_URLS:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Batch too large: max {MAX_BATCH_URLS} URLs per job",
        )

    if job_queue.full():
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Crawler queue is full; retry later or submit smaller batches.",
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
    try:
        job_queue.put_nowait(job_id)
    except queue.Full:
        db.update_job(job_id, "failed", error="Crawler queue is full")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Crawler queue is full; retry later or submit smaller batches.",
        )

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
    result = load_job_result(job_id)
    html_files = result.get("html_files") or []
    result["html_downloads"] = [f"/jobs/{job_id}/html/{idx}" for idx, _ in enumerate(html_files)]
    return result


@app.get("/jobs/{job_id}/html", dependencies=[Depends(require_auth)])
def get_first_html(job_id: str) -> Response:
    return get_html(job_id, 0)


@app.get("/jobs/{job_id}/html/{index}", dependencies=[Depends(require_auth)])
def get_html(job_id: str, index: int) -> Response:
    html_path = html_file_for_job(job_id, index)
    return Response(
        content=html_path.read_bytes(),
        media_type="text/html; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{job_id}-url{index}.html"'},
    )


@app.get("/jobs", dependencies=[Depends(require_auth)])
def list_jobs(limit: int = 100) -> list[dict[str, Any]]:
    return db.list_jobs(limit=max(1, min(limit, 500)))


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("api.server:app", host=HOST, port=PORT, reload=False)
