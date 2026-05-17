from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field, HttpUrl, field_validator


class CrawlOptions(BaseModel):
    mode: Literal["single_page", "site_crawl"] = "single_page"
    frontier_links: int = Field(default=3, ge=0, le=20)
    dfs_links: int = Field(default=2, ge=0, le=20)
    depth: int = Field(default=3, ge=0, le=10)
    sleep: int = Field(default=3, ge=0, le=120)
    timeout: int = Field(default=400, ge=30, le=3600)
    save_content: bool = True
    dump_html: bool = True
    dump_recursive_html: bool = False
    warmup_homepage: bool = True


class CrawlRequest(BaseModel):
    urls: list[HttpUrl] = Field(min_length=1)
    source: str = Field(default="unknown", max_length=100)
    metadata: dict[str, Any] = Field(default_factory=dict)
    options: CrawlOptions = Field(default_factory=CrawlOptions)

    @field_validator("urls")
    @classmethod
    def dedupe_urls(cls, urls: list[HttpUrl]) -> list[HttpUrl]:
        seen: set[str] = set()
        out: list[HttpUrl] = []
        for url in urls:
            key = str(url)
            if key not in seen:
                seen.add(key)
                out.append(url)
        return out


class CrawlJobCreated(BaseModel):
    job_id: str
    status: str
    status_url: str
    results_url: str


class CrawlJobStatus(BaseModel):
    job_id: str
    status: Literal["queued", "running", "succeeded", "failed"]
    source: str
    submitted_at: datetime | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    url_count: int
    output_dir: str
    log_path: str
    error: str | None = None


class HealthResponse(BaseModel):
    ok: bool
    service: str
    auth_required: bool
    queued_jobs: int
    running_jobs: int
