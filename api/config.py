from __future__ import annotations

import os
import sys
from pathlib import Path

OPENWPM_ROOT = Path(__file__).resolve().parents[1]
API_DIR = OPENWPM_ROOT / "api"
JOBS_DIR = Path(os.getenv("CRAWLER_API_JOBS_DIR", API_DIR / "jobs"))
DB_PATH = Path(os.getenv("CRAWLER_API_DB", API_DIR / "crawler_jobs.sqlite"))
PYTHON_BIN = os.getenv("CRAWLER_OPENWPM_PYTHON", sys.executable)
API_TOKEN = os.getenv("CRAWLER_API_TOKEN", "")
HOST = os.getenv("CRAWLER_API_HOST", "127.0.0.1")
PORT = int(os.getenv("CRAWLER_API_PORT", "8080"))
MAX_BATCH_URLS = int(os.getenv("CRAWLER_API_MAX_BATCH_URLS", "20"))
USE_XVFB = os.getenv("CRAWLER_API_USE_XVFB", "1") == "1"
DEFAULT_TIMEOUT = int(os.getenv("CRAWLER_API_CRAWL_TIMEOUT", "400"))
DEFAULT_SLEEP = int(os.getenv("CRAWLER_API_SLEEP", "3"))
DEFAULT_FRONTIER_LINKS = int(os.getenv("CRAWLER_API_FRONTIER_LINKS", "3"))
DEFAULT_DFS_LINKS = int(os.getenv("CRAWLER_API_DFS_LINKS", "2"))
DEFAULT_DEPTH = int(os.getenv("CRAWLER_API_DEPTH", "3"))

JOBS_DIR.mkdir(parents=True, exist_ok=True)
