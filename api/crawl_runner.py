from __future__ import annotations

import argparse
import json
import traceback
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from openwpm.commands.browser_commands import CrawlCommand, GetCommand
from openwpm.command_sequence import CommandSequence
from openwpm.config import BrowserParams, ManagerParams
from openwpm.storage.leveldb import LevelDbProvider
from openwpm.storage.sql_provider import SQLiteStorageProvider
from openwpm.task_manager import TaskManager
from openwpm.stealth.commands import SetPosition, SetResolution

from api import db
from api.config import JOBS_DIR


def normalize_url(url: str) -> str:
    if "://" not in url:
        return "http://" + url
    return url


def homepage_for_url(url: str) -> str:
    parsed = urlparse(url)
    if not parsed.scheme or not parsed.netloc:
        return url
    return f"{parsed.scheme}://{parsed.netloc}/"


def configure_browser(save_content: bool) -> BrowserParams:
    bp = BrowserParams(display_mode="native")
    bp.http_instrument = True
    bp.donottrack = False
    bp.cookie_instrument = True
    bp.navigation_instrument = True
    bp.stealth_js_instrument = True
    bp.disable_flash = False
    bp.tp_cookies = "always"
    bp.bot_mitigation = True
    bp.headless = False
    if save_content:
        bp.save_content = "script"
        bp.save_all_content = True
        bp.save_javascript = True
    return bp


def build_result(job_id: str, job_dir: Path, datadir: Path, sqlite_path: Path, leveldb_path: Path) -> dict[str, Any]:
    job = db.get_job(job_id)
    source_dir = datadir / "sources"
    html_files = sorted(str(path) for path in source_dir.glob("*.html")) if source_dir.exists() else []
    return {
        "job_id": job_id,
        "status": job["status"] if job else "unknown",
        "output_dir": str(job_dir),
        "datadir": str(datadir),
        "sqlite_path": str(sqlite_path),
        "leveldb_path": str(leveldb_path),
        "openwpm_log": str(datadir / "openwpm.log"),
        "runner_log": str(job_dir / "runner.log"),
        "source_dump_dir": str(source_dir),
        "html_files": html_files,
        "urls": job.get("urls", []) if job else [],
        "article_text_status": "not_extracted",
        "note": "html_files contain rendered top-level page source and can be passed to Trafilatura.",
    }


def run_job(job_id: str) -> None:
    db.init_db()
    job_dir = JOBS_DIR / job_id
    request_path = job_dir / "request.json"
    result_path = job_dir / "result.json"
    if not request_path.exists():
        raise FileNotFoundError(f"Missing request file: {request_path}")

    request = json.loads(request_path.read_text())
    urls = [normalize_url(str(url)) for url in request["urls"]]
    options = request.get("options", {})
    save_content = bool(options.get("save_content", True))

    datadir = job_dir / "crawl_output"
    datadir.mkdir(parents=True, exist_ok=True)
    sqlite_path = datadir / f"{job_id}.sqlite"
    leveldb_path = datadir / f"{job_id}.ldb"

    db.update_job(job_id, "running")
    manager = None
    failures = 0
    final_status = "failed"
    final_error = None

    try:
        manager_params = ManagerParams(num_browsers=1)
        manager_params.data_directory = datadir
        manager_params.log_path = datadir / "openwpm.log"

        browser_params = [configure_browser(save_content)]
        sqlite = SQLiteStorageProvider(sqlite_path)
        leveldb = LevelDbProvider(leveldb_path)
        manager = TaskManager(manager_params, browser_params, sqlite, leveldb)

        for idx, url in enumerate(urls):
            db.update_url(job_id, idx, "running")
            try:
                print(f"[{job_id}] visiting {url}", flush=True)
                source_dir = datadir / "sources"
                html_before = set(source_dir.glob("*.html")) if source_dir.exists() else set()

                cs = CommandSequence(url, site_rank=idx, blocking=True)
                cs.append_command(SetResolution(1280, 800), timeout=10)
                cs.append_command(SetPosition(50, 200), timeout=10)

                mode = options.get("mode", "single_page")
                if mode == "single_page" and bool(options.get("warmup_homepage", True)):
                    warmup_url = homepage_for_url(url)
                    if warmup_url != url:
                        cs.append_command(GetCommand(warmup_url, int(options.get("sleep", 3))), timeout=int(options.get("timeout", 400)))

                if mode == "site_crawl":
                    cs.append_command(
                        CrawlCommand(
                            url,
                            frontier_links=int(options.get("frontier_links", 3)),
                            dfs_links=int(options.get("dfs_links", 2)),
                            sleep=int(options.get("sleep", 3)),
                            depth=int(options.get("depth", 3)),
                        ),
                        timeout=int(options.get("timeout", 400)),
                    )
                else:
                    cs.get(
                        sleep=int(options.get("sleep", 3)),
                        timeout=int(options.get("timeout", 400)),
                    )

                if bool(options.get("dump_html", True)):
                    cs.dump_page_source(suffix=f"url{idx}", timeout=30)
                manager.execute_command_sequence(cs)

                html_after = set(source_dir.glob("*.html")) if source_dir.exists() else set()
                new_html_files = [path for path in sorted(html_after - html_before) if path.stat().st_size > 100]

                if bool(options.get("dump_html", True)) and not new_html_files:
                    raise RuntimeError("crawl finished but no rendered HTML dump was written")
                db.update_url(job_id, idx, "succeeded")
            except Exception as exc:
                failures += 1
                db.update_url(job_id, idx, "failed", error=f"{type(exc).__name__}: {exc}")
                traceback.print_exc()

        final_status = "failed" if failures == len(urls) else "succeeded"
    except Exception as exc:
        final_status = "failed"
        final_error = f"{type(exc).__name__}: {exc}"
        traceback.print_exc()
    finally:
        if manager is not None:
            try:
                manager.close()
            except Exception:
                traceback.print_exc()
        db.update_job(job_id, final_status, error=final_error)
        result = build_result(job_id, job_dir, datadir, sqlite_path, leveldb_path)
        result_path.write_text(json.dumps(result, indent=2, sort_keys=True))


def main() -> None:
    parser = argparse.ArgumentParser(description="Run one crawler API job.")
    parser.add_argument("--job-id", required=True)
    args = parser.parse_args()
    run_job(args.job_id)


if __name__ == "__main__":
    main()
