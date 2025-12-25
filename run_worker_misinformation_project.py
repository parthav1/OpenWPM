import sys
import time
from pathlib import Path
from datetime import date

from openwpm.commands.browser_commands import CrawlCommand
from openwpm.command_sequence import CommandSequence
from openwpm.config import BrowserParams, ManagerParams
from openwpm.storage.sql_provider import SQLiteStorageProvider
from openwpm.storage.leveldb import LevelDbProvider
from openwpm.task_manager import TaskManager
from openwpm.stealth.commands import SetResolution, SetPosition

# INPUT
sites_file = Path(sys.argv[1])
list_name = sys.argv[2]
shard_idx = sys.argv[3]
worker_id = sys.argv[4]

today = date.today().isoformat()

with open(sites_file, "r") as f:
    sites = [s.strip() for s in f.read().split(",") if s.strip()]

# OPENWPM SETUP
NUM_BROWSERS = 1
TIMEOUT_DURATION = 12 * 60 * 60
initial_time = time.time()

manager_params = ManagerParams(num_browsers=NUM_BROWSERS)
browser_params = [BrowserParams(display_mode="native")]

for bp in browser_params:
    bp.http_instrument = True
    bp.cookie_instrument = True
    bp.navigation_instrument = True
    bp.stealth_js_instrument = True
    bp.save_content = "script"
    bp.save_all_content = True
    bp.save_javascript = True
    bp.tp_cookies = "always"
    bp.bot_mitigation = True
    bp.headless = False

# STORAGE
datadir = Path(f"datadir/{today}_{list_name}_shard{shard_idx}")
datadir.mkdir(parents=True, exist_ok=True)

manager_params.data_directory = datadir
manager_params.log_path = datadir / "openwpm.log"

sqlite = SQLiteStorageProvider(
    datadir / f"{today}_{list_name}_shard{shard_idx}.sqlite"
)
leveldb = LevelDbProvider(
    datadir / f"{today}_{list_name}_shard{shard_idx}.ldb"
)

manager = TaskManager(manager_params, browser_params, sqlite, leveldb)

# RUN CRAWL
for idx, site in enumerate(sites):
    if time.time() - initial_time > TIMEOUT_DURATION:
        break

    print(f"[{list_name} shard {shard_idx}] visiting {site}")

    cs = CommandSequence(site, site_rank=idx)
    cs.append_command(SetResolution(1600, 800), timeout=10)
    cs.append_command(SetPosition(50, 200), timeout=10)
    cs.append_command(
        CrawlCommand(site, frontier_links=3, dfs_links=2, sleep=3, depth=3),
        timeout=400
    )

    manager.execute_command_sequence(cs)

manager.close()
print(f"[{list_name} shard {shard_idx}] finished")
