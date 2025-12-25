import sys
import time
import random
import math
import subprocess
from pathlib import Path
from datetime import date

TOTAL_WORKERS = 84
HARD_TIMEOUT_S = 12 * 60 * 60
PYTHON_EXEC = sys.executable  # current env python
WORKER_SCRIPT = "run_worker_misinformation_project.py" # worker script

today = date.today().isoformat()
Path("chunks").mkdir(exist_ok=True)

# HELPERS
def load_sites(path):
    with open(path, "r") as f:
        sites = [s.strip() for s in f.read().split(",") if s.strip()]
    random.shuffle(sites)
    return sites

# PREPARE SHARDS PER LIST
workers = []
worker_id = 0

for list_path in sys.argv[1:]:
    list_path = Path(list_path)
    list_name = list_path.stem  # e.g. iffys, newsguard_trustworthy

    sites = load_sites(list_path)
    num_sites = len(sites)

    # allocate workers proportional to list size
    list_workers = max(1, round(TOTAL_WORKERS * (num_sites / sum(
        len(load_sites(Path(p))) for p in sys.argv[1:]
    ))))

    chunk_size = math.ceil(num_sites / list_workers)
    chunks = [
        sites[i:i + chunk_size]
        for i in range(0, num_sites, chunk_size)
    ]

    for shard_idx, chunk in enumerate(chunks):
        chunk_file = Path(f"chunks/{today}_{list_name}_shard{shard_idx}.txt")
        chunk_file.write_text(",".join(chunk))

        workers.append({
            "worker_id": worker_id,
            "list_name": list_name,
            "shard_idx": shard_idx,
            "chunk_file": chunk_file
        })

        worker_id += 1

print(f"Prepared {len(workers)} shards total")

# LAUNCH WORKERS
procs = []
start = time.time()

for w in workers:
    cmd = [
        PYTHON_EXEC,
        WORKER_SCRIPT,
        str(w["chunk_file"]),
        w["list_name"],
        str(w["shard_idx"]),
        str(w["worker_id"])
    ]

    print(f"Starting {w['list_name']} shard {w['shard_idx']} (worker {w['worker_id']})")
    procs.append(subprocess.Popen(cmd))
    time.sleep(4)  # stagger launches

# WAIT / TIMEOUT
while True:
    alive = [p for p in procs if p.poll() is None]
    if not alive:
        break

    if time.time() - start > HARD_TIMEOUT_S:
        print("Hard timeout reached. Terminating remaining workers.")
        for p in alive:
            p.terminate()
        break

    time.sleep(30)

print("All workers completed.")