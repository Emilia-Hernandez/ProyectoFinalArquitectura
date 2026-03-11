from __future__ import annotations

import csv
import datetime as dt
import subprocess
import time
from pathlib import Path

OUTPUT = Path("output/logs/benchmark_runs.csv")
OUTPUT.parent.mkdir(parents=True, exist_ok=True)

COMMANDS = [
    ["python", "-m", "scripts.hardware_profile"],
    ["python", "-m", "app.pipeline.train_model"],
]

rows = []
for command in COMMANDS:
    started = time.perf_counter()
    proc = subprocess.run(command, capture_output=True, text=True)
    elapsed = time.perf_counter() - started
    rows.append(
        {
            "timestamp_utc": dt.datetime.utcnow().isoformat(),
            "command": " ".join(command),
            "elapsed_seconds": round(elapsed, 4),
            "return_code": proc.returncode,
        }
    )
    print(f"command={' '.join(command)} rc={proc.returncode} elapsed={elapsed:.2f}s")

write_header = not OUTPUT.exists()
with OUTPUT.open("a", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
    if write_header:
        writer.writeheader()
    writer.writerows(rows)

print(f"benchmark log appended to {OUTPUT}")
