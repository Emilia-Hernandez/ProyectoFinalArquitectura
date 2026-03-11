from __future__ import annotations

import json
import platform
from pathlib import Path

import psutil

OUTPUT = Path("output/logs/hardware_profile.json")
OUTPUT.parent.mkdir(parents=True, exist_ok=True)

payload = {
    "platform": platform.platform(),
    "machine": platform.machine(),
    "processor": platform.processor(),
    "python_version": platform.python_version(),
    "physical_cores": psutil.cpu_count(logical=False),
    "logical_cores": psutil.cpu_count(logical=True),
    "total_ram_gb": round(psutil.virtual_memory().total / (1024**3), 2),
}

OUTPUT.write_text(json.dumps(payload, indent=2), encoding="utf-8")
print(f"hardware profile saved to {OUTPUT}")
print(json.dumps(payload, indent=2))
