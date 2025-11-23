from __future__ import annotations
import json
import subprocess
from pathlib import Path
import sys
import time

# ========================= CONFIG =========================
MODULE_DIR = Path(__file__).resolve().parent

STEPS = [
    "duplicate_internal.py",   # first: build canonical clusters
    "duplicate_clean.py",      # second: geo/price/area cleanup inside each cluster
]


def run_step(path: Path):
    """Execute one step and return normalized json result."""
    t0 = time.time()

    try:
        proc = subprocess.run(
            [sys.executable, str(path)],
            capture_output=True,
            text=True,
            timeout=1200
        )
    except Exception as e:
        return {
            "file": path.name,
            "stage": path.stem,
            "status": "fail",
            "error": f"wrapper_exec_error: {e}",
            "elapsed_s": round(time.time() - t0, 3),
        }

    stdout = proc.stdout.strip()
    if not stdout:
        return {
            "file": path.name,
            "stage": path.stem,
            "status": "fail",
            "error": "empty_output",
            "elapsed_s": round(time.time() - t0, 3),
        }

    try:
        obj = json.loads(stdout)
    except json.JSONDecodeError:
        return {
            "file": path.name,
            "stage": path.stem,
            "status": "fail",
            "error": "invalid_json",
            "output": stdout[:500],
            "elapsed_s": round(time.time() - t0, 3),
        }

    # must return proper stage
    stage = obj.get("stage") or path.stem

    return {
        "file": path.name,
        "stage": stage,
        "status": obj.get("status", "ok"),
        "sync_summary": obj.get("sync_summary") or obj,
        "_raw": stdout,
        "elapsed_s": round(time.time() - t0, 3),
    }


# ========================= MAIN =========================

def main():
    t0 = time.time()

    results = []
    failed = 0

    for step in STEPS:
        path = MODULE_DIR / step
        if not path.exists():
            results.append({
                "file": step,
                "stage": step.replace(".py", ""),
                "status": "fail",
                "error": "missing_file",
            })
            failed += 1
            continue

        res = run_step(path)
        results.append(res)

        if res.get("status") != "ok":
            failed += 1

    out = {
        "stage": "photo_duplicate_wrapper",
        "status": "ok" if failed == 0 else "fail",
        "sync_summary": {
            "steps_total": len(STEPS),
            "failed_steps": failed,
            "elapsed_s": round(time.time() - t0, 3),
        },
        "steps": results,
    }

    print(json.dumps(out, ensure_ascii=False))


if __name__ == "__main__":
    main()