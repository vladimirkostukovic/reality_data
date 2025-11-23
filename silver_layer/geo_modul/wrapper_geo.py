from __future__ import annotations
import json
import subprocess
from pathlib import Path
import sys
import time

# -------- CONFIG --------
MODULE_DIR = Path(__file__).resolve().parent

STEPS = [
    "geo_garbage.py",
    "geo_garbage_openai.py",
    "geo_garbage_to_sumgeo.py",
    "geo_to_subset.py",
    "praha_match.py",
    "stredo_match.py",
    "sanity_check.py",
]

# -------- HELPERS --------

def run_step(path: Path):
    """Run one geo step, return dict with explicit file + stage."""
    t0 = time.time()

    try:
        proc = subprocess.run(
            [sys.executable, str(path)],
            capture_output=True,
            text=True,
            timeout=600
        )
    except Exception as e:
        return {
            "file": path.name,
            "stage": "wrapper_exec",
            "status": "fail",
            "error": f"wrapper_exec_error: {e}",
            "elapsed_s": round(time.time() - t0, 3),
        }

    stdout = proc.stdout.strip()
    if not stdout:
        return {
            "file": path.name,
            "stage": "unknown",
            "status": "fail",
            "error": "empty_output",
            "elapsed_s": round(time.time() - t0, 3),
        }

    try:
        obj = json.loads(stdout)
    except json.JSONDecodeError:
        return {
            "file": path.name,
            "stage": "unknown",
            "status": "fail",
            "error": "invalid_json",
            "output": stdout[:500],
            "elapsed_s": round(time.time() - t0, 3),
        }

    stage = obj.get("stage") or path.stem

    return {
        "file": path.name,
        "stage": stage,
        "status": obj.get("status", "ok"),
        "sync_summary": obj.get("sync_summary") or obj.get("summary"),
        "_raw": stdout,
        "elapsed_s": round(time.time() - t0, 3),
    }


# -------- MAIN --------

def main():
    t0 = time.time()

    results = []
    sanity = {
        "updated": 0,
        "moved": 0,
        "good": 0,
        "bad": 0,
        "failed_steps": 0,
    }

    for step in STEPS:
        path = MODULE_DIR / step
        if not path.exists():
            results.append({
                "file": step,
                "stage": step.replace(".py", ""),
                "status": "fail",
                "error": "missing_file"
            })
            sanity["failed_steps"] += 1
            continue

        res = run_step(path)
        results.append(res)

        if res.get("status") != "ok":
            sanity["failed_steps"] += 1

        ss = res.get("sync_summary") or {}

        sanity["updated"] += int(
            ss.get("rows_updated")
            or ss.get("added", 0)
            or ss.get("updated", 0)
            or 0
        )
        sanity["moved"] += int(ss.get("moved", 0))
        sanity["good"] += int(ss.get("good", 0))
        sanity["bad"] += int(ss.get("bad", 0))

    out = {
        "stage": "geo_modul_wrapper",
        "status": "ok" if sanity["failed_steps"] == 0 else "fail",
        "sync_summary": {
            "steps": len(STEPS),
            "failed_steps": sanity["failed_steps"],
            "updated": sanity["updated"],
            "moved": sanity["moved"],
            "good": sanity["good"],
            "bad": sanity["bad"],
            "elapsed_s": round(time.time() - t0, 3),
        },
        "steps": results,
    }

    print(json.dumps(out, ensure_ascii=False))


if __name__ == "__main__":
    main()