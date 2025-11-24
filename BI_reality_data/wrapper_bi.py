from __future__ import annotations
import subprocess
import sys
import json
import time
import shlex
import logging
from pathlib import Path
from typing import Dict, Any, List
from datetime import datetime

# ---------- LOGGING ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | wrapper_bi | %(message)s",
    handlers=[logging.StreamHandler(sys.stderr)],
    force=True,
)
log = logging.getLogger("wrapper_bi")

# ---------- CONFIG ----------
SCRIPT_DIR = Path(__file__).resolve().parent

# Scripts to run in order
SCRIPTS = ["gold_to_bi.py", "flat_table.py"]

# Per-step timeout (seconds)
STEP_TIMEOUT = 3600  # 1 hour


# ---------- UTILS ----------
def run_script(py_file: str) -> Dict[str, Any]:
    """Run a Python script and parse the last JSON line from stdout."""
    step_name = Path(py_file).stem
    script_path = SCRIPT_DIR / py_file

    if not script_path.exists():
        log.error(f"{py_file}: file not found at {script_path}")
        return {
            "step": step_name,
            "status": "error",
            "error": "file_not_found",
            "elapsed_s": 0.0
        }

    cmd = f"{shlex.quote(sys.executable)} {shlex.quote(str(script_path))}"
    log.info(f"Starting {py_file}...")
    t0 = time.perf_counter()

    proc = subprocess.Popen(
        shlex.split(cmd),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        cwd=str(SCRIPT_DIR),
    )

    try:
        out, err = proc.communicate(timeout=STEP_TIMEOUT)
    except subprocess.TimeoutExpired:
        log.error(f"{py_file}: timeout after {STEP_TIMEOUT}s")
        proc.kill()
        try:
            out, err = proc.communicate(timeout=5)
        except Exception:
            out, err = "", ""
        elapsed = time.perf_counter() - t0
        return {
            "step": step_name,
            "status": "error",
            "error": "timeout",
            "elapsed_s": round(elapsed, 3)
        }

    elapsed = time.perf_counter() - t0
    rc = proc.returncode

    # Log stderr
    if err:
        for line in err.splitlines():
            if line.strip():
                log.info(f"[{py_file}] {line}")

    # Parse last JSON line from stdout
    parsed = None
    if out:
        for line in reversed(out.splitlines()):
            line = line.strip()
            if not line:
                continue
            try:
                parsed = json.loads(line)
                break
            except Exception:
                continue

    if parsed is None:
        log.error(f"{py_file}: no valid JSON in stdout")
        return {
            "step": step_name,
            "status": "error",
            "error": "no_json_output",
            "rc": rc,
            "elapsed_s": round(elapsed, 3)
        }

    # Add metadata
    parsed["step"] = step_name
    parsed["elapsed_s"] = round(elapsed, 3)
    parsed["rc"] = rc

    log.info(f"Finished {py_file}: status={parsed.get('status')} rc={rc} elapsed={elapsed:.2f}s")
    return parsed


def aggregate_results(results: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Aggregate numeric fields from all steps."""
    agg = {}
    skip_keys = {"step", "status", "error", "elapsed_s", "rc", "timestamp", "stats"}

    for result in results:
        # Aggregate top-level numeric fields
        for key, value in result.items():
            if key in skip_keys:
                continue
            if isinstance(value, (int, float)):
                agg[key] = agg.get(key, 0) + value

        # Also aggregate from nested 'stats' dict
        if "stats" in result and isinstance(result["stats"], dict):
            for key, value in result["stats"].items():
                if isinstance(value, (int, float)):
                    agg[key] = agg.get(key, 0) + value

    return agg


def main():
    start_time = time.perf_counter()
    results = []

    # Run all scripts regardless of failures
    for script in SCRIPTS:
        result = run_script(script)
        results.append(result)

    total_elapsed = time.perf_counter() - start_time
    aggregated = aggregate_results(results)

    summary = {
        "status": "completed",
        "total_elapsed_s": round(total_elapsed, 3),
        "steps_run": len(results),
        "timestamp": datetime.now().isoformat(),
        "aggregated": aggregated,
        "details": results
    }

    # Output final JSON
    print(json.dumps(summary, ensure_ascii=False))


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log.exception(f"Wrapper fatal error: {e}")
        print(json.dumps({
            "status": "error",
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }, ensure_ascii=False))