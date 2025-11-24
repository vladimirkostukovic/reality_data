# Unified Silver Layer Orchestrator.
# This wrapper executes the full silver pipeline in modular stages:
# pre-ingestion transforms, seller/price updates, photo dedup, geo processing,
# duplicate clustering, and final integrity audit. Each step is run as an
# isolated subprocess that returns JSON-based metrics used for sanity checks
# and strict fail-fast gating.
# Modules are toggle-controlled and easily extendable: to add new logic,
# simply list the script inside the corresponding module config.

from __future__ import annotations
import sys, json, time, shlex, subprocess, logging
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | wrapper_silver | %(message)s",
    handlers=[logging.StreamHandler(sys.stderr)],
    force=True,
)
log = logging.getLogger("wrapper_silver")

ROOT = Path(__file__).resolve().parent

# ========== CONFIG TOGGLES ==========
MODULES = {
    "pre": {
        "enabled": True,
        "steps": [
            "transfer_to_silver.py",
            "sum_to_geo.py",
        ],
    },
    "seller_price": {
        "enabled": True,
        "steps": [
            "seller.py",
            "price_change.py",
        ],
    },
    "photo_modul": {
        "enabled": False,
        "steps": [
            "photo_modul/wrapper_photo_modul.py",
        ],
    },
    "geo": {
        "enabled": True,
        "steps": [
            "geo_modul/wrapper_geo.py",
        ],
    },
    "cluster": {
        "enabled": True,
        "steps": [
            "photo_cluster_builder/wrapper_cluster.py",
        ],
    },
    "audit": {
        "enabled": True,
        "steps": [
            "audit/wrapper_audit.py",
        ],
    },
}


# ========================================================

def extract_last_json(stdout: str) -> Optional[Dict[str, Any]]:
    if not stdout:
        return None

    buf = ""
    depth = 0
    capturing = False
    last_valid = None

    for ch in stdout:
        if ch == "{":
            if not capturing:
                capturing = True
                buf = ""
                depth = 0
            depth += 1

        if capturing:
            buf += ch

        if ch == "}":
            if capturing:
                depth -= 1
                if depth == 0:
                    try:
                        last_valid = json.loads(buf)
                    except Exception:
                        pass
                    capturing = False
                    buf = ""

    return last_valid


def extract_step_metrics(step_name: str, payload: Dict[str, Any]) -> Dict[str, Optional[int]]:
    """Extract metrics based on step type."""
    metrics = {
        "rows_input": None,
        "rows_output": None,
        "rows_inserted": None,
        "rows_updated": None,
    }

    # transfer_to_silver: {"stats": {"before": ..., "inserted": ..., "after": ...}}
    if step_name == "transfer_to_silver":
        stats = payload.get("stats", {})
        metrics["rows_input"] = stats.get("before")
        metrics["rows_output"] = stats.get("after")
        metrics["rows_inserted"] = stats.get("inserted")

    # sum_to_geo: {"rows_inserted": ..., "rows_updated": ..., "before": ..., "after": ...}
    elif step_name == "sum_to_geo":
        metrics["rows_input"] = payload.get("before")
        metrics["rows_output"] = payload.get("after")
        metrics["rows_inserted"] = payload.get("rows_inserted")
        metrics["rows_updated"] = payload.get("rows_updated")

    # seller: {"before": ..., "after": ..., "inserted_total": ...}
    elif step_name == "seller":
        metrics["rows_input"] = payload.get("before")
        metrics["rows_output"] = payload.get("after")
        metrics["rows_inserted"] = payload.get("inserted_total")

    # price_change: {"before": ..., "after": ..., "inserted": ...}
    elif step_name == "price_change":
        metrics["rows_input"] = payload.get("before")
        metrics["rows_output"] = payload.get("after")
        metrics["rows_inserted"] = payload.get("inserted")

    # wrapper_geo: проверяем sync_summary
    elif step_name == "wrapper_geo":
        sync = payload.get("sync_summary", {})
        metrics["rows_updated"] = sync.get("updated", 0)
        metrics["rows_inserted"] = sync.get("moved", 0)  # moved = новые строки
        # Попробуем найти before/after в steps
        steps = payload.get("steps", [])
        for step in steps:
            if step.get("file") == "sanity_check.py":
                step_sync = step.get("sync_summary", {})
                dedup_parent = step_sync.get("dedup_parent", {})
                if dedup_parent:
                    before = dedup_parent.get("before", {})
                    after = dedup_parent.get("after", {})
                    metrics["rows_input"] = before.get("total")
                    metrics["rows_output"] = after.get("total")
                break

    # wrapper_cluster: проверяем sync_summary
    elif step_name == "wrapper_cluster":
        steps = payload.get("steps", [])
        if steps and len(steps) > 0:
            first_step = steps[0]
            step_sync = first_step.get("sync_summary", {})
            metrics["rows_input"] = step_sync.get("pairs_total")
            metrics["rows_output"] = step_sync.get("pairs_kept_geo_valid")
            metrics["rows_inserted"] = step_sync.get("canon_rows")

    # wrapper_audit: проверяем steps
    elif step_name == "wrapper_audit":
        steps = payload.get("steps", [])
        if steps and len(steps) > 0:
            first_step = steps[0]
            sync = first_step.get("sync_summary", {})
            metrics["rows_input"] = sync.get("src_total_keys")
            metrics["rows_output"] = sync.get("silver_total_keys")

    return metrics


def run_step(step: str) -> Dict[str, Any]:
    t0 = time.perf_counter()
    path = (ROOT / step).resolve()
    cmd = f"python3 {shlex.quote(str(path))}"

    log.info("RUN: %s", cmd)
    proc = subprocess.Popen(cmd, shell=True,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            text=True)
    out, err = proc.communicate()
    elapsed = round(time.perf_counter() - t0, 3)

    if err:
        sys.stderr.write(err)

    payload = extract_last_json(out)

    return {
        "step": step,
        "rc": proc.returncode,
        "elapsed_s": elapsed,
        "json": payload,
        "stdout": None if payload is not None else out[-2000:],
        "status": "ok" if proc.returncode == 0 else "fail"
    }


def main():
    final = {
        "status": "completed",
        "steps": [],
        "modules": {},
        "failed_at": None,
        "started_at": time.time(),
        "finished_at": None,
        "timestamp": datetime.now().isoformat()
    }

    # Aggregated metrics
    aggregated = {
        "rows_input": 0,
        "rows_output": 0,
        "rows_inserted": 0,
        "rows_updated": 0,
    }

    for module_name, mod in MODULES.items():
        if not mod["enabled"]:
            log.info("SKIP MODULE: %s", module_name)
            continue

        log.info(">>> MODULE: %s", module_name)
        mod_results = []
        final["modules"][module_name] = mod_results

        for step in mod["steps"]:
            res = run_step(step)
            mod_results.append(res)

            # Extract metrics from JSON
            step_name = Path(step).stem
            step_metrics = {
                "step": step_name,
                "status": res["status"],
                "elapsed_s": res["elapsed_s"],
                "rows_input": None,
                "rows_output": None,
                "rows_inserted": None,
                "rows_updated": None,
            }

            if res.get("json"):
                metrics = extract_step_metrics(step_name, res["json"])
                step_metrics.update(metrics)

                # Aggregate metrics
                if metrics["rows_input"]:
                    aggregated["rows_input"] = max(aggregated["rows_input"], metrics["rows_input"])
                if metrics["rows_output"]:
                    aggregated["rows_output"] = metrics["rows_output"]
                if metrics["rows_inserted"]:
                    aggregated["rows_inserted"] += metrics["rows_inserted"]
                if metrics["rows_updated"]:
                    aggregated["rows_updated"] += metrics["rows_updated"]

            final["steps"].append(step_metrics)

            if res["status"] != "ok":
                final["status"] = "error"
                final["failed_at"] = step
                final["finished_at"] = time.time()
                final["aggregated"] = aggregated
                print(json.dumps(final, ensure_ascii=False))
                return

    final["finished_at"] = time.time()
    final["aggregated"] = aggregated
    print(json.dumps(final, ensure_ascii=False))


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.warning("user interrupted")
        print(json.dumps({
            "status": "error",
            "error": "user_interrupted",
            "timestamp": datetime.now().isoformat()
        }, ensure_ascii=False))
        sys.exit(130)
    except Exception as e:
        log.exception("FATAL: %s", e)
        print(json.dumps({
            "status": "error",
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }, ensure_ascii=False))
        sys.exit(1)