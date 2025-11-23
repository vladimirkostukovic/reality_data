#Unified Silver Layer Orchestrator.
#This wrapper executes the full silver pipeline in modular stages:
#pre-ingestion transforms, seller/price updates, photo dedup, geo processing,
#duplicate clustering, and final integrity audit. Each step is run as an
#isolated subprocess that returns JSON-based metrics used for sanity checks
#and strict fail-fast gating.
#Modules are toggle-controlled and easily extendable: to add new logic,
#simply list the script inside the corresponding module config.


from __future__ import annotations
import sys, json, time, shlex, subprocess, logging
from pathlib import Path
from typing import Dict, Any, List, Optional

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
            "wrapper_photo_modul.py",
        ],
    },
    "geo": {
        "enabled": True,
        "steps": [
            "wrapper_geo.py",
        ],
    },
    "cluster": {
        "enabled": True,
        "steps": [
            "wrapper_cluster.py",
        ],
    },
    "audit": {
        "enabled": True,
        "steps": [
            "wrapper_audit.py",
        ],
    },
}

# ========================================================

def extract_last_json(stdout: str) -> Optional[Dict[str, Any]]:
    """Robustly extracts last valid JSON block from stdout."""
    if not stdout:
        return None

    # try whole stdout first
    s = stdout.strip()
    try:
        return json.loads(s)
    except Exception:
        pass

    # try line-by-line backwards
    for line in reversed(s.splitlines()):
        t = line.strip()
        if t.startswith("{") and t.endswith("}"):
            try:
                return json.loads(t)
            except Exception:
                continue

    # final fallback — take last "{" and try from there
    last_open = s.rfind("{")
    if last_open != -1:
        cand = s[last_open:]
        try:
            return json.loads(cand)
        except Exception:
            pass

    return None


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
        "state": "ok",
        "modules": {},
        "failed_at": None,
        "started_at": time.time(),
        "finished_at": None,
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

            if res["status"] != "ok":
                final["state"] = "failed"
                final["failed_at"] = step
                final["finished_at"] = time.time()
                print(json.dumps(final, ensure_ascii=False, indent=2))
                return

    final["finished_at"] = time.time()
    print(json.dumps(final, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.warning("user interrupted")
        sys.exit(130)
    except Exception as e:
        log.exception("FATAL: %s", e)
        print(json.dumps({"state": "failed", "error": str(e)}))
        sys.exit(1)