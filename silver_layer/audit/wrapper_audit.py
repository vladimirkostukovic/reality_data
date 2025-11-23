import subprocess
import sys
import json
import time
from pathlib import Path

THIS_DIR = Path(__file__).resolve().parent

# ============================
AUDIT_STEPS = [
    "summarized_audit.py",
]
# ============================


def run_step(script_path: Path) -> dict:
    if not script_path.exists():
        return {
            "stage": script_path.name,
            "status": "fail",
            "error": "missing_file"
        }

    try:
        p = subprocess.run(
            [sys.executable, str(script_path)],
            capture_output=True,
            text=True,
            timeout=600
        )
    except Exception as e:
        return {
            "stage": script_path.name,
            "status": "fail",
            "error": f"subprocess_error: {e}"
        }

    if p.returncode != 0:
        return {
            "stage": script_path.name,
            "status": "fail",
            "stderr": p.stderr.strip()
        }

    try:
        return json.loads(p.stdout.strip())
    except json.JSONDecodeError:
        return {
            "stage": script_path.name,
            "status": "fail",
            "error": "invalid_json",
            "stdout": p.stdout[:400]
        }


def main():
    t0 = time.time()

    step_results = []
    failed = 0

    for script in AUDIT_STEPS:
        script_path = THIS_DIR / script
        res = run_step(script_path)
        step_results.append(res)
        if res.get("status") != "ok":
            failed += 1

    out = {
        "stage": "audit_wrapper",
        "status": "fail" if failed else "ok",
        "steps": step_results,
        "sync_summary": {
            "total_steps": len(step_results),
            "failed_steps": failed,
            "ok_steps": len(step_results) - failed,
        },
        "elapsed_s": round(time.time() - t0, 3)
    }

    print(json.dumps(out, ensure_ascii=False))


if __name__ == "__main__":
    main()