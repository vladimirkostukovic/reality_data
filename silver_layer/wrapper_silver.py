from __future__ import annotations
import os, sys, json, time, shlex, subprocess, logging, fcntl, signal
from pathlib import Path
from dataclasses import dataclass
from typing import Optional, Dict, Any, List, Tuple

# =============== LOGGING (stderr only) ===============
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | wrapper_silver | %(message)s",
    handlers=[logging.StreamHandler(sys.stderr)],
    force=True,
)
log = logging.getLogger("wrapper_silver")

SCRIPT_DIR = Path(__file__).resolve().parent
LOCK_PATH = SCRIPT_DIR / ".silver_wrapper.lock"

PRE_STEPS: List[str] = [
    "transfer_to_silver.py",
    "sum_to_geo.py",
]

POST_STEPS: List[str] = [
    "geo_garbage.py",
    "geo_garbage_openai.py",
    "geo_garbage_to_sumgeo.py",
    "geo_to_subset.py",
    "praha_match.py",
    "stredo_match.py",
    "duplicate_internal.py",
    "duplicate_clean.py",
    "seller.py",
    "price_change.py",
    "sanity_check.py",
]

# --- sanity key maps ---
BEFORE_KEYS  = ["before", "before_count", "rows_before", "rows_in", "input_count", "start_count"]
AFTER_KEYS   = ["after", "after_count", "rows_after", "rows_out", "output_count", "end_count"]

ADDED_KEYS   = [
    "loaded", "inserted", "added", "new_rows", "ingested", "upserted", "processed", "affected",
    "rows_inserted", "rows_added"
]
REMOVED_KEYS = ["deleted", "removed", "dropped", "duplicates_removed", "dedup_removed", "pruned"]
INTLIKE = (int, float)


@dataclass
class StepResult:
    step: str
    rc: int
    elapsed_s: float
    raw_json: Optional[Dict[str, Any]]
    stdout_preview: Optional[str]
    sanity_ok: bool
    sanity_msg: str
    ok_gate: bool
    concurrent: bool = False

    @property
    def status(self) -> str:
        if self.rc == 0 and self.ok_gate and self.sanity_ok:
            return "ok"
        return "fail"


def extract_last_json(stdout: str) -> Optional[Dict[str, Any]]:
    s = (stdout or "").strip()
    if not s:
        return None
    try:
        return json.loads(s)
    except Exception:
        pass
    for line in reversed(s.splitlines()):
        t = line.strip()
        if t.startswith("{") and t.endswith("}"):
            try:
                return json.loads(t)
            except Exception:
                continue
    last_open = s.rfind("{")
    if last_open != -1:
        cand = s[last_open:]
        try:
            return json.loads(cand)
        except Exception:
            pass
    return None


def _pick_first_number(d: Dict[str, Any], keys: List[str]) -> Optional[int]:
    for k in keys:
        if k in d and isinstance(d[k], INTLIKE):
            try:
                return int(d[k])
            except Exception:
                pass
    return None


def _sum_numeric(x: Any) -> int:
    if isinstance(x, (int, float)) and not isinstance(x, bool):
        return int(x)
    if isinstance(x, dict):
        return sum(_sum_numeric(v) for v in x.values())
    if isinstance(x, (list, tuple)):
        return sum(_sum_numeric(v) for v in x)
    return 0


def sanity_from_metrics(m: Dict[str, Any]) -> Tuple[bool, str]:
    before  = _pick_first_number(m, BEFORE_KEYS)
    after   = _pick_first_number(m, AFTER_KEYS)

    added = None
    added_present_keys = [k for k in ADDED_KEYS if k in m]
    if added_present_keys:
        s = sum(_sum_numeric(m[k]) for k in added_present_keys)
        added = s if s != 0 else None

    removed = None
    removed_present_keys = [k for k in REMOVED_KEYS if k in m]
    if removed_present_keys:
        s = sum(_sum_numeric(m[k]) for k in removed_present_keys)
        removed = s if s != 0 else None

    if before is not None and after is not None:
        delta = int(after) - int(before)
        if added is None and removed is None:
            if delta >= 0:
                added, removed = delta, 0
            else:
                added, removed = 0, -delta
        elif added is None:
            added = max(delta + (removed or 0), 0)
        elif removed is None:
            removed = max((added or 0) - delta, 0)

    for label, val in [("before", before), ("after", after), ("added", added), ("removed", removed)]:
        if val is not None and val < 0:
            return False, f"value_negative:{label}={val}"

    if before is None and after is None and added is None and removed is None:
        return True, "skipped_no_metrics"

    if before is not None and after is not None:
        exp = before + (added or 0) - (removed or 0)
        if after != exp:
            return False, f"mismatch_expected_after:{after}!={exp} (before={before}, added={added or 0}, removed={removed or 0})"
        return True, "ok_equal_after"

    return True, "skipped_partial_metrics"


def run_step(step_name: str) -> StepResult:
    t0 = time.perf_counter()
    step_path = (SCRIPT_DIR / step_name).resolve()
    cmd = f"python3 {shlex.quote(str(step_path))}" if step_path.suffix == ".py" else str(step_path)
    log.info("RUN: %s", cmd)

    proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    out, err = proc.communicate()
    elapsed = time.perf_counter() - t0

    if err:
        sys.stderr.write(err)
        sys.stderr.flush()

    payload = extract_last_json(out) if out else None
    sanity_ok, sanity_msg = True, "skipped_no_json"
    if payload is not None:
        sanity_ok, sanity_msg = sanity_from_metrics(payload)

    ok_field = None
    if isinstance(payload, dict) and "ok" in payload:
        try:
            ok_field = bool(payload["ok"])
        except Exception:
            ok_field = False
    ok_gate = ok_field if ok_field is not None else (proc.returncode == 0 and sanity_ok)

    return StepResult(
        step=step_name,
        rc=proc.returncode,
        elapsed_s=elapsed,
        raw_json=payload,
        stdout_preview=(out[-4000:] if (payload is None and out) else None),
        sanity_ok=sanity_ok,
        sanity_msg=sanity_msg,
        ok_gate=ok_gate,
    )


def acquire_lock() -> Optional[int]:
    try:
        fd = os.open(str(LOCK_PATH), os.O_CREAT | os.O_RDWR, 0o644)
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        os.write(fd, str(os.getpid()).encode("utf-8"))
        return fd
    except BlockingIOError:
        return None


def main():
    lock_fd = acquire_lock()
    if lock_fd is None:
        print(json.dumps({"state": "locked"}))
        return

    step_results: List[StepResult] = []
    first_fail: Optional[str] = None

    for step in PRE_STEPS:
        res = run_step(step)
        step_results.append(res)
        if res.status != "ok":
            first_fail = first_fail or step
            log.error("Step failed (pre): %s. Step failed.", step)
            out = {
                "state": "failed",
                "failed_at": step,
                "steps": [r.__dict__ for r in step_results],
            }
            print(json.dumps(out, ensure_ascii=False, indent=2))
            return

    for step in POST_STEPS:
        res = run_step(step)
        step_results.append(res)
        if res.status != "ok":
            first_fail = first_fail or step
            log.error("Step failed (post): %s. Step failed.", step)
            out = {
                "state": "failed",
                "failed_at": step,
                "steps": [r.__dict__ for r in step_results],
            }
            print(json.dumps(out, ensure_ascii=False, indent=2))
            return

    out = {
        "state": "ok",
        "failed_at": first_fail,
        "steps": [
            {
                "step": r.step,
                "status": r.status,
                "rc": r.rc,
                "elapsed_s": round(r.elapsed_s, 3),
                "sanity_msg": r.sanity_msg,
                "ok_gate": r.ok_gate,
                "concurrent": r.concurrent,
                "metrics": r.raw_json,
                "stdout_preview": r.stdout_preview,
            } for r in step_results
        ],
    }
    print(json.dumps(out, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.warning("Interrupted by user")
        sys.exit(130)
    except Exception as e:
        log.exception("wrapper_silver fatal error: %s", e)
        print(json.dumps({"state": "failed", "error": str(e)}))
        sys.exit(1)
