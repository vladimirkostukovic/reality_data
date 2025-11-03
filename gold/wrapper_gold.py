
from __future__ import annotations
import os, sys, json, time, shlex, subprocess, logging, socket, uuid
from pathlib import Path
from typing import Dict, Any, List
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# =============== LOGGING (stderr only) ===============
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | wrapper_gold | %(message)s",
    handlers=[logging.StreamHandler(sys.stderr)],
    force=True,
)
log = logging

# =============== CONFIG ===============
PROJECT_ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH  = PROJECT_ROOT / "config.json"

try:
    cfg = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
except Exception as e:
    log.exception(f"Cannot read config.json at {CONFIG_PATH}: {e}")
    sys.stdout.write(json.dumps({"ok": False, "rc": 1, "error": "config_read_failed"}) + "\n")
    sys.exit(1)

# tolerate config zoo
db_user = cfg.get("user") or cfg.get("USER")
db_pwd  = cfg.get("password") or cfg.get("PWD")
db_host = cfg.get("host") or cfg.get("HOST")
db_port = cfg.get("port") or cfg.get("PORT")
db_name = cfg.get("dbname") or cfg.get("DB")

if not all([db_user, db_pwd, db_host, db_port, db_name]):
    log.error("DB config incomplete. Expected keys: user/password/host/port/db (case-insensitive).")
    sys.stdout.write(json.dumps({"ok": False, "rc": 1, "error": "db_config_incomplete"}) + "\n")
    sys.exit(1)

DB_URL = f"postgresql+psycopg2://{db_user}:{db_pwd}@{db_host}:{db_port}/{db_name}"
engine = create_engine(DB_URL, pool_pre_ping=True, connect_args={"connect_timeout": 10})

# =============== SCRIPTS & ORDERING ===============
SCRIPT_DIR = Path(__file__).resolve().parent


DEP_ORDER = ["totalized.py", "seller_enrich.py", "price_enrich.py"]


DEFAULT_SCRIPTS = DEP_ORDER.copy()


ENV_SCRIPTS = [s.strip() for s in os.getenv("WRAP_SCRIPTS", ",".join(DEFAULT_SCRIPTS)).split(",") if s.strip()]


RESPECT_ORDER = os.getenv("WRAP_RESPECT_ORDER", "0") == "1"

def _order_by_deps(scripts: List[str]) -> List[str]:
    known = [s for s in DEP_ORDER if s in scripts]
    unknown = [s for s in scripts if s not in DEP_ORDER]
    return known + unknown

SCRIPTS = ENV_SCRIPTS if RESPECT_ORDER else _order_by_deps(ENV_SCRIPTS)
log.info(f"Final steps order: {SCRIPTS} (respect_order={int(RESPECT_ORDER)})")

# Per-step timeout
STEP_TIMEOUT = int(os.getenv("WRAP_TIMEOUT_S", "900"))  # 15 min per script

# =============== SQL: ensure stats table (no schema creation) ===============
DDL_RUN_STATS = """
CREATE TABLE IF NOT EXISTS gold.run_stats (
    run_id           UUID            NOT NULL,
    run_started_at   TIMESTAMPTZ     NOT NULL,
    run_finished_at  TIMESTAMPTZ     NOT NULL,
    step             TEXT            NOT NULL,   -- step name or "__summary__"
    ok               BOOLEAN         NOT NULL,
    rc               INT             NOT NULL,
    elapsed_s        NUMERIC(12,3)   NOT NULL,
    host             TEXT            NOT NULL,
    app              TEXT            NOT NULL,   -- "wrapper_gold"
    details          JSONB           NOT NULL,   -- raw JSON from step
    PRIMARY KEY (run_id, step, run_finished_at)
);
CREATE INDEX IF NOT EXISTS idx_run_stats_started ON gold.run_stats (run_started_at);
CREATE INDEX IF NOT EXISTS idx_run_stats_step    ON gold.run_stats (step);
"""

INSERT_RUN_STAT = """
INSERT INTO gold.run_stats
(run_id, run_started_at, run_finished_at, step, ok, rc, elapsed_s, host, app, details)
VALUES (:run_id, :run_started_at, :run_finished_at, :step, :ok, :rc, :elapsed_s, :host, :app, CAST(:details AS JSONB));
"""

# =============== utils ===============
def run_script(py_file: str) -> Dict[str, Any]:
    """Run a Python script and parse the LAST valid JSON from stdout."""
    step_name = Path(py_file).stem
    script_path = SCRIPT_DIR / py_file
    if not script_path.exists():
        log.error(f"{py_file}: file not found at {script_path}")
        return {"step": step_name, "ok": False, "rc": 127, "elapsed_s": 0.0, "error": "file_not_found"}

    cmd = f"{shlex.quote(sys.executable)} {shlex.quote(str(script_path))}"
    log.info(f"Start step: {py_file} | cmd={cmd} | timeout={STEP_TIMEOUT}s")
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
        log.error(f"{py_file}: timeout after {STEP_TIMEOUT}s, killing process...")
        try:
            proc.kill()
            out, err = proc.communicate(timeout=5)
        except Exception:
            out, err = "", ""
        elapsed = time.perf_counter() - t0
        return {"step": step_name, "ok": False, "rc": 124, "elapsed_s": round(elapsed, 3), "error": "timeout"}

    elapsed = time.perf_counter() - t0
    rc = proc.returncode

    if err:
        for line in err.splitlines():
            if line.strip():
                log.info(f"[{py_file}] {line}")

    parsed: Dict[str, Any] = {}
    last_json_line = None

    if out:
        for line in out.splitlines():
            candidate = line.strip()
            if not candidate:
                continue
            try:
                obj = json.loads(candidate)
                parsed = obj
                last_json_line = candidate
            except Exception:
                pass
        if last_json_line is None:
            log.error(f"{py_file}: stdout contains no valid JSON.")
            for i, line in enumerate(out.splitlines()[:5], 1):
                log.error(f"[{py_file}] stdout[{i}]: {line}")
    else:
        log.error(f"{py_file}: empty stdout.")

    if rc != 0 or not parsed.get("ok", False):
        if last_json_line:
            log.error(f"[{py_file}] last-json: {last_json_line}")

    step_ok = bool(parsed.get("ok", rc == 0))
    parsed.setdefault("ok", step_ok)
    parsed.setdefault("rc", rc)
    parsed.setdefault("elapsed_s", round(parsed.get("elapsed_s", elapsed), 3))
    parsed.setdefault("step", step_name)

    log.info(f"Finish step: {py_file} | ok={parsed['ok']} rc={parsed['rc']} elapsed_s={parsed['elapsed_s']}s")
    return parsed


def aggregate_numeric(details_list: List[Dict[str, Any]]) -> Dict[str, Any]:
    skip = {"rc", "ok", "elapsed_s"}
    agg: Dict[str, Any] = {}
    for d in details_list:
        for k, v in d.items():
            if k in skip:
                continue
            if isinstance(v, (int, float)):
                agg[k] = agg.get(k, 0) + v
    return agg


def ensure_stats_table():
    with engine.begin() as conn:
        conn.execute(text(DDL_RUN_STATS))


def persist_stats(run_id, started_at, finished_at, per_step, summary):
    host = socket.gethostname()
    app  = "wrapper_gold"
    with engine.begin() as conn:
        for d in per_step:
            conn.execute(text(INSERT_RUN_STAT), {
                "run_id": str(run_id),
                "run_started_at": started_at,
                "run_finished_at": finished_at,
                "step": str(d.get("step") or "unknown"),
                "ok": bool(d.get("ok", False)),
                "rc": int(d.get("rc", 0)),
                "elapsed_s": float(d.get("elapsed_s", 0)),
                "host": host,
                "app": app,
                "details": json.dumps(d, ensure_ascii=False),
            })
        conn.execute(text(INSERT_RUN_STAT), {
            "run_id": str(run_id),
            "run_started_at": started_at,
            "run_finished_at": finished_at,
            "step": "__summary__",
            "ok": bool(summary.get("ok", False)),
            "rc": int(summary.get("rc", 0)),
            "elapsed_s": float(summary.get("elapsed_s", 0.0)),
            "host": host,
            "app": app,
            "details": json.dumps(summary, ensure_ascii=False),
        })


def echo_json(obj: Dict[str, Any]):
    sys.stdout.write(json.dumps(obj, ensure_ascii=False) + "\n")
    sys.stdout.flush()


def main():
    ensure_stats_table()

    run_id = uuid.uuid4()
    from datetime import datetime, timezone
    started_iso = datetime.now(timezone.utc).isoformat()

    per_step: List[Dict[str, Any]] = []
    overall_ok = True
    total_elapsed = 0.0

    for py in SCRIPTS:
        d = run_script(py)
        per_step.append(d)
        total_elapsed += float(d.get("elapsed_s", 0.0))
        if not d.get("ok", False) or int(d.get("rc", 0)) != 0:
            overall_ok = False
            log.error(f"Step failed {d.get('step')}: aborting pipeline.")
            break

    agg = aggregate_numeric(per_step)

    summary = {
        "step": "__summary__",
        "ok": overall_ok,
        "rc": 0 if overall_ok else 1,
        "elapsed_s": round(total_elapsed, 3),
        "run_id": str(run_id),
        "steps": [d.get("step") for d in per_step],
        "stats": agg
    }

    finished_iso = datetime.now(timezone.utc).isoformat()
    try:
        persist_stats(run_id, started_iso, finished_iso, per_step, summary)
    except Exception as e:
        log.exception(f"Failed to write gold.run_stats: {e}")

    echo_json(summary)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log.exception(f"wrapper_gold fatal error: {e}")
        echo_json({"ok": False, "rc": 1, "error": str(e)})
        sys.exit(1)
