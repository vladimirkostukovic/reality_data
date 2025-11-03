import subprocess
import sys
import json
import time
import logging
from pathlib import Path
from sqlalchemy import create_engine, text

# ---------- LOGGING (stderr only) ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | wrapper_bi | %(message)s",
    handlers=[logging.StreamHandler(sys.stderr)],
    force=True,
)
log = logging.getLogger("wrapper_bi")

# ---------- CONFIG ----------
PROJECT_ROOT = Path(__file__).resolve().parents[1]
cfg = json.loads((PROJECT_ROOT / "config.json").read_text(encoding="utf-8"))

db_user = cfg.get("user") or cfg.get("USER")
db_pwd  = cfg.get("password") or cfg.get("PWD")
db_host = cfg.get("host") or cfg.get("HOST")
db_port = cfg.get("port") or cfg.get("PORT")
db_name = cfg.get("dbname") or cfg.get("DB")
DB_URL  = f"postgresql+psycopg2://{db_user}:{db_pwd}@{db_host}:{db_port}/{db_name}"

engine = create_engine(DB_URL, pool_pre_ping=True, connect_args={"connect_timeout": 10})
BI_SCHEMA = "bi_reality_data"

GOLD_SCRIPT = PROJECT_ROOT / "BI_reality_data" / "gold_to_bi.py"
FLAT_SCRIPT = PROJECT_ROOT / "BI_reality_data" / "flat_table.py"

# ---------- FUNCTIONS ----------
def ensure_stats_table(conn):
    conn.execute(text(f"""
        CREATE TABLE IF NOT EXISTS {BI_SCHEMA}.pipeline_stats (
            id              BIGSERIAL PRIMARY KEY,
            run_at          TIMESTAMPTZ DEFAULT now(),
            step            TEXT,
            ok              BOOLEAN,
            rc              INT,
            elapsed_s       NUMERIC,
            rows_dim_date   INT,
            rows_dim_geo    INT,
            rows_dim_seller INT,
            rows_dim_object INT,
            rows_fact_object_periods INT,
            rows_price_events INT,
            notes            JSONB
        );
    """))

def run_script(script_path: Path, step_name: str) -> dict:
    cmd = [sys.executable, str(script_path)]
    log.info(f"Launch {step_name}: {' '.join(cmd)}")
    t0 = time.perf_counter()
    proc = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=3600
    )
    elapsed = round(time.perf_counter() - t0, 2)

    if proc.returncode != 0:
        log.error(f"{step_name} exited with code {proc.returncode}")
        log.error(proc.stderr.strip())
        return {
            "step": step_name,
            "ok": False,
            "rc": proc.returncode,
            "error": proc.stderr.strip(),
            "elapsed_s": elapsed
        }

    try:
        out = json.loads(proc.stdout.strip() or "{}")
        out["elapsed_s"] = out.get("elapsed_s", elapsed)
        out["step"] = step_name
        out["ok"] = out.get("ok", True)
        out["rc"] = proc.returncode
        return out
    except Exception as e:
        log.error(f"Failed to parse JSON from stdout ({step_name}): {e}")
        log.error(proc.stdout)
        return {
            "step": step_name,
            "ok": False,
            "rc": 99,
            "error": f"JSON parse failed: {e}",
            "elapsed_s": elapsed
        }

def insert_stats(conn, result: dict):
    stats = result.get("stats", {}) or {}
    conn.execute(text(f"""
        INSERT INTO {BI_SCHEMA}.pipeline_stats (
            step, ok, rc, elapsed_s,
            rows_dim_date, rows_dim_geo, rows_dim_seller, rows_dim_object,
            rows_fact_object_periods, rows_price_events, notes
        )
        VALUES (
            :step, :ok, :rc, :elapsed_s,
            :rows_dim_date, :rows_dim_geo, :rows_dim_seller, :rows_dim_object,
            :rows_fact_object_periods, :rows_price_events, :notes
        )
    """), {
        "step": result.get("step"),
        "ok": result.get("ok"),
        "rc": result.get("rc", 1),
        "elapsed_s": result.get("elapsed_s"),
        "rows_dim_date": stats.get("rows_dim_date"),
        "rows_dim_geo": stats.get("rows_dim_geo"),
        "rows_dim_seller": stats.get("rows_dim_seller"),
        "rows_dim_object": stats.get("rows_dim_object"),
        "rows_fact_object_periods": stats.get("rows_fact_object_periods"),
        "rows_price_events": stats.get("rows_price_events"),
        "notes": json.dumps(stats or result)
    })

# ---------- MAIN ----------
def main():
    t0 = time.perf_counter()
    steps = [
        ("gold_to_bi", GOLD_SCRIPT),
        ("flat_table", FLAT_SCRIPT),
    ]
    all_ok = True

    with engine.begin() as conn:
        ensure_stats_table(conn)

        for step_name, script in steps:
            result = run_script(script, step_name)
            insert_stats(conn, result)
            if not result.get("ok"):
                all_ok = False
                log.error(f"Step {step_name} failed; stopping pipeline.")
                break

    elapsed = round(time.perf_counter() - t0, 3)
    log.info(f"BI pipeline finished in {elapsed}s (ok={all_ok})")

    print(json.dumps({
        "step": "wrapper_bi",
        "ok": all_ok,
        "elapsed_s": elapsed
    }, ensure_ascii=False))

if __name__ == "__main__":
    main()