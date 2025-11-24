from __future__ import annotations
import json
import sys
import os
import logging
import subprocess
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import List, Dict, Any, Tuple, Optional
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import smtplib
from email.message import EmailMessage
import time

# ============ CONFIGURATION ============
AUDIT_SCHEMA = "gold"
AUDIT_TABLE = "daily_job"

# Fail-fast toggle: stop pipeline on first error
FAIL_FAST = False

# Layer toggles
BRONZE_ENABLED = True
SILVER_ENABLED = True
GOLD_ENABLED = True
BI_ENABLED = True

# Bronze source toggles
BEZREALITKY_ENABLED = True
SREALITY_ENABLED = True
IDNES_ENABLED = True

# ============ LOGGING ============
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | main | %(message)s",
    handlers=[logging.StreamHandler(sys.stderr)],
    force=True,
)
log = logging.getLogger("main")

# ============ CONSTANTS ============
TZ = ZoneInfo("Europe/Prague")
PROJECT_ROOT = Path(__file__).resolve().parent
CONFIG_PATH = PROJECT_ROOT / "config.json"

# ============ PIPELINE STEPS ============
PIPELINE = [
    # Bronze Layer
    {
        "layer": "bronze",
        "source": "bezrealitky",
        "enabled": BRONZE_ENABLED and BEZREALITKY_ENABLED,
        "script": "ETL/bezrealitky/wrapper_bezrealitky.py",
    },
    {
        "layer": "bronze",
        "source": "sreality",
        "enabled": BRONZE_ENABLED and SREALITY_ENABLED,
        "script": "ETL/sreality/wrapper_sreality.py",
    },
    {
        "layer": "bronze",
        "source": "idnes",
        "enabled": BRONZE_ENABLED and IDNES_ENABLED,
        "script": "ETL/idnes/wrapper_idnes.py",
    },
    # Silver Layer
    {
        "layer": "silver",
        "source": None,
        "enabled": SILVER_ENABLED,
        "script": "silver_layer/wrapper_silver.py",
    },
    # Gold Layer
    {
        "layer": "gold",
        "source": None,
        "enabled": GOLD_ENABLED,
        "script": "gold/wrapper_gold.py",
    },
    # BI Layer
    {
        "layer": "bi",
        "source": None,
        "enabled": BI_ENABLED,
        "script": "BI_reality_data/wrapper_bi.py",
    },
]


# ============ CONFIG ============
def load_cfg() -> dict:
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def build_db_url(cfg: dict) -> str:
    u = cfg.get("USER") or cfg.get("user")
    p = cfg.get("PWD") or cfg.get("password")
    h = cfg.get("HOST") or cfg.get("host")
    port = cfg.get("PORT") or cfg.get("port")
    d = cfg.get("DB") or cfg.get("dbname")
    return f"postgresql+psycopg2://{u}:{p}@{h}:{port}/{d}"


# ============ DB INIT ============
DDL_DAILY_JOB = f"""
CREATE TABLE IF NOT EXISTS {AUDIT_SCHEMA}.{AUDIT_TABLE} (
    id BIGSERIAL PRIMARY KEY,
    run_ts TIMESTAMPTZ NOT NULL,
    layer TEXT NOT NULL,
    source TEXT,
    step_name TEXT NOT NULL,
    status TEXT NOT NULL,
    elapsed_s NUMERIC,
    rows_input BIGINT,
    rows_output BIGINT,
    rows_inserted BIGINT,
    rows_updated BIGINT,
    details JSONB,
    error_message TEXT
);
CREATE INDEX IF NOT EXISTS {AUDIT_TABLE}_run_ts_idx ON {AUDIT_SCHEMA}.{AUDIT_TABLE} (run_ts DESC);
CREATE INDEX IF NOT EXISTS {AUDIT_TABLE}_layer_idx ON {AUDIT_SCHEMA}.{AUDIT_TABLE} (layer);
"""


def ensure_audit_table(engine):
    with engine.begin() as conn:
        conn.execute(text(DDL_DAILY_JOB))


# ============ JSON PARSING ============
def parse_json_output(stdout: str) -> Tuple[Dict[str, Any], bool]:
    if not stdout:
        return {}, False
    try:
        obj = json.loads(stdout.strip())
        if isinstance(obj, dict):
            return obj, True
    except Exception:
        pass
    lines = stdout.strip().splitlines()
    for line in reversed(lines):
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
            if isinstance(obj, dict):
                return obj, True
        except Exception:
            continue

    return {}, False


# ============ WRAPPER EXECUTION ============
def run_wrapper(script_path: str) -> Tuple[bool, Dict[str, Any], str, float]:
    cmd = [sys.executable, str(PROJECT_ROOT / script_path)]
    start = time.perf_counter()

    try:
        proc = subprocess.Popen(
            cmd,
            cwd=str(PROJECT_ROOT),
            env=os.environ.copy(),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        stdout_text, stderr_text = proc.communicate(timeout=7200)  # 2 hour timeout
    except subprocess.TimeoutExpired:
        proc.kill()
        elapsed = time.perf_counter() - start
        return False, {"error": "timeout"}, "Process timeout after 2 hours", elapsed
    except Exception as e:
        elapsed = time.perf_counter() - start
        return False, {"error": str(e)}, f"Exception: {e}", elapsed

    elapsed = time.perf_counter() - start

    json_data, parsed = parse_json_output(stdout_text)

    success = False
    if parsed:
        status = str(json_data.get("status", "")).lower()
        state = str(json_data.get("state", "")).lower()
        ok = json_data.get("ok")

        if status in ("success", "completed", "ok"):
            success = True
        elif state == "ok":
            success = True
        elif ok is True:
            success = True
        elif proc.returncode == 0:
            success = True
    else:
        success = (proc.returncode == 0)

    stderr_tail = ""
    if stderr_text:
        lines = stderr_text.strip().splitlines()
        stderr_tail = "\n".join(lines[-50:])[-10000:]

    return success, json_data, stderr_tail, elapsed


# ============ DATA EXTRACTION ============
def extract_metrics(layer: str, source: Optional[str], json_data: Dict[str, Any]) -> Dict[str, Any]:
    metrics = {
        "rows_input": None,
        "rows_output": None,
        "rows_inserted": None,
        "rows_updated": None,
    }

    def find_number(data: Any, keys: List[str]) -> Optional[int]:
        if isinstance(data, dict):
            for key in keys:
                if key in data:
                    val = data[key]
                    if isinstance(val, (int, float)) and not isinstance(val, bool):
                        return int(val)
            for v in data.values():
                result = find_number(v, keys)
                if result is not None:
                    return result
        elif isinstance(data, (list, tuple)):
            for item in data:
                result = find_number(item, keys)
                if result is not None:
                    return result
        return None

    if layer == "bronze":
        results = json_data.get("results", [])
        if results and isinstance(results, list):
            first_result = results[0]
            summaries = first_result.get("summaries", [])
            if summaries and isinstance(summaries, list):
                stats = summaries[0]

                phase = stats.get("phase_counts", {})
                metrics["rows_input"] = phase.get("source_rows") or stats.get("source_rows")

                metrics["rows_output"] = stats.get("total_rows")

                metrics["rows_inserted"] = (
                        phase.get("added") or
                        stats.get("new_rows") or
                        stats.get("inserted_new") or
                        stats.get("added_today")
                )

    # Silver layer
    elif layer == "silver":
        agg = json_data.get("aggregated", {})
        metrics["rows_input"] = agg.get("rows_input")
        metrics["rows_output"] = agg.get("rows_output")
        metrics["rows_inserted"] = agg.get("rows_inserted")
        metrics["rows_updated"] = agg.get("rows_updated")

    # Gold layer
    elif layer == "gold":
        agg = json_data.get("aggregated", {})
        metrics["rows_input"] = agg.get("rows_fetched")
        metrics["rows_output"] = agg.get("rows_filtered") or agg.get("rows_matched")
        metrics["rows_inserted"] = agg.get("rows_inserted")
        metrics["rows_updated"] = agg.get("rows_updated")

    # BI layer
    elif layer == "bi":
        agg = json_data.get("aggregated", {})
        metrics["rows_input"] = find_number(agg, [
            "rows_fetched",
            "sanity_gold_totalized_distinct_objects"
        ])
        metrics["rows_output"] = (
                agg.get("rows_created") or
                find_number(agg, ["rows_dim_date", "rows_fact_active"])
        )
        metrics["rows_inserted"] = metrics["rows_output"]

    return metrics


# ============ PERSIST TO DB ============
def persist_step(engine, run_ts: datetime, step: Dict[str, Any], success: bool,
                 json_data: Dict[str, Any], stderr: str, elapsed_s: float):
    metrics = extract_metrics(step["layer"], step.get("source"), json_data)

    error_msg = None
    if not success:
        error_msg = json_data.get("error") or stderr[:500] if stderr else "Unknown error"

    with engine.begin() as conn:
        conn.execute(text(f"""
            INSERT INTO {AUDIT_SCHEMA}.{AUDIT_TABLE}
            (run_ts, layer, source, step_name, status, elapsed_s,
             rows_input, rows_output, rows_inserted, rows_updated,
             details, error_message)
            VALUES
            (:run_ts, :layer, :source, :step_name, :status, :elapsed_s,
             :rows_input, :rows_output, :rows_inserted, :rows_updated,
             :details, :error_message)
        """), {
            "run_ts": run_ts,
            "layer": step["layer"],
            "source": step.get("source"),
            "step_name": Path(step["script"]).stem,
            "status": "ok" if success else "fail",
            "elapsed_s": round(elapsed_s, 3),
            "rows_input": metrics["rows_input"],
            "rows_output": metrics["rows_output"],
            "rows_inserted": metrics["rows_inserted"],
            "rows_updated": metrics["rows_updated"],
            "details": json.dumps(json_data, ensure_ascii=False) if json_data else None,
            "error_message": error_msg,
        })


# ============ REPORT GENERATION ============
def format_number(n: Optional[int]) -> str:
    return f"{n:,}" if n is not None else "-"


def generate_report(run_ts: datetime, results: List[Dict[str, Any]], total_elapsed: float) -> str:
    lines = []
    lines.append("=" * 80)
    lines.append(f"Reality Pipeline Report — {run_ts.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    lines.append("=" * 80)
    lines.append("")

    layers = {}
    for r in results:
        layer = r["layer"]
        if layer not in layers:
            layers[layer] = []
        layers[layer].append(r)

    # Bronze Layer
    if "bronze" in layers:
        lines.append("BRONZE LAYER:")
        lines.append(
            "┌" + "─" * 14 + "┬" + "─" * 8 + "┬" + "─" * 10 + "┬" + "─" * 10 + "┬" + "─" * 10 + "┬" + "─" * 9 + "┐")
        lines.append("│ Source       │ Status │ Input    │ Output   │ Inserted │ Time(s) │")
        lines.append(
            "├" + "─" * 14 + "┼" + "─" * 8 + "┼" + "─" * 10 + "┼" + "─" * 10 + "┼" + "─" * 10 + "┼" + "─" * 9 + "┤")

        for r in layers["bronze"]:
            src = (r["source"] or "unknown")[:12].ljust(12)
            status = r["status"][:6].ljust(6)
            inp = format_number(r["metrics"]["rows_input"]).rjust(8)
            out = format_number(r["metrics"]["rows_output"]).rjust(8)
            ins = format_number(r["metrics"]["rows_inserted"]).rjust(8)
            elapsed = f"{r['elapsed_s']:.1f}".rjust(7)
            lines.append(f"│ {src} │ {status} │ {inp} │ {out} │ {ins} │ {elapsed} │")

        lines.append(
            "└" + "─" * 14 + "┴" + "─" * 8 + "┴" + "─" * 10 + "┴" + "─" * 10 + "┴" + "─" * 10 + "┴" + "─" * 9 + "┘")
        lines.append("")

    # Silver Layer - ДЕТАЛЬНАЯ ТАБЛИЦА
    if "silver" in layers:
        lines.append("SILVER LAYER:")
        r = layers["silver"][0]

        silver_steps = r.get("silver_steps", [])

        if silver_steps:
            # Детальная таблица по шагам
            lines.append("┌" + "─" * 22 + "┬" + "─" * 8 + "┬" + "─" * 10 + "┬" + "─" * 10 + "┬" + "─" * 9 + "┐")
            lines.append("│ Step                 │ Status │ Input    │ Output   │ Time(s) │")
            lines.append("├" + "─" * 22 + "┼" + "─" * 8 + "┼" + "─" * 10 + "┼" + "─" * 10 + "┼" + "─" * 9 + "┤")

            for step in silver_steps:
                step_name = step["step"][:20].ljust(20)
                status = step["status"][:6].ljust(6)
                inp = format_number(step.get("rows_input")).rjust(8)
                out = format_number(step.get("rows_output")).rjust(8)
                elapsed = f"{step['elapsed_s']:.1f}".rjust(7)
                lines.append(f"│ {step_name} │ {status} │ {inp} │ {out} │ {elapsed} │")

            lines.append("└" + "─" * 22 + "┴" + "─" * 8 + "┴" + "─" * 10 + "┴" + "─" * 10 + "┴" + "─" * 9 + "┘")
        else:
            status = r["status"][:6].ljust(6)
            inp = format_number(r["metrics"]["rows_input"]).rjust(8)
            out = format_number(r["metrics"]["rows_output"]).rjust(8)
            elapsed = f"{r['elapsed_s']:.1f}".rjust(7)

            lines.append("┌" + "─" * 21 + "┬" + "─" * 8 + "┬" + "─" * 10 + "┬" + "─" * 10 + "┬" + "─" * 9 + "┐")
            lines.append("│ Step                │ Status │ Input    │ Output   │ Time(s) │")
            lines.append("├" + "─" * 21 + "┼" + "─" * 8 + "┼" + "─" * 10 + "┼" + "─" * 10 + "┼" + "─" * 9 + "┤")
            lines.append(f"│ silver_pipeline     │ {status} │ {inp} │ {out} │ {elapsed} │")
            lines.append("└" + "─" * 21 + "┴" + "─" * 8 + "┴" + "─" * 10 + "┴" + "─" * 10 + "┴" + "─" * 9 + "┘")

        lines.append("")

    # Gold Layer
    if "gold" in layers:
        lines.append("GOLD LAYER:")
        r = layers["gold"][0]
        status = r["status"][:6].ljust(6)
        inp = format_number(r["metrics"]["rows_input"]).rjust(8)
        out = format_number(r["metrics"]["rows_output"]).rjust(8)
        ins = format_number(r["metrics"]["rows_inserted"]).rjust(8)
        elapsed = f"{r['elapsed_s']:.1f}".rjust(7)

        lines.append(
            "┌" + "─" * 17 + "┬" + "─" * 8 + "┬" + "─" * 10 + "┬" + "─" * 10 + "┬" + "─" * 10 + "┬" + "─" * 9 + "┐")
        lines.append("│ Step            │ Status │ Fetched  │ Matched  │ Inserted │ Time(s) │")
        lines.append(
            "├" + "─" * 17 + "┼" + "─" * 8 + "┼" + "─" * 10 + "┼" + "─" * 10 + "┼" + "─" * 10 + "┼" + "─" * 9 + "┤")
        lines.append(f"│ gold_pipeline   │ {status} │ {inp} │ {out} │ {ins} │ {elapsed} │")
        lines.append(
            "└" + "─" * 17 + "┴" + "─" * 8 + "┴" + "─" * 10 + "┴" + "─" * 10 + "┴" + "─" * 10 + "┴" + "─" * 9 + "┘")
        lines.append("")

    # BI Layer
    if "bi" in layers:
        lines.append("BI LAYER:")
        r = layers["bi"][0]
        status = r["status"][:6].ljust(6)
        inp = format_number(r["metrics"]["rows_input"]).rjust(8)
        out = format_number(r["metrics"]["rows_output"]).rjust(8)
        elapsed = f"{r['elapsed_s']:.1f}".rjust(7)

        lines.append("┌" + "─" * 14 + "┬" + "─" * 8 + "┬" + "─" * 10 + "┬" + "─" * 10 + "┬" + "─" * 9 + "┐")
        lines.append("│ Step         │ Status │ Input    │ Created  │ Time(s) │")
        lines.append("├" + "─" * 14 + "┼" + "─" * 8 + "┼" + "─" * 10 + "┼" + "─" * 10 + "┼" + "─" * 9 + "┤")
        lines.append(f"│ bi_pipeline  │ {status} │ {inp} │ {out} │ {elapsed} │")
        lines.append("└" + "─" * 14 + "┴" + "─" * 8 + "┴" + "─" * 10 + "┴" + "─" * 10 + "┴" + "─" * 9 + "┘")
        lines.append("")

    # Summary
    lines.append("=" * 80)
    overall_ok = all(r["status"] == "ok" for r in results if r["status"] != "skip")
    lines.append(f"Overall: {'SUCCESS ✓' if overall_ok else 'FAILED ✗'}")

    minutes = int(total_elapsed // 60)
    seconds = int(total_elapsed % 60)
    lines.append(f"Total elapsed: {total_elapsed:.1f}s ({minutes}m {seconds}s)")
    lines.append("=" * 80)

    # Errors
    errors = [r for r in results if r["status"] == "fail"]
    if errors:
        lines.append("")
        lines.append("ERRORS:")
        for err in errors:
            lines.append(
                f"  - {err['layer']}/{err.get('source') or err['step_name']}: {err.get('error_message', 'Unknown error')[:200]}")
    else:
        lines.append("")
        lines.append("Errors: None")

    return "\n".join(lines)


# ============ EMAIL ============
def send_email(cfg: dict, subject: str, body: str):
    host = cfg.get("SMTP_HOST")
    if not host:
        return

    port = int(cfg.get("SMTP_PORT", 587))
    msg = EmailMessage()
    msg["From"] = cfg.get("SMTP_FROM", cfg.get("SMTP_USER", "pipeline@localhost"))
    msg["To"] = cfg.get("SMTP_TO", "obchod@lifegoal.cz")
    msg["Subject"] = subject
    msg.set_content(body)

    try:
        with smtplib.SMTP(host, port, timeout=60) as s:
            if cfg.get("SMTP_STARTTLS", True):
                s.starttls()
            if cfg.get("SMTP_USER") and cfg.get("SMTP_PASS"):
                s.login(cfg["SMTP_USER"], cfg["SMTP_PASS"])
            s.send_message(msg)
        log.info("Email sent successfully")
    except Exception as e:
        log.error(f"Failed to send email: {e}")


# ============ MAIN ============
def main():
    cfg = load_cfg()
    engine = create_engine(build_db_url(cfg), pool_pre_ping=True, future=True)

    # Ensure audit table exists
    ensure_audit_table(engine)

    run_ts = datetime.now(tz=TZ)
    results = []
    overall_ok = True
    start_time = time.perf_counter()

    log.info("=" * 80)
    log.info(f"Starting Reality Pipeline — {run_ts.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    log.info("=" * 80)

    # Run pipeline
    for step in PIPELINE:
        if not step["enabled"]:
            results.append({
                "layer": step["layer"],
                "source": step.get("source"),
                "step_name": Path(step["script"]).stem,
                "status": "skip",
                "elapsed_s": 0.0,
                "metrics": {
                    "rows_input": None,
                    "rows_output": None,
                    "rows_inserted": None,
                    "rows_updated": None,
                },
                "error_message": None,
            })
            continue

        step_name = f"{step['layer']}/{step.get('source') or Path(step['script']).stem}"
        log.info(f"Running: {step_name}")

        success, json_data, stderr, elapsed = run_wrapper(step["script"])
        metrics = extract_metrics(step["layer"], step.get("source"), json_data)

        error_msg = None
        if not success:
            error_msg = json_data.get("error") or (stderr[:500] if stderr else "Unknown error")
            log.error(f"FAILED: {step_name} — {error_msg}")
            overall_ok = False
        else:
            log.info(f"OK: {step_name} ({elapsed:.1f}s)")

        result_entry = {
            "layer": step["layer"],
            "source": step.get("source"),
            "step_name": Path(step["script"]).stem,
            "status": "ok" if success else "fail",
            "elapsed_s": elapsed,
            "metrics": metrics,
            "error_message": error_msg,
        }

        if step["layer"] == "silver" and "steps" in json_data:
            result_entry["silver_steps"] = json_data["steps"]

        results.append(result_entry)

        # Persist to DB
        try:
            persist_step(engine, run_ts, step, success, json_data, stderr, elapsed)
        except Exception as e:
            log.error(f"Failed to persist step result: {e}")

        # Fail-fast check
        if FAIL_FAST and not success:
            log.error("FAIL_FAST enabled, stopping pipeline")
            # Mark remaining steps as skipped
            idx = PIPELINE.index(step)
            for remaining in PIPELINE[idx + 1:]:
                if remaining["enabled"]:
                    results.append({
                        "layer": remaining["layer"],
                        "source": remaining.get("source"),
                        "step_name": Path(remaining["script"]).stem,
                        "status": "skip",
                        "elapsed_s": 0.0,
                        "metrics": {
                            "rows_input": None,
                            "rows_output": None,
                            "rows_inserted": None,
                            "rows_updated": None,
                        },
                        "error_message": "Skipped due to previous failure",
                    })
            break

    total_elapsed = time.perf_counter() - start_time

    # Generate report
    report = generate_report(run_ts, results, total_elapsed)

    # Output to console
    print("\n" + report)

    # Save to log file
    log_dir = PROJECT_ROOT / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    ts = run_ts.strftime("%Y%m%d_%H%M")
    (log_dir / f"pipeline_{ts}.log").write_text(report, encoding="utf-8")
    (log_dir / "last_run.log").write_text(report, encoding="utf-8")

    # Send email
    subject = f"[Reality Pipeline] {'SUCCESS' if overall_ok else 'FAILED'} — {run_ts.strftime('%Y-%m-%d %H:%M')}"
    send_email(cfg, subject, report)

    # JSON output
    print(json.dumps({
        "run_ts": run_ts.isoformat(),
        "overall_ok": overall_ok,
        "total_elapsed_s": round(total_elapsed, 3),
        "fail_fast_enabled": FAIL_FAST,
        "results": results
    }, ensure_ascii=False))

    sys.exit(0 if overall_ok else 1)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log.exception(f"Fatal error: {e}")
        print(json.dumps({
            "error": str(e),
            "overall_ok": False
        }, ensure_ascii=False))
        sys.exit(1)