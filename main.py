import json
import sys
import os
import logging
import subprocess
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import List, Dict, Any, Tuple, Optional, Set
from sqlalchemy import create_engine, text
import smtplib
from email.message import EmailMessage
import re
import ast
import time

# ------------ LOGGING  ------------
logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s | %(levelname)s | main | %(message)s",
    handlers=[logging.StreamHandler(sys.stderr)],
    force=True,
)
log = logging.getLogger("main")

# ------------ CONSTANTS ------------
TZ = ZoneInfo("Europe/Prague")
PROJECT_ROOT = Path(__file__).resolve().parent
CONFIG_PATH = PROJECT_ROOT / "config.json"

BI_AUDIT_SCHEMA = "bi_reality_data"

# ------------ INLINE TOGGLES ------------
bezrealitky = True
sreality    = True
idnes       = True
photo_modul = False
silver      = True
gold        = True
bi          = True

NON_CRITICAL: Set[str] = {
    "photo_modul/wrapper_photo_modul.py",
}

STEPS: List[Tuple[str, bool]] = [
    ("ETL/bezrealitky/wrapper_bezrealitky.py", bezrealitky),
    ("ETL/sreality/wrapper_sreality.py",       sreality),
    ("ETL/idnes/wrapper_idnes.py",             idnes),
    ("photo_modul/wrapper_photo_modul.py",     photo_modul),
    ("silver_layer/wrapper_silver.py",         silver),
    ("gold/wrapper_gold.py",                   gold),
    ("BI_reality_data/wrapper_bi.py",          bi),
]

# ------------ CONFIG ------------
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

# ------------ DB INIT ------------
DDL_JOB_RUNS = f"""
CREATE TABLE IF NOT EXISTS {BI_AUDIT_SCHEMA}.job_runs (
    id BIGSERIAL PRIMARY KEY,
    run_ts TIMESTAMPTZ NOT NULL,
    step_name TEXT NOT NULL,
    status TEXT NOT NULL,
    elapsed_s NUMERIC,
    rows_in BIGINT,
    rows_out BIGINT,
    inserted BIGINT,
    updated BIGINT,
    deleted BIGINT,
    extra_json JSONB,
    stderr_tail TEXT
);
CREATE INDEX IF NOT EXISTS job_runs_run_ts_idx
    ON {BI_AUDIT_SCHEMA}.job_runs (run_ts DESC);
"""

def ensure_bi_tables(engine):

    with engine.begin() as conn:
        conn.execute(text(DDL_JOB_RUNS))

# ------------ HELPERS ------------
def _shortname(py_file: str) -> str:
    p = Path(py_file)
    name = p.stem.replace("wrapper_", "")
    parent = p.parent.name
    return name if parent == "ETL" or not parent else parent

JSON_OBJ_RE = re.compile(r"\{.*\}", re.DOTALL)

def _parse_stats(stdout: str) -> Tuple[Dict[str, Any], bool]:
    s = (stdout or "").strip()
    if not s:
        return {}, False
    try:
        obj = json.loads(s)
        if isinstance(obj, dict):
            return obj, True
    except Exception:
        pass

    matches = list(JSON_OBJ_RE.finditer(s))
    for m in reversed(matches):
        block = m.group(0).strip()
        for parser in (json.loads, ast.literal_eval):
            try:
                obj = parser(block)
                if isinstance(obj, dict):
                    return obj, True
            except Exception:
                continue

    last = s.splitlines()[-1].strip()
    for parser in (json.loads, ast.literal_eval):
        try:
            obj = parser(last)
            if isinstance(obj, dict):
                return obj, True
        except Exception:
            pass
    return {}, False

def run_wrapper(py_file: str) -> Tuple[bool, Dict[str, Any], str, float]:
    cmd = [sys.executable, str(PROJECT_ROOT / py_file)]
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
    except Exception as e:
        return False, {}, f"Exception on start: {e}", 0.0

    stdout_text, stderr_text = proc.communicate()
    dur = time.perf_counter() - start

    stats, parsed = _parse_stats(stdout_text)
    ok = False
    if parsed:
        status = str(stats.get("status", "")).lower()
        ok = (status == "ok") or (stats.get("success") is True) or (stats.get("ok") is True)
    if not ok:
        ok = (proc.returncode == 0)

    stderr_tail = ""
    if stderr_text:
        lines = stderr_text.strip().splitlines()
        stderr_tail = "\n".join(lines[-80:])[-20000:]

    return ok, (stats if parsed else {}), stderr_tail, dur

# ---- recursive numeric finder ----
_NUM_LIKE = (int, float)
def _first_num(d: Dict[str, Any], keys: List[str]) -> Optional[int]:
    def visit(node: Any) -> Optional[int]:
        if isinstance(node, dict):
            for k in keys:
                if k in node and isinstance(node[k], _NUM_LIKE) and not isinstance(node[k], bool):
                    return int(node[k])
            for v in node.values():
                r = visit(v)
                if r is not None:
                    return r
        elif isinstance(node, (list, tuple)):
            for v in node:
                r = visit(v)
                if r is not None:
                    return r
        return None
    return visit(d)

def extract_before_after_total(stats: Dict[str, Any]) -> Tuple[Optional[int], Optional[int], Optional[int]]:
    before_keys = ["before", "rows_before", "input_count", "start_count", "prev_rows", "prev_total"]
    after_keys  = ["after", "rows_after", "output_count", "end_count", "curr_rows", "curr_total"]
    total_keys  = [
        "total_rows", "rows_total", "photo_rows_total", "price_rows_total",
        "seller_rows_total", "gold_total", "total_typical", "total_standart", "total", "active_now"
    ]
    before = _first_num(stats, before_keys)
    after  = _first_num(stats, after_keys)
    total  = _first_num(stats, total_keys)
    if total is None and after is not None:
        total = after
    return before, after, total

# ------------ PERSIST ------------
def persist_step(engine, run_ts, step, ok, stats, stderr_tail, elapsed_s):
    def pick(d: Dict[str, Any], names: List[str]) -> Optional[int]:
        for n in names:
            v = d.get(n)
            if isinstance(v, _NUM_LIKE) and not isinstance(v, bool):
                return int(v)
        return None
    counts = {
        "rows_in":  pick(stats or {}, ["rows_in", "input_count", "before", "rows_before"]),
        "rows_out": pick(stats or {}, ["rows_out", "output_count", "after", "rows_after"]),
        "inserted": pick(stats or {}, ["inserted", "added", "new_rows", "upserted", "processed", "inserted_new"]),
        "updated":  pick(stats or {}, ["updated", "changed", "updated_changed"]),
        "deleted":  pick(stats or {}, ["deleted", "removed", "archived_today", "archived_on_snapshot"]),
    }
    extra = {k: v for k, v in (stats or {}).items() if k not in {"status", "success", "elapsed_s"}}
    with engine.begin() as conn:
        conn.execute(text(f"""
            INSERT INTO {BI_AUDIT_SCHEMA}.job_runs
            (run_ts, step_name, status, elapsed_s, rows_in, rows_out,
             inserted, updated, deleted, extra_json, stderr_tail)
            VALUES
            (:run_ts, :step, :status, :elapsed_s, :rows_in, :rows_out,
             :inserted, :updated, :deleted, :extra_json, :stderr_tail)
        """), dict(
            run_ts=run_ts,
            step=step,
            status="ok" if ok else "fail",
            elapsed_s=elapsed_s,
            rows_in=counts["rows_in"],
            rows_out=counts["rows_out"],
            inserted=counts["inserted"],
            updated=counts["updated"],
            deleted=counts["deleted"],
            extra_json=json.dumps(extra, ensure_ascii=False) if extra else None,
            stderr_tail=stderr_tail or None,
        ))

# ------------ EMAIL  ------------
def send_email(cfg, subject, body):
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
    except Exception:
        pass

# ------------ MAIN ------------
def main():
    cfg = load_cfg()
    engine = create_engine(build_db_url(cfg), pool_pre_ping=True, future=True)

    ensure_bi_tables(engine)

    run_ts = datetime.now(tz=TZ)
    summaries: List[Dict[str, Any]] = []
    overall_ok = True
    hard_stopped = False

    for i, (path, enabled) in enumerate(STEPS):
        name = _shortname(path)
        if not enabled:
            summaries.append({"name": name, "status": "skip", "before": None, "after": None, "total": None})
            continue

        script_path = PROJECT_ROOT / path
        if not script_path.exists():
            ok, stats, stderr, elapsed = False, {}, f"File not found: {path}", 0.0
        else:
            ok, stats, stderr, elapsed = run_wrapper(path)

        try:
            persist_step(engine, run_ts, path, ok, stats, stderr, elapsed)
        except Exception:
            pass

        before, after, total = extract_before_after_total(stats or {})
        summaries.append({
            "name": name,
            "status": "ok" if ok else "fail",
            "before": before,
            "after": after,
            "total": total
        })

        if not ok and (path not in NON_CRITICAL):
            overall_ok = False
            hard_stopped = True
            for rest_path, rest_enabled in STEPS[i+1:]:
                if rest_enabled:
                    summaries.append({
                        "name": _shortname(rest_path),
                        "status": "skip",
                        "before": None,
                        "after": None,
                        "total": None
                    })
            break

    # ======== Short log ========
    lines = []
    lines.append(f"Reality Pipeline — {run_ts.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    for s in summaries:
        b = "-" if s["before"] is None else f"{s['before']}"
        a = "-" if s["after"]  is None else f"{s['after']}"
        t = "-" if s["total"]  is None else f"{s['total']}"
        lines.append(f"{s['name']}: status={s['status']} | before={b} | after={a} | total={t}")
    lines.append(f"Overall: {'ok' if overall_ok else 'fail'}")
    if hard_stopped:
        lines.append("Stopped: first critical failure triggered early exit")

    text_block = "\n".join(lines)
    sys.stderr.write("\n" + text_block + "\n")

    log_dir = PROJECT_ROOT / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    ts = run_ts.strftime("%Y%m%d_%H%M")
    (log_dir / f"bi_run_{ts}.log").write_text(text_block, encoding="utf-8")
    (log_dir / "last_run.log").write_text(text_block, encoding="utf-8")

    subject = f"[Reality Pipeline] {('OK' if overall_ok else 'FAIL')} — {run_ts.strftime('%Y-%m-%d %H:%M')}"
    send_email(cfg, subject, text_block)

    print(json.dumps({
        "run_ts": run_ts.isoformat(),
        "overall_ok": overall_ok,
        "stopped_early": hard_stopped,
        "summary": summaries
    }, ensure_ascii=False))

    sys.exit(0 if overall_ok else 1)

if __name__ == "__main__":
    main()