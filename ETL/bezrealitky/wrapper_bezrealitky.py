
from __future__ import annotations
import sys, os, subprocess, json, time, re
from pathlib import Path
from datetime import datetime, timezone, timedelta, date
from typing import List, Dict, Any

# === DB (для записи в bezrealitky_stats) ===
from sqlalchemy import create_engine, text

PROJECT_DIR = Path(__file__).resolve().parent
RUNLOG_DIR  = PROJECT_DIR / ".runlogs" / "bezrealitky"
RUNLOG_DIR.mkdir(parents=True, exist_ok=True)

SCRIPTS = [
    "1_step.py",
    "bezrealitky_typical.py",
    "price.py",
    "photo.py",
]

# ---------- thresholds (ENV) ----------
TOL_SMALL            = int(os.getenv("WRAP_TOL_SMALL", "100"))
MAX_ACTIVE_DRIFT     = int(os.getenv("WRAP_MAX_ACTIVE_DRIFT", "5000"))
MAX_BLOWUP           = int(os.getenv("WRAP_MAX_BLOWUP", "30000"))
MAX_PRICE_RATE       = float(os.getenv("WRAP_MAX_PRICE_RATE", "0.25"))
MIN_ROWS_FOR_SIGNAL  = int(os.getenv("WRAP_MIN_ROWS_SIGNAL", "1000"))
REQUIRE_DATES        = os.getenv("WRAP_REQUIRE_DATES", "1") == "1"

# ---------- CONFIG + ENGINE ----------
PROJECT_ROOT = PROJECT_DIR.parent.parent if len(PROJECT_DIR.parents) >= 2 else PROJECT_DIR
cfg_path = PROJECT_ROOT / "config.json"
if not cfg_path.exists():
    alt = PROJECT_DIR / "config.json"
    cfg_path = alt if alt.exists() else cfg_path
cfg = json.loads(cfg_path.read_text(encoding="utf-8"))

USER = cfg.get("USER") or cfg.get("user")
PWD  = cfg.get("PWD")  or cfg.get("password")
HOST = cfg.get("HOST") or cfg.get("host")
PORT = cfg.get("PORT") or cfg.get("port")
DB   = cfg.get("DB")   or cfg.get("dbname")

def _make_db_url() -> str:
    try:
        import psycopg  # noqa
        return f"postgresql+psycopg://{USER}:{PWD}@{HOST}:{PORT}/{DB}"
    except ModuleNotFoundError:
        import psycopg2  # noqa
        return f"postgresql+psycopg2://{USER}:{PWD}@{HOST}:{PORT}/{DB}"

ENGINE = create_engine(_make_db_url(), pool_pre_ping=True, connect_args={"connect_timeout": 10})

# ---------- time helpers ----------
def now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat()

def today_date() -> date:
    return datetime.now().date()

def yesterday_date() -> date:
    return (datetime.now() - timedelta(days=1)).date()

def _today_key() -> str:
    return today_date().strftime("%Y-%m-%d")

def _yesterday_key() -> str:
    return yesterday_date().strftime("%Y-%m-%d")

# ---------- io ----------
def _save_daily_runlog(day_key: str, payload: dict) -> None:
    (RUNLOG_DIR / f"{day_key}.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )

def _load_daily_runlog(day_key: str) -> dict | None:
    p = RUNLOG_DIR / f"{day_key}.json"
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None

# ---------- stdout parsing ----------
def _parse_sync_summary(stdout: str) -> list[dict]:
    out = []
    for line in stdout.splitlines():
        line = line.strip()
        if not line or line[0] not in "{[":
            continue
        try:
            obj = json.loads(line)
            if isinstance(obj, dict) and obj.get("stage") == "sync_summary" and "stats" in obj:
                out.append(obj)
        except Exception:
            continue
    return out

def run_script(script_path: Path) -> dict:
    start = time.perf_counter()
    env = os.environ.copy()
    env.setdefault("PYTHONUNBUFFERED", "1")

    proc = subprocess.run(
        [sys.executable, str(script_path)],
        cwd=script_path.parent,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    dur_ms = int((time.perf_counter() - start) * 1000)
    sync_summaries = _parse_sync_summary(proc.stdout)

    return {
        "script": script_path.name,
        "returncode": proc.returncode,
        "duration_ms": dur_ms,
        "stdout_bytes": len(proc.stdout.encode("utf-8", errors="ignore")),
        "stderr_bytes": len(proc.stderr.encode("utf-8", errors="ignore")),
        "stdout": proc.stdout,
        "stderr": proc.stderr,
        "sync_summaries": sync_summaries,
    }

# ---------- stats extraction ----------
def _to_int(val, default=None):
    try:
        return int(val) if val is not None else default
    except Exception:
        return default

def _extract_counts(all_results: list[dict]) -> dict:
    out = {
        "standart": {
            "total_rows": None, "snapshot_date": None, "curr_table": None, "prev_table": None,
            "added_today": None, "archived_today": None, "active_now": None,
            "phase_counts": {}  # ожидаем added/reactivated/archived/source_rows
        },
        "typical": {
            "total_rows": None, "added_today": None, "archived_today": None, "active_now": None,
            "snapshot_date": None, "curr_table": None, "prev_table": None,
            "phase_counts": {}  # ожидаем inserted/updated/archived/standart_rows/typical_rows_before
        },
        "price":    {},   # prev_table, curr_table, change_date, candidates, inserted_new, prev_rows, curr_rows, price_rows_total
        "photo":    {},   # snapshot_table, source_rows, inserted_new, photo_rows_total (+ snapshot_date)
    }

    for res in all_results:
        name = res.get("script", "").lower()
        summaries = res.get("sync_summaries") or []
        summary = summaries[-1] if summaries else None
        stats = (summary or {}).get("stats") or {}
        if not stats:
            continue

        if "typical" in name:
            for k in ("total_rows","added_today","archived_today","active_now"):
                out["typical"][k] = _to_int(stats.get(k), out["typical"].get(k))
            for k in ("snapshot_date","curr_table","prev_table"):
                if stats.get(k): out["typical"][k] = stats.get(k)
            if isinstance(stats.get("phase_counts"), dict):
                out["typical"]["phase_counts"] = stats["phase_counts"]

        elif "1_step" in name or "standart" in name:
            for k in ("total_rows","added_today","archived_today","active_now"):
                if stats.get(k) is not None:
                    out["standart"][k] = int(stats.get(k))
            for k in ("snapshot_date","curr_table","prev_table"):
                if stats.get(k): out["standart"][k] = stats.get(k)
            if isinstance(stats.get("phase_counts"), dict):
                out["standart"]["phase_counts"] = stats["phase_counts"]

        elif "price" in name:
            out["price"] = {k: v for k, v in stats.items()}

        elif "photo" in name:
            out["photo"] = {k: v for k, v in stats.items()}

    return out

# ---------- table-name → date ----------
_BZ_RE    = re.compile(r"^bzereality_(\d{2})(\d{2})(\d{4})$")
_IDNES_RE = re.compile(r"^idnes_(\d{2})(\d{2})(\d{4})$")

def _table_to_date(tbl_name: str) -> date | None:
    for rx in (_BZ_RE, _IDNES_RE):
        m = rx.match(tbl_name or "")
        if m:
            dd, mm, yyyy = m.groups()
            try:
                return date(int(yyyy), int(mm), int(dd))
            except Exception:
                return None
    return None

def _parse_yyyy_mm_dd(s: str) -> date | None:
    try:
        y, m, d = s.split("-")
        return date(int(y), int(m), int(d))
    except Exception:
        return None

# ---------- sanity: math ----------
def _sanity_compare(today: dict, yesterday: dict | None) -> tuple[bool, list[str]]:
    problems: list[str] = []
    def pos(name: str, val):
        if val is None: return
        if isinstance(val, (int, float)) and val < 0:
            problems.append(f"{name} is negative: {val}")

    typ_t = today.get("typical") or {}
    typ_y = (yesterday or {}).get("typical") or {}
    for f in ("added_today","archived_today","active_now"):
        pos(f"typical.{f}", typ_t.get(f))
    if typ_t.get("total_rows") is not None and typ_y.get("total_rows") is not None:
        if int(typ_t["total_rows"]) < int(typ_y["total_rows"]):
            problems.append(f"typical.total_rows decreased: {typ_t['total_rows']} < {typ_y['total_rows']}")

    std_t = today.get("standart") or {}
    std_y = (yesterday or {}).get("standart") or {}
    for f in ("added_today","archived_today","active_now"):
        pos(f"standart.{f}", std_t.get(f))
    if std_t.get("total_rows") is not None and std_y.get("total_rows") is not None:
        if int(std_t["total_rows"]) < int(std_y["total_rows"]):
            problems.append(f"standart.total_rows decreased: {std_t['total_rows']} < {std_y['total_rows']}")

    return (len(problems) == 0, problems)

# ---------- sanity: mass balance ----------
def _mass_balance_checks(today: dict, yesterday: dict | None) -> tuple[bool, list[str]]:
    problems: list[str] = []
    ok = True

    def check_block(block_name: str):
        nonlocal ok
        t = (today.get(block_name) or {})
        y = ((yesterday or {}).get(block_name) or {})
        t_total, y_total, added = t.get("total_rows"), y.get("total_rows"), t.get("added_today")
        if t_total is None or y_total is None or added is None:
            return
        expected = int(y_total) + int(added)
        got = int(t_total)
        if got != expected:
            ok = False
            delta = got - expected
            if delta > 0:
                problems.append(f"{block_name}: total_rows={got} > expected={expected} (+{delta}) — возможны дубликаты")
            else:
                problems.append(f"{block_name}: total_rows={got} < expected={expected} ({delta}) — недовставили данные")

    check_block("standart")
    check_block("typical")
    return ok, problems

# ---------- sanity: dates ----------
def _collect_table_fields(stats: dict) -> list[tuple[str, str]]:
    out = []
    for key in ("prev_table", "curr_table", "snapshot_table"):
        v = stats.get(key)
        if isinstance(v, str) and v:
            out.append((key, v))
    return out

def _sanity_dates_all_scripts(results: list[dict]) -> tuple[bool, list[str], dict]:
    probs: list[str] = []
    tdy = today_date()
    yst = yesterday_date()

    snapshot_dump = {
        "expected_today": tdy.isoformat(),
        "expected_yesterday": yst.isoformat(),
        "by_script": []
    }

    for r in results:
        script = r.get("script", "")
        summaries = r.get("sync_summaries") or []
        if not summaries:
            continue
        stats = summaries[-1].get("stats") or {}
        pairs = _collect_table_fields(stats)
        if not pairs and "snapshot_date" not in stats:
            continue

        entry = {"script": script, "tables": []}
        for key, tbl in pairs:
            d = _table_to_date(tbl)
            entry["tables"].append({"key": key, "table": tbl, "parsed_date": (d.isoformat() if d else None)})

            if d is None:
                probs.append(f"{script}: {key}={tbl} — can't parse date")
                continue
            if key == "prev_table" and d != yst:
                probs.append(f"{script}: prev_table {tbl} != yesterday ({yst})")
            if key in ("curr_table", "snapshot_table") and d != tdy:
                probs.append(f"{script}: {key} {tbl} != today ({tdy})")

        sdate = stats.get("snapshot_date")
        if isinstance(sdate, str):
            sd = _parse_yyyy_mm_dd(sdate)
            if not sd or sd != tdy:
                probs.append(f"{script}: snapshot_date {sdate} != today ({tdy})")

        snapshot_dump["by_script"].append(entry)

    return (len(probs) == 0, probs, snapshot_dump)

# ---------- sanity: cross ----------
def _sanity_cross(today: dict, yesterday: dict | None) -> tuple[bool, list[str], list[str]]:
    probs: list[str] = []
    warns: list[str] = []

    def gi(*path):
        cur = today
        try:
            for k in path: cur = cur.get(k, {})
            return int(cur) if cur not in (None, {}) else None
        except Exception:
            return None

    def gi_y(*path):
        cur = yesterday or {}
        try:
            for k in path: cur = cur.get(k, {})
            return int(cur) if cur not in (None, {}) else None
        except Exception:
            return None

    ty_add, st_add = gi("typical","added_today"), gi("standart","added_today")
    ty_arc, st_arc = gi("typical","archived_today"), gi("standart","archived_today")
    if ty_add is not None and st_add is not None and abs(ty_add - st_add) > TOL_SMALL:
        warns.append(f"added_today mismatch typ({ty_add}) vs st({st_add}) > {TOL_SMALL}")
    if ty_arc is not None and st_arc is not None and abs(ty_arc - st_arc) > TOL_SMALL:
        warns.append(f"archived_today mismatch typ({ty_arc}) vs st({st_arc}) > {TOL_SMALL}")

    ty_act, ty_act_y = gi("typical","active_now"), gi_y("typical","active_now")
    if ty_act is not None and ty_act_y is not None and abs(ty_act - ty_act_y) > MAX_ACTIVE_DRIFT:
        warns.append(f"active_now drift {abs(ty_act - ty_act_y)} > {MAX_ACTIVE_DRIFT}")

    st_tot, st_tot_y = gi("standart","total_rows"), gi_y("standart","total_rows")
    if st_tot is not None and st_tot_y is not None and (st_tot - st_tot_y) > MAX_BLOWUP:
        warns.append(f"standart.total_rows grew by {st_tot - st_tot_y} > {MAX_BLOWUP}")

    ty_tot = gi("typical","total_rows")
    if st_tot is not None and ty_tot is not None and abs(st_tot - ty_tot) > MAX_BLOWUP:
        warns.append(f"typical.total_rows({ty_tot}) vs standart.total_rows({st_tot}) differ by > {MAX_BLOWUP}")

    return (len(probs) == 0, probs, warns)

# ---------- sanity: extra ----------
def _sanity_extra(today: dict, yesterday: dict | None) -> tuple[bool, list[str], list[str]]:
    probs: list[str] = []
    warns: list[str] = []

    def gi(*p):
        cur = today
        try:
            for k in p: cur = cur.get(k, {})
            return int(cur) if cur not in (None, {}) else None
        except Exception: return None

    pr_cand = gi("price","candidates")
    pr_prev = gi("price","prev_rows")
    pr_curr = gi("price","curr_rows")
    if pr_cand == 0 and (pr_prev or 0) > MIN_ROWS_FOR_SIGNAL and (pr_curr or 0) > MIN_ROWS_FOR_SIGNAL:
        warns.append("price.candidates=0 при больших prev/curr_rows")

    pr_ins = gi("price","inserted_new")
    if pr_ins is not None and pr_curr not in (None, 0):
        rate = pr_ins / max(pr_curr, 1)
        if rate > MAX_PRICE_RATE:
            warns.append(f"price.inserted_new ratio {rate:.2%} > {MAX_PRICE_RATE:.0%}")

    ph_src = gi("photo","source_rows")
    ph_new = gi("photo","inserted_new")
    if ph_src is not None and ph_new is not None and ph_new > ph_src:
        probs.append(f"photo.inserted_new({ph_new}) > source_rows({ph_src})")

    st_snap = (today.get("standart") or {}).get("snapshot_date")
    pr_chg  = (today.get("price") or {}).get("change_date")
    if st_snap and pr_chg and st_snap != pr_chg:
        warns.append(f"snapshot_date({st_snap}) != change_date({pr_chg})")

    return (len(probs) == 0, probs, warns)

# === BI STATS: схема таблицы и запись ===
_CREATE_STATS_TABLE = """
CREATE TABLE IF NOT EXISTS public.bezrealitky_stats (
  id bigserial PRIMARY KEY,
  run_ts timestamptz NOT NULL DEFAULT now(),
  run_date date NOT NULL,
  module text NOT NULL,
  snapshot_date date,
  step1_added int,
  step1_reactivated int,
  step1_archived int,
  typical_inserted int,
  typical_updated int,
  typical_archived int,
  price_candidates int,
  price_inserted int,
  photo_inserted int,
  total_standart int,
  total_typical int,
  status text,
  note text,
  raw jsonb
);
"""

def _ensure_stats_table():
    with ENGINE.begin() as conn:
        conn.execute(text(_CREATE_STATS_TABLE))

def _derive_step_maths(today_stats: dict) -> dict:
    # 1_step: берём то, что модуль 1_step прислал в phase_counts
    st_phase = (today_stats.get("standart") or {}).get("phase_counts") or {}
    step1_added        = _to_int(st_phase.get("added"), 0)
    step1_reactivated  = _to_int(st_phase.get("reactivated"), 0)
    step1_archived     = _to_int(st_phase.get("archived"), 0)

    typ_phase = (today_stats.get("typical") or {}).get("phase_counts") or {}
    typ_inserted = _to_int(typ_phase.get("inserted"), None)
    typ_updated  = _to_int(typ_phase.get("updated"), None)
    typ_archived = _to_int(typ_phase.get("archived"), None)

    if typ_inserted is None:
        typ_inserted = _to_int((today_stats.get("typical") or {}).get("added_today"), 0)
    if typ_archived is None:
        typ_archived = _to_int((today_stats.get("typical") or {}).get("archived_today"), 0)

    # price/photo
    price = today_stats.get("price") or {}
    photo = today_stats.get("photo") or {}
    price_candidates = _to_int(price.get("candidates"), 0)
    price_inserted   = _to_int(price.get("inserted_new"), 0)
    photo_inserted   = _to_int(photo.get("inserted_new"), 0)

    return {
        "step1_added": step1_added,
        "step1_reactivated": step1_reactivated,
        "step1_archived": step1_archived,
        "typical_inserted": typ_inserted,
        "typical_updated": typ_updated,
        "typical_archived": typ_archived,
        "price_candidates": price_candidates,
        "price_inserted": price_inserted,
        "photo_inserted": photo_inserted,
    }

def _insert_stats_row(module: str, status: str, note: str | None, today_stats: dict):
    _ensure_stats_table()
    math = _derive_step_maths(today_stats)

    st  = today_stats.get("standart") or {}
    typ = today_stats.get("typical") or {}
    snapshot_date = st.get("snapshot_date") or typ.get("snapshot_date")

    payload = {
        "run_date": _today_key(),                         # 'YYYY-MM-DD'
        "module": module,
        "snapshot_date": snapshot_date,                   # может быть None
        "step1_added": math["step1_added"],
        "step1_reactivated": math["step1_reactivated"],
        "step1_archived": math["step1_archived"],
        "typical_inserted": math["typical_inserted"],
        "typical_updated": math["typical_updated"],
        "typical_archived": math["typical_archived"],
        "price_candidates": math["price_candidates"],
        "price_inserted": math["price_inserted"],
        "photo_inserted": math["photo_inserted"],
        "total_standart": _to_int(st.get("total_rows")),
        "total_typical": _to_int(typ.get("total_rows")),
        "status": status,
        "note": note,
        "raw": json.dumps(today_stats, ensure_ascii=False),  # строка JSON
    }

    sql = text("""
        INSERT INTO public.bezrealitky_stats (
          run_date, module, snapshot_date,
          step1_added, step1_reactivated, step1_archived,
          typical_inserted, typical_updated, typical_archived,
          price_candidates, price_inserted, photo_inserted,
          total_standart, total_typical, status, note, raw
        )
        VALUES (
          CAST(:run_date AS date), :module, CAST(:snapshot_date AS date),
          :step1_added, :step1_reactivated, :step1_archived,
          :typical_inserted, :typical_updated, :typical_archived,
          :price_candidates, :price_inserted, :photo_inserted,
          :total_standart, :total_typical, :status, :note, CAST(:raw AS jsonb)
        )
    """)
    with ENGINE.begin() as conn:
        conn.execute(sql, payload)

# ---------- main ----------
def main():
    started = now_iso()
    print(f"[wrapper] start {started}")

    results: List[Dict[str, Any]] = []
    overall_ok = True

    for name in SCRIPTS:
        path = PROJECT_DIR / name
        if not path.exists():
            res = {
                "script": name,
                "returncode": 127,
                "duration_ms": 0,
                "stdout_bytes": 0,
                "stderr_bytes": 0,
                "error": "not_found",
                "stdout": "",
                "stderr": "",
                "sync_summaries": [],
            }
            results.append(res)
            overall_ok = False
            print(f"[wrapper] missing: {name}")
            continue

        print(f"[wrapper] run: {name}")
        res = run_script(path)
        results.append(res)
        if res["returncode"] != 0:
            overall_ok = False
            print(f"[wrapper] fail: {name} rc={res['returncode']} in {res['duration_ms']} ms")
        else:
            print(f"[wrapper] ok:   {name} in {res['duration_ms']} ms")

    finished = now_iso()

    today_stats = _extract_counts(results)

    today_key = _today_key()
    yest_key  = _yesterday_key()
    yesterday_log = _load_daily_runlog(yest_key)
    yesterday_stats = (yesterday_log or {}).get("stats") or {}

    math_ok, math_problems               = _sanity_compare(today_stats, yesterday_stats)
    mass_ok, mass_problems               = _mass_balance_checks(today_stats, yesterday_stats)
    dates_ok, date_problems, dates_snap  = _sanity_dates_all_scripts(results)
    cross_ok, cross_problems, cross_warn = _sanity_cross(today_stats, yesterday_stats)
    extra_ok, extra_problems, extra_warn = _sanity_extra(today_stats, yesterday_stats)

    overall_ok = overall_ok and math_ok and dates_ok and mass_ok and cross_ok and extra_ok

    daily_payload = {
        "date": today_key,
        "started": started,
        "finished": finished,
        "stats": today_stats,
        "dates_snapshot": dates_snap,
        "scripts": [
            {k: v for k, v in r.items() if k not in ("stdout", "stderr")}
            for r in results
        ],
        "math_ok": math_ok,   "math_problems": math_problems,
        "mass_ok": mass_ok,   "mass_problems": mass_problems,
        "dates_ok": dates_ok, "date_problems": date_problems,
        "cross_ok": cross_ok, "cross_problems": cross_problems, "cross_warnings": cross_warn,
        "extra_ok": extra_ok, "extra_problems": extra_problems, "extra_warnings": extra_warn,
    }
    _save_daily_runlog(today_key, daily_payload)

    note_parts = []
    if not math_ok:   note_parts.append("; ".join(math_problems))
    if not mass_ok:   note_parts.append("; ".join(mass_problems))
    if not dates_ok:  note_parts.append("; ".join(date_problems))
    if not cross_ok:  note_parts.append("; ".join(cross_problems))
    if not extra_ok:  note_parts.append("; ".join(extra_problems))
    if cross_warn:    note_parts.append("WARN: " + "; ".join(cross_warn[:3]))
    if extra_warn:    note_parts.append("WARN+: " + "; ".join(extra_warn[:3]))
    note = ("; ".join(note_parts)[:300]) if note_parts else None

    short = {
        "module": "bezrealitky_wrapper",
        "status": "ok" if overall_ok else "error",
        "added_today":   today_stats.get("typical", {}).get("added_today"),
        "archived_today":today_stats.get("typical", {}).get("archived_today"),
        "total_standart":today_stats.get("standart", {}).get("total_rows"),
        "total_typical": today_stats.get("typical", {}).get("total_rows"),
        "dates_ok": dates_ok,
        "mass_ok": mass_ok,
        "note": note,
    }
    print(json.dumps(short, ensure_ascii=False, separators=(",", ":")))

    # === BI STATS запись в БД ===
    _insert_stats_row(
        module="bezrealitky_pipeline",
        status=("ok" if overall_ok else "error"),
        note=note,
        today_stats=today_stats
    )

    summary = {
        "module": "bezrealitky_pipeline",  # как у тебя было
        "status": "ok" if overall_ok else "error",
        "started": started,
        "finished": finished,
        "duration_ms": sum(r.get("duration_ms", 0) for r in results),
        "ran": [r["script"] for r in results],
        "results": [
            {k: v for k, v in r.items() if k not in ("stdout", "stderr")}
            for r in results
        ],
        "today_stats": today_stats,
        "dates_snapshot": dates_snap,
        "math_ok": math_ok,   "math_problems": math_problems,
        "mass_ok": mass_ok,   "mass_problems": mass_problems,
        "dates_ok": dates_ok, "date_problems": date_problems,
        "cross_ok": cross_ok, "cross_problems": cross_problems, "cross_warnings": cross_warn,
        "extra_ok": extra_ok, "extra_problems": extra_problems, "extra_warnings": extra_warn,
        "yesterday_key": yest_key if yesterday_log else None
    }
    print(json.dumps(summary, ensure_ascii=False, separators=(",", ":")))

    sys.exit(0 if overall_ok else 1)

if __name__ == "__main__":
    main()
