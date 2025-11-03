
from __future__ import annotations
import sys, os, subprocess, json, time, re
from pathlib import Path
from datetime import datetime, timezone, timedelta, date
from typing import Any, Dict, List

# +++ DB +++
from sqlalchemy import create_engine, text

PROJECT_DIR = Path(__file__).resolve().parent
RUNLOG_DIR  = PROJECT_DIR / ".runlogs" / "idnes"
RUNLOG_DIR.mkdir(parents=True, exist_ok=True)

SCRIPTS = [
    "1_step.py",
    "idnes_typical.py",  # sync_summary (typical + phase_counts: inserted/updated/archived[/dedup_removed]/typical_rows_before)
    "seller.py",         # sync_summary (seller + source_rows/inserted_new)
    "price.py",          # sync_summary (price + prev_table/curr_table/change_date/candidates/inserted_new/prev_rows/curr_rows)
]

# ---------- thresholds (ENV) ----------
TOL_SMALL            = int(os.getenv("WRAP_TOL_SMALL", "100"))
MAX_ACTIVE_DRIFT     = int(os.getenv("WRAP_MAX_ACTIVE_DRIFT", "5000"))
MAX_PRICE_RATE       = float(os.getenv("WRAP_MAX_PRICE_RATE", "0.25"))
MAX_BLOWUP           = int(os.getenv("WRAP_MAX_BLOWUP", "30000"))
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
    out: List[dict] = []
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

    print(f"[wrapper] run: {script_path.name}")
    if proc.returncode == 0:
        print(f"[wrapper] ok:   {script_path.name} in {dur_ms} ms")
    else:
        print(f"[wrapper] fail: {script_path.name} rc={proc.returncode} in {dur_ms} ms")

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
        "standart": {"total_rows": None, "snapshot_date": None, "curr_table": None, "prev_table": None,
                     "added_today": None, "archived_today": None, "active_now": None, "phase_counts": {}},
        "typical":  {"total_rows": None, "added_today": None, "archived_today": None, "active_now": None,
                     "snapshot_date": None, "curr_table": None, "prev_table": None, "phase_counts": {}},
        "price":    {},
        "seller":   {},
    }

    for res in all_results:
        name = res.get("script", "").lower()
        summaries = res.get("sync_summaries") or []
        summary = summaries[-1] if summaries else None
        stats = (summary or {}).get("stats") or {}
        if not stats:
            continue

        if "idnes_typical" in name or "typical.py" in name:
            for k in ("total_rows","added_today","archived_today","active_now","snapshot_date","curr_table","prev_table"):
                if k in stats and stats.get(k) is not None:
                    out["typical"][k] = stats.get(k) if isinstance(stats.get(k), str) else _to_int(stats.get(k), out["typical"].get(k))
            if isinstance(stats.get("phase_counts"), dict):
                out["typical"]["phase_counts"] = stats["phase_counts"]

        elif "1_step.py" in name or "standart" in name:
            for k in ("total_rows","added_today","archived_today","active_now","snapshot_date","curr_table","prev_table"):
                if k in stats and stats.get(k) is not None:
                    out["standart"][k] = stats.get(k) if isinstance(stats.get(k), str) else _to_int(stats.get(k), out["standart"].get(k))
            if isinstance(stats.get("phase_counts"), dict):
                out["standart"]["phase_counts"] = stats["phase_counts"]

        elif "price" in name:
            out["price"] = {k: v for k, v in stats.items()}

        elif "seller" in name:
            out["seller"] = {k: v for k, v in stats.items()}

    return out

# ---------- date helpers ----------
_IDNES_RE = re.compile(r"^idnes_(\d{2})(\d{2})(\d{4})$")  # DDMMYYYY

def _table_to_date_idnes(tbl_name: str) -> date | None:
    m = _IDNES_RE.match(tbl_name or "")
    if not m:
        return None
    dd, mm, yyyy = m.groups()
    try:
        return date(int(yyyy), int(mm), int(dd))
    except Exception:
        return None

def _parse_yyyy_mm_dd(s: str) -> date | None:
    try:
        y, m, d = s.split("-")
        return date(int(y), int(m), int(d))
    except Exception:
        return None

# ---------- sanity ----------
def _sanity_compare(today: dict, yesterday: dict | None) -> tuple[bool, list[str]]:
    problems: list[str] = []

    typ = today.get("typical") or {}
    pc  = (typ.get("phase_counts") or {})
    before   = _to_int(pc.get("typical_rows_before"))
    inserted = _to_int(pc.get("inserted"), _to_int(typ.get("added_today"), 0))
    archived = _to_int(pc.get("archived"), _to_int(typ.get("archived_today"), 0))
    dedup    = _to_int(pc.get("dedup_removed"), 0)
    total    = _to_int(typ.get("total_rows"))

    if None not in (before, inserted, archived, total):
        expected = before + inserted - archived - (dedup or 0)
        if total != expected:
            delta = total - expected
            problems.append(f"typical.total_rows={total} vs expected={expected} ({delta})")
    else:
        typ_y = (yesterday or {}).get("typical") or {}
        if typ_y.get("total_rows") is not None and typ.get("total_rows") is not None and typ.get("added_today") is not None:
            ytot = int(typ_y["total_rows"])
            add  = int(typ["added_today"])
            exp  = ytot + add
            if int(typ["total_rows"]) != exp:
                problems.append(f"typical.total_rows decreased: {typ['total_rows']} < {ytot}")

    std = today.get("standart") or {}
    std_y = (yesterday or {}).get("standart") or {}
    if std.get("total_rows") is not None and std_y.get("total_rows") is not None and std.get("added_today") is not None:
        exp = int(std_y["total_rows"]) + int(std["added_today"])
        if int(std["total_rows"]) != exp:
            delta = int(std["total_rows"]) - exp
            problems.append(f"standart.total_rows={std['total_rows']} vs expected={exp} ({delta})")

    return (len(problems) == 0, problems)

# ---------- sanity: массовый баланс (внутренняя проверка по фазам) ----------
def _mass_balance_checks(today: dict, yesterday: dict | None) -> tuple[bool, list[str]]:
    problems: list[str] = []
    ok = True


    st  = today.get("standart") or {}
    sty = (yesterday or {}).get("standart") or {}
    if None not in (st.get("total_rows"), sty.get("total_rows"), st.get("added_today")):
        exp = int(sty["total_rows"]) + int(st["added_today"])
        got = int(st["total_rows"])
        if got != exp:
            ok = False
            delta = got - exp
            problems.append(f"standart: total_rows={got} != expected={exp} ({delta})")

    typ = today.get("typical") or {}
    pc  = (typ.get("phase_counts") or {})
    before   = _to_int(pc.get("typical_rows_before"))
    inserted = _to_int(pc.get("inserted"), _to_int(typ.get("added_today"), 0))
    archived = _to_int(pc.get("archived"), _to_int(typ.get("archived_today"), 0))
    dedup    = _to_int(pc.get("dedup_removed"), 0)
    total    = _to_int(typ.get("total_rows"))

    if None not in (before, inserted, archived, total):
        exp = before + inserted - archived - (dedup or 0)
        if total != exp:
            ok = False
            delta = total - exp
            if delta > 0:
                problems.append(f"typical: total_rows={total} > expected={exp} (+{delta}) — дубликаты?")
            else:
                problems.append(f"typical: total_rows={total} < expected={exp} ({delta}) — недовставили")
    return ok, problems

# ---------- sanity ----------
def _sanity_dates(results: list[dict]) -> tuple[bool, list[str], dict]:
    probs: list[str] = []
    snapshot = {
        "expected_today": today_date().isoformat(),
        "expected_yesterday": yesterday_date().isoformat(),
        "by_script": []
    }

    def last_stats(substr: str) -> dict:
        for r in results:
            if substr in (r.get("script","").lower()):
                summ = (r.get("sync_summaries") or [])
                if summ:
                    return summ[-1].get("stats") or {}
        return {}

    price_stats = last_stats("price.py")
    price_info = {"script":"price.py","tables":[]}
    prev_tbl = price_stats.get("prev_table")
    curr_tbl = price_stats.get("curr_table")
    if prev_tbl:
        dprev = _table_to_date_idnes(prev_tbl)
        price_info["tables"].append({"key":"prev_table","table":prev_tbl,"parsed_date": (dprev.isoformat() if dprev else None)})
        if not dprev or dprev != yesterday_date():
            probs.append(f"price.prev_table not yesterday: {prev_tbl}")
    elif REQUIRE_DATES:
        probs.append("price.prev_table missing")
    if curr_tbl:
        dcurr = _table_to_date_idnes(curr_tbl)
        price_info["tables"].append({"key":"curr_table","table":curr_tbl,"parsed_date": (dcurr.isoformat() if dcurr else None)})
        if not dcurr or dcurr != today_date():
            probs.append(f"price.curr_table not today: {curr_tbl}")
    elif REQUIRE_DATES:
        probs.append("price.curr_table missing")
    snapshot["by_script"].append(price_info)

    step_stats = last_stats("1_step.py")
    step_info = {"script":"1_step.py","fields":[]}
    snap_date = step_stats.get("snapshot_date")
    if snap_date:
        parsed = _parse_yyyy_mm_dd(snap_date)
        step_info["fields"].append({"key":"snapshot_date","value":snap_date,"parsed_date": (parsed.isoformat() if parsed else None)})
        if not parsed or parsed != today_date():
            probs.append(f"1_step.snapshot_date not today: {snap_date}")
    elif REQUIRE_DATES:
        step_info["fields"].append({"key":"snapshot_date","value":None,"parsed_date":None})
        probs.append("1_step.snapshot_date missing")
    snapshot["by_script"].append(step_info)

    return (len(probs) == 0, probs, snapshot)

# ---------- sanity ----------
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

    return (len(probs) == 0, probs, warns)

# ---------- sanity ----------
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

    sl_src = gi("seller","source_rows")
    sl_new = gi("seller","inserted_new")
    if sl_src is not None and sl_new is not None and sl_new > sl_src:
        probs.append(f"seller.inserted_new({sl_new}) > source_rows({sl_src})")

    st_snap = (today.get("standart") or {}).get("snapshot_date")
    pr_chg  = (today.get("price") or {}).get("change_date")
    if st_snap and pr_chg and st_snap != pr_chg:
        warns.append(f"snapshot_date({st_snap}) != change_date({pr_chg})")

    return (len(probs) == 0, probs, warns)

# ---------- BI stats: таблица и UPSERT ----------
_CREATE_STATS = """
CREATE TABLE IF NOT EXISTS public.idnes_stats (
  id bigserial PRIMARY KEY,
  run_ts timestamptz NOT NULL DEFAULT now(),
  run_date date NOT NULL,
  module text NOT NULL,
  snapshot_date date,
  -- 1_step (standart)
  step1_source_rows int,
  step1_added int,
  step1_reactivated int,
  step1_archived int,
  total_standart int,
  -- typical
  typical_rows_before int,
  typical_inserted int,
  typical_updated int,
  typical_archived int,
  total_typical int,
  -- seller
  seller_source_rows int,
  seller_inserted int,
  seller_rows_total int,
  -- price
  price_prev_rows int,
  price_curr_rows int,
  price_candidates int,
  price_inserted int,
  price_rows_total int,
  status text,
  note text,
  raw jsonb
);
"""
def _ensure_stats_table():
    with ENGINE.begin() as conn:
        conn.execute(text(_CREATE_STATS))
        conn.execute(text("""
            CREATE UNIQUE INDEX IF NOT EXISTS ux_idnes_stats_day_module
            ON public.idnes_stats (run_date, module)
        """))

def _derive_numbers(today_stats: dict) -> dict:
    st  = today_stats.get("standart") or {}
    typ = today_stats.get("typical") or {}
    sel = today_stats.get("seller") or {}
    pr  = today_stats.get("price") or {}

    st_phase  = st.get("phase_counts") or {}
    typ_phase = typ.get("phase_counts") or {}

    return {
        "snapshot_date": (st.get("snapshot_date") or typ.get("snapshot_date")),
        "step1_source_rows": _to_int(st_phase.get("source_rows"), None),
        "step1_added": _to_int(st_phase.get("added"), 0),
        "step1_reactivated": _to_int(st_phase.get("reactivated"), 0),
        "step1_archived": _to_int(st_phase.get("archived"), 0),
        "total_standart": _to_int(st.get("total_rows"), None),

        "typical_rows_before": _to_int(typ_phase.get("typical_rows_before"), None),
        "typical_inserted": _to_int(typ_phase.get("inserted"), _to_int(typ.get("added_today"), 0)),
        "typical_updated": _to_int(typ_phase.get("updated"), None),
        "typical_archived": _to_int(typ_phase.get("archived"), _to_int(typ.get("archived_today"), 0)),
        "total_typical": _to_int(typ.get("total_rows"), None),

        "seller_source_rows": _to_int(sel.get("source_rows"), None),
        "seller_inserted": _to_int(sel.get("inserted_new"), 0),
        "seller_rows_total": _to_int(sel.get("seller_rows_total"), None),

        "price_prev_rows": _to_int(pr.get("prev_rows"), None),
        "price_curr_rows": _to_int(pr.get("curr_rows"), None),
        "price_candidates": _to_int(pr.get("candidates"), 0),
        "price_inserted": _to_int(pr.get("inserted_new"), 0),
        "price_rows_total": _to_int(pr.get("price_rows_total"), None),
    }

def _upsert_stats(module: str, status: str, note: str | None, today_stats: dict) -> None:
    _ensure_stats_table()
    nums = _derive_numbers(today_stats)
    payload = {
        "run_date": _today_key(),
        "module": module,
        "snapshot_date": nums["snapshot_date"],
        **nums,
        "status": status,
        "note": note,
        "raw": json.dumps(today_stats, ensure_ascii=False)
    }
    sql = text("""
        INSERT INTO public.idnes_stats (
          run_date, module, snapshot_date,
          step1_source_rows, step1_added, step1_reactivated, step1_archived, total_standart,
          typical_rows_before, typical_inserted, typical_updated, typical_archived, total_typical,
          seller_source_rows, seller_inserted, seller_rows_total,
          price_prev_rows, price_curr_rows, price_candidates, price_inserted, price_rows_total,
          status, note, raw
        )
        VALUES (
          CAST(:run_date AS date), :module, CAST(:snapshot_date AS date),
          :step1_source_rows, :step1_added, :step1_reactivated, :step1_archived, :total_standart,
          :typical_rows_before, :typical_inserted, :typical_updated, :typical_archived, :total_typical,
          :seller_source_rows, :seller_inserted, :seller_rows_total,
          :price_prev_rows, :price_curr_rows, :price_candidates, :price_inserted, :price_rows_total,
          :status, :note, CAST(:raw AS jsonb)
        )
        ON CONFLICT (run_date, module) DO UPDATE SET
          snapshot_date       = EXCLUDED.snapshot_date,
          step1_source_rows   = EXCLUDED.step1_source_rows,
          step1_added         = EXCLUDED.step1_added,
          step1_reactivated   = EXCLUDED.step1_reactivated,
          step1_archived      = EXCLUDED.step1_archived,
          total_standart      = EXCLUDED.total_standart,
          typical_rows_before = EXCLUDED.typical_rows_before,
          typical_inserted    = EXCLUDED.typical_inserted,
          typical_updated     = EXCLUDED.typical_updated,
          typical_archived    = EXCLUDED.typical_archived,
          total_typical       = EXCLUDED.total_typical,
          seller_source_rows  = EXCLUDED.seller_source_rows,
          seller_inserted     = EXCLUDED.seller_inserted,
          seller_rows_total   = EXCLUDED.seller_rows_total,
          price_prev_rows     = EXCLUDED.price_prev_rows,
          price_curr_rows     = EXCLUDED.price_curr_rows,
          price_candidates    = EXCLUDED.price_candidates,
          price_inserted      = EXCLUDED.price_inserted,
          price_rows_total    = EXCLUDED.price_rows_total,
          status              = EXCLUDED.status,
          note                = EXCLUDED.note,
          raw                 = EXCLUDED.raw
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
            res = {"script": name, "returncode":127, "duration_ms":0,
                   "stdout_bytes":0, "stderr_bytes":0, "error":"not_found",
                   "stdout":"", "stderr":"", "sync_summaries":[]}
            results.append(res)
            overall_ok = False
            print(f"[wrapper] missing: {name}")
            continue

        res = run_script(path)
        results.append(res)
        if res["returncode"] != 0:
            overall_ok = False

    finished = now_iso()


    today_stats = _extract_counts(results)


    today_key = _today_key()
    yest_key  = _yesterday_key()
    yesterday_log = _load_daily_runlog(yest_key)
    yesterday_stats = (yesterday_log or {}).get("stats") or {}

    math_ok, math_problems                   = _sanity_compare(today_stats, yesterday_stats)
    mass_ok, mass_problems                   = _mass_balance_checks(today_stats, yesterday_stats)
    dates_ok, date_problems, dates_snapshot  = _sanity_dates(results)
    cross_ok, cross_problems, cross_warnings = _sanity_cross(today_stats, yesterday_stats)
    extra_ok, extra_problems, extra_warnings = _sanity_extra(today_stats, yesterday_stats)

    overall_ok = overall_ok and math_ok and mass_ok and dates_ok and cross_ok and extra_ok

    daily_payload = {
        "date": today_key,
        "started": started,
        "finished": finished,
        "stats": today_stats,
        "dates_snapshot": dates_snapshot,
        "scripts": [
            {k: v for k, v in r.items() if k not in ("stdout", "stderr")}
            for r in results
        ],
        "math_ok": math_ok, "math_problems": math_problems,
        "mass_ok": mass_ok, "mass_problems": mass_problems,
        "dates_ok": dates_ok, "date_problems": date_problems,
        "cross_ok": cross_ok, "cross_problems": cross_problems, "cross_warnings": cross_warnings,
        "extra_ok": extra_ok, "extra_problems": extra_problems, "extra_warnings": extra_warnings,
    }
    _save_daily_runlog(today_key, daily_payload)

    note_parts: List[str] = []
    if not math_ok:  note_parts.append("; ".join(math_problems))
    if not mass_ok:  note_parts.append("; ".join(mass_problems))
    if not dates_ok: note_parts.append("; ".join(date_problems))
    if not cross_ok: note_parts.append("; ".join(cross_problems))
    if not extra_ok: note_parts.append("; ".join(extra_problems))
    if cross_warnings: note_parts.append("WARN: " + "; ".join(cross_warnings[:3]))
    if extra_warnings: note_parts.append("WARN+: " + "; ".join(extra_warnings[:3]))
    st_phase = (today_stats.get("standart") or {}).get("phase_counts") or {}
    typ_phase = (today_stats.get("typical") or {}).get("phase_counts") or {}
    add_gap = None
    arch_gap = None
    if "added" in st_phase and "inserted" in typ_phase:
        add_gap = _to_int(st_phase.get("added"), 0) - _to_int(typ_phase.get("inserted"), 0)
        note_parts.append(f"gap_added(step1-typ)={add_gap}")
    if "archived" in st_phase and "archived" in typ_phase:
        arch_gap = _to_int(st_phase.get("archived"), 0) - _to_int(typ_phase.get("archived"), 0)
        note_parts.append(f"gap_arch(step1-typ)={arch_gap}")

    note = ("; ".join([p for p in note_parts if p])[:300]) if note_parts else None

    short = {
        "module": "idnes_wrapper",
        "status": "ok" if overall_ok else "error",
        "added_today":     today_stats.get("typical", {}).get("added_today"),
        "archived_today":  today_stats.get("typical", {}).get("archived_today"),
        "total_standart":  today_stats.get("standart", {}).get("total_rows"),
        "total_typical":   today_stats.get("typical", {}).get("total_rows"),
        "dates_ok": dates_ok,
        "mass_ok": mass_ok,
        "note": note,
    }
    print(json.dumps(short, ensure_ascii=False, separators=(",", ":")))
==
    _upsert_stats(
        module="idnes_pipeline",
        status=("ok" if overall_ok else "error"),
        note=note,
        today_stats=today_stats
    )

    # подробный JSON
    summary = {
        "module": "idnes_pipeline",
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
        "dates_snapshot": dates_snapshot,
        "math_ok": math_ok, "math_problems": math_problems,
        "mass_ok": mass_ok, "mass_problems": mass_problems,
        "dates_ok": dates_ok, "date_problems": date_problems,
        "cross_ok": cross_ok, "cross_problems": cross_problems, "cross_warnings": cross_warnings,
        "extra_ok": extra_ok, "extra_problems": extra_problems, "extra_warnings": extra_warnings,
        "yesterday_key": yest_key if yesterday_log else None
    }
    print(json.dumps(summary, ensure_ascii=False, separators=(",", ":")))

    sys.exit(0 if overall_ok else 1)

if __name__ == "__main__":
    main()