
from __future__ import annotations
import sys, os, subprocess, json, time, re
from pathlib import Path
from datetime import datetime, timezone, timedelta, date
from typing import Any, Dict, List, Optional

from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

# ========== Logging ==========
PROJECT_DIR = Path(__file__).resolve().parent
RUNLOG_DIR  = PROJECT_DIR / ".runlogs" / "sreality"
RUNLOG_DIR.mkdir(parents=True, exist_ok=True)

SCRIPTS = [
    "1_step.py",
    "sreality_typical.py",
    "price_change.py",
    "photo.py",
    "seller.py",
]

MAX_ADDED_HARD    = int(os.getenv("WRAP_MAX_ADDED", "20000"))
MAX_ARCHIVED_HARD = int(os.getenv("WRAP_MAX_ARCH",  "40000"))
DELTA_TOLERANCE   = int(os.getenv("WRAP_DELTA_TOL", "200"))
REQUIRE_ST_EQ_TY  = os.getenv("WRAP_REQUIRE_ST_EQ_TY", "0") == "1"  # по умолчанию только warning

# ========== CONFIG & ENGINE ==========
PROJECT_ROOT = Path(__file__).resolve().parents[2]
cfg = json.loads((PROJECT_ROOT / "config.json").read_text(encoding="utf-8"))

db_url = URL.create(
    drivername="postgresql",
    username=cfg["USER"],
    password=cfg["PWD"],
    host=cfg["HOST"],
    port=int(cfg["PORT"]),
    database=cfg["DB"],
)
ENGINE = create_engine(db_url, pool_pre_ping=True, connect_args={"connect_timeout": 10})

# ==========  IO ==========
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

# ========== sync_summary ==========
def _parse_sync_summary(stdout: str) -> list[dict]:
    out: List[dict] = []
    for line in stdout.splitlines():
        s = line.strip()
        if not s or s[0] not in "{[":
            continue
        try:
            obj = json.loads(s)
            if isinstance(obj, dict) and obj.get("stage") == "sync_summary" and "stats" in obj:
                out.append(obj)
        except Exception:
            continue
    return out

def run_script(path: Path) -> dict:
    start = time.perf_counter()
    env = os.environ.copy()
    env.setdefault("PYTHONUNBUFFERED", "1")

    proc = subprocess.run(
        [sys.executable, str(path)],
        cwd=path.parent,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    dur_ms = int((time.perf_counter() - start) * 1000)
    sync_summaries = _parse_sync_summary(proc.stdout)

    print(f"[wrapper] run: {path.name}")
    if proc.returncode == 0:
        print(f"[wrapper] ok:   {path.name} in {dur_ms} ms")
    else:
        print(f"[wrapper] fail: {path.name} rc={proc.returncode} in {dur_ms} ms")

    return {
        "script": path.name,
        "returncode": proc.returncode,
        "duration_ms": dur_ms,
        "stdout_bytes": len(proc.stdout.encode("utf-8", errors="ignore")),
        "stderr_bytes": len(proc.stderr.encode("utf-8", errors="ignore")),
        "stdout": proc.stdout,
        "stderr": proc.stderr,
        "sync_summaries": sync_summaries,
    }

# ========== Agr ==========
def _to_int(v, default=None):
    try:
        return int(v) if v is not None else default
    except Exception:
        return default

def _extract_counts(all_results: list[dict]) -> dict:
    out: Dict[str, Any] = {
        "standart": {"total_rows": None, "added_today": None, "archived_today": None, "active_now": None,
                     "snapshot_date": None, "curr_table": None, "prev_table": None, "phase_counts": {}},
        "typical":  {"total_rows": None, "added_today": None, "archived_today": None, "active_now": None,
                     "snapshot_date": None, "curr_table": None, "prev_table": None, "phase_counts": {}},
        "price":    {},
        "photo":    {},
        "seller":   {},
    }
    for res in all_results:
        name = (res.get("script") or "").lower()
        summaries = res.get("sync_summaries") or []
        if not summaries:
            continue
        stats = (summaries[-1] or {}).get("stats") or {}

        if "sreality_typical" in name or "typical.py" in name:
            for k in ("total_rows","added_today","archived_today","active_now"):
                out["typical"][k] = _to_int(stats.get(k), out["typical"].get(k))
            for k in ("snapshot_date","curr_table","prev_table"):
                if stats.get(k): out["typical"][k] = stats.get(k)
            if isinstance(stats.get("phase_counts"), dict):
                out["typical"]["phase_counts"] = stats["phase_counts"]

        elif "1_step.py" in name or "standart" in name:
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
        elif "seller" in name:
            out["seller"] = {k: v for k, v in stats.items()}
    return out

# ========== dates ==========
_SREALITY_RE = re.compile(r"^sreality_(\d{2})(\d{2})(\d{4})$")  # DDMMYYYY

def _tbl_to_date(tbl: str) -> Optional[date]:
    m = _SREALITY_RE.match(tbl or "")
    if not m:
        return None
    dd, mm, yyyy = m.groups()
    try:
        return date(int(yyyy), int(mm), int(dd))
    except Exception:
        return None

def _parse_yyyy_mm_dd(s: str) -> Optional[date]:
    try:
        y, m, d = s.split("-")
        return date(int(y), int(m), int(d))
    except Exception:
        return None

# ========== Sanity ==========
def _sanity_math(today: dict, yesterday: dict | None) -> tuple[bool, list[str], list[str]]:
    problems: List[str] = []
    warnings: List[str] = []

    def nonneg(name: str, val):
        if val is None:
            return
        try:
            if int(val) < 0:
                warnings.append(f"{name} is negative: {val}")
        except Exception:
            warnings.append(f"{name} not int-like: {val}")

    typ_t = today.get("typical") or {}
    typ_y = (yesterday or {}).get("typical") or {}
    for f in ("added_today","archived_today","active_now","total_rows"):
        nonneg(f"typical.{f}", typ_t.get(f))
    if typ_t.get("total_rows") is not None and typ_y.get("total_rows") is not None:
        if int(typ_t["total_rows"]) < int(typ_y["total_rows"]):
            warnings.append(f"typical.total_rows decreased: {typ_t['total_rows']} < {typ_y['total_rows']}")

    std_t = today.get("standart") or {}
    std_y = (yesterday or {}).get("standart") or {}
    for f in ("added_today","archived_today","active_now","total_rows"):
        nonneg(f"standart.{f}", std_t.get(f))
    if std_t.get("total_rows") is not None and std_y.get("total_rows") is not None:
        if int(std_t["total_rows"]) < int(std_y["total_rows"]):
            warnings.append(f"standart.total_rows decreased: {std_t['total_rows']} < {std_y['total_rows']}")

    # active_now <= total_rows проверки
    if std_t.get("active_now") is not None and std_t.get("total_rows") is not None:
        if int(std_t["active_now"]) > int(std_t["total_rows"]):
            warnings.append(f"standart.active_now({std_t['active_now']}) > total_rows({std_t['total_rows']})")
    if typ_t.get("active_now") is not None and typ_t.get("total_rows") is not None:
        if int(typ_t["active_now"]) > int(typ_t["total_rows"]):
            warnings.append(f"typical.active_now({typ_t['active_now']}) > total_rows({typ_t['total_rows']})")

    # всплески — предупреждения
    if typ_t.get("added_today") is not None and int(typ_t["added_today"]) > MAX_ADDED_HARD:
        warnings.append(f"suspicious typical.added_today={typ_t['added_today']} > {MAX_ADDED_HARD}")
    if typ_t.get("archived_today") is not None and int(typ_t["archived_today"]) > MAX_ARCHIVED_HARD:
        warnings.append(f"suspicious typical.archived_today={typ_t['archived_today']} > {MAX_ARCHIVED_HARD}")

    ok = True
    return ok, problems, warnings

def _collect_table_fields(stats: dict) -> list[tuple[str, str]]:
    out = []
    for key in ("prev_table","curr_table","snapshot_table"):
        v = stats.get(key)
        if isinstance(v, str) and v:
            out.append((key, v))
    return out

def _sanity_dates_all(results: list[dict]) -> tuple[bool, list[str], dict]:
    probs: List[str] = []
    tdy = today_date()
    yst = yesterday_date()
    snapshot_dump = {"expected_today": tdy.isoformat(), "expected_yesterday": yst.isoformat(), "by_script": []}

    for r in results:
        script = r.get("script","")
        summaries = r.get("sync_summaries") or []
        if not summaries:
            continue
        stats = summaries[-1].get("stats") or {}
        pairs = _collect_table_fields(stats)
        if not pairs and "snapshot_date" not in stats and "change_date" not in stats:
            continue

        entry = {"script": script, "tables": []}
        for key, tbl in pairs:
            d = _tbl_to_date(tbl)
            entry["tables"].append({"key": key, "table": tbl, "parsed_date": (d.isoformat() if d else None)})
            if d is None:
                probs.append(f"{script}: {key}={tbl} — can't parse date")
                continue
            if key == "prev_table" and d != yst:
                probs.append(f"{script}: prev_table {tbl} != yesterday ({yst})")
            if key in ("curr_table","snapshot_table") and d != tdy:
                probs.append(f"{script}: {key} {tbl} != today ({tdy})")

        if isinstance(stats.get("snapshot_date"), str):
            sd = _parse_yyyy_mm_dd(stats["snapshot_date"])
            entry.setdefault("fields", []).append({"key":"snapshot_date","value":stats["snapshot_date"],"parsed_date":(sd.isoformat() if sd else None)})
            if not sd or sd != tdy:
                probs.append(f"{script}: snapshot_date {stats['snapshot_date']} != today ({tdy})")
        if isinstance(stats.get("change_date"), str):
            cd = _parse_yyyy_mm_dd(stats["change_date"])
            entry.setdefault("fields", []).append({"key":"change_date","value":stats["change_date"],"parsed_date":(cd.isoformat() if cd else None)})
            if not cd or cd != tdy:
                probs.append(f"{script}: change_date {stats['change_date']} != today ({tdy})")

        snapshot_dump["by_script"].append(entry)

    ok = len(probs) == 0
    return ok, probs, snapshot_dump

def _as_int(d: dict, *path) -> Optional[int]:
    cur: Any = d
    for p in path:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(p)
    try:
        return int(cur) if cur is not None else None
    except Exception:
        return None

def _sanity_cross(today: dict, yesterday: dict | None) -> tuple[bool, list[str], list[str]]:
    problems: List[str] = []
    warnings: List[str] = []

    # 1) standart vs typical totals — warning, не ошибка
    st_tot = _as_int(today, "standart", "total_rows")
    ty_tot = _as_int(today, "typical",  "total_rows")
    if st_tot is not None and ty_tot is not None and st_tot != ty_tot:
        if REQUIRE_ST_EQ_TY:
            problems.append(f"standart.total_rows({st_tot}) != typical.total_rows({ty_tot})")
        else:
            warnings.append(f"standart.total_rows({st_tot}) != typical.total_rows({ty_tot})")

    # 2) Баланс дельт по standart считаем ТОЛЬКО если есть все три метрики из phase_counts
    st_phase = (today.get("standart") or {}).get("phase_counts") or {}
    st_added = _to_int(st_phase.get("added"))
    st_react = _to_int(st_phase.get("reactivated"))
    st_arch  = _to_int(st_phase.get("archived"))
    y_st_tot = _as_int(yesterday or {}, "standart", "total_rows")
    st_tot   = _as_int(today, "standart", "total_rows")
    if None not in (st_added, st_react, st_arch, y_st_tot, st_tot):
        delta_total = st_tot - y_st_tot
        delta_ops   = (st_added or 0) + (st_react or 0) - (st_arch or 0)
        if abs(delta_total - delta_ops) > DELTA_TOLERANCE:
            # это всё равно warning, потому что чистки/реактивации могут быть за прошлые даты
            warnings.append(
                f"delta mismatch: Δtotal={delta_total} vs added+reactivated-archived={delta_ops} (tol={DELTA_TOLERANCE})"
            )

    # 3) Баланс для typical с учётом возможного dedup_removed — warning
    ty_phase = (today.get("typical") or {}).get("phase_counts") or {}
    ty_add = _as_int(today, "typical", "added_today")
    ty_arc = _as_int(today, "typical", "archived_today")
    ty_ded = _to_int(ty_phase.get("dedup_removed"), 0)
    ty_tot_y = _as_int(yesterday or {}, "typical", "total_rows")
    ty_tot_t = _as_int(today, "typical", "total_rows")
    if None not in (ty_add, ty_arc, ty_tot_y, ty_tot_t):
        expected = ty_tot_y + (ty_add or 0) - (ty_arc or 0) - (ty_ded or 0)
        if abs((ty_tot_t or 0) - expected) > DELTA_TOLERANCE:
            warnings.append(
                f"typical mass-balance mismatch: total={ty_tot_t} vs expected={expected} (y={ty_tot_y} + {ty_add} - {ty_arc} - dedup:{ty_ded}, tol={DELTA_TOLERANCE})"
            )

    # 4) cross мелочи
    pr_cand = _as_int(today, "price", "candidates")
    pr_ins  = _as_int(today, "price", "inserted_new")
    if pr_cand is not None and pr_ins is not None and (pr_ins < 0 or pr_ins > pr_cand):
        warnings.append(f"price.inserted_new({pr_ins}) out of bounds [0..{pr_cand}]")

    pr_curr = _as_int(today, "price", "curr_rows")
    ph_src  = _as_int(today, "photo", "source_rows")
    sl_src  = _as_int(today, "seller", "source_rows")
    if pr_curr is not None and ph_src is not None and pr_curr != ph_src:
        warnings.append(f"price.curr_rows({pr_curr}) != photo.source_rows({ph_src})")
    if pr_curr is not None and sl_src is not None and pr_curr != sl_src:
        warnings.append(f"price.curr_rows({pr_curr}) != seller.source_rows({sl_src})")

    st_act = _as_int(today, "standart", "active_now")
    ty_act = _as_int(today, "typical",  "active_now")
    if st_act is not None and ty_act is not None and st_act != ty_act:
        warnings.append(f"active_now mismatch: standart={st_act} vs typical={ty_act}")

    ok = len(problems) == 0  # только настоящие «problems» валят пайплайн
    return ok, problems, warnings

# ==========sreality_stats ==========
_CREATE_STATS_BASE = """
CREATE TABLE IF NOT EXISTS public.sreality_stats (
  id bigserial PRIMARY KEY,
  run_ts timestamptz NOT NULL DEFAULT now(),
  run_date date NOT NULL,
  module text NOT NULL,
  snapshot_date date,
  status text,
  note text,
  raw jsonb
);
"""
_STATS_COLUMNS: dict[str, str] = {
  # standart
  "step1_source_rows":"int","step1_added":"int","step1_reactivated":"int","step1_archived":"int",
  "total_standart":"int","st_added_today":"int","st_archived_today":"int","st_active_now":"int",
  # typical
  "typical_rows_before":"int","typical_inserted":"int","typical_updated":"int","typical_archived":"int",
  "typical_dedup_removed":"int","total_typical":"int","ty_added_today":"int","ty_archived_today":"int","ty_active_now":"int",
  # photo
  "photo_source_rows":"int","photo_inserted":"int","photo_rows_total":"int",
  # seller
  "seller_source_rows":"int","seller_inserted":"int","seller_rows_total":"int",
  # price
  "price_prev_rows":"int","price_curr_rows":"int","price_candidates":"int","price_inserted":"int","price_rows_total":"int",
}

def _ensure_stats_table():
    with ENGINE.begin() as conn:
        conn.execute(text(_CREATE_STATS_BASE))
        for col, pgtype in _STATS_COLUMNS.items():
            conn.execute(text(f'ALTER TABLE public.sreality_stats ADD COLUMN IF NOT EXISTS {col} {pgtype}'))
        conn.execute(text("""
            CREATE UNIQUE INDEX IF NOT EXISTS ux_sreality_stats_day_module
            ON public.sreality_stats (run_date, module)
        """))

def _derive_numbers(today_stats: dict) -> dict:
    st  = today_stats.get("standart") or {}
    ty  = today_stats.get("typical") or {}
    ph  = today_stats.get("photo") or {}
    sl  = today_stats.get("seller") or {}
    pr  = today_stats.get("price") or {}
    stp = st.get("phase_counts") or {}
    typ = ty.get("phase_counts") or {}

    return {
        "snapshot_date": (st.get("snapshot_date") or ty.get("snapshot_date") or ph.get("snapshot_date") or sl.get("snapshot_date")),
        "step1_source_rows": _to_int(stp.get("source_rows"), None),
        "step1_added": _to_int(stp.get("added"), 0),
        "step1_reactivated": _to_int(stp.get("reactivated"), 0),
        "step1_archived": _to_int(stp.get("archived"), 0),
        "total_standart": _to_int(st.get("total_rows"), None),
        "st_added_today": _to_int(st.get("added_today"), None),
        "st_archived_today": _to_int(st.get("archived_today"), None),
        "st_active_now": _to_int(st.get("active_now"), None),

        "typical_rows_before": _to_int(typ.get("typical_rows_before"), None),
        "typical_inserted": _to_int(typ.get("inserted"), 0),
        "typical_updated": _to_int(typ.get("updated"), None),
        "typical_archived": _to_int(typ.get("archived"), _to_int(ty.get("archived_today"), 0)),
        "typical_dedup_removed": _to_int(typ.get("dedup_removed"), 0),
        "total_typical": _to_int(ty.get("total_rows"), None),
        "ty_added_today": _to_int(ty.get("added_today"), 0),
        "ty_archived_today": _to_int(ty.get("archived_today"), 0),
        "ty_active_now": _to_int(ty.get("active_now"), None),

        "photo_source_rows": _to_int(ph.get("source_rows"), None),
        "photo_inserted": _to_int(ph.get("inserted_new"), 0),
        "photo_rows_total": _to_int(ph.get("photo_rows_total"), None),

        "seller_source_rows": _to_int(sl.get("source_rows"), None),
        "seller_inserted": _to_int(sl.get("inserted_new"), 0),
        "seller_rows_total": _to_int(sl.get("seller_rows_total"), None),

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
        INSERT INTO public.sreality_stats (
          run_date, module, snapshot_date,
          step1_source_rows, step1_added, step1_reactivated, step1_archived, total_standart,
          st_added_today, st_archived_today, st_active_now,
          typical_rows_before, typical_inserted, typical_updated, typical_archived, typical_dedup_removed, total_typical,
          ty_added_today, ty_archived_today, ty_active_now,
          photo_source_rows, photo_inserted, photo_rows_total,
          seller_source_rows, seller_inserted, seller_rows_total,
          price_prev_rows, price_curr_rows, price_candidates, price_inserted, price_rows_total,
          status, note, raw
        )
        VALUES (
          CAST(:run_date AS date), :module, CAST(:snapshot_date AS date),
          :step1_source_rows, :step1_added, :step1_reactivated, :step1_archived, :total_standart,
          :st_added_today, :st_archived_today, :st_active_now,
          :typical_rows_before, :typical_inserted, :typical_updated, :typical_archived, :typical_dedup_removed, :total_typical,
          :ty_added_today, :ty_archived_today, :ty_active_now,
          :photo_source_rows, :photo_inserted, :photo_rows_total,
          :seller_source_rows, :seller_inserted, :seller_rows_total,
          :price_prev_rows, :price_curr_rows, :price_candidates, :price_inserted, :price_rows_total,
          :status, :note, CAST(:raw AS jsonb)
        )
        ON CONFLICT (run_date, module) DO UPDATE SET
          snapshot_date        = EXCLUDED.snapshot_date,
          step1_source_rows    = EXCLUDED.step1_source_rows,
          step1_added          = EXCLUDED.step1_added,
          step1_reactivated    = EXCLUDED.step1_reactivated,
          step1_archived       = EXCLUDED.step1_archived,
          total_standart       = EXCLUDED.total_standart,
          st_added_today       = EXCLUDED.st_added_today,
          st_archived_today    = EXCLUDED.st_archived_today,
          st_active_now        = EXCLUDED.st_active_now,
          typical_rows_before  = EXCLUDED.typical_rows_before,
          typical_inserted     = EXCLUDED.typical_inserted,
          typical_updated      = EXCLUDED.typical_updated,
          typical_archived     = EXCLUDED.typical_archived,
          typical_dedup_removed= EXCLUDED.typical_dedup_removed,
          total_typical        = EXCLUDED.total_typical,
          ty_added_today       = EXCLUDED.ty_added_today,
          ty_archived_today    = EXCLUDED.ty_archived_today,
          ty_active_now        = EXCLUDED.ty_active_now,
          photo_source_rows    = EXCLUDED.photo_source_rows,
          photo_inserted       = EXCLUDED.photo_inserted,
          photo_rows_total     = EXCLUDED.photo_rows_total,
          seller_source_rows   = EXCLUDED.seller_source_rows,
          seller_inserted      = EXCLUDED.seller_inserted,
          seller_rows_total    = EXCLUDED.seller_rows_total,
          price_prev_rows      = EXCLUDED.price_prev_rows,
          price_curr_rows      = EXCLUDED.price_curr_rows,
          price_candidates     = EXCLUDED.price_candidates,
          price_inserted       = EXCLUDED.price_inserted,
          price_rows_total     = EXCLUDED.price_rows_total,
          status               = EXCLUDED.status,
          note                 = EXCLUDED.note,
          raw                  = EXCLUDED.raw
    """)
    with ENGINE.begin() as conn:
        conn.execute(sql, payload)

# ========== MAIN ==========
def main():
    started = now_iso()
    print(f"[wrapper] start {started}")

    results: List[Dict[str, Any]] = []
    overall_ok = True

    for name in SCRIPTS:
        p = PROJECT_DIR / name
        if not p.exists():
            res = {"script": name, "returncode":127, "duration_ms":0,
                   "stdout_bytes":0, "stderr_bytes":0, "error":"not_found",
                   "stdout":"", "stderr":"", "sync_summaries":[]}
            results.append(res)
            overall_ok = False
            print(f"[wrapper] missing: {name}")
            continue

        r = run_script(p)
        results.append(r)
        if r["returncode"] != 0:
            overall_ok = False

    finished = now_iso()


    today_stats = _extract_counts(results)
    today_key   = _today_key()
    yest_key    = _yesterday_key()
    yesterday_log = _load_daily_runlog(yest_key)
    yesterday_stats = (yesterday_log or {}).get("stats") or {}


    math_ok,  math_problems, math_warnings = _sanity_math(today_stats, yesterday_stats)
    dates_ok, date_problems, dates_snapshot = _sanity_dates_all(results)
    cross_ok, cross_problems, cross_warnings = _sanity_cross(today_stats, yesterday_stats)

    overall_ok = overall_ok and dates_ok

    daily_payload = {
        "date": today_key,
        "started": started,
        "finished": finished,
        "stats": today_stats,
        "dates_snapshot": dates_snapshot,
        "scripts": [{k: v for k, v in r.items() if k not in ("stdout","stderr")} for r in results],
        "math_ok": math_ok, "math_problems": math_problems, "math_warnings": math_warnings,
        "dates_ok": dates_ok, "date_problems": date_problems,
        "cross_ok": cross_ok, "cross_problems": cross_problems, "cross_warnings": cross_warnings,
    }
    _save_daily_runlog(today_key, daily_payload)

    note_parts: List[str] = []
    if math_warnings: note_parts.append("; ".join(math_warnings[:3]))
    if date_problems: note_parts.append("; ".join(date_problems[:3]))
    warns = (cross_warnings or []) + (cross_problems or [])
    if warns: note_parts.append("WARN: " + "; ".join(warns[:3]))
    note = ("; ".join(note_parts)[:400]) if note_parts else None

    short = {
        "module": "sreality_wrapper",
        "status": "ok" if overall_ok else "error",
        "added_today":    today_stats.get("typical", {}).get("added_today"),
        "archived_today": today_stats.get("typical", {}).get("archived_today"),
        "total_standart": today_stats.get("standart", {}).get("total_rows"),
        "total_typical":  today_stats.get("typical", {}).get("total_rows"),
        "dates_ok": dates_ok,
        "note": note,
    }
    print(json.dumps(short, ensure_ascii=False, separators=(",", ":")))

    _upsert_stats(
        module="sreality_pipeline",
        status=("ok" if overall_ok else "error"),
        note=note,
        today_stats=today_stats
    )

    # подробная телеметрия
    summary = {
        "module": "sreality_pipeline",
        "status": "ok" if overall_ok else "error",
        "started": started,
        "finished": finished,
        "duration_ms": sum(r.get("duration_ms", 0) for r in results),
        "ran": [r["script"] for r in results],
        "results": [{k: v for k, v in r.items() if k not in ("stdout","stderr")} for r in results],
        "today_stats": today_stats,
        "dates_snapshot": dates_snapshot,
        "math_ok": math_ok, "math_problems": math_problems, "math_warnings": math_warnings,
        "dates_ok": dates_ok, "date_problems": date_problems,
        "cross_ok": cross_ok, "cross_problems": cross_problems, "cross_warnings": cross_warnings,
        "yesterday_key": yest_key if yesterday_log else None
    }
    print(json.dumps(summary, ensure_ascii=False, separators=(",", ":")))

    sys.exit(0 if overall_ok else 1)

if __name__ == "__main__":
    main()