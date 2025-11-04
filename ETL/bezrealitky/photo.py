
from __future__ import annotations

import sys
import json
import time
import logging
from pathlib import Path
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from sqlalchemy import create_engine, text

# === LOGGING  ===
_root = logging.getLogger()
for h in list(_root.handlers):
    _root.removeHandler(h)
stderr_handler = logging.StreamHandler(stream=sys.stderr)
stderr_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
_root.addHandler(stderr_handler)
_root.setLevel(logging.INFO)
log = logging.getLogger("BezrealitkyPhotoExtract")

# === CONFIG ===
PROJECT_ROOT = Path(__file__).resolve().parents[2] if len(Path(__file__).resolve().parents) >= 3 else Path.cwd()
cfg_path = PROJECT_ROOT / "config.json"
if not cfg_path.exists():
    alt = Path(__file__).resolve().parent / "config.json"
    cfg_path = alt if alt.exists() else cfg_path

cfg = json.loads(cfg_path.read_text(encoding="utf-8"))
USER = cfg.get("USER") or cfg.get("user")
PWD  = cfg.get("PWD")  or cfg.get("password")
HOST = cfg.get("HOST") or cfg.get("host")
PORT = cfg.get("PORT") or cfg.get("port")
DB   = cfg.get("DB")   or cfg.get("dbname")
DRY_RUN = bool(cfg.get("DRY_RUN", False))

def make_db_url() -> tuple[str, str]:
    try:
        import psycopg  # noqa: F401
        return ("psycopg", f"postgresql+psycopg://{USER}:{PWD}@{HOST}:{PORT}/{DB}")
    except ModuleNotFoundError:
        try:
            import psycopg2  # noqa: F401
            return ("psycopg2", f"postgresql+psycopg2://{USER}:{PWD}@{HOST}:{PORT}/{DB}")
        except ModuleNotFoundError:
            raise RuntimeError(
                "Нет драйвера Postgres. Установи один:\n"
                "  pip install 'psycopg[binary]>=3.1'\n"
                "  или\n"
                "  pip install 'psycopg2-binary>=2.9'"
            )

driver, DB_URL = make_db_url()
log.info(f"[DB] driver={driver} host={HOST} db={DB}")

engine = create_engine(DB_URL, pool_pre_ping=True, connect_args={"connect_timeout": 10})

# === JSON EMIT → STDOUT ===
def emit(event: str, **payload) -> None:
    print(json.dumps({"event": event, **payload}, ensure_ascii=False))

# === TIME (Europe/Prague) ===
PRG = ZoneInfo("Europe/Prague")
TODAY: date = datetime.now(PRG).date()
YESTERDAY: date = TODAY - timedelta(days=1)

# === HELPERS ===
def _parse_table_date(tbl: str) -> date | None:
    if not tbl.startswith("bzereality_") or len(tbl) < 19:
        return None
    d = tbl[-8:]  # DDMMYYYY
    try:
        return date(int(d[4:]), int(d[2:4]), int(d[:2]))
    except Exception:
        return None

def _iso_from_tbl(tbl: str) -> str | None:
    dt = _parse_table_date(tbl)
    return dt.isoformat() if dt else None

def _list_bz_tables_le_today() -> list[tuple[date, str]]:
    sql = text("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name LIKE 'bzereality\\_%' ESCAPE '\\'
          AND table_name ~ '^bzereality_[0-9]{8}$'
    """)
    with engine.begin() as conn:
        rows = [r[0] for r in conn.execute(sql)]
    out: list[tuple[date, str]] = []
    for t in rows:
        dt = _parse_table_date(t)
        if dt is not None and dt <= TODAY:
            out.append((dt, t))
    out.sort()
    return out

def _ensure_source_today() -> tuple[str, str]:
    dated = _list_bz_tables_le_today()
    if not dated:
        emit("source_missing", message="no bzereality_* tables <= TODAY", today=str(TODAY))
        # совместимость
        print(json.dumps({"stage": "sync_summary", "note": "no source tables <= TODAY"}, ensure_ascii=False))
        print("SYNC COMPLETE")
        raise SystemExit(1)

    by_date = {dt: t for dt, t in dated}
    tbl_today = by_date.get(TODAY)
    if not tbl_today:
        problems = [f"no snapshot for today ({TODAY})"]
        emit("dates_mismatch", ok=False, problems=problems, last_available=str(dated[-1][0]))
        print(json.dumps({"stage": "sync_summary", "error": "dates_mismatch", "problems": problems}, ensure_ascii=False))
        print("SYNC COMPLETE")
        raise SystemExit(1)

    return tbl_today, TODAY.isoformat()

def ensure_target_unique():
    if DRY_RUN:
        return
    with engine.begin() as conn:
        conn.execute(text("SET LOCAL lock_timeout = '5s'"))
        conn.execute(text("SET LOCAL statement_timeout = '5min'"))
        conn.execute(text("""
            CREATE UNIQUE INDEX IF NOT EXISTS ux_bz_photo_id
            ON public.bezrealitky_photo (id)
        """))

def ensure_source_columns(table: str, need: list[str]) -> tuple[bool, list[str]]:
    q = text("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = :t
    """)
    with engine.begin() as conn:
        have = {r[0] for r in conn.execute(q, {"t": table})}
    missing = [c for c in need if c not in have]
    if missing:
        log.warning("Table %s: missing columns %s", table, missing)
        return False, missing
    return True, []

# === MAIN ===
def extract_latest_bz_photos() -> None:
    t0 = time.perf_counter()
    try:
        emit("start", stage="photo_extract", driver=driver, dry_run=DRY_RUN, today=str(TODAY))

        # 1) Берем снапшот строго за сегодня (Europe/Prague)
        table_today, snapshot_date = _ensure_source_today()
        emit("source_selected", table=table_today, snapshot_date=snapshot_date)

        ok, missing = ensure_source_columns(table_today, ["id", "images"])
        emit("columns_checked", ok=ok, missing=missing)
        if not ok:
            emit("summary", stats={
                "source_rows": 0, "photo_rows_total": None, "inserted_new": 0, "snapshot_table": table_today, "snapshot_date": snapshot_date
            })
            emit("done", duration_s=round(time.perf_counter() - t0, 3))
            print(json.dumps({"stage": "sync_summary", "note": f"{table_today} missing columns"}, ensure_ascii=False))
            print("SYNC COMPLETE")
            return

        ensure_target_unique()

        inserted = 0
        if not DRY_RUN:
            insert_sql = text(f"""
                INSERT INTO public.bezrealitky_photo (id, images)
                SELECT s.id::text AS id, s.images
                FROM public."{table_today}" AS s
                ON CONFLICT (id) DO NOTHING
            """)
            with engine.begin() as conn:
                conn.execute(text("SET LOCAL synchronous_commit = off"))
                conn.execute(text("SET LOCAL lock_timeout = '5s'"))
                conn.execute(text("SET LOCAL statement_timeout = '5min'"))
                res = conn.execute(insert_sql)
                inserted = res.rowcount or 0
        emit("insert", inserted=inserted, dry_run=DRY_RUN)

        with engine.begin() as conn:
            total_src = conn.execute(text(f'SELECT COUNT(*) FROM public."{table_today}"')).scalar_one()
            total_dst = conn.execute(text('SELECT COUNT(*) FROM public.bezrealitky_photo')).scalar_one()

        stats = {
            "source_rows": int(total_src),
            "photo_rows_total": int(total_dst),
            "inserted_new": int(inserted),
            "snapshot_table": table_today,
            "snapshot_date": snapshot_date,
            "dry_run": DRY_RUN,
        }
        emit("summary", stats=stats)
        emit("done", duration_s=round(time.perf_counter() - t0, 3))

        print(json.dumps({"stage": "sync_summary", "stats": stats}, ensure_ascii=False))
        print("SYNC COMPLETE")

    except SystemExit:
        raise
    except Exception as e:
        log.exception("Fatal error in photo extract")
        emit("error", message=str(e))
        sys.exit(2)

if __name__ == "__main__":
    extract_latest_bz_photos()
