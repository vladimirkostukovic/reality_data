# Extracts photo metadata from the daily bezrealitky_* snapshot and emits a unified JSON summary.

from __future__ import annotations

import sys
import json
import time
import logging
from pathlib import Path
from datetime import date, datetime
from zoneinfo import ZoneInfo

from sqlalchemy import create_engine, text

# === LOGGING ===
_root = logging.getLogger()
for h in list(_root.handlers):
    _root.removeHandler(h)
stderr = logging.StreamHandler(stream=sys.stderr)
stderr.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
_root.addHandler(stderr)
_root.setLevel(logging.INFO)
log = logging.getLogger("BezrealitkyPhoto")

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

def make_db_url():
    try:
        import psycopg
        return ("psycopg", f"postgresql+psycopg://{USER}:{PWD}@{HOST}:{PORT}/{DB}")
    except ModuleNotFoundError:
        import psycopg2
        return ("psycopg2", f"postgresql+psycopg2://{USER}:{PWD}@{HOST}:{PORT}/{DB}")

driver, DB_URL = make_db_url()
engine = create_engine(DB_URL, pool_pre_ping=True, connect_args={"connect_timeout": 10})

# === TIME ===
TODAY = datetime.now(ZoneInfo("Europe/Prague")).date()

# === HELPERS ===
def parse_table_date(tbl: str) -> date | None:
    if not tbl.startswith("bzereality_") or len(tbl) < 19:
        return None
    d = tbl[-8:]
    try:
        return date(int(d[4:]), int(d[2:4]), int(d[:2]))
    except Exception:
        return None

def snapshots_valid() -> list[tuple[date, str]]:
    sql = text("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema='public'
          AND table_name LIKE 'bzereality\\_%' ESCAPE '\\'
          AND table_name ~ '^bzereality_[0-9]{8}$'
    """)
    with engine.begin() as conn:
        rows = [r[0] for r in conn.execute(sql)]

    out = []
    for t in rows:
        dt = parse_table_date(t)
        if dt is not None and dt <= TODAY:
            out.append((dt, t))
    return sorted(out)

def get_today_table() -> tuple[str, str]:
    snaps = snapshots_valid()
    if not snaps:
        raise RuntimeError("No snapshots found")

    by_dt = {d: t for d, t in snaps}
    tbl = by_dt.get(TODAY)
    if not tbl:
        raise RuntimeError(f"No snapshot for today={TODAY}")

    return tbl, TODAY.isoformat()

def ensure_target_unique():
    if DRY_RUN:
        return
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE UNIQUE INDEX IF NOT EXISTS ux_bz_photo_id
            ON public.bezrealitky_photo (id)
        """))

def ensure_columns(table: str, need: list[str]):
    sql = text("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema='public' AND table_name=:t
    """)
    with engine.begin() as conn:
        have = {r[0] for r in conn.execute(sql, {"t": table})}
    missing = [c for c in need if c not in have]
    if missing:
        raise RuntimeError(f"Missing columns {missing} in {table}")

# === PIPE ===
def extract_photos_latest():
    t0 = time.perf_counter()
    try:
        table_today, snapshot_date = get_today_table()
        ensure_columns(table_today, ["id", "images"])
        ensure_target_unique()

        # INSERT
        inserted = 0
        if not DRY_RUN:
            sql = text(f"""
                INSERT INTO public.bezrealitky_photo (id, images)
                SELECT s.id::text, s.images
                FROM public."{table_today}" s
                ON CONFLICT (id) DO NOTHING
            """)
            with engine.begin() as conn:
                res = conn.execute(sql)
                inserted = res.rowcount or 0

        # METRICS
        with engine.begin() as conn:
            src_rows = conn.execute(text(f'SELECT COUNT(*) FROM public."{table_today}"')).scalar_one()
            dst_rows = conn.execute(text('SELECT COUNT(*) FROM public.bezrealitky_photo')).scalar_one()

        # UNIFIED FINAL JSON
        final = {
            "dry_run": DRY_RUN,
            "snapshot_date": snapshot_date,
            "source_rows": int(src_rows),
            "inserted_new": int(inserted),
            "photo_rows_total": int(dst_rows),
            "status": "ok",
            "duration_s": round(time.perf_counter() - t0, 3)
        }

        print(json.dumps({
            "stage": "sync_summary",
            "stats": final
        }, ensure_ascii=False))

        sys.exit(0)

    except Exception as e:
        log.exception("Photo ETL failed")
        err = {
            "dry_run": DRY_RUN,
            "status": "failed",
            "error": str(e),
            "duration_s": round(time.perf_counter() - t0, 3)
        }

        print(json.dumps({
            "stage": "sync_summary",
            "stats": err
        }, ensure_ascii=False))

        sys.exit(2)


if __name__ == "__main__":
    extract_photos_latest()