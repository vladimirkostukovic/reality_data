# Detects price changes between yesterday and today's snapshots, writes new entries,
# enforces deduplication + indexes, and emits a unified JSON summary for the orchestrator.

from __future__ import annotations

import sys
import json
import time
import logging
from pathlib import Path
from datetime import datetime, timedelta, date
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
log = logging.getLogger("BezrealitkyPriceTrack")

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
PRG = ZoneInfo("Europe/Prague")
TODAY: date = datetime.now(PRG).date()
YESTERDAY: date = TODAY - timedelta(days=1)

# === HELPERS ===
def parse_table_date(table: str) -> date | None:
    if not table.startswith("bzereality_") or len(table) < 19:
        return None
    d = table[-8:]
    try:
        return date(int(d[4:]), int(d[2:4]), int(d[:2]))
    except Exception:
        return None

def list_snapshots() -> list[tuple[date, str]]:
    sql = text("""
        SELECT table_name 
        FROM information_schema.tables
        WHERE table_schema='public'
          AND table_name LIKE 'bzereality\\_%' ESCAPE '\\'
          AND table_name ~ '^bzereality_[0-9]{8}$'
    """)
    with engine.begin() as conn:
        names = [r[0] for r in conn.execute(sql)]

    out = []
    for t in names:
        dt = parse_table_date(t)
        if dt:
            out.append((dt, t))
    return sorted(out)

def pick_snapshots() -> tuple[str, str, str]:
    dated = list_snapshots()
    if len(dated) < 2:
        raise RuntimeError("Not enough snapshots to compare")

    by_date = {dt: t for dt, t in dated}

    prev_table = by_date.get(YESTERDAY)
    curr_table = by_date.get(TODAY)

    if not prev_table or not curr_table:
        raise RuntimeError(
            f"Missing snapshots for comparison: prev({YESTERDAY}), curr({TODAY})"
        )

    return prev_table, curr_table, TODAY.isoformat()

def run_dedupe():
    with engine.begin() as conn:
        deleted = len(conn.execute(text("""
            WITH ranked AS (
                SELECT ctid,
                       ROW_NUMBER() OVER (
                         PARTITION BY id::text, change_date::date, old_price::numeric, new_price::numeric
                         ORDER BY ctid
                       ) AS rn
                FROM public.bezrealitky_price
            )
            DELETE FROM public.bezrealitky_price p
            USING ranked r
            WHERE p.ctid = r.ctid AND r.rn > 1
            RETURNING 1
        """)).fetchall())
    return deleted

def ensure_indexes():
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE UNIQUE INDEX IF NOT EXISTS ux_bz_price_unique
            ON public.bezrealitky_price (id, change_date, old_price, new_price)
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS ix_bz_price_id_date
            ON public.bezrealitky_price (id, change_date)
        """))

# === MAIN ===
def run():
    t0 = time.perf_counter()
    try:
        prev_table, curr_table, change_date = pick_snapshots()

        # dedupe + indexes
        if not DRY_RUN:
            run_dedupe()
            ensure_indexes()

        SQL_COUNT = f"""
            WITH prev AS (
                SELECT id::text, price::numeric FROM public."{prev_table}"
            ),
            curr AS (
                SELECT id::text, price::numeric FROM public."{curr_table}"
            )
            SELECT COUNT(*)
            FROM prev p
            JOIN curr c USING (id)
            WHERE p.price IS NOT NULL
              AND c.price IS NOT NULL
              AND p.price <> c.price
        """

        SQL_INSERT = f"""
            WITH prev AS (
                SELECT id::text, price::numeric FROM public."{prev_table}"
            ),
            curr AS (
                SELECT id::text, price::numeric FROM public."{curr_table}"
            ),
            diffs AS (
                SELECT c.id, p.price AS old_price, c.price AS new_price
                FROM prev p
                JOIN curr c USING (id)
                WHERE p.price IS NOT NULL
                  AND c.price IS NOT NULL
                  AND p.price <> c.price
            )
            INSERT INTO public.bezrealitky_price (id, old_price, new_price, change_date)
            SELECT id, old_price, new_price, CAST(:d AS date)
            FROM diffs
            ON CONFLICT (id, change_date, old_price, new_price) DO NOTHING
        """

        with engine.begin() as conn:
            candidates = int(conn.execute(text(SQL_COUNT)).scalar_one())

            inserted = 0
            if not DRY_RUN and candidates > 0:
                res = conn.execute(text(SQL_INSERT), {"d": change_date})
                inserted = res.rowcount or 0

            prev_rows = int(conn.execute(text(f'SELECT COUNT(*) FROM public."{prev_table}"')).scalar_one())
            curr_rows = int(conn.execute(text(f'SELECT COUNT(*) FROM public."{curr_table}"')).scalar_one())
            price_rows_total = int(conn.execute(text('SELECT COUNT(*) FROM public.bezrealitky_price')).scalar_one())

        summary = {
            "dry_run": DRY_RUN,
            "snapshot_date": change_date,
            "prev_table": prev_table,
            "curr_table": curr_table,
            "candidates": candidates,
            "inserted_new": inserted,
            "prev_rows": prev_rows,
            "curr_rows": curr_rows,
            "price_rows_total": price_rows_total,
            "status": "ok",
            "duration_s": round(time.perf_counter() - t0, 3)
        }

        print(json.dumps({
            "stage": "sync_summary",
            "stats": summary
        }, ensure_ascii=False))

        sys.exit(0)

    except Exception as e:
        log.exception("Fatal error in price ETL")

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
    run()