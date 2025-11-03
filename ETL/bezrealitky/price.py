from __future__ import annotations

import sys
import json
import time
import logging
from pathlib import Path
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo

from sqlalchemy import create_engine, text

# === LOGGING  ===
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
config_path = PROJECT_ROOT / "config.json"
if not config_path.exists():
    alt = Path(__file__).resolve().parent / "config.json"
    config_path = alt if alt.exists() else config_path

cfg = json.loads(config_path.read_text(encoding="utf-8"))
USER = cfg.get("USER") or cfg.get("user")
PWD  = cfg.get("PWD")  or cfg.get("password")
HOST = cfg.get("HOST") or cfg.get("host")
PORT = cfg.get("PORT") or cfg.get("port")
DB   = cfg.get("DB")   or cfg.get("dbname")
DRY_RUN = bool(cfg.get("DRY_RUN", False))

def make_db_url() -> tuple[str, str]:
    try:
        import psycopg  # noqa
        return ("psycopg", f"postgresql+psycopg://{USER}:{PWD}@{HOST}:{PORT}/{DB}")
    except ModuleNotFoundError:
        try:
            import psycopg2  # noqa
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

# === EMIT JSON → STDOUT ===
def emit(event: str, **payload) -> None:
    print(json.dumps({"event": event, **payload}, ensure_ascii=False))

# === time helpers (Europe/Prague) ===
PRG = ZoneInfo("Europe/Prague")
def today_prague() -> date:
    return datetime.now(PRG).date()

TODAY = today_prague()
YESTERDAY = TODAY - timedelta(days=1)

# === helpers ===
def parse_table_date(table_name: str) -> date | None:
    if not table_name.startswith("bzereality_") or len(table_name) < 19:
        return None
    d = table_name[-8:]  # DDMMYYYY
    try:
        return date(int(d[4:]), int(d[2:4]), int(d[:2]))
    except Exception:
        return None

def date_from_table_name_str(table_name: str) -> str:
    dt = parse_table_date(table_name)
    return dt.isoformat() if dt else None

def get_all_bz_tables() -> list[str]:
    q = text("""
        SELECT table_name 
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name LIKE 'bzereality\\_%' ESCAPE '\\'
          AND table_name ~ '^bzereality_[0-9]{8}$'
    """)
    with engine.begin() as c:
        return [r[0] for r in c.execute(q)]

def pick_snapshots_for_today() -> tuple[str, str, str]:
    tables = get_all_bz_tables()
    dated = []
    for t in tables:
        dt = parse_table_date(t)
        if dt is not None and dt <= TODAY:
            dated.append((dt, t))
    dated.sort()  # по дате по возрастанию

    if len(dated) < 2:
        msg = "Not enough snapshots <= TODAY to compare"
        log.error(msg)
        emit("error", message=msg, today=str(TODAY))
        print(json.dumps({"stage": "sync_summary", "error": "not_enough_snapshots"}, ensure_ascii=False))
        print("SYNC COMPLETE")
        raise SystemExit(1)

    by_date = {dt: t for dt, t in dated}
    curr_table = by_date.get(TODAY)
    prev_table = by_date.get(YESTERDAY)

    if not curr_table or not prev_table:
        problems = []
        if not prev_table:
            problems.append(f"no snapshot for yesterday ({YESTERDAY})")
        if not curr_table:
            problems.append(f"no snapshot for today ({TODAY})")
        emit("dates_mismatch", ok=False, problems=problems, available_dates=[str(d) for d,_ in dated[-5:]])
        print(json.dumps({"stage":"sync_summary","error":"dates_mismatch","problems":problems}, ensure_ascii=False))
        print("SYNC COMPLETE")
        raise SystemExit(1)

    change_date = TODAY.isoformat()
    return prev_table, curr_table, change_date

def count_dupes(conn) -> int:
    sql = text("""
        SELECT COALESCE(SUM(cnt),0)::bigint
        FROM (
          SELECT COUNT(*) AS cnt
          FROM public.bezrealitky_price
          GROUP BY id::text, change_date::date, old_price::numeric, new_price::numeric
          HAVING COUNT(*) > 1
        ) s
    """)
    return int(conn.execute(sql).scalar_one())

def log_dupe_samples(conn):
    top = conn.execute(text("""
        SELECT id::text AS id, change_date::date AS change_date, 
               old_price::numeric, new_price::numeric, COUNT(*) AS cnt
        FROM public.bezrealitky_price
        GROUP BY 1,2,3,4
        HAVING COUNT(*) > 1
        ORDER BY cnt DESC
        LIMIT 10
    """)).mappings().all()
    if top:
        log.warning(f"[DEDUP] top duplicate keys: {[dict(r) for r in top]}")

def run_dedupe():
    with engine.begin() as conn:
        conn.execute(text("SET LOCAL lock_timeout = '5s'"))
        conn.execute(text("SET LOCAL statement_timeout = '5min'"))
        log_dupe_samples(conn)
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
            WHERE p.ctid = r.ctid
              AND r.rn > 1
            RETURNING 1
        """)).fetchall())
        log.warning(f"[DEDUP] removed={deleted}")

def ensure_read_indexes():
    with engine.begin() as conn:
        conn.execute(text("SET LOCAL lock_timeout = '5s'"))
        conn.execute(text("SET LOCAL statement_timeout = '5min'"))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS ix_bz_price_id_date
              ON public.bezrealitky_price (id, change_date)
        """))

def ensure_unique_index():
    with engine.begin() as conn:
        conn.execute(text("SET LOCAL lock_timeout = '5s'"))
        conn.execute(text("SET LOCAL statement_timeout = '5min'"))
        conn.execute(text("""
            CREATE UNIQUE INDEX IF NOT EXISTS ux_bz_price_unique
              ON public.bezrealitky_price (id, change_date, old_price, new_price)
        """))

# === main ===
def track_price_changes_latest():
    t0 = time.perf_counter()
    try:
        emit("start", stage="price_track", driver=driver, dry_run=DRY_RUN, today=str(TODAY), yesterday=str(YESTERDAY))

        prev_table, curr_table, change_date = pick_snapshots_for_today()
        log.info(f"Comparing snapshots: {prev_table} -> {curr_table} (change_date={change_date})")
        emit("snapshots_selected", prev=prev_table, curr=curr_table, change_date=change_date)

        ensure_read_indexes()

        with engine.begin() as conn:
            dupes = count_dupes(conn)
        emit("dedupe_checked", duplicates=dupes)
        if dupes > 0 and not DRY_RUN:
            log.warning(f"[DEDUP] duplicates detected: {dupes}")
            run_dedupe()
            emit("dedupe_performed", removed=">0")
        else:
            emit("dedupe_skipped", reason="dry_run" if DRY_RUN else "no_duplicates")

        if not DRY_RUN:
            ensure_unique_index()
            emit("unique_index", status="ensured")
        else:
            emit("unique_index", status="skipped_dry_run")

        SQL_COUNT = f"""
            WITH prev AS (
                SELECT id::text AS id, price::numeric AS price
                FROM public."{prev_table}"
            ),
            curr AS (
                SELECT id::text AS id, price::numeric AS price
                FROM public."{curr_table}"
            )
            SELECT COUNT(*)::bigint
            FROM prev p
            JOIN curr c USING (id)
            WHERE p.price IS NOT NULL
              AND c.price IS NOT NULL
              AND p.price <> c.price
        """

        SQL_INSERT = f"""
            WITH prev AS (
                SELECT id::text AS id, price::numeric AS price
                FROM public."{prev_table}"
            ),
            curr AS (
                SELECT id::text AS id, price::numeric AS price
                FROM public."{curr_table}"
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
            SELECT d.id, d.old_price, d.new_price, CAST(:change_date AS date)
            FROM diffs d
            ON CONFLICT (id, change_date, old_price, new_price) DO NOTHING
        """

        with engine.begin() as conn:
            conn.execute(text("SET LOCAL lock_timeout = '5s'"))
            conn.execute(text("SET LOCAL statement_timeout = '5min'"))

            candidates = int(conn.execute(text(SQL_COUNT)).scalar_one())
            emit("candidates_counted", count=candidates)

            inserted = 0
            if not DRY_RUN and candidates > 0:
                res = conn.execute(text(SQL_INSERT), {"change_date": change_date})
                inserted = res.rowcount or 0
            emit("insert", inserted=inserted, dry_run=DRY_RUN)

            total_prev = int(conn.execute(text(f'SELECT COUNT(*) FROM public."{prev_table}"')).scalar_one())
            total_curr = int(conn.execute(text(f'SELECT COUNT(*) FROM public."{curr_table}"')).scalar_one())
            price_total = int(conn.execute(text('SELECT COUNT(*) FROM public.bezrealitky_price')).scalar_one())

        stats = {
            "prev_table": prev_table,
            "curr_table": curr_table,
            "change_date": change_date,
            "candidates": candidates,
            "inserted_new": inserted,
            "prev_rows": total_prev,
            "curr_rows": total_curr,
            "price_rows_total": price_total,
            "dry_run": DRY_RUN
        }
        emit("summary", stats=stats)
        emit("done", duration_s=round(time.perf_counter() - t0, 3))

        print(json.dumps({"stage": "sync_summary", "stats": stats}, ensure_ascii=False))
        print("SYNC COMPLETE")
        sys.exit(0)

    except SystemExit:
        raise
    except Exception as e:
        log.exception("Fatal error in price tracking")
        emit("error", message=str(e))
        sys.exit(2)

if __name__ == "__main__":
    track_price_changes_latest()
