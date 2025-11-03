
from __future__ import annotations

import sys
import json
import logging
from pathlib import Path
from datetime import datetime
from sqlalchemy import create_engine, text

# ========== LOGGING==========
_root = logging.getLogger()
for h in list(_root.handlers):
    _root.removeHandler(h)
stderr = logging.StreamHandler(stream=sys.stderr)
stderr.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | idnes-price | %(message)s"))
_root.addHandler(stderr)
_root.setLevel(logging.INFO)
log = logging.getLogger("IdnesPriceTrack")

# ========== CONFIG ==========
PROJECT_ROOT = Path(__file__).resolve().parents[2] if len(Path(__file__).resolve().parents) >= 3 else Path.cwd()
cfg_path = PROJECT_ROOT / "config.json"
if not cfg_path.exists():
    alt = Path(__file__).resolve().parent / "config.json"
    cfg_path = alt if alt.exists() else cfg_path

cfg = json.loads(cfg_path.read_text(encoding="utf-8"))

db_user = cfg.get("USER") or cfg.get("user")
db_pwd  = cfg.get("PWD")  or cfg.get("password")
db_host = cfg.get("HOST") or cfg.get("host")
db_port = cfg.get("PORT") or cfg.get("port")
db_name = cfg.get("DB")   or cfg.get("dbname")
DRY_RUN = bool(cfg.get("DRY_RUN", False))

def _make_db_url() -> str:
    try:
        import psycopg  # noqa: F401
        return f"postgresql+psycopg://{db_user}:{db_pwd}@{db_host}:{db_port}/{db_name}"
    except ModuleNotFoundError:
        import psycopg2  # noqa: F401
        return f"postgresql+psycopg2://{db_user}:{db_pwd}@{db_host}:{db_port}/{db_name}"

engine = create_engine(_make_db_url(), pool_pre_ping=True, connect_args={"connect_timeout": 10})

# ========== EMIT JSON → STDOUT ==========
def emit(event: str, **payload) -> None:
    print(json.dumps({"event": event, **payload}, ensure_ascii=False))

# ========== HELPERS ==========
def get_sorted_idnes_tables():
    sql = text("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name LIKE 'idnes\\_%' ESCAPE '\\'
          AND table_name ~ '^idnes_[0-9]{8}$'
    """)
    with engine.begin() as conn:
        tables = [r[0] for r in conn.execute(sql)]
    if not tables:
        return []
    def key(t: str):
        d = t[-8:]  # ddmmyyyy
        return (int(d[4:]), int(d[2:4]), int(d[:2]))  # (year, month, day)
    return sorted(tables, key=key)

def date_from_table_name(name: str) -> str:
    d = name[-8:]
    return f"{d[4:]}-{d[2:4]}-{d[:2]}"  # YYYY-MM-DD

# ========== MAIN ==========
def track_idnes_price_changes_latest():
    t0 = datetime.now()
    try:
        emit("start", stage="idnes_price", dry_run=DRY_RUN)

        tables = get_sorted_idnes_tables()
        if len(tables) < 2:
            msg = "not enough idnes_* tables to compare"
            log.warning(msg)
            emit("error", message=msg)
            print(json.dumps({"stage": "sync_summary", "error": "not_enough_snapshots"}, ensure_ascii=False))
            print("SYNC COMPLETE")
            return

        prev_table, curr_table = tables[-2], tables[-1]
        change_date = date_from_table_name(curr_table)
        emit("snapshots_selected", prev=prev_table, curr=curr_table, change_date=change_date)

        with engine.begin() as conn:
            prev_rows = int(conn.execute(text(f'SELECT COUNT(*) FROM public."{prev_table}"')).scalar_one())
            curr_rows = int(conn.execute(text(f'SELECT COUNT(*) FROM public."{curr_table}"')).scalar_one())

            SQL_COUNT = f"""
                WITH prev AS (
                    SELECT (data->>'id')::text AS id,
                           NULLIF(regexp_replace(data->>'Cena_numeric', '[^0-9\\.,]', '', 'g'), '')::double precision AS price
                    FROM public."{prev_table}"
                    WHERE data ? 'Cena_numeric'
                ),
                curr AS (
                    SELECT (data->>'id')::text AS id,
                           NULLIF(regexp_replace(data->>'Cena_numeric', '[^0-9\\.,]', '', 'g'), '')::double precision AS price
                    FROM public."{curr_table}"
                    WHERE data ? 'Cena_numeric'
                )
                SELECT COUNT(*)
                FROM prev p
                JOIN curr c USING (id)
                WHERE p.price IS NOT NULL
                  AND c.price IS NOT NULL
                  AND p.price <> c.price
            """
            candidates = int(conn.execute(text(SQL_COUNT)).scalar_one())
        emit("candidates_counted", count=candidates, prev_rows=prev_rows, curr_rows=curr_rows)

        inserted = 0
        total_price_rows = None

        with engine.begin() as conn:
            conn.execute(text("SET LOCAL synchronous_commit = off"))
            conn.execute(text("SET LOCAL lock_timeout = '5s'"))
            conn.execute(text("SET LOCAL statement_timeout = '5min'"))

            if not DRY_RUN:
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS public.idnes_price (
                        id TEXT NOT NULL,
                        old_price DOUBLE PRECISION NOT NULL,
                        new_price DOUBLE PRECISION NOT NULL,
                        change_date DATE NOT NULL,
                        PRIMARY KEY (id, old_price, new_price, change_date)
                    )
                """))

            if not DRY_RUN and candidates > 0:
                SQL_INSERT = f"""
                    WITH prev AS (
                        SELECT (data->>'id')::text AS id,
                               NULLIF(regexp_replace(data->>'Cena_numeric', '[^0-9\\.,]', '', 'g'), '')::double precision AS price
                        FROM public."{prev_table}"
                        WHERE data ? 'Cena_numeric'
                    ),
                    curr AS (
                        SELECT (data->>'id')::text AS id,
                               NULLIF(regexp_replace(data->>'Cena_numeric', '[^0-9\\.,]', '', 'g'), '')::double precision AS price
                        FROM public."{curr_table}"
                        WHERE data ? 'Cena_numeric'
                    ),
                    diffs AS (
                        SELECT c.id, p.price AS old_price, c.price AS new_price
                        FROM prev p
                        JOIN curr c USING (id)
                        WHERE p.price IS NOT NULL
                          AND c.price IS NOT NULL
                          AND p.price <> c.price
                    )
                    INSERT INTO public.idnes_price (id, old_price, new_price, change_date)
                    SELECT id, old_price, new_price, CAST(:change_date AS date)
                    FROM diffs
                    ON CONFLICT DO NOTHING
                """
                res = conn.execute(text(SQL_INSERT), {"change_date": change_date})
                inserted = res.rowcount or 0

            total_price_rows = int(conn.execute(text("SELECT COUNT(*) FROM public.idnes_price")).scalar_one()) if not DRY_RUN else None

        log.info(f"[PRICE] candidates={candidates} inserted={inserted} total={total_price_rows} (dry_run={DRY_RUN})")
        emit("insert", inserted=inserted, dry_run=DRY_RUN)

        stats = {
            "prev_table": prev_table,
            "curr_table": curr_table,
            "change_date": change_date,
            "candidates": candidates,
            "inserted_new": inserted,
            "prev_rows": prev_rows,
            "curr_rows": curr_rows,
            "price_rows_total": total_price_rows,
            "dry_run": DRY_RUN
        }
        emit("summary", stats=stats)
        emit("done", duration_s=(datetime.now() - t0).total_seconds())

        print(json.dumps({"stage": "sync_summary", "stats": stats}, ensure_ascii=False))
        print("SYNC COMPLETE")

    except Exception as e:
        log.exception("Fatal error in idnes-price")
        emit("error", message=str(e))
        sys.exit(2)

if __name__ == "__main__":
    track_idnes_price_changes_latest()