#Tracks day-to-day price changes inside raw idnes_ snapshot tables.
#The script compares the latest two snapshots, detects listings where the price changed,
# writes new change records into public.idnes_price with deduplication
# (PK on id + old_price + new_price + date), and prints a single standardized JSON summary
# so the orchestrator can consume it.
from __future__ import annotations

import sys
import json
import logging
from pathlib import Path
from datetime import datetime
from sqlalchemy import create_engine, text

# ========== LOGGING ==========
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
        import psycopg
        return f"postgresql+psycopg://{db_user}:{db_pwd}@{db_host}:{db_port}/{db_name}"
    except ModuleNotFoundError:
        return f"postgresql+psycopg2://{db_user}:{db_pwd}@{db_host}:{db_port}/{db_name}"

engine = create_engine(_make_db_url(), pool_pre_ping=True, connect_args={"connect_timeout": 10})

# ========== EMIT ==========
def emit(event: str, **payload):
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

    def key(t: str):
        d = t[-8:]  # DDMMYYYY
        return (int(d[4:]), int(d[2:4]), int(d[:2]))

    return sorted(tables, key=key)

def date_from_table(name: str) -> str:
    d = name[-8:]
    return f"{d[4:]}-{d[2:4]}-{d[:2]}"

# ========== MAIN ==========
def track_idnes_price_changes_latest():
    t0 = datetime.now()

    try:
        emit("start", stage="idnes_price", dry_run=DRY_RUN)

        tables = get_sorted_idnes_tables()
        if len(tables) < 2:
            msg = "not enough idnes_* snapshots"
            log.warning(msg)
            final = {"event": "final_summary", "status": "failed", "error": msg}
            print(json.dumps({"stage": "sync_summary", "stats": final}, ensure_ascii=False))
            sys.exit(0)

        prev_table, curr_table = tables[-2], tables[-1]
        snapshot_date = date_from_table(curr_table)
        emit("snapshots_selected", prev=prev_table, curr=curr_table, snapshot_date=snapshot_date)

        with engine.begin() as conn:
            prev_rows = int(conn.execute(text(f'SELECT COUNT(*) FROM public."{prev_table}"')).scalar_one())
            curr_rows = int(conn.execute(text(f'SELECT COUNT(*) FROM public."{curr_table}"')).scalar_one())

            SQL_COUNT = f"""
                WITH prev AS (
                    SELECT (data->>'id')::text AS id,
                           NULLIF(regexp_replace(data->>'Cena_numeric','[^0-9\\.,]','','g'),'')::double precision AS price
                    FROM public."{prev_table}" WHERE data ? 'Cena_numeric'
                ),
                curr AS (
                    SELECT (data->>'id')::text AS id,
                           NULLIF(regexp_replace(data->>'Cena_numeric','[^0-9\\.,]','','g'),'')::double precision AS price
                    FROM public."{curr_table}" WHERE data ? 'Cena_numeric'
                )
                SELECT COUNT(*) FROM prev p
                JOIN curr c USING(id)
                WHERE p.price IS NOT NULL AND c.price IS NOT NULL AND p.price <> c.price
            """
            candidates = int(conn.execute(text(SQL_COUNT)).scalar_one())

        emit("candidates_counted", candidates=candidates)

        inserted = 0
        total_price_rows = None

        with engine.begin() as conn:
            conn.execute(text("SET LOCAL synchronous_commit = off"))

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

                if candidates > 0:
                    SQL_INSERT = f"""
                        WITH prev AS (
                            SELECT (data->>'id')::text AS id,
                                   NULLIF(regexp_replace(data->>'Cena_numeric','[^0-9\\.,]','','g'),'')::double precision AS price
                            FROM public."{prev_table}" WHERE data ? 'Cena_numeric'
                        ),
                        curr AS (
                            SELECT (data->>'id')::text AS id,
                                   NULLIF(regexp_replace(data->>'Cena_numeric','[^0-9\\.,]','','g'),'')::double precision AS price
                            FROM public."{curr_table}" WHERE data ? 'Cena_numeric'
                        ),
                        diffs AS (
                            SELECT c.id, p.price AS old_price, c.price AS new_price
                            FROM prev p JOIN curr c USING(id)
                            WHERE p.price IS NOT NULL AND c.price IS NOT NULL AND p.price <> c.price
                        )
                        INSERT INTO public.idnes_price (id, old_price, new_price, change_date)
                        SELECT id, old_price, new_price, CAST(:dt AS date)
                        FROM diffs
                        ON CONFLICT DO NOTHING
                    """
                    res = conn.execute(text(SQL_INSERT), {"dt": snapshot_date})
                    inserted = res.rowcount or 0

                total_price_rows = int(conn.execute(text("SELECT COUNT(*) FROM public.idnes_price")).scalar_one())

        stats = {
            "dry_run": DRY_RUN,
            "snapshot_date": snapshot_date,
            "prev_table": prev_table,
            "curr_table": curr_table,
            "candidates": candidates,
            "inserted_new": inserted,
            "prev_rows": prev_rows,
            "curr_rows": curr_rows,
            "price_rows_total": total_price_rows,
            "status": "ok",
            "duration_s": round((datetime.now() - t0).total_seconds(), 3)
        }

        emit("summary", **stats)

        print(json.dumps({"stage": "sync_summary", "stats": stats}, ensure_ascii=False))
        sys.exit(0)

    except Exception as e:
        log.exception("fatal")
        err = {
            "event": "final_summary",
            "status": "failed",
            "error": str(e),
            "dry_run": DRY_RUN
        }
        print(json.dumps({"stage": "sync_summary", "stats": err}, ensure_ascii=False))
        sys.exit(2)


if __name__ == "__main__":
    track_idnes_price_changes_latest()