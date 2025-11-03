
from __future__ import annotations

import json
import sys
import logging
from pathlib import Path
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from sqlalchemy.exc import ProgrammingError, OperationalError

# ======= ЛОГИ  =======
_root = logging.getLogger()
for h in list(_root.handlers):
    _root.removeHandler(h)
_stderr = logging.StreamHandler(stream=sys.stderr)
_stderr.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | sreality-price | %(message)s"))
_root.addHandler(_stderr)
_root.setLevel(logging.INFO)
log = logging.getLogger("SrealityPrice")

# ======= CONFIG & ENGINE =======
PROJECT_ROOT = Path(__file__).resolve().parents[2] if len(Path(__file__).resolve().parents) >= 3 else Path.cwd()
cfg_path = PROJECT_ROOT / "config.json"
if not cfg_path.exists():
    alt = Path(__file__).resolve().parent / "config.json"
    cfg_path = alt if alt.exists() else cfg_path
cfg = json.loads(cfg_path.read_text(encoding="utf-8"))

db_url = URL.create(
    drivername=("postgresql+psycopg" if cfg.get("DRIVER") == "psycopg3" else "postgresql+psycopg2"),
    username=cfg.get("USER") or cfg.get("user"),
    password=cfg.get("PWD") or cfg.get("password"),
    host=cfg.get("HOST") or cfg.get("host"),
    port=int(cfg.get("PORT") or 5432),
    database=cfg.get("DB") or cfg.get("dbname"),
)
engine = create_engine(db_url, pool_pre_ping=True, connect_args={"connect_timeout": 10})

# ======= helpers =======
def _sorted_sreality_tables() -> list[str]:
    sql = text("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema='public'
          AND table_name LIKE 'sreality\\_%' ESCAPE '\\'
          AND table_name ~ '^sreality_[0-9]{8}$'
    """)
    with engine.begin() as c:
        names = [r[0] for r in c.execute(sql)]
    def key(t: str):
        d = t[-8:]  # ddmmyyyy
        return int(d[4:]), int(d[2:4]), int(d[:2])  # yyyy,mm,dd
    return sorted(names, key=key)

def _date_from_table(name: str) -> str:
    d = name[-8:]
    return f"{d[4:]}-{d[2:4]}-{d[:2]}"  # YYYY-MM-DD

def _ensure_indexes_or_fallback() -> bool:

    try:
        with engine.begin() as conn:
            conn.execute(text("SET LOCAL lock_timeout = '5s'"))
            conn.execute(text("SET LOCAL statement_timeout = '5min'"))
            # индекс для быстрых чтений — безвредный
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS ix_sreality_price_change_id_date
                  ON public.sreality_price_change (id, change_date)
            """))
            # уникальный для идемпотентности
            conn.execute(text("""
                CREATE UNIQUE INDEX IF NOT EXISTS ux_sreality_price_change_unique
                  ON public.sreality_price_change (id, change_date, old_price, new_price)
            """))
        log.info("[INDEX] unique index present")
        return True
    except (ProgrammingError, OperationalError) as e:
        # бывает, если уже лежат точные дубли и уникальный индекс не построить
        log.warning("[INDEX] unique index unavailable, fallback to anti-join: %s", e)
        return False

# ======= MAIN =======
def main():
    tabs = _sorted_sreality_tables()
    if len(tabs) < 2:
        log.warning("not enough snapshots")
        print(json.dumps({"stage": "sync_summary", "stats": {
            "prev_table": None,
            "curr_table": tabs[-1] if tabs else None,
            "change_date": (_date_from_table(tabs[-1]) if tabs else None),
            "candidates": 0,
            "inserted_new": 0,
            "prev_rows": 0,
            "curr_rows": 0,
            "price_rows_total": 0
        }}, ensure_ascii=False))
        print("SYNC COMPLETE")
        return

    prev_tbl, curr_tbl = tabs[-2], tabs[-1]
    change_date = _date_from_table(curr_tbl)
    log.info("compare %s → %s | change_date=%s", prev_tbl, curr_tbl, change_date)

    have_unique = _ensure_indexes_or_fallback()

    SQL_COUNT = f"""
        WITH prev AS (
            SELECT
                CAST(NULLIF(regexp_replace(id::text, '\\D', '', 'g'), '') AS bigint) AS id,
                NULLIF(regexp_replace(price_czk::text, '[^0-9\\.,]', '', 'g'), '')::double precision AS price
            FROM public."{prev_tbl}"
        ),
        curr AS (
            SELECT
                CAST(NULLIF(regexp_replace(id::text, '\\D', '', 'g'), '') AS bigint) AS id,
                NULLIF(regexp_replace(price_czk::text, '[^0-9\\.,]', '', 'g'), '')::double precision AS price
            FROM public."{curr_tbl}"
        )
        SELECT COUNT(*)::bigint
        FROM prev p
        JOIN curr c USING (id)
        WHERE p.price IS NOT NULL
          AND c.price IS NOT NULL
          AND p.price <> c.price
    """

    if have_unique:
        SQL_INSERT = f"""
            WITH prev AS (
                SELECT
                    CAST(NULLIF(regexp_replace(id::text, '\\D', '', 'g'), '') AS bigint) AS id,
                    NULLIF(regexp_replace(price_czk::text, '[^0-9\\.,]', '', 'g'), '')::double precision AS price
                FROM public."{prev_tbl}"
            ),
            curr AS (
                SELECT
                    CAST(NULLIF(regexp_replace(id::text, '\\D', '', 'g'), '') AS bigint) AS id,
                    NULLIF(regexp_replace(price_czk::text, '[^0-9\\.,]', '', 'g'), '')::double precision AS price
                FROM public."{curr_tbl}"
            ),
            diffs AS (
                SELECT c.id, p.price AS old_price, c.price AS new_price
                FROM prev p
                JOIN curr c USING (id)
                WHERE p.price IS NOT NULL
                  AND c.price IS NOT NULL
                  AND p.price <> c.price
            )
            INSERT INTO public.sreality_price_change (id, old_price, new_price, change_date)
            SELECT d.id, d.old_price, d.new_price, CAST(:change_date AS date)
            FROM diffs d
            ON CONFLICT (id, change_date, old_price, new_price) DO NOTHING
        """
    else:
        SQL_INSERT = f"""
            WITH prev AS (
                SELECT
                    CAST(NULLIF(regexp_replace(id::text, '\\D', '', 'g'), '') AS bigint) AS id,
                    NULLIF(regexp_replace(price_czk::text, '[^0-9\\.,]', '', 'g'), '')::double precision AS price
                FROM public."{prev_tbl}"
            ),
            curr AS (
                SELECT
                    CAST(NULLIF(regexp_replace(id::text, '\\D', '', 'g'), '') AS bigint) AS id,
                    NULLIF(regexp_replace(price_czk::text, '[^0-9\\.,]', '', 'g'), '')::double precision AS price
                FROM public."{curr_tbl}"
            ),
            diffs AS (
                SELECT c.id, p.price AS old_price, c.price AS new_price
                FROM prev p
                JOIN curr c USING (id)
                WHERE p.price IS NOT NULL
                  AND c.price IS NOT NULL
                  AND p.price <> c.price
            )
            INSERT INTO public.sreality_price_change (id, old_price, new_price, change_date)
            SELECT d.id, d.old_price, d.new_price, CAST(:change_date AS date)
            FROM diffs d
            WHERE NOT EXISTS (
                SELECT 1 FROM public.sreality_price_change t
                WHERE t.id = d.id
                  AND t.change_date = CAST(:change_date AS date)
                  AND t.old_price = d.old_price
                  AND t.new_price = d.new_price
            )
        """

    with engine.begin() as conn:
        conn.execute(text("SET LOCAL synchronous_commit = off"))
        conn.execute(text("SET LOCAL lock_timeout = '5s'"))
        conn.execute(text("SET LOCAL statement_timeout = '5min'"))

        candidates = int(conn.execute(text(SQL_COUNT)).scalar_one())
        ins_res = conn.execute(text(SQL_INSERT), {"change_date": change_date})
        inserted = ins_res.rowcount or 0

        prev_rows = int(conn.execute(text(f'SELECT COUNT(*) FROM public."{prev_tbl}"')).scalar_one())
        curr_rows = int(conn.execute(text(f'SELECT COUNT(*) FROM public."{curr_tbl}"')).scalar_one())
        total_price = int(conn.execute(text("SELECT COUNT(*) FROM public.sreality_price_change")).scalar_one())

    log.info("candidates=%d | inserted_new=%d | prev_rows=%d | curr_rows=%d | price_total=%d",
             candidates, inserted, prev_rows, curr_rows, total_price)

    print(json.dumps({
        "stage": "sync_summary",
        "stats": {
            "prev_table": prev_tbl,
            "curr_table": curr_tbl,
            "change_date": change_date,
            "candidates": candidates,
            "inserted_new": inserted,
            "prev_rows": prev_rows,
            "curr_rows": curr_rows,
            "price_rows_total": total_price
        }
    }, ensure_ascii=False))
    print("SYNC COMPLETE")

if __name__ == "__main__":
    try:
        main()
    except Exception:
        log.exception("fatal")
        raise