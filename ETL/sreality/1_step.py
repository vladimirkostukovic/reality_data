#Synchronizes the latest sreality_YYYYMMDD snapshot into sreality_standart by inserting new listings,
# reactivating returned ones,
# archiving missing records, and outputting a standardized sync_summary log.
from __future__ import annotations
import json
import sys
import time
import logging
from pathlib import Path
from datetime import date
from typing import Dict, List

from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

# ========== LOGGING ==========
_root = logging.getLogger()
for h in list(_root.handlers):
    _root.removeHandler(h)
stderr = logging.StreamHandler(stream=sys.stderr)
stderr.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | sreality-standart | %(message)s"))
_root.addHandler(stderr)
_root.setLevel(logging.INFO)
log = logging.getLogger("SrealitySQLSync")

# ========== CONFIG ==========
PROJECT_ROOT = Path(__file__).resolve().parents[2] if len(Path(__file__).resolve().parents) >= 3 else Path.cwd()
cfg = json.loads((PROJECT_ROOT / "config.json").read_text(encoding="utf-8"))

db_url = URL.create(
    drivername="postgresql+psycopg2",
    username=cfg.get("USER") or cfg.get("user"),
    password=cfg.get("PWD") or cfg.get("password"),
    host=cfg.get("HOST") or cfg.get("host"),
    port=int(cfg.get("PORT") or 5432),
    database=cfg.get("DB") or cfg.get("dbname"),
)
engine = create_engine(db_url, pool_pre_ping=True, connect_args={"connect_timeout": 10})

# ========== UTILS ==========
def _sorted_sreality_tables() -> List[str]:
    sql = text("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name LIKE 'sreality\\_%' ESCAPE '\\'
          AND table_name ~ '^sreality_[0-9]{8}$'
    """)
    with engine.begin() as conn:
        rows = [r[0] for r in conn.execute(sql)]
    def key(t: str):
        d = t[-8:]
        return int(d[4:]), int(d[2:4]), int(d[:2])
    return sorted(rows, key=key)

def _date_from_table(tbl: str) -> str:
    d = tbl[-8:]
    return f"{d[4:]}-{d[2:4]}-{d[:2]}"


def _get_table_columns(table: str) -> Dict[str, str]:
    sql = text("""
        SELECT column_name, udt_name
        FROM information_schema.columns
        WHERE table_schema='public' AND table_name=:t
    """)
    with engine.begin() as conn:
        rows = conn.execute(sql, {"t": table}).fetchall()
    return {r[0]: r[1] for r in rows}

def _null_cast(udt: str) -> str:
    mapping = {
        "int2":"smallint","int4":"integer","int8":"bigint",
        "float4":"real","float8":"double precision","numeric":"numeric",
        "bool":"boolean","text":"text","varchar":"text","bpchar":"text",
        "json":"json","jsonb":"jsonb","date":"date",
        "timestamp":"timestamp","timestamptz":"timestamptz"
    }
    return mapping.get(udt, "text")

# ========== MAIN ==========
def sync_last_snapshot_sql_only() -> None:
    t0 = time.perf_counter()

    tables = _sorted_sreality_tables()
    if len(tables) < 2:
        print(json.dumps({
            "stage": "sync_summary",
            "stats": {
                "status": "failed",
                "error": "not_enough_snapshots"
            }
        }, ensure_ascii=False))
        print("SYNC COMPLETE")
        return

    prev_tbl, curr_tbl = tables[-2], tables[-1]
    snap_date = _date_from_table(curr_tbl)
    log.info(f"curr={curr_tbl} prev={prev_tbl} snapshot={snap_date}")

    target = "sreality_standart"
    target_types = _get_table_columns(target)

    target_cols = [
        "added_date","archived_date","avalaible",
        "id","name","price_czk","price_czk_per_sqm","price_summary_czk",
        "status","data_hash","city","city_part","district","street",
        "latitude","longitude","city_seo_name","city_part_seo_name",
        "country","country_id","district_id","district_seo_name",
        "entity_type","geo_hash","house_number","inaccuracy_type",
        "municipality","municipality_id","municipality_seo_name",
        "quarter","quarter_id","region","region_id","region_seo_name",
        "street_id","street_name","street_number","ward","ward_seo_name",
        "ward_id","zip","category_name","category_value"
    ]

    snapshot_cols = set(_get_table_columns(curr_tbl).keys())
    missing = []
    select_exprs = []

    for col in target_cols:
        if col == "added_date":
            select_exprs.append("CAST(:snap_date AS date) AS added_date")
        elif col == "archived_date":
            select_exprs.append("NULL::timestamp AS archived_date")
        elif col == "avalaible":
            select_exprs.append("TRUE AS avalaible")
        else:
            if col in snapshot_cols:
                select_exprs.append(f's."{col}"')
            else:
                missing.append(col)
                udt = target_types.get(col, "text")
                select_exprs.append(f"NULL::{_null_cast(udt)} AS {col}")

    insert_sql = text(f"""
        INSERT INTO public."{target}" ({", ".join(target_cols)})
        SELECT
            {", ".join(select_exprs)}
        FROM public."{curr_tbl}" s
        LEFT JOIN public."{target}" t ON t.id = s.id
        WHERE t.id IS NULL
    """)

    react_sql = text(f"""
        UPDATE public."{target}" t
        SET archived_date = NULL,
            avalaible    = TRUE
        WHERE t.avalaible = FALSE
          AND t.archived_date IS NOT NULL
          AND EXISTS (SELECT 1 FROM public."{curr_tbl}" c WHERE c.id = t.id)
    """)

    archive_sql = text(f"""
        UPDATE public."{target}" t
        SET archived_date = CAST(:snap_date AS date),
            avalaible     = FALSE
        WHERE t.avalaible = TRUE
          AND EXISTS (
                SELECT 1
                FROM public."{prev_tbl}" p
                LEFT JOIN public."{curr_tbl}" c USING (id)
                WHERE p.id = t.id AND c.id IS NULL
          )
    """)

    with engine.begin() as conn:
        conn.execute(text("SET LOCAL lock_timeout = '5s'"))
        conn.execute(text("SET LOCAL statement_timeout = '10min'"))

        r_ins = conn.execute(insert_sql, {"snap_date": snap_date})
        inserted = r_ins.rowcount or 0

        r_rea = conn.execute(react_sql)
        reactivated = r_rea.rowcount or 0

        r_arc = conn.execute(archive_sql, {"snap_date": snap_date})
        archived = r_arc.rowcount or 0

        agg = conn.execute(text(f"""
            SELECT
              COUNT(*)::bigint AS total_rows,
              COUNT(*) FILTER (WHERE added_date = CURRENT_DATE) AS added_today,
              COUNT(*) FILTER (WHERE archived_date::date = CURRENT_DATE) AS archived_today,
              COUNT(*) FILTER (WHERE avalaible) AS active_now
            FROM public."{target}";
        """)).mappings().one()

    source_rows = int(agg["total_rows"])  # fallback

    print(json.dumps({
        "stage": "sync_summary",
        "stats": {
            "snapshot_date": snap_date,
            "curr_table": curr_tbl,
            "prev_table": prev_tbl,

            "total_rows": int(agg["total_rows"]),
            "added_today": int(agg["added_today"]),
            "archived_today": int(agg["archived_today"]),
            "active_now": int(agg["active_now"]),

            "phase_counts": {
                "source_rows": source_rows,
                "added": inserted,
                "reactivated": reactivated,
                "archived": archived
            },

            "dry_run": False,
            "status": "ok",
            "duration_s": round((time.perf_counter() - t0), 3)
        }
    }, ensure_ascii=False))

    print("SYNC COMPLETE")


if __name__ == "__main__":
    sync_last_snapshot_sql_only()