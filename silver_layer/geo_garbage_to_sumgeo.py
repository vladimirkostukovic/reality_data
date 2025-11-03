
from __future__ import annotations
import sys, json, time, logging
from pathlib import Path
from typing import List, Dict

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# ==== LOGGING ====
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | geo_restore | %(message)s",
    handlers=[logging.StreamHandler(sys.stderr)],
    force=True,
)
log = logging.getLogger("geo_restore")

# ==== CONFIG ====
PROJECT_ROOT = Path(__file__).resolve().parents[1]
with open(PROJECT_ROOT / "config.json", "r", encoding="utf-8") as f:
    cfg = json.load(f)

DB_URL = f"postgresql+psycopg2://{cfg['USER']}:{cfg['PWD']}@{cfg['HOST']}:{cfg['PORT']}/{cfg['DB']}"
ENGINE = create_engine(DB_URL, pool_pre_ping=True, future=True)

SCHEMA = "silver"
SRC = "geo_garbage"
DST = "summarized_geo"

def _cols(conn, schema: str, table: str) -> List[str]:
    q = text("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema=:s AND table_name=:t
        ORDER BY ordinal_position
    """)
    return conn.execute(q, {"s": schema, "t": table}).scalars().all()

def _common_cols_for_insert(conn) -> List[str]:
    src = _cols(conn, SCHEMA, SRC)
    dst = _cols(conn, SCHEMA, DST)
    src = [c for c in src if c != "ingested_at"]
    dst = [c for c in dst if c != "ingested_at"]
    common = [c for c in src if c in dst]
    if "internal_id" not in common:
        raise RuntimeError("Нет общего internal_id между geo_garbage и summarized_geo")
    return common

def main():
    t0 = time.perf_counter()
    out: Dict[str, object] = {
        "module": "geo_garbage_restore",
        "ok": True,
        "candidates": 0,
        "inserted": 0,
        "updated": 0,
        "deleted_from_garbage": 0,
        "elapsed_s": 0.0,
    }

    try:
        with ENGINE.begin() as conn:
            conn.execute(text("SET LOCAL statement_timeout = '5min'"))

            conn.execute(text("""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns
                        WHERE table_schema = :s AND table_name = :t AND column_name = 'geo_status'
                    ) THEN
                        EXECUTE format('ALTER TABLE %I.%I ADD COLUMN geo_status boolean', :s, :t);
                    END IF;
                END$$;
            """), {"s": SCHEMA, "t": DST})

            cand = pd.read_sql(f"""
                SELECT g.*
                FROM {SCHEMA}.{SRC} g
                JOIN {SCHEMA}.summarized s USING (internal_id)   -- гарантируем FK
                WHERE g.in_cz IS TRUE
                  AND g.geo_status IS TRUE
            """, con=conn)
            out["candidates"] = int(len(cand))
            if cand.empty:
                log.info("no candidates (in_cz=TRUE & geo_status=TRUE)")
                out["elapsed_s"] = round(time.perf_counter() - t0, 3)
                sys.stdout.write(json.dumps(out) + "\n"); sys.stdout.flush()
                return

            existing_ids = set(pd.read_sql(
                f"SELECT internal_id FROM {SCHEMA}.{DST}",
                con=conn
            )["internal_id"].tolist())

            to_insert = cand[~cand["internal_id"].isin(existing_ids)].copy()
            to_update = cand[cand["internal_id"].isin(existing_ids)].copy()

            log.info("candidates=%d | to_insert=%d | to_update=%d",
                     len(cand), len(to_insert), len(to_update))

            ins_count = 0
            if not to_insert.empty:
                common = _common_cols_for_insert(conn)

                conn.execute(text("DROP TABLE IF EXISTS tmp_geo_restore_ins"))
                conn.execute(text(f"""
                    CREATE TEMP TABLE tmp_geo_restore_ins AS
                    SELECT {', '.join(common)} FROM {SCHEMA}.{SRC} WHERE 1=0
                """))

                to_insert[common].to_sql(
                    "tmp_geo_restore_ins",
                    con=conn,
                    if_exists="append",
                    index=False,
                    method="multi",
                    chunksize=50_000
                )

                insert_cols = common.copy()
                select_cols = [f"t.{c}" for c in common]

                if "geo_status" not in insert_cols:
                    insert_cols.append("geo_status")
                    select_cols.append("TRUE AS geo_status")
                else:
                    pass

                ins_sql = text(f"""
                    INSERT INTO {SCHEMA}.{DST} ({', '.join(insert_cols)}, ingested_at)
                    SELECT {', '.join(select_cols)}, NOW()
                    FROM tmp_geo_restore_ins t
                    WHERE NOT EXISTS (
                        SELECT 1 FROM {SCHEMA}.{DST} d
                        WHERE d.internal_id = t.internal_id
                    )
                """)
                res = conn.execute(ins_sql)
                ins_count = res.rowcount or 0
                out["inserted"] = int(ins_count)
                log.info("inserted=%d", ins_count)

            upd_count = 0
            if not to_update.empty:
                cols_needed = ["internal_id", "norm_district", "norm_okres", "norm_city", "norm_city_part", "norm_street"]
                present = [c for c in cols_needed if c in to_update.columns]
                if "internal_id" not in present:
                    raise RuntimeError("В geo_garbage нет internal_id для update")

                conn.execute(text("DROP TABLE IF EXISTS tmp_geo_restore_upd"))
                conn.execute(text("""
                    CREATE TEMP TABLE tmp_geo_restore_upd (
                        internal_id   BIGINT PRIMARY KEY,
                        norm_district TEXT,
                        norm_okres    TEXT,
                        norm_city     TEXT,
                        norm_city_part TEXT,
                        norm_street   TEXT
                    ) ON COMMIT DROP
                """))

                to_update[present].to_sql(
                    "tmp_geo_restore_upd",
                    con=conn,
                    if_exists="append",
                    index=False,
                    method="multi",
                    chunksize=50_000
                )

                upd_sql = text(f"""
                    UPDATE {SCHEMA}.{DST} AS d
                    SET
                      norm_district  = COALESCE(d.norm_district,  u.norm_district),
                      norm_okres     = COALESCE(d.norm_okres,     u.norm_okres),
                      norm_city      = COALESCE(d.norm_city,      u.norm_city),
                      norm_city_part = COALESCE(d.norm_city_part, u.norm_city_part),
                      norm_street    = COALESCE(d.norm_street,    u.norm_street),
                      geo_status     = TRUE
                    FROM tmp_geo_restore_upd u
                    WHERE d.internal_id = u.internal_id
                """)
                res = conn.execute(upd_sql)
                upd_count = res.rowcount or 0
                out["updated"] = int(upd_count)
                log.info("updated=%d", upd_count)

            if ins_count or upd_count:
                conn.execute(text("""
                    CREATE TEMP TABLE tmp_geo_restore_ids (internal_id BIGINT PRIMARY KEY) ON COMMIT DROP
                """))
                ids = pd.DataFrame({"internal_id": cand["internal_id"].unique()})
                ids.to_sql("tmp_geo_restore_ids", con=conn, if_exists="append", index=False, method="multi")
                del_res = conn.execute(text(f"""
                    DELETE FROM {SCHEMA}.{SRC} g
                    USING tmp_geo_restore_ids i
                    WHERE g.internal_id = i.internal_id
                """))
                out["deleted_from_garbage"] = int(del_res.rowcount or 0)
                log.info("deleted_from_garbage=%d", out["deleted_from_garbage"])

        out["elapsed_s"] = round(time.perf_counter() - t0, 3)
        sys.stdout.write(json.dumps(out) + "\n"); sys.stdout.flush()

    except SQLAlchemyError as e:
        log.error("db_error: %s", e)
        sys.stdout.write(json.dumps({"module": "geo_garbage_restore", "error": str(e)}) + "\n")
        raise
    except Exception as e:
        log.error("unexpected: %s", e)
        sys.stdout.write(json.dumps({"module": "geo_garbage_restore", "error": str(e)}) + "\n")
        raise

if __name__ == "__main__":
    main()