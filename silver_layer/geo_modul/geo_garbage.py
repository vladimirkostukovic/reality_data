#Moves non-normalized or invalid summarized_geo rows into geo_garbage
#based on a strict predicate, logging a compact sync summary for wrapper consumption.

from __future__ import annotations
import sys
import json
import logging
from time import perf_counter
from pathlib import Path
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# === LOGGING ===
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | geo_garbage | %(message)s",
    handlers=[logging.StreamHandler(sys.stderr)],
    force=True,
)
log = logging.getLogger("geo_garbage")

# === CONFIG ===
PROJECT_ROOT = Path(__file__).resolve().parents[2]
with open(PROJECT_ROOT / "config.json", "r", encoding="utf-8") as f:
    cfg = json.load(f)

DB_URL = (
    f"postgresql+psycopg2://{cfg['USER']}:{cfg['PWD']}"
    f"@{cfg['HOST']}:{cfg['PORT']}/{cfg['DB']}"
)
engine = create_engine(DB_URL, pool_pre_ping=True)

SCHEMA = "silver"
SRC = "summarized_geo"
DST = "geo_garbage"

BAD_PREDICATE = """
(
  t.geo_status IS FALSE
  AND (
        t.norm_district IS NULL
     OR BTRIM(t.norm_district) = ''
     OR UPPER(BTRIM(t.norm_district)) = 'NOT_CZ'
  )
)
"""


# === HELPERS ===
def ensure_ingested_col(conn):
    exists = conn.execute(text("""
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema=:s AND table_name=:t AND column_name='ingested_at'
        LIMIT 1
    """), {"s": SCHEMA, "t": DST}).scalar()

    if not exists:
        conn.execute(text(f"""
            ALTER TABLE {SCHEMA}.{DST}
            ADD COLUMN ingested_at timestamptz DEFAULT NOW()
        """))
        conn.execute(text(f"""
            ALTER TABLE {SCHEMA}.{DST}
            ALTER COLUMN ingested_at DROP DEFAULT
        """))


def get_common_columns(conn):
    src_cols = conn.execute(text("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema=:s AND table_name=:t
    """), {"s": SCHEMA, "t": SRC}).scalars().all()

    dst_cols = conn.execute(text("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema=:s AND table_name=:t
    """), {"s": SCHEMA, "t": DST}).scalars().all()

    src_cols = [c for c in src_cols if c != "ingested_at"]
    dst_cols = [c for c in dst_cols if c != "ingested_at"]

    common = [c for c in src_cols if c in dst_cols]

    if "internal_id" not in common:
        raise RuntimeError("Нет общего internal_id между summarized_geo и geo_garbage")

    return common


# === MAIN ===
def main():
    t0 = perf_counter()

    try:
        with engine.begin() as conn:
            ensure_ingested_col(conn)
            common = get_common_columns(conn)

            cols_dst = ", ".join(common + ["ingested_at"])
            cols_sel = ", ".join([f"t.{c}" for c in common])

            sql = f"""
                WITH to_move AS (
                    SELECT {cols_sel}, NOW() AS ingested_at
                    FROM {SCHEMA}.{SRC} t
                    WHERE {BAD_PREDICATE}
                      AND NOT EXISTS (
                        SELECT 1 FROM {SCHEMA}.{DST} g
                        WHERE g.internal_id = t.internal_id
                      )
                ),
                ins AS (
                    INSERT INTO {SCHEMA}.{DST} ({cols_dst})
                    SELECT * FROM to_move
                    RETURNING internal_id
                )
                DELETE FROM {SCHEMA}.{SRC} s
                USING ins i
                WHERE s.internal_id = i.internal_id;
            """

            res = conn.execute(text(sql))
            moved = res.rowcount or 0

        out = {
            "stage": "geo_garbage_sync",
            "status": "ok",
            "sync_summary": {
                "moved": moved,
                "elapsed_s": round(perf_counter() - t0, 3)
            }
        }

        print(json.dumps(out, ensure_ascii=False))
        sys.stdout.flush()

    except Exception as e:
        err = {
            "stage": "geo_garbage_sync",
            "status": "fail",
            "sync_summary": {
                "error": str(e)
            }
        }
        print(json.dumps(err, ensure_ascii=False))
        raise


if __name__ == "__main__":
    main()