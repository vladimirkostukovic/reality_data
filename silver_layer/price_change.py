import sys
import json
import logging
import time
from pathlib import Path

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# ==== LOGGING ====
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | price_change | %(message)s",
    handlers=[logging.StreamHandler(sys.stderr)],
    force=True,
)
log = logging.getLogger("price_change")

# ==== CONFIG ====
PROJECT_ROOT = Path(__file__).resolve().parents[1]
with open(PROJECT_ROOT / "config.json", "r", encoding="utf-8") as f:
    cfg = json.load(f)

DB_URL = f"postgresql+psycopg2://{cfg['USER']}:{cfg['PWD']}@{cfg['HOST']}:{cfg['PORT']}/{cfg['DB']}"
engine = create_engine(DB_URL, pool_pre_ping=True)

SCHEMA = "silver"
TARGET = "price_change"

# ==== UTILS ====
def ensure_ingested_at(conn):
    exists = conn.execute(text("""
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema=:sch AND table_name=:tbl AND column_name='ingested_at'
        LIMIT 1
    """), {"sch": SCHEMA, "tbl": TARGET}).scalar()
    if not exists:
        log.info(f"ADD COLUMN {SCHEMA}.{TARGET}.ingested_at")
        conn.execute(text(f"""
            ALTER TABLE {SCHEMA}.{TARGET}
            ADD COLUMN ingested_at timestamptz DEFAULT NOW()
        """))
        conn.execute(text(f"ALTER TABLE {SCHEMA}.{TARGET} ALTER COLUMN ingested_at DROP DEFAULT"))

def ensure_target_indexes(conn):
    conn.execute(text(f"""
        CREATE INDEX IF NOT EXISTS ix_{SCHEMA}_{TARGET}_pk3
        ON {SCHEMA}.{TARGET} ((id::text), change_date, source_id)
    """))

def count_target(conn) -> int:
    return int(conn.execute(text(f"SELECT COUNT(*) FROM {SCHEMA}.{TARGET}")).scalar() or 0)

def _process_df(df: pd.DataFrame, source_id: int) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["id", "old_price", "new_price", "change_date", "source_id"])

    s = df["id"].astype("string").str.strip()
    s = s.where(s.ne(""), pd.NA)
    df["id"] = s

    df["old_price"] = pd.to_numeric(df["old_price"], errors="coerce")
    df["new_price"] = pd.to_numeric(df["new_price"], errors="coerce")
    df["change_date"] = pd.to_datetime(df["change_date"], errors="coerce").dt.date
    df["source_id"] = int(source_id)

    mask_nan = df[["id", "old_price", "new_price", "change_date"]].isna().any(axis=1)
    drop_nan = int(mask_nan.sum())
    if drop_nan:
        log.info(f"sanitize:src={source_id} drop_nan={drop_nan}")
        df = df.loc[~mask_nan]

    MAX_PRICE = 10_000_000_000
    mask_bad = (df["old_price"].abs() >= MAX_PRICE) | (df["new_price"].abs() >= MAX_PRICE)
    drop_bad = int(mask_bad.sum())
    if drop_bad:
        log.info(f"sanitize:src={source_id} drop_price_outlier={drop_bad}")
        df = df.loc[~mask_bad]

    before = len(df)
    df = df.drop_duplicates(subset=["id", "change_date", "source_id"], keep="last")
    if before - len(df):
        log.info(f"dedup:src={source_id} local_dups={before - len(df)}")

    return df[["id", "old_price", "new_price", "change_date", "source_id"]]

def load_source(conn, sql: str, label: str) -> pd.DataFrame:
    log.info(f"load:{label}")
    df = pd.read_sql(sql, con=conn)
    log.info(f"{label}: rows={len(df)}")
    return df

def stage_and_insert(conn, df: pd.DataFrame) -> int:
    if df.empty:
        return 0

    conn.execute(text("SET LOCAL synchronous_commit = OFF"))

    conn.execute(text("DROP TABLE IF EXISTS tmp_price_stage"))
    conn.execute(text("""
        CREATE TEMP TABLE tmp_price_stage (
            id          TEXT,
            old_price   NUMERIC,
            new_price   NUMERIC,
            change_date DATE,
            source_id   INT
        ) ON COMMIT DROP
    """))

    df.to_sql("tmp_price_stage", con=conn, if_exists="append",
              index=False, method="multi", chunksize=50_000)

    conn.execute(text("ANALYZE tmp_price_stage"))

    res = conn.execute(text(f"""
        INSERT INTO {SCHEMA}.{TARGET} (id, old_price, new_price, change_date, source_id, ingested_at)
        SELECT t.id, t.old_price, t.new_price, t.change_date, t.source_id, NOW()
        FROM tmp_price_stage t
        WHERE NOT EXISTS (
            SELECT 1
            FROM {SCHEMA}.{TARGET} s
            WHERE s.id::text = t.id::text
              AND s.change_date = t.change_date
              AND s.source_id = t.source_id
        )
    """))

    return res.rowcount or 0

# ==== MAIN ====
def main():
    t0 = time.perf_counter()
    before = after = staged = inserted = 0

    try:
        with engine.begin() as conn:
            ensure_ingested_at(conn)
            ensure_target_indexes(conn)

            before = count_target(conn)

            df1 = load_source(conn, "SELECT id, old_price, new_price, change_date FROM sreality_price_change", "sreality_price_change")
            df2 = load_source(conn, "SELECT id, old_price, new_price, change_date FROM bezrealitky_price", "bezrealitky_price")
            df3 = load_source(conn, "SELECT id, old_price, new_price, change_date FROM idnes_price", "idnes_price")

            df1 = _process_df(df1, 1)
            df2 = _process_df(df2, 2)
            df3 = _process_df(df3, 3)

            df = pd.concat([df1, df2, df3], ignore_index=True)
            staged = len(df)

            b = len(df)
            df = df.drop_duplicates(subset=["id", "change_date", "source_id"], keep="last")
            if b - len(df):
                log.info(f"dedup:global_dups={b - len(df)}")

            if df.empty:
                after = count_target(conn)
                out = {
                    "module": "price_change_sync",
                    "before": int(before),
                    "after": int(after),
                    "staged": 0,
                    "inserted": 0,
                    "status": "ok",
                    "elapsed_s": round(time.perf_counter() - t0, 3)
                }
                sys.stdout.write(json.dumps(out) + "\n")
                return

            inserted = stage_and_insert(conn, df)
            after = count_target(conn)

        out = {
            "module": "price_change_sync",
            "before": int(before),
            "after": int(after),
            "staged": int(staged),
            "inserted": int(inserted),
            "status": "ok",
            "elapsed_s": round(time.perf_counter() - t0, 3)
        }
        sys.stdout.write(json.dumps(out) + "\n")

    except Exception as e:
        log.error(f"unexpected: {e}")
        sys.stdout.write(json.dumps({
            "module": "price_change_sync",
            "error": str(e),
            "status": "fail",
            "elapsed_s": round(time.perf_counter() - t0, 3)
        }) + "\n")
        raise

if __name__ == "__main__":
    main()