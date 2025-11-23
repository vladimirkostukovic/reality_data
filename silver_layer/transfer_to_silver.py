from __future__ import annotations
import sys, json, time, logging
from pathlib import Path
from datetime import datetime, timezone
import pandas as pd
from sqlalchemy import create_engine, text, Date
from sqlalchemy.exc import SQLAlchemyError

# ===================LOGGING (stderr)================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | silver_merge | %(message)s",
    handlers=[logging.StreamHandler(sys.stderr)],
    force=True,
)
log = logging.getLogger("silver_merge")

# =====================CONFIG===============================
PROJECT_ROOT = Path(__file__).resolve().parents[1]
with open(PROJECT_ROOT / "config.json", "r", encoding="utf-8") as f:
    cfg = json.load(f)

DB_URL = (
    f"postgresql+psycopg2://"
    f"{cfg['USER']}:{cfg['PWD']}@{cfg['HOST']}:{cfg['PORT']}/{cfg['DB']}"
)
engine = create_engine(DB_URL, pool_pre_ping=True)

INGEST_COL = "ingested_at"
INGEST_NOW = datetime.now(timezone.utc)
CHUNK = 50_000

SRC_TABLES = [
    ("public.sreality_typical", 1),
    ("public.bezrealitky_typical", 2),
    ("public.idnes_typical", 3),
]

# ======================HELPERS====================
def ensure_ingest_column(conn):
    exists = conn.execute(text("""
        SELECT 1 FROM information_schema.columns
        WHERE table_schema='silver' AND table_name='summarized'
          AND column_name=:col
    """), {"col": INGEST_COL}).scalar()
    if not exists:
        log.info("add_column ingested_at")
        conn.execute(text(
            f"ALTER TABLE silver.summarized ADD COLUMN {INGEST_COL} timestamptz DEFAULT NOW()"
        ))
        conn.execute(text(
            f"ALTER TABLE silver.summarized ALTER COLUMN {INGEST_COL} DROP DEFAULT"
        ))

def ensure_source_key_and_index(conn):
    conn.execute(text(
        "ALTER TABLE silver.summarized ADD COLUMN IF NOT EXISTS source_id int"
    ))
    conn.execute(text("""
        CREATE INDEX IF NOT EXISTS ix_silver_summarized_source_site
        ON silver.summarized(source_id, site_id)
    """))

def count_rows(conn) -> int:
    return int(conn.execute(text(
        "SELECT COUNT(*) FROM silver.summarized"
    )).scalar() or 0)

def count_sources_total(conn) -> int:
    q = text("""
        SELECT
          COALESCE((SELECT COUNT(DISTINCT site_id::text) FROM public.sreality_typical),0) +
          COALESCE((SELECT COUNT(DISTINCT site_id::text) FROM public.bezrealitky_typical),0) +
          COALESCE((SELECT COUNT(DISTINCT site_id::text) FROM public.idnes_typical),0)
        AS total
    """)
    return int(conn.execute(q).scalar() or 0)

def load_typical(conn, table: str, src_id: int) -> pd.DataFrame:
    df = pd.read_sql(f"SELECT * FROM {table}", con=conn)
    if df.empty:
        return df
    df["source_id"] = src_id
    df["site_id"] = df["site_id"].astype(str)
    return df

def load_existing_keys(conn) -> pd.DataFrame:
    df = pd.read_sql("""
        SELECT source_id, site_id::text AS site_id
        FROM silver.summarized
    """, con=conn)
    if df.empty:
        return pd.DataFrame(columns=["source_id", "site_id"])
    return df

def _coerce_keys(df: pd.DataFrame, who: str) -> pd.DataFrame:
    if "source_id" in df.columns:
        df["source_id"] = (
            pd.to_numeric(df["source_id"], errors="coerce")
              .dropna()
              .astype("int64")
        )
    if "site_id" in df.columns:
        df["site_id"] = df["site_id"].astype("string")
    return df

# ======================MAIN============================
def main():
    t0 = time.perf_counter()
    rows_inserted = 0
    sanity_total_ok = True
    sanity_total_reason = None
    silver_total = None
    expected_total = None

    try:
        with engine.begin() as conn:
            ensure_ingest_column(conn)
            ensure_source_key_and_index(conn)

            before = count_rows(conn)
            log.info("before=%d", before)

            # ==== LOAD SOURCES ====
            dfs = []
            for tbl, sid in SRC_TABLES:
                df = load_typical(conn, tbl, sid)
                log.info("loaded %s rows=%d", tbl, len(df))
                if not df.empty:
                    dfs.append(df)

            if not dfs:
                log.info("no_data_to_merge")
                out = {
                    "stage": "sync_summary",
                    "stats": {
                        "before": before,
                        "inserted": 0,
                        "after": before,
                        "elapsed_s": round(time.perf_counter() - t0, 3),
                        "ingest_ts": INGEST_NOW.isoformat(),
                        "sanity_total_ok": True,
                        "sanity_total": {
                            "silver_total": before,
                            "expected_total": before,
                            "reason": None
                        }
                    }
                }
                print(json.dumps(out, ensure_ascii=False))
                return out

            df_all = pd.concat(dfs, ignore_index=True, sort=False)
            log.info("concat=%d", len(df_all))

            for col in ("added_date", "archived_date"):
                if col in df_all.columns:
                    df_all[col] = pd.to_datetime(df_all[col], errors="coerce").dt.date

            # ==== REMOVE DUPLICATES ====
            before_dedup = len(df_all)
            df_all = df_all.drop_duplicates(subset=["source_id", "site_id"], keep="last")
            dropped = before_dedup - len(df_all)
            if dropped:
                log.info("dropped_duplicates=%d", dropped)

            # ==== AVAILABLE FLAG ====
            if "archived_date" in df_all.columns:
                arch = pd.to_datetime(df_all["archived_date"], errors="coerce")
                df_all["available"] = arch.isna()
            elif "available" not in df_all.columns:
                df_all["available"] = True

            # ==== FILTER EXISTING ====
            log.info("filter existing keys")
            df_existing = load_existing_keys(conn)

            df_all = _coerce_keys(df_all, "df_all")
            if not df_existing.empty:
                df_existing = _coerce_keys(df_existing, "existing")
                merged = df_all.merge(df_existing, on=["source_id", "site_id"], how="left", indicator=True)
                df_all = merged.loc[merged["_merge"] == "left_only", df_all.columns]
                log.info("new_rows=%d", len(df_all))
            else:
                log.info("no_existing_keys insert_all=%d", len(df_all))

            if df_all.empty:
                log.info("no_new_rows_to_insert")
                elapsed = round(time.perf_counter() - t0, 3)
                silver_total = count_rows(conn)
                expected_total = count_sources_total(conn)
                if silver_total != expected_total:
                    sanity_total_ok = False
                    sanity_total_reason = (
                        "duplicates suspected" if silver_total > expected_total
                        else "missing rows"
                    )
                out = {
                    "stage": "sync_summary",
                    "stats": {
                        "before": before,
                        "inserted": 0,
                        "after": before,
                        "elapsed_s": elapsed,
                        "ingest_ts": INGEST_NOW.isoformat(),
                        "sanity_total_ok": sanity_total_ok,
                        "sanity_total": {
                            "silver_total": silver_total,
                            "expected_total": expected_total,
                            "reason": sanity_total_reason
                        }
                    }
                }
                print(json.dumps(out, ensure_ascii=False))
                return out

            # ==== INSERT ====
            df_all[INGEST_COL] = INGEST_NOW

            for col in df_all.columns:
                if pd.api.types.is_float_dtype(df_all[col]) or pd.api.types.is_datetime64_any_dtype(df_all[col]):
                    df_all[col] = df_all[col].where(pd.notnull(df_all[col]), None)

            dtype_map = {}
            for col in ("added_date", "archived_date"):
                if col in df_all.columns:
                    dtype_map[col] = Date()

            df_all.to_sql(
                "summarized",
                schema="silver",
                con=conn,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=CHUNK,
                dtype=dtype_map
            )

            rows_inserted = len(df_all)
            log.info("inserted=%d", rows_inserted)

            after = count_rows(conn)
            log.info("after=%d", after)

            expected_total = count_sources_total(conn)
            silver_total = after

            if silver_total != expected_total:
                sanity_total_ok = False
                sanity_total_reason = (
                    "duplicates suspected" if silver_total > expected_total else "missing rows"
                )
                log.warning("sanity mismatch: silver=%d expected=%d", silver_total, expected_total)

        # ================FINAL JSON (stdout)===========================
        out = {
            "stage": "sync_summary",
            "stats": {
                "before": before,
                "inserted": rows_inserted,
                "after": after,
                "elapsed_s": round(time.perf_counter() - t0, 3),
                "ingest_ts": INGEST_NOW.isoformat(),
                "sanity_total_ok": sanity_total_ok,
                "sanity_total": {
                    "silver_total": silver_total,
                    "expected_total": expected_total,
                    "reason": sanity_total_reason,
                }
            }
        }
        print(json.dumps(out, ensure_ascii=False))
        return out

    except Exception as e:
        log.exception("fatal_error")
        print(json.dumps({"module": "silver_merge", "ok": False, "error": str(e)}))
        raise


if __name__ == "__main__":
    main()