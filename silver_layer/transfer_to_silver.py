
from __future__ import annotations
import sys, json, time, logging
from pathlib import Path
from datetime import datetime, timezone
import pandas as pd
from sqlalchemy import create_engine, text, Date
from sqlalchemy.exc import SQLAlchemyError

# ===== LOGGING =====
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | silver_merge | %(message)s",
    handlers=[logging.StreamHandler(sys.stderr)],
    force=True,
)
log = logging.getLogger("silver_merge")

# ===== CONFIG =====
PROJECT_ROOT = Path(__file__).resolve().parents[1]
with open(PROJECT_ROOT / "config.json", "r", encoding="utf-8") as f:
    cfg = json.load(f)

DB_URL = f"postgresql+psycopg2://{cfg['USER']}:{cfg['PWD']}@{cfg['HOST']}:{cfg['PORT']}/{cfg['DB']}"
engine = create_engine(DB_URL, pool_pre_ping=True)

INGEST_COL = "ingested_at"
INGEST_NOW = datetime.now(timezone.utc)
CHUNK = 50_000

SRC_TABLES = [
    ("public.sreality_typical", 1),
    ("public.bezrealitky_typical", 2),
    ("public.idnes_typical", 3),
]

# ===== HELPERS =====
def ensure_ingest_column(conn):
    exists = conn.execute(text("""
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'silver' AND table_name = 'summarized' AND column_name = :col
    """), {"col": INGEST_COL}).scalar()
    if not exists:
        log.info("ADD COLUMN silver.summarized.%s", INGEST_COL)
        conn.execute(text(f"ALTER TABLE silver.summarized ADD COLUMN {INGEST_COL} timestamptz DEFAULT NOW()"))
        conn.execute(text(f"ALTER TABLE silver.summarized ALTER COLUMN {INGEST_COL} DROP DEFAULT"))

def ensure_source_key_and_index(conn):
    conn.execute(text("ALTER TABLE silver.summarized ADD COLUMN IF NOT EXISTS source_id int"))
    conn.execute(text("""
        CREATE INDEX IF NOT EXISTS ix_silver_summarized_source_site
        ON silver.summarized(source_id, site_id)
    """))

def count_rows(conn) -> int:
    return conn.execute(text("SELECT COUNT(*) FROM silver.summarized")).scalar() or 0

def count_sources_total(conn) -> int:
    q = text("""
        SELECT
          COALESCE((SELECT COUNT(DISTINCT site_id::text) FROM public.sreality_typical),0) +
          COALESCE((SELECT COUNT(DISTINCT site_id::text) FROM public.bezrealitky_typical),0) +
          COALESCE((SELECT COUNT(DISTINCT site_id::text) FROM public.idnes_typical),0) AS total
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
    df = pd.read_sql("SELECT source_id, site_id::text AS site_id FROM silver.summarized", con=conn)
    if df.empty:
        return pd.DataFrame(columns=["source_id", "site_id"])
    return df

def _coerce_keys(df: pd.DataFrame, who: str) -> pd.DataFrame:
    if "source_id" in df.columns:
        before = df["source_id"].dtype
        df["source_id"] = pd.to_numeric(df["source_id"], errors="coerce")
        df = df.dropna(subset=["source_id"])
        df["source_id"] = df["source_id"].astype("int64")
        after = df["source_id"].dtype
        if before != after:
            log.info("%s: cast source_id %s -> %s", who, before, after)
    if "site_id" in df.columns:
        before = df["site_id"].dtype
        df["site_id"] = df["site_id"].astype("string")
        after = df["site_id"].dtype
        if before != after:
            log.info("%s: cast site_id %s -> %s", who, before, after)
    return df

# --- NEW: simple row-count sanity helpers (no *_stats tables) ---
def _table_exists(conn, schema: str, table: str) -> bool:
    q = text("""
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = :s AND table_name = :t
        LIMIT 1
    """)
    return bool(conn.execute(q, {"s": schema, "t": table}).scalar())

def _count_rows_if_exists(conn, schema: str, table: str) -> int | None:
    if not _table_exists(conn, schema, table):
        return None
    return int(conn.execute(text(f"SELECT COUNT(*) FROM {schema}.{table}")).scalar() or 0)

def sanity_rows_count(conn):
    checks = [
        ("public", "sreality_typical"),
        ("public", "bezrealitky_typical"),
        ("public", "idnes_typical"),
    ]
    per_table = {}
    total = 0
    for sch, tbl in checks:
        n = _count_rows_if_exists(conn, sch, tbl)
        if n is not None:
            per_table[f"{sch}.{tbl}"] = n
            total += n
    if not per_table:
        log.info("SANITY rows skipped: no typical tables present")
        return None
    log.info("SANITY rows ok: %s | total=%d", per_table, total)
    return {"per_table": per_table, "total": total}
# --- END NEW ---

# ===== MAIN =====
def main() -> dict:
    t0 = time.perf_counter()
    rows_inserted = 0
    before, after = 0, 0
    sanity_total_ok = True
    sanity_total_reason = None
    expected_total = None
    silver_total = None

    try:
        with engine.begin() as conn:
            ensure_ingest_column(conn)
            ensure_source_key_and_index(conn)

            before = count_rows(conn)
            log.info("before=%d", before)

            log.info("load typicals")
            dfs = []
            for tbl, sid in SRC_TABLES:
                df = load_typical(conn, tbl, sid)
                log.info("loaded %s=%d", tbl, len(df))
                if not df.empty:
                    dfs.append(df)

            if not dfs:
                log.info("no data to merge, all sources empty")
                summary = {"stage": "sync_summary",
                           "stats": {"before": before, "after": before, "inserted": 0, "elapsed_s": 0.0}}
                print(json.dumps(summary, ensure_ascii=False)); sys.stdout.flush()
                return summary

            dfs = [df for df in dfs if not df.empty and not all(df.isna().all())]

            df_all = pd.concat(dfs, ignore_index=True, sort=False)
            log.info("concat=%d", len(df_all))

            for col in ("added_date", "archived_date"):
                if col in df_all.columns:
                    df_all[col] = pd.to_datetime(df_all[col], errors="coerce").dt.date

            before_dedup = len(df_all)
            df_all = df_all.drop_duplicates(subset=["source_id", "site_id"], keep="last")
            dropped = before_dedup - len(df_all)
            if dropped:
                log.info("dropped_duplicates=%d", dropped)


            if "archived_date" in df_all.columns:
                arch = pd.to_datetime(df_all["archived_date"], errors="coerce")
                df_all["available"] = arch.isna()
            elif "available" not in df_all.columns:
                df_all["available"] = True

            log.info("filter existing keys")
            df_existing = load_existing_keys(conn)
            log.info(
                "dtypes before align | df_all: source_id=%s, site_id=%s | existing: source_id=%s, site_id=%s",
                df_all["source_id"].dtype, df_all["site_id"].dtype,
                (df_existing["source_id"].dtype if not df_existing.empty else "n/a"),
                (df_existing["site_id"].dtype if not df_existing.empty else "n/a"),
            )
            df_all = _coerce_keys(df_all, "df_all")
            if not df_existing.empty:
                df_existing = _coerce_keys(df_existing, "existing")
                merged = df_all.merge(df_existing, on=["source_id", "site_id"], how="left", indicator=True)
                new_mask = merged["_merge"].eq("left_only")
                filtered_out = int((~new_mask).sum())
                df_all = merged.loc[new_mask, df_all.columns]
                log.info("new_rows=%d | filtered_out=%d", len(df_all), filtered_out)
            else:
                log.info("no existing keys found — insert all (%d)", len(df_all))

            if df_all.empty:
                log.info("no new rows to insert after filtering")
                elapsed = round(time.perf_counter() - t0, 3)
                silver_total = count_rows(conn)
                expected_total = count_sources_total(conn)
                if silver_total != expected_total:
                    sanity_total_ok = False
                    sanity_total_reason = (
                        "duplicates suspected (silver>expected)" if silver_total > expected_total
                        else "missing rows (silver<expected)"
                    )
                    log.warning("SANITY total mismatch: silver=%d expected=%d (%s)",
                                silver_total, expected_total, sanity_total_reason)
                summary = {
                    "stage": "sync_summary",
                    "stats": {
                        "before": before, "after": before, "inserted": 0,
                        "elapsed_s": elapsed,
                        "sanity_total_ok": sanity_total_ok,
                        "sanity_total": {
                            "silver_total": silver_total,
                            "expected_total": expected_total,
                            "reason": sanity_total_reason
                        }
                    }
                }
                print(json.dumps(summary, ensure_ascii=False)); sys.stdout.flush()
                return summary

            df_all[INGEST_COL] = INGEST_NOW
            for col in df_all.columns:
                if pd.api.types.is_float_dtype(df_all[col]) or pd.api.types.is_datetime64_any_dtype(df_all[col]):
                    df_all[col] = df_all[col].where(pd.notnull(df_all[col]), None)

            dtype_map = {}
            for col in ("added_date", "archived_date"):
                if col in df_all.columns:
                    dtype_map[col] = Date()

            df_all.to_sql(
                "summarized", schema="silver", con=conn,
                if_exists="append", index=False,
                method="multi", chunksize=CHUNK, dtype=dtype_map
            )
            rows_inserted = len(df_all)
            log.info("inserted=%d", rows_inserted)

            after = count_rows(conn)
            log.info("after=%d", after)

            if after != before + rows_inserted:
                log.warning("mass-balance mismatch: after(%d) != before(%d) + inserted(%d)",
                            after, before, rows_inserted)

            expected_total = count_sources_total(conn)
            silver_total = after
            if silver_total != expected_total:
                sanity_total_ok = False
                sanity_total_reason = (
                    "duplicates suspected (silver>expected)" if silver_total > expected_total
                    else "missing rows (silver<expected)"
                )
                log.warning("SANITY total mismatch: silver=%d expected=%d (%s)",
                            silver_total, expected_total, sanity_total_reason)
            else:
                log.info("SANITY total ok: silver==expected==%d", silver_total)

            try:
                dup_row = conn.execute(text("""
                    SELECT COUNT(*) - COUNT(DISTINCT (source_id, site_id)) AS dupes
                    FROM silver.summarized
                """)).scalar() or 0
                if dup_row > 0:
                    log.warning("SANITY unique failed: dupes=%d on (source_id, site_id)", dup_row)
                else:
                    log.info("SANITY unique ok: no duplicates on (source_id, site_id)")
            except Exception as _e:
                log.warning("SANITY unique skipped: %s", _e)

            try:
                nk = conn.execute(text("""
                    SELECT 
                      SUM(CASE WHEN source_id IS NULL THEN 1 ELSE 0 END) AS null_source_id,
                      SUM(CASE WHEN site_id   IS NULL THEN 1 ELSE 0 END) AS null_site_id
                    FROM silver.summarized
                """)).mappings().first() or {}
                if (nk.get("null_source_id") or 0) > 0 or (nk.get("null_site_id") or 0) > 0:
                    log.warning("SANITY null keys: %s", dict(nk))
                else:
                    log.info("SANITY null keys ok")
            except Exception as _e:
                log.warning("SANITY null-keys skipped: %s", _e)

            try:
                bad_av = conn.execute(text("""
                    SELECT COUNT(*) FROM silver.summarized
                    WHERE (archived_date IS NULL  AND available IS NOT TRUE )
                       OR (archived_date IS NOT NULL AND available IS NOT FALSE)
                """)).scalar() or 0
                if bad_av > 0:
                    log.warning("SANITY available/archived mismatch: %d rows", bad_av)
                else:
                    log.info("SANITY available/archived ok")
            except Exception as _e:
                log.warning("SANITY available/archived skipped: %s", _e)

            try:
                sanity_rows_count(conn)
            except Exception as _e:
                log.warning("SANITY rows skipped: %s", _e)

            if before > 0:
                new_rate = rows_inserted / max(1, before)
                if new_rate > 0.5:
                    log.warning("SANITY new-rate high: inserted=%d (%.1f%% of before=%d)",
                                rows_inserted, 100*new_rate, before)
                else:
                    log.info("SANITY new-rate ok: %.1f%%", 100*new_rate)

        elapsed = round(time.perf_counter() - t0, 3)
        result = {
            "stage": "sync_summary",
            "stats": {
                "before": before,
                "inserted": rows_inserted,
                "after": after,
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
        print(json.dumps(result, ensure_ascii=False)); sys.stdout.flush()
        return result

    except SQLAlchemyError as e:
        log.error("failed: %s", e)
        print(json.dumps({"module": "silver_merge", "error": str(e)})); sys.stdout.flush()
        raise
    except Exception as e:
        log.exception("unexpected")
        print(json.dumps({"module": "silver_merge", "error": str(e)})); sys.stdout.flush()
        raise

if __name__ == "__main__":
    main()