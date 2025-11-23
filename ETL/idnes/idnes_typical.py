#Script processes the latest idnes_* snapshot, normalizes records, inserts missing listings into
# idnes_typical, updates existing ones, archives disappeared IDs, enforces a unique site_id index,
# removes duplicates, and outputs a clean standardized JSON summary for the orchestrator.
from __future__ import annotations

import sys
import json
import logging
from pathlib import Path
from datetime import datetime, date
from typing import List

import pandas as pd
from sqlalchemy import create_engine, text

# ========== LOGGING ==========
_root = logging.getLogger()
for h in list(_root.handlers):
    _root.removeHandler(h)
stderr = logging.StreamHandler(stream=sys.stderr)
stderr.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | idnes-typical | %(message)s"))
_root.addHandler(stderr)
_root.setLevel(logging.INFO)
log = logging.getLogger("IdnesTypicalETL")

# ========== CONFIG ==========
PROJECT_ROOT = Path(__file__).resolve().parents[2] if len(Path(__file__).resolve().parents) >= 3 else Path.cwd()
cfg_path = PROJECT_ROOT / "config.json"
if not cfg_path.exists():
    alt = Path(__file__).resolve().parent / "config.json"
    cfg_path = alt if alt.exists() else cfg_path

with open(cfg_path, "r", encoding="utf-8") as f:
    cfg = json.load(f)

DB_USER = cfg.get("USER") or cfg.get("user")
DB_PWD  = cfg.get("PWD")  or cfg.get("password")
DB_HOST = cfg.get("HOST") or cfg.get("host")
DB_PORT = cfg.get("PORT") or cfg.get("port")
DB_NAME = cfg.get("DB")   or cfg.get("dbname")
DRY_RUN = bool(cfg.get("DRY_RUN", False))

def _make_db_url() -> str:
    try:
        import psycopg  # noqa
        return f"postgresql+psycopg://{DB_USER}:{DB_PWD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    except ModuleNotFoundError:
        return f"postgresql+psycopg2://{DB_USER}:{DB_PWD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(_make_db_url(), pool_pre_ping=True, connect_args={"connect_timeout": 10})

# ========== EMIT JSON → STDOUT ==========
def emit(event: str, **payload) -> None:
    print(json.dumps({"event": event, **payload}, ensure_ascii=False))

# ========== HELPERS ==========
def _get_latest_idnes_table() -> str | None:
    sql = text("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema='public'
          AND table_name LIKE 'idnes\\_%' ESCAPE '\\'
          AND table_name ~ '^idnes_[0-9]{8}$'
    """)
    with engine.begin() as conn:
        rows = [r[0] for r in conn.execute(sql)]

    if not rows:
        return None

    def key(t: str):
        d = t[-8:]
        return int(d[4:]), int(d[2:4]), int(d[:2])

    return sorted(rows, key=key)[-1]

def _date_from_tbl(tbl: str) -> date:
    d = tbl[-8:]
    return date(int(d[4:]), int(d[2:4]), int(d[:2]))

def _transform(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame(index=df.index)
    out["site_id"]       = df["id"].astype(str)
    out["added_date"]    = pd.to_datetime(df["added_date"], errors="coerce")
    out["archived_date"] = pd.to_datetime(df["archived_date"], errors="coerce")
    out["available"]     = out["archived_date"].isna()
    out["source_id"]     = 3

    raw = df.get("category_name", "").astype(str).str.lower()
    out["category_value"] = raw.map({"apartments": 1, "houses": 2, "land": 3}).fillna(5).astype("Int64")
    out["category_name"]  = raw.map({"apartments": "byt", "houses": "dum", "land": "pozemek"}).fillna("ostatni")

    name_col = df.get("name", "").astype(str)
    out["name"]      = name_col
    out["deal_type"] = name_col.str.split().str[0].str.lower()

    price_series = df.get("price_czk", df.get("price"))
    out["price"] = pd.to_numeric(
        price_series.astype(str)
        .str.replace("\xa0", "", regex=False)
        .str.replace(" ", "", regex=False)
        .str.replace(",", ".", regex=False),
        errors="coerce"
    )

    out["rooms"] = name_col.str.extract(r"\b(\d+\+\w+)\b", expand=False)

    addr = df.get("address", "").fillna("").astype(str)
    parts = addr.str.split(",", expand=True).fillna("")
    out["street"]    = parts.iloc[:, 0].str.strip().replace("", None)
    out["city_part"] = parts.iloc[:, 1].str.strip().replace("", None) if parts.shape[1] > 1 else None
    out["city"]      = parts.iloc[:, -1].str.strip().replace("", None)
    out["district"]  = addr.replace("", None)

    out["area_build"]   = None
    out["area_land"]    = None
    out["house_number"] = None
    out["longitude"]    = None
    out["latitude"]     = None

    out = out.astype(object)
    for c in out.columns:
        out[c] = out[c].where(pd.notnull(out[c]), None)

    return out


def _has_unique_site_id(conn) -> bool:
    q = text("""
        SELECT 1
        FROM pg_index i
        JOIN pg_class t ON t.oid = i.indrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(i.indkey)
        WHERE n.nspname = 'public'
          AND t.relname = 'idnes_typical'
          AND i.indisunique
        GROUP BY i.indexrelid, i.indnatts
        HAVING i.indnatts = 1 AND bool_and(a.attname = 'site_id')
        LIMIT 1
    """)
    return conn.execute(q).first() is not None


def _count_dupes_site_id(conn) -> int:
    q = text("""
        SELECT COUNT(*) FROM (
          SELECT site_id
          FROM public."idnes_typical"
          GROUP BY site_id
          HAVING COUNT(*) > 1
        ) s
    """)
    return int(conn.execute(q).scalar_one())


def _dedupe_site_id(conn) -> int:
    q = text("""
        WITH ranked AS (
          SELECT ctid,
                 ROW_NUMBER() OVER (
                   PARTITION BY site_id
                   ORDER BY archived_date NULLS FIRST,
                            added_date DESC NULLS LAST,
                            ctid
                 ) AS rn
          FROM public."idnes_typical"
        )
        DELETE FROM public."idnes_typical" t
        USING ranked r
        WHERE t.ctid = r.ctid
          AND r.rn > 1
        RETURNING 1
    """)
    return len(conn.execute(q).fetchall())


def _ensure_unique_index() -> int:
    if DRY_RUN:
        return 0
    removed = 0
    with engine.begin() as conn:
        conn.execute(text("SET LOCAL lock_timeout = '5s'"))
        conn.execute(text("SET LOCAL statement_timeout = '10min'"))

        if not _has_unique_site_id(conn):
            dupes = _count_dupes_site_id(conn)

            if dupes > 0:
                removed = _dedupe_site_id(conn)
                log.warning(f"[DEDUP] removed {removed} duplicates")

            conn.execute(text("""
                CREATE UNIQUE INDEX IF NOT EXISTS ux_idnes_typical_site_id
                ON public."idnes_typical"(site_id)
            """))

    return removed

# ========== MAIN ==========
def main():
    t0 = datetime.now()
    snapshot_date = None

    try:
        emit("start", stage="idnes_typical", dry_run=DRY_RUN)

        latest_tbl = _get_latest_idnes_table()
        if not latest_tbl:
            raise RuntimeError("No idnes_* snapshot tables found")

        snapshot_date = _date_from_tbl(latest_tbl)
        emit("source_selected", table=latest_tbl, snapshot_date=str(snapshot_date))

        with engine.begin() as conn:
            df_std = pd.read_sql(
                text('SELECT id, added_date, archived_date, name, price_czk, category_name, address FROM public."idnes_standart"'),
                con=conn
            )
            df_typ = pd.read_sql(
                text('SELECT site_id, archived_date AS archived_old FROM public."idnes_typical"'),
                con=conn
            )

        df_std["id"] = df_std["id"].astype(str)
        df_typ["site_id"] = df_typ["site_id"].astype(str)

        typical_before = int(df_typ.shape[0])
        emit("source_loaded", standart_rows=int(df_std.shape[0]), typical_rows=typical_before)

        new_mask = ~df_std["id"].isin(df_typ["site_id"])
        df_new = df_std[new_mask]
        to_insert = int(df_new.shape[0])
        emit("new_detected", count=to_insert)

        inserted = updated = archived = dedup_removed = 0

        if to_insert:
            df_out = _transform(df_new)

            dedup_removed = _ensure_unique_index()
            if dedup_removed:
                emit("dedup", removed=dedup_removed)

            cols: List[str] = [
                "site_id","added_date","archived_date","available","source_id",
                "category_value","category_name","name","deal_type","price","rooms",
                "area_build","area_land","street","city_part","city","district",
                "house_number","longitude","latitude"
            ]

            if not DRY_RUN:
                with engine.begin() as conn:
                    conn.execute(text("SET LOCAL lock_timeout = '5s'"))
                    conn.execute(text("SET LOCAL statement_timeout = '5min'"))
                    conn.execute(text("DROP TABLE IF EXISTS tmp_idnes_typical"))
                    conn.execute(text(
                        'CREATE TEMP TABLE tmp_idnes_typical (LIKE public."idnes_typical" INCLUDING ALL)'
                    ))

                    df_out[cols].to_sql(
                        "tmp_idnes_typical",
                        con=conn,
                        if_exists="append",
                        index=False,
                        method="multi",
                        chunksize=10_000
                    )

                    ins = conn.execute(text(f"""
                        INSERT INTO public."idnes_typical" ({", ".join(cols)})
                        SELECT {", ".join(cols)} FROM tmp_idnes_typical
                        ON CONFLICT (site_id) DO NOTHING
                    """))
                    inserted = ins.rowcount or 0

                    conn.execute(text("DROP TABLE IF EXISTS tmp_idnes_typical"))

        emit("insert", inserted=inserted, dry_run=DRY_RUN)

        if not DRY_RUN:
            with engine.begin() as conn:
                upd = conn.execute(text("""
                    UPDATE public."idnes_typical" AS t
                    SET archived_date = s.archived_date,
                        available = (s.archived_date IS NULL)
                    FROM public."idnes_standart" AS s
                    WHERE t.site_id = s.id::text
                      AND COALESCE(t.archived_date,'1970-01-01') 
                          IS DISTINCT FROM COALESCE(s.archived_date,'1970-01-01')
                """))
                updated = upd.rowcount or 0

        emit("update", updated=updated, dry_run=DRY_RUN)

        if not DRY_RUN:
            with engine.begin() as conn:
                arc = conn.execute(text("""
                    UPDATE public."idnes_typical" t
                    SET archived_date = NOW(),
                        available = FALSE
                    WHERE t.available = TRUE
                      AND NOT EXISTS (
                          SELECT 1 FROM public."idnes_standart" s 
                          WHERE s.id::text = t.site_id
                      )
                """))
                archived = arc.rowcount or 0

        emit("archive", archived=archived, dry_run=DRY_RUN)

        with engine.begin() as conn:
            agg = conn.execute(text("""
                SELECT 
                  COUNT(*) AS total_rows,
                  COUNT(*) FILTER (WHERE added_date::date = :d) AS added_today,
                  COUNT(*) FILTER (WHERE archived_date::date = :d) AS archived_today,
                  COUNT(*) FILTER (WHERE available) AS active_now
                FROM public."idnes_typical";
            """), {"d": snapshot_date}).mappings().first()

        stats = {
            "total_rows": int(agg["total_rows"]),
            "added_today": int(agg["added_today"]),
            "archived_today": int(agg["archived_today"]),
            "active_now": int(agg["active_now"]),
            "snapshot_date": str(snapshot_date),
            "phase_counts": {
                "typical_rows_before": typical_before,
                "inserted": inserted,
                "updated": updated,
                "archived": archived,
                "dedup_removed": dedup_removed
            },
            "dry_run": DRY_RUN
        }

        final = {
            "stage": "sync_summary",
            "stats": {
                **stats,
                "status": "ok",
                "duration_s": (datetime.now() - t0).total_seconds()
            }
        }

        print(json.dumps(final, ensure_ascii=False))
        sys.exit(0)

    except Exception as e:
        log.exception("Fatal error in idnes-typical")

        fail = {
            "stage": "sync_summary",
            "stats": {
                "status": "failed",
                "error": str(e),
                "snapshot_date": str(snapshot_date) if snapshot_date else None,
                "dry_run": DRY_RUN,
                "duration_s": (datetime.now() - t0).total_seconds()
            }
        }

        print(json.dumps(fail, ensure_ascii=False))
        sys.exit(2)


if __name__ == "__main__":
    main()