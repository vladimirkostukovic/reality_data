#Transforms and maintains sreality_typical using sreality_standart snapshots,
#emitting clean sync_summary JSON for the wrapper orchestrator.

from __future__ import annotations

import sys
import json
import logging
import uuid
from pathlib import Path
from datetime import date
from typing import Tuple

import pandas as pd
from sqlalchemy import create_engine, text

# ========== LOGGING ==========
_root = logging.getLogger()
for h in list(_root.handlers):
    _root.removeHandler(h)

stderr = logging.StreamHandler(stream=sys.stderr)
stderr.setFormatter(logging.Formatter(
    "%(asctime)s | %(levelname)s | sreality-typical | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
))
_root.addHandler(stderr)
_root.setLevel(logging.INFO)

log = logging.getLogger("SrealityTypicalETL")

# ========== EMIT ==========
def emit_json(obj: dict):
    # wrapper парсит только строки, начинающиеся с "{"
    print(json.dumps(obj, ensure_ascii=False))


# ========== CONFIG & ENGINE ==========
PROJECT_ROOT = Path(__file__).resolve().parents[2] if len(Path(__file__).resolve().parents) >= 3 else Path.cwd()
cfg = json.loads((PROJECT_ROOT / "config.json").read_text(encoding="utf-8"))

DB_USER = cfg.get("USER") or cfg.get("user")
DB_PWD  = cfg.get("PWD")  or cfg.get("password")
DB_HOST = cfg.get("HOST") or cfg.get("host")
DB_PORT = cfg.get("PORT") or cfg.get("port")
DB_NAME = cfg.get("DB")   or cfg.get("dbname")

def _make_db_url() -> str:
    try:
        import psycopg
        return f"postgresql+psycopg://{DB_USER}:{DB_PWD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    except ModuleNotFoundError:
        return f"postgresql+psycopg2://{DB_USER}:{DB_PWD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(_make_db_url(), pool_pre_ping=True, connect_args={"connect_timeout": 10})


# ========== HELPERS ==========
def _last_two_snapshot_tables() -> Tuple[str | None, str | None]:
    sql = text("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema='public'
          AND table_name LIKE 'sreality\\_%' ESCAPE '\\'
          AND table_name ~ '^sreality_[0-9]{8}$'
    """)
    with engine.begin() as conn:
        names = [r[0] for r in conn.execute(sql)]

    if not names:
        return (None, None)

    def key(t: str):
        d = t[-8:]
        return (int(d[4:]), int(d[2:4]), int(d[:2]))

    names.sort(key=key)
    if len(names) == 1:
        return (None, names[-1])
    return (names[-2], names[-1])


def _date_from_name(name: str | None) -> str | None:
    if not name:
        return None
    d = name[-8:]
    return f"{d[4:]}-{d[2:4]}-{d[:2]}"


def clean_float(val):
    if pd.isna(val) or (isinstance(val, str) and val.strip().lower() in ("", "nan")):
        return None
    try:
        return float(
            str(val)
            .replace("Kč", "")
            .replace("CZK", "")
            .replace(" ", "")
            .replace("\xa0", "")
            .replace(",", ".")
        )
    except Exception:
        return None


def _col(df: pd.DataFrame, name: str):
    return df[name] if name in df.columns else pd.Series([None] * len(df), index=df.index)


# ========== MAIN ==========
def main():
    try:
        prev_tbl, curr_tbl = _last_two_snapshot_tables()
        snap_date = _date_from_name(curr_tbl) or str(date.today())

        if curr_tbl is None:
            emit_json({"stage": "sync_summary", "stats": {"error": "no_snapshots"}})
            return

        # ---------- LOAD ----------
        with engine.begin() as conn:
            df_std = pd.read_sql(text("""
                SELECT
                  id, added_date, archived_date,
                  category_value, category_name, name,
                  price_summary_czk,
                  district, city, city_part, street, house_number,
                  longitude, latitude
                FROM public.sreality_standart
            """), con=conn)

            df_typ = pd.read_sql(text("""
                SELECT site_id, archived_date
                FROM public.sreality_typical
            """), con=conn)

        if df_std.empty:
            emit_json({
                "stage": "sync_summary",
                "stats": {
                    "snapshot_date": snap_date,
                    "curr_table": curr_tbl,
                    "prev_table": prev_tbl,
                    "total_rows": len(df_typ),
                    "added_today": 0,
                    "archived_today": 0,
                    "active_now": None,
                    "inserted_new": 0,
                    "updated_changed": 0,
                    "phase_counts": {
                        "typical_rows_before": len(df_typ),
                        "inserted": 0,
                        "updated_changed": 0,
                        "archived": 0,
                        "dedup_removed": 0,
                        "source_rows": len(df_std),
                    },
                    "dry_run": False,
                    "status": "ok"
                }
            })
            return

        df_std["id"] = df_std["id"].astype(str)
        df_typ["site_id"] = df_typ["site_id"].astype(str)

        typical_before = len(df_typ)

        # ---------- NEW CANDIDATES ----------
        new_mask = ~df_std["id"].isin(df_typ["site_id"])
        df_new = df_std.loc[new_mask].copy()

        inserted_new = 0
        updated_changed = 0
        dedup_removed = 0

        if not df_new.empty:
            # ---------- TRANSFORM ----------
            def transform(df):
                out = pd.DataFrame(index=df.index)
                out["source_id"] = 1
                out["site_id"] = df["id"].astype(str)
                out["added_date"] = pd.to_datetime(_col(df, "added_date"), errors="coerce")
                out["archived_date"] = pd.to_datetime(_col(df, "archived_date"), errors="coerce")
                out["available"] = out["archived_date"].isna()
                out["category_value"] = _col(df, "category_value")
                out["category_name"]  = _col(df, "category_name")
                out["name"] = _col(df, "name").astype(str)
                out["deal_type"] = out["name"].str.split().str[0].str.lower()
                out["rooms"] = None
                out["price"] = _col(df, "price_summary_czk").apply(clean_float)
                out["area_build"] = None
                out["area_land"]  = None
                for col in ["district","city","city_part","street","house_number"]:
                    out[col] = _col(df, col)
                out["longitude"] = pd.to_numeric(_col(df, "longitude"), errors="coerce")
                out["latitude"]  = pd.to_numeric(_col(df, "latitude"), errors="coerce")
                for col in out.columns:
                    out[col] = out[col].where(pd.notnull(out[col]), None)
                return out

            df_out = transform(df_new)

            # ---------- DEDUP ----------
            with engine.begin() as conn:
                dup_groups = conn.execute(text("""
                    SELECT COUNT(*)
                    FROM (
                      SELECT site_id
                      FROM public.sreality_typical
                      GROUP BY site_id
                      HAVING COUNT(*) > 1
                    ) q
                """)).scalar_one()

                if dup_groups:
                    dedup_removed = conn.execute(text("""
                        WITH r AS (
                          SELECT ctid,
                                 ROW_NUMBER() OVER (PARTITION BY site_id ORDER BY added_date DESC NULLS LAST) AS rn
                          FROM public.sreality_typical
                        )
                        DELETE FROM public.sreality_typical t
                        USING r WHERE r.ctid = t.ctid AND r.rn > 1
                    """)).rowcount or 0

            # ---------- INSERT ----------
            cols = [
                "source_id","site_id","added_date","archived_date","available",
                "category_value","category_name","name","deal_type","rooms",
                "price","area_build","area_land","district","city","city_part",
                "street","house_number","longitude","latitude"
            ]

            staging = f"_stg_typical_{uuid.uuid4().hex[:8]}"

            with engine.begin() as conn:
                conn.execute(text(f'CREATE TABLE public."{staging}" (LIKE public."sreality_typical" INCLUDING ALL)'))

            df_out[cols].to_sql(
                staging,
                con=engine,
                schema="public",
                if_exists="append",
                index=False,
                method="multi",
                chunksize=10_000
            )

            with engine.begin() as conn:
                ins = conn.execute(text(f"""
                    INSERT INTO public."sreality_typical" ({", ".join(cols)})
                    SELECT {", ".join(cols)} FROM public."{staging}"
                    ON CONFLICT (site_id) DO NOTHING
                """))
                inserted_new = ins.rowcount or 0
                conn.execute(text(f'DROP TABLE public."{staging}"'))

        # ---------- UPDATE ----------
        with engine.begin() as conn:
            upd = conn.execute(text("""
                WITH cmp AS (
                  SELECT t.site_id, s.archived_date AS new_archived
                  FROM public.sreality_typical t
                  JOIN public.sreality_standart s ON s.id::text = t.site_id
                  WHERE COALESCE(t.archived_date, '1970-01-01') IS DISTINCT FROM
                        COALESCE(s.archived_date, '1970-01-01')
                )
                UPDATE public.sreality_typical t
                SET archived_date = c.new_archived,
                    available = (c.new_archived IS NULL)
                FROM cmp c
                WHERE t.site_id = c.site_id
            """))
            updated_changed = upd.rowcount or 0

        # ---------- FINAL AGG ----------
        with engine.begin() as conn:
            agg = conn.execute(text("""
                SELECT
                  COUNT(*) AS total_rows,
                  COUNT(*) FILTER (WHERE added_date = CURRENT_DATE) AS added_today,
                  COUNT(*) FILTER (WHERE archived_date::date = CURRENT_DATE) AS archived_today,
                  COUNT(*) FILTER (WHERE available) AS active_now
                FROM public.sreality_typical
            """)).mappings().first()

        stats = {
            "snapshot_date": snap_date,
            "curr_table": curr_tbl,
            "prev_table": prev_tbl,

            "total_rows": int(agg["total_rows"]),
            "added_today": int(agg["added_today"]),
            "archived_today": int(agg["archived_today"]),
            "active_now": int(agg["active_now"]),

            "inserted_new": inserted_new,
            "updated_changed": updated_changed,

            "phase_counts": {
                "typical_rows_before": typical_before,
                "inserted": inserted_new,
                "updated_changed": updated_changed,
                "archived": int(agg["archived_today"]),
                "dedup_removed": int(dedup_removed or 0),
                "source_rows": int(len(df_std)),
            },

            "dry_run": False,
            "status": "ok"
        }

        emit_json({"stage": "sync_summary", "stats": stats})

    except Exception as e:
        log.exception("fatal")
        emit_json({"stage": "sync_summary", "stats": {"status": "failed", "error": str(e)}})
        sys.exit(1)


if __name__ == "__main__":
    main()