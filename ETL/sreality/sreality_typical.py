
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

# ========== LOGGING==========
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

# ========== CONFIG & ENGINE ==========
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

def _make_db_url() -> str:
    try:
        import psycopg  # noqa: F401
        return f"postgresql+psycopg://{DB_USER}:{DB_PWD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    except ModuleNotFoundError:
        import psycopg2  # noqa: F401
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
        d = t[-8:]  # ddmmyyyy
        return (int(d[4:]), int(d[2:4]), int(d[:2]))
    names.sort(key=key)
    if len(names) == 1:
        return (None, names[-1])
    return (names[-2], names[-1])

def _date_from_name(name: str | None) -> str | None:
    if not name or len(name) < 9:
        return None
    d = name[-8:]  # ddmmyyyy
    try:
        return f"{d[4:]}-{d[2:4]}-{d[:2]}"
    except Exception:
        return None

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

def transform(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame(index=df.index)

    out["source_id"] = 1
    out["site_id"] = df["id"].astype(str)

    out["added_date"]    = pd.to_datetime(_col(df, "added_date"), errors="coerce")
    out["archived_date"] = pd.to_datetime(_col(df, "archived_date"), errors="coerce")
    out["available"]     = out["archived_date"].isna()

    out["category_value"] = _col(df, "category_value")
    out["category_name"]  = _col(df, "category_name")
    out["name"]           = _col(df, "name").astype(object)

    name_col = out["name"].astype(str)
    out["deal_type"] = name_col.str.split().str[0].str.lower()

    rooms_tok = name_col.str.extract(r"(?i)\b(\d{1,2}\s*\+\s*(?:kk|[0-5]))\b", expand=False)
    out["rooms"] = rooms_tok.str.replace(r"\s+", "", regex=True).str.lower()

    mask_flat = name_col.str.contains(r"garsoni[ée]ra|garsonk[ayu]?|studio|ateli[eé]r|apartment", case=False, regex=True)
    out.loc[out["rooms"].isna() & mask_flat, "rooms"] = "1+kk"

    valid_rooms = [f"{n}+kk" for n in range(0, 7)] + [f"{n}+{k}" for n in range(0, 7) for k in range(1, 6)]
    out.loc[~out["rooms"].isna() & ~out["rooms"].isin(valid_rooms), "rooms"] = None

    out["price"] = _col(df, "price_summary_czk").apply(clean_float)

    out["area_build"] = None
    out["area_land"]  = None

    mask_house = name_col.str.contains(r"rodinn[ýle] d[oů]m", case=False, regex=True)
    mask_land  = name_col.str.contains(r"pozemek", case=False, regex=True)

    out.loc[~mask_house, "area_build"] = name_col.where(~mask_house).str.extract(r"(\d+)\s*m²", expand=False)
    out.loc[mask_house,  "area_build"] = name_col.where(mask_house).str.extract(r"domu\s*(\d+)\s*m²", expand=False)
    out["area_build"] = out["area_build"].apply(clean_float)

    out.loc[mask_house, "area_land"] = name_col.where(mask_house).str.extract(r"pozemek\s*(\d+)\s*m²", expand=False)
    out.loc[mask_land,  "area_land"] = name_col.where(mask_land).str.extract(r"pozemek\s*(\d+)\s*m²", expand=False)
    out["area_land"] = out["area_land"].apply(clean_float)

    for col in ["district", "city", "city_part", "street", "house_number"]:
        out[col] = _col(df, col).replace("", None)

    out["longitude"] = pd.to_numeric(_col(df, "longitude"), errors="coerce")
    out["latitude"]  = pd.to_numeric(_col(df, "latitude"), errors="coerce")

    out = out.astype(object)
    for col in out.columns:
        out[col] = out[col].where(pd.notnull(out[col]), None)
    return out

# ---------- DEDUP ----------
_DEDUP_COUNT = text("""
    SELECT COUNT(*)::bigint
    FROM (
      SELECT site_id
      FROM public.sreality_typical
      GROUP BY site_id
      HAVING COUNT(*) > 1
    ) d
""")

_DEDUP_DELETE = text("""
    WITH ranked AS (
      SELECT
        ctid,
        ROW_NUMBER() OVER (
          PARTITION BY site_id
          ORDER BY
            added_date NULLS LAST,
            archived_date NULLS LAST,
            ctid
        ) AS rn
      FROM public.sreality_typical
    )
    DELETE FROM public.sreality_typical t
    USING ranked r
    WHERE t.ctid = r.ctid
      AND r.rn > 1
""")

def _dedupe_typical_before_index() -> tuple[int, int]:
    with engine.begin() as conn:
        conn.execute(text("SET LOCAL lock_timeout = '5s'"))
        conn.execute(text("SET LOCAL statement_timeout = '5min'"))
        dup_groups = int(conn.execute(_DEDUP_COUNT).scalar_one() or 0)
        if dup_groups > 0:
            removed = conn.execute(_DEDUP_DELETE).rowcount or 0
            return dup_groups, int(removed)
        return 0, 0

# ========== MAIN ==========
def main():
    try:
        prev_tbl, curr_tbl = _last_two_snapshot_tables()
        log.info("snapshots: prev=%s | curr=%s | curr_date=%s", prev_tbl, curr_tbl, _date_from_name(curr_tbl))

        # 1) load standart
        log.info("load sreality_standart for transform/compare")
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
            log.warning("sreality_standart is empty, nothing to do")
            stats = {
                "total_rows": int(df_typ.shape[0]),
                "added_today": 0,
                "archived_today": 0,
                "active_now": None,
                "inserted_new": 0,
                "updated_changed": 0,
                "curr_table": curr_tbl,
                "prev_table": prev_tbl,
                "snapshot_date": _date_from_name(curr_tbl) or str(date.today()),
                "phase_counts": {"typical_rows_before": int(df_typ.shape[0]), "inserted": 0, "updated_changed": 0},
                "dry_run": False
            }
            print(json.dumps({"stage": "sync_summary", "stats": stats}, ensure_ascii=False))
            print("SYNC COMPLETE")
            return

        df_std["id"] = df_std["id"].astype(str)
        df_typ["site_id"] = df_typ["site_id"].astype(str)

        typical_rows_before = int(df_typ.shape[0])
        log.info("rows: standart=%d | typical=%d", len(df_std), typical_rows_before)

        # 2) new
        new_mask = ~df_std["id"].isin(df_typ["site_id"])
        df_new = df_std.loc[new_mask].copy()
        to_insert = int(df_new.shape[0])
        log.info("new candidates: %d", to_insert)

        inserted_new = 0

        if to_insert:
            df_out = transform(df_new)

            dup_groups, removed = _dedupe_typical_before_index()
            if dup_groups:
                log.info("[DEDUP] site_id groups with duplicates=%d, removed_rows=%d", dup_groups, removed)
            else:
                log.info("[DEDUP] no duplicates in sreality_typical")

            with engine.begin() as conn:
                conn.execute(text("""
                    CREATE UNIQUE INDEX IF NOT EXISTS ux_sreality_typical_site_id
                    ON public.sreality_typical(site_id)
                """))

            cols = [
                "source_id","site_id","added_date","archived_date","available",
                "category_value","category_name","name","deal_type","rooms",
                "price","area_build","area_land","district","city","city_part",
                "street","house_number","longitude","latitude"
            ]

            staging = f"_stg_sreality_typical_{uuid.uuid4().hex[:8]}"
            log.info("staging table: %s", staging)

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

            log.info("inserted_new=%d", inserted_new)
        else:
            log.info("no new rows to insert")

        log.info("update archived_date/available if changed (set-based)")
        upd_sql = text("""
            WITH cmp AS (
              SELECT
                t.site_id,
                s.archived_date AS new_archived
              FROM public."sreality_typical" t
              JOIN public."sreality_standart" s
                ON s.id::text = t.site_id
              WHERE COALESCE(t.archived_date, '1970-01-01'::timestamptz)
                    IS DISTINCT FROM
                    COALESCE(s.archived_date, '1970-01-01'::timestamptz)
            )
            UPDATE public."sreality_typical" AS t
            SET archived_date = c.new_archived,
                available = (c.new_archived IS NULL)
            FROM cmp c
            WHERE t.site_id = c.site_id
        """)
        with engine.begin() as conn:
            res_upd = conn.execute(upd_sql)
            updated_changed = res_upd.rowcount or 0
        log.info("updated_changed=%d", updated_changed)

        with engine.begin() as conn:
            agg = conn.execute(text("""
                SELECT
                  COUNT(*)::bigint AS total_rows,
                  COUNT(*) FILTER (WHERE added_date = CURRENT_DATE)::bigint AS added_today,
                  COUNT(*) FILTER (WHERE archived_date::date = CURRENT_DATE)::bigint AS archived_today,
                  COUNT(*) FILTER (WHERE available)::bigint AS active_now
                FROM public."sreality_typical"
            """)).mappings().first()

        stats = {
            "total_rows": int(agg["total_rows"]) if agg and agg["total_rows"] is not None else 0,
            "added_today": int(agg["added_today"]) if agg and agg["added_today"] is not None else 0,
            "archived_today": int(agg["archived_today"]) if agg and agg["archived_today"] is not None else 0,
            "active_now": int(agg["active_now"]) if agg and agg["active_now"] is not None else 0,
            "inserted_new": inserted_new,
            "updated_changed": updated_changed,
            "curr_table": curr_tbl,
            "prev_table": prev_tbl,
            "snapshot_date": _date_from_name(curr_tbl) or str(date.today()),
            "phase_counts": {
                "typical_rows_before": typical_rows_before,
                "inserted": inserted_new,
                "updated_changed": updated_changed
            },
            "dry_run": False
        }

        log.info(
            "[STATS] total=%d added_today=%d archived_today=%d active_now=%d ins_new=%d upd_changed=%d",
            stats["total_rows"], stats["added_today"], stats["archived_today"], stats["active_now"],
            stats["inserted_new"], stats["updated_changed"]
        )

        print(json.dumps({"stage": "sync_summary", "stats": stats}, ensure_ascii=False))
        print("SYNC COMPLETE")

    except Exception as e:
        log.exception("fatal error")
        print(json.dumps({"stage": "sync_summary", "error": str(e)}, ensure_ascii=False))
        print("SYNC COMPLETE")
        sys.exit(1)

if __name__ == "__main__":
    main()