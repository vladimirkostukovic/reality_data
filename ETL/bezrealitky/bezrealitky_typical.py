#Synchronizes the normalized bezrealitky_typical table with the latest state of bezrealitky_standart
#by inserting new listings, updating changed ones, archiving missing entries,
#and emitting a single final JSON summary for the orchestrator.

from __future__ import annotations

import sys
import json
import time
from pathlib import Path
import logging
import pandas as pd
from sqlalchemy import create_engine, text

# === LOGGING ===
_root = logging.getLogger()
for h in list(_root.handlers):
    _root.removeHandler(h)
stderr = logging.StreamHandler(stream=sys.stderr)
stderr.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
_root.addHandler(stderr)
_root.setLevel(logging.INFO)
log = logging.getLogger("BezrealitkyTypical")

# === CONFIG ===
PROJECT_ROOT = Path(__file__).resolve().parents[2] if len(Path(__file__).resolve().parents) >= 3 else Path.cwd()
cfg_path = PROJECT_ROOT / "config.json"
if not cfg_path.exists():
    alt = Path(__file__).resolve().parent / "config.json"
    cfg_path = alt if alt.exists() else cfg_path

cfg = json.loads(cfg_path.read_text(encoding="utf-8"))

USER = cfg.get("USER") or cfg.get("user")
PWD = cfg.get("PWD") or cfg.get("password")
HOST = cfg.get("HOST") or cfg.get("host")
PORT = cfg.get("PORT") or cfg.get("port")
DB = cfg.get("DB") or cfg.get("dbname")
DRY_RUN = bool(cfg.get("DRY_RUN", False))


def make_db_url() -> str:
    try:
        import psycopg
        return f"postgresql+psycopg://{USER}:{PWD}@{HOST}:{PORT}/{DB}"
    except ModuleNotFoundError:
        import psycopg2
        return f"postgresql+psycopg2://{USER}:{PWD}@{HOST}:{PORT}/{DB}"


DB_URL = make_db_url()
engine = create_engine(DB_URL, pool_pre_ping=True, connect_args={"connect_timeout": 10})


# === HELPERS ===
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


def clean_area(val):
    if not isinstance(val, str):
        return None
    try:
        return float(val.replace(".", "").replace(",", "."))
    except Exception:
        return None


def transform(df: pd.DataFrame) -> pd.DataFrame:
    idx = df.index
    out = pd.DataFrame(index=idx)

    added = pd.to_datetime(df["added_date"], errors="coerce")
    archived = pd.to_datetime(df["archived_date"], errors="coerce")
    price = df["price"].apply(clean_float)

    estate_raw = df["estate_type"].astype(str)
    estate_lower = estate_raw.str.lower()
    alt = df["image_alt_text"].astype(str)

    query = df["query_name"].astype(str)
    dispo = df["disposition"].astype(str)
    address = df["address"].astype(str)

    out["source_id"] = 2
    out["site_id"] = df["id"].astype(str)
    out["added_date"] = added
    out["archived_date"] = archived
    out["available"] = archived.isna()

    out["category_name"] = estate_raw
    out["category_value"] = estate_lower.map({"byt": 1, "dum": 2, "pozemek": 3}).astype("Int64")

    out["name"] = alt
    out["deal_type"] = query.str.split("_").str[0].str.lower()
    out["price"] = price

    out["rooms"] = (
        dispo.str.replace(r"^DISP_", "", regex=True)
        .str.replace("_", "+")
        .str.lower()
        .replace("", None)
    )

    build = alt.str.extract(r"([\d\.]+)\s*m²", expand=False)
    out["area_build"] = build.apply(clean_area)
    out.loc[~out["category_value"].isin([1, 2]), "area_build"] = None

    all_vals = alt.str.findall(r"([\d\.]+)\s*m²")

    def pick(vals, cat):
        if pd.isna(cat):
            return None
        if cat == 2 and len(vals) > 1:
            return clean_area(vals[1])
        if cat == 3 and len(vals) > 0:
            return clean_area(vals[0])
        return None

    out["area_land"] = [pick(v, c) for v, c in zip(all_vals, out["category_value"])]

    parts = address.str.split(",", expand=True).fillna("")
    n = parts.shape[1]
    out["street"] = parts.iloc[:, 0].str.strip().replace("", None)
    out["city_part"] = parts.iloc[:, n - 2].str.strip().replace("", None) if n >= 2 else None
    out["district"] = parts.iloc[:, n - 1].str.strip().replace("", None) if n >= 1 else None
    out["city"] = out["city_part"].astype(str).str.split("-").str[0].str.strip().replace("", None)

    out["house_number"] = None
    out["longitude"] = None
    out["latitude"] = None

    out = out.astype(object)
    for c in out.columns:
        out[c] = out[c].where(pd.notnull(out[c]), None)

    return out


# === PIPE ===
def run():
    t0 = time.perf_counter()
    try:
        # snapshot_date
        with engine.begin() as conn:
            snap = conn.execute(text("""
                SELECT MAX(added_date::date) AS snapshot_date
                FROM public.bezrealitky_standart
            """)).mappings().first()

        snapshot_date = snap["snapshot_date"]

        # load both tables
        with engine.begin() as conn:
            df_std = pd.read_sql_query("""
                SELECT id, added_date, archived_date, available,
                       estate_type, image_alt_text, query_name,
                       disposition, address, price
                FROM public.bezrealitky_standart
            """, con=conn)

            df_typ = pd.read_sql_query("""
                SELECT site_id FROM public.bezrealitky_typical
            """, con=conn)

        standart_rows = df_std.shape[0]
        typical_before = df_typ.shape[0]

        # NEW rows
        df_std["id"] = df_std["id"].astype(str)
        df_typ["site_id"] = df_typ["site_id"].astype(str)

        new_mask = ~df_std["id"].isin(df_typ["site_id"])
        df_new = df_std[new_mask]
        inserted = len(df_new)

        if inserted and not DRY_RUN:
            out_new = transform(df_new)
            out_new.to_sql(
                "bezrealitky_typical",
                con=engine,
                schema="public",
                if_exists="append",
                index=False,
                method="multi",
                chunksize=2000
            )

        # UPDATE + ARCHIVE
        upd_count = 0
        arch_count = 0

        if not DRY_RUN:
            with engine.begin() as conn:
                upd = conn.execute(text("""
                    WITH cur AS (
                      SELECT DISTINCT ON (s.id)
                        s.id::text AS site_id,
                        s.archived_date::timestamptz AS archived_date,
                        (s.archived_date IS NULL) AS available
                      FROM public.bezrealitky_standart s
                      ORDER BY s.id, s.added_date DESC NULLS LAST
                    )
                    UPDATE public.bezrealitky_typical t
                    SET archived_date = cur.archived_date,
                        available = cur.available
                    FROM cur
                    WHERE t.site_id = cur.site_id
                      AND (
                        COALESCE(t.archived_date, '1970-01-01')
                         IS DISTINCT FROM COALESCE(cur.archived_date, '1970-01-01')
                        OR t.available IS DISTINCT FROM cur.available
                      )
                """))
                upd_count = upd.rowcount or 0

                arch = conn.execute(text("""
                    WITH cur AS (
                      SELECT DISTINCT ON (s.id) s.id::text AS site_id
                      FROM public.bezrealitky_standart s
                      ORDER BY s.id, s.added_date DESC NULLS LAST
                    )
                    UPDATE public.bezrealitky_typical t
                    SET archived_date = NOW(), available = FALSE
                    WHERE t.available = TRUE
                      AND NOT EXISTS (SELECT 1 FROM cur WHERE cur.site_id = t.site_id)
                """))
                arch_count = arch.rowcount or 0

        # Total rows
        with engine.begin() as conn:
            total_rows = conn.execute(text("""
                SELECT COUNT(*) FROM public.bezrealitky_typical
            """)).scalar()

        # FINAL JSON for wrapper
        final_stats = {
            "dry_run": DRY_RUN,
            "snapshot_date": str(snapshot_date),
            "new_rows": inserted,
            "updated": upd_count,
            "archived": arch_count,
            "standart_rows": standart_rows,
            "typical_before": typical_before,
            "total_rows": total_rows,
            "status": "ok",
            "duration_s": round(time.perf_counter() - t0, 3)
        }

        print(json.dumps({
            "stage": "sync_summary",
            "stats": final_stats
        }, ensure_ascii=False))

        sys.exit(0)

    except Exception as e:
        log.exception("Fatal error in typical ETL")

        err = {
            "dry_run": DRY_RUN,
            "status": "failed",
            "error": str(e),
            "snapshot_date": str(snapshot_date) if "snapshot_date" in locals() else None,
            "duration_s": round(time.perf_counter() - t0, 3)
        }

        print(json.dumps({
            "stage": "sync_summary",
            "stats": err
        }, ensure_ascii=False))

        sys.exit(2)


if __name__ == "__main__":
    run()