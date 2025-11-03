
from __future__ import annotations

import sys
import json
import time
from pathlib import Path
import logging
import pandas as pd
from sqlalchemy import create_engine, text

# === LOGGING  ===
_root = logging.getLogger()
for h in list(_root.handlers):
    _root.removeHandler(h)
stderr_handler = logging.StreamHandler(stream=sys.stderr)
stderr_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
_root.addHandler(stderr_handler)
_root.setLevel(logging.INFO)
log = logging.getLogger("BezrealitkyTypicalETL")

# === CONFIG ===
PROJECT_ROOT = Path(__file__).resolve().parents[2] if len(Path(__file__).resolve().parents) >= 3 else Path.cwd()
cfg_path = PROJECT_ROOT / "config.json"
if not cfg_path.exists():
    alt = Path(__file__).resolve().parent / "config.json"
    cfg_path = alt if alt.exists() else cfg_path

with open(cfg_path, "r", encoding="utf-8") as f:
    cfg = json.load(f)

USER = cfg.get("USER") or cfg.get("user")
PWD  = cfg.get("PWD")  or cfg.get("password")
HOST = cfg.get("HOST") or cfg.get("host")
PORT = cfg.get("PORT") or cfg.get("port")
DB   = cfg.get("DB")   or cfg.get("dbname")
DRY_RUN = bool(cfg.get("DRY_RUN", False))

def make_db_url() -> tuple[str, str]:
    try:
        import psycopg  # noqa: F401
        return ("psycopg", f"postgresql+psycopg://{USER}:{PWD}@{HOST}:{PORT}/{DB}")
    except ModuleNotFoundError:
        try:
            import psycopg2  # noqa: F401
            return ("psycopg2", f"postgresql+psycopg2://{USER}:{PWD}@{HOST}:{PORT}/{DB}")
        except ModuleNotFoundError:
            raise RuntimeError(
                "Нет драйвера Postgres. Установи один:\n"
                "  pip install 'psycopg[binary]>=3.1'\n"
                "  или\n"
                "  pip install 'psycopg2-binary>=2.9'"
            )

driver, DB_URL = make_db_url()
log.info(f"[DB] driver={driver} host={HOST} db={DB}")

engine = create_engine(DB_URL, pool_pre_ping=True, connect_args={"connect_timeout": 10})

# === JSON EMIT → STDOUT ===
def emit(event: str, **payload) -> None:
    print(json.dumps({"event": event, **payload}, ensure_ascii=False))

# === UTILS ===
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

    added    = pd.to_datetime(df.get("added_date"), errors="coerce")
    archived = pd.to_datetime(df.get("archived_date"), errors="coerce")
    price    = df.get("price").apply(clean_float)

    estate_raw   = df.get("estate_type", "").astype(str)
    estate_lower = estate_raw.str.lower()
    image_alt    = df.get("image_alt_text", "").astype(str)
    query        = df.get("query_name", "").astype(str)
    dispo        = df.get("disposition", "").astype(str)
    address      = df.get("address", "").astype(str)

    out["source_id"]     = 2
    out["site_id"]       = df["id"].astype(str)
    out["added_date"]    = added
    out["archived_date"] = archived
    out["available"]     = archived.isna()

    out["category_name"]  = estate_raw
    out["category_value"] = estate_lower.map({"byt": 1, "dum": 2, "pozemek": 3}).astype("Int64")

    out["name"]      = image_alt
    out["deal_type"] = query.str.split("_").str[0].str.lower()
    out["price"]     = price

    out["rooms"] = (
        dispo.str.replace(r"^DISP_", "", regex=True)
             .str.replace("_", "+")
             .str.lower()
             .replace("", None)
    )

    builds = image_alt.str.extract(r"([\d\.]+)\s*m²", expand=False)
    out["area_build"] = builds.apply(clean_area)
    out.loc[~out["category_value"].isin([1, 2]), "area_build"] = None

    all_vals = image_alt.str.findall(r"([\d\.]+)\s*m²")
    def pick(vals, cat):
        if pd.isna(cat):
            return None
        if cat == 2 and isinstance(vals, list) and len(vals) > 1:
            return clean_area(vals[1])
        if cat == 3 and isinstance(vals, list) and vals:
            return clean_area(vals[0])
        return None
    out["area_land"] = [pick(v, cat) for v, cat in zip(all_vals, out["category_value"])]

    parts = address.str.split(",", expand=True).fillna("")
    n = parts.shape[1]
    out["street"]    = parts.iloc[:, 0].str.strip().replace("", None)
    out["city_part"] = parts.iloc[:, n - 2].str.strip().replace("", None) if n >= 2 else None
    out["district"]  = parts.iloc[:, n - 1].str.strip().replace("", None) if n >= 1 else None
    out["city"]      = out["city_part"].astype(str).str.split("-").str[0].str.strip().replace("", None)

    out["house_number"] = None
    out["longitude"]    = None
    out["latitude"]     = None

    # === DIAGNOSTICS → stderr
    diag = {
        "price_null": int(out["price"].isna().sum()),
        "category_null": int(out["category_value"].isna().sum()),
        "area_build_null": int(out["area_build"].isna().sum()),
        "area_land_null": int(out["area_land"].isna().sum()),
        "rooms_null": int(out["rooms"].isna().sum()),
        "deal_type_null": int(out["deal_type"].isna().sum()),
    }
    total = len(out)
    log.info(
        f"[TRANSFORM] total={total} "
        f"price_null={diag['price_null']} "
        f"category_null={diag['category_null']} "
        f"area_build_null={diag['area_build_null']} "
        f"area_land_null={diag['area_land_null']} "
        f"rooms_null={diag['rooms_null']} "
        f"deal_type_null={diag['deal_type_null']}"
    )

    sample_null_price = out[out["price"].isna()].head(3)
    if not sample_null_price.empty:
        log.info(f"[SAMPLE] null price examples:\n"
                 f"{sample_null_price[['site_id','category_name','name','price']].to_dict(orient='records')}")

    out = out.astype(object)
    for c in out.columns:
        out[c] = out[c].where(pd.notnull(out[c]), None)
    return out

# === PIPE ===
def main() -> None:
    t0 = time.perf_counter()
    try:
        emit("start", stage="typical_sync", driver=driver, dry_run=DRY_RUN)

        # определяем последний снапшот по standart
        with engine.begin() as conn:
            snap_row = conn.execute(text("""
                SELECT MAX(added_date::date) AS snapshot_date
                FROM public.bezrealitky_standart
            """)).mappings().first()
        snapshot_date = snap_row["snapshot_date"]
        emit("snapshot_selected", snapshot_date=str(snapshot_date) if snapshot_date else None)

        # === LOAD
        log.info("1. Loading bezrealitky_standart and bezrealitky_typical...")
        with engine.begin() as conn:
            std_cols = [
                "id", "added_date", "archived_date", "available",
                "estate_type", "image_alt_text", "query_name",
                "disposition", "address", "price"
            ]
            col_list = ", ".join([f'"{c}"' for c in std_cols])
            df_std = pd.read_sql_query(f'SELECT {col_list} FROM public."bezrealitky_standart"', con=conn)
            df_typical = pd.read_sql_query(
                'SELECT site_id, archived_date AS archived_old, available AS available_old '
                'FROM public."bezrealitky_typical"', con=conn
            )
        emit("source_loaded", standart_rows=int(df_std.shape[0]), typical_rows=int(df_typical.shape[0]))

        # === FILTER NEW
        df_std["id"] = df_std["id"].astype(str)
        df_typical["site_id"] = df_typical["site_id"].astype(str)
        new_mask = ~df_std["id"].isin(df_typical["site_id"])
        df_new = df_std[new_mask]
        emit("new_detected", count=int(df_new.shape[0]))

        # === INSERT NEW
        inserted = 0
        if not df_new.empty:
            log.info("2. Transforming and inserting new records...")
            df_new_out = transform(df_new)
            inserted = int(len(df_new_out))
            if not DRY_RUN:
                df_new_out.to_sql(
                    "bezrealitky_typical",
                    con=engine,
                    schema="public",
                    if_exists="append",
                    index=False,
                    method="multi",
                    chunksize=10_000
                )
            log.info(f"   → Added: {inserted} rows (dry_run={DRY_RUN})")
        else:
            log.info("2. No new records, skipping insert.")
        emit("insert", inserted=inserted, dry_run=DRY_RUN)

        # === UPDATE + ARCHIVE
        log.info("3. Updating archived_date/available and archiving missing...")
        with engine.begin() as conn:
            conn.execute(text("SET LOCAL lock_timeout = '5s'"))
            conn.execute(text("SET LOCAL statement_timeout = '5min'"))

            upd_count = 0
            arch_count = 0
            if not DRY_RUN:
                upd_res = conn.execute(text("""
                    WITH cur AS (
                      SELECT DISTINCT ON (s.id)
                             s.id::text                   AS site_id,
                             s.archived_date::timestamptz AS archived_date,
                             (s.archived_date IS NULL)    AS available
                      FROM public.bezrealitky_standart s
                      ORDER BY s.id, s.added_date DESC NULLS LAST
                    )
                    UPDATE public.bezrealitky_typical AS t
                    SET archived_date = cur.archived_date,
                        available     = cur.available
                    FROM cur
                    WHERE t.site_id = cur.site_id
                      AND (
                        COALESCE(t.archived_date, '1970-01-01'::timestamptz)
                          IS DISTINCT FROM COALESCE(cur.archived_date, '1970-01-01'::timestamptz)
                        OR t.available IS DISTINCT FROM cur.available
                      )
                """))
                upd_count = upd_res.rowcount or 0

                arch_res = conn.execute(text("""
                    WITH cur AS (
                      SELECT DISTINCT ON (s.id)
                             s.id::text AS site_id
                      FROM public.bezrealitky_standart s
                      ORDER BY s.id, s.added_date DESC NULLS LAST
                    )
                    UPDATE public.bezrealitky_typical AS t
                    SET archived_date = NOW(),
                        available     = FALSE
                    WHERE t.available = TRUE
                      AND NOT EXISTS (SELECT 1 FROM cur WHERE cur.site_id = t.site_id)
                """))
                arch_count = arch_res.rowcount or 0
            log.info(f"   → Updated changed rows: {upd_count} (dry_run={DRY_RUN})")
            log.info(f"   → Archived missing: {arch_count} (dry_run={DRY_RUN})")

        emit("update", updated=upd_count, dry_run=DRY_RUN)
        emit("archive", archived=arch_count, dry_run=DRY_RUN)

        # === STATS (к снапшоту, а не к системной дате)
        with engine.begin() as conn:
            res = conn.execute(text("""
                SELECT 
                  COUNT(*)::bigint AS total_rows,
                  COUNT(*) FILTER (WHERE added_date::date = :d)::bigint AS added_on_snapshot,
                  COUNT(*) FILTER (WHERE archived_date::date = :d)::bigint AS archived_on_snapshot,
                  COUNT(*) FILTER (WHERE available)::bigint AS active_now
                FROM public.bezrealitky_typical;
            """), {"d": snapshot_date}).mappings().first()

        stats = {
            "total_rows": int(res["total_rows"]) if res and res["total_rows"] is not None else 0,
            "added_on_snapshot": int(res["added_on_snapshot"]) if res and res["added_on_snapshot"] is not None else 0,
            "archived_on_snapshot": int(res["archived_on_snapshot"]) if res and res["archived_on_snapshot"] is not None else 0,
            "active_now": int(res["active_now"]) if res and res["active_now"] is not None else 0,
            "snapshot_date": str(snapshot_date) if snapshot_date else None,
            "phase_counts": {
                "standart_rows": int(df_std.shape[0]),
                "typical_rows_before": int(df_typical.shape[0]),
                "inserted": inserted,
                "updated": int(upd_count),
                "archived": int(arch_count),
            },
            "dry_run": DRY_RUN,
        }
        log.info(
            f"[STATS] total={stats['total_rows']} "
            f"added_on_snapshot={stats['added_on_snapshot']} "
            f"archived_on_snapshot={stats['archived_on_snapshot']} "
            f"active_now={stats['active_now']} "
            f"standart_rows={stats['phase_counts']['standart_rows']} "
            f"typical_rows_before={stats['phase_counts']['typical_rows_before']} "
            f"inserted={inserted} updated={upd_count} archived={arch_count}"
        )
        emit("summary", **stats)

        emit("done", duration_s=round(time.perf_counter() - t0, 3))
        # совместимость со старыми обвязками
        print(json.dumps({"stage": "sync_complete"}, ensure_ascii=False))
        sys.exit(0)

    except SystemExit:
        raise
    except Exception as e:
        log.exception("Fatal error in typical sync")
        emit("error", message=str(e))
        sys.exit(2)

if __name__ == "__main__":
    main()