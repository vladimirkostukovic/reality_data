
from __future__ import annotations

import sys
import json
import math
import logging
from pathlib import Path
from datetime import datetime, date
from typing import List, Dict, Any

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

# ---------- LOGGING----------
_root = logging.getLogger()
for h in list(_root.handlers):
    _root.removeHandler(h)
stderr = logging.StreamHandler(stream=sys.stderr)
stderr.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | idnes | %(message)s"))
_root.addHandler(stderr)
_root.setLevel(logging.INFO)
log = logging.getLogger("IdnesETL")

# ---------- CONFIG & ENGINE ----------
PROJECT_ROOT = Path(__file__).resolve().parents[2] if len(Path(__file__).resolve().parents) >= 3 else Path.cwd()
cfg_path = PROJECT_ROOT / "config.json"
if not cfg_path.exists():
    alt = Path(__file__).resolve().parent / "config.json"
    cfg_path = alt if alt.exists() else cfg_path

with open(cfg_path, "r", encoding="utf-8") as f:
    cfg = json.load(f)

db_user = cfg.get("USER") or cfg.get("user")
db_pwd  = cfg.get("PWD")  or cfg.get("password")
db_host = cfg.get("HOST") or cfg.get("host")
db_port = cfg.get("PORT") or cfg.get("port")
db_name = cfg.get("DB")   or cfg.get("dbname")
DRY_RUN = bool(cfg.get("DRY_RUN", False))

def _make_db_url() -> str:
    try:
        import psycopg  # noqa: F401
        return f"postgresql+psycopg://{db_user}:{db_pwd}@{db_host}:{db_port}/{db_name}"
    except ModuleNotFoundError:
        import psycopg2  # noqa: F401
        return f"postgresql+psycopg2://{db_user}:{db_pwd}@{db_host}:{db_port}/{db_name}"

engine = create_engine(_make_db_url(), pool_pre_ping=True, connect_args={"connect_timeout": 10})

# ---------- EMIT JSON → STDOUT ----------
def emit(event: str, **payload) -> None:
    print(json.dumps({"event": event, **payload}, ensure_ascii=False))

# ---------- УТИЛИТЫ ----------
def get_sorted_snapshot_tables() -> list[str]:
    sql = text("""
        SELECT table_name 
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name LIKE 'idnes\\_%' ESCAPE '\\'
          AND table_name ~ '^idnes_[0-9]{8}$'
    """)
    with engine.begin() as conn:
        rows = conn.execute(sql).fetchall()
    tables = [r[0] for r in rows]

    def table_date_key(t: str):
        d = t[-8:]  # DDMMYYYY
        return int(d[4:]), int(d[2:4]), int(d[:2])  # YYYY,MM,DD

    return sorted(tables, key=table_date_key)

def date_from_table_name(table_name: str) -> date:
    d = table_name[-8:]
    return date(int(d[4:]), int(d[2:4]), int(d[:2]))

def sanitize_val(x):
    if pd.isna(x) or x is pd.NaT:
        return None
    if isinstance(x, (np.integer, np.int64, np.int32, np.int16, np.int8)):
        return int(x)
    if isinstance(x, (np.floating, np.float64, np.float32, np.float16)):
        return float(x)
    if isinstance(x, (dict, list)):
        return json.dumps(x, ensure_ascii=False)
    return x

def sanitize_batch(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    df = df.copy()
    for c in cols:
        if c not in df.columns:
            df[c] = None
    df = df[cols]
    for col in df.columns:
        df[col] = df[col].apply(sanitize_val)
    return df

def clean_nan(obj):
    if isinstance(obj, dict):
        return {k: clean_nan(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [clean_nan(i) for i in obj]
    elif isinstance(obj, float) and math.isnan(obj):
        return None
    elif pd.isna(obj):
        return None
    else:
        return obj

def extract_row_fields(row: dict) -> dict:
    return {
        "id": row.get("id"),
        "name": row.get("title"),
        "address": row.get("address"),
        "category_name": row.get("type"),
        "price_czk": row.get("Cena_numeric", None),
        "surface": row.get("Užitná plocha_numeric", None),
        "scraped_at": row.get("scraped_at"),
        "floor": row.get("floor"),
        "ownership": row.get("ownership"),
        "building_condition": row.get("building_condition"),
        "equipment": row.get("equipment"),
        "energy_label": row.get("energy_label"),
        "parking": row.get("parking"),
        "constructed_area": row.get("Zastavěná plocha_numeric", None),
        "building_type": row.get("building_type"),
        "seller_info": row.get("seller_info"),
        "raw_json": json.dumps(clean_nan(row), ensure_ascii=False),
    }

# ---------- main ----------
def sync_latest_snapshot():
    t0 = datetime.now()
    try:
        emit("start", stage="idnes_sync", dry_run=DRY_RUN)

        cols = [
            "id", "name", "address", "category_name", "price_czk", "surface", "scraped_at", "floor",
            "ownership", "building_condition", "equipment", "energy_label", "parking", "constructed_area",
            "building_type", "seller_info", "raw_json",
        ]
        cols_insert = ["added_date", "archived_date", "available"] + cols

        tables_sorted = get_sorted_snapshot_tables()
        if not tables_sorted:
            msg = "No snapshot tables found"
            log.error(msg)
            emit("error", message=msg)
            print(json.dumps({"stage": "sync_summary", "error": "no_snapshot_tables"}, ensure_ascii=False))
            print("SYNC COMPLETE")
            return

        latest_table = tables_sorted[-1]
        snapshot_date = date_from_table_name(latest_table)
        log.info("Latest snapshot: %s | date: %s", latest_table, snapshot_date.isoformat())
        emit("source_selected", table=latest_table, snapshot_date=str(snapshot_date))

        with engine.begin() as conn:
            df_raw = pd.read_sql(text(f'SELECT * FROM public."{latest_table}"'), con=conn)

        if "data" not in df_raw.columns or df_raw.empty:
            log.warning("Empty snapshot or missing 'data' column")
            emit("source_loaded", rows=int(df_raw.shape[0]), has_data_col=("data" in df_raw.columns))
            emit("summary", stats={
                "total_rows": None, "added_on_snapshot": 0, "archived_on_snapshot": 0,
                "active_now": None, "snapshot_date": str(snapshot_date),
                "phase_counts": {"source_rows": int(df_raw.shape[0]), "added": 0, "reactivated": 0, "archived": 0},
                "dry_run": DRY_RUN
            })
            print(json.dumps({"stage": "sync_summary", "stats": {
                "snapshot_date": str(snapshot_date),
                "total_rows": None, "added_on_snapshot": 0, "archived_on_snapshot": 0, "active_now": None
            }}, ensure_ascii=False))
            print("SYNC COMPLETE")
            return

        df_data = pd.json_normalize(df_raw["data"].dropna())
        if "id" not in df_data.columns:
            log.warning("No 'id' found after json_normalize")
            emit("source_loaded", rows=int(df_raw.shape[0]), has_id=False)
            print(json.dumps({"stage": "sync_summary", "stats": {
                "snapshot_date": str(snapshot_date),
                "total_rows": None, "added_on_snapshot": 0, "archived_on_snapshot": 0, "active_now": None
            }}, ensure_ascii=False))
            print("SYNC COMPLETE")
            return

        df_data["id"] = df_data["id"].astype(str)
        df_norm = pd.DataFrame([extract_row_fields(row) for row in df_data.to_dict(orient="records")])
        df_norm["added_date"] = pd.to_datetime(snapshot_date)
        df_norm["archived_date"] = pd.NaT
        df_norm["available"] = True
        curr_ids = set(df_norm["id"])
        emit("source_loaded", rows=int(len(df_norm)))

        try:
            with engine.begin() as conn:
                existing_df = pd.read_sql(
                    text('SELECT id, available FROM public."idnes_standart"'),
                    con=conn,
                )
            existing_df["id"] = existing_df["id"].astype(str)
        except Exception:
            existing_df = pd.DataFrame(columns=["id", "available"])

        active_ids = set(existing_df.loc[existing_df["available"] == True, "id"])
        archived_ids = set(existing_df.loc[existing_df["available"] == False, "id"])

        reac_ids = list(archived_ids & curr_ids)
        reactivated = len(reac_ids)
        if reactivated and not DRY_RUN:
            with engine.begin() as conn:
                conn.execute(
                    text("""
                        UPDATE public.idnes_standart
                        SET available = TRUE, archived_date = NULL
                        WHERE id = ANY(:reac_ids) AND available = FALSE
                    """),
                    {"reac_ids": reac_ids},
                )
        log.info(f"[REACT] {reactivated} (dry_run={DRY_RUN})")
        emit("reactivate", count=reactivated, dry_run=DRY_RUN)

        all_known_ids = set(existing_df["id"])
        df_new = df_norm[~df_norm["id"].isin(all_known_ids)].copy()
        df_standart = sanitize_batch(df_new, cols_insert)
        added = int(len(df_standart))
        if added and not DRY_RUN:
            with engine.begin() as conn:
                df_standart.to_sql(
                    "idnes_standart",
                    con=conn,
                    schema="public",
                    if_exists="append",
                    index=False,
                    method="multi",
                    chunksize=10_000
                )
        log.info(f"[ADD] {added} (dry_run={DRY_RUN})")
        emit("add", count=added, dry_run=DRY_RUN)

        gone_ids = list(active_ids - curr_ids)
        archived = int(len(gone_ids))
        if archived and not DRY_RUN:
            with engine.begin() as conn:
                conn.execute(
                    text("""
                        UPDATE public.idnes_standart
                        SET archived_date = :archived_date, available = FALSE
                        WHERE id = ANY(:gone_ids) AND available = TRUE
                    """),
                    {"archived_date": snapshot_date, "gone_ids": gone_ids},
                )
        log.info(f"[ARCH] {archived} (dry_run={DRY_RUN})")
        emit("archive", count=archived, dry_run=DRY_RUN)

        with engine.begin() as conn:
            res = conn.execute(text("""
                SELECT 
                  COUNT(*)::bigint AS total_rows,
                  COUNT(*) FILTER (WHERE added_date::date = :d)::bigint AS added_on_snapshot,
                  COUNT(*) FILTER (WHERE archived_date::date = :d)::bigint AS archived_on_snapshot,
                  COUNT(*) FILTER (WHERE available)::bigint AS active_now
                FROM public.idnes_standart;
            """), {"d": snapshot_date}).mappings().first()

        stats = {
            "total_rows": int(res["total_rows"]) if res and res["total_rows"] is not None else 0,
            "added_on_snapshot": int(res["added_on_snapshot"]) if res and res["added_on_snapshot"] is not None else 0,
            "archived_on_snapshot": int(res["archived_on_snapshot"]) if res and res["archived_on_snapshot"] is not None else 0,
            "active_now": int(res["active_now"]) if res and res["active_now"] is not None else 0,
            "snapshot_date": str(snapshot_date),
            "phase_counts": {
                "source_rows": int(len(df_norm)),
                "added": added,
                "reactivated": reactivated,
                "archived": archived,
            },
            "dry_run": DRY_RUN,
        }

        log.info(
            f"[STATS] total={stats['total_rows']} "
            f"added_on_snapshot={stats['added_on_snapshot']} "
            f"archived_on_snapshot={stats['archived_on_snapshot']} "
            f"active_now={stats['active_now']} "
            f"source_rows={stats['phase_counts']['source_rows']} "
            f"added={added} reactivated={reactivated} archived={archived}"
        )
        emit("summary", **stats)
        emit("done", duration_s=(datetime.now() - t0).total_seconds())

        print(json.dumps({"stage": "sync_summary", "stats": stats}, ensure_ascii=False))
        print("SYNC COMPLETE")

    except Exception as e:
        log.exception("Fatal error in idnes sync")
        emit("error", message=str(e))
        sys.exit(2)

if __name__ == "__main__":
    sync_latest_snapshot()