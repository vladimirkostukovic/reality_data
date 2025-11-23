#Synchronizes the latest idnes_ snapshot with the idnes_standart master table by inserting
#new listings, reactivating returned ones, archiving missing entries, and producing a single
# standardized JSON summary for the orchestrator.
from __future__ import annotations

import sys
import json
import time
import logging
from pathlib import Path
from datetime import date
from typing import Any, Dict, List

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

# ---------------- LOGGING ----------------
_root = logging.getLogger()
for h in list(_root.handlers):
    _root.removeHandler(h)
stderr = logging.StreamHandler(stream=sys.stderr)
stderr.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
_root.addHandler(stderr)
_root.setLevel(logging.INFO)
log = logging.getLogger("IdnesStep1")

# ---------------- CONFIG ----------------
PROJECT_ROOT = Path(__file__).resolve().parents[2] if len(Path(__file__).resolve().parents) >= 3 else Path.cwd()
cfg_path = PROJECT_ROOT / "config.json"
if not cfg_path.exists():
    alt = Path(__file__).resolve().parent / "config.json"
    cfg_path = alt if alt.exists() else cfg_path

cfg = json.loads(cfg_path.read_text(encoding="utf-8"))

USER = cfg.get("USER") or cfg.get("user")
PWD  = cfg.get("PWD")  or cfg.get("password")
HOST = cfg.get("HOST") or cfg.get("host")
PORT = cfg.get("PORT") or cfg.get("port")
DB   = cfg.get("DB")   or cfg.get("dbname")
DRY_RUN = bool(cfg.get("DRY_RUN", False))


def _make_db_url():
    try:
        import psycopg
        return "postgresql+psycopg://%s:%s@%s:%s/%s" % (USER, PWD, HOST, PORT, DB)
    except ModuleNotFoundError:
        import psycopg2
        return "postgresql+psycopg2://%s:%s@%s:%s/%s" % (USER, PWD, HOST, PORT, DB)


engine = create_engine(_make_db_url(), pool_pre_ping=True, connect_args={"connect_timeout": 10})


# ---------------- HELPERS ----------------

def get_sorted_snapshot_tables() -> List[str]:
    sql = text("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema='public'
          AND table_name LIKE 'idnes\\_%' ESCAPE '\\'
          AND table_name ~ '^idnes_[0-9]{8}$'
    """)
    with engine.begin() as conn:
        tables = [r[0] for r in conn.execute(sql).fetchall()]

    def key(t):
        d = t[-8:]
        return int(d[4:]), int(d[2:4]), int(d[:2])

    return sorted(tables, key=key)


def date_from_tbl(t: str) -> date:
    d = t[-8:]
    return date(int(d[4:]), int(d[2:4]), int(d[:2]))


def normalize_record(r: dict) -> dict:
    return {
        "id": r.get("id"),
        "name": r.get("title"),
        "address": r.get("address"),
        "category_name": r.get("type"),
        "price_czk": r.get("Cena_numeric"),
        "surface": r.get("Užitná plocha_numeric"),
        "scraped_at": r.get("scraped_at"),
        "floor": r.get("floor"),
        "ownership": r.get("ownership"),
        "building_condition": r.get("building_condition"),
        "equipment": r.get("equipment"),
        "energy_label": r.get("energy_label"),
        "parking": r.get("parking"),
        "constructed_area": r.get("Zastavěná plocha_numeric"),
        "building_type": r.get("building_type"),
        "seller_info": r.get("seller_info"),
        "raw_json": json.dumps(r, ensure_ascii=False),
    }


# ---------------- MAIN ----------------

def main():
    t0 = time.perf_counter()

    try:
        tables = get_sorted_snapshot_tables()
        if not tables:
            print(json.dumps({
                "stage": "sync_summary",
                "stats": {"status": "failed", "error": "no_snapshot_tables"}
            }, ensure_ascii=False))
            sys.exit(1)

        latest = tables[-1]
        snapshot_date = date_from_tbl(latest)

        # load raw
        with engine.begin() as conn:
            df_raw = pd.read_sql(text(f'SELECT data FROM public."{latest}"'), conn)

        if df_raw.empty:
            print(json.dumps({
                "stage": "sync_summary",
                "stats": {
                    "snapshot_date": str(snapshot_date),
                    "source_rows": 0,
                    "new_rows": 0,
                    "archived": 0,
                    "reactivated": 0,
                    "total_rows": None,
                    "prev_active_rows": None,
                    "status": "ok",
                    "duration_s": round(time.perf_counter() - t0, 3)
                }
            }, ensure_ascii=False))
            sys.exit(0)

        df = pd.json_normalize(df_raw["data"].dropna())
        df["id"] = df["id"].astype(str)

        records = [normalize_record(x) for x in df.to_dict(orient="records")]
        df_norm = pd.DataFrame(records)
        df_norm["added_date"] = snapshot_date
        df_norm["archived_date"] = pd.NaT
        df_norm["available"] = True

        # load existing
        with engine.begin() as conn:
            try:
                df_exist = pd.read_sql(
                    text('SELECT id, available FROM public."idnes_standart"'),
                    conn
                )
            except Exception:
                df_exist = pd.DataFrame(columns=["id", "available"])

        df_exist["id"] = df_exist["id"].astype(str)

        prev_active = int(df_exist[df_exist["available"] == True].shape[0])

        curr_ids = set(df_norm["id"])
        active_ids = set(df_exist.loc[df_exist["available"] == True, "id"])
        archived_ids = set(df_exist.loc[df_exist["available"] == False, "id"])
        known_ids = set(df_exist["id"])

        # reactivated
        to_reac = list(archived_ids & curr_ids)
        reactivated = len(to_reac)

        if reactivated and not DRY_RUN:
            with engine.begin() as conn:
                conn.execute(text("""
                    UPDATE public.idnes_standart
                    SET available=TRUE, archived_date=NULL
                    WHERE id = ANY(:ids)
                """), {"ids": to_reac})

        # new
        df_new = df_norm[~df_norm["id"].isin(known_ids)]
        new_rows = int(df_new.shape[0])
        if new_rows and not DRY_RUN:
            with engine.begin() as conn:
                df_new.to_sql(
                    "idnes_standart",
                    conn,
                    schema="public",
                    if_exists="append",
                    index=False,
                    method="multi",
                    chunksize=10000
                )

        # arch
        gone = list(active_ids - curr_ids)
        archived = len(gone)
        if archived and not DRY_RUN:
            with engine.begin() as conn:
                conn.execute(text("""
                    UPDATE public.idnes_standart
                    SET archived_date=:dt, available=FALSE
                    WHERE id = ANY(:ids)
                """), {"ids": gone, "dt": snapshot_date})

        # count
        with engine.begin() as conn:
            total_rows = int(conn.execute(text("SELECT COUNT(*) FROM public.idnes_standart")).scalar_one())

        # ---------------- sync_summary ----------------
        summary = {
            "snapshot_date": str(snapshot_date),
            "source_rows": int(df_norm.shape[0]),
            "new_rows": new_rows,
            "archived": archived,
            "reactivated": reactivated,
            "prev_active_rows": prev_active,
            "total_rows": total_rows,
            "status": "ok",
            "duration_s": round(time.perf_counter() - t0, 3)
        }

        print(json.dumps({"stage": "sync_summary", "stats": summary}, ensure_ascii=False))

    except Exception as e:
        print(json.dumps({
            "stage": "sync_summary",
            "stats": {
                "status": "failed",
                "error": str(e)
            }
        }, ensure_ascii=False))
        sys.exit(2)


if __name__ == "__main__":
    main()