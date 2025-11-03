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
log = logging.getLogger("BezrealitkyETL")

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
        import psycopg  # v3
        return ("psycopg", f"postgresql+psycopg://{USER}:{PWD}@{HOST}:{PORT}/{DB}")
    except ModuleNotFoundError:
        try:
            import psycopg2  # v2
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

# === JSON EMIT (stdout) ===
def emit(event: str, **payload) -> None:
    obj = {"event": event, **payload}
    print(json.dumps(obj, ensure_ascii=False))

# === HELPERS ===
def get_sorted_bz_tables() -> list[str]:
    sql = text("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name LIKE 'bzereality\\_%' ESCAPE '\\'
          AND table_name ~ '^bzereality_[0-9]{8}$'
    """)
    with engine.begin() as conn:
        tables = [r[0] for r in conn.execute(sql).fetchall()]

    def table_date_key(t: str):
        d = t[-8:]  # DDMMYYYY
        return int(d[4:]), int(d[2:4]), int(d[:2])  # YYYY, MM, DD

    return sorted(tables, key=table_date_key)

def date_from_table_name(table_name: str) -> pd.Timestamp:
    d = table_name[-8:]  # DDMMYYYY
    return pd.to_datetime(f"{d[4:]}-{d[2:4]}-{d[:2]}")  # YYYY-MM-DD

def get_bezrealitky_cols() -> list[str]:
    return [
        "id",
        "estate_type",
        "offer_type",
        "disposition",
        "image_alt_text",
        "address",
        "surface",
        "price",
        "currency",
        "query_name",
    ]

# === PIPE ===
def sync_bz_latest() -> None:
    t0 = time.perf_counter()
    try:
        emit("start", stage="sync_bezrealitky", driver=driver)
        tables_sorted = get_sorted_bz_tables()
        if not tables_sorted:
            msg = "No bzereality_* tables found for sync"
            log.error(msg)
            emit("error", message=msg)
            sys.exit(1)

        latest_table = tables_sorted[-1]
        snapshot_date = date_from_table_name(latest_table).date()
        log.info(f"Processing latest snapshot: {latest_table} ({snapshot_date})")
        emit("source_selected", table=latest_table, snapshot_date=str(snapshot_date))

        cols = get_bezrealitky_cols()
        cols_insert = ["added_date", "archived_date", "available"] + cols
        select_cols = ", ".join([f'"{c}"' for c in cols])

        # Load source
        t_load0 = time.perf_counter()
        with engine.begin() as conn:
            df = pd.read_sql_query(f'SELECT {select_cols} FROM public."{latest_table}"', con=conn)
        t_load = time.perf_counter() - t_load0

        df["id"] = df["id"].astype(str)
        df["added_date"] = pd.to_datetime(snapshot_date)
        df["archived_date"] = pd.NaT
        df["available"] = True
        df = df[cols_insert]
        emit("source_loaded", rows=len(df), duration_s=round(t_load, 3))

        # Existing active ids
        with engine.begin() as conn:
            try:
                existing_df = pd.read_sql_query(
                    'SELECT id FROM public."bezrealitky_standart" WHERE available = TRUE',
                    con=conn
                )
                existing_ids = set(existing_df["id"].astype(str))
            except Exception:
                existing_ids = set()

        # Add new
        new_df = df[~df["id"].isin(existing_ids)].copy()
        added = int(len(new_df))
        if added and not DRY_RUN:
            new_df["added_date"] = pd.to_datetime(new_df["added_date"])
            new_df["archived_date"] = pd.to_datetime(new_df["archived_date"])
            new_df.to_sql(
                "bezrealitky_standart",
                con=engine,
                schema="public",
                if_exists="append",
                index=False,
                method="multi",
                chunksize=1000,
            )
        log.info(f"[ADD] New records: {added} (dry_run={DRY_RUN})")
        emit("add", count=added, dry_run=DRY_RUN)

        # Reactivate
        with engine.begin() as conn:
            try:
                archived_df = pd.read_sql_query(
                    'SELECT id FROM public."bezrealitky_standart" WHERE available = FALSE',
                    con=conn
                )
                archived_ids = set(archived_df["id"].astype(str))
            except Exception:
                archived_ids = set()

        to_reactivate = list(set(df["id"]) & archived_ids)
        reactivated = int(len(to_reactivate))
        if reactivated and not DRY_RUN:
            with engine.begin() as conn:
                conn.execute(
                    text("""
                        UPDATE public.bezrealitky_standart
                        SET archived_date = NULL, available = TRUE
                        WHERE id = ANY(:ids)
                    """),
                    {"ids": to_reactivate},
                )
        log.info(f"[REACT] Reactivated: {reactivated} (dry_run={DRY_RUN})")
        emit("reactivate", count=reactivated, dry_run=DRY_RUN)

        # Archive missing
        gone_ids = list(existing_ids - set(df["id"]))
        archived = int(len(gone_ids))
        if archived and not DRY_RUN:
            with engine.begin() as conn:
                conn.execute(
                    text("""
                        UPDATE public.bezrealitky_standart
                        SET archived_date = :archive_date, available = FALSE
                        WHERE id = ANY(:gone_ids) AND available = TRUE
                    """),
                    {"archive_date": snapshot_date, "gone_ids": gone_ids},
                )
        log.info(f"[ARCH] Archived: {archived} (dry_run={DRY_RUN})")
        emit("archive", count=archived, dry_run=DRY_RUN)

        # Summary (привязка к snapshot_date, а не к текущему дню)
        with engine.begin() as conn:
            res = conn.execute(text("""
                SELECT 
                  COUNT(*)::bigint AS total_rows,
                  COUNT(*) FILTER (WHERE added_date = :d)::bigint AS added_on_snapshot,
                  COUNT(*) FILTER (WHERE archived_date = :d)::bigint AS archived_on_snapshot,
                  COUNT(*) FILTER (WHERE available)::bigint AS active_now
                FROM public.bezrealitky_standart;
            """), {"d": snapshot_date}).mappings().first()

        stats = {
            "total_rows": int(res["total_rows"]) if res and res["total_rows"] is not None else 0,
            "added_on_snapshot": int(res["added_on_snapshot"]) if res and res["added_on_snapshot"] is not None else 0,
            "archived_on_snapshot": int(res["archived_on_snapshot"]) if res and res["archived_on_snapshot"] is not None else 0,
            "active_now": int(res["active_now"]) if res and res["active_now"] is not None else 0,
            "snapshot_date": str(snapshot_date),
            "phase_counts": {
                "source_rows": int(len(df)),
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

        emit("done", duration_s=round(time.perf_counter() - t0, 3))
        print(json.dumps({"stage": "sync_complete"}, ensure_ascii=False))
        sys.exit(0)

    except SystemExit:
        raise
    except Exception as e:
        log.exception("Fatal error in sync_bz_latest")
        emit("error", message=str(e))
        sys.exit(2)

if __name__ == "__main__":
    sync_bz_latest()