#Extracts listing photo metadata from the latest sreality_ snapshot
# and stores new photo entries into sreality_photo.
from __future__ import annotations

import json
import sys
import logging
from pathlib import Path
from datetime import datetime
from sqlalchemy import create_engine, text

# ================= LOGGING =================
_root = logging.getLogger()
for h in list(_root.handlers):
    _root.removeHandler(h)
stderr = logging.StreamHandler(stream=sys.stderr)
stderr.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | sreality-photo | %(message)s"))
_root.addHandler(stderr)
_root.setLevel(logging.INFO)
log = logging.getLogger("SrealityPhotoExtract")

def emit(event: str, **payload) -> None:
    print(json.dumps({"event": event, **payload}, ensure_ascii=False))


# ================= CONFIG =================
PROJECT_ROOT = Path(__file__).resolve().parents[2] if len(Path(__file__).resolve().parents) >= 3 else Path.cwd()
cfg_path = PROJECT_ROOT / "config.json"
if not cfg_path.exists():
    alt = Path(__file__).resolve().parent / "config.json"
    cfg_path = alt if alt.exists() else cfg_path

cfg = json.loads(cfg_path.read_text(encoding="utf-8"))

USER = cfg.get("USER") or cfg.get("user")
PWD  = cfg.get("PWD") or cfg.get("password")
HOST = cfg.get("HOST") or cfg.get("host")
PORT = cfg.get("PORT") or cfg.get("port")
DB   = cfg.get("DB") or cfg.get("dbname")

def _make_db_url():
    try:
        import psycopg
        return f"postgresql+psycopg://{USER}:{PWD}@{HOST}:{PORT}/{DB}"
    except ModuleNotFoundError:
        return f"postgresql+psycopg2://{USER}:{PWD}@{HOST}:{PORT}/{DB}"

engine = create_engine(_make_db_url(), pool_pre_ping=True, connect_args={"connect_timeout": 10})


# ================= HELPERS =================
def _sorted_snapshot_tables() -> list[str]:
    sql = text("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema='public'
        AND table_name LIKE 'sreality\\_%' ESCAPE '\\'
        AND table_name ~ '^sreality_[0-9]{8}$'
    """)
    with engine.begin() as conn:
        tabs = [r[0] for r in conn.execute(sql)]
    def key(t: str):
        d = t[-8:]
        return (int(d[4:]), int(d[2:4]), int(d[:2]))
    return sorted(tabs, key=key)

def _date_from_table(t: str) -> str:
    d = t[-8:]
    return f"{d[4:]}-{d[2:4]}-{d[:2]}"

def _has_cols(table: str, cols: list[str]) -> bool:
    with engine.begin() as conn:
        colset = {r[0] for r in conn.execute(
            text("""SELECT column_name FROM information_schema.columns
                    WHERE table_schema='public' AND table_name=:t"""),
            {"t": table}
        )}
    return all(c in colset for c in cols)


# ================= MAIN =================
def main():
    t0 = datetime.now()

    try:
        emit("start", stage="sreality_photo")

        tabs = _sorted_snapshot_tables()
        if not tabs:
            stats = {
                "snapshot_table": None,
                "snapshot_date": None,
                "source_rows": 0,
                "inserted_new": 0,
                "photo_rows_total": None,
                "dry_run": False,
                "status": "failed",
                "duration_s": round((datetime.now() - t0).total_seconds(), 3)
            }
            print(json.dumps({"stage": "sync_summary", "stats": stats}, ensure_ascii=False))
            return

        latest = tabs[-1]
        snap_date = _date_from_table(latest)
        emit("source_selected", table=latest, snapshot_date=snap_date)

        if not _has_cols(latest, ["id", "images"]):
            stats = {
                "snapshot_table": latest,
                "snapshot_date": snap_date,
                "source_rows": 0,
                "inserted_new": 0,
                "photo_rows_total": None,
                "dry_run": False,
                "status": "failed",
                "duration_s": round((datetime.now() - t0).total_seconds(), 3)
            }
            print(json.dumps({"stage": "sync_summary", "stats": stats}, ensure_ascii=False))
            return

        with engine.begin() as conn:
            conn.execute(text("SET LOCAL lock_timeout = '5s'"))
            conn.execute(text("SET LOCAL statement_timeout = '5min'"))
            conn.execute(text("""
                CREATE UNIQUE INDEX IF NOT EXISTS ux_sreality_photo_id
                ON public.sreality_photo(id)
            """))

            src_rows = int(conn.execute(text(f'SELECT COUNT(*) FROM public."{latest}"')).scalar_one())

            insert_sql = text(f"""
                WITH src AS (
                  SELECT
                    id::bigint AS id,
                    NULLIF(btrim(
                        CASE WHEN pg_typeof(images)::text IN ('json','jsonb')
                             THEN images::text
                             ELSE images::text
                        END
                    ), '')::jsonb AS images_json
                  FROM public."{latest}"
                )
                INSERT INTO public.sreality_photo (id, images)
                SELECT s.id, s.images_json
                FROM src s
                LEFT JOIN public.sreality_photo t ON t.id = s.id
                WHERE t.id IS NULL
                  AND s.images_json IS NOT NULL
            """)
            inserted = (conn.execute(insert_sql).rowcount or 0)

            total = int(conn.execute(text("SELECT COUNT(*) FROM public.sreality_photo")).scalar_one())

        stats = {
            "snapshot_table": latest,
            "snapshot_date": snap_date,
            "source_rows": src_rows,
            "inserted_new": inserted,
            "photo_rows_total": total,
            "dry_run": False,
            "status": "ok",
            "duration_s": round((datetime.now() - t0).total_seconds(), 3)
        }

        print(json.dumps({"stage": "sync_summary", "stats": stats}, ensure_ascii=False))

    except Exception as e:
        log.exception("fatal")
        stats = {
            "snapshot_table": None,
            "snapshot_date": None,
            "source_rows": None,
            "inserted_new": None,
            "photo_rows_total": None,
            "dry_run": False,
            "status": "failed",
            "error": str(e),
            "duration_s": round((datetime.now() - t0).total_seconds(), 3)
        }
        print(json.dumps({"stage": "sync_summary", "stats": stats}, ensure_ascii=False))
        sys.exit(2)


if __name__ == "__main__":
    main()