
from __future__ import annotations

import json
import sys
import logging
from pathlib import Path
from sqlalchemy import create_engine, text

# ========== LOGGING==========
_root = logging.getLogger()
for h in list(_root.handlers):
    _root.removeHandler(h)
stderr = logging.StreamHandler(stream=sys.stderr)
stderr.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | sreality-photo | %(message)s"))
_root.addHandler(stderr)
_root.setLevel(logging.INFO)
log = logging.getLogger("SrealityPhotoExtract")

# ========== CONFIG & ENGINE ==========
PROJECT_ROOT = Path(__file__).resolve().parents[2] if len(Path(__file__).resolve().parents) >= 3 else Path.cwd()
cfg_path = PROJECT_ROOT / "config.json"
if not cfg_path.exists():
    alt = Path(__file__).resolve().parent / "config.json"
    cfg_path = alt if alt.exists() else cfg_path

cfg = json.loads(cfg_path.read_text(encoding="utf-8"))

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
        d = t[-8:]  # ddmmyyyy
        return (int(d[4:]), int(d[2:4]), int(d[:2]))
    return sorted(tabs, key=key)

def _date_from_table(t: str) -> str:
    d = t[-8:]  # ddmmyyyy
    return f"{d[4:]}-{d[2:4]}-{d[:2]}"  # yyyy-mm-dd

def _table_has_cols(table: str, cols: list[str]) -> bool:
    with engine.begin() as conn:
        have = {r[0] for r in conn.execute(
            text("""SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema='public' AND table_name=:t"""),
            {"t": table}
        )}
    return all(c in have for c in cols)

# ========== MAIN ==========
def main():
    try:
        tabs = _sorted_snapshot_tables()
        if not tabs:
            log.warning("no sreality_* snapshot tables found")
            print(json.dumps({"stage": "sync_summary", "error": "no_snapshots"}, ensure_ascii=False))
            print("SYNC COMPLETE")
            return

        latest = tabs[-1]
        snap_date = _date_from_table(latest)
        log.info("processing snapshot: %s (date=%s)", latest, snap_date)

        if not _table_has_cols(latest, ["id", "images"]):
            log.warning('%s: required columns missing: ["id","images"]', latest)
            stats = {
                "snapshot_table": latest,
                "snapshot_date": snap_date,
                "source_rows": 0,
                "inserted_new": 0,
                "photo_rows_total": None
            }
            print(json.dumps({"stage": "sync_summary", "stats": stats}, ensure_ascii=False))
            print("SYNC COMPLETE")
            return

        with engine.begin() as conn:
            conn.execute(text("SET LOCAL lock_timeout = '5s'"))
            conn.execute(text("SET LOCAL statement_timeout = '5min'"))
            conn.execute(text("""
                CREATE UNIQUE INDEX IF NOT EXISTS ux_sreality_photo_id
                  ON public.sreality_photo (id)
            """))

            src_rows = int(conn.execute(text(f'SELECT COUNT(*) FROM public."{latest}"')).scalar_one())

            # images может быть text/json/jsonb. Приводим к jsonb через ::jsonb, пустые строки режем.
            insert_sql = text(f"""
                WITH src AS (
                  SELECT
                    id::bigint AS id,
                    NULLIF(btrim(
                      CASE
                        WHEN pg_typeof(images)::text IN ('json','jsonb') THEN images::text
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
            ins_res = conn.execute(insert_sql)
            inserted = ins_res.rowcount or 0

            total = int(conn.execute(text("SELECT COUNT(*) FROM public.sreality_photo")).scalar_one())

        log.info("source_rows=%d | inserted_new=%d | photo_rows_total=%d", src_rows, inserted, total)

        stats = {
            "snapshot_table": latest,
            "snapshot_date": snap_date,
            "source_rows": src_rows,
            "inserted_new": inserted,
            "photo_rows_total": total,
        }
        print(json.dumps({"stage": "sync_summary", "stats": stats}, ensure_ascii=False))
        print("SYNC COMPLETE")

    except Exception as e:
        log.exception("failed")
        raise

if __name__ == "__main__":
    main()