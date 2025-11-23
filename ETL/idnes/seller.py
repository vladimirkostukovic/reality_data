#Extracts unique seller metadata from the latest idnes_ snapshot and appends new sellers into idnes_seller,
# emitting a unified sync_summary for the pipeline.
from __future__ import annotations

import sys
import json
import logging
from pathlib import Path
from datetime import datetime
from sqlalchemy import create_engine, text

# ===== LOGGING =====
_root = logging.getLogger()
for h in list(_root.handlers):
    _root.removeHandler(h)
stderr = logging.StreamHandler(stream=sys.stderr)
stderr.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | idnes-seller | %(message)s"))
_root.addHandler(stderr)
_root.setLevel(logging.INFO)
log = logging.getLogger("IdnesSellerExtract")

# ===== CONFIG =====
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

def _make_db_url() -> str:
    try:
        import psycopg  # noqa
        return f"postgresql+psycopg://{USER}:{PWD}@{HOST}:{PORT}/{DB}"
    except ModuleNotFoundError:
        import psycopg2  # noqa
        return f"postgresql+psycopg2://{USER}:{PWD}@{HOST}:{PORT}/{DB}"

engine = create_engine(_make_db_url(), pool_pre_ping=True, connect_args={"connect_timeout": 10})


# ===== HELPERS =====
def get_sorted_idnes_tables() -> list[str]:
    sql = text("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name LIKE 'idnes\\_%' ESCAPE '\\'
          AND table_name ~ '^idnes_[0-9]{8}$'
    """)
    with engine.begin() as conn:
        rows = [r[0] for r in conn.execute(sql)]
    if not rows:
        return []
    def key(t: str):
        d = t[-8:]
        return int(d[4:]), int(d[2:4]), int(d[:2])
    return sorted(rows, key=key)

def _snapshot_date(t: str) -> str:
    d = t[-8:]
    return f"{d[4:]}-{d[2:4]}-{d[:2]}"


def ensure_indexes():
    if DRY_RUN:
        return
    with engine.begin() as conn:
        conn.execute(text("SET LOCAL lock_timeout = '5s'"))
        conn.execute(text("SET LOCAL statement_timeout = '5min'"))
        conn.execute(text("""
            CREATE UNIQUE INDEX IF NOT EXISTS ux_idnes_seller_id
            ON public.idnes_seller (id)
        """))


# ===== MAIN =====
def extract_idnes_sellers():
    t0 = datetime.now()

    try:
        tables = get_sorted_idnes_tables()
        if not tables:
            summary = {
                "stage": "sync_summary",
                "stats": {
                    "snapshot_table": None,
                    "snapshot_date": None,
                    "source_rows": 0,
                    "inserted_new": 0,
                    "seller_rows_total": None,
                    "dry_run": DRY_RUN,
                    "status": "failed",
                    "duration_s": round((datetime.now() - t0).total_seconds(), 3)
                }
            }
            print(json.dumps(summary, ensure_ascii=False))
            sys.exit(0)

        latest = tables[-1]
        snapshot_date = _snapshot_date(latest)

        ensure_indexes()

        with engine.begin() as conn:
            conn.execute(text("SET LOCAL lock_timeout = '5s'"))
            conn.execute(text("SET LOCAL statement_timeout = '5min'"))

            src_rows = int(conn.execute(text(
                f'SELECT COUNT(*) FROM public."{latest}"'
            )).scalar_one())

            inserted = 0
            if not DRY_RUN:
                sql_insert = text(f"""
                    INSERT INTO public.idnes_seller (id, seller_info, first_seen, raw_data)
                    SELECT
                        (s.data->>'id')::text,
                        (s.data->'seller_info')::jsonb,
                        NULLIF(s.data->>'scraped_at', '')::timestamptz,
                        s.data::jsonb
                    FROM public."{latest}" s
                    LEFT JOIN public.idnes_seller t
                        ON t.id = (s.data->>'id')::text
                    WHERE s.data ? 'id'
                      AND t.id IS NULL
                    ON CONFLICT (id) DO NOTHING
                """)
                res = conn.execute(sql_insert)
                inserted = res.rowcount or 0

            total_dst = int(conn.execute(text(
                "SELECT COUNT(*) FROM public.idnes_seller"
            )).scalar_one()) if not DRY_RUN else None

        # ---- unified sync_summary ----
        stats = {
            "snapshot_table": latest,
            "snapshot_date": snapshot_date,
            "source_rows": src_rows,
            "inserted_new": inserted,
            "seller_rows_total": total_dst,
            "dry_run": DRY_RUN,
            "status": "ok",
            "duration_s": round((datetime.now() - t0).total_seconds(), 3)
        }

        print(json.dumps({"stage": "sync_summary", "stats": stats}, ensure_ascii=False))

    except Exception as e:
        log.exception("Fatal error in idnes seller extract")
        print(json.dumps({
            "stage": "sync_summary",
            "stats": {
                "status": "failed",
                "error": str(e),
                "dry_run": DRY_RUN
            }
        }, ensure_ascii=False))
        sys.exit(2)


if __name__ == "__main__":
    extract_idnes_sellers()