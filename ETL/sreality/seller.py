#Extracts seller/agent metadata from the latest sreality_* snapshot into sreality_seller,
#emitting clean JSON sync_summary for the orchestrator.

from __future__ import annotations

import json
import logging
import sys
from pathlib import Path
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL


# ---------- LOGGING ----------
_root = logging.getLogger()
for h in list(_root.handlers):
    _root.removeHandler(h)
stderr = logging.StreamHandler(stream=sys.stderr)
stderr.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | sreality-seller | %(message)s"))
_root.addHandler(stderr)
_root.setLevel(logging.INFO)
log = logging.getLogger("SrealitySeller")


# ---------- CLEAN EMIT ----------
def emit_json(obj: dict):
    print(json.dumps(obj, ensure_ascii=False))


# ---------- CONFIG ----------
PROJECT_ROOT = Path(__file__).resolve().parents[2]
cfg = json.loads((PROJECT_ROOT / "config.json").read_text(encoding="utf-8"))

db_url = URL.create(
    drivername="postgresql+psycopg2",
    username=cfg["USER"],
    password=cfg["PWD"],
    host=cfg["HOST"],
    port=int(cfg["PORT"]),
    database=cfg["DB"],
)
engine = create_engine(db_url, pool_pre_ping=True, connect_args={"connect_timeout": 10})


# ---------- HELPERS ----------
def _sorted_snapshot_tables() -> list[str]:
    sql = text("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema='public'
          AND table_name LIKE 'sreality\\_%' ESCAPE '\\'
          AND table_name ~ '^sreality_[0-9]{8}$'
    """)
    with engine.begin() as conn:
        rows = [r[0] for r in conn.execute(sql)]

    def key(t: str):
        d = t[-8:]
        return int(d[4:]), int(d[2:4]), int(d[:2])

    return sorted(rows, key=key)


def _date_from_table(t: str) -> str:
    d = t[-8:]
    return f"{d[4:]}-{d[2:4]}-{d[:2]}"


def _table_has_cols(table: str, cols: list[str]) -> bool:
    sql = text("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema='public' AND table_name=:t
    """)
    with engine.begin() as conn:
        got = {r[0] for r in conn.execute(sql, {"t": table})}
    return all(c in got for c in cols)


# ---------- MAIN ----------
def main():
    tables = _sorted_snapshot_tables()
    if not tables:
        emit_json({
            "stage": "sync_summary",
            "stats": {"error": "no_snapshots"}
        })
        return

    latest = tables[-1]
    snap_date = _date_from_table(latest)

    required = ["id", "agent_name", "agent_phone", "agent_email"]
    if not _table_has_cols(latest, required):
        emit_json({
            "stage": "sync_summary",
            "stats": {
                "snapshot_table": latest,
                "snapshot_date": snap_date,
                "error": "missing_columns",
                "missing": required
            }
        })
        return

    with engine.begin() as conn:
        conn.execute(text("SET LOCAL lock_timeout = '5s'"))
        conn.execute(text("SET LOCAL statement_timeout = '5min'"))

        conn.execute(text("""
            CREATE UNIQUE INDEX IF NOT EXISTS ux_sreality_seller_id
            ON public.sreality_seller (id)
        """))

        src_rows = int(conn.execute(text(f'SELECT COUNT(*) FROM public."{latest}"')).scalar_one())

        insert_sql = text(f"""
            WITH src AS (
              SELECT
                id::bigint AS id,
                NULLIF(btrim(agent_name), '') AS agent_name_raw,
                NULLIF(lower(btrim(agent_email)), '') AS agent_email_raw,
                NULLIF(regexp_replace(btrim(agent_phone), '[^0-9+]+', '', 'g'), '') AS phone_raw
              FROM public."{latest}"
            ),
            norm AS (
              SELECT
                id,
                agent_name_raw AS agent_name,
                agent_email_raw AS agent_email,
                CASE
                  WHEN phone_raw IS NULL THEN NULL
                  WHEN phone_raw ~ '^[0-9]{9}$' THEN '+420' || phone_raw
                  WHEN phone_raw ~ '^\\+[0-9]{9,15}$' THEN phone_raw
                  WHEN phone_raw ~ '^[0-9]{10,15}$' THEN '+' || phone_raw
                  ELSE NULL
                END AS agent_phone
              FROM src
            )
            INSERT INTO public.sreality_seller (id, agent_name, agent_phone, agent_email)
            SELECT n.id, n.agent_name, n.agent_phone, n.agent_email
            FROM norm n
            LEFT JOIN public.sreality_seller t ON t.id = n.id
            WHERE t.id IS NULL
              AND (
                    n.agent_name IS NOT NULL
                 OR n.agent_email IS NOT NULL
                 OR n.agent_phone IS NOT NULL
              )
        """)

        res = conn.execute(insert_sql)
        inserted = res.rowcount or 0

        total_rows = int(conn.execute(
            text("SELECT COUNT(*) FROM public.sreality_seller")
        ).scalar_one())

    # ---------- FINAL sync_summary ----------
    emit_json({
        "stage": "sync_summary",
        "stats": {
            "snapshot_table": latest,
            "snapshot_date": snap_date,
            "source_rows": src_rows,
            "inserted_new": inserted,
            "seller_rows_total": total_rows,
            "status": "ok"
        }
    })


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log.exception("fatal")
        emit_json({
            "stage": "sync_summary",
            "stats": {"status": "failed", "error": str(e)}
        })
        sys.exit(1)