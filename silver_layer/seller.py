# Extracts and normalizes seller metadata from all sources into silver.seller using SQL + streaming pipelines.
# Performs staging, dedup, idempotent merge and outputs strict JSON metrics for the orchestrator.

import os
os.environ.setdefault("OMP_NUM_THREADS", "1")
os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")
os.environ.setdefault("NUMEXPR_MAX_THREADS", "1")
os.environ.setdefault("MKL_THREADING_LAYER", "GNU")

import sys
import re
import json
import time
import logging
from pathlib import Path
from typing import Optional, Tuple, List

import psycopg2
import psycopg2.extras
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# === LOGGING ===
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | seller_sync | %(message)s",
    handlers=[logging.StreamHandler(sys.stderr)],
    force=True,
)
log = logging.getLogger("seller_sync")

# === CONFIG ===
PROJECT_ROOT = Path(__file__).resolve().parents[1]
with open(PROJECT_ROOT / "config.json", "r", encoding="utf-8") as f:
    cfg = json.load(f)

DB_URL_SA = f"postgresql+psycopg2://{cfg['USER']}:{cfg['PWD']}@{cfg['HOST']}:{cfg['PORT']}/{cfg['DB']}"
DSN = f"host={cfg['HOST']} port={cfg['PORT']} dbname={cfg['DB']} user={cfg['USER']} password={cfg['PWD']}"

engine = create_engine(DB_URL_SA, pool_pre_ping=True)

# === CONSTANTS ===
EMAIL_RE = re.compile(r"[A-Za-z0-9_.+-]+@[A-Za-z0-9-]+\.[A-Za-z0-9-.]+")
PHONE_RE = re.compile(r"\+?\d[\d\s]{7,}")
SAFE_MAX   = 2000
FETCH_SIZE = 10_000
FLUSH_SIZE = 25_000

# === PARSER ===
def parse_seller_info(raw) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    if raw is None:
        return None, None, None, None
    try:
        s = str(raw)
        if not s:
            return None, None, None, None
        if len(s) > SAFE_MAX:
            s = s[:SAFE_MAX]

        email_m = EMAIL_RE.search(s)
        email = email_m.group(0) if email_m else None

        phone_m = PHONE_RE.search(s)
        phone = phone_m.group(0) if phone_m else None

        head = s.split(email, 1)[0] if email else s
        parts = [p.strip() for p in head.split(",") if p.strip()]
        agent_name = parts[0] if parts else None
        agency = parts[1] if len(parts) > 1 else None
        return agent_name or None, phone or None, email or None, agency or None
    except Exception:
        return None, None, None, None

# === DB HELPERS ===
def insert_stage_and_merge(conn, rows: List[Tuple[str, Optional[str], Optional[str], Optional[str]]], label: str) -> int:
    if not rows:
        return 0

    with conn.cursor() as cur:
        cur.execute("SET LOCAL synchronous_commit = OFF;")

        cur.execute("DROP TABLE IF EXISTS tmp_seller_stage_dedup;")
        cur.execute("DROP TABLE IF EXISTS tmp_seller_stage;")

        cur.execute("""
            CREATE TEMP TABLE tmp_seller_stage (
                id           TEXT,
                agent_name   TEXT,
                agent_phone  TEXT,
                agent_email  TEXT
            ) ON COMMIT DROP;
        """)

        psycopg2.extras.execute_values(
            cur,
            "INSERT INTO tmp_seller_stage (id, agent_name, agent_phone, agent_email) VALUES %s",
            rows,
            page_size=50_000
        )

        cur.execute("""
            CREATE TEMP TABLE tmp_seller_stage_dedup AS
            SELECT DISTINCT ON (id) id, agent_name, agent_phone, agent_email
            FROM tmp_seller_stage
            ORDER BY id, agent_email NULLS LAST, agent_phone NULLS LAST;
        """)

        cur.execute("""
            INSERT INTO silver.seller (id, agent_name, agent_phone, agent_email)
            SELECT d.id, d.agent_name, d.agent_phone, d.agent_email
            FROM tmp_seller_stage_dedup d
            WHERE NOT EXISTS (SELECT 1 FROM silver.seller s WHERE s.id = d.id)
        """)
        inserted = cur.rowcount or 0

    return inserted

# === SYNC: SREALITY ===
def sync_from_sreality_sqlalchemy(sa_conn) -> int:
    log.info("load:sreality_seller")
    res = sa_conn.execute(text("""
        INSERT INTO silver.seller (id, agent_name, agent_phone, agent_email)
        SELECT CAST(id AS TEXT),
               NULLIF(trim(agent_name), ''),
               NULLIF(trim(agent_phone), ''),
               NULLIF(trim(agent_email), '')
        FROM sreality_seller s
        WHERE (NULLIF(trim(agent_email), '') IS NOT NULL OR NULLIF(trim(agent_phone), '') IS NOT NULL)
          AND NOT EXISTS (SELECT 1 FROM silver.seller t WHERE t.id = CAST(s.id AS TEXT))
    """))
    return res.rowcount or 0

# === SYNC: IDNES ===
def sync_from_idnes_psycopg2(raw_conn) -> int:
    log.info("load:idnes_seller (streaming psycopg2)")
    inserted_total = 0
    buf = []

    with raw_conn.cursor(name=None) as cur:
        cur.itersize = FETCH_SIZE
        cur.execute("SELECT id, seller_info FROM idnes_seller;")

        while True:
            rows = cur.fetchmany(FETCH_SIZE)
            if not rows:
                break

            for _id, info in rows:
                a, ph, em, _ag = parse_seller_info(info)
                if (em and em.strip()) or (ph and ph.strip()):
                    buf.append((str(_id), a, ph, em))

                if len(buf) >= FLUSH_SIZE:
                    inserted_total += insert_stage_and_merge(raw_conn, buf, "idnes")
                    buf.clear()

    if buf:
        inserted_total += insert_stage_and_merge(raw_conn, buf, "idnes")

    return inserted_total

# === MAIN ===
def main():
    t0 = time.perf_counter()

    rows_inserted = {"sreality": 0, "idnes": 0}
    before = after = 0
    sanity_problems = []
    sanity_warnings = []

    try:
        with engine.begin() as sa_conn:
            before = sa_conn.execute(text("SELECT COUNT(*) FROM silver.seller")).scalar() or 0

        with engine.begin() as sa_conn:
            rows_inserted["sreality"] = sync_from_sreality_sqlalchemy(sa_conn)

        with psycopg2.connect(DSN) as raw_conn:
            raw_conn.autocommit = False
            rows_inserted["idnes"] = sync_from_idnes_psycopg2(raw_conn)
            raw_conn.commit()

        with engine.begin() as sa_conn:
            after = sa_conn.execute(text("SELECT COUNT(*) FROM silver.seller")).scalar() or 0

            if after < before:
                sanity_problems.append(f"rowcount decreased: before={before}, after={after}")

            if (rows_inserted["sreality"] + rows_inserted["idnes"]) != (after - before):
                sanity_warnings.append(
                    f"mass mismatch: inserted={rows_inserted}, delta={after - before}"
                )

            dupes = sa_conn.execute(text(
                "SELECT COUNT(*) - COUNT(DISTINCT id) FROM silver.seller"
            )).scalar() or 0
            if dupes > 0:
                sanity_problems.append(f"duplicate ids={dupes}")

        # ==== JSON OUTPUT ====
        out = {
            "module": "seller_sync",
            "status": "ok" if not sanity_problems else "fail",
            "before": before,
            "after": after,
            "inserted_total": rows_inserted["sreality"] + rows_inserted["idnes"],
            "inserted_breakdown": rows_inserted,
            "sanity_problems": sanity_problems,
            "sanity_warnings": sanity_warnings,
            "elapsed_s": round(time.perf_counter() - t0, 3),
        }
        sys.stdout.write(json.dumps(out, ensure_ascii=False) + "\n")

    except Exception as e:
        log.error(f"unexpected: {e}")
        sys.stdout.write(json.dumps({
            "module": "seller_sync",
            "status": "fail",
            "error": str(e),
            "elapsed_s": round(time.perf_counter() - t0, 3),
        }) + "\n")
        raise

if __name__ == "__main__":
    main()