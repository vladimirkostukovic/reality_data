#This script audits silver.summarized integrity by comparing it to all source “typical” tables.
#It reports missing keys, extra keys, duplicates, mismatches and value-level drifts.

from __future__ import annotations
import sys, json, time, logging
from pathlib import Path
from typing import List, Set, Dict
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection

# ===== LOGGING =====
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | silver_audit | %(message)s",
    handlers=[logging.StreamHandler(sys.stderr)],
    force=True,
)
log = logging.getLogger("silver_audit")

# ===== CONFIG =====
PROJECT_ROOT = Path(__file__).resolve().parents[2]
cfg_path = PROJECT_ROOT / "config.json"
if not cfg_path.exists():
    cfg_path = Path(__file__).resolve().parent / "config.json"

cfg = json.loads(cfg_path.read_text(encoding="utf-8"))

DB_URL = f"postgresql+psycopg2://{cfg['USER']}:{cfg['PWD']}@{cfg['HOST']}:{cfg['PORT']}/{cfg['DB']}"
engine = create_engine(DB_URL, pool_pre_ping=True)

SRC_TABLES = [
    ("public", "sreality_typical", 1),
    ("public", "bezrealitky_typical", 2),
    ("public", "idnes_typical", 3),
]

EXCLUDE_COLS = {
    "ingested_at", "ingested_ts",
    "created_at", "updated_at",
    "source_id", "site_id"
}

SAMPLE_LIMIT = 20


# ===== HELPERS =====

def _table_exists(conn: Connection, schema: str, table: str) -> bool:
    q = text("""
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema=:s AND table_name=:t
        LIMIT 1
    """)
    return bool(conn.execute(q, {"s": schema, "t": table}).scalar())


def _columns(conn: Connection, schema: str, table: str) -> List[str]:
    q = text("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema=:s AND table_name=:t
    """)
    return [r[0] for r in conn.execute(q, {"s": schema, "t": table}).fetchall()]


def _column_types(conn: Connection, schema: str, table: str) -> Dict[str, str]:
    q = text("""
      SELECT column_name, data_type
      FROM information_schema.columns
      WHERE table_schema=:s AND table_name=:t
    """)
    return {r[0]: r[1] for r in conn.execute(q, {"s": schema, "t": table}).fetchall()}


def _find_common_columns(conn: Connection) -> List[str]:
    silver_cols = set(_columns(conn, "silver", "summarized")) - EXCLUDE_COLS
    src_sets: List[Set[str]] = []

    for sch, tbl, _sid in SRC_TABLES:
        if _table_exists(conn, sch, tbl):
            src_sets.append(set(_columns(conn, sch, tbl)) - EXCLUDE_COLS)

    if not src_sets:
        return []

    return sorted(silver_cols.intersection(*src_sets))


def _build_sources_union_sql(conn: Connection, common_cols: List[str]) -> str:
    blocks = []

    for sch, tbl, sid in SRC_TABLES:
        if not _table_exists(conn, sch, tbl):
            log.warning("missing source: %s.%s", sch, tbl)
            continue

        tbl_cols = set(_columns(conn, sch, tbl))
        exprs = [
            f'CAST("{c}" AS text) AS "{c}"' if c in tbl_cols else f'NULL::text AS "{c}"'
            for c in common_cols
        ]

        blocks.append(f"""
            SELECT
                {sid}::int AS source_id,
                site_id::text AS site_id,
                {", ".join(exprs)}
            FROM {sch}.{tbl}
        """)

    return "\nUNION ALL\n".join(blocks)


def _build_cmp_exprs(types: Dict[str, str], cols: List[str]) -> List[str]:
    exprs = []
    for c in cols:
        t = types.get(c, "text")
        if t.startswith("timestamp") or t == "date":
            exprs.append(f'(CAST(s."{c}" AS date) IS DISTINCT FROM CAST(u."{c}" AS date))')
        elif t in ("integer", "bigint", "smallint"):
            exprs.append(f'(CAST(s."{c}" AS numeric) IS DISTINCT FROM CAST(u."{c}" AS numeric))')
        elif t in ("double precision", "real", "numeric", "decimal"):
            exprs.append(f'(CAST(s."{c}" AS numeric) IS DISTINCT FROM CAST(u."{c}" AS numeric))')
        elif t == "boolean":
            exprs.append(f'(CAST(s."{c}" AS boolean) IS DISTINCT FROM CAST(u."{c}" AS boolean))')
        else:
            exprs.append(f'(CAST(s."{c}" AS text) IS DISTINCT FROM CAST(u."{c}" AS text))')
    return exprs


# ===== MAIN =====

def main():
    t0 = time.time()

    out = {
        "stage": "silver_audit",
        "status": "ok",
        "sync_summary": {},
        "details": {},
        "elapsed_s": 0,
    }

    try:
        with engine.begin() as conn:

            # TABLE CHECKS
            if not _table_exists(conn, "silver", "summarized"):
                raise RuntimeError("silver.summarized missing")

            if not any(_table_exists(conn, s, t) for s, t, _ in SRC_TABLES):
                raise RuntimeError("no typical source tables found")

            # COMMON COLUMNS
            common = _find_common_columns(conn)
            log.info("common columns: %s", common)

            union_sql = _build_sources_union_sql(conn, common)
            if not union_sql:
                raise RuntimeError("failed to build union_sql")

            silver_types = _column_types(conn, "silver", "summarized")

            # KEY STATS (типизировано!)
            q_tot = f"""
            WITH src_union AS ({union_sql}),
            src_keys AS (
              SELECT DISTINCT source_id, site_id FROM src_union
            ),
            silver_keys AS (
              SELECT DISTINCT 
                  source_id::int AS source_id,
                  site_id::text AS site_id
              FROM silver.summarized
            )
            SELECT
              (SELECT COUNT(*) FROM src_keys) AS src_total_keys,
              (SELECT COUNT(*) FROM silver_keys) AS silver_total_keys,
              (SELECT COUNT(*) FROM (
                SELECT s.* FROM src_keys s
                LEFT JOIN silver_keys k
                      ON k.source_id=s.source_id AND k.site_id=s.site_id
                WHERE k.site_id IS NULL
              ) z) AS missing_in_silver,
              (SELECT COUNT(*) FROM (
                SELECT k.* FROM silver_keys k
                LEFT JOIN src_keys s
                      ON s.source_id=k.source_id AND s.site_id=k.site_id
                WHERE s.site_id IS NULL
              ) z) AS extra_in_silver
            """
            row = conn.execute(text(q_tot)).mappings().first()

            # DUPES
            q_dups = text("""
                SELECT
                  COUNT(*) AS total_rows,
                  COUNT(DISTINCT (source_id::int, site_id::text)) AS distinct_keys,
                  COUNT(*) - COUNT(DISTINCT (source_id::int, site_id::text)) AS dupes
                FROM silver.summarized
            """)
            d = conn.execute(q_dups).mappings().first()

            # AVAILABLE/ARCHIVED
            q_av = text("""
                SELECT COUNT(*) FROM silver.summarized
                WHERE (archived_date IS NULL AND available IS NOT TRUE)
                   OR (archived_date IS NOT NULL AND available IS NOT FALSE)
            """)
            bad_av = int(conn.execute(q_av).scalar() or 0)

            # MISMATCHES
            mismatches = {}

            if common:
                cmp_exprs = _build_cmp_exprs(silver_types, common)
                col_agg = [
                    f"SUM(CASE WHEN {e} THEN 1 ELSE 0 END) AS \"{c}\""
                    for e, c in zip(cmp_exprs, common)
                ]

                q_diff = f"""
                WITH src_union AS ({union_sql}),
                src_dedup AS (
                  SELECT *, ROW_NUMBER() OVER (
                     PARTITION BY source_id, site_id ORDER BY site_id
                  ) rn
                  FROM src_union
                ),
                src_one AS (SELECT * FROM src_dedup WHERE rn=1)
                SELECT {", ".join(col_agg)}
                FROM silver.summarized s
                JOIN src_one u
                  ON u.source_id = s.source_id::int
                 AND u.site_id = s.site_id::text
                """
                agg = conn.execute(text(q_diff)).mappings().first() or {}
                mismatches = {c: int(agg.get(c) or 0) for c in common}

            # SUMMARY
            out["sync_summary"] = {
                "src_total_keys": int(row["src_total_keys"] or 0),
                "silver_total_keys": int(row["silver_total_keys"] or 0),
                "missing_in_silver": int(row["missing_in_silver"] or 0),
                "extra_in_silver": int(row["extra_in_silver"] or 0),
                "silver_dupes": int(d["dupes"] or 0),
                "available_archived_mismatch": bad_av,
                "mismatch_columns": sum(1 for v in mismatches.values() if v > 0),
            }

            out["details"] = {
                "common_columns": common,
                "column_mismatches": mismatches,
            }

    except Exception as e:
        out["status"] = "fail"
        out["details"] = {"error": f"{e.__class__.__name__}: {e}"}

    finally:
        out["elapsed_s"] = round(time.time() - t0, 3)
        sys.stdout.write(json.dumps(out, ensure_ascii=False) + "\n")


if __name__ == "__main__":
    main()