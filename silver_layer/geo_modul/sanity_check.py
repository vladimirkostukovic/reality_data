# Maintains silver layer: dedup summarized(+geo cascade) and normalized regions.
# Produces clean, canonical geo data with consistent site_id integrity.

from __future__ import annotations
import sys, json, time
from pathlib import Path
from typing import Dict, Any, List

from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# ---------- CONFIG ----------
PROJECT_ROOT = Path(__file__).resolve().parents[2]
CFG_PATH = PROJECT_ROOT / "config.json"
cfg = json.loads(CFG_PATH.read_text(encoding="utf-8"))

DB_URL = (
    f"postgresql+psycopg2://"
    f"{cfg['USER']}:{cfg['PWD']}@{cfg['HOST']}:{cfg['PORT']}/{cfg['DB']}"
)
engine = create_engine(DB_URL, pool_pre_ping=True, future=True)

SCHEMA = "silver"
PARENT = "summarized"
CHILD = "summarized_geo"

REGION_COL = "norm_district"

REGION_MAP = {
    "hlavni mesto praha": "Hlavní město Praha",
    "jihocesky kraj": "Jihočeský kraj",
    "jihomoravsky kraj": "Jihomoravský kraj",
    "karlovarsky kraj": "Karlovarský kraj",
    "kralovehradecky kraj": "Královéhradecký kraj",
    "liberecky kraj": "Liberecký kraj",
    "moravskoslezsky kraj": "Moravskoslezský kraj",
    "olomoucky kraj": "Olomoucký kraj",
    "pardubicky kraj": "Pardubický kraj",
    "plzensky kraj": "Plzeňský kraj",
    "stredocesky kraj": "Středočeský kraj",
    "ustecky kraj": "Ústecký kraj",
    "zlinsky kraj": "Zlínský kraj",
    "vysocina": "Kraj Vysočina",
    "kraj vysocina": "Kraj Vysočina",
    "kraj vysocina ": "Kraj Vysočina",
}

# ---------- HELPERS ----------

def column_exists(conn, schema: str, table: str, col: str) -> bool:
    q = text("""
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = :schema
          AND table_name   = :table
          AND column_name  = :col
        LIMIT 1
    """)
    return bool(conn.execute(q, {"schema": schema, "table": table, "col": col}).scalar())


def count_total_distinct(conn, schema: str, table: str, keycol: str) -> Dict[str, int]:
    q = text(f"""
        SELECT COUNT(*) AS total,
               COUNT(DISTINCT {keycol}) AS distinct_keys
        FROM "{schema}"."{table}"
    """)
    row = conn.execute(q).mappings().first()
    return {
        "total": int(row["total"] or 0),
        "distinct": int(row["distinct_keys"] or 0),
        "dupes": max(0, int(row["total"] or 0) - int(row["distinct_keys"] or 0)),
    }


def build_order_by(conn, schema: str, table: str) -> str:
    candidates = ["ingested_at", "added_date", "archived_date"]
    present = [c for c in candidates if column_exists(conn, schema, table, c)]
    order_parts = [f"{c} DESC NULLS LAST" for c in present]
    order_parts.append("ctid DESC")
    return ", ".join(order_parts)


# ---------- STEP 1: dedup summarized + cascade ----------

def dedup_summarized_with_geo_cascade(conn) -> Dict[str, Any]:
    schema, parent, child = SCHEMA, PARENT, CHILD

    if not column_exists(conn, schema, parent, "site_id") or not column_exists(conn, schema, parent, "internal_id"):
        return {"skipped": True}

    stats_before = count_total_distinct(conn, schema, parent, "site_id")

    if stats_before["dupes"] == 0:
        return {
            "skipped": False,
            "before": stats_before,
            "removed_parent": 0,
            "removed_child": 0,
            "after": stats_before,
        }

    order_by = build_order_by(conn, schema, parent)

    sql_del_child = f"""
        WITH ranked AS (
            SELECT internal_id,
                   ROW_NUMBER() OVER (PARTITION BY site_id ORDER BY {order_by}) AS rn
            FROM "{schema}"."{parent}"
        ),
        bad AS (SELECT internal_id FROM ranked WHERE rn > 1)
        DELETE FROM "{schema}"."{child}" g
        USING bad b
        WHERE g.internal_id = b.internal_id;
    """
    removed_child = conn.execute(text(sql_del_child)).rowcount or 0

    sql_del_parent = f"""
        WITH ranked AS (
            SELECT ctid, internal_id,
                   ROW_NUMBER() OVER (PARTITION BY site_id ORDER BY {order_by}) AS rn
            FROM "{schema}"."{parent}"
        )
        DELETE FROM "{schema}"."{parent}" p
        USING ranked r
        WHERE p.ctid = r.ctid AND r.rn > 1;
    """
    removed_parent = conn.execute(text(sql_del_parent)).rowcount or 0

    stats_after = count_total_distinct(conn, schema, parent, "site_id")

    return {
        "skipped": False,
        "before": stats_before,
        "removed_child": removed_child,
        "removed_parent": removed_parent,
        "after": stats_after,
    }


# ---------- STEP 2: dedup summarized_geo ----------

def dedup_summarized_geo(conn) -> Dict[str, Any]:
    schema, table = SCHEMA, CHILD

    if not column_exists(conn, schema, table, "site_id"):
        return {"skipped": True}

    stats_before = count_total_distinct(conn, schema, table, "site_id")

    if stats_before["dupes"] == 0:
        return {"skipped": False, "before": stats_before, "removed": 0, "after": stats_before}

    order_by = build_order_by(conn, schema, table)

    sql_del = f"""
        WITH ranked AS (
            SELECT ctid,
                   ROW_NUMBER() OVER (PARTITION BY site_id ORDER BY {order_by}) AS rn
            FROM "{schema}"."{table}"
        )
        DELETE FROM "{schema}"."{table}" t
        USING ranked r
        WHERE t.ctid = r.ctid AND r.rn > 1;
    """
    removed = conn.execute(text(sql_del)).rowcount or 0
    stats_after = count_total_distinct(conn, schema, table, "site_id")

    return {
        "skipped": False,
        "before": stats_before,
        "removed": removed,
        "after": stats_after,
    }


# ---------- STEP 3: region normalization ----------

DIACRITIC_MAP = {
    "á": "a", "ä": "a", "č": "c", "ć": "c", "ď": "d",
    "é": "e", "ě": "e", "ê": "e", "ë": "e",
    "í": "i", "ï": "i",
    "ľ": "l", "ĺ": "l",
    "ň": "n", "ń": "n",
    "ó": "o", "ö": "o", "ô": "o",
    "ř": "r",
    "š": "s",
    "ť": "t",
    "ú": "u", "ů": "u", "ü": "u",
    "ý": "y",
    "ž": "z",
}

def _base_norm_expr(col: str) -> str:
    expr = f"lower(trim({col}))"
    expr = f"replace({expr}, '-', ' ')"
    expr = f"regexp_replace({expr}, '\\s+', ' ', 'g')"
    for bad, good in DIACRITIC_MAP.items():
        expr = f"replace({expr}, '{bad}', '{good}')"
    expr = f"btrim(regexp_replace({expr}, '\\s+', ' ', 'g'))"
    return expr


def collect_distinct(conn, table: str, col: str, limit=200):
    q = text(f"""
        SELECT DISTINCT {col} AS v
        FROM "{SCHEMA}"."{table}"
        WHERE {col} IS NOT NULL AND {col} <> ''
        ORDER BY 1
        LIMIT {limit}
    """)
    return [r[0] for r in conn.execute(q).fetchall()]


def preview_region(conn):
    before = collect_distinct(conn, CHILD, REGION_COL)
    expr = _base_norm_expr(REGION_COL)
    q = text(f"""
        SELECT DISTINCT {expr} AS v
        FROM "{SCHEMA}"."{CHILD}"
        WHERE {REGION_COL} IS NOT NULL AND {REGION_COL} <> ''
        LIMIT 200
    """)
    after = [r[0] for r in conn.execute(q).fetchall()]
    return {"before_sample": before[:50], "after_base_sample": after[:50]}


def apply_base_normalization(conn):
    expr = _base_norm_expr(REGION_COL)
    q = text(f"""UPDATE "{SCHEMA}"."{CHILD}" SET {REGION_COL} = {expr}""")
    res = conn.execute(q)
    return res.rowcount or 0


def ensure_region_map(conn):
    conn.execute(text(f"""
        CREATE TABLE IF NOT EXISTS "{SCHEMA}"."region_map" (
            from_norm text PRIMARY KEY,
            to_norm   text NOT NULL
        )
    """))


def sync_region_map(conn):
    ensure_region_map(conn)

    existing = dict(conn.execute(text(f"""
        SELECT from_norm, to_norm
        FROM "{SCHEMA}"."region_map"
    """)).fetchall())

    for k, v in REGION_MAP.items():
        if k in existing:
            if existing[k] != v:
                conn.execute(text(f"""
                    UPDATE "{SCHEMA}"."region_map"
                    SET to_norm=:v WHERE from_norm=:k
                """), {"k": k, "v": v})
        else:
            conn.execute(text(f"""
                INSERT INTO "{SCHEMA}"."region_map"(from_norm, to_norm)
                VALUES (:k, :v)
            """), {"k": k, "v": v})

    conn.execute(text(f"""
        UPDATE "{SCHEMA}"."{CHILD}" g
        SET {REGION_COL} = m.to_norm
        FROM "{SCHEMA}"."region_map" m
        WHERE g.{REGION_COL} = m.from_norm
    """))


# ---------- MAIN ----------

def main():
    t0 = time.time()

    out = {
        "stage": "silver_maintenance",
        "status": "ok",
        "sync_summary": {
            "dedup_parent": {},
            "dedup_child": {},
            "region_preview": {},
            "rows_updated_base": 0,
            "map_applied": False,
            "distinct_after": [],
            "elapsed_s": 0.0,
        }
    }

    try:
        with engine.begin() as conn:
            conn.execute(text("SET LOCAL statement_timeout='5min'"))

            d1 = dedup_summarized_with_geo_cascade(conn)
            out["sync_summary"]["dedup_parent"] = d1

            d2 = dedup_summarized_geo(conn)
            out["sync_summary"]["dedup_child"] = d2

            prev = preview_region(conn)
            out["sync_summary"]["region_preview"] = prev

            upd = apply_base_normalization(conn)
            out["sync_summary"]["rows_updated_base"] = upd

            sync_region_map(conn)
            out["sync_summary"]["map_applied"] = True

            final_vals = collect_distinct(conn, CHILD, REGION_COL, limit=200)
            out["sync_summary"]["distinct_after"] = final_vals[:100]

    except Exception as e:
        out["status"] = "fail"
        out["sync_summary"]["error"] = str(e)

    finally:
        out["sync_summary"]["elapsed_s"] = round(time.time() - t0, 3)
        sys.stdout.write(json.dumps(out, ensure_ascii=False) + "\n")
        sys.stdout.flush()


if __name__ == "__main__":
    main()