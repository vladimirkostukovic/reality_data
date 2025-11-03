from __future__ import annotations
import sys, json, time, logging
from pathlib import Path
from typing import Dict, Any, List

from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# ---------- LOGGING ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | silver_maintenance | %(message)s",
    handlers=[logging.StreamHandler(sys.stderr)],
    force=True,
)
log = logging.getLogger("silver_maintenance")

# ---------- CONFIG ----------
PROJECT_ROOT = Path(__file__).resolve().parents[1]
CFG_PATH = PROJECT_ROOT / "config.json"
cfg = json.loads(CFG_PATH.read_text(encoding="utf-8"))

DB_URL = (
    f"postgresql+psycopg2://"
    f"{cfg['USER']}:{cfg['PWD']}@{cfg['HOST']}:{cfg['PORT']}/{cfg['DB']}"
)
engine = create_engine(DB_URL, pool_pre_ping=True, future=True)

SCHEMA = "silver"
PARENT = "summarized"
CHILD  = "summarized_geo"

REGION_COL = "norm_district"

REGION_MAP = {
    "hlavni mesto praha":      "Hlavní město Praha",
    "jihocesky kraj":          "Jihočeský kraj",
    "jihomoravsky kraj":       "Jihomoravský kraj",
    "karlovarsky kraj":        "Karlovarský kraj",
    "kralovehradecky kraj":    "Královéhradecký kraj",
    "liberecky kraj":          "Liberecký kraj",
    "moravskoslezsky kraj":    "Moravskoslezský kraj",
    "olomoucky kraj":          "Olomoucký kraj",
    "pardubicky kraj":         "Pardubický kraj",
    "plzensky kraj":           "Plzeňský kraj",
    "stredocesky kraj":        "Středočeský kraj",
    "ustecky kraj":            "Ústecký kraj",
    "zlinsky kraj":            "Zlínský kraj",
    "vysocina":                "Kraj Vysočina",
    "kraj vysocina":           "Kraj Vysočina",
    "kraj vysocina ":          "Kraj Vysočina",
}

# ---------- GENERIC HELPERS ----------

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
    total = int(row["total"] or 0)
    distinct_keys = int(row["distinct_keys"] or 0)
    return {
        "total": total,
        "distinct": distinct_keys,
        "dupes": max(0, total - distinct_keys),
    }

def build_order_by(conn, schema: str, table: str) -> str:
    """Build ORDER BY clause for deduplication based on available timestamp columns"""
    candidates = ["ingested_at", "added_date", "archived_date"]
    present: List[str] = [
        c for c in candidates
        if column_exists(conn, schema, table, c)
    ]
    order_parts = [f"{c} DESC NULLS LAST" for c in present]
    order_parts.append("ctid DESC")
    return ", ".join(order_parts)

# ---------- STEP 1: DEDUP SUMMARIZED + CASCADE DELETE ----------

def dedup_summarized_with_geo_cascade(conn) -> Dict[str, Any]:
    """Remove duplicates from summarized table and cascade delete related geo records"""
    schema = SCHEMA
    parent = PARENT
    child  = CHILD

    if not column_exists(conn, schema, parent, "site_id") \
       or not column_exists(conn, schema, parent, "internal_id"):
        log.warning('%s.%s: skip — no "site_id" or "internal_id"', schema, parent)
        return {
            "table": f"{schema}.{parent}",
            "skipped": True,
        }

    stats_before = count_total_distinct(conn, schema, parent, "site_id")
    log.info("%s.%s | before: total=%d distinct=%d dupes=%d",
             schema, parent,
             stats_before["total"], stats_before["distinct"], stats_before["dupes"])

    if stats_before["dupes"] == 0:
        return {
            "table": f"{schema}.{parent}",
            "skipped": False,
            "before": stats_before,
            "removed_parent": 0,
            "removed_child": 0,
            "after": stats_before,
        }

    order_by = build_order_by(conn, schema, parent)
    log.info("%s.%s | ORDER BY keep=1: %s", schema, parent, order_by)

    delete_children_sql = f"""
        WITH ranked AS (
            SELECT internal_id,
                   ROW_NUMBER() OVER (
                       PARTITION BY site_id
                       ORDER BY {order_by}
                   ) AS rn
            FROM "{schema}"."{parent}"
        ),
        to_delete AS (
            SELECT internal_id FROM ranked WHERE rn > 1
        )
        DELETE FROM "{schema}"."{child}" g
        USING to_delete d
        WHERE g.internal_id = d.internal_id
        RETURNING 1;
    """
    res_child = conn.execute(text(delete_children_sql))
    removed_child = res_child.rowcount or 0
    log.info("%s.%s -> %s.%s | removed_child=%d",
             schema, parent, schema, child, removed_child)

    delete_parent_sql = f"""
        WITH ranked AS (
            SELECT ctid, internal_id,
                   ROW_NUMBER() OVER (
                       PARTITION BY site_id
                       ORDER BY {order_by}
                   ) AS rn
            FROM "{schema}"."{parent}"
        )
        DELETE FROM "{schema}"."{parent}" t
        USING ranked r
        WHERE t.ctid = r.ctid
          AND r.rn > 1
        RETURNING 1;
    """
    res_parent = conn.execute(text(delete_parent_sql))
    removed_parent = res_parent.rowcount or 0
    log.info("%s.%s | removed_parent=%d",
             schema, parent, removed_parent)

    stats_after = count_total_distinct(conn, schema, parent, "site_id")
    log.info("%s.%s | after: total=%d distinct=%d dupes=%d",
             schema, parent,
             stats_after["total"], stats_after["distinct"], stats_after["dupes"])

    return {
        "table": f"{schema}.{parent}",
        "skipped": False,
        "before": stats_before,
        "removed_child": removed_child,
        "removed_parent": removed_parent,
        "after": stats_after,
    }

# ---------- STEP 2: DEDUP SUMMARIZED_GEO ----------

def dedup_summarized_geo(conn) -> Dict[str, Any]:
    """Remove duplicates from summarized_geo table by site_id"""
    schema = SCHEMA
    table  = CHILD

    if not column_exists(conn, schema, table, "site_id"):
        log.info('%s.%s: skip — no "site_id"', schema, table)
        return {
            "table": f"{schema}.{table}",
            "skipped": True,
        }

    stats_before = count_total_distinct(conn, schema, table, "site_id")
    log.info("%s.%s | before: total=%d distinct=%d dupes=%d",
             schema, table,
             stats_before["total"], stats_before["distinct"], stats_before["dupes"])

    if stats_before["dupes"] == 0:
        return {
            "table": f"{schema}.{table}",
            "skipped": False,
            "before": stats_before,
            "removed": 0,
            "after": stats_before,
        }

    order_by = build_order_by(conn, schema, table)
    sql = f"""
        WITH ranked AS (
            SELECT ctid,
                   ROW_NUMBER() OVER (
                       PARTITION BY site_id
                       ORDER BY {order_by}
                   ) AS rn
            FROM "{schema}"."{table}"
        )
        DELETE FROM "{schema}"."{table}" t
        USING ranked r
        WHERE t.ctid = r.ctid
          AND r.rn > 1
        RETURNING 1;
    """
    res = conn.execute(text(sql))
    removed = res.rowcount or 0

    stats_after = count_total_distinct(conn, schema, table, "site_id")
    log.info("%s.%s | removed=%d | after: total=%d distinct=%d dupes=%d",
             schema, table,
             removed,
             stats_after["total"], stats_after["distinct"], stats_after["dupes"])

    return {
        "table": f"{schema}.{table}",
        "skipped": False,
        "before": stats_before,
        "removed": removed,
        "after": stats_after,
    }

# ---------- STEP 3: REGION NORMALIZATION ----------

DIACRITIC_MAP = {
    "á": "a", "ä": "a",
    "č": "c", "ć": "c",
    "ď": "d",
    "é": "e", "ě": "e", "ê": "e", "ë": "e",
    "í": "i", "ï": "i",
    "ľ": "l", "ĺ": "l",
    "ň": "n", "ń": "n",
    "ó": "o", "ö": "o", "ô": "o",
    "ř": "r",
    "š": "s", "ß": "s",
    "ť": "t",
    "ú": "u", "ů": "u", "ü": "u",
    "ý": "y",
    "ž": "z",
    "Á": "a", "Ä": "a",
    "Č": "c", "Ć": "c",
    "Ď": "d",
    "É": "e", "Ě": "e", "Ê": "e", "Ë": "e",
    "Í": "i", "Ï": "i",
    "Ľ": "l", "Ĺ": "l",
    "Ň": "n", "Ń": "n",
    "Ó": "o", "Ö": "o", "Ô": "o",
    "Ř": "r",
    "Š": "s",
    "Ť": "t",
    "Ú": "u", "Ů": "u", "Ü": "u",
    "Ý": "y",
    "Ž": "z",
}

def _base_normalizer_sql_expr(colname: str) -> str:
    """Build SQL expression for base normalization: lowercase, strip diacritics, normalize spaces"""
    expr = f"lower(trim({colname}))"
    expr = f"replace({expr}, '-', ' ')"
    expr = f"regexp_replace({expr}, '\\s+', ' ', 'g')"

    for bad, good in DIACRITIC_MAP.items():
        bad_sql = bad.replace("'", "''")
        good_sql = good.replace("'", "''")
        expr = f"replace({expr}, '{bad_sql}', '{good_sql}')"

    expr = "regexp_replace(" + expr + ", '\\s+', ' ', 'g')"
    expr = f"btrim({expr})"
    return expr

def collect_distinct(conn, table: str, col: str, limit: int = 200) -> List[str]:
    q = text(f"""
        SELECT DISTINCT {col} AS v
        FROM "{SCHEMA}"."{table}"
        WHERE {col} IS NOT NULL AND {col} <> ''
        ORDER BY 1
        LIMIT {limit}
    """)
    return [r[0] for r in conn.execute(q).fetchall()]

def preview_region_before_after(conn) -> Dict[str, Any]:
    """Preview region values before and after base normalization"""
    before_vals = collect_distinct(conn, CHILD, REGION_COL)
    expr = _base_normalizer_sql_expr(REGION_COL)
    q_after = text(f"""
        SELECT DISTINCT {expr} AS v
        FROM "{SCHEMA}"."{CHILD}"
        WHERE {REGION_COL} IS NOT NULL AND {REGION_COL} <> ''
        ORDER BY 1
        LIMIT 200
    """)
    after_vals = [r[0] for r in conn.execute(q_after).fetchall()]
    return {
        "before_sample": before_vals[:50],
        "after_base_sample": after_vals[:50],
    }

def apply_base_region_normalization(conn) -> int:
    """Apply base normalization to region column"""
    expr = _base_normalizer_sql_expr(REGION_COL)
    upd_sql = text(f"""
        UPDATE "{SCHEMA}"."{CHILD}"
        SET {REGION_COL} = {expr}
    """)
    res = conn.execute(upd_sql)
    return res.rowcount if res.rowcount is not None else 0

def ensure_region_map_table(conn):
    conn.execute(text(f"""
        CREATE TABLE IF NOT EXISTS "{SCHEMA}"."region_map" (
            from_norm text PRIMARY KEY,
            to_norm   text NOT NULL
        )
    """))

def sync_region_map(conn):
    """Sync REGION_MAP to database table and apply mappings"""
    ensure_region_map_table(conn)

    existing_rows = conn.execute(text(f"""
        SELECT from_norm, to_norm
        FROM "{SCHEMA}"."region_map"
    """)).fetchall()
    existing = {row[0]: row[1] for row in existing_rows}

    for k, v in REGION_MAP.items():
        if k in existing:
            if existing[k] != v:
                conn.execute(
                    text(f"""
                        UPDATE "{SCHEMA}"."region_map"
                        SET to_norm = :to_norm
                        WHERE from_norm = :from_norm
                    """),
                    {"to_norm": v, "from_norm": k},
                )
        else:
            conn.execute(
                text(f"""
                    INSERT INTO "{SCHEMA}"."region_map"(from_norm, to_norm)
                    VALUES (:from_norm, :to_norm)
                """),
                {"from_norm": k, "to_norm": v},
            )

    conn.execute(text(f"""
        UPDATE "{SCHEMA}"."{CHILD}" AS g
        SET {REGION_COL} = m.to_norm
        FROM "{SCHEMA}"."region_map" AS m
        WHERE g.{REGION_COL} = m.from_norm
    """))


# ---------- MAIN ----------

def main():
    t0 = time.time()
    out: Dict[str, Any] = {
        "module": "silver_maintenance",
        "ok": True,
        "elapsed_s": 0.0,
        "dedup_parent": {},
        "dedup_child": {},
        "region_preview": {},
        "region_rows_updated_base": 0,
        "region_map_applied": False,
        "region_distinct_after": [],
    }

    try:
        with engine.begin() as conn:
            conn.execute(text("SET LOCAL statement_timeout = '5min'"))

            ded1 = dedup_summarized_with_geo_cascade(conn)
            out["dedup_parent"] = ded1

            ded2 = dedup_summarized_geo(conn)
            out["dedup_child"] = ded2

            reg_prev = preview_region_before_after(conn)
            out["region_preview"] = reg_prev

            rows_upd = apply_base_region_normalization(conn)
            out["region_rows_updated_base"] = rows_upd

            sync_region_map(conn)
            out["region_map_applied"] = True

            final_vals = collect_distinct(conn, CHILD, REGION_COL, limit=200)
            out["region_distinct_after"] = final_vals[:100]

    except SQLAlchemyError as e:
        log.exception("db error")
        out["ok"] = False
        out["error"] = f"SQLAlchemyError: {e.__class__.__name__}: {e}"

    except Exception as e:
        log.exception("unexpected")
        out["ok"] = False
        out["error"] = f"{e.__class__.__name__}: {e}"

    finally:
        out["elapsed_s"] = round(time.time() - t0, 3)
        sys.stdout.write(json.dumps(out, ensure_ascii=False) + "\n")
        sys.stdout.flush()


if __name__ == "__main__":
    main()