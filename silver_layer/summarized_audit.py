

from __future__ import annotations
import sys, json, time, logging
from pathlib import Path
from typing import List, Set, Dict
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection

# ===== LOGGING  =====
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | silver_audit | %(message)s",
    handlers=[logging.StreamHandler(sys.stderr)],
    force=True,
)
log = logging.getLogger("silver_audit")

# ===== CONFIG =====
PROJECT_ROOT = Path(__file__).resolve().parents[1]
cfg_path = PROJECT_ROOT / "config.json"
if not cfg_path.exists():
    cfg_path = Path(__file__).resolve().parent / "config.json"

with open(cfg_path, "r", encoding="utf-8") as f:
    cfg = json.load(f)

DB_URL = f"postgresql+psycopg2://{cfg['USER']}:{cfg['PWD']}@{cfg['HOST']}:{cfg['PORT']}/{cfg['DB']}"
engine = create_engine(DB_URL, pool_pre_ping=True)

SRC_TABLES = [
    ("public", "sreality_typical", 1),
    ("public", "bezrealitky_typical", 2),
    ("public", "idnes_typical", 3),
]

EXCLUDE_COLS = {
    "ingested_at", "ingested_ts", "created_at", "updated_at",
    "source_id", "site_id"
}

SAMPLE_LIMIT = 20


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
    if not _table_exists(conn, "silver", "summarized"):
        raise RuntimeError("silver.summarized not found")

    silver_cols = set(_columns(conn, "silver", "summarized")) - EXCLUDE_COLS

    src_sets: List[Set[str]] = []
    for sch, tbl, _sid in SRC_TABLES:
        if _table_exists(conn, sch, tbl):
            s = set(_columns(conn, sch, tbl)) - EXCLUDE_COLS
            src_sets.append(s)

    if not src_sets:
        return []

    common = silver_cols.intersection(*src_sets)
    return sorted(common)


def _build_sources_union_sql(conn: Connection, common_cols: List[str]) -> str:
    select_blocks = []
    for sch, tbl, sid in SRC_TABLES:
        if not _table_exists(conn, sch, tbl):
            log.warning("source table missing: %s.%s", sch, tbl)
            continue

        tbl_cols = set(_columns(conn, sch, tbl))
        cols_expr = []
        for c in common_cols:
            if c in tbl_cols:
                cols_expr.append(f'CAST("{c}" AS text) AS "{c}"')
            else:
                cols_expr.append(f'NULL::text AS "{c}"')

        block = f"""
            SELECT
                {sid}::int AS source_id,
                site_id::text AS site_id,
                {", ".join(cols_expr)}
            FROM {sch}.{tbl}
        """
        select_blocks.append(block)

    if not select_blocks:
        return ""

    return "\nUNION ALL\n".join(select_blocks)


def _build_cmp_exprs(silver_types: Dict[str, str], cols: List[str]) -> List[str]:
    exprs = []
    for c in cols:
        t = silver_types.get(c, "text")
        if t in ("date", "timestamp without time zone", "timestamp with time zone"):
            exprs.append(f'(CAST(s."{c}" AS date) IS DISTINCT FROM CAST(u."{c}" AS date))')
        elif t in ("integer", "bigint", "smallint"):
            exprs.append(f'(CAST(s."{c}" AS numeric) IS DISTINCT FROM CAST(u."{c}" AS numeric))')
        elif t in ("double precision", "real", "numeric", "decimal"):
            exprs.append(f'(CAST(s."{c}" AS numeric) IS DISTINCT FROM CAST(u."{c}" AS numeric))')
        elif t in ("boolean",):
            exprs.append(f'(CAST(s."{c}" AS boolean) IS DISTINCT FROM CAST(u."{c}" AS boolean))')
        else:
            exprs.append(f'(CAST(s."{c}" AS text) IS DISTINCT FROM CAST(u."{c}" AS text))')
    return exprs


def main() -> dict:
    t0 = time.perf_counter()
    result = {
        "stage": "silver_audit",
        "stats": {},
        "mismatches": {"by_column": {}, "samples": []},
        "sanity": {},
        "elapsed_s": None,
    }

    with engine.begin() as conn:
        if not _table_exists(conn, "silver", "summarized"):
            raise RuntimeError("silver.summarized does not exist")

        if not any(_table_exists(conn, s, t) for s, t, _ in SRC_TABLES):
            raise RuntimeError("no source typical tables present")

        common_cols = _find_common_columns(conn)
        log.info("common columns for value-compare: %s", common_cols)

        union_sql = _build_sources_union_sql(conn, common_cols)
        if not union_sql:
            raise RuntimeError("failed to build sources union")

        silver_types = _column_types(conn, "silver", "summarized")

        q_totals = f"""
        WITH src_union AS (
            {union_sql}
        )
        , src_keys AS (
            SELECT DISTINCT source_id::int AS source_id, site_id::text AS site_id
            FROM src_union
        )
        , silver_keys AS (
            SELECT DISTINCT source_id::int AS source_id, site_id::text AS site_id
            FROM silver.summarized
        )
        SELECT
            (SELECT COUNT(*) FROM src_keys)    AS src_total_keys,
            (SELECT COUNT(*) FROM silver_keys) AS silver_total_keys,
            (SELECT COUNT(*) FROM (
                 SELECT s.source_id, s.site_id
                 FROM src_keys s
                 LEFT JOIN silver_keys k
                   ON k.source_id = s.source_id
                  AND k.site_id   = s.site_id
                 WHERE k.site_id IS NULL
            ) z) AS missing_in_silver,
            (SELECT COUNT(*) FROM (
                 SELECT k.source_id, k.site_id
                 FROM silver_keys k
                 LEFT JOIN src_keys s
                   ON s.source_id = k.source_id
                  AND s.site_id   = k.site_id
                 WHERE s.site_id IS NULL
            ) z) AS extra_in_silver
        """
        r = conn.execute(text(q_totals)).mappings().first()
        result["stats"].update({
            "src_total_keys": int(r["src_total_keys"] or 0),
            "silver_total_keys": int(r["silver_total_keys"] or 0),
            "missing_in_silver": int(r["missing_in_silver"] or 0),
            "extra_in_silver": int(r["extra_in_silver"] or 0),
        })

        q_samples = f"""
        WITH src_union AS ( {union_sql} ),
        src_keys AS (
          SELECT DISTINCT source_id::int AS source_id, site_id::text AS site_id
          FROM src_union
        ),
        silver_keys AS (
          SELECT DISTINCT source_id::int AS source_id, site_id::text AS site_id
          FROM silver.summarized
        ),
        missing AS (
          SELECT 'missing'::text AS kind, s.source_id, s.site_id
          FROM src_keys s
          LEFT JOIN silver_keys k
            ON k.source_id = s.source_id
           AND k.site_id   = s.site_id
          WHERE k.site_id IS NULL
          LIMIT :lim
        ),
        extra AS (
          SELECT 'extra'::text AS kind, k.source_id, k.site_id
          FROM silver_keys k
          LEFT JOIN src_keys s
            ON s.source_id = k.source_id
           AND s.site_id   = k.site_id
          WHERE s.site_id IS NULL
          LIMIT :lim
        )
        SELECT * FROM missing
        UNION ALL
        SELECT * FROM extra
        """
        rows = conn.execute(text(q_samples), {"lim": SAMPLE_LIMIT}).fetchall()
        result["stats"]["key_samples"] = [
            {"kind": kind, "source_id": int(sid), "site_id": site_id}
            for kind, sid, site_id in rows
        ]

        q_dups = text("""
            SELECT
              COUNT(*) AS total_rows,
              COUNT(DISTINCT (source_id, site_id::text)) AS distinct_keys,
              (COUNT(*) - COUNT(DISTINCT (source_id, site_id::text))) AS dupes,
              SUM(CASE WHEN source_id IS NULL THEN 1 ELSE 0 END) AS null_source_id,
              SUM(CASE WHEN site_id   IS NULL THEN 1 ELSE 0 END) AS null_site_id
            FROM silver.summarized
        """)
        d = conn.execute(q_dups).mappings().first()
        result["stats"].update({
            "silver_rows": int(d["total_rows"] or 0),
            "silver_distinct_keys": int(d["distinct_keys"] or 0),
            "silver_dupes": int(d["dupes"] or 0),
            "silver_null_source_id": int(d["null_source_id"] or 0),
            "silver_null_site_id": int(d["null_site_id"] or 0),
        })

        q_av = text("""
            SELECT COUNT(*) AS bad
            FROM silver.summarized
            WHERE (archived_date IS NULL  AND available IS NOT TRUE )
               OR (archived_date IS NOT NULL AND available IS NOT FALSE)
        """)
        bad = int(conn.execute(q_av).scalar() or 0)
        result["sanity"]["available_archived_mismatch"] = bad

        if common_cols:
            cmp_exprs = _build_cmp_exprs(silver_types, common_cols)

            col_sums = ", ".join([
                f"SUM(CASE WHEN {e} THEN 1 ELSE 0 END) AS \"{c}\""
                for e, c in zip(cmp_exprs, common_cols)
            ])
            q_agg = f"""
            WITH src_union AS ( {union_sql} ),
            src_dedup AS (
              SELECT source_id::int AS source_id,
                     site_id::text  AS site_id,
                     {", ".join([f'"{c}"' for c in common_cols])},
                     ROW_NUMBER() OVER (PARTITION BY source_id, site_id ORDER BY site_id) AS rn
              FROM src_union
            ),
            src_one AS ( SELECT * FROM src_dedup WHERE rn=1 ),
            diff AS (
              SELECT {col_sums}
              FROM silver.summarized s
              JOIN src_one u
                ON u.source_id = s.source_id::int
               AND u.site_id   = s.site_id::text
            )
            SELECT * FROM diff
            """
            agg = conn.execute(text(q_agg)).mappings().first() or {}
            result["mismatches"]["by_column"] = {c: int(agg.get(c) or 0) for c in common_cols}

            diff_filters = " OR ".join(cmp_exprs)
            select_pairs = []
            for c in common_cols:
                select_pairs.append(f'CAST(s."{c}" AS text) AS "silver__{c}"')
                select_pairs.append(f'CAST(u."{c}" AS text) AS "source__{c}"')

            q_examples = f"""
            WITH src_union AS ( {union_sql} ),
            src_dedup AS (
              SELECT source_id::int AS source_id,
                     site_id::text  AS site_id,
                     {", ".join([f'"{c}"' for c in common_cols])},
                     ROW_NUMBER() OVER (PARTITION BY source_id, site_id ORDER BY site_id) AS rn
              FROM src_union
            ),
            src_one AS ( SELECT * FROM src_dedup WHERE rn=1 )
            SELECT
              s.source_id::int AS source_id,
              s.site_id::text  AS site_id,
              {", ".join(select_pairs)}
            FROM silver.summarized s
            JOIN src_one u
              ON u.source_id = s.source_id::int
             AND u.site_id   = s.site_id::text
            WHERE {diff_filters}
            LIMIT :lim
            """
            ex_rows = conn.execute(text(q_examples), {"lim": SAMPLE_LIMIT}).mappings().fetchall()
            for row in ex_rows:
                item = {"source_id": int(row["source_id"]), "site_id": str(row["site_id"])}
                for c in common_cols:
                    item[f"silver.{c}"] = row.get(f"silver__{c}")
                    item[f"source.{c}"] = row.get(f"source__{c}")
                result["mismatches"]["samples"].append(item)

        result["elapsed_s"] = round(time.perf_counter() - t0, 3)

    print(json.dumps(result, ensure_ascii=False))
    sys.stdout.flush()
    return result


if __name__ == "__main__":
    main()