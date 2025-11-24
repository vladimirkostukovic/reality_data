from __future__ import annotations
import json
import logging
import sys
import time
from pathlib import Path
from typing import List, Dict, Tuple
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# ---------- LOGGING ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | bi_star | %(message)s",
    handlers=[logging.StreamHandler(sys.stderr)],
    force=True
)
log = logging.getLogger("bi_star")

# ---------- CONFIG ----------
PROJECT_ROOT = Path(__file__).resolve().parents[1]
cfg = json.loads((PROJECT_ROOT / "config.json").read_text(encoding="utf-8"))

db_user = cfg.get("user") or cfg.get("USER")
db_pwd = cfg.get("password") or cfg.get("PWD")
db_host = cfg.get("host") or cfg.get("HOST")
db_port = cfg.get("port") or cfg.get("PORT")
db_name = cfg.get("dbname") or cfg.get("DB")
DB_URL = f"postgresql+psycopg2://{db_user}:{db_pwd}@{db_host}:{db_port}/{db_name}"
engine = create_engine(DB_URL, pool_pre_ping=True, connect_args={"connect_timeout": 10})

BI_SCHEMA = "bi_reality_data"


# ---------- UTILS ----------
def _pg_diag_payload(exc: Exception):
    d = {"type": type(exc).__name__}
    orig = getattr(exc, "orig", None)
    if orig is not None:
        d["pgcode"] = getattr(orig, "pgcode", None)
        d["pgerror"] = getattr(orig, "pgerror", None)
        diag = getattr(orig, "diag", None)
        if diag:
            for k in ("schema_name", "table_name", "column_name", "datatype_name",
                      "constraint_name", "message_primary", "message_detail", "context"):
                v = getattr(diag, k, None)
                if v:
                    d[k] = str(v)
    stmt = getattr(exc, "statement", None)
    if stmt:
        d["statement_preview"] = stmt[:300]
    return d


def fq(schema: str, name: str) -> str:
    return f'"{schema}"."{name}"'


def schema_exists(conn, schema: str) -> bool:
    return bool(conn.execute(
        text("SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = :s)"),
        {"s": schema}
    ).scalar())


def table_exists(conn, schema: str, name: str) -> bool:
    return bool(conn.execute(
        text("SELECT to_regclass(:r) IS NOT NULL"),
        {"r": f"{schema}.{name}"}
    ).scalar())


def count_rows(conn, schema: str, table: str) -> int:
    return int(conn.execute(text(f"SELECT COUNT(*) FROM {fq(schema, table)}")).scalar() or 0)


def publish_table(conn, schema: str, target: str, stage: str):
    conn.execute(text(f'DROP TABLE IF EXISTS {fq(schema, target)}'))
    conn.execute(text(f'ALTER TABLE {fq(schema, stage)} RENAME TO "{target}"'))
    conn.execute(text(f'ANALYZE {fq(schema, target)}'))


# ---------- NORMALIZATION / SPLITS ----------
DEAL_TYPE_NORMALIZE_SQL = """
CASE
  WHEN lower(deal_type) IN ('prodej','sale','sell') THEN 'prodej'
  WHEN lower(deal_type) IN ('pronajem','pronájem','rent','rental','lease') THEN 'pronajem'
  WHEN lower(deal_type) IN ('drazba','dražba','auction','aukce') THEN 'drazba'
  ELSE 'ostatni'
END
"""

BUCKETS_SQL_CASE = """
CASE
  WHEN category_name ILIKE 'byt%%'    THEN 'byty'
  WHEN category_name ILIKE 'dom%%'    THEN 'domy'
  WHEN category_name ILIKE 'pozem%%'  THEN 'pozemky'
  WHEN category_name ILIKE 'komerc%%' THEN 'komercni'
  ELSE 'ostatni'
END
"""

DEALS: List[str] = ["prodej", "pronajem", "drazba"]
BUCKETS: List[str] = ["byty", "domy", "pozemky", "komercni", "ostatni"]


# ---------- DIMENSIONS ----------
def rebuild_dim_date(conn) -> int:
    stage = "dim_date__stage"
    conn.execute(text(f'DROP TABLE IF EXISTS {fq(BI_SCHEMA, stage)}'))
    conn.execute(text(f"""
        CREATE TABLE {fq(BI_SCHEMA, stage)} AS
        WITH mm AS (
          SELECT
            MIN(added_date)::date                           AS dmin,
            GREATEST(
              COALESCE(MAX(archived_date)::date, DATE '1900-01-01'),
              COALESCE(MAX(added_date)::date,   DATE '1900-01-01'),
              CURRENT_DATE
            )                                               AS dmax
          FROM gold.totalized
          WHERE added_date IS NOT NULL
        ),
        cal AS (
          SELECT gs::date AS d
          FROM mm, generate_series(mm.dmin, mm.dmax, interval '1 day') AS gs
        )
        SELECT
          d                            AS date_key,
          EXTRACT(YEAR    FROM d)::int AS year,
          EXTRACT(QUARTER FROM d)::int AS quarter,
          EXTRACT(MONTH   FROM d)::int AS month,
          TO_CHAR(d, 'Mon')            AS month_name,
          EXTRACT(WEEK    FROM d)::int AS week_iso,
          EXTRACT(DAY     FROM d)::int AS day,
          EXTRACT(ISODOW  FROM d)::int AS dow_iso,
          (EXTRACT(ISODOW  FROM d) IN (6,7)) AS is_weekend
        FROM cal;
    """))
    publish_table(conn, BI_SCHEMA, "dim_date", stage)
    conn.execute(text(f'ALTER TABLE {fq(BI_SCHEMA, "dim_date")} ADD PRIMARY KEY (date_key)'))
    return count_rows(conn, BI_SCHEMA, "dim_date")


def rebuild_dim_geo(conn) -> int:
    stage = "dim_geo__stage"
    conn.execute(text(f'DROP TABLE IF EXISTS {fq(BI_SCHEMA, stage)}'))
    conn.execute(text(f"""
        CREATE TABLE {fq(BI_SCHEMA, stage)} AS
        WITH src AS (
          SELECT DISTINCT
            NULLIF(trim(norm_district),    '') AS region_name,
            NULLIF(trim(norm_okres),       '') AS district_name,
            NULLIF(trim(norm_city),        '') AS city_name,
            NULLIF(trim(norm_city_part),   '') AS city_part_name,
            NULLIF(trim(norm_street),      '') AS street_name,
            NULLIF(trim(norm_house_number),'') AS house_number
          FROM gold.totalized
        ),
        normed AS (
          SELECT
            COALESCE(region_name,   'Neznámý kraj')  AS region,
            COALESCE(district_name, 'Neznámý okres') AS district,
            COALESCE(city_name,     '')              AS city,
            COALESCE(city_part_name,'')              AS city_part,
            COALESCE(street_name,   '')              AS street,
            COALESCE(house_number,  '')              AS house_number,
            'CZ'                                     AS country,
            lower(regexp_replace(COALESCE(region_name,   'Neznámý kraj'),  '\\s+', ' ', 'g')) AS region_norm,
            lower(regexp_replace(COALESCE(district_name, 'Neznámý okres'), '\\s+', ' ', 'g')) AS district_norm,
            lower(regexp_replace(COALESCE(city_name,     ''),              '\\s+', ' ', 'g')) AS city_norm,
            lower(regexp_replace(COALESCE(city_part_name,''),              '\\s+', ' ', 'g')) AS city_part_norm,
            lower(regexp_replace(COALESCE(street_name,   ''),              '\\s+', ' ', 'g')) AS street_norm,
            lower(regexp_replace(COALESCE(house_number,  ''),              '\\s+', '',  'g')) AS house_number_norm
          FROM src
        ),
        dedup AS (
          SELECT
            MIN(region)       AS region,
            MIN(district)     AS district,
            MIN(city)         AS city,
            MIN(city_part)    AS city_part,
            MIN(street)       AS street,
            MIN(house_number) AS house_number,
            country,
            region_norm, district_norm, city_norm, city_part_norm, street_norm, house_number_norm
          FROM normed
          GROUP BY
            region_norm, district_norm, city_norm, city_part_norm, street_norm, house_number_norm, country
        )
        SELECT
          region, district, city, city_part, street, house_number, country,
          region_norm, district_norm, city_norm, city_part_norm, street_norm, house_number_norm
        FROM dedup;
    """))
    publish_table(conn, BI_SCHEMA, "dim_geo", stage)

    conn.execute(text(f'ALTER TABLE {fq(BI_SCHEMA, "dim_geo")} ADD COLUMN IF NOT EXISTS geo_key BIGSERIAL'))
    conn.execute(text(f'ALTER TABLE {fq(BI_SCHEMA, "dim_geo")} ADD PRIMARY KEY (geo_key)'))

    conn.execute(text(f'''
        CREATE UNIQUE INDEX IF NOT EXISTS dim_geo_uniq_norm
        ON {fq(BI_SCHEMA, "dim_geo")}
          (region_norm, district_norm, city_norm, city_part_norm, street_norm, house_number_norm, country)
    '''))

    conn.execute(text(f'CREATE INDEX IF NOT EXISTS dim_geo_region_idx   ON {fq(BI_SCHEMA, "dim_geo")} (region)'))
    conn.execute(text(f'CREATE INDEX IF NOT EXISTS dim_geo_district_idx ON {fq(BI_SCHEMA, "dim_geo")} (district)'))
    conn.execute(text(f'CREATE INDEX IF NOT EXISTS dim_geo_city_idx     ON {fq(BI_SCHEMA, "dim_geo")} (city)'))
    conn.execute(text(f'CREATE INDEX IF NOT EXISTS dim_geo_street_idx   ON {fq(BI_SCHEMA, "dim_geo")} (street)'))
    return count_rows(conn, BI_SCHEMA, "dim_geo")


def rebuild_dim_seller(conn) -> int:
    stage = "dim_seller__stage"
    conn.execute(text(f'DROP TABLE IF EXISTS {fq(BI_SCHEMA, stage)}'))
    conn.execute(text(f"""
        CREATE TABLE {fq(BI_SCHEMA, stage)} AS
        SELECT DISTINCT
          COALESCE(agent_name,'')        AS seller_name,
          COALESCE(category_name,'')     AS seller_type,
          COALESCE(source_id::text,'')   AS seller_domain,
          COALESCE(agent_phone,'')       AS phone_norm,
          COALESCE(agent_email,'')       AS email_norm
        FROM gold.totalized;
    """))
    publish_table(conn, BI_SCHEMA, "dim_seller", stage)
    conn.execute(text(f'ALTER TABLE {fq(BI_SCHEMA, "dim_seller")} ADD COLUMN IF NOT EXISTS seller_key BIGSERIAL'))
    conn.execute(text(f'ALTER TABLE {fq(BI_SCHEMA, "dim_seller")} ADD PRIMARY KEY (seller_key)'))
    conn.execute(text(f'''
        CREATE UNIQUE INDEX IF NOT EXISTS dim_seller_uniq
        ON {fq(BI_SCHEMA, "dim_seller")} (seller_name, seller_type, seller_domain, phone_norm, email_norm)
    '''))
    return count_rows(conn, BI_SCHEMA, "dim_seller")


def rebuild_dim_object(conn) -> int:
    stage = "dim_object__stage"
    conn.execute(text(f'DROP TABLE IF EXISTS {fq(BI_SCHEMA, stage)}'))
    conn.execute(text(f"""
        CREATE TABLE {fq(BI_SCHEMA, stage)} AS
        WITH pick AS (
          SELECT DISTINCT ON (t.object_id)
                 t.object_id,
                 t.deal_type,
                 t.category_name AS object_type,
                 t.rooms,
                 t.area_build    AS area_build_m2,
                 t.area_land     AS area_land_m2,
                 t.longitude,
                 t.latitude,
                 t.name
          FROM gold.totalized t
          WHERE t.object_id IS NOT NULL
          ORDER BY t.object_id, t.updated_at DESC NULLS LAST, t.added_date DESC NULLS LAST
        )
        SELECT * FROM pick;
    """))
    publish_table(conn, BI_SCHEMA, "dim_object", stage)
    conn.execute(text(f'ALTER TABLE {fq(BI_SCHEMA, "dim_object")} ADD PRIMARY KEY (object_id)'))
    return count_rows(conn, BI_SCHEMA, "dim_object")


# ---------- FACTS: ACTIVE / CLOSED ----------
G_JOIN_REGION = "lower(regexp_replace(COALESCE(r.norm_district,'Neznámý kraj'),  '\\\\s+',' ','g'))"
G_JOIN_DIST = "lower(regexp_replace(COALESCE(r.norm_okres,'Neznámý okres'),    '\\\\s+',' ','g'))"
G_JOIN_CITY = "lower(regexp_replace(COALESCE(r.norm_city,''),                  '\\\\s+',' ','g'))"
G_JOIN_PART = "lower(regexp_replace(COALESCE(r.norm_city_part,''),             '\\\\s+',' ','g'))"
G_JOIN_STREET = "lower(regexp_replace(COALESCE(r.norm_street,''),                '\\\\s+',' ','g'))"
G_JOIN_HNO = "lower(regexp_replace(COALESCE(r.norm_house_number,''),          '\\\\s+','',  'g'))"


def _fact_sql(is_closed: bool) -> str:
    select_closed = "t.archived_date::date  AS archived_date," if is_closed else ""
    order_tail = ", r.archived_date" if is_closed else ""
    return f"""
        WITH base AS (
          SELECT
            t.object_id,
            t.internal_id,
            t.source_id,
            t.available,
            t.price,
            t.deal_type,
            t.category_name,
            t.agent_name,
            t.agent_phone,
            t.agent_email,
            t.norm_district, t.norm_okres, t.norm_city, t.norm_city_part, t.norm_street, t.norm_house_number,
            t.area_build, t.area_land,
            t.added_date::date   AS added_date,
            {select_closed}
            t.updated_at
          FROM gold.totalized t
          WHERE t.object_id IS NOT NULL
            AND t.added_date IS NOT NULL
            {"AND t.archived_date IS NULL" if not is_closed else "AND t.archived_date IS NOT NULL"}
        ),
        rep AS (
          SELECT *
          FROM (
            SELECT
              b.*,
              ROW_NUMBER() OVER (
                PARTITION BY b.object_id
                ORDER BY
                  {"b.archived_date DESC," if is_closed else ""}
                  b.updated_at DESC NULLS LAST,
                  b.added_date DESC,
                  b.source_id NULLS LAST,
                  b.internal_id NULLS LAST
              ) AS rn
            FROM base b
          ) z
          WHERE rn = 1
        ),
        g1 AS (
          SELECT r.object_id, g.geo_key
          FROM rep r
          LEFT JOIN {fq(BI_SCHEMA, "dim_geo")} g
            ON g.region_norm       = {G_JOIN_REGION}
           AND g.district_norm     = {G_JOIN_DIST}
           AND g.city_norm         = {G_JOIN_CITY}
           AND g.city_part_norm    = {G_JOIN_PART}
           AND g.street_norm       = {G_JOIN_STREET}
           AND g.house_number_norm = {G_JOIN_HNO}
        ),
        g2 AS (
          SELECT r.object_id, g.geo_key
          FROM rep r
          LEFT JOIN {fq(BI_SCHEMA, "dim_geo")} g
            ON g.region_norm       = {G_JOIN_REGION}
           AND g.district_norm     = {G_JOIN_DIST}
           AND g.city_norm         = {G_JOIN_CITY}
           AND g.city_part_norm    = {G_JOIN_PART}
           AND g.street_norm       = {G_JOIN_STREET}
           AND g.house_number_norm = ''
        ),
        g3 AS (
          SELECT r.object_id, g.geo_key
          FROM rep r
          LEFT JOIN {fq(BI_SCHEMA, "dim_geo")} g
            ON g.region_norm       = {G_JOIN_REGION}
           AND g.district_norm     = {G_JOIN_DIST}
           AND g.city_norm         = {G_JOIN_CITY}
           AND g.city_part_norm    = {G_JOIN_PART}
           AND g.street_norm       = ''
           AND g.house_number_norm = ''
        ),
        g4 AS (
          SELECT r.object_id, g.geo_key
          FROM rep r
          LEFT JOIN {fq(BI_SCHEMA, "dim_geo")} g
            ON g.region_norm       = {G_JOIN_REGION}
           AND g.district_norm     = {G_JOIN_DIST}
           AND g.city_norm         = {G_JOIN_CITY}
           AND g.city_part_norm    = ''
           AND g.street_norm       = ''
           AND g.house_number_norm = ''
        )
        SELECT
          r.object_id,
          r.added_date,
          {"r.archived_date," if is_closed else ""}
          r.internal_id   AS internal_id_rep,
          r.source_id     AS source_id_rep,
          COALESCE(g1.geo_key, g2.geo_key, g3.geo_key, g4.geo_key) AS geo_key,
          {DEAL_TYPE_NORMALIZE_SQL} AS deal_type,
          r.category_name,
          r.price,
          r.available,
          r.agent_name    AS agent_name_rep,
          r.agent_phone   AS agent_phone_rep,
          r.agent_email   AS agent_email_rep,
          r.area_build,
          r.area_land
        FROM rep r
        LEFT JOIN g1 USING (object_id)
        LEFT JOIN g2 USING (object_id)
        LEFT JOIN g3 USING (object_id)
        LEFT JOIN g4 USING (object_id)
        ORDER BY r.object_id, r.added_date{order_tail};
    """


def rebuild_fact_active(conn) -> int:
    stage = "fact_active__stage"
    conn.execute(text(f'DROP TABLE IF EXISTS {fq(BI_SCHEMA, stage)}'))
    conn.execute(text(f"CREATE TABLE {fq(BI_SCHEMA, stage)} AS {_fact_sql(is_closed=False)}"))
    publish_table(conn, BI_SCHEMA, "fact_active", stage)
    conn.execute(text(f'ALTER TABLE {fq(BI_SCHEMA, "fact_active")} ALTER COLUMN added_date SET NOT NULL'))
    conn.execute(text(f'ALTER TABLE {fq(BI_SCHEMA, "fact_active")} ADD PRIMARY KEY (object_id, added_date)'))
    conn.execute(
        text(f'CREATE INDEX IF NOT EXISTS fact_active_deal_idx  ON {fq(BI_SCHEMA, "fact_active")} (deal_type)'))
    conn.execute(text(f'CREATE INDEX IF NOT EXISTS fact_active_geo_idx   ON {fq(BI_SCHEMA, "fact_active")} (geo_key)'))
    conn.execute(text(f'CREATE INDEX IF NOT EXISTS fact_active_price_idx ON {fq(BI_SCHEMA, "fact_active")} (price)'))
    return count_rows(conn, BI_SCHEMA, "fact_active")


def rebuild_fact_closed(conn) -> int:
    stage = "fact_closed__stage"
    conn.execute(text(f'DROP TABLE IF EXISTS {fq(BI_SCHEMA, stage)}'))
    conn.execute(text(f"CREATE TABLE {fq(BI_SCHEMA, stage)} AS {_fact_sql(is_closed=True)}"))
    publish_table(conn, BI_SCHEMA, "fact_closed", stage)
    conn.execute(text(f'ALTER TABLE {fq(BI_SCHEMA, "fact_closed")} ALTER COLUMN added_date SET NOT NULL'))
    conn.execute(text(f'ALTER TABLE {fq(BI_SCHEMA, "fact_closed")} ALTER COLUMN archived_date SET NOT NULL'))
    conn.execute(text(f'ALTER TABLE {fq(BI_SCHEMA, "fact_closed")} ADD PRIMARY KEY (object_id, added_date)'))
    conn.execute(
        text(f'CREATE INDEX IF NOT EXISTS fact_closed_deal_idx  ON {fq(BI_SCHEMA, "fact_closed")} (deal_type)'))
    conn.execute(text(f'CREATE INDEX IF NOT EXISTS fact_closed_geo_idx   ON {fq(BI_SCHEMA, "fact_closed")} (geo_key)'))
    conn.execute(text(f'CREATE INDEX IF NOT EXISTS fact_closed_price_idx ON {fq(BI_SCHEMA, "fact_closed")} (price)'))
    return count_rows(conn, BI_SCHEMA, "fact_closed")


# ---------- SPLITS ----------
def rebuild_split_active(conn) -> Dict[str, int]:
    stats: Dict[str, int] = {}
    if not table_exists(conn, BI_SCHEMA, "fact_active"):
        raise RuntimeError("fact_active is missing; build it first")
    for deal in DEALS:
        for bucket in BUCKETS:
            stage = f"fact_active_{deal}_{bucket}__stage"
            target = f"fact_active_{deal}_{bucket}"
            cnt = conn.execute(text(f"""
                SELECT COUNT(*) FROM {fq(BI_SCHEMA, "fact_active")}
                WHERE deal_type = :deal AND {BUCKETS_SQL_CASE} = :bucket
            """), {"deal": deal, "bucket": bucket}).scalar()
            if not cnt:
                conn.execute(text(f'DROP TABLE IF EXISTS {fq(BI_SCHEMA, target)}'))
                stats[f"rows_{target}"] = 0
                continue
            conn.execute(text(f'DROP TABLE IF EXISTS {fq(BI_SCHEMA, stage)}'))
            conn.execute(text(f"""
                CREATE TABLE {fq(BI_SCHEMA, stage)} AS
                SELECT object_id, added_date, internal_id_rep, source_id_rep, geo_key,
                       deal_type, category_name, price, available,
                       agent_name_rep, agent_phone_rep, agent_email_rep,
                       area_build, area_land
                FROM {fq(BI_SCHEMA, "fact_active")}
                WHERE deal_type = :deal AND {BUCKETS_SQL_CASE} = :bucket
            """), {"deal": deal, "bucket": bucket})
            publish_table(conn, BI_SCHEMA, target, stage)
            conn.execute(text(f'ALTER TABLE {fq(BI_SCHEMA, target)} ADD PRIMARY KEY (object_id, added_date)'))
            conn.execute(text(f'CREATE INDEX IF NOT EXISTS {target}_geo_idx   ON {fq(BI_SCHEMA, target)} (geo_key)'))
            conn.execute(text(f'CREATE INDEX IF NOT EXISTS {target}_price_idx ON {fq(BI_SCHEMA, target)} (price)'))
            stats[f"rows_{target}"] = count_rows(conn, BI_SCHEMA, target)
    return stats


def rebuild_split_closed(conn) -> Dict[str, int]:
    stats: Dict[str, int] = {}
    if not table_exists(conn, BI_SCHEMA, "fact_closed"):
        raise RuntimeError("fact_closed is missing; build it first")
    for deal in DEALS:
        for bucket in BUCKETS:
            stage = f"fact_closed_{deal}_{bucket}__stage"
            target = f"fact_closed_{deal}_{bucket}"
            cnt = conn.execute(text(f"""
                SELECT COUNT(*) FROM {fq(BI_SCHEMA, "fact_closed")}
                WHERE deal_type = :deal AND {BUCKETS_SQL_CASE} = :bucket
            """), {"deal": deal, "bucket": bucket}).scalar()
            if not cnt:
                conn.execute(text(f'DROP TABLE IF EXISTS {fq(BI_SCHEMA, target)}'))
                stats[f"rows_{target}"] = 0
                continue
            conn.execute(text(f'DROP TABLE IF EXISTS {fq(BI_SCHEMA, stage)}'))
            conn.execute(text(f"""
                CREATE TABLE {fq(BI_SCHEMA, stage)} AS
                SELECT object_id, added_date, archived_date, internal_id_rep, source_id_rep, geo_key,
                       deal_type, category_name, price, available,
                       agent_name_rep, agent_phone_rep, agent_email_rep,
                       area_build, area_land
                FROM {fq(BI_SCHEMA, "fact_closed")}
                WHERE deal_type = :deal AND {BUCKETS_SQL_CASE} = :bucket
            """), {"deal": deal, "bucket": bucket})
            publish_table(conn, BI_SCHEMA, target, stage)
            conn.execute(text(f'ALTER TABLE {fq(BI_SCHEMA, target)} ADD PRIMARY KEY (object_id, added_date)'))
            conn.execute(text(f'CREATE INDEX IF NOT EXISTS {target}_geo_idx   ON {fq(BI_SCHEMA, target)} (geo_key)'))
            conn.execute(text(f'CREATE INDEX IF NOT EXISTS {target}_price_idx ON {fq(BI_SCHEMA, target)} (price)'))
            stats[f"rows_{target}"] = count_rows(conn, BI_SCHEMA, target)
    return stats


# ---------- SANITY CHECKS ----------
def run_sanity_checks(conn) -> Tuple[bool, Dict[str, int]]:
    stats: Dict[str, int] = {}

    totalized_distinct = int(conn.execute(text("""
        SELECT COUNT(DISTINCT object_id)
        FROM gold.totalized
        WHERE object_id IS NOT NULL AND added_date IS NOT NULL
    """)).scalar() or 0)
    stats["gold_totalized_distinct_objects"] = totalized_distinct

    fa_rows = count_rows(conn, BI_SCHEMA, "fact_active")
    fc_rows = count_rows(conn, BI_SCHEMA, "fact_closed")
    stats["fact_active_rows"] = fa_rows
    stats["fact_closed_rows"] = fc_rows

    union_distinct = int(conn.execute(text(f"""
        SELECT COUNT(DISTINCT object_id) FROM (
          SELECT object_id FROM {fq(BI_SCHEMA, 'fact_active')}
          UNION
          SELECT object_id FROM {fq(BI_SCHEMA, 'fact_closed')}
        ) u
    """)).scalar() or 0)
    stats["facts_union_distinct_objects"] = union_distinct

    intersect_cnt = int(conn.execute(text(f"""
        SELECT COUNT(*) FROM (
          SELECT a.object_id
          FROM {fq(BI_SCHEMA, 'fact_active')} a
          INNER JOIN {fq(BI_SCHEMA, 'fact_closed')} c
            ON c.object_id = a.object_id
          GROUP BY a.object_id
        ) z
    """)).scalar() or 0)
    stats["facts_intersection_objects"] = intersect_cnt

    placeholders = ",".join([f":d{i}" for i in range(len(DEALS))])
    params = {f"d{i}": d for i, d in enumerate(DEALS)}

    active_known = int(conn.execute(
        text(f"SELECT COUNT(*) FROM {fq(BI_SCHEMA, 'fact_active')} WHERE deal_type IN ({placeholders})"),
        params).scalar() or 0)
    closed_known = int(conn.execute(
        text(f"SELECT COUNT(*) FROM {fq(BI_SCHEMA, 'fact_closed')} WHERE deal_type IN ({placeholders})"),
        params).scalar() or 0)

    stats["fact_active_unknown_deals"] = fa_rows - active_known
    stats["fact_closed_unknown_deals"] = fc_rows - closed_known

    def sum_split(prefix: str) -> int:
        s = 0
        for deal in DEALS:
            for bucket in BUCKETS:
                name = f"{prefix}_{deal}_{bucket}"
                if table_exists(conn, BI_SCHEMA, name):
                    s += count_rows(conn, BI_SCHEMA, name)
        return s

    active_split_sum = sum_split("fact_active")
    closed_split_sum = sum_split("fact_closed")
    stats["active_split_sum"] = active_split_sum
    stats["closed_split_sum"] = closed_split_sum

    problems = []
    if totalized_distinct != union_distinct:
        problems.append(
            f"gold.distinct({totalized_distinct}) != facts.union.distinct({union_distinct}); "
            f"intersection={intersect_cnt}"
        )
    if stats["fact_active_unknown_deals"] or stats["fact_closed_unknown_deals"]:
        problems.append(
            f"unknown deal_type present: active={stats['fact_active_unknown_deals']}, "
            f"closed={stats['fact_closed_unknown_deals']}"
        )
    if active_split_sum != active_known or closed_split_sum != closed_known:
        problems.append(
            f"splits mismatch: active_splits={active_split_sum} vs active_known={active_known}; "
            f"closed_splits={closed_split_sum} vs closed_known={closed_known}"
        )

    if problems:
        top_active = conn.execute(text(f"""
            SELECT deal_type, COUNT(*) AS cnt
            FROM {fq(BI_SCHEMA, 'fact_active')}
            GROUP BY deal_type
            ORDER BY cnt DESC
            LIMIT 10
        """)).fetchall()
        top_closed = conn.execute(text(f"""
            SELECT deal_type, COUNT(*) AS cnt
            FROM {fq(BI_SCHEMA, 'fact_closed')}
            GROUP BY deal_type
            ORDER BY cnt DESC
            LIMIT 10
        """)).fetchall()

        problems.append(f"top_active_deal_type={[(r[0], r[1]) for r in top_active]}")
        problems.append(f"top_closed_deal_type={[(r[0], r[1]) for r in top_closed]}")

        raise RuntimeError("Sanity check failed: " + " | ".join(problems))

    return True, stats


# ---------- PRICE EVENTS ----------
def rebuild_price_change_events(conn) -> int:
    stage = "fact_object_price_events__stage"
    conn.execute(text(f'DROP TABLE IF EXISTS {fq(BI_SCHEMA, stage)}'))
    conn.execute(text(f"""
        CREATE TABLE {fq(BI_SCHEMA, stage)} AS
        SELECT
          NULL::bigint  AS object_id,
          NULL::date    AS event_date,
          NULL::date    AS prev_period_end,
          NULL::float8  AS prev_price,
          NULL::float8  AS new_price,
          NULL::float8  AS delta_abs,
          NULL::numeric AS delta_pct,
          NULL::bigint  AS internal_id_rep,
          NULL::text    AS source_id_rep,
          NULL::bigint  AS geo_key,
          NULL::text    AS deal_type,
          NULL::text    AS category_name,
          NULL::boolean AS available,
          NULL::text    AS agent_name_rep,
          NULL::text    AS agent_phone_rep,
          NULL::text    AS agent_email_rep
        WHERE FALSE
    """))
    publish_table(conn, BI_SCHEMA, "fact_object_price_events", stage)
    conn.execute(
        text(f'ALTER TABLE {fq(BI_SCHEMA, "fact_object_price_events")} ADD PRIMARY KEY (object_id, event_date)'))
    return 0


# ---------- MAIN ----------
def main():
    result = {
        "status": "success",
        "dimensions_created": 0,
        "facts_created": 0,
        "splits_created": 0,
        "timestamp": datetime.now().isoformat()
    }

    try:
        with engine.begin() as conn:
            if not schema_exists(conn, BI_SCHEMA):
                raise RuntimeError(f'schema "{BI_SCHEMA}" not found')

            ok = conn.execute(text("SELECT to_regclass('gold.totalized') IS NOT NULL")).scalar()
            if not ok:
                raise RuntimeError("gold.totalized not found")

            stats: Dict[str, int] = {}

            # Dimensions
            stats["rows_dim_date"] = rebuild_dim_date(conn)
            stats["rows_dim_geo"] = rebuild_dim_geo(conn)
            stats["rows_dim_seller"] = rebuild_dim_seller(conn)
            stats["rows_dim_object"] = rebuild_dim_object(conn)

            result["dimensions_created"] = 4

            # Facts
            stats["rows_fact_active"] = rebuild_fact_active(conn)
            stats["rows_fact_closed"] = rebuild_fact_closed(conn)

            result["facts_created"] = 2

            # Splits
            stats.update(rebuild_split_active(conn))
            stats.update(rebuild_split_closed(conn))

            splits_count = sum(1 for k in stats.keys() if
                               k.startswith("rows_fact_") and "_prodej_" in k or "_pronajem_" in k or "_drazba_" in k)
            result["splits_created"] = splits_count

            # Price events stub
            stats["rows_price_events"] = rebuild_price_change_events(conn)

            # Sanity checks
            _, sanity = run_sanity_checks(conn)
            stats.update({f"sanity_{k}": v for k, v in sanity.items()})

            result["stats"] = stats

    except SQLAlchemyError as e:
        result["status"] = "error"
        result["error"] = _pg_diag_payload(e)
        print(json.dumps(result, ensure_ascii=False))
        sys.exit(1)
    except Exception as e:
        result["status"] = "error"
        result["error"] = {"type": type(e).__name__, "message": str(e)[:500]}
        print(json.dumps(result, ensure_ascii=False))
        sys.exit(1)

    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()