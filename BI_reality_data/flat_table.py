
from __future__ import annotations
import json
import logging
import time
from pathlib import Path
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# ---------- LOGGING ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | reality_flat | %(message)s"
)
log = logging.getLogger("reality_flat")

# ---------- CONFIG ----------
PROJECT_ROOT = Path(__file__).resolve().parents[1]
cfg = json.loads((PROJECT_ROOT / "config.json").read_text(encoding="utf-8"))

db_user = cfg.get("user") or cfg.get("USER")
db_pwd  = cfg.get("password") or cfg.get("PWD")
db_host = cfg.get("host") or cfg.get("HOST")
db_port = cfg.get("port") or cfg.get("PORT")
db_name = cfg.get("dbname") or cfg.get("DB")

DB_URL = f"postgresql+psycopg2://{db_user}:{db_pwd}@{db_host}:{db_port}/{db_name}"
engine  = create_engine(DB_URL, pool_pre_ping=True, connect_args={"connect_timeout": 10})

TARGET_SCHEMA = "bi_reality_data"
TARGET_TABLE  = "reality_flat"
STAGE_TABLE   = TARGET_TABLE + "__stage"

BASE_SQL = f"""
WITH filtered AS (
    -- валидные квартиры за последние 180 дней
    SELECT *
    FROM gold.totalized t
    WHERE
        t.added_date IS NOT NULL
        AND t.archived_date IS NOT NULL
        AND t.archived_date >= CURRENT_DATE - INTERVAL '180 days'
        AND t.category_value = 1                 -- только квартиры
        AND t.area_build IS NOT NULL
        AND t.area_build BETWEEN 10 AND 200      -- адекватная площадь
        AND t.price IS NOT NULL
        AND t.price > 0                          -- адекватная цена
),
normed AS (
    SELECT
        f.*,
        lower(trim(f.rooms::text))                                                   AS rooms_raw,
        NULLIF(substring(lower(trim(f.rooms::text)) from '([0-9]+)'), '')::int       AS rooms_num,
        (f.archived_date::date - f.added_date::date) AS deal_duration_days,
        CASE 
            WHEN f.area_build BETWEEN 10 AND 200
             AND f.price IS NOT NULL 
             AND f.price > 0
            THEN round((f.price::numeric / NULLIF(f.area_build::numeric,0))::numeric, 0)
            ELSE NULL
        END AS price_per_m2,

        CASE 
            WHEN f.price < 100000
                THEN round(f.price::numeric, -3)   -- шаг 1k
            ELSE
                round(f.price::numeric, -4)        -- шаг 10k
        END AS price_rounded,

        date_trunc('month', f.archived_date)::date AS archived_month,
        date_trunc('week',  f.archived_date)::date AS archived_week,

        CASE 
            WHEN NULLIF(substring(lower(trim(f.rooms::text)) from '([0-9]+)'), '')::int IS NULL
                THEN 'Atypický'
            WHEN NULLIF(substring(lower(trim(f.rooms::text)) from '([0-9]+)'), '')::int <= 1
                THEN '1kk'
            WHEN NULLIF(substring(lower(trim(f.rooms::text)) from '([0-9]+)'), '')::int = 2
                THEN '2kk'
            WHEN NULLIF(substring(lower(trim(f.rooms::text)) from '([0-9]+)'), '')::int = 3
                THEN '3kk'
            WHEN NULLIF(substring(lower(trim(f.rooms::text)) from '([0-9]+)'), '')::int = 4
                THEN '4kk'
            ELSE '5kk+'
        END AS rooms_bucket,

        CASE 
            WHEN f.area_build BETWEEN 10 AND 200 THEN
                CASE 
                    WHEN f.area_build < 30  THEN '<30'
                    WHEN f.area_build < 40  THEN '30-39'
                    WHEN f.area_build < 50  THEN '40-49'
                    WHEN f.area_build < 60  THEN '50-59'
                    WHEN f.area_build < 70  THEN '60-69'
                    WHEN f.area_build < 90  THEN '70-89'
                    WHEN f.area_build < 100 THEN '90-99'
                    WHEN f.area_build < 120 THEN '100-119'
                    WHEN f.area_build < 150 THEN '120-149'
                    ELSE '150-200'
                END
            ELSE 'excluded'
        END AS area_bucket,

        CASE 
            WHEN f.area_build BETWEEN 10 AND 200
            THEN round(f.area_build::numeric, -1)
            ELSE NULL
        END AS area_build_bin
    FROM filtered f
),
shares AS (
    SELECT 
        lower(trim(n.deal_type))          AS deal_type_norm,
        n.norm_district                   AS region_norm,
        n.rooms_bucket,
        COUNT(*) AS deals_in_bucket,
        SUM(COUNT(*)) OVER (
            PARTITION BY lower(trim(n.deal_type)), n.norm_district
        ) AS total_region_deals,
        ROUND(
          100.0
          * COUNT(*)::numeric 
          / NULLIF(SUM(COUNT(*)) OVER (
              PARTITION BY lower(trim(n.deal_type)), n.norm_district
            ), 0),
          2
        ) AS share_of_market_pct
    FROM normed n
    GROUP BY lower(trim(n.deal_type)), n.norm_district, n.rooms_bucket
),
speed AS (
    -- средняя скорость сделки (в днях) по типу комнаты / региону / типу сделки
    SELECT
        lower(trim(n.deal_type))            AS deal_type_norm,
        n.norm_district                     AS region_norm,
        n.rooms_bucket,
        AVG(n.deal_duration_days)::numeric  AS avg_deal_duration_days_bucket
    FROM normed n
    WHERE n.deal_duration_days IS NOT NULL
    GROUP BY lower(trim(n.deal_type)), n.norm_district, n.rooms_bucket
)
SELECT
    n.*,  -- все поля из totalized плюс наши рассчитанные колонки

    -- сколько всего сделок и какая доля
    s.total_region_deals,
    s.share_of_market_pct,

    -- типичная скорость закрытия для такого класса квартиры в регионе
    sp.avg_deal_duration_days_bucket

FROM normed n
LEFT JOIN shares s
  ON  s.deal_type_norm = lower(trim(n.deal_type))
  AND s.region_norm     = n.norm_district
  AND s.rooms_bucket    = n.rooms_bucket
LEFT JOIN speed sp
  ON  sp.deal_type_norm = lower(trim(n.deal_type))
  AND sp.region_norm     = n.norm_district
  AND sp.rooms_bucket    = n.rooms_bucket
"""

def fq(schema: str, table: str) -> str:
    return f'"{schema}"."{table}"'

def schema_exists(conn, schema: str) -> bool:
    return bool(conn.execute(
        text("SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = :s)"),
        {"s": schema}
    ).scalar())

def build_stage(conn):
    log.info("build stage table (gold.totalized -> flat)")
    conn.execute(text(f'DROP TABLE IF EXISTS {fq(TARGET_SCHEMA, STAGE_TABLE)}'))
    conn.execute(text(f"""
        CREATE TABLE {fq(TARGET_SCHEMA, STAGE_TABLE)} AS
        {BASE_SQL}
    """))

def finalize(conn):
    log.info("swap stage -> final")
    conn.execute(text(f'DROP TABLE IF EXISTS {fq(TARGET_SCHEMA, TARGET_TABLE)}'))
    conn.execute(text(f'ALTER TABLE {fq(TARGET_SCHEMA, STAGE_TABLE)} RENAME TO "{TARGET_TABLE}"'))

    #-- индексы под фильтры и слайсеры
    conn.execute(text(f'CREATE INDEX IF NOT EXISTS {TARGET_TABLE}_deal_type_idx        ON {fq(TARGET_SCHEMA, TARGET_TABLE)} (deal_type)'))
    conn.execute(text(f'CREATE INDEX IF NOT EXISTS {TARGET_TABLE}_region_idx           ON {fq(TARGET_SCHEMA, TARGET_TABLE)} (norm_district)'))
    conn.execute(text(f'CREATE INDEX IF NOT EXISTS {TARGET_TABLE}_rooms_bucket_idx     ON {fq(TARGET_SCHEMA, TARGET_TABLE)} (rooms_bucket)'))
    conn.execute(text(f'CREATE INDEX IF NOT EXISTS {TARGET_TABLE}_archived_month_idx   ON {fq(TARGET_SCHEMA, TARGET_TABLE)} (archived_month)'))
    conn.execute(text(f'CREATE INDEX IF NOT EXISTS {TARGET_TABLE}_price_rounded_idx    ON {fq(TARGET_SCHEMA, TARGET_TABLE)} (price_rounded)'))
    conn.execute(text(f'CREATE INDEX IF NOT EXISTS {TARGET_TABLE}_area_build_bin_idx   ON {fq(TARGET_SCHEMA, TARGET_TABLE)} (area_build_bin)'))
    conn.execute(text(f'ANALYZE {fq(TARGET_SCHEMA, TARGET_TABLE)}'))

def main():
    t0 = time.perf_counter()
    try:
        with engine.begin() as conn:
            if not schema_exists(conn, TARGET_SCHEMA):
                raise RuntimeError(f'schema {TARGET_SCHEMA} not found')

            build_stage(conn)
            finalize(conn)

            rows_cnt = conn.execute(
                text(f"SELECT COUNT(*) FROM {fq(TARGET_SCHEMA, TARGET_TABLE)}")
            ).scalar()

            elapsed = round(time.perf_counter() - t0, 3)
            print(json.dumps({
                "ok": True,
                "table": f"{TARGET_SCHEMA}.{TARGET_TABLE}",
                "rows": int(rows_cnt or 0),
                "elapsed_s": elapsed
            }, ensure_ascii=False))

    except SQLAlchemyError as e:
        log.exception("db error")
        print(json.dumps({"ok": False, "error": str(e)}, ensure_ascii=False))
        raise
    except Exception as e:
        log.exception("unexpected")
        print(json.dumps({"ok": False, "error": str(e)}, ensure_ascii=False))
        raise

if __name__ == "__main__":
    main()