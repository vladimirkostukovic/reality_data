# Loads silver.price_change, maps it to summarized_geo to resolve internal_id,
# enforces append-only gold.price_change table with DDL and trigger guards,
# inserts only new (internal_id, change_date, new_price) records and emits strict JSON sync_summary.

from __future__ import annotations

import sys
import json
from pathlib import Path
from datetime import datetime, date
from decimal import Decimal

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# ============== CONFIG ==============
PROJECT_ROOT = Path(__file__).resolve().parents[1]
CFG = json.loads((PROJECT_ROOT / "config.json").read_text(encoding="utf-8"))
DB_URL = f"postgresql+psycopg2://{CFG['USER']}:{CFG['PWD']}@{CFG['HOST']}:{CFG['PORT']}/{CFG['DB']}"

SCHEMA_SILVER = "silver"
SCHEMA_GOLD = "gold"

SRC_TABLE = "price_change"
SUMM_TABLE = "summarized_geo"
TGT_TABLE = "price_change"

SRC_ID_COL = "id"
SRC_SRCID_COL = "source_id"
SUMM_SITE_COL = "site_id"
SUMM_SRCID_COL = "source_id"
SUMM_IID_COL = "internal_id"
SUMM_TS_COL = "ingested_at"


# ============== JSON helpers ==============
def _json_default(o):
    if isinstance(o, (datetime, pd.Timestamp)): return o.isoformat()
    if isinstance(o, date): return o.isoformat()
    if isinstance(o, (np.integer,)): return int(o)
    if isinstance(o, (np.floating,)): return float(o)
    if isinstance(o, (np.bool_, bool)): return bool(o)
    if isinstance(o, Decimal): return float(o)
    return str(o)


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
                if v: d[k] = str(v)
    stmt = getattr(exc, "statement", None)
    if stmt: d["statement_preview"] = stmt[:300]
    return d


# ============== DDL ==============
DDL_TABLE = f"""
CREATE TABLE IF NOT EXISTS {SCHEMA_GOLD}.{TGT_TABLE} (
  internal_id  BIGINT        NOT NULL,
  old_price    NUMERIC(12,2),
  new_price    NUMERIC(12,2) NOT NULL,
  change_date  DATE          NOT NULL,
  source_id    INT,
  ingested_at  TIMESTAMPTZ,
  snapshot_at  TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
  updated_at   TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
  CONSTRAINT pk_{TGT_TABLE} PRIMARY KEY (internal_id, change_date, new_price)
);

CREATE INDEX IF NOT EXISTS idx_{TGT_TABLE}_internal_id ON {SCHEMA_GOLD}.{TGT_TABLE} (internal_id);
CREATE INDEX IF NOT EXISTS idx_{TGT_TABLE}_change_date ON {SCHEMA_GOLD}.{TGT_TABLE} (change_date);
"""

DDL_ENSURE_APPEND_ONLY = f"""
CREATE OR REPLACE FUNCTION {SCHEMA_GOLD}.prevent_mutations_{TGT_TABLE}() RETURNS trigger AS $$
BEGIN
  IF TG_OP = 'UPDATE' OR TG_OP = 'DELETE' THEN
    RAISE EXCEPTION 'Table {SCHEMA_GOLD}.{TGT_TABLE} is append-only. Operation % is not allowed.', TG_OP;
  END IF;
  RETURN NEW;
END
$$ LANGUAGE plpgsql;

DO $$
DECLARE
  v_relid oid;
  v_trg_exists boolean;
BEGIN
  SELECT c.oid INTO v_relid
  FROM pg_class c
  JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE n.nspname = '{SCHEMA_GOLD}'
    AND c.relname = '{TGT_TABLE}'
    AND c.relkind = 'r';

  IF v_relid IS NULL THEN
    RAISE EXCEPTION 'Table %.% not found.', '{SCHEMA_GOLD}', '{TGT_TABLE}';
  END IF;

  SELECT EXISTS (
    SELECT 1 FROM pg_trigger t
    WHERE t.tgname = 'trg_{TGT_TABLE}_append_only'
      AND t.tgrelid = v_relid
  ) INTO v_trg_exists;

  IF NOT v_trg_exists THEN
    EXECUTE 'CREATE TRIGGER trg_{TGT_TABLE}_append_only
             BEFORE UPDATE OR DELETE ON {SCHEMA_GOLD}.{TGT_TABLE}
             FOR EACH ROW EXECUTE FUNCTION {SCHEMA_GOLD}.prevent_mutations_{TGT_TABLE}()';
  END IF;
END$$;
"""

# ============== SQL templates ==============
SQL_COUNT_SRC = f"""
SELECT COUNT(*)::bigint
FROM {SCHEMA_SILVER}.{SRC_TABLE}
WHERE {SRC_ID_COL} IS NOT NULL;
"""

MAP_CTE = f"""
WITH map AS (
  SELECT DISTINCT ON (s.{SUMM_SRCID_COL}, s.{SUMM_SITE_COL})
         s.{SUMM_SRCID_COL}::int    AS source_id,
         s.{SUMM_SITE_COL}::text    AS site_id_text,
         s.{SUMM_IID_COL}::bigint   AS internal_id
  FROM {SCHEMA_SILVER}.{SUMM_TABLE} s
  WHERE s.{SUMM_SITE_COL} IS NOT NULL
  ORDER BY s.{SUMM_SRCID_COL}, s.{SUMM_SITE_COL}, s.{SUMM_TS_COL} DESC NULLS LAST
)
"""

SQL_COUNT_MATCH = MAP_CTE + f"""
SELECT COUNT(*)::bigint
FROM {SCHEMA_SILVER}.{SRC_TABLE} pc
JOIN map m
  ON m.source_id = pc.{SRC_SRCID_COL}
 AND m.site_id_text = pc.{SRC_ID_COL}::text
WHERE pc.{SRC_ID_COL} IS NOT NULL;
"""

SQL_INSERT = MAP_CTE + f"""
INSERT INTO {SCHEMA_GOLD}.{TGT_TABLE}
  (internal_id, old_price, new_price, change_date, source_id, ingested_at)
SELECT
  m.internal_id,
  pc.old_price,
  pc.new_price,
  pc.change_date,
  pc.{SRC_SRCID_COL},
  pc.ingested_at
FROM {SCHEMA_SILVER}.{SRC_TABLE} pc
JOIN map m
  ON m.source_id = pc.{SRC_SRCID_COL}
 AND m.site_id_text = pc.{SRC_ID_COL}::text
WHERE pc.{SRC_ID_COL} IS NOT NULL
  AND pc.new_price IS NOT NULL
  AND pc.change_date IS NOT NULL
ON CONFLICT DO NOTHING;
"""


# ============== MAIN ==============
def main():
    result = {
        "status": "success",
        "source_rows": 0,
        "rows_matched": 0,
        "rows_inserted": 0,
        "timestamp": datetime.now().isoformat()
    }

    engine = create_engine(DB_URL, pool_pre_ping=True)

    try:
        # DDL operations
        with engine.begin() as conn:
            conn.execute(text(DDL_TABLE))
            conn.execute(text(DDL_ENSURE_APPEND_ONLY))

        # Get stats and insert
        with engine.begin() as conn:
            total_src = conn.execute(text(SQL_COUNT_SRC)).scalar_one()
            total_match = conn.execute(text(SQL_COUNT_MATCH)).scalar_one()

            result["source_rows"] = int(total_src)
            result["rows_matched"] = int(total_match)

            res = conn.execute(text(SQL_INSERT))
            inserted = 0 if res.rowcount in (None, -1) else int(res.rowcount)
            result["rows_inserted"] = inserted

    except SQLAlchemyError as e:
        result["status"] = "error"
        result["error"] = _pg_diag_payload(e)
        print(json.dumps(result, ensure_ascii=False, default=_json_default))
        sys.exit(1)
    except Exception as e:
        result["status"] = "error"
        result["error"] = {"type": type(e).__name__, "message": str(e)[:500]}
        print(json.dumps(result, ensure_ascii=False, default=_json_default))
        sys.exit(1)

    print(json.dumps(result, ensure_ascii=False, default=_json_default))


if __name__ == "__main__":
    main()