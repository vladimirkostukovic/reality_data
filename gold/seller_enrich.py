from __future__ import annotations

import sys
import json
from pathlib import Path
from datetime import date, datetime
from decimal import Decimal

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# ================= CONFIG =================
PROJECT_ROOT = Path(__file__).resolve().parents[1]
CFG = json.loads((PROJECT_ROOT / "config.json").read_text(encoding="utf-8"))
DB_URL = f"postgresql+psycopg2://{CFG['USER']}:{CFG['PWD']}@{CFG['HOST']}:{CFG['PORT']}/{CFG['DB']}"

SCHEMA_SILVER = "silver"
SCHEMA_GOLD   = "gold"

SRC_TABLE     = "seller_info_silver"
TARGET_TABLE  = "totalized"

# ================= JSON utils =================
def _json_default(o):
    if isinstance(o, (datetime, pd.Timestamp)): return o.isoformat()
    if isinstance(o, date): return o.isoformat()
    if isinstance(o, (np.integer,)): return int(o)
    if isinstance(o, (np.floating,)): return float(o)
    if isinstance(o, (np.bool_, bool)): return bool(o)
    if isinstance(o, Decimal): return float(o)
    return str(o)

def _emit(stage: str, **payload):
    print(json.dumps({"stage": stage, **payload}, ensure_ascii=False, default=_json_default), flush=True)

def _pg_diag_payload(exc: Exception):
    d = {"type": type(exc).__name__}
    orig = getattr(exc, "orig", None)
    if orig is not None:
        d["pgcode"]  = getattr(orig, "pgcode", None)
        d["pgerror"] = getattr(orig, "pgerror", None)
        diag = getattr(orig, "diag", None)
        if diag:
            for k in ("schema_name","table_name","column_name","datatype_name",
                      "constraint_name","message_primary","message_detail","context"):
                v = getattr(diag, k, None)
                if v: d[k] = str(v)
    stmt = getattr(exc, "statement", None)
    if stmt: d["statement_preview"] = stmt[:300]
    return d

# ================= DDL  =================

DDL_DROP_OLD_TRIGGERS = f"""
DO $$
DECLARE
  v_relid oid;
BEGIN
  SELECT c.oid INTO v_relid
  FROM pg_class c
  JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE n.nspname = '{SCHEMA_GOLD}'
    AND c.relname = '{TARGET_TABLE}'
    AND c.relkind = 'r';

  IF v_relid IS NULL THEN
    RAISE EXCEPTION 'Table %.% not found. Create it before seller-enrich.', '{SCHEMA_GOLD}', '{TARGET_TABLE}';
  END IF;

  -- снять старый «полный запрет», если есть
  IF EXISTS (
    SELECT 1 FROM pg_trigger t
    WHERE t.tgname = 'trg_{TARGET_TABLE}_append_only'
      AND t.tgrelid = v_relid
  ) THEN
    EXECUTE format('DROP TRIGGER %I ON %I.%I', 'trg_{TARGET_TABLE}_append_only', '{SCHEMA_GOLD}', '{TARGET_TABLE}');
  END IF;

  -- снять старый whitelist, если остался
  IF EXISTS (
    SELECT 1 FROM pg_trigger t
    WHERE t.tgname = 'trg_{TARGET_TABLE}_append_only_whitelist'
      AND t.tgrelid = v_relid
  ) THEN
    EXECUTE format('DROP TRIGGER %I ON %I.%I', 'trg_{TARGET_TABLE}_append_only_whitelist', '{SCHEMA_GOLD}', '{TARGET_TABLE}');
  END IF;
END$$;
"""


DDL_CREATE_WHITELIST_FUNC = f"""
CREATE OR REPLACE FUNCTION {SCHEMA_GOLD}.allow_only_agent_updates() RETURNS trigger AS $FN$
BEGIN
  IF TG_OP = 'DELETE' THEN
    RAISE EXCEPTION 'Table {SCHEMA_GOLD}.{TARGET_TABLE} is append-only. DELETE is not allowed.';
  END IF;

  IF TG_OP = 'UPDATE' THEN
    IF (NEW.object_id       IS DISTINCT FROM OLD.object_id)       OR
       (NEW.internal_id     IS DISTINCT FROM OLD.internal_id)     OR
       (NEW.added_date      IS DISTINCT FROM OLD.added_date)      OR
       (NEW.available       IS DISTINCT FROM OLD.available)       OR
       (NEW.archived_date   IS DISTINCT FROM OLD.archived_date)   OR
       (NEW.source_id       IS DISTINCT FROM OLD.source_id)       OR
       (NEW.category_value  IS DISTINCT FROM OLD.category_value)  OR
       (NEW.category_name   IS DISTINCT FROM OLD.category_name)   OR
       (NEW.name            IS DISTINCT FROM OLD.name)            OR
       (NEW.deal_type       IS DISTINCT FROM OLD.deal_type)       OR
       (NEW.price           IS DISTINCT FROM OLD.price)           OR
       (NEW.rooms           IS DISTINCT FROM OLD.rooms)           OR
       (NEW.area_build      IS DISTINCT FROM OLD.area_build)      OR
       (NEW.longitude       IS DISTINCT FROM OLD.longitude)       OR
       (NEW.latitude        IS DISTINCT FROM OLD.latitude)        OR
       (NEW.norm_district   IS DISTINCT FROM OLD.norm_district)   OR
       (NEW.norm_city       IS DISTINCT FROM OLD.norm_city)       OR
       (NEW.norm_city_part  IS DISTINCT FROM OLD.norm_city_part)  OR
       (NEW.norm_street     IS DISTINCT FROM OLD.norm_street)     OR
       (NEW.norm_house_number IS DISTINCT FROM OLD.norm_house_number) OR
       (NEW.area_land       IS DISTINCT FROM OLD.area_land)       OR
       (NEW.norm_okres      IS DISTINCT FROM OLD.norm_okres)      OR
       (NEW.snapshot_at     IS DISTINCT FROM OLD.snapshot_at)
    THEN
       RAISE EXCEPTION 'Table {SCHEMA_GOLD}.{TARGET_TABLE} is append-only. Only agent_* fields may be updated.';
    END IF;

    NEW.updated_at := NOW();
    RETURN NEW;
  END IF;

  RETURN NEW;
END
$FN$ LANGUAGE plpgsql;
"""


DDL_CREATE_WHITELIST_TRIGGER = f"""
DO $$
DECLARE
  v_relid oid;
BEGIN
  SELECT c.oid INTO v_relid
  FROM pg_class c
  JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE n.nspname = '{SCHEMA_GOLD}'
    AND c.relname = '{TARGET_TABLE}'
    AND c.relkind = 'r';

  IF v_relid IS NULL THEN
    RAISE EXCEPTION 'Table %.% not found. Create it before seller-enrich.', '{SCHEMA_GOLD}', '{TARGET_TABLE}';
  END IF;

  EXECUTE 'CREATE TRIGGER trg_{TARGET_TABLE}_append_only_whitelist
           BEFORE UPDATE OR DELETE ON {SCHEMA_GOLD}.{TARGET_TABLE}
           FOR EACH ROW EXECUTE FUNCTION {SCHEMA_GOLD}.allow_only_agent_updates()';
END$$;
"""

# ================= Load & sanitize sellers =================
def load_seller_df(conn) -> pd.DataFrame:
    df = pd.read_sql(f"""
        SELECT
            internal_id::bigint AS internal_id,
            agent_name,
            agent_phone,
            agent_email
        FROM {SCHEMA_SILVER}.{SRC_TABLE}
        WHERE internal_id IS NOT NULL
    """, conn)


    for c in ("agent_name","agent_phone","agent_email"):
        if c not in df.columns:
            df[c] = None
        df[c] = df[c].astype(object).apply(
            lambda v: None if v is None or (isinstance(v, float) and pd.isna(v))
            else (lambda t: t if t != "" else None)(str(v).strip())
        )


    df["internal_id"] = pd.to_numeric(df["internal_id"], errors="coerce")
    df = df.dropna(subset=["internal_id"]).copy()
    df["internal_id"] = df["internal_id"].astype("int64")


    df = (
        df.sort_values(["internal_id"])
          .groupby("internal_id", as_index=False)
          .agg({"agent_name":"last","agent_phone":"last","agent_email":"last"})
    )

    _emit("seller_input_ready",
          rows_after_clean=int(len(df)),
          dtypes={k: str(v) for k,v in df.dtypes.items()})
    return df

# ================= Main =================
def main():
    engine = create_engine(DB_URL, pool_pre_ping=True)

    try:
        with engine.begin() as conn:
            conn.execute(text(DDL_DROP_OLD_TRIGGERS))
        _emit("ddl_drop_old_triggers_ok", ok=True)
    except SQLAlchemyError as e:
        _emit("error", ok=False, where="ddl_drop_triggers", diag=_pg_diag_payload(e))
        sys.exit(1)


    try:
        with engine.begin() as conn:
            conn.execute(text(DDL_CREATE_WHITELIST_FUNC))
        _emit("ddl_create_whitelist_func_ok", ok=True)
    except SQLAlchemyError as e:
        _emit("error", ok=False, where="ddl_create_whitelist_func", diag=_pg_diag_payload(e))
        sys.exit(1)


    try:
        with engine.begin() as conn:
            conn.execute(text(DDL_CREATE_WHITELIST_TRIGGER))
        _emit("ddl_create_whitelist_trigger_ok", ok=True)
    except SQLAlchemyError as e:
        _emit("error", ok=False, where="ddl_create_whitelist_trigger", diag=_pg_diag_payload(e))
        sys.exit(1)


    try:
        with engine.begin() as conn:
            sellers = load_seller_df(conn)
    except SQLAlchemyError as e:
        _emit("error", ok=False, where="load_sellers", diag=_pg_diag_payload(e))
        sys.exit(1)

    if sellers.empty:
        _emit("no-op", seen=0, note="seller_info_silver is empty")
        return


    try:
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS tmp_seller_enrich"))
            conn.execute(text("""
                CREATE TEMP TABLE tmp_seller_enrich (
                  internal_id BIGINT,
                  agent_name  TEXT,
                  agent_phone TEXT,
                  agent_email TEXT
                ) ON COMMIT DROP
            """))

            payload = sellers[["internal_id","agent_name","agent_phone","agent_email"]].copy()
            payload["internal_id"] = payload["internal_id"].astype("int64")

            _emit("tmp_load_preview",
                  cols=payload.columns.tolist(),
                  dtypes={k: str(v) for k,v in payload.dtypes.items()},
                  sample=payload.head(3).to_dict("records"))

            try:
                payload.to_sql("tmp_seller_enrich", con=conn, if_exists="append",
                               index=False, method="multi", chunksize=50_000)
            except Exception as e:
                _emit("tmp_load_multi_failed", error=str(e)[:400])
                payload.to_sql("tmp_seller_enrich", con=conn, if_exists="append",
                               index=False, method=None, chunksize=1000)

            cnt_tmp   = conn.execute(text("SELECT COUNT(*) FROM tmp_seller_enrich")).scalar_one()
            cnt_nullk = conn.execute(text("SELECT COUNT(*) FROM tmp_seller_enrich WHERE internal_id IS NULL")).scalar_one()
            if cnt_nullk:
                raise RuntimeError(f"tmp_seller_enrich has NULL keys: {cnt_nullk}/{cnt_tmp}")

            matched = conn.execute(text(f"""
                SELECT COUNT(*) FROM {SCHEMA_GOLD}.{TARGET_TABLE} t
                JOIN tmp_seller_enrich s USING (internal_id)
            """)).scalar_one()

            updated = conn.execute(text(f"""
                WITH src AS (
                  SELECT
                    s.internal_id,
                    NULLIF(s.agent_name,  '') AS agent_name,
                    NULLIF(s.agent_phone, '') AS agent_phone,
                    NULLIF(s.agent_email, '') AS agent_email
                  FROM tmp_seller_enrich s
                )
                UPDATE {SCHEMA_GOLD}.{TARGET_TABLE} AS t
                SET agent_name  = src.agent_name,
                    agent_phone = src.agent_phone,
                    agent_email = src.agent_email
                FROM src
                WHERE t.internal_id = src.internal_id
                  AND (
                       t.agent_name  IS DISTINCT FROM src.agent_name OR
                       t.agent_phone IS DISTINCT FROM src.agent_phone OR
                       t.agent_email IS DISTINCT FROM src.agent_email
                  );
            """)).rowcount
    except SQLAlchemyError as e:
        _emit("error", ok=False, where="update_totalized", diag=_pg_diag_payload(e))
        sys.exit(1)
    except Exception as e:
        _emit("error", ok=False, where="update_totalized", error=str(e)[:500])
        sys.exit(1)

    _emit("seller_enriched",
          seen=int(len(sellers)),
          matched_in_totalized=int(matched),
          updated_rows=int(updated))

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        _emit("error", ok=False, where="unhandled", error=str(e)[:500])
        sys.exit(1)
