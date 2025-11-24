#  Syncs real estate listings from silver.summarized_geo to gold.totalized:
#  Deduplicates using Union-Find on image_duplicate_clean pairs
#  Assigns stable object_id to each canonical group via object_id_map
#  Inserts only new internal_id records (append-only), filtering by valid Czech regions

from __future__ import annotations

import json
import sys
import time
from pathlib import Path
from typing import List
from datetime import date, datetime
from decimal import Decimal

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# ================= CONFIG =================
PROJECT_ROOT = Path(__file__).resolve().parents[1]
CFG = json.loads((PROJECT_ROOT / "config.json").read_text(encoding="utf-8"))
DB_URL = (
    f"postgresql+psycopg2://"
    f"{CFG['USER']}:{CFG['PWD']}@{CFG['HOST']}:{CFG['PORT']}/{CFG['DB']}"
)

SCHEMA_SILVER = "silver"
SCHEMA_GOLD = "gold"

T_SUMMARY = "summarized_geo"
T_SUBSET = "summarized_geo_subset"
T_DUP = "image_duplicate_clean"
T_OBJMAP = "object_id_map"
T_TARGET = "totalized"

OBJECT_ID_START = 800_000_000

VALID_DISTRICTS = {
    "Hlavní město Praha",
    "Jihočeský kraj",
    "Jihomoravský kraj",
    "Karlovarský kraj",
    "Královéhradecký kraj",
    "Liberecký kraj",
    "Moravskoslezský kraj",
    "Olomoucký kraj",
    "Pardubický kraj",
    "Plzeňský kraj",
    "Středočeský kraj",
    "Ústecký kraj",
    "Zlínský kraj",
    "Kraj Vysočina",
}

# ================= DDL =================
DDL = f"""
CREATE TABLE IF NOT EXISTS {SCHEMA_GOLD}.{T_OBJMAP} (
  canonical_internal_id BIGINT PRIMARY KEY,
  object_id             BIGINT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS {SCHEMA_GOLD}.{T_TARGET} (
  object_id         BIGINT      NOT NULL,
  internal_id       BIGINT      PRIMARY KEY,
  added_date        DATE,
  available         BOOLEAN,
  archived_date     DATE,
  source_id         TEXT,
  category_value    BIGINT,
  category_name     TEXT,
  name              TEXT,
  deal_type         TEXT,
  deal_type_value   INTEGER,
  price             DOUBLE PRECISION,
  rooms             TEXT,
  area_build        DOUBLE PRECISION,
  longitude         DOUBLE PRECISION,
  latitude          DOUBLE PRECISION,
  norm_district     TEXT,
  norm_city         TEXT,
  norm_city_part    TEXT,
  norm_street       TEXT,
  norm_house_number TEXT,
  area_land         DOUBLE PRECISION,
  norm_okres        TEXT,
  agent_name        TEXT,
  agent_phone       TEXT,
  agent_email       TEXT,
  snapshot_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE {SCHEMA_GOLD}.{T_TARGET} DROP COLUMN IF EXISTS record_hash;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = '{SCHEMA_GOLD}'
      AND table_name   = '{T_TARGET}'
      AND column_name  = 'deal_type_value'
  ) THEN
    ALTER TABLE {SCHEMA_GOLD}.{T_TARGET}
    ADD COLUMN deal_type_value INTEGER;
  END IF;
END$$;

CREATE INDEX IF NOT EXISTS idx_{T_TARGET}_object_id       ON {SCHEMA_GOLD}.{T_TARGET} (object_id);
CREATE INDEX IF NOT EXISTS idx_{T_TARGET}_added_date      ON {SCHEMA_GOLD}.{T_TARGET} (added_date);
CREATE INDEX IF NOT EXISTS idx_{T_TARGET}_archived        ON {SCHEMA_GOLD}.{T_TARGET} (archived_date);
CREATE INDEX IF NOT EXISTS idx_{T_TARGET}_deal_type       ON {SCHEMA_GOLD}.{T_TARGET} (deal_type);
CREATE INDEX IF NOT EXISTS idx_{T_TARGET}_deal_type_value ON {SCHEMA_GOLD}.{T_TARGET} (deal_type_value);

CREATE OR REPLACE FUNCTION {SCHEMA_GOLD}.prevent_mutations_{T_TARGET}() RETURNS trigger AS $$
BEGIN
  IF TG_OP = 'UPDATE' OR TG_OP = 'DELETE' THEN
    RAISE EXCEPTION 'Table {SCHEMA_GOLD}.{T_TARGET} is append-only. Operation % is not allowed.', TG_OP;
  END IF;
  RETURN NEW;
END
$$ LANGUAGE plpgsql;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_trigger WHERE tgname = 'trg_{T_TARGET}_append_only'
  ) THEN
    CREATE TRIGGER trg_{T_TARGET}_append_only
    BEFORE UPDATE OR DELETE ON {SCHEMA_GOLD}.{T_TARGET}
    FOR EACH ROW EXECUTE FUNCTION {SCHEMA_GOLD}.prevent_mutations_{T_TARGET}();
  END IF;
END$$;
"""


# ================= JSON utils =================
def _json_default(o):
    if isinstance(o, (datetime, pd.Timestamp)):
        return o.isoformat()
    if isinstance(o, date):
        return o.isoformat()
    if isinstance(o, (np.integer,)):
        return int(o)
    if isinstance(o, (np.floating,)):
        return float(o)
    if isinstance(o, (np.bool_,)):
        return bool(o)
    if isinstance(o, Decimal):
        return float(o)
    return str(o)


def _pg_diag_payload(exc: Exception):
    d = {"type": type(exc).__name__}
    orig = getattr(exc, "orig", None)
    if orig is not None:
        d["pgcode"] = getattr(orig, "pgcode", None)
        d["pgerror"] = getattr(orig, "pgerror", None)
        diag = getattr(orig, "diag", None)
        if diag:
            for k in (
                    "schema_name", "table_name", "column_name", "datatype_name",
                    "constraint_name", "message_primary", "message_detail", "context",
            ):
                v = getattr(diag, k, None)
                if v:
                    d[k] = str(v)
    stmt = getattr(exc, "statement", None)
    if stmt:
        d["statement_preview"] = stmt[:300]
    return d


# ================= Union-Find =================
class UF:
    __slots__ = ("p", "r")

    def __init__(self, nodes: List[int]):
        self.p = {int(n): int(n) for n in nodes}
        self.r = {int(n): 0 for n in nodes}

    def f(self, x: int) -> int:
        x = int(x)
        px = self.p[x]
        if px != x:
            self.p[x] = self.f(px)
        return self.p[x]

    def u(self, a: int, b: int):
        a = self.f(a)
        b = self.f(b)
        if a == b: return
        if self.r[a] < self.r[b]:
            a, b = b, a
        self.p[b] = a
        if self.r[a] == self.r[b]:
            self.r[a] += 1


# ================= MAIN FETCH =================
def latest_summary(conn) -> pd.DataFrame:
    cols = [
        "g.internal_id", "g.added_date", "g.available", "g.archived_date", "g.source_id",
        "g.category_value", "g.category_name", "g.name", "g.deal_type", "g.deal_type_value",
        "g.price", "g.rooms", "g.area_build", "g.longitude", "g.latitude", "g.area_land",
        "g.norm_house_number", "g.ingested_at"
    ]

    q = f"""
        SELECT DISTINCT ON (g.internal_id)
               {", ".join(cols)},
               s.norm_district,
               s.norm_city,
               s.norm_city_part,
               s.norm_street,
               s.norm_okres
        FROM {SCHEMA_SILVER}.{T_SUMMARY} g
        INNER JOIN {SCHEMA_SILVER}.{T_SUBSET} s
                ON s.internal_id = g.internal_id
               AND s.geo_ok IS TRUE
               AND s.not_true IS FALSE
               AND s.norm_district IS NOT NULL
               AND s.norm_city IS NOT NULL
        ORDER BY g.internal_id,
                 g.ingested_at DESC NULLS LAST,
                 g.archived_date DESC NULLS LAST,
                 g.added_date DESC NULLS LAST
    """

    df = pd.read_sql(q, conn)
    if "deal_type" in df.columns:
        df["deal_type"] = df["deal_type"].astype(str).str.strip().str.lower()
    return df


# ================= OTHER HELPERS =================

def groups_from_clean(conn, all_iids: pd.Series) -> pd.DataFrame:
    dup = pd.read_sql(
        f"SELECT iid1::bigint AS a, iid2::bigint AS b FROM {SCHEMA_SILVER}.{T_DUP}",
        conn,
    )

    ids = set(map(int, all_iids.astype("int64").tolist()))

    if dup.empty:
        return pd.DataFrame(
            {
                "internal_id": list(ids),
                "canonical_internal_id": list(ids),
            },
            dtype="int64",
        )

    dup = dup[(dup["a"].isin(ids)) & (dup["b"].isin(ids))].copy()

    nodes = sorted(set(dup["a"].tolist()) | set(dup["b"].tolist()) | ids)

    uf = UF(nodes)
    for a, b in dup.itertuples(index=False):
        uf.u(int(a), int(b))

    root = {n: uf.f(n) for n in nodes}
    comp = pd.DataFrame({"internal_id": nodes, "root": [root[n] for n in nodes]})

    comp_sorted = comp.sort_values(["root", "internal_id"])
    leaders = (
        comp_sorted.groupby("root", as_index=False)
        .first()
        .rename(columns={"internal_id": "canonical_internal_id"})
    )

    out = (
        comp.merge(leaders, on="root", how="left")[["internal_id", "canonical_internal_id"]]
        .astype("int64")
    )
    return out


def load_objmap(conn) -> pd.DataFrame:
    try:
        m = pd.read_sql(
            f"""
            SELECT canonical_internal_id::bigint AS canonical_internal_id,
                   object_id::bigint             AS object_id
            FROM {SCHEMA_GOLD}.{T_OBJMAP}
            """,
            conn,
        )
    except Exception:
        m = pd.DataFrame(columns=["canonical_internal_id", "object_id"], dtype="int64")
    return m


def upsert_new_objmap(conn, need_ids: List[int], existing: pd.DataFrame) -> pd.DataFrame:
    if not need_ids:
        return existing

    next_start = (
        int(existing["object_id"].max()) + 1
        if not existing.empty
        else OBJECT_ID_START
    )

    to_assign = pd.DataFrame(
        {
            "canonical_internal_id": need_ids,
            "object_id": list(range(next_start, next_start + len(need_ids))),
        },
        dtype="int64",
    )

    conn.execute(text("DROP TABLE IF EXISTS tmp_new_objmap"))
    conn.execute(
        text(
            "CREATE TEMP TABLE tmp_new_objmap (canonical_internal_id BIGINT, object_id BIGINT) ON COMMIT DROP"
        )
    )

    to_assign.to_sql(
        "tmp_new_objmap",
        con=conn,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=100_000,
    )

    conn.execute(
        text(
            f"""
            INSERT INTO {SCHEMA_GOLD}.{T_OBJMAP} (canonical_internal_id, object_id)
            SELECT t.canonical_internal_id, t.object_id
            FROM tmp_new_objmap t
            LEFT JOIN {SCHEMA_GOLD}.{T_OBJMAP} m USING (canonical_internal_id)
            WHERE m.canonical_internal_id IS NULL
            """
        )
    )

    return load_objmap(conn)


# ================= Main =================
def main():
    result = {
        "status": "success",
        "rows_fetched": 0,
        "rows_filtered": 0,
        "new_object_ids": 0,
        "rows_inserted": 0,
        "timestamp": datetime.now().isoformat()
    }

    engine = create_engine(DB_URL, pool_pre_ping=True)

    try:
        # DDL
        with engine.begin() as conn:
            conn.execute(text(DDL))

        # Read and process data
        with engine.begin() as conn:
            facts = latest_summary(conn)
            result["rows_fetched"] = len(facts)

            # Filter by valid districts
            before_cut = len(facts)
            facts = facts[facts["norm_district"].isin(VALID_DISTRICTS)].copy()
            after_cut = len(facts)
            result["rows_filtered"] = after_cut

            if facts.empty:
                result["status"] = "no_data"
                print(json.dumps(result, ensure_ascii=False, default=_json_default))
                return

            iids = facts["internal_id"].astype("int64")

            # Build groups
            groups = groups_from_clean(conn, iids)

            # Load object map
            objmap = load_objmap(conn)
            known = (
                set(objmap["canonical_internal_id"].astype("int64"))
                if not objmap.empty
                else set()
            )
            need = sorted(
                set(groups["canonical_internal_id"].astype("int64")) - known
            )
            result["new_object_ids"] = len(need)

        # Upsert new object IDs if needed
        if need:
            with engine.begin() as conn:
                objmap = upsert_new_objmap(conn, need, objmap)

        # Prepare and insert data
        with engine.begin() as conn:
            objmap = load_objmap(conn)

            to_ins = (
                facts.merge(groups, on="internal_id", how="left")
                .merge(objmap, on="canonical_internal_id", how="left")
            )

            to_ins["object_id"] = (
                to_ins["object_id"]
                .fillna(to_ins["internal_id"])
                .astype("int64")
            )

            # Filter out existing records
            try:
                existing = pd.read_sql(
                    f"""
                    SELECT internal_id::bigint AS internal_id
                    FROM {SCHEMA_GOLD}.{T_TARGET}
                    """,
                    conn,
                )
                have = set(existing["internal_id"].astype("int64").tolist())
            except Exception:
                have = set()

            to_ins = to_ins[
                ~to_ins["internal_id"].astype("int64").isin(have)
            ].copy()

            if to_ins.empty:
                result["status"] = "no_new_data"
                print(json.dumps(result, ensure_ascii=False, default=_json_default))
                return

        # Prepare columns
        for c in ["agent_name", "agent_phone", "agent_email"]:
            if c not in to_ins.columns:
                to_ins[c] = None

        out = to_ins.reindex(
            columns=[
                "object_id",
                "internal_id",
                "added_date",
                "available",
                "archived_date",
                "source_id",
                "category_value",
                "category_name",
                "name",
                "deal_type",
                "deal_type_value",
                "price",
                "rooms",
                "area_build",
                "longitude",
                "latitude",
                "norm_district",
                "norm_city",
                "norm_city_part",
                "norm_street",
                "norm_house_number",
                "area_land",
                "norm_okres",
                "agent_name",
                "agent_phone",
                "agent_email",
            ]
        ).copy()

        # Type conversions
        int_cols = ["object_id", "internal_id", "category_value", "deal_type_value"]
        for c in int_cols:
            out[c] = (
                pd.to_numeric(out[c], errors="coerce")
                .apply(lambda v: None if pd.isna(v) else int(v))
            )

        float_cols = ["price", "area_build", "longitude", "latitude", "area_land"]
        for c in float_cols:
            out[c] = (
                pd.to_numeric(out[c], errors="coerce")
                .apply(lambda v: None if pd.isna(v) else float(v))
            )

        def _to_bool(v):
            if v is None or (isinstance(v, float) and pd.isna(v)):
                return None
            if isinstance(v, (bool, np.bool_)):
                return bool(v)
            s = str(v).strip().lower()
            if s in ("1", "true", "t", "yes", "y"):
                return True
            if s in ("0", "false", "f", "no", "n", ""):
                return False
            return None

        out["available"] = out["available"].apply(_to_bool)

        for c in ["added_date", "archived_date"]:
            dt = pd.to_datetime(out[c], errors="coerce").dt.date
            out[c] = dt.where(~pd.isna(dt), None)

        TEXT_COLS = [
            "source_id",
            "category_name",
            "name",
            "deal_type",
            "rooms",
            "norm_district",
            "norm_city",
            "norm_city_part",
            "norm_street",
            "norm_house_number",
            "norm_okres",
            "agent_name",
            "agent_phone",
            "agent_email",
        ]
        for c in TEXT_COLS:
            s = out[c]
            out[c] = s.apply(
                lambda v: None
                if (v is None or (isinstance(v, float) and pd.isna(v)))
                else (str(v).strip() or None)
            )

        # Insert
        out.to_sql(
            T_TARGET,
            con=engine,
            schema=SCHEMA_GOLD,
            if_exists='append',
            index=False,
            method='multi',
            chunksize=10000
        )

        result["rows_inserted"] = len(out)

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