#Builds canonical clusters of duplicate property listings detected by image similarity.
#Maps each internal_id to a canonical root using Union-Find, aggregates cluster stats,
#and produces two tables: internal_id → canonical_internal_id and cluster-level metadata.
#Used as the primary consolidation layer before geo- and attribute-based deduplication.

import json
import re
from pathlib import Path
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text, String
from sqlalchemy.dialects.postgresql import ARRAY, BIGINT
import time

# ===================== Config =====================
PROJECT_ROOT = Path(__file__).resolve().parents[2]
CFG = json.loads((PROJECT_ROOT / "config.json").read_text(encoding="utf-8"))
DB_URL = f"postgresql+psycopg2://{CFG['USER']}:{CFG['PWD']}@{CFG['HOST']}:{CFG['PORT']}/{CFG['DB']}"

SCHEMA_SILVER = "silver"
TABLE_IMAGE_DUP = "image_duplicate"
TABLE_SUMMARY   = "summarized_geo"

TABLE_MAP       = "internal_id_canon_map"
TABLE_CANON     = "image_duplicate_canonical"

TMP_MAP   = "tmp_canon_map"
TMP_CANON = "tmp_canon_agg"

# ===================== DDL =============================
DDL_TARGETS = f"""
CREATE SCHEMA IF NOT EXISTS {SCHEMA_SILVER};

CREATE TABLE IF NOT EXISTS {SCHEMA_SILVER}.{TABLE_MAP} (
  internal_id BIGINT PRIMARY KEY,
  canonical_internal_id BIGINT NOT NULL,
  iid_added_min TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS {SCHEMA_SILVER}.{TABLE_CANON} (
  canonical_internal_id BIGINT PRIMARY KEY,
  members_count INT,
  member_internal_ids BIGINT[],
  site_ids TEXT[],
  first_seen TIMESTAMPTZ,
  last_seen  TIMESTAMPTZ,
  max_percentage NUMERIC,
  edges_count INT  
);
"""

# ===================== Merge SQL =========================
MERGE_MAP = f"""
INSERT INTO {SCHEMA_SILVER}.{TABLE_MAP} AS t
  (internal_id, canonical_internal_id, iid_added_min)
SELECT internal_id, canonical_internal_id, iid_added_min
FROM {TMP_MAP}
ON CONFLICT (internal_id) DO UPDATE
SET canonical_internal_id = EXCLUDED.canonical_internal_id,
    iid_added_min         = LEAST(t.iid_added_min, EXCLUDED.iid_added_min);
"""

MERGE_CANON = f"""
INSERT INTO {SCHEMA_SILVER}.{TABLE_CANON} AS t
  (canonical_internal_id, members_count, member_internal_ids, site_ids,
   first_seen, last_seen, max_percentage, edges_count)
SELECT canonical_internal_id, members_count, member_internal_ids, site_ids,
       first_seen, last_seen, max_percentage, edges_count
FROM {TMP_CANON}
ON CONFLICT (canonical_internal_id) DO UPDATE
SET members_count       = EXCLUDED.members_count,
    member_internal_ids = EXCLUDED.member_internal_ids,
    site_ids            = EXCLUDED.site_ids,
    first_seen          = LEAST(t.first_seen, EXCLUDED.first_seen),
    last_seen           = GREATEST(t.last_seen, EXCLUDED.last_seen),
    max_percentage      = GREATEST(t.max_percentage, EXCLUDED.max_percentage),
    edges_count         = t.edges_count + EXCLUDED.edges_count;
"""

# ===================== utils =========================
def norm_site(x: str) -> str:
    if x is None:
        return None
    s = str(x).strip().lower()
    s = re.sub(r'^(sr:|sreality:|idnes:|bez:?|avizo:)\s*', '', s)
    s = re.sub(r'\s+', ' ', s)
    return s

class UnionFind:
    __slots__ = ("parent", "rank")
    def __init__(self, nodes):
        self.parent = {int(x): int(x) for x in nodes}
        self.rank   = {int(x): 0 for x in nodes}
    def find(self, x):
        x = int(x)
        p = self.parent[x]
        if p != x:
            self.parent[x] = self.find(p)
        return self.parent[x]
    def union(self, a, b):
        a, b = self.find(a), self.find(b)
        if a == b: return
        if self.rank[a] < self.rank[b]:
            a, b = b, a
        self.parent[b] = a
        if self.rank[a] == self.rank[b]:
            self.rank[a] += 1

# ========================= MAIN =========================
def main():
    t0 = time.time()
    engine = create_engine(DB_URL, pool_pre_ping=True)

    try:
        with engine.begin() as conn:
            conn.execute(text(DDL_TARGETS))

            # 1) read duplicates
            dup_sql = f"""
                SELECT site_id1::text AS site_id1,
                       site_id2::text AS site_id2,
                       percentage,
                       date
                FROM {SCHEMA_SILVER}.{TABLE_IMAGE_DUP}
                WHERE site_id1 IS NOT NULL AND site_id2 IS NOT NULL
            """
            dup = pd.read_sql(dup_sql, conn)

            # 2) read geo
            geo_sql = f"""
                SELECT DISTINCT ON (site_id)
                       site_id::text AS site_id,
                       internal_id,
                       added_date
                FROM {SCHEMA_SILVER}.{TABLE_SUMMARY}
                ORDER BY site_id, added_date ASC NULLS LAST
            """
            geo = pd.read_sql(geo_sql, conn)

            if "added_date" not in geo.columns or geo["added_date"].isna().all():
                dc = [c for c in geo.columns if c.lower() == "date"]
                if dc:
                    geo["added_date"] = geo[dc[0]]
                else:
                    raise RuntimeError("summarized_geo missing added_date/date")

            # normalize
            dup["site_id1"] = dup["site_id1"].astype(str).map(norm_site)
            dup["site_id2"] = dup["site_id2"].astype(str).map(norm_site)
            geo["site_id"]  = geo["site_id"].astype(str).map(norm_site)

            # filter pairs
            geo_ids = set(geo["site_id"].values)
            pairs_total = len(dup)
            dup = dup[dup["site_id1"].isin(geo_ids) & dup["site_id2"].isin(geo_ids)].copy()
            pairs_kept = len(dup)

            # join internal ids
            dup = dup.merge(
                geo.rename(columns={"site_id":"site_id1","internal_id":"iid1"}),
                on="site_id1", how="left"
            ).merge(
                geo.rename(columns={"site_id":"site_id2","internal_id":"iid2"}),
                on="site_id2", how="left"
            )
            miss1 = int(dup["iid1"].isna().sum())
            miss2 = int(dup["iid2"].isna().sum())
            dup = dup.dropna(subset=["iid1","iid2"]).copy()
            dup["iid1"] = dup["iid1"].astype("int64")
            dup["iid2"] = dup["iid2"].astype("int64")

            # union-find clusters
            nodes = pd.Index(pd.unique(pd.concat([dup["iid1"], dup["iid2"]], ignore_index=True))).astype("int64")
            uf = UnionFind(nodes)
            for a, b in dup[["iid1","iid2"]].itertuples(index=False):
                uf.union(a, b)

            comp_root = {n: uf.find(n) for n in nodes}
            comp_df = pd.DataFrame({"internal_id": nodes.values,
                                    "comp_root": [comp_root[int(x)] for x in nodes.values]})

            iid_dates = (geo.dropna(subset=["internal_id"])
                           .groupby("internal_id", as_index=False)["added_date"]
                           .min()
                           .rename(columns={"added_date":"iid_added_min"}))

            comp_df = comp_df.merge(iid_dates, on="internal_id", how="left")
            comp_df["ts"] = pd.to_datetime(comp_df["iid_added_min"], errors="coerce")

            canon = (comp_df
                     .sort_values(["comp_root","ts","internal_id"], na_position="last")
                     .groupby("comp_root", as_index=False)
                     .first()[["comp_root","internal_id"]]
                     .rename(columns={"internal_id":"canonical_internal_id"}))

            map_df = comp_df.merge(canon, on="comp_root", how="left")[["internal_id",
                                                                       "canonical_internal_id",
                                                                       "iid_added_min"]]

            # cluster edge stats
            dup["cid1"] = dup["iid1"].map(dict(zip(map_df.internal_id, map_df.canonical_internal_id))).astype("int64")
            dup["cid2"] = dup["iid2"].map(dict(zip(map_df.internal_id, map_df.canonical_internal_id))).astype("int64")

            cluster_edges = (
                dup.assign(canonical_internal_id=np.where(dup["cid1"] <= dup["cid2"], dup["cid1"], dup["cid2"]))
                  .groupby("canonical_internal_id")
                  .agg(first_seen=("date","min"),
                       last_seen=("date","max"),
                       max_percentage=("percentage","max"),
                       edges_count=("percentage","size"),
                       site_ids=("site_id1", lambda s: list(s.astype(str))))
                  .reset_index()
            )

            s2 = (dup.groupby(np.where(dup["cid1"] <= dup["cid2"], dup["cid1"], dup["cid2"]))
                    .agg(site_ids2=("site_id2", lambda s: list(s.astype(str)))))
            s2.index.name = "canonical_internal_id"
            s2 = s2.reset_index()

            cluster_edges = cluster_edges.merge(s2, on="canonical_internal_id", how="left")
            cluster_edges["site_ids"] = cluster_edges.apply(
                lambda r: sorted(set((r["site_ids"] or []) + (r["site_ids2"] or []))),
                axis=1
            )
            cluster_edges = cluster_edges.drop(columns=["site_ids2"])

            members = (
                map_df.groupby("canonical_internal_id")
                      .agg(members_count=("internal_id","size"),
                           member_internal_ids=("internal_id",
                                                lambda s: sorted(list(map(int, set(s))))))
                      .reset_index()
            )

            canon_agg = (
                members.merge(cluster_edges, on="canonical_internal_id", how="left")
                       .fillna({"edges_count":0})
            )

            # write temp → merge
            conn.execute(text(f"DROP TABLE IF EXISTS {TMP_MAP};"))
            conn.execute(text(f"""
                CREATE TEMP TABLE {TMP_MAP} (
                  internal_id BIGINT,
                  canonical_internal_id BIGINT,
                  iid_added_min TIMESTAMPTZ
                ) ON COMMIT DROP;
            """))
            map_df.to_sql(TMP_MAP, con=conn, if_exists="append", index=False, method="multi",
                          chunksize=20000,
                          dtype={"internal_id": BIGINT(), "canonical_internal_id": BIGINT()})

            conn.execute(text(f"DROP TABLE IF EXISTS {TMP_CANON};"))
            conn.execute(text(f"""
                CREATE TEMP TABLE {TMP_CANON} (
                  canonical_internal_id BIGINT,
                  members_count INT,
                  member_internal_ids BIGINT[],
                  site_ids TEXT[],
                  first_seen TIMESTAMPTZ,
                  last_seen  TIMESTAMPTZ,
                  max_percentage NUMERIC,
                  edges_count INT
                ) ON COMMIT DROP;
            """))
            canon_agg.to_sql(TMP_CANON, con=conn, if_exists="append", index=False, method="multi",
                             chunksize=20000,
                             dtype={
                                 "canonical_internal_id": BIGINT(),
                                 "member_internal_ids": ARRAY(BIGINT()),
                                 "site_ids": ARRAY(String()),
                             })

            conn.execute(text(MERGE_MAP))
            conn.execute(text(MERGE_CANON))

            # ===================== NEW LOG OUTPUT =========================
            out = {
                "stage": "image_duplicate_clustering",
                "status": "ok",
                "sync_summary": {
                    "pairs_total": int(pairs_total),
                    "pairs_kept_geo_valid": int(pairs_kept),
                    "missing_iid1": miss1,
                    "missing_iid2": miss2,
                    "map_rows": int(len(map_df)),
                    "canon_rows": int(len(canon_agg)),
                    "elapsed_s": round(time.time() - t0, 3),
                }
            }

            print(json.dumps(out, ensure_ascii=False))
            return

    except Exception as e:
        out = {
            "stage": "image_duplicate_clustering",
            "status": "fail",
            "error": str(e),
            "elapsed_s": round(time.time() - t0, 3),
        }
        print(json.dumps(out, ensure_ascii=False))


if __name__ == "__main__":
    main()