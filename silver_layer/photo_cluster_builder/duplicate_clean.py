#This script takes canonical duplicate clusters, expands all internal IDs,
#and evaluates each pair for geographic, category, deal-type, price, and area consistency.
#Valid matched pairs are ranked, deduplicated, and then upserted into silver.image_duplicate_clean.
#Effectively it produces a clean, filtered set of true duplicates inside each canonical cluster.

from __future__ import annotations
import json
import time
from pathlib import Path
import itertools
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text, String
from sqlalchemy.dialects.postgresql import BIGINT, DOUBLE_PRECISION, BOOLEAN

# ============== CONFIG =================
PROJECT_ROOT = Path(__file__).resolve().parents[2]
CFG = json.loads((PROJECT_ROOT / "config.json").read_text(encoding="utf-8"))
DB_URL = f"postgresql+psycopg2://{CFG['USER']}:{CFG['PWD']}@{CFG['HOST']}:{CFG['PORT']}/{CFG['DB']}"

SCHEMA_SILVER  = "silver"
T_CANON        = "image_duplicate_canonical"
T_SUMMARY      = "summarized_geo"
T_CLEAN        = "image_duplicate_clean"
T_TMP          = "tmp_image_duplicate_clean"

SUMMARY_COLS = [
    "internal_id","added_date","available","archived_date","source_id",
    "category_value","category_name","deal_type","price","rooms",
    "area_build","area_land","longitude","latitude",
    "norm_okres","norm_city","norm_city_part","norm_street","norm_house_number",
    "ingested_at"
]

# ====== RULES ======
PRICE_RATIO    = 1.20
AREA_REL_TOL   = 0.05
AREA_ABS_TOL   = 5.0
CAT_LAND_VALUES = {3, 5, 90, 99}

DDL = f"""
CREATE SCHEMA IF NOT EXISTS {SCHEMA_SILVER};
CREATE TABLE IF NOT EXISTS {SCHEMA_SILVER}.{T_CLEAN} (
  canonical_internal_id BIGINT NOT NULL,
  iid1 BIGINT NOT NULL,
  iid2 BIGINT NOT NULL,
  match_level TEXT,            
  same_category BOOLEAN,
  same_deal_type BOOLEAN,
  price_ok BOOLEAN,
  area_ok BOOLEAN,
  price_hi DOUBLE PRECISION,
  price_lo DOUBLE PRECISION,
  area_mode TEXT,
  area1 DOUBLE PRECISION,
  area2 DOUBLE PRECISION,
  added1 DATE,
  added2 DATE,
  PRIMARY KEY (iid1, iid2)
);
"""

MERGE_SQL = f"""
INSERT INTO {SCHEMA_SILVER}.{T_CLEAN} AS t (
  canonical_internal_id, iid1, iid2, match_level, same_category, same_deal_type,
  price_ok, area_ok, price_hi, price_lo, area_mode, area1, area2, added1, added2
)
SELECT canonical_internal_id, iid1, iid2, match_level, same_category, same_deal_type,
       price_ok, area_ok, price_hi, price_lo, area_mode, area1, area2, added1, added2
FROM {T_TMP}
ON CONFLICT (iid1, iid2) DO UPDATE
SET match_level   = EXCLUDED.match_level,
    same_category = EXCLUDED.same_category,
    same_deal_type= EXCLUDED.same_deal_type,
    price_ok      = EXCLUDED.price_ok,
    area_ok       = EXCLUDED.area_ok,
    price_hi      = EXCLUDED.price_hi,
    price_lo      = EXCLUDED.price_lo,
    area_mode     = EXCLUDED.area_mode,
    area1         = EXCLUDED.area1,
    area2         = EXCLUDED.area2,
    added1        = LEAST(t.added1, EXCLUDED.added1),
    added2        = GREATEST(t.added2, EXCLUDED.added2);
"""


# ============ HELPERS ============

def _norm_txt(x):
    if x is None or (isinstance(x, float) and np.isnan(x)): return None
    s = str(x).strip().lower()
    return s if s else None


def _geo_level(a: pd.Series, b: pd.Series) -> str | None:
    ao, ac, ap, as_, ah = a["norm_okres"], a["norm_city"], a["norm_city_part"], a["norm_street"], a["norm_house_number"]
    bo, bc, bp, bs, bh = b["norm_okres"], b["norm_city"], b["norm_city_part"], b["norm_street"], b["norm_house_number"]
    ao, ac, ap, as_, ah = map(_norm_txt, (ao, ac, ap, as_, ah))
    bo, bc, bp, bs, bh = map(_norm_txt, (bo, bc, bp, bs, bh))

    if ao is not None and bo is not None and ao != bo: return None
    if ac is not None and bc is not None and ac != bc: return None
    if ap is not None and bp is not None and ap != bp: return None
    if as_ is not None and bs is not None and as_ != bs: return None

    if as_ and as_ == bs: return "lvl1"
    if ap and ap == bp:   return "lvl2"
    if ac and ac == bc:   return "lvl3"
    if ao and ao == bo:   return "lvl4"
    return None


def _price_check(p1, p2, ratio=PRICE_RATIO):
    p1 = float(p1) if pd.notna(p1) else None
    p2 = float(p2) if pd.notna(p2) else None
    if not p1 or not p2 or p1 <= 0 or p2 <= 0:
        return True, None, None
    hi, lo = (p1, p2) if p1 >= p2 else (p2, p1)
    return hi / lo <= ratio, hi, lo


def _areas_close(a1, a2, rel=AREA_REL_TOL, abs_tol=AREA_ABS_TOL):
    if a1 is None or a2 is None: return False
    try:
        a1 = float(a1); a2 = float(a2)
    except: return False
    if a1 <= 0 or a2 <= 0: return False
    diff = abs(a1 - a2)
    return diff <= max(abs_tol, rel * max(a1, a2))


def _area_check(r1: pd.Series, r2: pd.Series):
    cat1 = r1.get("category_value"); cat2 = r2.get("category_value")
    land1 = cat1 in CAT_LAND_VALUES if pd.notna(cat1) else False
    land2 = cat2 in CAT_LAND_VALUES if pd.notna(cat2) else False

    a1b, a1l = r1.get("area_build"), r1.get("area_land")
    a2b, a2l = r2.get("area_build"), r2.get("area_land")

    both = pd.notna(a1b) and pd.notna(a1l) and pd.notna(a2b) and pd.notna(a2l)
    if both:
        if _areas_close(a1b, a2b): return True, "build", float(a1b), float(a2b)
        if _areas_close(a1l, a2l): return True, "land",  float(a1l), float(a2l)
        return False, "mixed", None, None

    def pick(is_land, ab, al):
        primary = al if is_land else ab
        sec = ab if is_land else al
        if pd.notna(primary):   return float(primary), ("land" if is_land else "build")
        if pd.notna(sec):       return float(sec), ("build" if is_land else "land")
        return None, "none"

    v1, m1 = pick(land1, a1b, a1l)
    v2, m2 = pick(land2, a2b, a2l)
    if v1 is None or v2 is None: return False, "none", None, None
    return _areas_close(v1, v2), ("mixed" if m1 != m2 else m1), v1, v2


# ============ MAIN ============

def main():
    t0 = time.time()
    out = {
        "stage": "duplicate_internal_clean",
        "status": "ok",
        "sync_summary": {},
        "details": {},
        "elapsed_s": 0
    }

    try:
        engine = create_engine(DB_URL, pool_pre_ping=True)

        with engine.begin() as conn:
            conn.execute(text(DDL))

            # ===== CANONICAL IDS =====
            canon = pd.read_sql(f"""
                SELECT canonical_internal_id::bigint AS canonical_internal_id,
                       member_internal_ids          AS member_internal_ids
                FROM {SCHEMA_SILVER}.{T_CANON}
            """, conn)

            if canon.empty:
                out["sync_summary"] = {
                    "clusters_seen": 0,
                    "pairs_checked": 0,
                    "pairs_clean": 0,
                    "dedup_pairs_dropped": 0,
                    "missing_ids_total": 0,
                    "target": f"{SCHEMA_SILVER}.{T_CLEAN}",
                    "note": "no canonical clusters"
                }
                print(json.dumps(out, ensure_ascii=False)); return

            canon["member_internal_ids"] = canon["member_internal_ids"].apply(
                lambda x: x if isinstance(x, list) else []
            )
            canon = canon.explode("member_internal_ids").dropna()
            canon["internal_id"] = (
                pd.to_numeric(canon["member_internal_ids"], errors="coerce")
                .astype("Int64")
            )
            canon = canon.dropna(subset=["internal_id"]).copy()
            canon["internal_id"] = canon["internal_id"].astype("int64")
            canon = canon[["canonical_internal_id","internal_id"]]

            # ===== SUMMARY DATA =====
            df = pd.read_sql(f"""
                SELECT DISTINCT ON (s.internal_id)
                       {", ".join([f"s.{c}" for c in SUMMARY_COLS])}
                FROM {SCHEMA_SILVER}.{T_SUMMARY} s
                ORDER BY s.internal_id,
                         s.ingested_at DESC NULLS LAST,
                         s.archived_date DESC NULLS LAST,
                         s.added_date DESC NULLS LAST
            """, conn)

            if df.empty:
                out["sync_summary"] = {
                    "clusters_seen": 0,
                    "pairs_checked": 0,
                    "pairs_clean": 0,
                    "dedup_pairs_dropped": 0,
                    "missing_ids_total": 0,
                    "target": f"{SCHEMA_SILVER}.{T_CLEAN}",
                    "note": "empty summarized_geo"
                }
                print(json.dumps(out, ensure_ascii=False)); return

            for col in ["norm_okres","norm_city","norm_city_part","norm_street","norm_house_number"]:
                df[col] = df[col].apply(_norm_txt)

            df["price"] = pd.to_numeric(df["price"], errors="coerce")
            df["area_build"] = pd.to_numeric(df["area_build"], errors="coerce")
            df["area_land"]  = pd.to_numeric(df["area_land"], errors="coerce")
            df["internal_id"] = pd.to_numeric(df["internal_id"], errors="coerce").astype("Int64")
            df = df.dropna(subset=["internal_id"]).copy()
            df["internal_id"] = df["internal_id"].astype("int64")

            facts = df.drop_duplicates(subset=["internal_id"]).set_index("internal_id", drop=False)
            facts_idx = set(map(int, facts.index.tolist()))

            missing_ids_total = 0
            pairs_checked_total = 0

            rows = []

            # ===== PAIRS COMPARISON =====
            for canon_id, g in canon.groupby("canonical_internal_id"):
                raw_ids = (
                    pd.to_numeric(g["internal_id"], errors="coerce")
                    .dropna().astype("int64").tolist()
                )
                if len(raw_ids) < 2: continue

                ids = [i for i in raw_ids if i in facts_idx]
                missing_ids_total += len(raw_ids) - len(ids)
                if len(ids) < 2: continue

                ids = sorted(set(ids))

                for a, b in itertools.combinations(ids, 2):
                    pairs_checked_total += 1
                    r1 = facts.loc[int(a)]
                    r2 = facts.loc[int(b)]

                    lvl = _geo_level(r1, r2)
                    if lvl is None: continue

                    same_cat  = pd.notna(r1.category_value) and pd.notna(r2.category_value) and int(r1.category_value) == int(r2.category_value)
                    same_deal = str(r1.deal_type) == str(r2.deal_type)
                    if not (same_cat and same_deal): continue

                    price_ok, phi, plo = _price_check(r1.price, r2.price)
                    area_ok, mode, a1, a2 = _area_check(r1, r2)
                    if not area_ok: continue

                    iid1, iid2 = (a, b) if a < b else (b, a)

                    rows.append({
                        "canonical_internal_id": int(canon_id),
                        "iid1": int(iid1),
                        "iid2": int(iid2),
                        "match_level": lvl,
                        "same_category": True,
                        "same_deal_type": True,
                        "price_ok": bool(price_ok),
                        "area_ok": True,
                        "price_hi": None if phi is None else float(phi),
                        "price_lo": None if plo is None else float(plo),
                        "area_mode": mode,
                        "area1": None if a1 is None else float(a1),
                        "area2": None if a2 is None else float(a2),
                        "added1": r1.added_date,
                        "added2": r2.added_date,
                    })

            clean = pd.DataFrame(rows, columns=[
                "canonical_internal_id","iid1","iid2","match_level","same_category","same_deal_type",
                "price_ok","area_ok","price_hi","price_lo","area_mode","area1","area2","added1","added2"
            ])

            # ===== DEDUP =====
            if not clean.empty:
                _lvl_rank = {'lvl1': 1, 'lvl2': 2, 'lvl3': 3, 'lvl4': 4}
                _area_rank = {'build': 1, 'land': 2, 'mixed': 3, 'none': 4}

                clean["__lvl_r"]  = clean["match_level"].map(_lvl_rank).fillna(99)
                clean["__pr_r"]   = np.where(clean["price_ok"].fillna(False), 0, 1)
                clean["__area_r"] = clean["area_mode"].map(_area_rank).fillna(9)

                clean.sort_values(
                    by=["iid1", "iid2", "__lvl_r", "__pr_r", "__area_r"],
                    ascending=[True, True, True, True, True],
                    inplace=True
                )
                before = len(clean)
                clean = clean.drop_duplicates(subset=["iid1", "iid2"], keep="first").copy()
                clean.drop(columns=["__lvl_r","__pr_r","__area_r"], inplace=True)
                dedup_dropped = before - len(clean)
            else:
                dedup_dropped = 0

            # ===== UPSERT =====
            conn.execute(text(f"DROP TABLE IF EXISTS {T_TMP};"))
            conn.execute(text(f"""
                CREATE TEMP TABLE {T_TMP} (
                  canonical_internal_id BIGINT,
                  iid1 BIGINT,
                  iid2 BIGINT,
                  match_level TEXT,
                  same_category BOOLEAN,
                  same_deal_type BOOLEAN,
                  price_ok BOOLEAN,
                  area_ok BOOLEAN,
                  price_hi DOUBLE PRECISION,
                  price_lo DOUBLE PRECISION,
                  area_mode TEXT,
                  area1 DOUBLE PRECISION,
                  area2 DOUBLE_PRECISION,
                  added1 DATE,
                  added2 DATE
                ) ON COMMIT DROP;
            """))

            if not clean.empty:
                clean.to_sql(
                    T_TMP, con=conn, if_exists="append", index=False, method="multi",
                    dtype={
                        "canonical_internal_id": BIGINT(),
                        "iid1": BIGINT(), "iid2": BIGINT(),
                        "same_category": BOOLEAN(), "same_deal_type": BOOLEAN(),
                        "price_ok": BOOLEAN(), "area_ok": BOOLEAN(),
                        "price_hi": DOUBLE_PRECISION(), "price_lo": DOUBLE_PRECISION(),
                        "area1": DOUBLE_PRECISION(), "area2": DOUBLE_PRECISION(),
                        "match_level": String(), "area_mode": String()
                    }
                )
                conn.execute(text(MERGE_SQL))

            # ===== SYNC SUMMARY =====
            out["sync_summary"] = {
                "clusters_seen": int(canon["canonical_internal_id"].nunique()),
                "pairs_checked": int(pairs_checked_total),
                "pairs_clean": int(len(clean)),
                "dedup_pairs_dropped": int(dedup_dropped),
                "missing_ids_total": int(missing_ids_total),
                "target": f"{SCHEMA_SILVER}.{T_CLEAN}"
            }

    except Exception as e:
        out["status"] = "fail"
        out["details"] = {"error": f"{type(e).__name__}: {e}"}

    finally:
        out["elapsed_s"] = round(time.time() - t0, 3)
        print(json.dumps(out, ensure_ascii=False))


if __name__ == "__main__":
    main()