# This script normalizes geo fields across Czech regions using RÚIAN reference data.
# It processes each region independently and returns a standard sync_summary JSON log.

from __future__ import annotations
import json
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from pathlib import Path
from rapidfuzz import process, fuzz
import unicodedata
import sys
import time

# ================== CONFIG ==================

PROJECT_ROOT = Path(__file__).resolve().parents[2]
cfg = json.loads((PROJECT_ROOT / "config.json").read_text(encoding="utf-8"))

DB_USER = cfg["USER"]
DB_PWD = cfg["PWD"]
DB_HOST = cfg["HOST"]
DB_PORT = cfg["PORT"]
DB_NAME = cfg["DB"]

SCHEMA_SILVER = "silver"
TABLE_SUBSET = "summarized_geo_subset"

KRAJ_CONFIG = {
    "Středočeský kraj": "ruian_ulice_stredocesky",
    "Jihočeský kraj": "ruian_ulice_jihocesky",
    "Jihomoravský kraj": "ruian_ulice_jihomoravsky",
    "Karlovarský kraj": "ruian_ulice_karlovarsky",
    "Královéhradecký kraj": "ruian_ulice_kralovehradecky",
    "Liberecký kraj": "ruian_ulice_liberecky",
    "Moravskoslezský kraj": "ruian_ulice_moravskoslezsky",
    "Olomoucký kraj": "ruian_ulice_olomoucky",
    "Pardubický kraj": "ruian_ulice_pardubicky",
    "Plzeňský kraj": "ruian_ulice_plzensky",
    "Ústecký kraj": "ruian_ulice_ustecky",
    "Kraj Vysočina": "ruian_ulice_vysocina",
    "Zlínský kraj": "ruian_ulice_zlinsky",
}


# ================== UTILS ==================

def strip_diacritics(s: str) -> str:
    if s is None:
        return ""
    return "".join(
        ch for ch in unicodedata.normalize("NFKD", s)
        if not unicodedata.combining(ch)
    )


def normalize(s: str | None) -> str:
    if s is None:
        return ""
    s = s.strip().lower()
    s = strip_diacritics(s)
    s = s.replace("-", " ")
    s = " ".join(s.split())
    return s


def fuzzy_best_match(query: str, candidates: list[str], score_cutoff=87) -> str | None:
    if not query or not candidates:
        return None
    res = process.extractOne(
        query,
        candidates,
        scorer=fuzz.WRatio,
        score_cutoff=score_cutoff,
    )
    return res[0] if res else None


def get_conn():
    return psycopg2.connect(
        user=DB_USER,
        password=DB_PWD,
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
    )


def ensure_columns_exist(cur):
    for col in ("geo_ok", "not_true"):
        cur.execute("""
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = %s
              AND table_name   = %s
              AND column_name  = %s
        """, (SCHEMA_SILVER, TABLE_SUBSET, col))
        if not cur.fetchone():
            cur.execute(
                f'ALTER TABLE {SCHEMA_SILVER}.{TABLE_SUBSET} '
                f'ADD COLUMN {col} boolean;'
            )


# ================== RUIAN LOADER ==================

def fetch_ruian_for_kraj(cur, kraj_name: str, ruian_table: str):
    cur.execute(f"""
        SELECT
            nazev_obce,
            nazev_casti_obce,
            nazev_ulice,
            region,
            kraj
        FROM {SCHEMA_SILVER}.{ruian_table}
        WHERE kraj = %s
    """, (kraj_name,))
    rows = cur.fetchall()

    if not rows:
        return None, None, None

    df = pd.DataFrame(
        rows,
        columns=["town_name", "part_name", "street_name", "region_name", "kraj_name"]
    )

    df["town_key_norm"] = df["town_name"].map(normalize)
    df["part_key_norm"] = df["part_name"].map(normalize)
    df["street_key_norm"] = df["street_name"].map(normalize)

    town_df = (
        df[df["town_key_norm"] != ""]
        .groupby("town_key_norm", as_index=False)
        .first()[["town_key_norm", "town_name", "region_name"]]
    )

    part_df = (
        df[df["part_key_norm"] != ""]
        .groupby("part_key_norm", as_index=False)
        .first()[["part_key_norm", "town_name", "region_name"]]
    )

    street_df = (
        df[df["street_key_norm"] != ""]
        .groupby("street_key_norm", as_index=False)
        .first()[["street_key_norm", "town_name", "region_name"]]
    )

    return town_df, street_df, part_df


# ================== SUBSET LOADER ==================

def fetch_subset_for_kraj(cur, kraj_name: str) -> pd.DataFrame:
    cur.execute(f"""
        SELECT
            internal_id,
            norm_district,
            norm_city,
            norm_city_part,
            norm_street,
            norm_okres,
            geo_ok,
            not_true
        FROM {SCHEMA_SILVER}.{TABLE_SUBSET}
        WHERE norm_district = %s
    """, (kraj_name,))
    rows = cur.fetchall()

    df = pd.DataFrame(
        rows,
        columns=[
            "internal_id",
            "norm_district",
            "norm_city",
            "norm_city_part",
            "norm_street",
            "norm_okres",
            "geo_ok",
            "not_true",
        ],
    )

    df["city_key_norm"] = df["norm_city"].map(normalize)
    df["part_key_norm"] = df["norm_city_part"].map(normalize)
    df["street_key_norm"] = df["norm_street"].map(normalize)

    df["geo_ok"] = df["geo_ok"].fillna(False).astype(bool)
    df["not_true"] = df["not_true"].fillna(True).astype(bool)

    return df


# ================== DECISION LOGIC ==================

def decide_row_for_kraj(row, kraj_name: str, town_df, part_df, street_df):
    city_norm = row["city_key_norm"]
    part_norm = row["part_key_norm"]
    street_norm = row["street_key_norm"]

    town_keys = town_df["town_key_norm"].tolist()
    part_keys = part_df["part_key_norm"].tolist()
    street_keys = street_df["street_key_norm"].tolist()

    town_name = None
    region_nm = None

    # direct or fuzzy match: city
    if city_norm:
        direct = town_df.loc[town_df["town_key_norm"] == city_norm]
        if not direct.empty:
            m = direct.iloc[0]
            town_name = m["town_name"]
            region_nm = m["region_name"]
        else:
            best = fuzzy_best_match(city_norm, town_keys, 87)
            if best:
                m = town_df.loc[town_df["town_key_norm"] == best].iloc[0]
                town_name = m["town_name"]
                region_nm = m["region_name"]

    # fuzzy: part
    if town_name is None and part_norm:
        best = fuzzy_best_match(part_norm, part_keys, 87)
        if best:
            m = part_df.loc[part_df["part_key_norm"] == best].iloc[0]
            town_name = m["town_name"]
            region_nm = m["region_name"]

    # fuzzy: street
    if town_name is None and street_norm:
        best = fuzzy_best_match(street_norm, street_keys, 87)
        if best:
            m = street_df.loc[street_df["street_key_norm"] == best].iloc[0]
            town_name = m["town_name"]
            region_nm = m["region_name"]

    if town_name:
        return {
            "fix_city": town_name,
            "fix_city_part": row["norm_city_part"],
            "fix_okres": region_nm,
            "fix_district": kraj_name,
            "geo_ok": True,
            "not_true": False,
            "matched": True,
        }

    # fallback bad
    return {
        "fix_city": row["norm_city"],
        "fix_city_part": row["norm_city_part"],
        "fix_okres": row["norm_okres"],
        "fix_district": row["norm_district"],
        "geo_ok": False,
        "not_true": True,
        "matched": False,
    }


# ================== BATCH UPDATER ==================

def apply_updates(cur, rows_to_update):
    if not rows_to_update:
        return 0

    tmp_name = "tmp_geo_upd_stage"
    cur.execute(f"DROP TABLE IF EXISTS {tmp_name}")

    cur.execute(f"""
        CREATE TEMP TABLE {tmp_name} (
            internal_id BIGINT PRIMARY KEY,
            fix_city TEXT,
            fix_city_part TEXT,
            fix_okres TEXT,
            fix_district TEXT,
            geo_ok BOOLEAN,
            not_true BOOLEAN
        ) ON COMMIT DROP
    """)

    execute_values(
        cur,
        f"""
        INSERT INTO {tmp_name}
        (fix_city, fix_city_part, fix_okres, fix_district, geo_ok, not_true, internal_id)
        VALUES %s
        """,
        rows_to_update
    )

    cur.execute(f"""
        UPDATE {SCHEMA_SILVER}.{TABLE_SUBSET} AS s
        SET
            norm_city      = COALESCE(u.fix_city,      s.norm_city),
            norm_city_part = COALESCE(u.fix_city_part, s.norm_city_part),
            norm_okres     = COALESCE(u.fix_okres,     s.norm_okres),
            norm_district  = COALESCE(u.fix_district,  s.norm_district),
            geo_ok         = u.geo_ok,
            not_true       = u.not_true
        FROM {tmp_name} u
        WHERE s.internal_id = u.internal_id
    """)

    return cur.rowcount


# ================== MAIN REGION PROCESSOR ==================

def process_one_kraj(cur, kraj_name: str, ruian_table: str) -> int:
    town_df, street_df, part_df = fetch_ruian_for_kraj(cur, kraj_name, ruian_table)
    if town_df is None:
        return 0

    df = fetch_subset_for_kraj(cur, kraj_name)
    if df.empty:
        return 0

    updates = []

    for _, row in df.iterrows():
        if row["geo_ok"]:
            continue

        dec = decide_row_for_kraj(row, kraj_name, town_df, part_df, street_df)

        updates.append((
            dec["fix_city"],
            dec["fix_city_part"],
            dec["fix_okres"],
            dec["fix_district"],
            dec["geo_ok"],
            dec["not_true"],
            int(row["internal_id"]),
        ))

    return apply_updates(cur, updates)


# ================== MAIN ==================

def main():
    start_ts = time.time()

    summary = {
        "stage": "geo_normalization_regions",
        "status": "ok",
        "sync_summary": {
            "regions_total": len(KRAJ_CONFIG),
            "regions_processed": 0,
            "rows_updated_total": 0,
            "per_region": {},
            "duration_s": 0.0
        }
    }

    try:
        with get_conn() as conn:
            conn.autocommit = False
            cur = conn.cursor()

            ensure_columns_exist(cur)
            conn.commit()

            for kraj_name, ruian_table in KRAJ_CONFIG.items():
                updated_rows = process_one_kraj(cur, kraj_name, ruian_table)
                conn.commit()

                summary["sync_summary"]["regions_processed"] += 1
                summary["sync_summary"]["rows_updated_total"] += updated_rows
                summary["sync_summary"]["per_region"][kraj_name] = {
                    "rows_updated": updated_rows
                }

            summary["sync_summary"]["duration_s"] = round(time.time() - start_ts, 3)

    except Exception as e:
        summary["status"] = "fail"
        summary["sync_summary"]["error"] = str(e)

    print(json.dumps(summary, ensure_ascii=False))


if __name__ == "__main__":
    main()