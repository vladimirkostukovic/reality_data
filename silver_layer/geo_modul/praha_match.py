#Cleans and reclassifies Prague-related address records in summarized_geo_subset.
#Applies multi-phase matching (Praha X, fuzzy Prague, Středočeský) and updates geo flags accordingly.

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
RUIAN_PRAHA_TABLE = "ruian_ulice_praha"
RUIAN_STRED_TABLE = "ruian_ulice_stredocesky"

PRAHA_DISTRICT_CANONICAL = "Hlavní město Praha"
STREDOCESKY_DISTRICT_CANONICAL = "Středočeský kraj"


# ================== NORMALIZATION ==================

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
    return " ".join(s.split())


def looks_like_praha_x(city_key_norm: str) -> bool:
    if not city_key_norm:
        return False
    parts = city_key_norm.split()
    return len(parts) >= 2 and parts[0] == "praha" and parts[1].isdigit()


def looks_like_just_praha(city_key_norm: str) -> bool:
    return city_key_norm in ("praha", "praga")


def canonicalize_praha_label(raw: str | None) -> str | None:
    if not raw:
        return None
    raw_n = normalize(raw)
    parts = raw_n.split()
    if len(parts) >= 2 and parts[0] == "praha" and parts[1].isdigit():
        return f"Praha {parts[1]}"
    return raw.strip() if raw else None


# ================== FUZZY ==================

def fuzzy_best_match(query: str, candidates: list[str], score_cutoff=87) -> str | None:
    if not query or not candidates:
        return None
    res = process.extractOne(
        query, candidates, scorer=fuzz.WRatio, score_cutoff=score_cutoff
    )
    return res[0] if res else None


# ================== DB HELPERS ==================

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
                f"ALTER TABLE {SCHEMA_SILVER}.{TABLE_SUBSET} ADD COLUMN {col} boolean;"
            )


def fetch_ruian_prague(cur):
    cur.execute(f"""
        SELECT nazev_ulice, nazev_casti_obce, nazev_obvodu_prahy
        FROM {SCHEMA_SILVER}.{RUIAN_PRAHA_TABLE}
        WHERE nazev_obce = 'Praha' AND nazev_obvodu_prahy IS NOT NULL
    """)
    rows = cur.fetchall()

    df = pd.DataFrame(
        rows,
        columns=["street_name", "city_part_name", "praha_district_raw"]
    )

    df["street_key_norm"] = df["street_name"].map(normalize)
    df["part_key_norm"] = df["city_part_name"].map(normalize)

    prg_part_df = df[df["part_key_norm"] != ""].groupby("part_key_norm").first().reset_index()
    prg_street_df = df[df["street_key_norm"] != ""].groupby("street_key_norm").first().reset_index()

    return prg_street_df, prg_part_df


def fetch_ruian_stredo(cur):
    cur.execute(f"""
        SELECT nazev_obce, nazev_casti_obce, nazev_ulice, region, kraj
        FROM {SCHEMA_SILVER}.{RUIAN_STRED_TABLE}
    """)
    rows = cur.fetchall()

    df = pd.DataFrame(
        rows,
        columns=["town_name", "part_name", "street_name", "region_name", "kraj_name"]
    )

    df["town_key_norm"] = df["town_name"].map(normalize)
    df["part_key_norm"] = df["part_name"].map(normalize)
    df["street_key_norm"] = df["street_name"].map(normalize)

    stc_town_df = df[df["town_key_norm"] != ""].groupby("town_key_norm").first().reset_index()
    stc_part_df = df[df["part_key_norm"] != ""].groupby("part_key_norm").first().reset_index()
    stc_street_df = df[df["street_key_norm"] != ""].groupby("street_key_norm").first().reset_index()

    return stc_town_df, stc_street_df, stc_part_df


def fetch_subset_for_praha(cur) -> pd.DataFrame:
    cur.execute(f"""
        SELECT internal_id, norm_district, norm_city, norm_city_part,
               norm_street, norm_okres, geo_ok, not_true
        FROM {SCHEMA_SILVER}.{TABLE_SUBSET}
        WHERE norm_district = %s
    """, (PRAHA_DISTRICT_CANONICAL,))

    rows = cur.fetchall()

    df = pd.DataFrame(rows, columns=[
        "internal_id", "norm_district", "norm_city", "norm_city_part",
        "norm_street", "norm_okres", "geo_ok", "not_true"
    ])

    df["city_key_norm"] = df["norm_city"].map(normalize)
    df["part_key_norm"] = df["norm_city_part"].map(normalize)
    df["street_key_norm"] = df["norm_street"].map(normalize)

    return df


# ================== DECISION LOGIC ==================

def decide_prague_phase1(row):
    city_norm = row["city_key_norm"]

    if looks_like_praha_x(city_norm):
        return {
            "fix_city": canonicalize_praha_label(row["norm_city"]),
            "fix_city_part": row["norm_city_part"],
            "fix_okres": row["norm_okres"],
            "fix_district": PRAHA_DISTRICT_CANONICAL,
            "geo_ok": True,
            "not_true": False,
            "matched": True,
        }

    if looks_like_just_praha(city_norm):
        return {
            "fix_city": "Praha",
            "fix_city_part": row["norm_city_part"],
            "fix_okres": row["norm_okres"],
            "fix_district": PRAHA_DISTRICT_CANONICAL,
            "geo_ok": True,
            "not_true": False,
            "matched": True,
        }

    return {"matched": False}


def decide_prague_phase2(row, prg_part_df, prg_street_df):
    part_norm = row["part_key_norm"]
    street_norm = row["street_key_norm"]

    part_keys = prg_part_df["part_key_norm"].tolist()
    street_keys = prg_street_df["street_key_norm"].tolist()

    guessed_district = None
    guessed_part_from_street = None

    if part_norm:
        best_part = fuzzy_best_match(part_norm, part_keys)
        if best_part:
            m = prg_part_df.loc[prg_part_df["part_key_norm"] == best_part].iloc[0]
            guessed_district = m["praha_district_raw"]

    if guessed_district is None and street_norm:
        best_street = fuzzy_best_match(street_norm, street_keys)
        if best_street:
            m = prg_street_df.loc[prg_street_df["street_key_norm"] == best_street].iloc[0]
            guessed_district = m["praha_district_raw"]
            guessed_part_from_street = m["city_part_name"]

    if guessed_district:
        return {
            "fix_city": canonicalize_praha_label(guessed_district),
            "fix_city_part": row["norm_city_part"] if row["norm_city_part"] else guessed_part_from_street,
            "fix_okres": row["norm_okres"],
            "fix_district": PRAHA_DISTRICT_CANONICAL,
            "geo_ok": True,
            "not_true": False,
            "matched": True,
        }

    return {"matched": False}


def decide_stredoczech_reclass(row, stc_town_df, stc_part_df, stc_street_df):
    city_norm = row["city_key_norm"]
    part_norm = row["part_key_norm"]
    street_norm = row["street_key_norm"]

    # Try by city
    city_match = stc_town_df.loc[stc_town_df["town_key_norm"] == city_norm]
    if not city_match.empty:
        m = city_match.iloc[0]
        return {
            "fix_city": m["town_name"],
            "fix_city_part": None,
            "fix_okres": m["region_name"],
            "fix_district": STREDOCESKY_DISTRICT_CANONICAL,
            "geo_ok": True,
            "not_true": False,
            "matched": True,
        }

    # fuzzy by city
    best_city = fuzzy_best_match(city_norm, stc_town_df["town_key_norm"].tolist())
    if best_city:
        m = stc_town_df.loc[stc_town_df["town_key_norm"] == best_city].iloc[0]
        return {
            "fix_city": m["town_name"],
            "fix_city_part": None,
            "fix_okres": m["region_name"],
            "fix_district": STREDOCESKY_DISTRICT_CANONICAL,
            "geo_ok": True,
            "not_true": False,
            "matched": True,
        }

    # try by part
    best_part = fuzzy_best_match(part_norm, stc_part_df["part_key_norm"].tolist())
    if best_part:
        m = stc_part_df.loc[stc_part_df["part_key_norm"] == best_part].iloc[0]
        return {
            "fix_city": m["town_name"],
            "fix_city_part": None,
            "fix_okres": m["region_name"],
            "fix_district": STREDOCESKY_DISTRICT_CANONICAL,
            "geo_ok": True,
            "not_true": False,
            "matched": True,
        }

    # try by street
    best_street = fuzzy_best_match(street_norm, stc_street_df["street_key_norm"].tolist())
    if best_street:
        m = stc_street_df.loc[stc_street_df["street_key_norm"] == best_street].iloc[0]
        return {
            "fix_city": m["town_name"],
            "fix_city_part": None,
            "fix_okres": m["region_name"],
            "fix_district": STREDOCESKY_DISTRICT_CANONICAL,
            "geo_ok": True,
            "not_true": False,
            "matched": True,
        }

    return {"matched": False}


# ================== MAIN ==================

def main():
    t0 = time.time()

    # final JSON result
    summary = {
        "stage": "geo_subset_cleanup",
        "status": "ok",
        "sync_summary": {
            "rows_total": 0,
            "phase1": 0,
            "phase2": 0,
            "phase3_ok": 0,
            "phase3_bad": 0,
            "rows_updated": 0,
            "elapsed_s": 0.0
        }
    }

    try:
        with get_conn() as conn:
            conn.autocommit = False
            cur = conn.cursor()

            ensure_columns_exist(cur)
            conn.commit()

            prg_street_df, prg_part_df = fetch_ruian_prague(cur)
            stc_town_df, stc_street_df, stc_part_df = fetch_ruian_stredo(cur)
            df = fetch_subset_for_praha(cur)

            total_rows = len(df)
            summary["sync_summary"]["rows_total"] = total_rows

            if df.empty:
                summary["sync_summary"]["elapsed_s"] = round(time.time() - t0, 3)
                print(json.dumps(summary, ensure_ascii=False))
                return

            updates_phase1 = []
            updates_phase2 = []
            updates_phase3_ok = []
            updates_phase3_bad = []

            # Iterate rows
            for _, row in df.iterrows():

                if row["geo_ok"] is True:
                    continue

                dec1 = decide_prague_phase1(row)
                if dec1["matched"]:
                    updates_phase1.append((
                        dec1["fix_city"],
                        dec1["fix_city_part"],
                        dec1["fix_okres"],
                        dec1["fix_district"],
                        True,
                        False,
                        int(row["internal_id"]),
                    ))
                    continue

                dec2 = decide_prague_phase2(row, prg_part_df, prg_street_df)
                if dec2["matched"]:
                    updates_phase2.append((
                        dec2["fix_city"],
                        dec2["fix_city_part"],
                        dec2["fix_okres"],
                        dec2["fix_district"],
                        True,
                        False,
                        int(row["internal_id"]),
                    ))
                    continue

                dec3 = decide_stredoczech_reclass(row, stc_town_df, stc_part_df, stc_street_df)
                if dec3["matched"]:
                    updates_phase3_ok.append((
                        dec3["fix_city"],
                        dec3["fix_city_part"],
                        dec3["fix_okres"],
                        dec3["fix_district"],
                        True,
                        False,
                        int(row["internal_id"]),
                    ))
                    continue

                # fallback
                updates_phase3_bad.append((
                    row["norm_city"],
                    row["norm_city_part"],
                    row["norm_okres"],
                    row["norm_district"],
                    False,
                    True,
                    int(row["internal_id"]),
                ))

            # Update counts
            summary["sync_summary"]["phase1"] = len(updates_phase1)
            summary["sync_summary"]["phase2"] = len(updates_phase2)
            summary["sync_summary"]["phase3_ok"] = len(updates_phase3_ok)
            summary["sync_summary"]["phase3_bad"] = len(updates_phase3_bad)

            # apply updates
            def apply_updates(batch_rows):
                if not batch_rows:
                    return 0

                cur.execute("DROP TABLE IF EXISTS tmp_geo_upd_clean")

                cur.execute("""
                    CREATE TEMP TABLE tmp_geo_upd_clean (
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
                    """
                    INSERT INTO tmp_geo_upd_clean
                    (fix_city, fix_city_part, fix_okres, fix_district, geo_ok, not_true, internal_id)
                    VALUES %s
                    """,
                    batch_rows
                )

                cur.execute(f"""
                    UPDATE {SCHEMA_SILVER}.{TABLE_SUBSET} AS s
                    SET
                        norm_city      = COALESCE(u.fix_city, s.norm_city),
                        norm_city_part = COALESCE(u.fix_city_part, s.norm_city_part),
                        norm_okres     = COALESCE(u.fix_okres, s.norm_okres),
                        norm_district  = COALESCE(u.fix_district, s.norm_district),
                        geo_ok         = u.geo_ok,
                        not_true       = u.not_true
                    FROM tmp_geo_upd_clean u
                    WHERE s.internal_id = u.internal_id
                """)

                return cur.rowcount

            total_upd = (
                apply_updates(updates_phase1)
                + apply_updates(updates_phase2)
                + apply_updates(updates_phase3_ok)
                + apply_updates(updates_phase3_bad)
            )

            conn.commit()

            summary["sync_summary"]["rows_updated"] = total_upd
            summary["sync_summary"]["elapsed_s"] = round(time.time() - t0, 3)

            print(json.dumps(summary, ensure_ascii=False))

    except Exception as e:
        err = {
            "stage": "geo_subset_cleanup",
            "status": "fail",
            "error": str(e)
        }
        print(json.dumps(err, ensure_ascii=False))
        sys.exit(1)


if __name__ == "__main__":
    main()