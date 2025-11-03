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

PROJECT_ROOT = Path(__file__).resolve().parents[1]
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


# ================== LOG ==================

def log(msg: str):
    ts = time.strftime("%H:%M:%S")
    sys.stdout.write(f"[{ts}] {msg}\n")
    sys.stdout.flush()


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
    s = " ".join(s.split())
    return s


def looks_like_praha_x(city_key_norm: str) -> bool:
    """Check if city name looks like 'praha 5', 'praha 10', etc."""
    if not city_key_norm:
        return False
    parts = city_key_norm.split()
    return len(parts) >= 2 and parts[0] == "praha" and parts[1].isdigit()


def looks_like_just_praha(city_key_norm: str) -> bool:
    """Check if city name is just 'praha' or 'praga'"""
    return city_key_norm in ("praha", "praga")


def canonicalize_praha_label(raw: str | None) -> str | None:
    """Convert 'praha 5' to 'Praha 5' format"""
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
        query,
        candidates,
        scorer=fuzz.WRatio,
        score_cutoff=score_cutoff,
    )
    if res:
        return res[0]
    return None


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
            log(f"Adding column {col}")
            cur.execute(
                f'ALTER TABLE {SCHEMA_SILVER}.{TABLE_SUBSET} '
                f'ADD COLUMN {col} boolean;'
            )


def fetch_ruian_prague(cur):
    log("Loading RÚIAN data for Prague...")
    cur.execute(f"""
        SELECT
            nazev_ulice,
            nazev_casti_obce,
            nazev_obvodu_prahy
        FROM {SCHEMA_SILVER}.{RUIAN_PRAHA_TABLE}
        WHERE nazev_obce = 'Praha'
          AND nazev_obvodu_prahy IS NOT NULL
    """)
    rows = cur.fetchall()

    df = pd.DataFrame(
        rows,
        columns=["street_name", "city_part_name", "praha_district_raw"]
    )

    log(f"  {len(df):,} RÚIAN rows for Prague")

    df["street_key_norm"] = df["street_name"].map(normalize)
    df["part_key_norm"] = df["city_part_name"].map(normalize)

    prg_part_df = (
        df[df["part_key_norm"] != ""]
        .groupby("part_key_norm", as_index=False)
        .first()[["part_key_norm", "praha_district_raw"]]
    )

    prg_street_df = (
        df[df["street_key_norm"] != ""]
        .groupby("street_key_norm", as_index=False)
        .first()[["street_key_norm", "praha_district_raw", "city_part_name"]]
    )

    log(f"  Unique city parts (Prague): {len(prg_part_df):,}")
    log(f"  Unique streets (Prague): {len(prg_street_df):,}")

    return prg_street_df, prg_part_df


def fetch_ruian_stredo(cur):
    log("Loading RÚIAN data for Středočeský kraj...")
    cur.execute(f"""
        SELECT
            nazev_obce,
            nazev_casti_obce,
            nazev_ulice,
            region,
            kraj
        FROM {SCHEMA_SILVER}.{RUIAN_STRED_TABLE}
    """)
    rows = cur.fetchall()

    df = pd.DataFrame(
        rows,
        columns=["town_name", "part_name", "street_name", "region_name", "kraj_name"]
    )

    log(f"  {len(df):,} RÚIAN rows for Středočeský")

    df["town_key_norm"] = df["town_name"].map(normalize)
    df["part_key_norm"] = df["part_name"].map(normalize)
    df["street_key_norm"] = df["street_name"].map(normalize)

    stc_town_df = (
        df[df["town_key_norm"] != ""]
        .groupby("town_key_norm", as_index=False)
        .first()[["town_key_norm", "town_name", "region_name"]]
    )

    stc_part_df = (
        df[df["part_key_norm"] != ""]
        .groupby("part_key_norm", as_index=False)
        .first()[["part_key_norm", "town_name", "region_name"]]
    )

    stc_street_df = (
        df[df["street_key_norm"] != ""]
        .groupby("street_key_norm", as_index=False)
        .first()[["street_key_norm", "town_name", "region_name"]]
    )

    log(f"  Unique towns (STČ): {len(stc_town_df):,}")
    log(f"  Unique town parts (STČ): {len(stc_part_df):,}")
    log(f"  Unique streets (STČ): {len(stc_street_df):,}")

    return stc_town_df, stc_street_df, stc_part_df


def fetch_subset_for_praha(cur) -> pd.DataFrame:
    log("Fetching Prague records from subset...")
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
    """, (PRAHA_DISTRICT_CANONICAL,))
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

    log(f"  Loaded {len(df):,} rows for Prague")

    df["city_key_norm"] = df["norm_city"].map(normalize)
    df["part_key_norm"] = df["norm_city_part"].map(normalize)
    df["street_key_norm"] = df["norm_street"].map(normalize)

    return df


# ================== DECISION LOGIC ==================

def decide_prague_phase1(row):
    """
    Phase 1: Check if norm_city is already in correct format
    - Case 1: 'Praha 5' or 'Praha 10' -> normalize and mark as OK
    - Case 2: Just 'Praha' or 'Praga' -> keep as is, mark as OK
    """
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
    """
    Phase 2: Try to determine Prague district via fuzzy matching
    of city part or street name against RÚIAN data
    """
    part_norm = row["part_key_norm"]
    street_norm = row["street_key_norm"]

    part_keys = prg_part_df["part_key_norm"].tolist()
    street_keys = prg_street_df["street_key_norm"].tolist()

    guessed_district = None
    guessed_part_from_street = None

    if part_norm:
        best_part = fuzzy_best_match(part_norm, part_keys, 87)
        if best_part:
            m = prg_part_df.loc[
                prg_part_df["part_key_norm"] == best_part
                ].iloc[0]
            guessed_district = m["praha_district_raw"]

    if guessed_district is None and street_norm:
        best_street = fuzzy_best_match(street_norm, street_keys, 87)
        if best_street:
            m = prg_street_df.loc[
                prg_street_df["street_key_norm"] == best_street
                ].iloc[0]
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
    """
    Phase 3: Check if this is actually Středočeský kraj (not Prague proper)
    Try matching by: city name -> city part -> street
    If found, reclassify to Středočeský kraj
    """
    city_norm = row["city_key_norm"]
    part_norm = row["part_key_norm"]
    street_norm = row["street_key_norm"]

    town_keys = stc_town_df["town_key_norm"].tolist()
    part_keys = stc_part_df["part_key_norm"].tolist()
    street_keys = stc_street_df["street_key_norm"].tolist()

    town_name = None
    region_nm = None

    if city_norm:
        city_match = stc_town_df.loc[stc_town_df["town_key_norm"] == city_norm]
        if not city_match.empty:
            m = city_match.iloc[0]
            town_name = m["town_name"]
            region_nm = m["region_name"]
        else:
            best_city = fuzzy_best_match(city_norm, town_keys, 87)
            if best_city:
                m = stc_town_df.loc[stc_town_df["town_key_norm"] == best_city].iloc[0]
                town_name = m["town_name"]
                region_nm = m["region_name"]

    if town_name is None and part_norm:
        best_part = fuzzy_best_match(part_norm, part_keys, 87)
        if best_part:
            m = stc_part_df.loc[
                stc_part_df["part_key_norm"] == best_part
                ].iloc[0]
            town_name = m["town_name"]
            region_nm = m["region_name"]

    if town_name is None and street_norm:
        best_street = fuzzy_best_match(street_norm, street_keys, 87)
        if best_street:
            m = stc_street_df.loc[
                stc_street_df["street_key_norm"] == best_street
                ].iloc[0]
            town_name = m["town_name"]
            region_nm = m["region_name"]

    if town_name:
        return {
            "fix_city": town_name,
            "fix_city_part": None,
            "fix_okres": region_nm,
            "fix_district": STREDOCESKY_DISTRICT_CANONICAL,
            "geo_ok": True,
            "not_true": False,
            "matched": True,
        }

    return {"matched": False}


# ================== MAIN ==================

def main():
    log("=== START GEO CHECK ===")

    with get_conn() as conn:
        conn.autocommit = False
        cur = conn.cursor()

        ensure_columns_exist(cur)
        conn.commit()

        prg_street_df, prg_part_df = fetch_ruian_prague(cur)
        stc_town_df, stc_street_df, stc_part_df = fetch_ruian_stredo(cur)
        df = fetch_subset_for_praha(cur)

        if df.empty:
            log("No Prague records found - exiting.")
            return

        updates_phase1 = []
        updates_phase2 = []
        updates_phase3_ok = []
        updates_phase3_bad = []

        t0 = time.time()

        for i, row in df.iterrows():

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

            updates_phase3_bad.append((
                row["norm_city"],
                row["norm_city_part"],
                row["norm_okres"],
                row["norm_district"],
                False,
                True,
                int(row["internal_id"]),
            ))

            if i % 5000 == 0 and i > 0:
                log(f"  processed {i:,} rows...")

        log(f"Phase 1 (Praha X + just Praha): {len(updates_phase1):,}")
        log(f"Phase 2 (Praha fuzzy): {len(updates_phase2):,}")
        log(f"Phase 3 (Středočeský OK): {len(updates_phase3_ok):,}")
        log(f"Phase 3 (fallback BAD): {len(updates_phase3_bad):,}")

        def apply_updates(batch_rows, label):
            if not batch_rows:
                log(f"{label}: no updates")
                return 0

            tmp_table_name = f"tmp_geo_upd_{label.lower().replace(' ', '_')}"

            cur.execute(f"DROP TABLE IF EXISTS {tmp_table_name}")

            cur.execute(f"""
                CREATE TEMP TABLE {tmp_table_name} (
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
                INSERT INTO {tmp_table_name}
                (fix_city, fix_city_part, fix_okres, fix_district, geo_ok, not_true, internal_id)
                VALUES %s
                """,
                batch_rows
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
                FROM {tmp_table_name} u
                WHERE s.internal_id = u.internal_id
            """)

            updated = cur.rowcount
            log(f"{label}: updated {updated:,} rows")
            return updated

        total_upd = 0
        total_upd += apply_updates(updates_phase1, "PHASE1 Praha")
        total_upd += apply_updates(updates_phase2, "PHASE2 Praha fuzzy")
        total_upd += apply_updates(updates_phase3_ok, "PHASE3 Středočeský OK")
        total_upd += apply_updates(updates_phase3_bad, "PHASE3 fallback BAD")

        conn.commit()

        log(f"Done. Total updated: {total_upd:,} rows in {round(time.time() - t0, 2)}s.")
        log("=== DONE ===")


if __name__ == "__main__":
    main()