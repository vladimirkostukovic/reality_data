
import sys
import json
import time
import logging
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# --------- Logging ---------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | geo_sync | %(message)s",
    handlers=[logging.StreamHandler(sys.stderr)],
    force=True,
)
log = logging.getLogger("geo_sync")

# --------- CONFIG ----------
PROJECT_ROOT = Path(__file__).resolve().parents[1]
with open(PROJECT_ROOT / "config.json", "r", encoding="utf-8") as f:
    cfg = json.load(f)

DB_URL = (
    f"postgresql+psycopg2://"
    f"{cfg['USER']}:{cfg['PWD']}@{cfg['HOST']}:{cfg['PORT']}/{cfg['DB']}"
)
engine = create_engine(DB_URL, pool_pre_ping=True)

INGEST_COL = "ingested_at"  # timestamptz
INGEST_NOW = datetime.now(timezone.utc)


def _count_geo(conn) -> int:
    return int(
        conn.execute(
            text("SELECT COUNT(*) FROM silver.summarized_geo")
        ).scalar()
        or 0
    )


def ensure_columns(conn):
    exists_ing = conn.execute(
        text("""
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'silver'
              AND table_name   = 'summarized_geo'
              AND column_name  = :col
            LIMIT 1
        """),
        {"col": INGEST_COL},
    ).scalar()
    if not exists_ing:
        log.info(f"ADD COLUMN silver.summarized_geo.{INGEST_COL}")
        conn.execute(text(f"""
            ALTER TABLE silver.summarized_geo
            ADD COLUMN {INGEST_COL} timestamptz
            DEFAULT NOW()
        """))
        conn.execute(text(f"""
            ALTER TABLE silver.summarized_geo
            ALTER COLUMN {INGEST_COL} DROP DEFAULT
        """))

    # deal_type_value
    exists_dtv = conn.execute(
        text("""
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'silver'
              AND table_name   = 'summarized_geo'
              AND column_name  = 'deal_type_value'
            LIMIT 1
        """)
    ).scalar()
    if not exists_dtv:
        log.info("ADD COLUMN silver.summarized_geo.deal_type_value")
        conn.execute(
            text("""
                ALTER TABLE silver.summarized_geo
                ADD COLUMN deal_type_value integer
            """)
        )


def _strip_diacritics(s: str) -> str:
    repl = {
        "á": "a", "č": "c", "ď": "d", "é": "e", "ě": "e",
        "í": "i", "ň": "n", "ó": "o", "ř": "r", "š": "s",
        "ť": "t", "ú": "u", "ů": "u", "ý": "y", "ž": "z",
    }
    return "".join(repl.get(ch, ch) for ch in s)


def classify_row(raw_deal: str, raw_name: str):
    def normtxt(x):
        if x is None:
            return ""
        x0 = str(x).strip().lower()
        return _strip_diacritics(x0)

    dt = normtxt(raw_deal)
    nm = normtxt(raw_name)

    # -------- prodej / sale --------
    if (
        dt.startswith("prodej")
        or dt in ("prodej", "sale", "prodaja", "продажа")
        or nm.startswith("prodej")
        or nm.startswith("sale")
        or nm.startswith("продажа")
    ):
        return "Prodej", 1

    # -------- pronajem / nájem / аренда / rent --------
    if (
        dt.startswith("pronajem")
        or dt in ("pronajem", "najem", "najmem", "аренда", "arenda", "rent")
        or nm.startswith("pronajem")
        or nm.startswith("najem")
        or nm.startswith("аренда")
        or nm.startswith("rent")
        or "pronajem" in nm
        or "pronajmu" in nm
        or "nájem" in raw_deal.lower() if raw_deal else False
        or "nájem" in raw_name.lower() if raw_name else False
    ):
        return "Pronájem", 2

    # -------- dražba / aukce / аукцион --------
    if (
        dt.startswith("drazb")
        or dt in ("drazba", "drazby", "aukce", "dražba", "аукцион")
        or "drazb" in nm
        or "dražb" in nm
        or "aukce" in nm
        or "аукцион" in nm
    ):
        return "Dražba", 3

    return "Neznámé", 0


def normalize_deal_type(df_sum: pd.DataFrame) -> pd.DataFrame:
    nice_list = []
    code_list = []

    deal_col = df_sum["deal_type"] if "deal_type" in df_sum.columns else pd.Series([None] * len(df_sum))
    name_col = df_sum["name"] if "name" in df_sum.columns else pd.Series([None] * len(df_sum))

    for raw_deal, raw_name in zip(deal_col.tolist(), name_col.tolist()):
        nice, code = classify_row(raw_deal, raw_name)
        nice_list.append(nice)
        code_list.append(code)

    return pd.DataFrame({
        "deal_type": nice_list,
        "deal_type_value": code_list
    })


def main() -> dict:
    t0 = time.perf_counter()
    rows_inserted = 0
    rows_updated = 0
    before = 0
    after = 0
    sanity_problems = []
    sanity_warnings = []

    try:
        with engine.begin() as conn:
            ensure_columns(conn)

            before = _count_geo(conn)
            log.info("before=%d", before)
            log.info("load summarized")
            df_sum = pd.read_sql(
                """
                SELECT internal_id,
                       site_id,
                       added_date,
                       available,
                       archived_date,
                       source_id,
                       category_value,
                       category_name,
                       name,
                       deal_type,
                       price,
                       rooms,
                       area_build,
                       district,
                       city,
                       city_part,
                       street,
                       house_number,
                       longitude,
                       latitude
                FROM silver.summarized
                """,
                con=conn,
            )

            norm_dt = normalize_deal_type(df_sum)
            df_sum["deal_type"] = norm_dt["deal_type"]
            df_sum["deal_type_value"] = norm_dt["deal_type_value"]

            log.info("load summarized_geo")
            df_geo = pd.read_sql(
                """
                SELECT internal_id,
                       archived_date,
                       available,
                       added_date
                FROM silver.summarized_geo
                """,
                con=conn,
            )

            log.info("load geo_garbage")
            df_garbage = pd.read_sql(
                "SELECT internal_id FROM silver.geo_garbage",
                con=conn,
            )

            ids_in_geo = set(df_geo["internal_id"])
            ids_in_garbage = set(df_garbage["internal_id"])
            new_ids = set(df_sum["internal_id"]) - ids_in_geo - ids_in_garbage

            df_new = df_sum[df_sum["internal_id"].isin(new_ids)].copy()

            df_new = df_new[
                (df_new["price"] > 1000)
                & (df_new["price"] < 10_000_000_000)
            ].copy()

            expected_to_insert = len(df_new)

            df_new["geo_status"] = False
            df_new[INGEST_COL] = INGEST_NOW

            for col in df_new.columns:
                if (
                    pd.api.types.is_float_dtype(df_new[col])
                    or pd.api.types.is_datetime64_any_dtype(df_new[col])
                ):
                    df_new[col] = df_new[col].where(pd.notnull(df_new[col]), None)

            allowed_columns = [
                "internal_id",
                "site_id",
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
                "district",
                "city",
                "city_part",
                "street",
                "house_number",
                "longitude",
                "latitude",
                "geo_status",
                INGEST_COL,
            ]

            df_new = df_new[[c for c in allowed_columns if c in df_new.columns]]

            if not df_new.empty:
                df_new.to_sql(
                    "summarized_geo",
                    schema="silver",
                    con=conn,
                    if_exists="append",
                    index=False,
                    method="multi",
                    chunksize=50_000,
                )
                rows_inserted = len(df_new)
                log.info(f"inserted: {rows_inserted}")
            else:
                log.info("inserted: 0")

            merged = pd.merge(
                df_sum[
                    [
                        "internal_id",
                        "archived_date",
                        "available",
                        "added_date",
                    ]
                ],
                df_geo[
                    [
                        "internal_id",
                        "archived_date",
                        "available",
                        "added_date",
                    ]
                ],
                on="internal_id",
                suffixes=("_sum", "_geo"),
            )

            mask_change = (
                (merged["archived_date_sum"].astype(str) != merged["archived_date_geo"].astype(str))
                | (merged["available_sum"] != merged["available_geo"])
                | (merged["added_date_sum"].astype(str) != merged["added_date_geo"].astype(str))
            )

            to_update = merged.loc[
                mask_change,
                [
                    "internal_id",
                    "archived_date_sum",
                    "available_sum",
                    "added_date_sum",
                ],
            ].copy()

            to_update = to_update.rename(
                columns={
                    "archived_date_sum": "archived_date",
                    "available_sum": "available",
                    "added_date_sum": "added_date",
                }
            )

            if not to_update.empty:
                conn.execute(
                    text("""
                        CREATE TEMP TABLE tmp_geo_update (
                            internal_id  BIGINT PRIMARY KEY,
                            archived_date DATE,
                            available     BOOLEAN,
                            added_date    DATE
                        ) ON COMMIT DROP
                    """)
                )

                to_update.to_sql(
                    "tmp_geo_update",
                    con=conn,
                    if_exists="append",
                    index=False,
                    method="multi",
                    chunksize=50_000,
                )

                res = conn.execute(
                    text("""
                        UPDATE silver.summarized_geo AS tgt
                        SET archived_date = src.archived_date,
                            available     = src.available,
                            added_date    = src.added_date
                        FROM tmp_geo_update AS src
                        WHERE tgt.internal_id = src.internal_id
                    """)
                )

                rows_updated = (
                    res.rowcount
                    if (hasattr(res, "rowcount") and res.rowcount and res.rowcount > 0)
                    else len(to_update)
                )
                log.info(f"updated: {rows_updated}")
            else:
                log.info("updated: 0")

            # ---------- SANITY ----------
            after = _count_geo(conn)

            # 1) mass-balance check
            if after != before + rows_inserted:
                sanity_problems.append(
                    f"mass-balance: after({after}) != before({before}) + inserted({rows_inserted})"
                )

            # 2) unique internal_id
            try:
                dupes = int(
                    conn.execute(
                        text("""
                            SELECT COUNT(*) - COUNT(DISTINCT internal_id) AS dupes
                            FROM silver.summarized_geo
                        """)
                    ).scalar()
                    or 0
                )
                if dupes > 0:
                    sanity_problems.append(
                        f"dupes in summarized_geo.internal_id: {dupes}"
                    )
                else:
                    log.info("sanity: unique internal_id OK")
            except Exception as e:
                sanity_warnings.append(f"unique check skipped: {e}")

            # 3) FK coverage
            try:
                missing_fk = int(
                    conn.execute(
                        text("""
                            SELECT COUNT(*)
                            FROM silver.summarized_geo g
                            LEFT JOIN silver.summarized s
                              ON s.internal_id = g.internal_id
                            WHERE s.internal_id IS NULL
                        """)
                    ).scalar()
                    or 0
                )
                if missing_fk > 0:
                    sanity_problems.append(
                        f"fk coverage: {missing_fk} rows in geo without parent in summarized"
                    )
                else:
                    log.info("sanity: fk coverage OK")
            except Exception as e:
                sanity_warnings.append(f"fk check skipped: {e}")
            try:
                in_garbage = int(
                    conn.execute(
                        text("""
                            SELECT COUNT(*)
                            FROM silver.summarized_geo g
                            JOIN silver.geo_garbage gg USING(internal_id)
                        """)
                    ).scalar()
                    or 0
                )
                if in_garbage > 0:
                    sanity_problems.append(
                        f"garbage leak: {in_garbage} rows present in summarized_geo"
                    )
                else:
                    log.info("sanity: garbage exclusion OK")
            except Exception as e:
                sanity_warnings.append(f"garbage check skipped: {e}")
            if expected_to_insert != rows_inserted:
                sanity_warnings.append(
                    f"expected_to_insert({expected_to_insert}) != rows_inserted({rows_inserted})"
                )
            else:
                log.info("sanity: expected_to_insert matches rows_inserted")

        elapsed = round(time.perf_counter() - t0, 3)
        out = {
            "module": "geo_sync",
            "ok": len(sanity_problems) == 0,
            "rows_inserted": rows_inserted,
            "rows_updated": rows_updated,
            "before": before,
            "after": after,
            "elapsed_s": elapsed,
            "sanity_ok": len(sanity_problems) == 0,
            "sanity_problems": sanity_problems,
            "sanity_warnings": sanity_warnings,
        }
        sys.stdout.write(json.dumps(out, ensure_ascii=False) + "\n")
        return out

    except SQLAlchemyError as e:
        log.error(f"failed: {e}")
        sys.stdout.write(
            json.dumps(
                {
                    "module": "geo_sync",
                    "ok": False,
                    "error": str(e),
                },
                ensure_ascii=False,
            )
            + "\n"
        )
        raise
    except Exception as e:
        log.error(f"unexpected: {e}")
        sys.stdout.write(
            json.dumps(
                {
                    "module": "geo_sync",
                    "ok": False,
                    "error": str(e),
                },
                ensure_ascii=False,
            )
            + "\n"
        )
        raise


if __name__ == "__main__":
    main()