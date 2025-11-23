#This script synchronizes summarized_geo_subset with new records from summarized_geo.
#It inserts all missing internal_id rows into the subset table and marks them as not yet validated.
from __future__ import annotations
import json
import time
import sys
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# ============ CONFIG ============
PROJECT_ROOT = Path(__file__).resolve().parents[2]
cfg = json.loads((PROJECT_ROOT / "config.json").read_text(encoding="utf-8"))

DB_URL = f"postgresql+psycopg2://{cfg['USER']}:{cfg['PWD']}@{cfg['HOST']}:{cfg['PORT']}/{cfg['DB']}"
engine = create_engine(DB_URL, pool_pre_ping=True, future=True)

SCHEMA = "silver"
SRC = "summarized_geo"
DST = "summarized_geo_subset"

# ============ MAIN ============
def main():
    t0 = time.time()

    summary = {
        "stage": "subset_sync",
        "status": "ok",
        "stats": {
            "total_before": 0,
            "total_after": 0,
            "added": 0,
            "duration_s": 0.0
        },
        "note": None
    }

    try:
        with engine.begin() as conn:
            conn.execute(text("SET LOCAL statement_timeout = '5min'"))

            before = conn.execute(text(f"SELECT COUNT(*) FROM {SCHEMA}.{DST}")).scalar()
            summary["stats"]["total_before"] = before

            missing_ids = pd.read_sql(
                f"""
                SELECT s.internal_id,
                       s.norm_district,
                       s.norm_city,
                       s.norm_city_part,
                       s.norm_street,
                       s.norm_okres
                FROM {SCHEMA}.{SRC} s
                LEFT JOIN {SCHEMA}.{DST} d USING (internal_id)
                WHERE d.internal_id IS NULL
                """,
                conn,
            )

            if missing_ids.empty:
                summary["note"] = "no new rows"
                return summary   # <--- ВАЖНО: просто возвращаем!

            missing_ids["geo_ok"] = False
            missing_ids["not_true"] = True

            missing_ids.to_sql(
                DST,
                con=conn,
                schema=SCHEMA,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=10_000,
            )

            added = len(missing_ids)
            after = conn.execute(text(f"SELECT COUNT(*) FROM {SCHEMA}.{DST}")).scalar()

            summary["stats"]["added"] = added
            summary["stats"]["total_after"] = after

    except SQLAlchemyError as e:
        summary["status"] = "fail"
        summary["error"] = str(e)
    except Exception as e:
        summary["status"] = "fail"
        summary["error"] = str(e)
    finally:
        summary["stats"]["duration_s"] = round(time.time() - t0, 3)
        print(json.dumps(summary, ensure_ascii=False))


if __name__ == "__main__":
    main()