import os
import sys
import json
import time
import logging
from pathlib import Path
from sqlalchemy import create_engine, text

# ---------- LOGGING----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | cleanup | %(message)s",
    handlers=[logging.StreamHandler(sys.stderr)],
    force=True,
)
log = logging.getLogger("cleanup")

# ---------- CONFIG ----------
PROJECT_ROOT = Path(__file__).resolve().parents[1]
with open(PROJECT_ROOT / "config.json", "r", encoding="utf-8") as f:
    cfg = json.load(f)

DB_URL = f"postgresql+psycopg2://{cfg['USER']}:{cfg['PWD']}@{cfg['HOST']}:{cfg['PORT']}/{cfg['DB']}"
TABLE = os.getenv("IMGDL_TABLE", "image_downloaded")

BATCH_LIMIT = int(os.getenv("IMGDL_CLEAN_BATCH", "50000"))
SLEEP_SEC = float(os.getenv("IMGDL_CLEAN_SLEEP", "0.2"))

engine = create_engine(DB_URL, pool_pre_ping=True)

# ---------- SQL  ----------
SQL_CUTOFF = text("select date_trunc('month', now()) - interval '1 month' as cutoff")

SQL_CREATE_IDX_CONCURRENT = f"CREATE INDEX IF NOT EXISTS ix_{TABLE}_download_date ON {TABLE} (download_date)"
SQL_CREATE_IDX_CONCURRENT += "/*+ CONCURRENTLY */"

SQL_DELETE_BATCH = text(f"""
with cut as ({SQL_CUTOFF.text}),
     todel as (
         select t.ctid
         from {TABLE} t, cut
         where t.download_date < cut.cutoff
         limit :lim
     )
delete from {TABLE} d
using todel
where d.ctid = todel.ctid
returning 1
""")

SQL_COUNT_TODEL = text(f"""
with cut as ({SQL_CUTOFF.text})
select count(*) from {TABLE} t, cut
where t.download_date < cut.cutoff
""")

def get_cutoff(conn):
    return conn.execute(SQL_CUTOFF).scalar()

def ensure_index_concurrently():
    ddl = f"CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_{TABLE}_download_date ON {TABLE} (download_date);"
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        conn.exec_driver_sql(ddl)

def count_to_delete(conn):
    return conn.execute(SQL_COUNT_TODEL).scalar()

def delete_batch(conn, limit: int) -> int:
    res = conn.execute(SQL_DELETE_BATCH, {"lim": limit})
    return res.rowcount or 0

def vacuum_analyze():
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        conn.exec_driver_sql(f"VACUUM (ANALYZE) {TABLE};")

def main():
    log.info("Cleanup started for table=%s", TABLE)

    ensure_index_concurrently()

    with engine.begin() as conn:
        cutoff = get_cutoff(conn)
        log.info("Cutoff (keep >=): %s", cutoff.strftime("%Y-%m-%d %H:%M:%S"))
а
    with engine.begin() as conn:
        to_del = count_to_delete(conn)
    if to_del == 0:
        log.info("Nothing to delete. Table already within prev month window.")
        return

    deleted_total = 0
    loops = 0
    t0 = time.perf_counter()

    while True:
        with engine.begin() as conn:
            deleted = delete_batch(conn, BATCH_LIMIT)
            remain = count_to_delete(conn)

        deleted_total += deleted
        loops += 1
        log.info("Loop %d: deleted=%d, remain=%d", loops, deleted, remain)

        if remain <= 0 or deleted == 0:
            break
        time.sleep(SLEEP_SEC)

    log.info("Running VACUUM ANALYZE on %s ...", TABLE)
    vacuum_analyze()

    elapsed = round(time.perf_counter() - t0, 2)
    log.info("Cleanup finished. Deleted=%d rows in %ss", deleted_total, elapsed)

if __name__ == "__main__":
    main()