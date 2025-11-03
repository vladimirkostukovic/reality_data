import os
import sys
import json
import logging
import gc
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

from sqlalchemy import create_engine, text
from imagehash import hex_to_hash

# =============== LOGGING (stderr only) ===============
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | comparer | %(message)s",
    handlers=[logging.StreamHandler(sys.stderr)],
    force=True,
)
log = logging.getLogger("comparer")

# === CONFIG & ENGINE SETUP ===
PROJECT_ROOT = Path(__file__).resolve().parents[1]
with open(PROJECT_ROOT / 'config.json') as f:
    cfg = json.load(f)

DB_URL = f"postgresql+psycopg2://{cfg['USER']}:{cfg['PWD']}@{cfg['HOST']}:{cfg['PORT']}/{cfg['DB']}"
engine = create_engine(DB_URL, pool_pre_ping=True)

# === PARAMS ===
BUCKET_BITS = 8          # первые 2 hex-символа
HASH_SIZE = 16           # 64 hex chars
HAMMING_THRESHOLD = 6    # <= 6 считаем дублем
PROCESSES = max(1, os.cpu_count() - 1)
DRY_RUN = False

# === HASH BUCKET HELPERS ===
def hash_to_bucket(hash_str, bucket_bits=8):
    return int(hash_str[:bucket_bits // 4], 16)

def neighbors(bucket, bucket_bits):
    return [bucket ^ (1 << i) for i in range(bucket_bits)]

# === DUPLICATE TABLE CREATION ===
def create_duplicates_table():
    ddl = """
    CREATE TABLE IF NOT EXISTS silver.image_duplicate (
        site_id1 BIGINT NOT NULL,
        site_id2 BIGINT NOT NULL,
        hash1 VARCHAR NOT NULL,
        hash2 VARCHAR NOT NULL,
        percentage NUMERIC NOT NULL,
        date TIMESTAMP NOT NULL DEFAULT now()
    );
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))

# === LOAD HASHES WITH BUCKETS ===
def load_hashes_with_buckets(bucket_bits, is_old_value):
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT site_id, phash FROM image_hashes WHERE is_old = :is_old"),
            {'is_old': is_old_value}
        ).mappings().all()
    data = []
    for row in rows:
        rec = dict(row)
        rec['bucket_id'] = hash_to_bucket(rec['phash'], bucket_bits)
        data.append(rec)
    return data

# === HAMMING TO PERCENT ===
def hamming_to_percent(dist, hash_len):
    return 100 * (1 - dist / (hash_len * 4))

# === PAIRWISE COMPARE ===
def compare_pair(e1, e2, hash_size, threshold):
    h1 = hex_to_hash(e1['phash'])
    h2 = hex_to_hash(e2['phash'])
    dist = h1 - h2
    if dist <= threshold:
        return {
            'site_id1': e1['site_id'],
            'site_id2': e2['site_id'],
            'hash1': e1['phash'],
            'hash2': e2['phash'],
            'percentage': hamming_to_percent(dist, hash_size),
            'date': datetime.now()
        }
    return None

def compare_bucket(bucket_idx, total_buckets, bid, buckets_new, buckets_old, hash_size, threshold):
    bucket_entries = buckets_new.get(bid, [])
    if not bucket_entries:
        return [], 0

    neighbors_ids = neighbors(bid, BUCKET_BITS)
    candidates = []
    candidates.extend(buckets_old.get(bid, []))
    for nb in neighbors_ids:
        candidates.extend(buckets_old.get(nb, []))

    out = []
    local_matches = 0
    for idx, e1 in enumerate(bucket_entries):
        for e2 in candidates:
            if e1['site_id'] >= e2['site_id']:
                continue
            cmp = compare_pair(e1, e2, hash_size, threshold)
            if cmp:
                out.append(cmp)
                local_matches += 1
        if (idx + 1) % 500 == 0:
            log.info(
                "Bucket %s/%s [%s] — %s/%s entries processed, matches: %s",
                bucket_idx+1, total_buckets, bid, idx+1, len(bucket_entries), local_matches
            )
    log.info(
        "Bucket %s/%s [%s] DONE: %s entries, matches: %s",
        bucket_idx+1, total_buckets, bid, len(bucket_entries), local_matches
    )
    return out, local_matches

# === BATCH INSERT DUPLICATES ===
def batch_insert_duplicates(batch):
    if not batch:
        return
    sql = text(
        "INSERT INTO silver.image_duplicate"
        "(site_id1, site_id2, hash1, hash2, percentage, date)"
        " VALUES (:site_id1, :site_id2, :hash1, :hash2, :percentage, :date)"
        " ON CONFLICT DO NOTHING"
    )
    with engine.begin() as conn:
        conn.execute(sql, batch)

# === MAIN ===
def main():
    create_duplicates_table()

    log.info("Load new hashes (is_old=false) and old hashes (is_old=true)")
    new_data = load_hashes_with_buckets(BUCKET_BITS, False)
    old_data = load_hashes_with_buckets(BUCKET_BITS, True)

    if not new_data:
        log.info("No new hashes to compare. Exit.")
        return {"prefixes": 0, "links": 0}

    buckets_new = {}
    for r in new_data:
        buckets_new.setdefault(r['bucket_id'], []).append(r)
    buckets_old = {}
    for r in old_data:
        buckets_old.setdefault(r['bucket_id'], []).append(r)

    bucket_ids = sorted(buckets_new.keys())
    total_buckets = len(bucket_ids)
    log.info("Total buckets to compare: %s", total_buckets)

    inserted = 0
    total_found = 0
    compared = 0

    with ProcessPoolExecutor(max_workers=PROCESSES) as ex:
        futs = []
        for i, bid in enumerate(bucket_ids):
            futs.append(ex.submit(
                compare_bucket, i, total_buckets, bid, buckets_new, buckets_old, HASH_SIZE, HAMMING_THRESHOLD
            ))
        for fut in as_completed(futs):
            results, found = fut.result()
            compared += 1
            total_found += found
            if results and not DRY_RUN:
                batch_insert_duplicates(results)
                inserted += len(results)
            if compared % 10 == 0 or found:
                log.info("Progress: %s/%s buckets, new pairs inserted: %s, found total: %s",
                         compared, total_buckets, inserted, total_found)
            gc.collect()

    # Финальный вброс, если что-то осталось (на самом деле уже вставлено по месту)
    log.info("Done. Compared %s buckets, inserted %s duplicates, total found: %s",
             compared, inserted, total_found)

    # Отметить все новые как старые
    with engine.begin() as conn:
        conn.execute(text("UPDATE image_hashes SET is_old = true WHERE is_old = false"))
    log.info("All processed new hashes are now marked as old.")

    return {"prefixes": total_buckets, "links": inserted}

if __name__ == '__main__':
    main()