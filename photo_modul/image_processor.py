import os
import sys
import json
import logging
import gc
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

from PIL import Image, ImageOps
import imagehash
from sqlalchemy import create_engine, text

# =============== LOGGING (stderr only) ===============
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | hasher | %(message)s",
    handlers=[logging.StreamHandler(sys.stderr)],
    force=True,
)
log = logging.getLogger("hasher")

# =============== CONFIG & DB =========================
PROJECT_ROOT = Path(__file__).resolve().parents[1]
with open(PROJECT_ROOT / "config.json", "r", encoding="utf-8") as f:
    cfg = json.load(f)

DB_URL = f"postgresql+psycopg2://{cfg['USER']}:{cfg['PWD']}@{cfg['HOST']}:{cfg['PORT']}/{cfg['DB']}"
engine = create_engine(DB_URL, pool_pre_ping=True)

_base = Path(__file__).resolve().parent
_cfg_dir = Path(cfg.get("local_image_dir", "images"))
IMAGE_DIR = (_cfg_dir if _cfg_dir.is_absolute() else (_base / _cfg_dir)).resolve()
os.makedirs(IMAGE_DIR, exist_ok=True)
log.info("Using image directory: %s", IMAGE_DIR)

BATCH_SIZE = 1000
HASH_SIZE = 16
MAX_SIZE = 512
MAX_WORKERS = cfg.get("processor", {}).get("max_workers", 8)

# =========== FILES TO PROCESS =========
def _load_candidates():
    all_files = []
    with engine.connect() as conn:
        res = conn.execute(text(
            "SELECT site_id, filenames, source FROM image_downloaded WHERE downloaded = true"
        ))
        for site_id, filenames, source in res:
            if not filenames:
                continue
            try:
                files_list = json.loads(filenames) if isinstance(filenames, str) else filenames
            except Exception:
                continue
            for fn in files_list:
                p = (IMAGE_DIR / fn)
                if p.exists():
                    all_files.append({'site_id': site_id, 'filename': fn, 'source': source})

    done = set()
    with engine.connect() as conn:
        res = conn.execute(text("SELECT site_id, filename, source_id FROM image_hashes"))
        for site_id, filename, source_id in res:
            done.add((str(site_id), str(filename), str(source_id)))

    to_do = [x for x in all_files if (str(x['site_id']), str(x['filename']), str(x['source'])) not in done]
    log.info("To process: %s files", len(to_do))
    return to_do

# =========== PHASH ====================
def _compute_phash(path: Path, hash_size=16, max_size=512):
    try:
        with Image.open(path) as img:
            img = ImageOps.exif_transpose(img).convert("RGB")
            img.thumbnail((max_size, max_size))
            return str(imagehash.phash(img, hash_size=hash_size))
    except Exception:
        return None

def _hash_entry(entry, hash_size, max_size):
    ph = _compute_phash(IMAGE_DIR / entry['filename'], hash_size, max_size)
    if not ph:
        return None
    return {
        'site_id': entry['site_id'],
        'filename': entry['filename'],
        'phash': ph,
        'source_id': entry['source'],
        'is_old': False
    }

# =========== BATCH INSERT & CLEANUP ============
def _insert_hashes(batch):
    if not batch:
        return
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO image_hashes (site_id, filename, phash, source_id, is_old) "
                "VALUES (:site_id, :filename, :phash, :source_id, :is_old) "
                "ON CONFLICT DO NOTHING"
            ),
            batch
        )

def _remove_files(batch):
    base = IMAGE_DIR
    cnt = 0
    for rec in batch:
        try:
            p = (base / rec['filename']).resolve()
            if not str(p).startswith(str(base) + os.sep):
                continue
            if p.exists():
                p.unlink()
                cnt += 1
        except Exception:
            pass
    if cnt:
        log.info("Removed %s files after hashing from %s", cnt, base)

# ================ MAIN ==========================
def main():
    t0 = time.time()
    files = _load_candidates()
    if not files:
        log.info("No files to hash. Exit.")
        return {"requested": 0, "ok": 0, "fail": 0}

    total = len(files)
    processed_ok = 0

    i = 0
    while i < total:
        batch = files[i:i+BATCH_SIZE]
        results = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futs = [ex.submit(_hash_entry, e, HASH_SIZE, MAX_SIZE) for e in batch]
            for fut in as_completed(futs):
                r = fut.result()
                if r:
                    results.append(r)

        if results:
            _insert_hashes(results)
            _remove_files(results)
            processed_ok += len(results)

        i += BATCH_SIZE
        gc.collect()
        log.info("Batch %s done (%s/%s)", (i + BATCH_SIZE - 1) // BATCH_SIZE, min(i, total), total)

    elapsed = time.time() - t0
    log.info("Image hashing finished. Time: %.1f sec.", elapsed)
    return {"requested": total, "ok": processed_ok, "fail": total - processed_ok, "elapsed_s": round(elapsed, 1)}

if __name__ == "__main__":
    main()