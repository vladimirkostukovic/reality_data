import os
import sys
import json
import random
from pathlib import Path
from datetime import datetime
from collections import defaultdict, Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlsplit, urlunsplit
import time
import logging

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from sqlalchemy import create_engine, text

# =============== LOGGING (stderr only) ===============
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | downloader | %(message)s",
    handlers=[logging.StreamHandler(sys.stderr)],
    force=True,
)
log = logging.getLogger("downloader")

# =============== CONFIG & DB =========================
PROJECT_ROOT = Path(__file__).resolve().parents[1]
with open(PROJECT_ROOT / "config.json", "r", encoding="utf-8") as f:
    cfg = json.load(f)

DB_URL = f"postgresql+psycopg2://{cfg['USER']}:{cfg['PWD']}@{cfg['HOST']}:{cfg['PORT']}/{cfg['DB']}"
engine = create_engine(DB_URL, pool_pre_ping=True)

# Каталог для сохранения изображений
_base = Path(__file__).resolve().parent
_cfg_dir = Path(cfg.get("local_image_dir", "images"))
LOCAL_IMAGE_DIR = str(_cfg_dir if _cfg_dir.is_absolute() else _base / _cfg_dir)
os.makedirs(LOCAL_IMAGE_DIR, exist_ok=True)
log.info("Saving images to: %s", LOCAL_IMAGE_DIR)

# --- Настройки ---
BATCH_SIZE = int(os.getenv("IMG_BATCH_SIZE", "100"))
MAX_WORKERS = int(os.getenv("IMG_MAX_WORKERS", "2"))

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/124.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Version/17.5 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (X11; Linux x86_64) Chrome/124.0 Safari/537.36",
]

REFERERS = {
    "sreality": "https://www.sreality.cz/",
    "bezrealitky": "https://www.bezrealitky.cz/",
}

COMMON_HEADERS = {
    "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
    "Accept-Language": "cs-CZ,cs;q=0.9,en;q=0.7",
    "Connection": "keep-alive",
}


# =============== HTTP session builder ===============
def _build_session(pool_maxsize: int) -> requests.Session:
    sess = requests.Session()
    retry = Retry(
        total=4, connect=4, read=4, status=4,
        backoff_factor=0.4,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "HEAD"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_maxsize=pool_maxsize)
    sess.mount("http://", adapter)
    sess.mount("https://", adapter)
    sess.headers.update(COMMON_HEADERS)

    # Прогрев cookie (иначе sdn.cz даёт 401)
    for warm in ("https://www.sreality.cz/", "https://www.bezrealitky.cz/"):
        try:
            sess.get(warm, timeout=10)
        except Exception:
            pass
    return sess


# =============== DB util ===============
def _batch_insert(recs):
    if not recs:
        return
    sql = """
      INSERT INTO image_downloaded
        (site_id, source, urls, filenames, downloaded, download_date)
      VALUES (:site_id, :source, :urls, :filenames, :downloaded, :download_date)
    """
    payload = [{
        "site_id": r["site_id"],
        "source": r["source"],
        "urls": json.dumps(r["urls"]),
        "filenames": json.dumps(r["filenames"]) if r["filenames"] else None,
        "downloaded": r["downloaded"],
        "download_date": r["download_date"],
    } for r in recs]
    with engine.begin() as conn:
        conn.execute(text(sql), payload)


def _get_downloaded_site_ids():
    with engine.connect() as conn:
        res = conn.execute(text("SELECT DISTINCT site_id FROM image_downloaded WHERE downloaded = true"))
        return set(str(row[0]) for row in res)


# =============== Helpers ===============
def _chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def _strip_query(u: str) -> str:
    p = urlsplit(u)
    return urlunsplit((p.scheme, p.netloc, p.path, "", ""))


def _ext_from_ct(ct: str) -> str:
    ct = (ct or "").lower()
    if "image/webp" in ct:
        return "webp"
    if "image/png" in ct:
        return "png"
    return "jpg"


# =============== Downloader core ===============
def _download_one(task, local_dir, session: requests.Session):
    site_id, source, url, idx = task
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Referer": REFERERS.get(source, "https://www.seznam.cz/"),
        **COMMON_HEADERS,
    }
    try:
        r = session.get(url, timeout=20, headers=headers, stream=True, allow_redirects=True)
        code = r.status_code

        # Unauthorized/Forbidden/Not Found — пробуем без query
        if code in (401, 403, 404) and "?" in url:
            url2 = _strip_query(url)
            r2 = session.get(url2, timeout=20, headers=headers, stream=True, allow_redirects=True)
            code = r2.status_code
            if code >= 400:
                return (site_id, source, url2, None, False, f"http_{code}")
            r = r2

        if code >= 400:
            return (site_id, source, url, None, False, f"http_{code}")

        ct = (r.headers.get("Content-Type") or "").lower()
        if "image" not in ct:
            return (site_id, source, url, None, False, f"bad_ct_{ct[:30]}")

        ext = _ext_from_ct(ct)
        fname = f"{site_id}_{source}_{idx}.{ext}"
        os.makedirs(local_dir, exist_ok=True)
        local_path = os.path.join(local_dir, fname)

        with open(local_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=65536):
                if chunk:
                    f.write(chunk)

        time.sleep(random.uniform(0.01, 0.05))
        return (site_id, source, url, fname, True, "")
    except requests.exceptions.RequestException as e:
        return (site_id, source, url, None, False, type(e).__name__)


# =============== MAIN ===============
def main():
    log.info("Starting image downloader...")
    downloaded_ids = _get_downloaded_site_ids()
    tasks = []
    source_stats = defaultdict(int)

    # --- SREALITY ---
    with engine.connect() as conn:
        res = conn.execute(text("SELECT id, images FROM sreality_photo"))
        for row in res:
            sid, images_json = str(row[0]), row[1]
            if sid in downloaded_ids:
                continue
            try:
                urls = json.loads(images_json)
                if not isinstance(urls, list):
                    continue
            except Exception:
                continue
            for idx, url in enumerate(urls[:5], 1):
                tasks.append((sid, "sreality", url, idx))
                source_stats["sreality"] += 1

    # --- BEZREALITKY ---
    with engine.connect() as conn:
        res = conn.execute(text("SELECT id, images FROM bezrealitky_photo"))
        for row in res:
            sid, images_json = str(row[0]), row[1]
            if sid in downloaded_ids:
                continue
            try:
                images = json.loads(images_json)
                if not isinstance(images, list):
                    continue
            except Exception:
                continue
            for idx, obj in enumerate(images[:5], 1):
                url = obj.get("url") if isinstance(obj, dict) else None
                if not url:
                    continue
                tasks.append((sid, "bezrealitky", url, idx))
                source_stats["bezrealitky"] += 1

    log.info("TOTAL TASKS: %s | by source: %s", len(tasks), dict(source_stats))
    if not tasks:
        log.warning("No tasks to download. Check sources or image_downloaded table.")
        return {"requested": 0, "ok": 0, "fail": 0}

    session = _build_session(MAX_WORKERS)
    total_ok = defaultdict(int)
    total_fail = defaultdict(int)

    try:
        for batch_id, batch in enumerate(_chunks(tasks, BATCH_SIZE), 1):
            results = []
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
                futs = [ex.submit(_download_one, t, LOCAL_IMAGE_DIR, session) for t in batch]
                for fut in as_completed(futs):
                    results.append(fut.result())

            by_source = defaultdict(list)
            for res in results:
                by_source[res[1]].append(res)

            ok_count = sum(1 for src in by_source for r in by_source[src] if r[4])
            fail_count = sum(1 for src in by_source for r in by_source[src] if not r[4])
            reasons = Counter([r[5] for src in by_source for r in by_source[src] if not r[4]])
            log.info("[BATCH %s] OK: %s | FAIL: %s | reasons: %s",
                     batch_id, ok_count, fail_count, dict(reasons.most_common(5)))

            to_insert = [{
                "site_id": r[0],
                "source": r[1],
                "urls": [r[2]],
                "filenames": [r[3]] if r[3] else None,
                "downloaded": r[4],
                "download_date": datetime.now(),
            } for src in by_source for r in by_source[src]]

            for src in by_source:
                total_ok[src] += sum(1 for r in by_source[src] if r[4])
                total_fail[src] += sum(1 for r in by_source[src] if not r[4])

            _batch_insert(to_insert)
    finally:
        session.close()

    log.info(
        "FINISH total=%s, success=%s, fail=%s",
        sum(total_ok.values()) + sum(total_fail.values()),
        sum(total_ok.values()),
        sum(total_fail.values()),
    )
    return {"requested": len(tasks), "ok": sum(total_ok.values()), "fail": sum(total_fail.values())}


if __name__ == "__main__":
    main()
