
from __future__ import annotations
import os, sys, json, time, logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Callable

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from openai import OpenAI
from openai._exceptions import APIStatusError, APIConnectionError, RateLimitError

# ---------- LOGGING ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | geo_garbage_batchsync | %(message)s",
    handlers=[logging.StreamHandler(sys.stderr)],
    force=True,
)
log = logging.getLogger("geo_garbage_batchsync")

# ---------- CONFIG ----------
SCRIPT_DIR   = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
with open(PROJECT_ROOT / "config.json", "r", encoding="utf-8") as f:
    cfg = json.load(f)

def _cfg_get(*path, default=None):
    cur = cfg
    for k in path:
        if isinstance(cur, dict) and k in cur:
            cur = cur[k]
        else:
            return default
    return cur

DB_URL = f"postgresql+psycopg2://{cfg['USER']}:{cfg['PWD']}@{cfg['HOST']}:{cfg['PORT']}/{cfg['DB']}"
engine: Engine = create_engine(DB_URL, pool_pre_ping=True, future=True)

API_KEY = os.getenv("OPENAI_API_KEY") or _cfg_get("OPENAI_API_KEY") or _cfg_get("OPENAI","API_KEY")
if not API_KEY:
    print(json.dumps({"module":"geo_garbage_batchsync","ok":False,"error":"no_openai_api_key"}))
    sys.exit(1)
client = OpenAI(api_key=API_KEY)

PROMPT_TEMPLATE = (
    _cfg_get("PROMPTS","GEO_GARBAGE_TEMPLATE_V2")
    or _cfg_get("PROMPTS","CZ_ADDRESS_TEMPLATE_V2")
)
if not PROMPT_TEMPLATE:
    print(json.dumps({"module":"geo_garbage_batchsync","ok":False,"error":"no_prompt_in_config"}))
    sys.exit(1)

BATCH = _cfg_get("BATCH_GEO", default={}) or {}
CHUNK_SIZE   = int(BATCH.get("CHUNK_SIZE", 500))
POLL_SEC     = int(BATCH.get("POLL_SEC", 60))
MODEL        = BATCH.get("MODEL","gpt-4o-mini")
FILTER_NULL_IN_CZ = bool(BATCH.get("FILTER_NULL_IN_CZ", True))
MAX_ROWS     = BATCH.get("MAX_ROWS", None)
MAX_ACTIVE   = int(BATCH.get("MAX_ACTIVE", 7))
FAIL_STOP_N  = int(BATCH.get("FAIL_STOP_N", 8))

if MAX_ROWS is not None:
    try: MAX_ROWS = int(MAX_ROWS)
    except: MAX_ROWS = None

IO_DIR = SCRIPT_DIR / "geo_batch_io"
IN_DIR  = IO_DIR / "inputs"
OUT_DIR = IO_DIR / "outputs"
ERR_DIR = IO_DIR / "errors"
for d in (IN_DIR, OUT_DIR, ERR_DIR): d.mkdir(parents=True, exist_ok=True)

# ---------- RETRY ----------
def _retry(n: int, pause: float, fn: Callable, *args, **kwargs):
    last = None
    for i in range(1, n+1):
        try:
            return fn(*args, **kwargs)
        except (APIConnectionError, RateLimitError, APIStatusError) as e:
            last = e
            log.warning("API retry %d/%d after %.1fs: %s", i, n, pause, e)
            time.sleep(pause)
        except Exception as e:
            last = e
            log.warning("retry %d/%d after %.1fs: %s", i, n, pause, e)
            time.sleep(pause)
    raise last

# ---------- LOAD ----------
def _load_candidates(limit: Optional[int]=None) -> pd.DataFrame:
    where_extra = "AND g.in_cz IS NULL" if FILTER_NULL_IN_CZ else ""
    sql = f"""
        SELECT internal_id, district, city, city_part, street
        FROM silver.geo_garbage g
        WHERE g.geo_status IS FALSE
        {where_extra}
        ORDER BY g.internal_id
    """
    if limit:
        sql += f" LIMIT {int(limit)}"
    with engine.begin() as conn:
        conn.execute(text("SET LOCAL statement_timeout = '120s'"))
        df = pd.read_sql(sql, con=conn)
    return df

def _s(val) -> str:
    if pd.isna(val): return ""
    s = str(val).strip()
    return "" if s.lower() in {"none","nan","null","neuvedeno","neznámá"} else s

# ---------- SAFE PROMPT RENDER ----------
_VALID_KEYS = ("district","city","city_part","street")
_LEGACY_MAP = {
    "norm_district": "district",
    "norm_okres":    "district",
    "norm_city":     "city",
    "norm_city_part":"city_part",
    "norm_street":   "street",
}

def _render_prompt(template: str, values: Dict[str,str]) -> str:
    safe_tpl = template.replace("{", "{{").replace("}", "}}")
    for k in _VALID_KEYS:
        safe_tpl = safe_tpl.replace("{{"+k+"}}", "{"+k+"}")
    for old, new in _LEGACY_MAP.items():
        safe_tpl = safe_tpl.replace("{{"+old+"}}", "{"+new+"}")
    return safe_tpl.format(
        district=values.get("district",""),
        city=values.get("city",""),
        city_part=values.get("city_part",""),
        street=values.get("street",""),
    )

# ---------- JSONL ----------
def _write_jsonl_for_chunk(df: pd.DataFrame, path: Path) -> int:
    written = 0
    with open(path, "w", encoding="utf-8") as f:
        for _, r in df.iterrows():
            vals = {
                "district": _s(r.get("district")),
                "city": _s(r.get("city")),
                "city_part": _s(r.get("city_part")),
                "street": _s(r.get("street")),
            }
            prompt = _render_prompt(PROMPT_TEMPLATE, vals)
            job = {
                "custom_id": str(int(r["internal_id"])),
                "method": "POST",
                "url": "/v1/chat/completions",
                "body": {
                    "model": MODEL,
                    "messages": [{"role": "user", "content": prompt}],
                    "max_tokens": 200,
                    "temperature": 0
                }
            }
            f.write(json.dumps(job, ensure_ascii=False) + "\n")
            written += 1
    return written

def _upload_and_start(infile: Path) -> Tuple[str,str]:
    with open(infile, "rb") as fh:
        file_obj = _retry(3, 2.0, client.files.create, file=fh, purpose="batch")
    batch = _retry(3, 2.0, client.batches.create,
                   input_file_id=file_obj.id,
                   endpoint="/v1/chat/completions",
                   completion_window="24h")
    return batch.id, file_obj.id

def _get_batch(bid: str):
    return _retry(3, 2.0, client.batches.retrieve, bid)

def _download(file_id: str) -> bytes:
    return _retry(3, 2.0, lambda: client.files.content(file_id).read())

def _cancel_batch(bid: str):
    try:
        client.batches.cancel(bid)
        log.info("cancelled batch %s", bid)
    except Exception as e:
        log.warning("cancel batch %s failed: %s", bid, e)

# ---------- PARSE ----------
def _parse_choice(obj: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    try:
        body = (obj.get("response") or {}).get("body") or {}
        choices = body.get("choices") or []
        if not choices: return None
        content = choices[0]["message"]["content"]
        if isinstance(content, dict):
            return content
        if isinstance(content, str):
            s = content.strip()
            try:
                return json.loads(s)
            except Exception:
                return None
    except Exception:
        return None

def _coerce_bool(v):
    if v is None: return None
    s = str(v).strip().lower()
    if s in {"true","1","yes"}: return True
    if s in {"false","0","no"}: return False
    return None

# ---------- DB SAFETY ----------
def _ensure_index():
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS geo_garbage_internal_id_idx
            ON silver.geo_garbage (internal_id)
        """))

# ---------- APPLY (fixed JSONB binding) ----------
def _apply_rows_to_db(rows: List[Dict[str, Any]]) -> Tuple[int,int,int]:
    if not rows:
        return (0,0,0)

    data = []
    icz_true = icz_false = 0
    for it in rows:
        cid = int(it["custom_id"])
        p = it.get("parsed") or {}
        in_cz = _coerce_bool(p.get("in_cz"))
        if in_cz is True: icz_true += 1
        elif in_cz is False: icz_false += 1
        data.append({
            "internal_id":   cid,
            "norm_district": p.get("norm_district"),
            "norm_okres":    p.get("norm_okres"),
            "norm_city":     p.get("norm_city"),
            "norm_city_part":p.get("norm_city_part"),
            "norm_street":   p.get("norm_street"),
            "in_cz":         in_cz
        })

    payload_json = json.dumps(data, ensure_ascii=False).replace("'", "''")

    sql = f"""
        WITH src AS (
            SELECT
                (x->>'internal_id')::bigint     AS internal_id,
                NULLIF(x->>'norm_district','')  AS norm_district,
                NULLIF(x->>'norm_okres','')     AS norm_okres,
                NULLIF(x->>'norm_city','')      AS norm_city,
                NULLIF(x->>'norm_city_part','') AS norm_city_part,
                NULLIF(x->>'norm_street','')    AS norm_street,
                CASE
                  WHEN x ? 'in_cz' THEN
                    CASE LOWER(x->>'in_cz')
                      WHEN 'true'  THEN TRUE
                      WHEN '1'     THEN TRUE
                      WHEN 'yes'   THEN TRUE
                      WHEN 'false' THEN FALSE
                      WHEN '0'     THEN FALSE
                      WHEN 'no'    THEN FALSE
                      ELSE NULL
                    END
                  ELSE NULL
                END AS in_cz
            FROM jsonb_array_elements('{payload_json}'::jsonb) AS x
        )
        UPDATE silver.geo_garbage AS tgt
        SET norm_district  = COALESCE(src.norm_district,  tgt.norm_district),
            norm_okres     = COALESCE(src.norm_okres,     tgt.norm_okres),
            norm_city      = COALESCE(src.norm_city,      tgt.norm_city),
            norm_city_part = COALESCE(src.norm_city_part, tgt.norm_city_part),
            norm_street    = COALESCE(src.norm_street,    tgt.norm_street),
            in_cz          = COALESCE(src.in_cz,          tgt.in_cz),
            geo_status     = TRUE
        FROM src
        WHERE tgt.internal_id = src.internal_id;
    """

    with engine.begin() as conn:
        conn.execute(text("SET LOCAL statement_timeout = '120s'"))
        res = conn.execute(text(sql))
        updated = int(res.rowcount or 0)

    return (updated, icz_true, icz_false)

# ---------- MAIN ----------
def main():
    _ensure_index()
    t0 = time.perf_counter()
    summary = {
        "module": "geo_garbage_batchsync",
        "ok": True,
        "chunks": 0,
        "rows_total": 0,
        "rows_sent": 0,
        "rows_rcv": 0,
        "rows_updated": 0,
        "sanity_mismatches": 0,
        "in_cz_true": 0,
        "in_cz_false": 0,
        "in_cz_null": 0,
        "consecutive_failures": 0,
        "aborted_due_to_failures": False,
        "elapsed_s": 0.0
    }

    try:
        df = _load_candidates(limit=MAX_ROWS)
        total = len(df)
        summary["rows_total"] = int(total)
        if total == 0:
            log.info("no candidates: geo_status=FALSE%s", " AND in_cz IS NULL" if FILTER_NULL_IN_CZ else "")
            summary["elapsed_s"] = round(time.perf_counter() - t0, 3)
            print(json.dumps(summary, ensure_ascii=False)); sys.stdout.flush()
            return

        chunks: List[pd.DataFrame] = [df.iloc[i:i+CHUNK_SIZE] for i in range(0, total, CHUNK_SIZE)]
        summary["chunks"] = len(chunks)
        log.info("chunks=%d | chunk_size=%d", summary["chunks"], CHUNK_SIZE)

        queue: List[Tuple[int, pd.DataFrame]] = list(enumerate(chunks, start=1))
        active: Dict[str, Dict[str, Any]] = {}
        consec_fail = 0

        while queue or active:
            while queue and len(active) < MAX_ACTIVE:
                idx, cdf = queue.pop(0)
                in_file = IN_DIR / f"geo_{int(time.time()*1000)}_{idx:04d}.jsonl"
                rows = _write_jsonl_for_chunk(cdf, in_file)
                summary["rows_sent"] += int(rows)
                log.info("enqueue %d/%d | rows=%d | file=%s", idx, len(chunks), rows, in_file.name)
                bid, fid = _upload_and_start(in_file)
                try: in_file.unlink(missing_ok=True)
                except Exception: pass
                active[bid] = {"idx": idx, "rows": rows, "file_id": fid}
                log.info("enqueued batch=%s file_id=%s (active=%d)", bid, fid, len(active))

            if not active:
                break

            time.sleep(POLL_SEC)

            finished: List[str] = []
            for bid, meta in list(active.items()):
                b = _get_batch(bid)
                st = getattr(b, "status", "unknown")
                if st in ("completed","failed","expired","cancelled"):
                    meta["status"] = st
                    meta["output_file_id"] = getattr(b, "output_file_id", None)
                    meta["error_file_id"]  = getattr(b, "error_file_id",  None)
                    finished.append(bid)

            for bid in finished:
                meta = active.pop(bid)
                rows = meta["rows"]
                st   = meta.get("status","unknown")

                if st != "completed":
                    consec_fail += 1
                    summary["consecutive_failures"] = consec_fail
                    log.error("batch %s failed (status=%s) | consecutive_failures=%d", bid, st, consec_fail)
                    if consec_fail >= FAIL_STOP_N:
                        log.error("too many consecutive failures — aborting run")
                        summary["ok"] = False
                        summary["aborted_due_to_failures"] = True
                        queue.clear(); active.clear()
                        break
                    continue

                if consec_fail:
                    consec_fail = 0; summary["consecutive_failures"] = 0

                ofid = meta.get("output_file_id")
                if not ofid:
                    log.warning("completed but no output file | batch=%s", bid)
                    continue

                content = _download(ofid)
                parsed_rows: List[Dict[str,Any]] = []
                for line in content.splitlines():
                    try:
                        obj = json.loads(line.decode("utf-8") if isinstance(line,(bytes,bytearray)) else line)
                        cid = obj.get("custom_id")
                        parsed = _parse_choice(obj)
                        if cid and isinstance(parsed, dict):
                            parsed_rows.append({"custom_id": cid, "parsed": parsed})
                    except Exception:
                        continue

                summary["rows_rcv"] += int(len(parsed_rows))
                updated, icz_true, icz_false = _apply_rows_to_db(parsed_rows)
                summary["rows_updated"] += updated
                summary["in_cz_true"] += icz_true
                summary["in_cz_false"] += icz_false

            if summary["aborted_due_to_failures"]:
                break

        summary["in_cz_null"] = max(0, summary["rows_rcv"] - summary["in_cz_true"] - summary["in_cz_false"])
        summary["elapsed_s"] = round(time.perf_counter() - t0, 3)
        print(json.dumps(summary, ensure_ascii=False)); sys.stdout.flush()

    except Exception as e:
        log.exception("fatal")
        print(json.dumps({"module":"geo_garbage_batchsync","ok":False,"error":f"{e.__class__.__name__}: {e}"}))
        sys.stdout.flush(); sys.exit(1)

if __name__ == "__main__":
    main()