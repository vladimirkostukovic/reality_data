#Runs all Sreality ETL steps, captures their sync_summary logs, and validates
# basic snapshot consistency.

from __future__ import annotations
import sys
import subprocess
import json
import time
from datetime import date
from pathlib import Path
from typing import List, Dict, Any

# -------------------- CONFIG --------------------

PROJECT_DIR = Path(__file__).resolve().parent

SCRIPTS = [
    "1_step.py",
    "sreality_typical.py",
    "price_change.py",
    "photo.py",
    "seller.py",
]


# -------------------- HELPERS --------------------

def _today() -> str:
    return date.today().isoformat()


def _parse_sync_summaries(stdout: str) -> List[dict]:
    out = []
    for line in stdout.splitlines():
        s = line.strip()
        if not s.startswith("{"):
            continue
        try:
            obj = json.loads(s)
            if isinstance(obj, dict) and obj.get("stage") == "sync_summary":
                stats = obj.get("stats", {})
                if stats:
                    out.append(stats)  # ← БЕЗ обёртки "stage"
        except Exception:
            continue
    return out


def run_script(path: Path) -> dict:
    t0 = time.perf_counter()
    proc = subprocess.run(
        [sys.executable, str(path)],
        cwd=path.parent,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    dur_ms = int((time.perf_counter() - t0) * 1000)
    summaries = _parse_sync_summaries(proc.stdout)

    return {
        "script": path.name,
        "returncode": proc.returncode,
        "duration_ms": dur_ms,
        "summaries": summaries,
        "stdout": proc.stdout,
        "stderr": proc.stderr,
    }


# -------------------- SANITY --------------------

def sanity_check(results: List[dict]) -> dict:
    today = _today()

    problems = []
    warnings = []

    st = {}
    typ = {}
    pr = {}
    ph = {}
    sl = {}

    # extract stats
    for r in results:
        if not r["summaries"]:
            continue
        stats = r["summaries"][-1]
        name = r["script"]

        if name.startswith("1_"):
            st = stats
        elif "typical" in name:
            typ = stats
        elif "price" in name:
            pr = stats
        elif "photo" in name:
            ph = stats
        elif "seller" in name:
            sl = stats

    # ---- DATE CONSISTENCY ----
    dates = {
        st.get("snapshot_date"),
        typ.get("snapshot_date"),
        pr.get("change_date"),
        ph.get("snapshot_date"),
        sl.get("snapshot_date"),
    }
    dates = {d for d in dates if d}

    date_ok = True
    if len(dates) > 1:
        problems.append(f"snapshot_date mismatch: {dates}")
        date_ok = False
    if dates and list(dates)[0] != today:
        problems.append(f"snapshot_date != today: {dates}")
        date_ok = False

    # ---- BASIC CHECKS ----
    math_ok = True

    if st.get("total_rows") and typ.get("total_rows"):
        if typ["total_rows"] > st["total_rows"]:
            problems.append(
                f"typical.total_rows({typ['total_rows']}) > standart.total_rows({st['total_rows']})"
            )
            math_ok = False

    # ---- PRICE ----
    price_ok = True
    if pr:
        if pr.get("inserted_new", 0) > pr.get("candidates", 0):
            problems.append("price.inserted_new > candidates")
            price_ok = False

    # ---- PHOTO ----
    photo_ok = True
    if ph and st:
        if ph.get("source_rows") and st.get("source_rows"):
            if ph["source_rows"] != st["source_rows"]:
                warnings.append(
                    f"photo.source_rows != standart.source_rows ({ph['source_rows']} vs {st['source_rows']})"
                )
                photo_ok = False

    # ---- SELLER ----
    seller_ok = True
    if sl and st:
        if sl.get("source_rows") and st.get("source_rows"):
            if sl["source_rows"] != st["source_rows"]:
                warnings.append(
                    f"seller.source_rows != standart.source_rows ({sl['source_rows']} vs {st['source_rows']})"
                )
                seller_ok = False

        if sl.get("inserted_new", 0) > sl.get("source_rows", 0):
            problems.append("seller.inserted_new > source_rows")
            seller_ok = False

    # ---- FINAL ----
    if problems:
        status = "error"
    elif warnings:
        status = "warn"
    else:
        status = "ok"

    return {
        "status": status,
        "date_ok": date_ok,
        "math_ok": math_ok,
        "price_ok": price_ok,
        "photo_ok": photo_ok,
        "seller_ok": seller_ok,
        "problems": problems,
        "warnings": warnings,
    }


# -------------------- MAIN --------------------

def main():
    results = []

    for name in SCRIPTS:
        p = PROJECT_DIR / name
        if not p.exists():
            results.append({
                "script": name,
                "returncode": 127,
                "duration_ms": 0,
                "summaries": [],
                "error": "not_found",
            })
            continue

        results.append(run_script(p))

    san = sanity_check(results)

    out = {
        "date": _today(),
        "status": san["status"],
        "results": [
            {
                "script": r["script"],
                "returncode": r["returncode"],
                "duration_ms": r["duration_ms"],
                "summaries": r["summaries"],
            }
            for r in results
        ],
        "sanity": san,
    }

    print(json.dumps(out, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()