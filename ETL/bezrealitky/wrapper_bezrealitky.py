from __future__ import annotations
import sys
import json
import subprocess
import time
from datetime import datetime, date
from pathlib import Path
from typing import List, Dict, Any

# --------------------CONFIG--------------------------

PROJECT_DIR = Path(__file__).resolve().parent

SCRIPTS = [
    "1_step.py",
    "bezrealitky_typical.py",
    "price.py",
    "photo.py",
]


# -----------------------HELPERS------------------------------

def _today() -> str:
    return date.today().isoformat()


def _parse_sync_summaries(stdout: str) -> List[dict]:
    out = []
    for line in stdout.splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            obj = json.loads(line)
            if isinstance(obj, dict) and obj.get("stage") == "sync_summary":
                out.append(obj)
        except Exception:
            continue
    return out


def run_script(path: Path) -> dict:
    t0 = time.perf_counter()

    proc = subprocess.run(
        [sys.executable, str(path)],
        cwd=path.parent,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
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


# ----------------SANITY CHECKS---------------

def sanity_check(results: List[dict]) -> dict:
    today = _today()

    problems = []
    warnings = []

    # --- extract stats ---
    st = {}      # from 1_step
    typ = {}     # from typical
    pr = {}      # from price
    ph = {}      # from photo

    for r in results:
        if not r["summaries"]:
            continue
        stats = r["summaries"][0]["stats"]

        if r["script"].startswith("1_"):
            st = stats
        elif "typical" in r["script"]:
            typ = stats
        elif "price" in r["script"]:
            pr = stats
        elif "photo" in r["script"]:
            ph = stats

    # ---------------- DATE CHECK -----------------
    snapshots = set()
    for block in (st, typ, pr, ph):
        sd = block.get("snapshot_date")
        if sd:
            snapshots.add(sd)

    date_ok = True
    if len(snapshots) > 1:
        date_ok = False
        problems.append(f"snapshot_date mismatch across steps: {snapshots}")

    if snapshots and list(snapshots)[0] != today:
        date_ok = False
        problems.append(f"snapshot_date != today: {snapshots}")

    # ---------------- MATH CHECK -----------------
    math_ok = True

    # 1) total_rows sanity
    st_total = st.get("total_rows")
    typ_total = typ.get("total_rows")

    if st_total and typ_total:
        if typ_total > st_total:
            math_ok = False
            problems.append(f"typical.total_rows({typ_total}) > step1.total_rows({st_total})")

    # 2) added rows match (loose threshold)
    st_new = st.get("new_rows")
    typ_new = typ.get("new_rows")

    if st_new is not None and typ_new is not None:
        drift = abs(st_new - typ_new)
        if drift > 50:
            warnings.append(f"new_rows drift: step1={st_new}, typical={typ_new}, drift={drift}")

    # ---------------- PRICE CHECK -----------------
    price_ok = True

    if pr:
        prev_rows = pr.get("prev_rows")
        curr_rows = pr.get("curr_rows")

        if prev_rows and curr_rows:
            if abs(prev_rows - curr_rows) > 100:
                warnings.append(f"price snapshot row drift too high: prev={prev_rows}, curr={curr_rows}")

        if pr.get("inserted_new") > pr.get("candidates", 0):
            price_ok = False
            problems.append("price.inserted_new > candidates (logically impossible)")

    # ---------------- PHOTO CHECK ------------------
    photo_ok = True

    if ph and st:
        if ph.get("source_rows") and st.get("source_rows"):
            if ph["source_rows"] != st["source_rows"]:
                photo_ok = False
                problems.append(f"photo.source_rows({ph['source_rows']}) != step1.source_rows({st['source_rows']})")

        if ph.get("inserted_new") > ph.get("source_rows", 0):
            photo_ok = False
            problems.append("photo.inserted_new > source_rows")

    # ---------------- OVERALL STATUS ----------------

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
        "problems": problems,
        "warnings": warnings
    }

# -----------------------------------MAIN-----------------------------

def main():
    all_results = []

    for name in SCRIPTS:
        p = PROJECT_DIR / name
        if not p.exists():
            all_results.append({
                "script": name,
                "returncode": 127,
                "error": "not_found",
                "summaries": [],
            })
            continue

        res = run_script(p)
        all_results.append(res)

    san = sanity_check(all_results)

    # ---- final JSON out ----
    final = {
        "date": _today(),
        "results": [
            {
                "script": r["script"],
                "returncode": r["returncode"],
                "duration_ms": r.get("duration_ms"),
                "summaries": r.get("summaries", []),
            }
            for r in all_results
        ],
        "sanity": san,
    }

    print(json.dumps(final, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()