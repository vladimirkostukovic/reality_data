import sys
import json
import time
import logging
import calendar
from pathlib import Path
from importlib import import_module
from datetime import datetime

# Логи только в stderr, stdout чистый JSON отчета
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stderr)],
    force=True,
)
log = logging.getLogger("Wrapper")

# Базовый порядок шагов
DEFAULT_STEPS = [
    ("downloader", "image_downloader", "main"),
    ("processor",  "image_processor",  "main"),
    ("compare",    "phash_compare",    "main"),
]

# cleanup выполняем только 1-го числа месяца
CLEANUP_STEP = ("cleanup", "cleanup_image_downloaded", "main")

def run_step(alias: str, module_name: str, func_name: str) -> dict:
    t0 = time.perf_counter()
    mod = import_module(module_name)
    fn  = getattr(mod, func_name)
    stats = fn() or {}
    stats["elapsed_s"] = stats.get("elapsed_s") or round(time.perf_counter() - t0, 3)
    return stats

def normalize_steps(order_csv: str | None) -> list[tuple[str, str, str]]:
    if not order_csv:
        steps = DEFAULT_STEPS.copy()
    else:
        map_alias = {a: (a, m, f) for a, m, f in DEFAULT_STEPS}
        result = []
        seen = set()
        for raw in order_csv.split(","):
            a = raw.strip().lower()
            if not a or a in seen or a not in map_alias:
                continue
            result.append(map_alias[a])
            seen.add(a)
        steps = result or DEFAULT_STEPS

    # если сегодня первое число месяца — добавить cleanup
    today = datetime.now()
    if today.day == 1:
        steps.append(CLEANUP_STEP)
    return steps

def main() -> None:
    order_env = None
    steps = normalize_steps(order_env)
    report = {"started_at": time.strftime("%Y-%m-%d %H:%M:%S"), "steps": []}
    ok = True
    for alias, module_name, func_name in steps:
        try:
            stats = run_step(alias, module_name, func_name)
            report["steps"].append({"alias": alias, "ok": True, "stats": stats})
        except Exception as e:
            ok = False
            report["steps"].append({
                "alias": alias,
                "ok": False,
                "error": f"{type(e).__name__}: {e}"
            })
    report["ok"] = ok
    sys.stdout.write(json.dumps(report, ensure_ascii=False))
    sys.stdout.flush()

if __name__ == "__main__":
    main()