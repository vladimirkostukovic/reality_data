from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from contextlib import contextmanager
import os

DEFAULT_IO_WORKERS = min(32, (os.cpu_count() or 4) * 5)
DEFAULT_CPU_WORKERS = max(1, (os.cpu_count() or 2) - 1)

@contextmanager
def executor(kind: str, max_workers: int | None = None):
    if kind not in {"io", "cpu"}:
        raise ValueError('executor kind must be "io" or "cpu"')
    if kind == "io":
        pool = ThreadPoolExecutor(max_workers=max_workers or DEFAULT_IO_WORKERS, thread_name_prefix="io")
    else:
        pool = ProcessPoolExecutor(max_workers=max_workers or DEFAULT_CPU_WORKERS)
    try:
        yield pool
    finally:
        pool.shutdown(wait=True, cancel_futures=True)