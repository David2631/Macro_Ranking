"""Small utilities for fetchers: timing, retry/backoff, and cache key generation."""
import time
import hashlib
import json
from typing import Callable, Any


def time_ms():
    return int(time.time() * 1000)


def simple_backoff_retry(fn: Callable[..., Any], attempts: int = 3, base_delay: float = 0.5) -> Any:
    """Call fn with simple exponential backoff. Returns fn() result or raises the last exception.

    This is intentionally simple: it retries on any Exception. The previous
    implementation allowed passing an exceptions tuple which made static typing
    unhappy; keeping a single Exception class keeps behavior predictable and
    mypy-friendly.
    """
    last_exc: Exception | None = None
    for i in range(attempts):
        try:
            return fn()
        except Exception as e:
            last_exc = e
            delay = base_delay * (2 ** i)
            time.sleep(delay)
    # Re-raise the last caught exception
    if last_exc is None:
        raise RuntimeError("simple_backoff_retry failed without capturing exception")
    raise last_exc


def cache_key_for_sdmx(source: str, resource: str, key: str, start: str, end: str):
    payload = json.dumps({"source": source, "resource": resource, "key": key, "start": start, "end": end}, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
