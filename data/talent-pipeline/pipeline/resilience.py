"""Retry helper for calls to external systems (PostgreSQL, S3).

Transient failures such as a dropped connection or a throttled request often
succeed on a second try. ``retry`` re-runs a function with exponential backoff
(base, 2*base, 4*base, ... seconds) and re-raises the last error if every
attempt fails, so real outages still stop the pipeline.
"""

from __future__ import annotations

import functools
import logging
import time
from collections.abc import Callable
from typing import ParamSpec, TypeVar

P = ParamSpec("P")
R = TypeVar("R")

_logger = logging.LoggerAdapter(logging.getLogger("talent_pipeline"), {"step": "retry"})


def retry(
    attempts: int = 3,
    base_delay: float = 2.0,
    exceptions: tuple[type[BaseException], ...] = (Exception,),
    sleep: Callable[[float], None] = time.sleep,
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """Decorate a function so it is retried on failure.

    Args:
        attempts: Total number of tries (1 = no retry).
        base_delay: Seconds to wait after the first failure; doubles each time.
        exceptions: Only these exception types trigger a retry.
        sleep: Injected for tests so they don't actually wait.
    """
    if attempts < 1:
        raise ValueError("attempts must be >= 1")

    def decorator(func: Callable[P, R]) -> Callable[P, R]:
        @functools.wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            for attempt in range(1, attempts + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as exc:
                    if attempt == attempts:
                        _logger.error("%s failed after %d attempt(s): %s", func.__name__, attempts, exc)
                        raise
                    delay = base_delay * 2 ** (attempt - 1)
                    _logger.warning(
                        "%s failed (attempt %d/%d): %s - retrying in %.1fs",
                        func.__name__, attempt, attempts, exc, delay,
                    )
                    sleep(delay)
            raise AssertionError("unreachable")  # loop always returns or raises

        return wrapper

    return decorator
