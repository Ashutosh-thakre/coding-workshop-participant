"""Structured logging shared by every pipeline step.

Each log line carries the run id and the step name, so a single run can be
followed end-to-end with ``grep run=<id>``:

    2026-10-14 16:30:01 | INFO    | run=20261014T163000Z-a1b2c3 | bronze | Wrote 2000 rows
"""

from __future__ import annotations

import logging
import sys
import time
from collections.abc import Iterator
from contextlib import contextmanager

LOG_FORMAT = "%(asctime)s | %(levelname)-7s | run=%(run_id)s | %(step)s | %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


class _ContextFilter(logging.Filter):
    """Inject run_id and a default step name into every log record."""

    def __init__(self, run_id: str) -> None:
        super().__init__()
        self.run_id = run_id

    def filter(self, record: logging.LogRecord) -> bool:
        record.run_id = self.run_id
        if not hasattr(record, "step"):
            record.step = "main"
        return True


def setup_logging(run_id: str, level: int = logging.INFO) -> logging.Logger:
    """Configure the 'talent_pipeline' logger to write structured lines to stdout.

    Safe to call more than once (e.g. in tests): existing handlers are replaced.
    """
    logger = logging.getLogger("talent_pipeline")
    logger.setLevel(level)
    logger.propagate = False
    logger.handlers.clear()

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(LOG_FORMAT, DATE_FORMAT))
    handler.addFilter(_ContextFilter(run_id))
    logger.addHandler(handler)

    # Spark's Python bridge (py4j) is very chatty at INFO; keep only real problems.
    logging.getLogger("py4j").setLevel(logging.WARNING)
    return logger


def get_logger(step: str) -> logging.LoggerAdapter:
    """Return a logger that tags every message with the given step name."""
    return logging.LoggerAdapter(logging.getLogger("talent_pipeline"), {"step": step})


@contextmanager
def log_step(logger: logging.LoggerAdapter, description: str) -> Iterator[None]:
    """Log start, end, duration and status of a block of work.

    Exceptions are logged with their traceback and then re-raised, so the caller
    still decides whether the pipeline stops.
    """
    logger.info("START %s", description)
    started = time.perf_counter()
    try:
        yield
    except Exception:
        logger.exception("FAILED %s after %.2fs", description, time.perf_counter() - started)
        raise
    logger.info("DONE %s in %.2fs", description, time.perf_counter() - started)
