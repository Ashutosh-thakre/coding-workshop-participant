"""Talent pipeline entry point.

Orchestration only: load config, start logging and Spark, run the requested
steps in order, always stop Spark, and exit non-zero on failure.

Usage:
    source data/talent-pipeline/env.local
    python data/talent-pipeline/job.py                 # run all steps
    python data/talent-pipeline/job.py --steps check   # run selected steps (comma-separated)
"""

from __future__ import annotations

import argparse
import logging
import sys
from collections.abc import Callable

from pyspark.sql import SparkSession

from pipeline.bronze import run_bronze
from pipeline.config import ConfigError, PipelineConfig
from pipeline.healthcheck import run_healthcheck
from pipeline.lineage import DatasetMetric
from pipeline.logging_utils import get_logger, log_step, setup_logging
from pipeline.spark_session import build_spark

# Ordered registry of pipeline steps; later phases add silver and gold.
# A step may return dataset metrics, which are collected for the run summary.
STEPS: dict[str, Callable[[SparkSession, PipelineConfig], list[DatasetMetric] | None]] = {
    "check": run_healthcheck,
    "bronze": run_bronze,
}


def log_summary(logger: logging.LoggerAdapter, metrics: list[DatasetMetric]) -> None:
    """Log a reconciliation table: rows in / out / rejected / dropped per dataset."""
    if not metrics:
        return
    logger.info("%-7s %-24s %8s %8s %9s %8s", "layer", "dataset", "rows_in", "rows_out", "rejected", "dropped")
    for m in metrics:
        logger.info(
            "%-7s %-24s %8d %8d %9d %8d",
            m.layer, m.dataset, m.rows_in, m.rows_out, m.rows_rejected, m.rows_dropped,
        )


def parse_steps(argv: list[str] | None = None) -> list[str]:
    """Parse --steps into an ordered list of known step names."""
    parser = argparse.ArgumentParser(description="ACME talent pipeline (Bronze -> Silver -> Gold)")
    parser.add_argument(
        "--steps",
        default="all",
        help=f"Comma-separated steps to run, or 'all'. Available: {', '.join(STEPS)}",
    )
    requested = parser.parse_args(argv).steps
    if requested == "all":
        return list(STEPS)
    steps = [s.strip() for s in requested.split(",") if s.strip()]
    unknown = [s for s in steps if s not in STEPS]
    if unknown:
        parser.error(f"unknown step(s): {', '.join(unknown)}. Available: {', '.join(STEPS)}")
    return [s for s in STEPS if s in steps]  # always run in pipeline order


def main(argv: list[str] | None = None) -> int:
    """Run the pipeline and return a process exit code (0 = success)."""
    steps = parse_steps(argv)

    try:
        config = PipelineConfig.from_env()
    except ConfigError as exc:
        print(f"CONFIG ERROR: {exc}", file=sys.stderr)
        return 2

    setup_logging(config.run_id)
    logger = get_logger("main")
    logger.info(
        "Pipeline starting: steps=%s run_date=%s lake=%s is_local=%s",
        steps, config.run_date, config.lake_root, config.is_local,
    )

    spark = build_spark(config)
    metrics: list[DatasetMetric] = []
    try:
        with log_step(logger, f"pipeline {steps}"):
            for step in steps:
                metrics += STEPS[step](spark, config) or []
    except Exception:  # already logged with traceback by log_step
        log_summary(logger, metrics)
        logger.error("Pipeline FAILED")
        return 1
    finally:
        spark.stop()

    log_summary(logger, metrics)
    logger.info("Pipeline SUCCEEDED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
