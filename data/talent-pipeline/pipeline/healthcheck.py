"""Connectivity check: can this run read both sources and write to the lake?

Run it before the ETL steps (``python job.py --steps check``) so connection
problems are reported clearly, separate from data or transformation bugs.
"""

from __future__ import annotations

from pyspark.sql import SparkSession

from pipeline.config import PipelineConfig
from pipeline.sources import read_postgres_table, read_text_lines
from pipeline.logging_utils import get_logger, log_step

logger = get_logger("check")


def run_healthcheck(spark: SparkSession, config: PipelineConfig) -> None:
    """Read PostgreSQL, read the S3 landing zone, and round-trip a file through the lake.

    Raises:
        RuntimeError: if the lake write/read round-trip returns unexpected data.
    """
    with log_step(logger, "postgres read"):
        employees = read_postgres_table(spark, config, "employees")
        logger.info("Postgres %s: employees rows=%d", config.postgres.host, employees.count())

    with log_step(logger, "landing zone read"):
        events = read_text_lines(spark, f"{config.landing_path}/learning_activity")
        logger.info("Landing zone %s: learning_activity lines=%d", config.landing_path, events.count())

    with log_step(logger, "lake write/read round-trip"):
        # No leading '_' or '.': Spark treats such paths as hidden and skips them on read.
        probe_path = f"{config.lake_root}/healthcheck"
        spark.createDataFrame([(config.run_id,)], ["run_id"]).write.mode("overwrite").parquet(probe_path)
        read_back = [row.run_id for row in spark.read.parquet(probe_path).collect()]
        if read_back != [config.run_id]:
            raise RuntimeError(f"Lake round-trip mismatch at {probe_path}: wrote {config.run_id}, read {read_back}")
        logger.info("Lake %s is writable", config.lake_root)
