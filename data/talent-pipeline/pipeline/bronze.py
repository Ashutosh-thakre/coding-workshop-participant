"""Bronze layer: land source data in the lake unchanged, plus audit columns.

Rules of this layer:
  * No cleaning or filtering - Bronze is a faithful, replayable copy of the sources.
  * Every row gets audit columns (_ingested_at, _source_system, _source_object, _run_id).
  * Data is partitioned by ``ingest_date`` (the run date). Re-running the same date
    overwrites only that partition, so reruns never create duplicates.
  * Each write is verified by reading the partition back and comparing row counts.

Layout:  <lake>/bronze/<dataset>/ingest_date=YYYY-MM-DD/part-*.parquet
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pipeline.config import PipelineConfig
from pipeline.lineage import DatasetMetric
from pipeline.logging_utils import get_logger, log_step
from pipeline.sources import list_files, read_postgres_table, read_text_lines

logger = get_logger("bronze")

# Source tables in PostgreSQL (schema: public)
POSTGRES_TABLES: tuple[str, ...] = (
    "teams",
    "competencies",
    "employees",
    "projects",
    "performance_reviews",
    "competency_assessments",
    "talent_review",
)

# Datasets dropped as files into the landing zone (S3)
LANDING_DATASETS: tuple[str, ...] = ("learning_activity",)

PARTITION_COLUMN = "ingest_date"


class EmptySourceError(RuntimeError):
    """Raised when a source returns zero rows (likely an upstream failure)."""


class BronzeWriteError(RuntimeError):
    """Raised when the rows read back from Bronze don't match the rows written."""


def add_audit_columns(df: DataFrame, config: PipelineConfig, source_system: str, source_object: str) -> DataFrame:
    """Append lineage/audit columns and the ingest_date partition column."""
    return (
        df.withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_system", F.lit(source_system))
        .withColumn("_source_object", F.lit(source_object))
        .withColumn("_run_id", F.lit(config.run_id))
        .withColumn(PARTITION_COLUMN, F.lit(config.run_date))
    )


def write_bronze(spark: SparkSession, df: DataFrame, path: str, config: PipelineConfig, target_files: int = 1) -> int:
    """Write one ingest_date partition (idempotently) and verify it by reading it back.

    Args:
        target_files: Number of Parquet files per partition. The datasets are small,
            so one file avoids the "many small files" problem; raise it for big data.

    Returns:
        Number of rows in the written partition.
    """
    df.coalesce(target_files).write.mode("overwrite").partitionBy(PARTITION_COLUMN).parquet(path)

    written = (
        spark.read.parquet(path)
        .where(F.col(PARTITION_COLUMN) == F.lit(config.run_date))
        .count()
    )
    return written


def _ingest(
    spark: SparkSession,
    df: DataFrame,
    config: PipelineConfig,
    dataset: str,
    source_system: str,
    source_object: str,
) -> DatasetMetric:
    """Audit, write and reconcile one dataset; return its metrics."""
    rows_in = df.count()
    if rows_in == 0:
        raise EmptySourceError(f"{source_system}:{source_object} returned 0 rows - refusing to write an empty Bronze partition")

    target = f"{config.bronze_path}/{dataset}"
    rows_out = write_bronze(spark, add_audit_columns(df, config, source_system, source_object), target, config)

    if rows_out != rows_in:
        raise BronzeWriteError(f"{dataset}: read {rows_in} rows from source but {rows_out} rows in Bronze")

    logger.info("%-24s %6d rows  %s -> %s/%s=%s", dataset, rows_out, source_object, target, PARTITION_COLUMN, config.run_date)
    return DatasetMetric("bronze", dataset, f"{source_system}:{source_object}", target, rows_in, rows_out)


def ingest_postgres(spark: SparkSession, config: PipelineConfig) -> list[DatasetMetric]:
    """Copy each PostgreSQL source table into Bronze."""
    metrics = []
    for table in POSTGRES_TABLES:
        df = read_postgres_table(spark, config, table)
        metrics.append(_ingest(spark, df, config, table, "postgres", f"public.{table}"))
        df.unpersist()
    return metrics


def ingest_landing(spark: SparkSession, config: PipelineConfig) -> list[DatasetMetric]:
    """Copy each landing-zone dataset into Bronze as raw lines (one row per line)."""
    metrics = []
    for dataset in LANDING_DATASETS:
        source_dir = f"{config.landing_path}/{dataset}"
        files = list_files(spark, source_dir)
        empty_files = [path for path, size in files if size == 0]
        logger.info("%s: %d files found in %s", dataset, len(files), source_dir)
        for path in empty_files:
            # Not an error (nothing to lose), but an empty daily drop usually means an upstream export problem.
            logger.warning("%s: empty file (0 bytes) - check the source system for this day: %s", dataset, path)

        raw = read_text_lines(spark, source_dir)

        df = raw.select(F.col("value").alias("raw_line"), F.col("source_file").alias("_source_file"))
        metrics.append(_ingest(spark, df, config, dataset, "s3", source_dir))
        raw.unpersist()
    return metrics


def run_bronze(spark: SparkSession, config: PipelineConfig) -> list[DatasetMetric]:
    """Ingest all sources into the Bronze layer."""
    metrics: list[DatasetMetric] = []
    with log_step(logger, "ingest postgres tables"):
        metrics += ingest_postgres(spark, config)
    with log_step(logger, "ingest landing zone files"):
        metrics += ingest_landing(spark, config)
    return metrics
