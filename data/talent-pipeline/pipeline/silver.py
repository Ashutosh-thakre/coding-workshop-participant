"""Silver layer: validated, de-duplicated, conformed data.

For each table (parents before children):
  1. Read the Bronze partition for the run date.
  2. Validate every row against its Pydantic contract (JSON is parsed first for
     landing datasets; known schema drift such as 'notes' -> 'text' is absorbed).
  3. Drop duplicate primary keys (keep one, deterministic).
  4. Check foreign keys against the parent table's *valid* rows.
  5. Add light, entity-level derived columns (e.g. event_year).
  6. Write Silver (full refresh) and the rejected rows to quarantine, with reasons.
  7. Reconcile: rows_in == rows_out + rows_quarantined + duplicates_removed.

Layout:  <lake>/silver/<table>/[event_year=YYYY/]part-*.parquet
         <lake>/quarantine/<table>/run_date=YYYY-MM-DD/part-*.parquet
"""

from __future__ import annotations

from collections.abc import Callable

from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pipeline.bronze import PARTITION_COLUMN
from pipeline.config import PipelineConfig
from pipeline.lineage import DatasetMetric
from pipeline.logging_utils import get_logger, log_step
from pipeline.schemas import CONTRACTS, TableContract
from pipeline.validation import (
    DQ_ERRORS,
    DQ_RAW,
    DQ_STAGE,
    check_foreign_keys,
    count_conflicting_duplicates,
    deduplicate,
    validate_rows,
)

logger = get_logger("silver")

JSON_COLUMN = "raw_line"  # Bronze column holding the raw JSON line for landing datasets
LINEAGE_COLUMNS = ("_source_system", "_source_object", "_source_file", "_ingested_at", "_bronze_run_id")
QUARANTINE_COLUMNS = (DQ_STAGE, DQ_ERRORS, DQ_RAW)

# Silver tables partitioned on disk (others are small enough for a single file)
PARTITION_BY: dict[str, list[str]] = {"learning_activity": ["event_year"]}


class MissingBronzeError(RuntimeError):
    """Raised when the Bronze partition for the run date is missing or empty."""


class ReconciliationError(RuntimeError):
    """Raised when rows in != rows out + rows quarantined + duplicates removed."""


# --- Light, entity-level conformance (business metrics belong in Gold) ----------

def _conform_performance_reviews(df: DataFrame) -> DataFrame:
    """Split '2024-H2' into period_year=2024, period_half=2 for sorting and trends."""
    return (
        df.withColumn("period_year", F.substring("review_period", 1, 4).cast("int"))
        .withColumn("period_half", F.substring("review_period", 7, 1).cast("int"))
    )


def _conform_learning_activity(df: DataFrame) -> DataFrame:
    """Add the calendar day and year of each event (year is the partition column)."""
    return df.withColumn("event_day", F.to_date("event_date")).withColumn("event_year", F.year("event_date"))


CONFORM: dict[str, Callable[[DataFrame], DataFrame]] = {
    "performance_reviews": _conform_performance_reviews,
    "learning_activity": _conform_learning_activity,
}


# --- I/O ----------------------------------------------------------------------------

def read_bronze_partition(spark: SparkSession, config: PipelineConfig, table: str) -> DataFrame:
    """Read the Bronze partition for the run date; rename Bronze's _run_id to _bronze_run_id."""
    path = f"{config.bronze_path}/{table}"
    try:
        df = spark.read.parquet(path)
    except AnalysisException as exc:
        raise MissingBronzeError(f"No Bronze data at {path} - run the bronze step first") from exc
    return (
        df.where(F.col(PARTITION_COLUMN) == F.lit(config.run_date))
        .drop(PARTITION_COLUMN)
        .withColumnRenamed("_run_id", "_bronze_run_id")
    )


def write_silver(df: DataFrame, path: str, partition_by: list[str]) -> None:
    """Full refresh of a Silver table (static overwrite replaces every partition)."""
    out = df.repartition(*partition_by) if partition_by else df.coalesce(1)
    writer = out.write.mode("overwrite").option("partitionOverwriteMode", "static")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.parquet(path)


def write_quarantine(df: DataFrame, path: str) -> None:
    """Replace this run date's quarantine folder for a table (idempotent on rerun)."""
    df.coalesce(1).write.mode("overwrite").parquet(path)


# --- Per-table processing ---------------------------------------------------------

def _log_schema_drift(bronze: DataFrame, contract: TableContract) -> None:
    """Warn about rows that used a renamed (drifted) field, e.g. 'notes' instead of 'text'."""
    if JSON_COLUMN not in bronze.columns:
        return
    for source, target in contract.renames.items():
        drifted = bronze.where(F.get_json_object(JSON_COLUMN, f"$.{source}").isNotNull()).count()
        if drifted:
            logger.warning("%s: schema drift - %d rows use '%s' instead of '%s' (renamed)", contract.name, drifted, source, target)


def _log_top_reasons(quarantined: DataFrame, table: str, limit: int = 6) -> None:
    """Log the most frequent rejection reasons for a table."""
    reasons = (
        quarantined.groupBy(DQ_STAGE, DQ_ERRORS).count()
        .orderBy(F.desc("count"), DQ_ERRORS)
        .limit(limit)
        .collect()
    )
    for row in reasons:
        logger.warning("%s: %5d quarantined [%s] %s", table, row["count"], row[DQ_STAGE], row[DQ_ERRORS])


def process_table(
    spark: SparkSession,
    config: PipelineConfig,
    contract: TableContract,
    parents: dict[str, DataFrame],
) -> tuple[DatasetMetric, DataFrame]:
    """Validate, de-duplicate, check references, write Silver + quarantine, reconcile.

    Returns the metric and the (cached) Silver DataFrame, which child tables use
    as the parent for their foreign-key checks.
    """
    name = contract.name
    fields = list(contract.model.model_fields)

    bronze = read_bronze_partition(spark, config, name).cache()
    rows_in = bronze.count()
    if rows_in == 0:
        raise MissingBronzeError(f"Bronze partition {name}/{PARTITION_COLUMN}={config.run_date} is empty - run the bronze step first")
    _log_schema_drift(bronze, contract)

    # 1. Row-level contract
    passthrough = [c for c in LINEAGE_COLUMNS if c in bronze.columns]
    json_column = JSON_COLUMN if JSON_COLUMN in bronze.columns else None
    checked = validate_rows(bronze, contract.model, passthrough, json_column, contract.renames).cache()
    schema_ok = checked.where(F.col(DQ_ERRORS).isNull()).drop(DQ_ERRORS, DQ_RAW)
    schema_rejected = checked.where(F.col(DQ_ERRORS).isNotNull()).withColumn(DQ_STAGE, F.lit("schema"))

    # 2. Primary key uniqueness
    keys = list(contract.primary_key)
    conflicts = count_conflicting_duplicates(schema_ok, keys, fields)
    if conflicts:
        logger.warning("%s: %d keys have conflicting versions - keeping one deterministically", name, conflicts)
    deduped = deduplicate(schema_ok, keys, order_by=fields + passthrough)

    # 3. Referential integrity
    valid, orphans = check_foreign_keys(deduped, contract.foreign_keys, parents)
    orphans = orphans.withColumn(DQ_RAW, F.to_json(F.struct(*fields))).withColumn(DQ_STAGE, F.lit("referential"))

    # 4. Outputs
    conform = CONFORM.get(name, lambda df: df)
    silver = (
        conform(valid)
        .withColumn("_processed_at", F.current_timestamp())
        .withColumn("_run_id", F.lit(config.run_id))
        .cache()
    )
    quarantine_columns = [*QUARANTINE_COLUMNS, *passthrough]
    quarantined = (
        schema_rejected.select(*quarantine_columns)
        .unionByName(orphans.select(*quarantine_columns))
        .withColumn("_quarantined_at", F.current_timestamp())
        .withColumn("_run_id", F.lit(config.run_id))
    )

    target = f"{config.silver_path}/{name}"
    quarantine_path = f"{config.quarantine_path}/{name}/run_date={config.run_date}"
    write_silver(silver, target, PARTITION_BY.get(name, []))
    write_quarantine(quarantined, quarantine_path)

    # 5. Reconciliation (from what is actually on disk)
    rows_out = spark.read.parquet(target).count()
    rows_rejected = spark.read.parquet(quarantine_path).count()
    duplicates = schema_ok.count() - deduped.count()
    if rows_in != rows_out + rows_rejected + duplicates:
        raise ReconciliationError(
            f"{name}: rows_in={rows_in} != rows_out={rows_out} + quarantined={rows_rejected} + duplicates={duplicates}"
        )

    logger.info(
        "%-24s in=%6d  out=%6d  quarantined=%5d  duplicates=%4d -> %s",
        name, rows_in, rows_out, rows_rejected, duplicates, target,
    )
    if rows_rejected:
        _log_top_reasons(spark.read.parquet(quarantine_path), name)

    checked.unpersist()
    bronze.unpersist()
    metric = DatasetMetric("silver", name, f"{config.bronze_path}/{name}", target, rows_in, rows_out, rows_rejected)
    return metric, silver


def run_silver(spark: SparkSession, config: PipelineConfig) -> list[DatasetMetric]:
    """Build every Silver table from the Bronze partition of the run date."""
    metrics: list[DatasetMetric] = []
    parents: dict[str, DataFrame] = {}
    try:
        for contract in CONTRACTS:
            with log_step(logger, f"silver {contract.name}"):
                metric, silver = process_table(spark, config, contract, parents)
            metrics.append(metric)
            parents[contract.name] = silver
    finally:
        for df in parents.values():
            df.unpersist()
    return metrics
