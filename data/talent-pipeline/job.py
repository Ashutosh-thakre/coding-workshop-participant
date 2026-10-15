
"""ACME Talent Pipeline: Bronze -> Silver -> Gold batch ETL for employee performance data.

Sources:   PostgreSQL (HR tables) + S3 landing zone (learning activity JSONL events)
Lake:      <lake>/bronze, silver, quarantine, gold, lineage  (Parquet on S3 / LocalStack)
Serving:   Gold Parquet + Postgres gold_* tables, consumed by the Jupyter notebook

This file is the whole deployable job: the cloud deployment ships exactly
job.py + requirements.txt (see infra/locals.tf, infra/helm/templates/python-job.yaml).
It is organised in sections that mirror the pipeline's concerns:

     1. Configuration           6. Source readers            11. Validation engine
     2. Logging                 7. Lineage & run recorder    12. Silver layer
     3. Resilience (retry)      8. Health check              13. Gold layer
     4. Spark session           9. Bronze layer              14. Orchestration (main)
     5. Storage helpers        10. Schema contracts

Usage:
    source data/talent-pipeline/env.local
    python data/talent-pipeline/job.py                       # all steps: check, bronze, silver, gold
    python data/talent-pipeline/job.py --steps bronze,silver # selected steps (always run in order)
"""

from __future__ import annotations

import argparse
import functools
import importlib
import json
import logging
import os
import sys
import time
import types
import uuid
from collections.abc import Callable, Generator, Iterator
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, UTC
from pathlib import Path
from typing import Annotated, Any, Literal, ParamSpec, TypeVar, Union, get_args, get_origin
from urllib.parse import urlencode

# Always use the PySpark pinned in requirements.txt. Some environments (the workshop VDI,
# the jupyter/all-spark-notebook image) point SPARK_HOME at a different Spark installation.
os.environ.pop("SPARK_HOME", None)

import pandas as pd  # noqa: E402  (imported after the SPARK_HOME guard on purpose)
from pydantic import AfterValidator, BaseModel, ConfigDict, Field, ValidationError, model_validator  # noqa: E402
from pyspark.errors import AnalysisException  # noqa: E402
from pyspark.sql import Column, DataFrame, SparkSession, Window  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql import types as T  # noqa: E402

JOB_ROOT = str(Path(__file__).resolve().parent)  # folder containing this file
JOB_MODULE = Path(__file__).stem                  # "job" - imported by Spark Python workers


# =============================================================================
# 1. CONFIGURATION - every setting comes from environment variables
# =============================================================================

class ConfigError(ValueError):
    """Raised when a required setting is missing or invalid."""


def _require(name: str) -> str:
    """Return the value of a required environment variable, or fail with a clear message."""
    value = os.environ.get(name, "").strip()
    if not value:
        raise ConfigError(
            f"Environment variable {name} is required but not set. "
            "Locally, run: source data/talent-pipeline/env.local"
        )
    return value


def _parse_bool(value: str) -> bool:
    """Interpret 'true'/'1'/'yes' (any case) as True, everything else as False."""
    return value.strip().lower() in {"true", "1", "yes"}


@dataclass(frozen=True)
class PostgresConfig:
    """Connection settings for the PostgreSQL source system."""

    host: str
    port: int
    database: str
    user: str
    password: str = field(repr=False)  # never printed in logs
    require_ssl: bool = False

    @property
    def jdbc_url(self) -> str:
        """JDBC URL used by Spark, with timeouts so a dead connection fails instead of hanging.

        AWS Aurora requires SSL; local Postgres does not.
        """
        params = {"connectTimeout": 10, "socketTimeout": 600, "ApplicationName": "talent-pipeline"}
        if self.require_ssl:
            params["sslmode"] = "require"
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.database}?{urlencode(params)}"


@dataclass(frozen=True)
class PipelineConfig:
    """All settings for one pipeline run."""

    is_local: bool
    postgres: PostgresConfig
    lake_root: str            # e.g. s3a://coding-workshop-datalake-xxx  or  /tmp/lake
    s3_endpoint: str | None   # LocalStack URL locally, None in AWS (use the real endpoint)
    aws_region: str
    run_date: date            # logical date of the batch; drives Bronze partitioning
    run_id: str               # unique id of this execution, stamped on every row and log line
    publish_gold_to_postgres: bool = True  # also copy Gold tables to Postgres (gold_* tables)
    spark_cores: str = "*"                 # local[<cores>]: "*" = all cores available to the container
    spark_driver_memory: str = "512m"      # JVM heap (see DEFAULT_JAVA_OPTIONS for why it is small)
    spark_java_options: str = ""           # JVM flags; empty -> DEFAULT_JAVA_OPTIONS

    # --- Data lake zone paths (one place defines the layout) --------------------
    @property
    def landing_path(self) -> str:
        """Where source systems drop raw files (input to Bronze)."""
        return f"{self.lake_root}/landing"

    @property
    def bronze_path(self) -> str:
        """Raw, immutable copies of source data plus audit columns."""
        return f"{self.lake_root}/bronze"

    @property
    def silver_path(self) -> str:
        """Validated, typed, de-duplicated data."""
        return f"{self.lake_root}/silver"

    @property
    def gold_path(self) -> str:
        """Business-level datasets that answer the analytics questions."""
        return f"{self.lake_root}/gold"

    @property
    def quarantine_path(self) -> str:
        """Rows that failed validation, kept with the reason they failed."""
        return f"{self.lake_root}/quarantine"

    @property
    def lineage_path(self) -> str:
        """Run manifests and dataset metrics."""
        return f"{self.lake_root}/lineage"

    @classmethod
    def from_env(cls) -> PipelineConfig:
        """Build the configuration from environment variables.

        Required: POSTGRES_HOST, POSTGRES_NAME, POSTGRES_USER, POSTGRES_PASS and
        either LAKE_ROOT (any path/URI) or LAKE_BUCKET (an S3 bucket name).
        Optional: IS_LOCAL (default true), POSTGRES_PORT (5432), AWS_ENDPOINT_URL,
        AWS_REGION (us-east-1), RUN_DATE (YYYY-MM-DD, default today in UTC),
        PUBLISH_GOLD_TO_POSTGRES (default true), SPARK_CORES (default *),
        SPARK_DRIVER_MEMORY (default 512m), SPARK_JAVA_OPTIONS (default DEFAULT_JAVA_OPTIONS).
        """
        is_local = _parse_bool(os.environ.get("IS_LOCAL", "true"))

        port_raw = os.environ.get("POSTGRES_PORT", "5432").strip() or "5432"
        if not port_raw.isdigit():
            raise ConfigError(f"POSTGRES_PORT must be a number, got {port_raw!r}")

        postgres = PostgresConfig(
            host=_require("POSTGRES_HOST"),
            port=int(port_raw),
            database=_require("POSTGRES_NAME"),
            user=_require("POSTGRES_USER"),
            password=_require("POSTGRES_PASS"),
            require_ssl=not is_local,
        )

        # LAKE_ROOT wins if set (handy for tests: a local folder); otherwise use the bucket.
        lake_root = os.environ.get("LAKE_ROOT", "").strip().rstrip("/")
        if not lake_root:
            lake_root = f"s3a://{_require('LAKE_BUCKET')}"

        run_date_raw = os.environ.get("RUN_DATE", "").strip()
        try:
            run_date = date.fromisoformat(run_date_raw) if run_date_raw else datetime.now(UTC).date()
        except ValueError as exc:
            raise ConfigError(f"RUN_DATE must be YYYY-MM-DD, got {run_date_raw!r}") from exc

        return cls(
            is_local=is_local,
            postgres=postgres,
            lake_root=lake_root,
            s3_endpoint=os.environ.get("AWS_ENDPOINT_URL", "").strip() or None,
            aws_region=os.environ.get("AWS_REGION", "us-east-1").strip() or "us-east-1",
            run_date=run_date,
            run_id=datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:6],
            publish_gold_to_postgres=_parse_bool(os.environ.get("PUBLISH_GOLD_TO_POSTGRES", "true")),
            spark_cores=os.environ.get("SPARK_CORES", "*").strip() or "*",
            spark_driver_memory=os.environ.get("SPARK_DRIVER_MEMORY", "512m").strip() or "512m",
            spark_java_options=os.environ.get("SPARK_JAVA_OPTIONS", "").strip() or DEFAULT_JAVA_OPTIONS,
        )


# =============================================================================
# 2. LOGGING - structured lines tagged with run id and step
#    2026-10-14 16:30:01 | INFO    | run=20261014T163000Z-a1b2c3 | bronze | Wrote 2000 rows
# =============================================================================

LOGGER_NAME = "talent_pipeline"
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


class _StdoutHandler(logging.StreamHandler):
    """Write to whatever sys.stdout is *now* (survives stdout redirection, e.g. under pytest)."""

    @property
    def stream(self):  # noqa: ANN201
        return sys.stdout

    @stream.setter
    def stream(self, _value: object) -> None:
        pass


def setup_logging(run_id: str, level: int = logging.INFO) -> logging.Logger:
    """Configure the pipeline logger to write structured lines to stdout (idempotent)."""
    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(level)
    logger.propagate = False
    logger.handlers.clear()

    handler = _StdoutHandler()
    handler.setFormatter(logging.Formatter(LOG_FORMAT, DATE_FORMAT))
    handler.addFilter(_ContextFilter(run_id))
    logger.addHandler(handler)

    # Spark's Python bridge (py4j) is very chatty at INFO; keep only real problems.
    logging.getLogger("py4j").setLevel(logging.WARNING)
    return logger


def get_logger(step: str) -> logging.LoggerAdapter:
    """Return a logger that tags every message with the given step name."""
    return logging.LoggerAdapter(logging.getLogger(LOGGER_NAME), {"step": step})


@contextmanager
def log_step(logger: logging.LoggerAdapter, description: str) -> Generator[None]:
    """Log start, end, duration and status of a block of work (errors are logged, then re-raised)."""
    logger.info("START %s", description)
    started = time.perf_counter()
    try:
        yield
    except Exception:
        logger.exception("FAILED %s after %.2fs", description, time.perf_counter() - started)
        raise
    logger.info("DONE %s in %.2fs", description, time.perf_counter() - started)


# =============================================================================
# 3. RESILIENCE - retry transient failures with exponential backoff
# =============================================================================

P = ParamSpec("P")
R = TypeVar("R")


def retry(
    attempts: int = 3,
    base_delay: float = 2.0,
    exceptions: tuple[type[BaseException], ...] = (Exception,),
    sleep: Callable[[float], None] = time.sleep,
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """Retry a function on failure, waiting base, 2*base, 4*base... seconds between tries.

    The last error is re-raised (and logged at ERROR) when every attempt fails, so a
    real outage still stops the pipeline. ``sleep`` is injectable so tests don't wait.
    """
    if attempts < 1:
        raise ValueError("attempts must be >= 1")
    logger = get_logger("retry")

    def decorator(func: Callable[P, R]) -> Callable[P, R]:
        name = getattr(func, "__name__", type(func).__name__)  # callables like partial() have no __name__

        @functools.wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            for attempt in range(1, attempts + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as exc:
                    if attempt == attempts:
                        logger.error("%s failed after %d attempt(s): %s", name, attempts, exc)
                        raise
                    delay = base_delay * 2 ** (attempt - 1)
                    logger.warning(
                        "%s failed (attempt %d/%d): %s - retrying in %.1fs",
                        name, attempt, attempts, exc, delay,
                    )
                    sleep(delay)
            raise AssertionError("unreachable")  # loop always returns or raises

        return wrapper

    return decorator


# =============================================================================
# 4. SPARK SESSION - local mode (laptop or Kubernetes pod) with Postgres + S3 connectors
# =============================================================================

# Lean JVM for a small single-node batch. Measured peak memory of the whole job (Python driver + JVM +
# Python workers) dropped from 2.3 GB to 1.8 GB, under the Kubernetes job's 2 GiB limit
# (infra/helm/values.yaml), with identical results and a faster run (see docs/testing.md):
#   SerialGC          - one GC thread instead of several (no benefit from parallel GC at this size)
#   TieredStopAtLevel - lightweight JIT compiler only (less code cache, quicker start-up)
#   caps on code cache, class metadata, thread stacks and off-heap buffers
DEFAULT_JAVA_OPTIONS = (
    "-XX:+UseSerialGC -XX:TieredStopAtLevel=1 -XX:ReservedCodeCacheSize=64m "
    "-XX:MaxMetaspaceSize=256m -XX:MaxDirectMemorySize=128m -Xss512k"
)

# Connector versions must match the Spark build: Spark 4.1.2 ships Hadoop 3.4.2.
POSTGRES_JDBC_PACKAGE = "org.postgresql:postgresql:42.7.7"
HADOOP_AWS_PACKAGE = "org.apache.hadoop:hadoop-aws:3.4.2"


def build_spark(config: PipelineConfig, app_name: str = "talent-pipeline") -> SparkSession:
    """Create (or reuse) a SparkSession configured for this pipeline run."""
    builder = (
        SparkSession.builder.appName(app_name)
        .master(f"local[{config.spark_cores}]")
        # Applied when this process starts the JVM (it does: Spark runs in-process, local mode).
        .config("spark.driver.memory", config.spark_driver_memory)
        .config("spark.driver.extraJavaOptions", config.spark_java_options or DEFAULT_JAVA_OPTIONS)
        .config("spark.jars.packages", f"{POSTGRES_JDBC_PACKAGE},{HADOOP_AWS_PACKAGE}")
        # Idempotent writes: overwriting a partitioned table replaces only the partitions written.
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.session.timeZone", "UTC")
        # Small data: the default of 200 shuffle partitions only adds overhead.
        .config("spark.sql.shuffle.partitions", "8")
        # Progress bars flood batch logs; our own step logs report progress instead.
        .config("spark.ui.showConsoleProgress", "false")
        # Python worker processes (mapInPandas) import this file as a module.
        .config("spark.executorEnv.PYTHONPATH", JOB_ROOT)
        # S3A: credentials come from the default chain (env vars locally, IAM role in AWS).
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint.region", config.aws_region)
    )

    if config.s3_endpoint:
        # LocalStack (or any S3-compatible endpoint)
        builder = (
            builder.config("spark.hadoop.fs.s3a.endpoint", config.s3_endpoint)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(config.s3_endpoint.startswith("https")).lower())
        )

    if config.is_local:
        # The workshop VDI hostname resolves to two IPs; pin Spark to loopback.
        builder = (
            builder.config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
        )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


# =============================================================================
# 5. STORAGE HELPERS - Hadoop FileSystem API (works for s3a:// and local paths)
# =============================================================================

def _filesystem(spark: SparkSession, path: str):  # noqa: ANN202 - py4j JVM objects are untyped
    """Return (hadoop_fs, hadoop_path) for a path."""
    jvm = spark.sparkContext._jvm
    hadoop_path = jvm.org.apache.hadoop.fs.Path(path)
    return hadoop_path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration()), hadoop_path


@retry(attempts=3, base_delay=2.0)
def list_files(spark: SparkSession, directory: str, suffix: str = ".jsonl") -> list[tuple[str, int]]:
    """List every file under a directory as (path, size_in_bytes), including empty files.

    Spark's DataFrame readers can't be used here: they silently skip zero-byte files.
    """
    fs, path = _filesystem(spark, directory)
    files = []
    iterator = fs.listFiles(path, True)  # True = recursive
    while iterator.hasNext():
        status = iterator.next()
        file_path = status.getPath().toString()
        if file_path.endswith(suffix):
            files.append((file_path, status.getLen()))
    return sorted(files)


@retry(attempts=3, base_delay=2.0)
def write_text(spark: SparkSession, path: str, text: str) -> None:
    """Write (or overwrite) a single UTF-8 text file, e.g. a run manifest."""
    fs, hadoop_path = _filesystem(spark, path)
    stream = fs.create(hadoop_path, True)  # True = overwrite
    try:
        stream.write(bytearray(text.encode("utf-8")))
    finally:
        stream.close()


# =============================================================================
# 6. SOURCE READERS - PostgreSQL tables and raw landing-zone files
# =============================================================================

@retry(attempts=3, base_delay=2.0)
def read_postgres_table(spark: SparkSession, config: PipelineConfig, table: str) -> DataFrame:
    """Read a full PostgreSQL table over JDBC.

    The DataFrame is cached and counted here, so the database is queried exactly once
    inside this retried function and connection errors surface (and are retried) here.
    """
    df = (
        spark.read.format("jdbc")
        .option("url", config.postgres.jdbc_url)
        .option("dbtable", table)
        .option("user", config.postgres.user)
        .option("password", config.postgres.password)
        .option("driver", "org.postgresql.Driver")
        .option("fetchsize", "10000")  # rows per network round-trip
        .load()
        .cache()
    )
    df.count()
    return df


@retry(attempts=3, base_delay=2.0)
def read_text_lines(spark: SparkSession, directory: str, file_glob: str = "*.jsonl") -> DataFrame:
    """Read every matching file under a directory, line by line, as raw text.

    Returns columns 'value' (the line) and 'source_file' (its file). Raw text keeps
    malformed lines instead of silently dropping them; Silver parses and quarantines.
    """
    df = (
        spark.read.option("recursiveFileLookup", "true")
        .option("pathGlobFilter", file_glob)
        .text(directory)
        .select("value", F.col("_metadata.file_path").alias("source_file"))
        .cache()
    )
    df.count()
    return df


# =============================================================================
# 7. LINEAGE - row counts per dataset, per-step timing, run manifest per run
#    <lake>/lineage/runs/run_date=YYYY-MM-DD/<run_id>.json        (one per run, even failed)
#    <lake>/lineage/dataset_metrics/run_date=YYYY-MM-DD/*.parquet  (appended: audit log)
# =============================================================================

def _utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


@dataclass(frozen=True)
class DatasetMetric:
    """Row counts for one dataset moving through one layer."""

    layer: str          # bronze | silver | gold
    dataset: str        # e.g. employees
    source: str         # where it was read from
    target: str         # where it was written to
    rows_in: int
    rows_out: int
    rows_rejected: int = 0  # quarantined (Silver only)

    @property
    def rows_dropped(self) -> int:
        """Rows neither written nor quarantined (e.g. removed duplicates)."""
        return self.rows_in - self.rows_out - self.rows_rejected

    def as_dict(self) -> dict[str, Any]:
        """Plain dict, ready for JSON serialisation."""
        return {**asdict(self), "rows_dropped": self.rows_dropped}


@dataclass
class StepRecord:
    """Timing and outcome of one pipeline step."""

    name: str
    started_at: str
    finished_at: str | None = None
    duration_seconds: float | None = None
    status: str = "RUNNING"
    error: str | None = None


@dataclass
class RunRecorder:
    """Collects everything that happened in one run, for the run manifest."""

    config: PipelineConfig
    started_at: str = field(default_factory=_utc_now)
    steps: list[StepRecord] = field(default_factory=list)
    datasets: list[DatasetMetric] = field(default_factory=list)
    status: str = "RUNNING"
    finished_at: str | None = None
    _clock_start: float = field(default_factory=time.perf_counter, repr=False)

    @contextmanager
    def step(self, name: str) -> Generator[None]:
        """Record a step's start, end, duration and status (errors are re-raised)."""
        record = StepRecord(name=name, started_at=_utc_now())
        self.steps.append(record)
        started = time.perf_counter()
        try:
            yield
            record.status = "SUCCEEDED"
        except Exception as exc:
            record.status = "FAILED"
            record.error = f"{type(exc).__name__}: {exc}"[:2000]
            raise
        finally:
            record.finished_at = _utc_now()
            record.duration_seconds = round(time.perf_counter() - started, 2)

    def finish(self, succeeded: bool) -> None:
        """Mark the run as finished."""
        self.status = "SUCCEEDED" if succeeded else "FAILED"
        self.finished_at = _utc_now()

    def manifest(self) -> dict[str, Any]:
        """The full run manifest as a JSON-ready dict (no secrets)."""
        cfg = self.config
        return {
            "run_id": cfg.run_id,
            "run_date": cfg.run_date.isoformat(),
            "status": self.status,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "duration_seconds": round(time.perf_counter() - self._clock_start, 2),
            "environment": {
                "is_local": cfg.is_local,
                "lake_root": cfg.lake_root,
                "postgres_host": cfg.postgres.host,
                "postgres_database": cfg.postgres.database,
                "publish_gold_to_postgres": cfg.publish_gold_to_postgres,
            },
            "business_rules": business_rules(),
            "steps": [asdict(s) for s in self.steps],
            "datasets": [m.as_dict() for m in self.datasets],
        }


DATASET_METRICS_SCHEMA = T.StructType([
    T.StructField("run_id", T.StringType()),
    T.StructField("layer", T.StringType()),
    T.StructField("dataset", T.StringType()),
    T.StructField("source", T.StringType()),
    T.StructField("target", T.StringType()),
    T.StructField("rows_in", T.LongType()),
    T.StructField("rows_out", T.LongType()),
    T.StructField("rows_rejected", T.LongType()),
    T.StructField("rows_dropped", T.LongType()),
    T.StructField("run_status", T.StringType()),
    T.StructField("recorded_at", T.StringType()),
    T.StructField("run_date", T.DateType()),
])


def write_run_lineage(spark: SparkSession, recorder: RunRecorder) -> str:
    """Write the run manifest (JSON) and append dataset metrics (Parquet). Returns the manifest path."""
    cfg = recorder.config
    manifest_path = f"{cfg.lineage_path}/runs/run_date={cfg.run_date}/{cfg.run_id}.json"
    write_text(spark, manifest_path, json.dumps(recorder.manifest(), indent=2))

    if recorder.datasets:
        recorded_at = _utc_now()
        rows = [
            (cfg.run_id, m.layer, m.dataset, m.source, m.target, m.rows_in, m.rows_out,
             m.rows_rejected, m.rows_dropped, recorder.status, recorded_at, cfg.run_date)
            for m in recorder.datasets
        ]
        (
            spark.createDataFrame(rows, DATASET_METRICS_SCHEMA)
            .coalesce(1)
            .write.mode("append")
            .partitionBy("run_date")
            .parquet(f"{cfg.lineage_path}/dataset_metrics")
        )
    return manifest_path


# =============================================================================
# 8. HEALTH CHECK - can this run read both sources and write to the lake?
# =============================================================================

def run_healthcheck(spark: SparkSession, config: PipelineConfig) -> None:
    """Read PostgreSQL, read the S3 landing zone, and round-trip a file through the lake."""
    logger = get_logger("check")
    with log_step(logger, "postgres read"):
        employees = read_postgres_table(spark, config, "employees")
        logger.info("Postgres %s: employees rows=%d", config.postgres.host, employees.count())
        employees.unpersist()

    with log_step(logger, "landing zone read"):
        events = read_text_lines(spark, f"{config.landing_path}/learning_activity")
        logger.info("Landing zone %s: learning_activity lines=%d", config.landing_path, events.count())
        events.unpersist()

    with log_step(logger, "lake write/read round-trip"):
        # No leading '_' or '.': Spark treats such paths as hidden and skips them on read.
        probe_path = f"{config.lake_root}/healthcheck"
        spark.createDataFrame([(config.run_id,)], ["run_id"]).write.mode("overwrite").parquet(probe_path)
        read_back = [row.run_id for row in spark.read.parquet(probe_path).collect()]
        if read_back != [config.run_id]:
            raise RuntimeError(f"Lake round-trip mismatch at {probe_path}: wrote {config.run_id}, read {read_back}")
        logger.info("Lake %s is writable", config.lake_root)


# =============================================================================
# 9. BRONZE - land source data unchanged, plus audit columns
#    <lake>/bronze/<dataset>/ingest_date=YYYY-MM-DD/part-*.parquet
#    No cleaning. Partitioned by run date; rerunning a date replaces only that date.
# =============================================================================

POSTGRES_TABLES: tuple[str, ...] = (
    "teams", "competencies", "employees", "projects",
    "performance_reviews", "competency_assessments", "talent_review",
)
LANDING_DATASETS: tuple[str, ...] = ("learning_activity",)
BRONZE_PARTITION = "ingest_date"


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
        .withColumn(BRONZE_PARTITION, F.lit(config.run_date))
    )


def write_bronze(spark: SparkSession, df: DataFrame, path: str, config: PipelineConfig, target_files: int = 1) -> int:
    """Write one ingest_date partition (idempotently); return the rows read back from it.

    ``target_files`` Parquet files per partition: the data is small, so one file avoids
    the "many small files" problem (783 JSONL files become 1 Parquet file).
    """
    df.coalesce(target_files).write.mode("overwrite").partitionBy(BRONZE_PARTITION).parquet(path)
    return spark.read.parquet(path).where(F.col(BRONZE_PARTITION) == F.lit(config.run_date)).count()


def _ingest(
    spark: SparkSession, df: DataFrame, config: PipelineConfig, dataset: str, source_system: str, source_object: str,
) -> DatasetMetric:
    """Audit, write and reconcile one dataset; return its metrics."""
    logger = get_logger("bronze")
    rows_in = df.count()
    if rows_in == 0:
        raise EmptySourceError(f"{source_system}:{source_object} returned 0 rows - refusing to write an empty Bronze partition")

    target = f"{config.bronze_path}/{dataset}"
    rows_out = write_bronze(spark, add_audit_columns(df, config, source_system, source_object), target, config)
    if rows_out != rows_in:
        raise BronzeWriteError(f"{dataset}: read {rows_in} rows from source but {rows_out} rows in Bronze")

    logger.info("%-24s %6d rows  %s -> %s/%s=%s", dataset, rows_out, source_object, target, BRONZE_PARTITION, config.run_date)
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
    logger = get_logger("bronze")
    metrics = []
    for dataset in LANDING_DATASETS:
        source_dir = f"{config.landing_path}/{dataset}"
        files = list_files(spark, source_dir)
        logger.info("%s: %d files, %d bytes found in %s", dataset, len(files), sum(s for _, s in files), source_dir)
        for path, size in files:
            if size == 0:
                # Not an error (nothing to lose), but an empty daily drop usually means an upstream export problem.
                logger.warning("%s: empty file (0 bytes) - check the source system for this day: %s", dataset, path)

        raw = read_text_lines(spark, source_dir)
        df = raw.select(F.col("value").alias("raw_line"), F.col("source_file").alias("_source_file"))
        metrics.append(_ingest(spark, df, config, dataset, "s3", source_dir))
        raw.unpersist()
    return metrics


def run_bronze(spark: SparkSession, config: PipelineConfig) -> list[DatasetMetric]:
    """Ingest all sources into the Bronze layer."""
    logger = get_logger("bronze")
    metrics: list[DatasetMetric] = []
    with log_step(logger, "ingest postgres tables"):
        metrics += ingest_postgres(spark, config)
    with log_step(logger, "ingest landing zone files"):
        metrics += ingest_landing(spark, config)
    return metrics


# =============================================================================
# 10. SCHEMA CONTRACTS - what a valid row looks like (source of truth for Silver)
#     One Pydantic model per dataset; it validates rows, derives the Silver Spark
#     schema, and documents the contract. CONTRACTS adds table-level rules.
# =============================================================================

def _not_in_future(value: date) -> date:
    """Reject dates (or datetimes) later than today."""
    day = value.date() if isinstance(value, datetime) else value
    if day > date.today():
        raise ValueError(f"must not be in the future (got {value})")
    return value


PositiveId = Annotated[int, Field(gt=0)]
NonEmptyStr = Annotated[str, Field(min_length=1)]
PastDate = Annotated[date, AfterValidator(_not_in_future)]
PastDateTime = Annotated[datetime, AfterValidator(_not_in_future)]
Level1to5 = Annotated[int, Field(ge=1, le=5)]
HalfYearPeriod = Annotated[str, Field(pattern=r"^\d{4}-H[12]$")]   # e.g. 2024-H2
LowMediumHigh = Literal["Low", "Medium", "High"]


class Contract(BaseModel):
    """Base for all contracts: trim strings, ignore unknown fields (they are reported separately)."""

    model_config = ConfigDict(str_strip_whitespace=True, extra="ignore")


class Team(Contract):
    team_id: PositiveId
    name: NonEmptyStr
    department: NonEmptyStr


class Competency(Contract):
    competency_id: PositiveId
    name: NonEmptyStr
    category: Literal["Technical", "Leadership", "Domain"]


class Employee(Contract):
    employee_id: PositiveId
    first_name: NonEmptyStr
    last_name: NonEmptyStr
    email: Annotated[str, Field(pattern=r"^[^@\s]+@[^@\s]+\.[^@\s]+$")]
    team_id: PositiveId
    job_level: Literal["IC1", "IC2", "IC3", "Senior IC", "Manager", "Senior Manager", "Director"]
    hire_date: PastDate


class Project(Contract):
    project_id: PositiveId
    name: NonEmptyStr
    team_id: PositiveId
    outcome_status: Literal["on-time", "delayed", "at-risk", "cancelled"]
    outcome_score: Annotated[float, Field(ge=0, le=100)]


class PerformanceReview(Contract):
    review_id: PositiveId
    employee_id: PositiveId
    review_period: HalfYearPeriod
    rating: Level1to5
    manager_comment: str | None = None
    submitted_date: PastDate


class CompetencyAssessment(Contract):
    assessment_id: PositiveId
    employee_id: PositiveId
    competency_id: PositiveId
    proficiency_level: Level1to5
    required_level: Level1to5
    assessed_date: PastDate


class TalentReview(Contract):
    employee_id: PositiveId
    review_cycle: HalfYearPeriod
    potential_rating: LowMediumHigh
    promotion_readiness: Literal["Ready Now", "1-2 Years", "Not Ready"]
    attrition_risk: LowMediumHigh


QUIZ_EVENTS = frozenset({"quiz_attempted", "quiz_passed"})


class LearningEvent(Contract):
    event_id: Annotated[str, Field(pattern=r"^EV\d{10}$")]
    employee_id: PositiveId          # arrives as a string ("324"); Pydantic converts it
    event_date: PastDateTime         # rejects "yesterday", "N/A", "", "2026-13-45"
    event_type: Literal[
        "enrollment", "module_started", "module_completed", "quiz_attempted",
        "quiz_passed", "course_completed", "certification_earned",
    ]
    course_name: NonEmptyStr
    text: str | None = None
    score: Annotated[int, Field(ge=0, le=100)] | None = None

    @model_validator(mode="after")
    def score_only_on_quizzes(self) -> LearningEvent:
        """Quiz events must have a score; other events must not."""
        if self.event_type in QUIZ_EVENTS and self.score is None:
            raise ValueError(f"score is required for {self.event_type} events")
        if self.event_type not in QUIZ_EVENTS and self.score is not None:
            raise ValueError(f"score is not allowed for {self.event_type} events")
        return self


@dataclass(frozen=True)
class TableContract:
    """Row contract plus table-level rules for one Silver table."""

    name: str
    model: type[Contract]
    primary_key: tuple[str, ...]
    # column -> (parent table, parent column); checked against the parent's *valid* rows
    foreign_keys: dict[str, tuple[str, str]] = field(default_factory=dict)
    # Source-to-Silver field renames applied before validation (known schema drift)
    renames: dict[str, str] = field(default_factory=dict)
    # Field whose date must match the landing folder's year=/month=/day= partition
    partition_date_field: str | None = None


# Dependency order: parents before children.
CONTRACTS: tuple[TableContract, ...] = (
    TableContract("teams", Team, ("team_id",)),
    TableContract("competencies", Competency, ("competency_id",)),
    TableContract("employees", Employee, ("employee_id",), {"team_id": ("teams", "team_id")}),
    TableContract("projects", Project, ("project_id",), {"team_id": ("teams", "team_id")}),
    TableContract("performance_reviews", PerformanceReview, ("review_id",), {"employee_id": ("employees", "employee_id")}),
    TableContract(
        "competency_assessments", CompetencyAssessment, ("assessment_id",),
        {"employee_id": ("employees", "employee_id"), "competency_id": ("competencies", "competency_id")},
    ),
    TableContract("talent_review", TalentReview, ("employee_id",), {"employee_id": ("employees", "employee_id")}),
    TableContract(
        "learning_activity", LearningEvent, ("event_id",), {"employee_id": ("employees", "employee_id")},
        renames={"notes": "text"},          # some producers send 'notes' instead of 'text'
        partition_date_field="event_date",  # must match landing year=/month=/day=
    ),
)


# =============================================================================
# 11. VALIDATION ENGINE - generic data-quality checks used by Silver
#     Row validation runs Pydantic in Python workers (mapInPandas): one readable
#     contract per table, at the cost of per-row Python overhead - negligible at tens
#     of thousands of rows; at hundreds of millions, translate rules to Spark expressions.
# =============================================================================

DQ_ERRORS = "_dq_errors"  # semicolon-separated validation messages, null when valid
DQ_RAW = "_dq_raw"        # the original record (JSON text) for rejected rows
DQ_STAGE = "_dq_stage"    # which check rejected the row: schema | partition | referential

_SPARK_TYPES: dict[type, T.DataType] = {
    bool: T.BooleanType(), int: T.LongType(), float: T.DoubleType(), str: T.StringType(),
    datetime: T.TimestampType(), date: T.DateType(),
}


def _spark_type(annotation: Any) -> T.DataType:
    """Map a (possibly Annotated / Optional / Literal) Python type to a Spark type."""
    origin = get_origin(annotation)
    if origin is Annotated:
        return _spark_type(get_args(annotation)[0])
    if origin in (Union, types.UnionType):
        non_null = [arg for arg in get_args(annotation) if arg is not type(None)]
        if len(non_null) != 1:
            raise TypeError(f"Unsupported union type: {annotation}")
        return _spark_type(non_null[0])
    if origin is Literal:
        return T.StringType()
    if annotation in _SPARK_TYPES:
        return _SPARK_TYPES[annotation]
    raise TypeError(f"No Spark type mapping for {annotation}")


def model_to_spark_schema(model: type[BaseModel]) -> T.StructType:
    """Spark schema with one nullable column per contract field, in declaration order."""
    return T.StructType(
        [T.StructField(name, _spark_type(info.annotation), nullable=True) for name, info in model.model_fields.items()]
    )


def format_errors(exc: ValidationError) -> str:
    """Compact, human-readable list of validation errors: 'field: message; ...'."""
    return "; ".join(f"{'.'.join(str(p) for p in err['loc']) or 'row'}: {err['msg']}" for err in exc.errors())


def validate_record(
    record: dict[str, Any] | str | None,
    model: type[BaseModel],
    renames: dict[str, str] | None = None,
) -> tuple[dict[str, Any] | None, str | None]:
    """Validate one record (a dict, or a JSON string parsed first).

    Returns (clean_values, None) when valid, or (None, error_message) when not.
    Renames (e.g. {'notes': 'text'}) absorb known schema drift but never overwrite
    a field that is already present.
    """
    if not isinstance(record, dict):
        try:
            record = json.loads(record)  # type: ignore[arg-type]
        except (TypeError, json.JSONDecodeError) as exc:
            return None, f"malformed JSON: {getattr(exc, 'msg', exc)}"
        if not isinstance(record, dict):
            return None, "malformed JSON: not an object"

    record = dict(record)
    for source, target in (renames or {}).items():
        if source in record and target not in record:
            record[target] = record.pop(source)

    try:
        return model.model_validate(record).model_dump(), None
    except ValidationError as exc:
        return None, format_errors(exc)


def _contract_model(name: str) -> type[BaseModel]:
    """Resolve a contract class by name; Spark Python workers import this file as a module."""
    return getattr(importlib.import_module(JOB_MODULE), name)


def validate_rows(
    df: DataFrame,
    model: type[BaseModel],
    passthrough: list[str],
    json_column: str | None = None,
    renames: dict[str, str] | None = None,
) -> DataFrame:
    """Apply a contract to every row.

    Returns contract fields (typed; null for invalid rows) + passthrough columns +
    ``_dq_errors`` (null when valid) + ``_dq_raw`` (original record when invalid).
    If ``json_column`` is set, each record is parsed from that JSON text column;
    otherwise the contract's fields are read from columns of the same name.
    """
    fields = list(model.model_fields)
    model_name = model.__name__
    input_types = {f.name: f.dataType for f in df.schema.fields}
    output_schema = T.StructType(
        model_to_spark_schema(model).fields
        + [T.StructField(c, input_types[c], True) for c in passthrough]
        + [T.StructField(DQ_ERRORS, T.StringType(), True), T.StructField(DQ_RAW, T.StringType(), True)]
    )
    columns = output_schema.fieldNames()

    def _validate_batches(batches: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        contract = _contract_model(model_name)
        for batch in batches:
            out = []
            for row in batch.astype(object).where(batch.notna(), None).to_dict("records"):
                record = row[json_column] if json_column else {f: row.get(f) for f in fields}
                values, error = validate_record(record, contract, renames)
                result = dict(values) if values else dict.fromkeys(fields)
                result.update({c: row[c] for c in passthrough})
                result[DQ_ERRORS] = error
                result[DQ_RAW] = None if error is None else (
                    record if isinstance(record, str) else json.dumps(record, default=str)
                )
                out.append(result)
            # dtype=object keeps None as None (pandas would turn int+None columns into float+NaN)
            yield pd.DataFrame(out, columns=columns, dtype=object)

    return df.mapInPandas(_validate_batches, schema=output_schema)


def check_partition_dates(df: DataFrame, date_field: str, file_column: str = "_source_file") -> tuple[DataFrame, DataFrame]:
    """Split rows into (matching, mismatched) by comparing a date field with the file's year=/month=/day=.

    Dates are compared as 'yyyy-MM-dd' text, so a malformed folder name (e.g. month=13)
    is reported as a mismatch instead of crashing Spark's strict (ANSI) date casting.
    Files without year=/month=/day= in their path are not checked.
    """
    pattern = r"year=(\d{4})/month=(\d{1,2})/day=(\d{1,2})"
    partition_day = F.when(
        F.col(file_column).rlike(pattern),
        F.concat_ws(
            "-",
            F.regexp_extract(file_column, pattern, 1),
            F.lpad(F.regexp_extract(file_column, pattern, 2), 2, "0"),
            F.lpad(F.regexp_extract(file_column, pattern, 3), 2, "0"),
        ),
    )
    record_day = F.date_format(F.col(date_field), "yyyy-MM-dd")
    matches = partition_day.isNull() | (partition_day == record_day)
    mismatched = df.where(~matches).withColumn(
        DQ_ERRORS,
        F.format_string(
            "%s: %s does not match landing partition %s",
            F.lit(date_field), record_day, partition_day,
        ),
    )
    return df.where(matches), mismatched


def deduplicate(df: DataFrame, keys: list[str], order_by: list[str]) -> DataFrame:
    """Keep one row per key: the first by ``order_by`` (deterministic across reruns)."""
    window = Window.partitionBy(*keys).orderBy(*[F.col(c).asc_nulls_last() for c in order_by])
    return df.withColumn("_row_number", F.row_number().over(window)).where("_row_number = 1").drop("_row_number")


def count_conflicting_duplicates(df: DataFrame, keys: list[str], value_columns: list[str]) -> int:
    """Number of keys that appear with *different* values (not just repeated identical rows)."""
    return (
        df.groupBy(*keys)
        .agg(F.countDistinct(F.struct(*value_columns)).alias("_versions"))
        .where("_versions > 1")
        .count()
    )


def check_foreign_keys(
    df: DataFrame, foreign_keys: dict[str, tuple[str, str]], parents: dict[str, DataFrame],
) -> tuple[DataFrame, DataFrame]:
    """Split rows into (valid, orphans); orphans get ``_dq_errors`` naming the missing parent."""
    orphans: list[DataFrame] = []
    for column, (parent_table, parent_column) in foreign_keys.items():
        ref = parents[parent_table].select(F.col(parent_column).alias("_ref_key")).distinct()
        condition = df[column] == ref["_ref_key"]
        orphans.append(
            df.join(ref, condition, "left_anti")
            .withColumn(DQ_ERRORS, F.lit(f"{column}: no matching {parent_table}.{parent_column}"))
        )
        df = df.join(ref, condition, "left_semi")
    if not orphans:
        return df, df.limit(0).withColumn(DQ_ERRORS, F.lit(None).cast("string"))
    rejected = orphans[0]
    for extra in orphans[1:]:
        rejected = rejected.unionByName(extra)
    return df, rejected


# =============================================================================
# 12. SILVER - validated, de-duplicated, conformed data (full refresh per run)
#     <lake>/silver/<table>/[event_year=YYYY/]part-*.parquet
#     <lake>/quarantine/<table>/run_date=YYYY-MM-DD/part-*.parquet
#     Reconciled per table: rows_in == rows_out + quarantined + duplicates
# =============================================================================

JSON_COLUMN = "raw_line"  # Bronze column holding the raw JSON line for landing datasets
LINEAGE_COLUMNS = ("_source_system", "_source_object", "_source_file", "_ingested_at", "_bronze_run_id")
QUARANTINE_COLUMNS = (DQ_STAGE, DQ_ERRORS, DQ_RAW)
SILVER_PARTITION_BY: dict[str, list[str]] = {"learning_activity": ["event_year"]}


class MissingBronzeError(RuntimeError):
    """Raised when the Bronze partition for the run date is missing or empty."""


class SilverCheckError(RuntimeError):
    """Raised when Silver output fails reconciliation or its post-write checks."""


def _conform_performance_reviews(df: DataFrame) -> DataFrame:
    """Split '2024-H2' into period_year=2024, period_half=2 for sorting and trends."""
    return (
        df.withColumn("period_year", F.substring("review_period", 1, 4).cast("int"))
        .withColumn("period_half", F.substring("review_period", 7, 1).cast("int"))
    )


def _conform_learning_activity(df: DataFrame) -> DataFrame:
    """Add the calendar day and year of each event (year is the partition column)."""
    return df.withColumn("event_day", F.to_date("event_date")).withColumn("event_year", F.year("event_date"))


# Light, entity-level conformance only; business metrics belong in Gold.
CONFORM: dict[str, Callable[[DataFrame], DataFrame]] = {
    "performance_reviews": _conform_performance_reviews,
    "learning_activity": _conform_learning_activity,
}


def read_bronze_partition(spark: SparkSession, config: PipelineConfig, table: str) -> DataFrame:
    """Read the Bronze partition for the run date; rename Bronze's _run_id to _bronze_run_id."""
    path = f"{config.bronze_path}/{table}"
    try:
        df = spark.read.parquet(path)
    except AnalysisException as exc:
        raise MissingBronzeError(f"No Bronze data at {path} - run the bronze step first") from exc
    return (
        df.where(F.col(BRONZE_PARTITION) == F.lit(config.run_date))
        .drop(BRONZE_PARTITION)
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


def _report_schema_drift(bronze: DataFrame, contract: TableContract, logger: logging.LoggerAdapter) -> None:
    """Warn about known renamed fields and about fields the contract doesn't know at all."""
    if JSON_COLUMN not in bronze.columns:
        return
    keys = bronze.select(F.explode(F.json_object_keys(JSON_COLUMN)).alias("key")).groupBy("key").count().collect()
    known = set(contract.model.model_fields)
    for row in keys:
        if row["key"] in contract.renames:
            logger.warning("%s: schema drift - %d rows use '%s' instead of '%s' (renamed)",
                           contract.name, row["count"], row["key"], contract.renames[row["key"]])
        elif row["key"] not in known:
            logger.warning("%s: schema drift - unknown field '%s' in %d rows (ignored; update the contract?)",
                           contract.name, row["key"], row["count"])


def _log_top_reasons(quarantined: DataFrame, table: str, logger: logging.LoggerAdapter, limit: int = 6) -> None:
    """Log the most frequent rejection reasons for a table."""
    reasons = (
        quarantined.groupBy(DQ_STAGE, DQ_ERRORS).count()
        .orderBy(F.desc("count"), DQ_ERRORS)
        .limit(limit)
        .collect()
    )
    for row in reasons:
        logger.warning("%s: %5d quarantined [%s] %s", table, row["count"], row[DQ_STAGE], row[DQ_ERRORS])


def check_silver_output(written: DataFrame, contract: TableContract) -> None:
    """Post-write checks: not empty, no null primary keys, contract columns have contract types."""
    name = contract.name
    if written.count() == 0:
        raise SilverCheckError(f"{name}: Silver table is empty - all rows were rejected or the source was empty")
    null_keys = written.where(" OR ".join(f"{k} IS NULL" for k in contract.primary_key)).count()
    if null_keys:
        raise SilverCheckError(f"{name}: {null_keys} rows with a null primary key {contract.primary_key}")
    actual = {f.name: f.dataType for f in written.schema.fields}
    expected = {f.name: f.dataType for f in model_to_spark_schema(contract.model).fields}
    mismatched = {c: (str(t), str(actual.get(c))) for c, t in expected.items() if actual.get(c) != t}
    if mismatched:
        raise SilverCheckError(f"{name}: written schema differs from contract (expected, actual): {mismatched}")


def process_table(
    spark: SparkSession, config: PipelineConfig, contract: TableContract, parents: dict[str, DataFrame],
) -> tuple[DatasetMetric, DataFrame]:
    """Validate, de-duplicate, check references, write Silver + quarantine, reconcile.

    Returns the metric and the (cached) Silver DataFrame, which child tables use as
    the parent for their foreign-key checks.
    """
    logger = get_logger("silver")
    name = contract.name
    fields = list(contract.model.model_fields)

    bronze = read_bronze_partition(spark, config, name).cache()
    rows_in = bronze.count()
    if rows_in == 0:
        raise MissingBronzeError(
            f"Bronze partition {name}/{BRONZE_PARTITION}={config.run_date} is empty - run the bronze step first"
        )
    _report_schema_drift(bronze, contract, logger)

    # 1. Row-level contract
    passthrough = [c for c in LINEAGE_COLUMNS if c in bronze.columns]
    json_column = JSON_COLUMN if JSON_COLUMN in bronze.columns else None
    checked = validate_rows(bronze, contract.model, passthrough, json_column, contract.renames).cache()
    schema_ok = checked.where(F.col(DQ_ERRORS).isNull()).drop(DQ_ERRORS, DQ_RAW)
    rejected = [checked.where(F.col(DQ_ERRORS).isNotNull()).withColumn(DQ_STAGE, F.lit("schema"))]

    # 2. Landing partition consistency (record date must match its year=/month=/day= folder)
    if contract.partition_date_field and "_source_file" in passthrough:
        schema_ok, mismatched = check_partition_dates(schema_ok, contract.partition_date_field)
        rejected.append(
            mismatched.withColumn(DQ_RAW, F.to_json(F.struct(*fields))).withColumn(DQ_STAGE, F.lit("partition"))
        )

    # 3. Primary key uniqueness
    keys = list(contract.primary_key)
    conflicts = count_conflicting_duplicates(schema_ok, keys, fields)
    if conflicts:
        logger.warning("%s: %d keys have conflicting versions - keeping one deterministically", name, conflicts)
    deduped = deduplicate(schema_ok, keys, order_by=fields + passthrough)

    # 4. Referential integrity
    valid, orphans = check_foreign_keys(deduped, contract.foreign_keys, parents)
    rejected.append(orphans.withColumn(DQ_RAW, F.to_json(F.struct(*fields))).withColumn(DQ_STAGE, F.lit("referential")))

    # 5. Outputs
    silver = (
        CONFORM.get(name, lambda df: df)(valid)
        .withColumn("_processed_at", F.current_timestamp())
        .withColumn("_run_id", F.lit(config.run_id))
        .cache()
    )
    quarantine_columns = [*QUARANTINE_COLUMNS, *passthrough]
    quarantined = rejected[0].select(*quarantine_columns)
    for part in rejected[1:]:
        quarantined = quarantined.unionByName(part.select(*quarantine_columns))
    quarantined = quarantined.withColumn("_quarantined_at", F.current_timestamp()).withColumn("_run_id", F.lit(config.run_id))

    target = f"{config.silver_path}/{name}"
    quarantine_path = f"{config.quarantine_path}/{name}/run_date={config.run_date}"
    write_silver(silver, target, SILVER_PARTITION_BY.get(name, []))
    write_quarantine(quarantined, quarantine_path)

    # 6. Reconciliation and post-write checks (on what is actually on disk)
    written = spark.read.parquet(target)
    check_silver_output(written, contract)
    rows_out = written.count()
    rows_rejected = spark.read.parquet(quarantine_path).count()
    duplicates = schema_ok.count() - deduped.count()
    if rows_in != rows_out + rows_rejected + duplicates:
        raise SilverCheckError(
            f"{name}: rows_in={rows_in} != rows_out={rows_out} + quarantined={rows_rejected} + duplicates={duplicates}"
        )

    logger.info("%-24s in=%6d  out=%6d  quarantined=%5d  duplicates=%4d -> %s",
                name, rows_in, rows_out, rows_rejected, duplicates, target)
    if rows_rejected:
        _log_top_reasons(spark.read.parquet(quarantine_path), name, logger)

    checked.unpersist()
    bronze.unpersist()
    return DatasetMetric("silver", name, f"{config.bronze_path}/{name}", target, rows_in, rows_out, rows_rejected), silver


def run_silver(spark: SparkSession, config: PipelineConfig) -> list[DatasetMetric]:
    """Build every Silver table from the Bronze partition of the run date."""
    logger = get_logger("silver")
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


# =============================================================================
# 13. GOLD - one dataset per business question (full refresh per run)
#
# | # | Question                                       | Gold table                   | Grain (one row per)             |
# |---|------------------------------------------------|------------------------------|---------------------------------|
# | 1 | Performance ratings and review history         | employee_performance_history | employee x review period        |
# | 2 | Critical skill gaps in key competencies        | skill_gaps                   | employee x competency           |
# | 3 | High-potential employees ready for promotion   | promotion_pipeline           | employee                        |
# | 4 | Training and development completed             | employee_learning_summary    | employee                        |
# | 5 | Attrition risk based on performance trends     | attrition_risk               | employee                        |
# | 6 | Performance vs. team and project outcomes      | team_performance_outcomes    | team                            |
# | 7 | Distribution of skills across the organisation | skill_distribution           | department x competency x level |
#
# Builders are pure functions (DataFrames in, DataFrame out) so they can be unit tested.
# =============================================================================

# --- Business rules (assumptions - documented, easy to change) ------------------
CRITICAL_GAP_LEVELS = 2          # required_level - proficiency_level >= 2  -> critical gap
HIGH_PERFORMER_RATING = 4        # latest rating >= 4 -> high performer
PROMOTION_MIN_RATING = 4         # promotion candidate: High potential + Ready Now + latest rating >= 4
LOW_RATING = 2                   # latest rating <= 2 -> attrition risk factor
LEARNING_INACTIVE_DAYS = 180     # no learning activity for this many days -> attrition risk factor
RISK_POINTS = {"hr_high": 3, "hr_medium": 1, "rating_declined": 2, "low_rating": 1, "learning_inactive": 1}
RISK_TIER_HIGH = 4               # score >= 4 -> High
RISK_TIER_MEDIUM = 2             # score >= 2 -> Medium, else Low


def business_rules() -> dict[str, Any]:
    """The rule values used by Gold, recorded in every run manifest so reports can quote them."""
    return {
        "critical_gap_levels": CRITICAL_GAP_LEVELS,
        "high_performer_rating": HIGH_PERFORMER_RATING,
        "promotion_min_rating": PROMOTION_MIN_RATING,
        "low_rating": LOW_RATING,
        "learning_inactive_days": LEARNING_INACTIVE_DAYS,
        "risk_points": dict(RISK_POINTS),
        "risk_tier_high": RISK_TIER_HIGH,
        "risk_tier_medium": RISK_TIER_MEDIUM,
    }


class GoldCheckError(RuntimeError):
    """Raised when a Gold table violates its grain or doesn't reconcile with Silver."""


def _count_if(condition: Column) -> Column:
    return F.sum(F.when(condition, 1).otherwise(0))


def build_employee_dim(employees: DataFrame, teams: DataFrame, as_of: date) -> DataFrame:
    """One row per employee with team, department and tenure (years, as of the run date)."""
    return employees.join(
        teams.select("team_id", F.col("name").alias("team_name"), "department"), "team_id", "left"
    ).select(
        "employee_id",
        F.concat_ws(" ", "first_name", "last_name").alias("full_name"),
        "job_level", "team_id", "team_name", "department", "hire_date",
        F.round(F.datediff(F.lit(as_of), "hire_date") / 365.25, 1).alias("tenure_years"),
    )


def _employee_context(employee_dim: DataFrame) -> DataFrame:
    """The descriptive employee columns repeated on most Gold tables."""
    return employee_dim.select("employee_id", "full_name", "department", "team_name", "job_level")


def build_performance_history(reviews: DataFrame, employee_dim: DataFrame) -> DataFrame:
    """Q1: each review with the previous rating, change and trend for the same employee."""
    by_period = Window.partitionBy("employee_id").orderBy("period_year", "period_half")
    change = F.col("rating") - F.col("previous_rating")
    return (
        reviews.withColumn("review_seq", F.row_number().over(by_period))
        .withColumn("previous_rating", F.lag("rating").over(by_period))
        .withColumn("rating_change", change)
        .withColumn(
            "rating_trend",
            F.when(F.col("previous_rating").isNull(), "first review")
            .when(change > 0, "improved").when(change < 0, "declined").otherwise("stable"),
        )
        .join(_employee_context(employee_dim), "employee_id", "inner")
        .select(
            "employee_id", "full_name", "department", "team_name", "job_level",
            "review_id", "review_period", "period_year", "period_half", "review_seq",
            "rating", "previous_rating", "rating_change", "rating_trend", "manager_comment", "submitted_date",
        )
    )


def build_skill_gaps(assessments: DataFrame, competencies: DataFrame, employee_dim: DataFrame) -> DataFrame:
    """Q2: each assessment with gap = required - proficiency and a severity label."""
    gap = F.col("required_level") - F.col("proficiency_level")
    return (
        assessments.join(
            competencies.select("competency_id", F.col("name").alias("competency_name"), "category"),
            "competency_id", "inner",
        )
        .withColumn("gap", gap)
        .withColumn("gap_severity", F.when(gap >= CRITICAL_GAP_LEVELS, "critical").when(gap >= 1, "moderate").otherwise("none"))
        .withColumn("is_critical_gap", gap >= CRITICAL_GAP_LEVELS)
        .join(_employee_context(employee_dim), "employee_id", "inner")
        .select(
            "employee_id", "full_name", "department", "team_name", "job_level",
            "competency_id", "competency_name", "category",
            "proficiency_level", "required_level", "gap", "gap_severity", "is_critical_gap", "assessed_date",
        )
    )


def events_as_of(events: DataFrame, as_of: date) -> DataFrame:
    """Learning events that had happened by the end of ``as_of`` (point-in-time view)."""
    return events.where(F.col("event_day") <= F.lit(as_of))


def build_learning_summary(events: DataFrame, employee_dim: DataFrame, as_of: date) -> DataFrame:
    """Q4: per-employee learning activity; every employee gets a row (zeros if no activity).

    completion_rate = courses completed / courses engaged with (any event). The event feed
    is not a strict funnel (completions without enrollment events exist), so
    'completed / enrolled' could exceed 100%.

    Point-in-time: only events on or before ``as_of`` count, so a backfill for an earlier
    run date reports what was known then (and days_since_last_activity is never negative).
    """
    event_type, course = F.col("event_type"), F.col("course_name")
    per_employee = events_as_of(events, as_of).groupBy("employee_id").agg(
        F.count("*").alias("total_events"),
        F.countDistinct(course).alias("courses_engaged"),
        F.countDistinct(F.when(event_type == "enrollment", course)).alias("courses_enrolled"),
        F.countDistinct(F.when(event_type == "course_completed", course)).alias("courses_completed"),
        F.countDistinct(F.when(event_type == "certification_earned", course)).alias("certifications_earned"),
        _count_if(event_type.isin(*QUIZ_EVENTS)).alias("quiz_events"),
        _count_if(event_type == "quiz_passed").alias("quizzes_passed"),
        F.round(F.avg("score"), 1).alias("avg_quiz_score"),
        F.min("event_day").alias("first_activity_date"),
        F.max("event_day").alias("last_activity_date"),
        F.sort_array(F.collect_set(F.when(event_type == "course_completed", course))).alias("completed_courses"),
    )
    counts = ["total_events", "courses_engaged", "courses_enrolled", "courses_completed",
              "certifications_earned", "quiz_events", "quizzes_passed"]
    return (
        _employee_context(employee_dim)
        .join(per_employee, "employee_id", "left")
        .fillna(0, subset=counts)
        .withColumn("completed_courses", F.coalesce("completed_courses", F.array().cast("array<string>")))
        .withColumn("completion_rate",
                    F.when(F.col("courses_engaged") > 0, F.round(F.col("courses_completed") / F.col("courses_engaged"), 2)))
        .withColumn("days_since_last_activity", F.datediff(F.lit(as_of), "last_activity_date"))
    )


def build_employee_profile(
    employee_dim: DataFrame, history: DataFrame, talent: DataFrame, skill_gaps: DataFrame, learning: DataFrame,
) -> DataFrame:
    """One row per employee: latest performance, talent review, gaps and learning (feeds Q3, Q5, Q6)."""
    latest_first = Window.partitionBy("employee_id").orderBy(F.desc("period_year"), F.desc("period_half"))
    latest = (
        history.withColumn("_rank", F.row_number().over(latest_first)).where("_rank = 1")
        .select("employee_id", F.col("review_period").alias("latest_period"), F.col("rating").alias("latest_rating"),
                "previous_rating", "rating_change", "rating_trend")
    )
    averages = history.groupBy("employee_id").agg(
        F.round(F.avg("rating"), 2).alias("avg_rating"), F.count("*").alias("review_count"),
    )
    gaps = skill_gaps.groupBy("employee_id").agg(
        _count_if(F.col("is_critical_gap")).alias("critical_gap_count"), F.count("*").alias("competencies_assessed"),
    )
    return (
        employee_dim.join(latest, "employee_id", "left")
        .join(averages, "employee_id", "left")
        .join(talent.select("employee_id", "review_cycle", "potential_rating", "promotion_readiness",
                            F.col("attrition_risk").alias("hr_attrition_risk")), "employee_id", "left")
        .join(gaps, "employee_id", "left")
        .join(learning.select("employee_id", "courses_completed", "certifications_earned",
                              "last_activity_date", "days_since_last_activity"), "employee_id", "left")
        .fillna(0, subset=["critical_gap_count", "competencies_assessed"])
    )


def build_promotion_pipeline(profile: DataFrame) -> DataFrame:
    """Q3: flag promotion candidates and give every employee a pipeline stage."""
    high_potential = F.col("potential_rating") == "High"
    candidate = high_potential & (F.col("promotion_readiness") == "Ready Now") & (F.col("latest_rating") >= PROMOTION_MIN_RATING)
    return (
        profile.withColumn("is_high_potential", high_potential)
        .withColumn("is_promotion_candidate", candidate)
        .withColumn(
            "pipeline_stage",
            F.when(candidate & (F.col("critical_gap_count") == 0), "Promote now")
            .when(candidate, "Promote after closing skill gaps")
            .when(high_potential, "Develop (high potential)")
            .otherwise("Not in pipeline"),
        )
        .select(
            "employee_id", "full_name", "department", "team_name", "job_level", "tenure_years",
            "potential_rating", "promotion_readiness", "latest_period", "latest_rating", "avg_rating",
            "rating_trend", "critical_gap_count", "certifications_earned",
            "is_high_potential", "is_promotion_candidate", "pipeline_stage",
        )
    )


def build_attrition_risk(profile: DataFrame) -> DataFrame:
    """Q5: explainable attrition risk score from HR flag, rating trend, low rating and learning inactivity."""
    hr = F.col("hr_attrition_risk")
    declined = F.col("rating_trend") == "declined"
    low_rating = F.col("latest_rating") <= LOW_RATING
    inactive = F.col("days_since_last_activity").isNull() | (F.col("days_since_last_activity") > LEARNING_INACTIVE_DAYS)
    score = (
        F.when(hr == "High", RISK_POINTS["hr_high"]).when(hr == "Medium", RISK_POINTS["hr_medium"]).otherwise(0)
        + F.when(declined, RISK_POINTS["rating_declined"]).otherwise(0)
        + F.when(low_rating, RISK_POINTS["low_rating"]).otherwise(0)
        + F.when(inactive, RISK_POINTS["learning_inactive"]).otherwise(0)
    )
    factors = F.array_compact(F.array(
        F.when(hr == "High", F.lit("HR flagged high attrition risk")),
        F.when(hr == "Medium", F.lit("HR flagged medium attrition risk")),
        F.when(declined, F.format_string("Rating declined %d -> %d", "previous_rating", "latest_rating")),
        F.when(low_rating, F.format_string("Low latest rating (%d)", "latest_rating")),
        F.when(inactive, F.lit(f"No learning activity in {LEARNING_INACTIVE_DAYS} days")),
    ))
    return (
        profile.withColumn("risk_score", score)
        .withColumn("risk_tier", F.when(F.col("risk_score") >= RISK_TIER_HIGH, "High")
                    .when(F.col("risk_score") >= RISK_TIER_MEDIUM, "Medium").otherwise("Low"))
        .withColumn("risk_factors", factors)
        .select(
            "employee_id", "full_name", "department", "team_name", "job_level", "tenure_years",
            "hr_attrition_risk", "latest_rating", "previous_rating", "rating_trend",
            "days_since_last_activity", "potential_rating", "risk_score", "risk_tier", "risk_factors",
        )
    )


def build_team_outcomes(profile: DataFrame, teams: DataFrame, projects: DataFrame) -> DataFrame:
    """Q6: per team, people metrics (ratings, risk) next to project outcome metrics."""
    people = profile.groupBy("team_id").agg(
        F.count("*").alias("headcount"),
        F.round(F.avg("latest_rating"), 2).alias("avg_latest_rating"),
        F.round(F.avg(F.when(F.col("latest_rating") >= HIGH_PERFORMER_RATING, 1).otherwise(0)), 2).alias("high_performer_share"),
        F.round(F.avg("rating_change"), 2).alias("avg_rating_change"),
        F.round(F.avg("critical_gap_count"), 2).alias("avg_critical_gaps"),
        _count_if(F.col("hr_attrition_risk") == "High").alias("hr_high_risk_count"),
    )
    project_stats = projects.groupBy("team_id").agg(
        F.count("*").alias("project_count"),
        F.round(F.avg("outcome_score"), 2).alias("avg_outcome_score"),
        F.round(F.avg(F.when(F.col("outcome_status") == "on-time", 1).otherwise(0)), 2).alias("on_time_share"),
        _count_if(F.col("outcome_status").isin("at-risk", "cancelled")).alias("troubled_projects"),
    )
    return (
        teams.select("team_id", F.col("name").alias("team_name"), "department")
        .join(people, "team_id", "left")
        .join(project_stats, "team_id", "left")
        .fillna(0, subset=["headcount", "hr_high_risk_count", "project_count", "troubled_projects"])
        .withColumn("has_projects", F.col("project_count") > 0)
    )


def build_skill_distribution(skill_gaps: DataFrame) -> DataFrame:
    """Q7: employees per department x competency x proficiency level, with share within the competency."""
    within_competency = Window.partitionBy("department", "competency_name")
    return (
        skill_gaps.groupBy("department", "category", "competency_name", "proficiency_level")
        .agg(F.countDistinct("employee_id").alias("employees"), _count_if(F.col("is_critical_gap")).alias("critical_gaps"))
        .withColumn("share_of_competency", F.round(F.col("employees") / F.sum("employees").over(within_competency), 3))
    )


def check_grain(df: DataFrame, table: str, keys: list[str]) -> int:
    """Fail if the table is empty or any grain key is null or duplicated; return the row count."""
    rows = df.count()
    if rows == 0:
        raise GoldCheckError(f"{table}: Gold table is empty")
    distinct = df.select(*keys).distinct().count()
    null_keys = df.where(" OR ".join(f"{k} IS NULL" for k in keys)).count()
    if distinct != rows or null_keys:
        raise GoldCheckError(f"{table}: grain {keys} violated (rows={rows}, distinct keys={distinct}, null keys={null_keys})")
    return rows


def check_equal(table: str, what: str, actual: int, expected: int) -> None:
    """Fail when a Gold total doesn't reconcile with its Silver source."""
    if actual != expected:
        raise GoldCheckError(f"{table}: {what} = {actual}, expected {expected} (from Silver)")
    get_logger("gold").info("%-28s check OK: %s = %d", table, what, actual)


@retry(attempts=3, base_delay=2.0)
def publish_to_postgres(df: DataFrame, config: PipelineConfig, table: str) -> None:
    """Replace the Postgres table gold_<table> with the Gold dataset (queryable with SQL)."""
    (
        df.write.format("jdbc")
        .option("url", config.postgres.jdbc_url)
        .option("dbtable", f"gold_{table}")
        .option("user", config.postgres.user)
        .option("password", config.postgres.password)
        .option("driver", "org.postgresql.Driver")
        .mode("overwrite")
        .save()
    )


@dataclass(frozen=True)
class GoldTable:
    """A built Gold dataset with its grain."""

    name: str
    df: DataFrame
    grain: tuple[str, ...]


def run_gold(spark: SparkSession, config: PipelineConfig) -> list[DatasetMetric]:
    """Build, verify, write (and publish) every Gold table from Silver."""
    logger = get_logger("gold")

    def silver(name: str) -> DataFrame:
        return spark.read.parquet(f"{config.silver_path}/{name}")

    employees, teams = silver("employees"), silver("teams")
    reviews, assessments, events = silver("performance_reviews"), silver("competency_assessments"), silver("learning_activity")

    with log_step(logger, "build gold datasets"):
        employee_dim = build_employee_dim(employees, teams, config.run_date).cache()
        history = build_performance_history(reviews, employee_dim).cache()
        gaps = build_skill_gaps(assessments, silver("competencies"), employee_dim).cache()
        learning = build_learning_summary(events, employee_dim, config.run_date).cache()
        profile = build_employee_profile(employee_dim, history, silver("talent_review"), gaps, learning).cache()
        tables = [
            GoldTable("employee_performance_history", history, ("employee_id", "review_period")),
            GoldTable("skill_gaps", gaps, ("employee_id", "competency_id")),
            GoldTable("promotion_pipeline", build_promotion_pipeline(profile), ("employee_id",)),
            GoldTable("employee_learning_summary", learning, ("employee_id",)),
            GoldTable("attrition_risk", build_attrition_risk(profile), ("employee_id",)),
            GoldTable("team_performance_outcomes", build_team_outcomes(profile, teams, silver("projects")), ("team_id",)),
            GoldTable(
                "skill_distribution", build_skill_distribution(gaps), ("department", "competency_name", "proficiency_level"),
            ),
        ]

    with log_step(logger, "verify grain and reconcile with silver"):
        n_employees, n_teams = employees.count(), teams.count()
        n_reviews, n_assessments = reviews.count(), assessments.count()
        n_events = events_as_of(events, config.run_date).count()   # Gold counts events up to the run date
        rows = {t.name: check_grain(t.df, t.name, list(t.grain)) for t in tables}
        frames = {t.name: t.df for t in tables}

        def total(name: str, column: str) -> int:
            return frames[name].agg(F.sum(column)).first()[0]

        check_equal("employee_performance_history", "rows", rows["employee_performance_history"], n_reviews)
        check_equal("skill_gaps", "rows", rows["skill_gaps"], n_assessments)
        for name in ("promotion_pipeline", "employee_learning_summary", "attrition_risk"):
            check_equal(name, "rows", rows[name], n_employees)
        check_equal(
            "employee_learning_summary", "sum(total_events)", total("employee_learning_summary", "total_events"), n_events,
        )
        check_equal("team_performance_outcomes", "rows", rows["team_performance_outcomes"], n_teams)
        check_equal("team_performance_outcomes", "sum(headcount)", total("team_performance_outcomes", "headcount"), n_employees)
        check_equal("skill_distribution", "sum(employees)", total("skill_distribution", "employees"), n_assessments)

    metrics: list[DatasetMetric] = []
    with log_step(logger, "write gold datasets"):
        for table in tables:
            target = f"{config.gold_path}/{table.name}"
            out = table.df.withColumn("_generated_at", F.current_timestamp()).withColumn("_run_id", F.lit(config.run_id))
            out.coalesce(1).write.mode("overwrite").parquet(target)
            if config.publish_gold_to_postgres:
                publish_to_postgres(out, config, table.name)
            logger.info("%-28s %6d rows -> %s%s", table.name, rows[table.name], target,
                        f" + postgres gold_{table.name}" if config.publish_gold_to_postgres else "")
            # Gold reshapes Silver, so rows_in is the Gold row count; totals are reconciled above.
            metrics.append(DatasetMetric("gold", table.name, config.silver_path, target, rows[table.name], rows[table.name]))

    for df in (employee_dim, history, gaps, learning, profile):
        df.unpersist()
    return metrics


# =============================================================================
# 14. ORCHESTRATION - parse steps, run them in order, record lineage, exit code
# =============================================================================

# Ordered registry of pipeline steps; a step may return dataset metrics.
STEPS: dict[str, Callable[[SparkSession, PipelineConfig], list[DatasetMetric] | None]] = {
    "check": run_healthcheck,
    "bronze": run_bronze,
    "silver": run_silver,
    "gold": run_gold,
}


def parse_steps(argv: list[str] | None = None) -> list[str]:
    """Parse --steps into an ordered list of known step names."""
    parser = argparse.ArgumentParser(description="ACME talent pipeline (Bronze -> Silver -> Gold)")
    parser.add_argument("--steps", default="all",
                        help=f"Comma-separated steps to run, or 'all'. Available: {', '.join(STEPS)}")
    requested = parser.parse_args(argv).steps
    if requested == "all":
        return list(STEPS)
    steps = [s.strip() for s in requested.split(",") if s.strip()]
    unknown = [s for s in steps if s not in STEPS]
    if unknown:
        parser.error(f"unknown step(s): {', '.join(unknown)}. Available: {', '.join(STEPS)}")
    return [s for s in STEPS if s in steps]  # always run in pipeline order


def log_summary(logger: logging.LoggerAdapter, metrics: list[DatasetMetric]) -> None:
    """Log a reconciliation table: rows in / out / rejected / dropped per dataset."""
    if not metrics:
        return
    logger.info("%-7s %-28s %8s %8s %9s %8s", "layer", "dataset", "rows_in", "rows_out", "rejected", "dropped")
    for m in metrics:
        logger.info("%-7s %-28s %8d %8d %9d %8d", m.layer, m.dataset, m.rows_in, m.rows_out, m.rows_rejected, m.rows_dropped)


def main(argv: list[str] | None = None) -> int:
    """Run the pipeline and return a process exit code (0 = success, 1 = failed, 2 = bad config)."""
    steps = parse_steps(argv)
    try:
        config = PipelineConfig.from_env()
    except ConfigError as exc:
        print(f"CONFIG ERROR: {exc}", file=sys.stderr)
        return 2

    setup_logging(config.run_id)
    logger = get_logger("main")
    logger.info("Pipeline starting: steps=%s run_date=%s lake=%s is_local=%s",
                steps, config.run_date, config.lake_root, config.is_local)

    spark = build_spark(config)
    recorder = RunRecorder(config)
    succeeded = False
    try:
        with log_step(logger, f"pipeline {steps}"):
            for step in steps:
                with recorder.step(step):
                    recorder.datasets += STEPS[step](spark, config) or []
        succeeded = True
    except Exception:  # already logged with traceback by log_step
        pass
    finally:
        recorder.finish(succeeded)
        log_summary(logger, recorder.datasets)
        try:
            logger.info("Lineage manifest written: %s", write_run_lineage(spark, recorder))
        except Exception:  # never let lineage failures hide the pipeline's own outcome
            logger.exception("Could not write lineage for this run")
        spark.stop()

    logger.info("Pipeline %s", recorder.status)
    return 0 if succeeded else 1


if __name__ == "__main__":
    sys.exit(main())
