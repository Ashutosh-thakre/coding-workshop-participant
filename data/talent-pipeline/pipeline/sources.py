"""Readers for the pipeline's source systems, shared by the health check and Bronze."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pipeline.config import PipelineConfig
from pipeline.resilience import retry


@retry(attempts=3, base_delay=2.0)
def read_postgres_table(spark: SparkSession, config: PipelineConfig, table: str) -> DataFrame:
    """Read a full PostgreSQL table over JDBC.

    The returned DataFrame is cached, so the database is queried exactly once
    (inside this retried function) and later actions don't hit it again.
    """
    df = (
        spark.read.format("jdbc")
        .option("url", config.postgres.jdbc_url)
        .option("dbtable", table)
        .option("user", config.postgres.user)
        .option("password", config.postgres.password)
        .option("driver", "org.postgresql.Driver")
        .load()
        .cache()
    )
    df.count()  # materialise now, so connection errors surface here and are retried
    return df


@retry(attempts=3, base_delay=2.0)
def list_files(spark: SparkSession, directory: str, suffix: str = ".jsonl") -> list[tuple[str, int]]:
    """List every file under a directory as (path, size_in_bytes), including empty files.

    Uses the Hadoop FileSystem API (works for s3a:// and local paths). Spark's
    DataFrame readers can't be used here: they silently skip zero-byte files.
    """
    jvm = spark.sparkContext._jvm
    path = jvm.org.apache.hadoop.fs.Path(directory)
    fs = path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())

    files = []
    iterator = fs.listFiles(path, True)  # True = recursive
    while iterator.hasNext():
        status = iterator.next()
        file_path = status.getPath().toString()
        if file_path.endswith(suffix):
            files.append((file_path, status.getLen()))
    return sorted(files)


@retry(attempts=3, base_delay=2.0)
def read_text_lines(spark: SparkSession, directory: str, file_glob: str = "*.jsonl") -> DataFrame:
    """Read every matching file under a directory, line by line, as raw text.

    Returns one row per line with columns 'value' (the line) and 'source_file'
    (full path of the file it came from). Raw text keeps malformed lines instead
    of silently dropping them; parsing happens later, in Silver, where failures
    can be quarantined.
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
