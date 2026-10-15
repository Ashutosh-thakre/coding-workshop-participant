"""Low-level file operations on the lake via the Hadoop FileSystem API.

Works for any path Spark understands (s3a://..., file:/..., plain local paths).
Used where DataFrame readers/writers don't fit: listing files including empty
ones, and writing a single small file such as a run manifest.
"""

from __future__ import annotations

from pyspark.sql import SparkSession

from pipeline.resilience import retry


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
    """Write (or overwrite) a single UTF-8 text file."""
    fs, hadoop_path = _filesystem(spark, path)
    stream = fs.create(hadoop_path, True)  # True = overwrite
    try:
        stream.write(bytearray(text.encode("utf-8")))
    finally:
        stream.close()


@retry(attempts=3, base_delay=2.0)
def read_text(spark: SparkSession, path: str) -> str:
    """Read a single UTF-8 text file (small files only)."""
    return "\n".join(row.value for row in spark.read.text(path, wholetext=True).collect())
