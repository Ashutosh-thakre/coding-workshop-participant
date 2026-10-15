"""SparkSession factory with the PostgreSQL and S3 connectors configured.

Spark runs in local mode (all cores of one machine) both on a laptop and inside
the Kubernetes job pod; the dataset is small enough that a cluster is not needed.
"""

from __future__ import annotations

from pyspark.sql import SparkSession

from pipeline.config import PipelineConfig

# Connector versions must match the Spark build: Spark 4.1.2 ships Hadoop 3.4.2.
POSTGRES_JDBC_PACKAGE = "org.postgresql:postgresql:42.7.7"
HADOOP_AWS_PACKAGE = "org.apache.hadoop:hadoop-aws:3.4.2"


def build_spark(config: PipelineConfig, app_name: str = "talent-pipeline") -> SparkSession:
    """Create (or reuse) a SparkSession configured for this pipeline run."""
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.jars.packages", f"{POSTGRES_JDBC_PACKAGE},{HADOOP_AWS_PACKAGE}")
        # Idempotent writes: overwriting a partitioned table replaces only the partitions written.
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.session.timeZone", "UTC")
        # Small data: the default of 200 shuffle partitions only adds overhead.
        .config("spark.sql.shuffle.partitions", "8")
        # Progress bars flood batch logs; our own step logs report progress instead.
        .config("spark.ui.showConsoleProgress", "false")
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
