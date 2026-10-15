"""Integration tests: real reads/writes against PostgreSQL and S3 (LocalStack).

Prerequisites: `source env.local`, Postgres and LocalStack running, and the sources
seeded with scripts/seed_sources.sh.
"""

from __future__ import annotations

import os
import time
import uuid
from collections.abc import Iterator

import boto3
import psycopg
import pytest
from pyspark.sql import SparkSession

import job
from conftest import make_config, requires_services

pytestmark = [pytest.mark.integration, requires_services]

BUCKET = os.environ.get("LAKE_BUCKET", "")
EXPECTED_LANDING = {"files": 783, "bytes": 7_302_325, "lines": 29_868}   # the sample export


def _pg_connect() -> psycopg.Connection:
    return psycopg.connect(host=os.environ["POSTGRES_HOST"], port=os.environ.get("POSTGRES_PORT", "5432"),
                           dbname=os.environ["POSTGRES_NAME"], user=os.environ["POSTGRES_USER"],
                           password=os.environ["POSTGRES_PASS"])


@pytest.fixture
def s3_prefix() -> Iterator[str]:
    """A unique S3 prefix for one test, deleted afterwards."""
    prefix = f"pytest/{uuid.uuid4().hex[:8]}"
    yield prefix
    s3 = boto3.client("s3")
    for obj in s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix).get("Contents", []):
        s3.delete_object(Bucket=BUCKET, Key=obj["Key"])


@pytest.fixture
def s3_config(lake) -> job.PipelineConfig:
    return make_config(f"s3a://{BUCKET}")


# --- PostgreSQL ---------------------------------------------------------------------

@pytest.mark.parametrize("table", job.POSTGRES_TABLES)
def test_postgres_read_matches_an_independent_count(spark: SparkSession, s3_config: job.PipelineConfig, table: str) -> None:
    with _pg_connect() as conn:
        expected = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]  # noqa: S608 - fixed table names
    df = job.read_postgres_table(spark, s3_config, table)
    assert df.count() == expected > 0
    df.unpersist()


def test_postgres_types_are_preserved(spark: SparkSession, s3_config: job.PipelineConfig) -> None:
    types = dict(job.read_postgres_table(spark, s3_config, "projects").dtypes)
    assert types["project_id"] == "int" and types["outcome_score"] == "decimal(5,2)"
    assert dict(job.read_postgres_table(spark, s3_config, "employees").dtypes)["hire_date"] == "date"


def test_unreachable_postgres_is_retried_with_backoff_then_fails(spark: SparkSession, lake, logs) -> None:
    config = make_config(lake)
    broken = make_config(lake, postgres=job.PostgresConfig("localhost", 1, "x", "u", "p"))   # nothing listens on port 1
    assert config.postgres.port != 1
    started = time.perf_counter()
    with pytest.raises(Exception, match="(?i)connect"):
        job.read_postgres_table(spark, broken, "employees")
    elapsed = time.perf_counter() - started
    out = logs.readouterr().out
    assert out.count("retrying in") == 2 and "retrying in 2.0s" in out and "retrying in 4.0s" in out
    assert "read_postgres_table failed after 3 attempt(s)" in out
    assert elapsed >= 6  # waited 2s + 4s between the three attempts


def test_publish_gold_to_postgres_round_trip(spark: SparkSession, s3_config: job.PipelineConfig) -> None:
    table = f"pytest_{uuid.uuid4().hex[:6]}"
    try:
        job.publish_to_postgres(spark.createDataFrame([(1, ["a", "b"]), (2, [])], "id long, tags array<string>"), s3_config, table)
        job.publish_to_postgres(spark.createDataFrame([(3, ["c"])], "id long, tags array<string>"), s3_config, table)  # replaces
        with _pg_connect() as conn:
            assert conn.execute(f"SELECT id, tags FROM gold_{table}").fetchall() == [(3, ["c"])]  # noqa: S608
    finally:
        with _pg_connect() as conn:
            conn.execute(f"DROP TABLE IF EXISTS gold_{table}")  # noqa: S608


# --- S3 -------------------------------------------------------------------------------

def test_s3_parquet_round_trip(spark: SparkSession, s3_prefix: str) -> None:
    path = f"s3a://{BUCKET}/{s3_prefix}/table"
    spark.createDataFrame([(1, "a"), (2, "b")], "id int, v string").write.mode("overwrite").parquet(path)
    assert sorted(tuple(r) for r in spark.read.parquet(path).collect()) == [(1, "a"), (2, "b")]
    keys = [o["Key"] for o in boto3.client("s3").list_objects_v2(Bucket=BUCKET, Prefix=s3_prefix)["Contents"]]
    assert any(k.endswith(".parquet") for k in keys)


def test_s3_listing_includes_empty_objects(spark: SparkSession, s3_prefix: str) -> None:
    s3 = boto3.client("s3")
    s3.put_object(Bucket=BUCKET, Key=f"{s3_prefix}/day=01/events-000.jsonl", Body=b'{"a": 1}\n')
    s3.put_object(Bucket=BUCKET, Key=f"{s3_prefix}/day=02/events-000.jsonl", Body=b"")
    files = job.list_files(spark, f"s3a://{BUCKET}/{s3_prefix}")
    assert [size for _, size in files] == [9, 0]


def test_s3_manifest_is_readable_by_other_tools(spark: SparkSession, s3_prefix: str) -> None:
    job.write_text(spark, f"s3a://{BUCKET}/{s3_prefix}/run.json", '{"status": "SUCCEEDED"}')
    body = boto3.client("s3").get_object(Bucket=BUCKET, Key=f"{s3_prefix}/run.json")["Body"].read()
    assert body == b'{"status": "SUCCEEDED"}'


def test_landing_zone_matches_the_source_export(spark: SparkSession, s3_config: job.PipelineConfig) -> None:
    files = job.list_files(spark, f"{s3_config.landing_path}/learning_activity")
    assert len(files) == EXPECTED_LANDING["files"]
    assert sum(size for _, size in files) == EXPECTED_LANDING["bytes"]
    lines = job.read_text_lines(spark, f"{s3_config.landing_path}/learning_activity")
    assert lines.count() == EXPECTED_LANDING["lines"]
    lines.unpersist()


def test_healthcheck_passes(spark: SparkSession, s3_config: job.PipelineConfig, logs) -> None:
    job.run_healthcheck(spark, s3_config)
    out = logs.readouterr().out
    assert "employees rows=2000" in out and f"lines={EXPECTED_LANDING['lines']}" in out and "is writable" in out


def test_run_bronze_ingests_postgres_and_landing(spark: SparkSession, lake) -> None:
    """Full Bronze step: real Postgres tables + a small local landing zone into a temporary lake."""
    config = make_config(lake)
    day = lake / "landing" / "learning_activity" / "year=2024" / "month=01" / "day=01"
    day.mkdir(parents=True)
    (day / "events-000.jsonl").write_text('{"event_id": "EV0000000001"}\n{"event_id": "EV0000000002"}\n')
    metrics = {m.dataset: (m.rows_in, m.rows_out) for m in job.run_bronze(spark, config)}
    with _pg_connect() as conn:
        expected = {t: conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0] for t in job.POSTGRES_TABLES}  # noqa: S608
    assert metrics == {**{t: (n, n) for t, n in expected.items()}, "learning_activity": (2, 2)}


def test_gold_publishing_to_postgres_from_run_gold(spark: SparkSession, lake, monkeypatch: pytest.MonkeyPatch) -> None:
    """run_gold with publishing enabled writes Postgres tables (uses the real Silver from the lake).

    Publishing is redirected to temporary gold_pytest_* tables so the test never touches the
    real gold_* tables that the notebook reads.
    """
    real = make_config(f"s3a://{BUCKET}")
    config = make_config(lake, publish_gold_to_postgres=True)
    for name in ["employees", "teams", "performance_reviews", "competency_assessments", "competencies",
                 "talent_review", "projects", "learning_activity"]:
        spark.read.parquet(f"{real.silver_path}/{name}").write.mode("overwrite").parquet(f"{config.silver_path}/{name}")

    original, published = job.publish_to_postgres, []

    def to_test_table(df, cfg, table: str) -> None:
        published.append(f"pytest_{table}")
        original(df, cfg, f"pytest_{table}")

    monkeypatch.setattr(job, "publish_to_postgres", to_test_table)
    try:
        metrics = {m.dataset: m.rows_out for m in job.run_gold(spark, config)}
        with _pg_connect() as conn:
            assert conn.execute("SELECT COUNT(*) FROM gold_pytest_attrition_risk").fetchone()[0] == metrics["attrition_risk"] == 2000
        assert len(published) == 7
    finally:
        with _pg_connect() as conn:
            for table in published:
                conn.execute(f"DROP TABLE IF EXISTS gold_{table}")  # noqa: S608
