"""Bronze layer: audit columns, idempotent partition writes, empty-source guard, file listing."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

import job
from conftest import make_config


def test_audit_columns_are_added(spark: SparkSession, config: job.PipelineConfig) -> None:
    df = job.add_audit_columns(spark.createDataFrame([(1,)], "id int"), config, "postgres", "public.t")
    row = df.collect()[0]
    assert row["_source_system"] == "postgres" and row["_source_object"] == "public.t"
    assert row["_run_id"] == "test-run" and row[job.BRONZE_PARTITION] == config.run_date
    assert row["_ingested_at"] is not None


def _write(spark: SparkSession, config: job.PipelineConfig, rows: list[tuple[int]]) -> int:
    df = job.add_audit_columns(spark.createDataFrame(rows, "id int"), config, "test", "t")
    return job.write_bronze(spark, df, f"{config.bronze_path}/t", config)


def test_rerunning_the_same_date_replaces_the_partition(spark: SparkSession, config: job.PipelineConfig) -> None:
    assert _write(spark, config, [(1,), (2,)]) == 2
    assert _write(spark, config, [(1,), (2,)]) == 2        # same date again: no duplicates
    assert spark.read.parquet(f"{config.bronze_path}/t").count() == 2


def test_other_dates_are_kept_as_history(spark: SparkSession, lake: Path) -> None:
    day1, day2 = make_config(lake, run_date=date(2026, 1, 1)), make_config(lake, run_date=date(2026, 1, 2))
    _write(spark, day1, [(1,), (2,)])
    _write(spark, day2, [(3,)])
    _write(spark, day2, [(3,), (4,)])                       # rerun of day 2 only
    counts = {r[job.BRONZE_PARTITION]: r["count"] for r in
              spark.read.parquet(f"{day1.bronze_path}/t").groupBy(job.BRONZE_PARTITION).count().collect()}
    assert counts == {date(2026, 1, 1): 2, date(2026, 1, 2): 2}


def test_empty_source_halts_the_pipeline(spark: SparkSession, config: job.PipelineConfig) -> None:
    empty = spark.createDataFrame([], "id int")
    with pytest.raises(job.EmptySourceError, match="returned 0 rows"):
        job._ingest(spark, empty, config, "t", "postgres", "public.t")


def test_ingest_returns_reconciled_metric(spark: SparkSession, config: job.PipelineConfig) -> None:
    metric = job._ingest(spark, spark.createDataFrame([(1,), (2,), (3,)], "id int"), config, "t", "postgres", "public.t")
    assert (metric.layer, metric.dataset, metric.rows_in, metric.rows_out) == ("bronze", "t", 3, 3)
    assert metric.source == "postgres:public.t"


def _landing(lake: Path) -> Path:
    base = lake / "landing" / "learning_activity"
    (base / "year=2024/month=01/day=01").mkdir(parents=True)
    (base / "year=2024/month=01/day=02").mkdir(parents=True)
    (base / "year=2024/month=01/day=01/events-000.jsonl").write_text('{"a": 1}\n{"a": 2}\nnot json\n')
    (base / "year=2024/month=01/day=02/events-000.jsonl").write_text("")   # empty daily drop
    (base / "year=2024/month=01/day=02/readme.txt").write_text("ignored")
    return base


def test_list_files_includes_empty_files_and_filters_suffix(spark: SparkSession, lake: Path) -> None:
    files = job.list_files(spark, str(_landing(lake)))
    assert [(Path(p).parent.name, size) for p, size in files] == [("day=01", 27), ("day=02", 0)]


def test_ingest_landing_keeps_malformed_lines_and_warns_on_empty_files(
    spark: SparkSession, config: job.PipelineConfig, lake: Path, logs: pytest.CaptureFixture[str],
) -> None:
    _landing(lake)
    [metric] = job.ingest_landing(spark, config)
    assert metric.rows_in == metric.rows_out == 3            # the malformed line is kept for Silver
    out = logs.readouterr().out
    assert "2 files, 27 bytes" in out
    assert "WARNING" in out and "empty file (0 bytes)" in out and "day=02" in out

    bronze = spark.read.parquet(f"{config.bronze_path}/learning_activity")
    assert set(bronze.columns) >= {"raw_line", "_source_file", "_ingested_at", "_run_id", job.BRONZE_PARTITION}
    assert all("day=01" in r._source_file for r in bronze.collect())


def test_write_text_creates_and_overwrites_a_single_file(spark: SparkSession, lake: Path) -> None:
    path = lake / "lineage" / "x.json"
    job.write_text(spark, str(path), "first")
    job.write_text(spark, str(path), "second")
    assert path.read_text() == "second"


def test_write_mismatch_is_detected(spark: SparkSession, config: job.PipelineConfig, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(job, "write_bronze", lambda *args, **kwargs: 1)   # pretend a row went missing on write
    with pytest.raises(job.BronzeWriteError, match="read 2 rows from source but 1 rows in Bronze"):
        job._ingest(spark, spark.createDataFrame([(1,), (2,)], "id int"), config, "t", "postgres", "public.t")
