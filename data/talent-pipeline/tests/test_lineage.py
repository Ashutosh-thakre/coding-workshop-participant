"""Run-level lineage: step recording, manifest content, append-only dataset metrics."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

import job
from conftest import make_config


def _metric(dataset: str = "employees", rows_in: int = 10, rows_out: int = 8, rejected: int = 1) -> job.DatasetMetric:
    return job.DatasetMetric("silver", dataset, "src", "dst", rows_in, rows_out, rejected)


def test_dataset_metric_computes_dropped_rows() -> None:
    metric = _metric()
    assert metric.rows_dropped == 1
    assert metric.as_dict() == {"layer": "silver", "dataset": "employees", "source": "src", "target": "dst",
                                "rows_in": 10, "rows_out": 8, "rows_rejected": 1, "rows_dropped": 1}


def test_recorder_captures_success_and_failure(config: job.PipelineConfig) -> None:
    recorder = job.RunRecorder(config)
    with recorder.step("bronze"):
        pass
    with pytest.raises(RuntimeError), recorder.step("silver"):
        raise RuntimeError("boom")
    recorder.finish(succeeded=False)

    bronze, silver = recorder.steps
    assert (bronze.status, bronze.error) == ("SUCCEEDED", None)
    assert (silver.status, silver.error) == ("FAILED", "RuntimeError: boom")
    assert bronze.duration_seconds is not None and silver.finished_at is not None
    assert recorder.status == "FAILED"


def test_manifest_has_run_details_and_no_secrets(lake: Path) -> None:
    config = make_config(lake)
    recorder = job.RunRecorder(config)
    recorder.datasets.append(_metric())
    recorder.finish(succeeded=True)
    manifest = recorder.manifest()

    assert manifest["run_id"] == "test-run" and manifest["status"] == "SUCCEEDED"
    assert manifest["run_date"] == config.run_date.isoformat()
    assert manifest["datasets"][0]["rows_dropped"] == 1
    assert config.postgres.password not in json.dumps(manifest)
    # the business rules that produced the data travel with it
    assert manifest["business_rules"]["critical_gap_levels"] == job.CRITICAL_GAP_LEVELS
    assert manifest["business_rules"]["risk_points"] == job.RISK_POINTS


def test_write_run_lineage_writes_manifest_and_appends_metrics(spark: SparkSession, config: job.PipelineConfig) -> None:
    for _ in range(2):  # two runs of the same date: the audit log keeps both
        recorder = job.RunRecorder(config)
        recorder.datasets += [_metric("employees"), _metric("teams")]
        recorder.finish(succeeded=True)
        path = job.write_run_lineage(spark, recorder)

    manifest = json.loads(Path(path).read_text())
    assert manifest["status"] == "SUCCEEDED" and len(manifest["datasets"]) == 2
    assert path.endswith(f"lineage/runs/run_date={config.run_date}/test-run.json")

    metrics = spark.read.parquet(f"{config.lineage_path}/dataset_metrics")
    assert metrics.count() == 4                                  # appended, not overwritten
    assert {r.dataset for r in metrics.collect()} == {"employees", "teams"}


def test_failed_run_still_writes_a_manifest(spark: SparkSession, config: job.PipelineConfig) -> None:
    recorder = job.RunRecorder(config)
    with pytest.raises(ValueError), recorder.step("silver"):
        raise ValueError("bad data")
    recorder.finish(succeeded=False)
    manifest = json.loads(Path(job.write_run_lineage(spark, recorder)).read_text())
    assert manifest["status"] == "FAILED" and manifest["steps"][0]["error"] == "ValueError: bad data"
    assert manifest["datasets"] == []
