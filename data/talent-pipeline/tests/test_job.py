"""Orchestration: step parsing, exit codes, lineage written on success and failure."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

import job


def test_all_steps_by_default() -> None:
    assert job.parse_steps([]) == ["check", "bronze", "silver", "gold"]


def test_selected_steps_always_run_in_pipeline_order() -> None:
    assert job.parse_steps(["--steps", "gold, bronze"]) == ["bronze", "gold"]


def test_unknown_step_is_rejected(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as exc:
        job.parse_steps(["--steps", "bronze,platinum"])
    assert exc.value.code == 2 and "unknown step(s): platinum" in capsys.readouterr().err


def test_missing_configuration_exits_with_code_2(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    monkeypatch.delenv("POSTGRES_HOST", raising=False)
    assert job.main(["--steps", "check"]) == 2
    assert "CONFIG ERROR: Environment variable POSTGRES_HOST" in capsys.readouterr().err


class _SparkProxy:
    """The shared test SparkSession, but main()'s spark.stop() must not stop it."""

    def __init__(self, spark: SparkSession) -> None:
        self._spark = spark

    def __getattr__(self, name: str) -> object:
        return getattr(self._spark, name)

    def stop(self) -> None:
        pass


@pytest.fixture
def pipeline_env(monkeypatch: pytest.MonkeyPatch, spark: SparkSession, lake: Path) -> Path:
    """Environment for main(): local lake, dummy Postgres, shared Spark."""
    for name, value in {"POSTGRES_HOST": "localhost", "POSTGRES_NAME": "db", "POSTGRES_USER": "u",
                        "POSTGRES_PASS": "p", "LAKE_ROOT": str(lake), "RUN_DATE": "2026-01-15"}.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setattr(job, "build_spark", lambda config: _SparkProxy(spark))
    return lake


def _manifest(lake: Path) -> dict:
    [path] = (lake / "lineage" / "runs" / "run_date=2026-01-15").glob("*.json")
    return json.loads(path.read_text())


def test_main_succeeds_and_writes_lineage(monkeypatch: pytest.MonkeyPatch, pipeline_env: Path, logs) -> None:
    calls: list[str] = []
    monkeypatch.setattr(job, "STEPS", {
        "bronze": lambda spark, config: calls.append("bronze") or [job.DatasetMetric("bronze", "t", "s", "d", 3, 3)],
        "silver": lambda spark, config: calls.append("silver") or None,
    })
    assert job.main([]) == 0
    assert calls == ["bronze", "silver"]
    manifest = _manifest(pipeline_env)
    assert manifest["status"] == "SUCCEEDED" and [s["name"] for s in manifest["steps"]] == ["bronze", "silver"]
    out = logs.readouterr().out
    assert "Pipeline SUCCEEDED" in out and "bronze  t" in out   # reconciliation summary line


def test_main_stops_at_first_failure_and_exits_1(monkeypatch: pytest.MonkeyPatch, pipeline_env: Path, logs) -> None:
    calls: list[str] = []

    def failing(spark: SparkSession, config: job.PipelineConfig) -> None:
        calls.append("silver")
        raise job.SilverCheckError("broken")

    monkeypatch.setattr(job, "STEPS", {"silver": failing, "gold": lambda s, c: calls.append("gold")})
    assert job.main([]) == 1
    assert calls == ["silver"]                                    # gold never ran
    manifest = _manifest(pipeline_env)
    assert manifest["status"] == "FAILED"
    assert manifest["steps"][0]["error"] == "SilverCheckError: broken"
    assert "Pipeline FAILED" in logs.readouterr().out


def test_lineage_failure_does_not_hide_the_pipeline_result(monkeypatch: pytest.MonkeyPatch, pipeline_env: Path, logs) -> None:
    def broken_lineage(spark: SparkSession, recorder: job.RunRecorder) -> str:
        raise OSError("S3 unavailable")

    monkeypatch.setattr(job, "STEPS", {"bronze": lambda spark, config: []})
    monkeypatch.setattr(job, "write_run_lineage", broken_lineage)
    assert job.main([]) == 0                                     # the pipeline itself succeeded
    out = logs.readouterr().out
    assert "Could not write lineage for this run" in out and "Pipeline SUCCEEDED" in out
