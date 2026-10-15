"""Configuration: environment variables in, typed settings out, fail fast on bad input."""

from __future__ import annotations

from datetime import date

import pytest

import job

REQUIRED = {
    "POSTGRES_HOST": "db.example",
    "POSTGRES_NAME": "hr",
    "POSTGRES_USER": "etl",
    "POSTGRES_PASS": "s3cret-password",
    "LAKE_BUCKET": "coding-workshop-datalake-test",
}


@pytest.fixture
def env(monkeypatch: pytest.MonkeyPatch) -> pytest.MonkeyPatch:
    """A clean environment containing only the required variables."""
    for name in [*REQUIRED, "IS_LOCAL", "POSTGRES_PORT", "LAKE_ROOT", "RUN_DATE", "AWS_ENDPOINT_URL",
                 "AWS_REGION", "PUBLISH_GOLD_TO_POSTGRES", "SPARK_CORES", "SPARK_DRIVER_MEMORY", "SPARK_JAVA_OPTIONS"]:
        monkeypatch.delenv(name, raising=False)
    for name, value in REQUIRED.items():
        monkeypatch.setenv(name, value)
    return monkeypatch


def test_defaults_from_required_variables(env: pytest.MonkeyPatch) -> None:
    config = job.PipelineConfig.from_env()
    assert config.is_local is True
    assert config.postgres.port == 5432
    assert config.lake_root == "s3a://coding-workshop-datalake-test"
    assert config.s3_endpoint is None
    assert config.aws_region == "us-east-1"
    assert config.publish_gold_to_postgres is True
    assert config.run_id and config.run_id != job.PipelineConfig.from_env().run_id  # unique per run


@pytest.mark.parametrize("missing", list(REQUIRED))
def test_missing_required_variable_fails_fast(env: pytest.MonkeyPatch, missing: str) -> None:
    env.delenv(missing)
    with pytest.raises(job.ConfigError, match=missing if missing != "LAKE_BUCKET" else "LAKE_BUCKET"):
        job.PipelineConfig.from_env()


def test_lake_root_overrides_bucket(env: pytest.MonkeyPatch) -> None:
    env.setenv("LAKE_ROOT", "/tmp/my-lake/")
    assert job.PipelineConfig.from_env().lake_root == "/tmp/my-lake"


def test_zone_paths_derive_from_lake_root(env: pytest.MonkeyPatch) -> None:
    config = job.PipelineConfig.from_env()
    root = config.lake_root
    assert (config.landing_path, config.bronze_path, config.silver_path) == (f"{root}/landing", f"{root}/bronze", f"{root}/silver")
    assert (config.gold_path, config.quarantine_path, config.lineage_path) == (f"{root}/gold", f"{root}/quarantine", f"{root}/lineage")


def test_run_date_parsing(env: pytest.MonkeyPatch) -> None:
    env.setenv("RUN_DATE", "2026-03-01")
    assert job.PipelineConfig.from_env().run_date == date(2026, 3, 1)
    env.setenv("RUN_DATE", "01/03/2026")
    with pytest.raises(job.ConfigError, match="RUN_DATE"):
        job.PipelineConfig.from_env()


def test_invalid_port_rejected(env: pytest.MonkeyPatch) -> None:
    env.setenv("POSTGRES_PORT", "abc")
    with pytest.raises(job.ConfigError, match="POSTGRES_PORT"):
        job.PipelineConfig.from_env()


def test_cloud_mode_requires_ssl_and_local_does_not(env: pytest.MonkeyPatch) -> None:
    assert "sslmode" not in job.PipelineConfig.from_env().postgres.jdbc_url
    env.setenv("IS_LOCAL", "false")
    url = job.PipelineConfig.from_env().postgres.jdbc_url
    assert url.startswith("jdbc:postgresql://db.example:5432/hr?")
    assert "sslmode=require" in url and "connectTimeout=30" in url


def test_password_never_printed(env: pytest.MonkeyPatch) -> None:
    config = job.PipelineConfig.from_env()
    assert "s3cret-password" not in repr(config)
    assert "s3cret-password" not in repr(config.postgres)


@pytest.mark.parametrize(("raw", "expected"), [("true", True), ("TRUE", True), ("1", True), ("yes", True),
                                               ("false", False), ("0", False), ("", False)])
def test_parse_bool(raw: str, expected: bool) -> None:
    assert job._parse_bool(raw) is expected


def test_spark_resources_default_and_override(env: pytest.MonkeyPatch) -> None:
    config = job.PipelineConfig.from_env()
    assert (config.spark_cores, config.spark_driver_memory) == ("*", "512m")
    assert config.spark_java_options == job.DEFAULT_JAVA_OPTIONS and "-XX:+UseSerialGC" in config.spark_java_options
    env.setenv("SPARK_CORES", "1")
    env.setenv("SPARK_DRIVER_MEMORY", "768m")
    env.setenv("SPARK_JAVA_OPTIONS", "-Xss1m")
    config = job.PipelineConfig.from_env()
    assert (config.spark_cores, config.spark_driver_memory, config.spark_java_options) == ("1", "768m", "-Xss1m")
