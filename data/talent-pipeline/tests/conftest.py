"""Shared pytest fixtures for the talent pipeline tests.

Unit tests run Spark in local mode against a temporary folder (no external services).
Integration / e2e tests need PostgreSQL and LocalStack and are skipped when those
are unreachable. Run from the job folder after `source env.local`:

    pytest -m "not integration and not e2e"     # unit tests only (~1 min)
    pytest                                      # everything
"""

from __future__ import annotations

import dataclasses
import os
import socket
import sys
from datetime import date
from pathlib import Path

import pytest

JOB_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(JOB_DIR))

import job  # noqa: E402

RUN_DATE = date(2026, 1, 15)


def _reachable(host: str, port: int) -> bool:
    """True if a TCP connection to host:port succeeds within 2 seconds."""
    try:
        with socket.create_connection((host, port), timeout=2):
            return True
    except OSError:
        return False


POSTGRES_UP = bool(os.environ.get("POSTGRES_PASS")) and _reachable(
    os.environ.get("POSTGRES_HOST", "localhost"), int(os.environ.get("POSTGRES_PORT", "5432"))
)
S3_UP = bool(os.environ.get("AWS_ENDPOINT_URL") and os.environ.get("LAKE_BUCKET")) and _reachable("localhost", 4566)

requires_services = pytest.mark.skipif(
    not (POSTGRES_UP and S3_UP), reason="PostgreSQL/LocalStack not reachable - run `source env.local` and start them"
)


def make_config(lake_root: str | Path, run_date: date = RUN_DATE, **overrides: object) -> job.PipelineConfig:
    """A PipelineConfig for tests: local lake folder, Postgres from env (or dummies), no Gold publishing."""
    postgres = job.PostgresConfig(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        database=os.environ.get("POSTGRES_NAME", "postgres"),
        user=os.environ.get("POSTGRES_USER", "postgres"),
        password=os.environ.get("POSTGRES_PASS", "unused"),
    )
    config = job.PipelineConfig(
        is_local=True,
        postgres=postgres,
        lake_root=str(lake_root),
        s3_endpoint=os.environ.get("AWS_ENDPOINT_URL") or None,
        aws_region=os.environ.get("AWS_REGION", "us-east-1"),
        run_date=run_date,
        run_id="test-run",
        publish_gold_to_postgres=False,
    )
    return dataclasses.replace(config, **overrides)


@pytest.fixture(scope="session")
def spark(tmp_path_factory: pytest.TempPathFactory):
    """One local SparkSession for the whole test session (startup is expensive)."""
    job.setup_logging("pytest")
    session = job.build_spark(make_config(tmp_path_factory.mktemp("session-lake")))
    yield session
    session.stop()


@pytest.fixture
def logs(capsys: pytest.CaptureFixture[str]) -> pytest.CaptureFixture[str]:
    """Capture pipeline log lines: re-attach the log handler to this test's captured stdout."""
    job.setup_logging("pytest")
    return capsys


@pytest.fixture
def lake(tmp_path: Path) -> Path:
    """A fresh, empty lake folder for one test."""
    path = tmp_path / "lake"
    path.mkdir()
    return path


@pytest.fixture
def config(lake: Path) -> job.PipelineConfig:
    """Config pointing at the per-test lake folder."""
    return make_config(lake)
