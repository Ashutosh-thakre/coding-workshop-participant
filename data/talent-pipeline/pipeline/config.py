"""Pipeline configuration, read once from environment variables.

Every setting the pipeline needs lives here. Other modules receive a
``PipelineConfig`` object instead of reading ``os.environ`` themselves, so the
same code runs locally (Postgres + LocalStack) and in the cloud (Aurora + S3)
with only the environment variables changing.
"""

from __future__ import annotations

import os
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, timezone


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
        """JDBC URL used by Spark. AWS Aurora requires SSL; local Postgres does not."""
        url = f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"
        return f"{url}?sslmode=require" if self.require_ssl else url


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
        """Business-level aggregates that answer the analytics questions."""
        return f"{self.lake_root}/gold"

    @property
    def quarantine_path(self) -> str:
        """Rows that failed validation, kept with the reason they failed."""
        return f"{self.lake_root}/quarantine"

    @property
    def lineage_path(self) -> str:
        """Run manifests: what was read, written and rejected at each step."""
        return f"{self.lake_root}/lineage"

    @classmethod
    def from_env(cls) -> "PipelineConfig":
        """Build the configuration from environment variables.

        Required: POSTGRES_HOST, POSTGRES_NAME, POSTGRES_USER, POSTGRES_PASS and
        either LAKE_ROOT (any path/URI) or LAKE_BUCKET (an S3 bucket name).
        Optional: IS_LOCAL (default true), POSTGRES_PORT (5432), AWS_ENDPOINT_URL,
        AWS_REGION (us-east-1), RUN_DATE (YYYY-MM-DD, default today in UTC),
        PUBLISH_GOLD_TO_POSTGRES (default true).
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
            run_date = date.fromisoformat(run_date_raw) if run_date_raw else datetime.now(timezone.utc).date()
        except ValueError as exc:
            raise ConfigError(f"RUN_DATE must be YYYY-MM-DD, got {run_date_raw!r}") from exc

        return cls(
            is_local=is_local,
            postgres=postgres,
            lake_root=lake_root,
            s3_endpoint=os.environ.get("AWS_ENDPOINT_URL", "").strip() or None,
            aws_region=os.environ.get("AWS_REGION", "us-east-1").strip() or "us-east-1",
            run_date=run_date,
            run_id=datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:6],
            publish_gold_to_postgres=_parse_bool(os.environ.get("PUBLISH_GOLD_TO_POSTGRES", "true")),
        )
