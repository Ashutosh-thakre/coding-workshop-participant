"""End-to-end smoke + idempotency test: run the real job twice as a separate process.

Checks exit codes, identical reconciliation across runs, no duplicate rows in
Bronze/Silver, expected Gold tables in Postgres, and the runtime SLA.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import boto3
import psycopg
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from conftest import JOB_DIR, make_config, requires_services

pytestmark = [pytest.mark.e2e, requires_services]

SLA_SECONDS = 600   # a daily batch of this size must finish within 10 minutes
EXPECTED_GOLD_ROWS = {
    "employee_performance_history": 4000, "skill_gaps": 4000, "promotion_pipeline": 2000,
    "employee_learning_summary": 2000, "attrition_risk": 2000, "team_performance_outcomes": 100,
    "skill_distribution": 1595,
}


def _run_job() -> dict:
    """Run job.py in a fresh process; return its lineage manifest."""
    result = subprocess.run([sys.executable, str(JOB_DIR / "job.py")], capture_output=True, text=True,
                            env=os.environ.copy(), timeout=SLA_SECONDS + 300, check=False)
    assert result.returncode == 0, result.stdout[-3000:] + result.stderr[-3000:]
    manifest_uri = re.search(r"Lineage manifest written: s3a://([^/]+)/(\S+)", result.stdout)
    assert manifest_uri, "no lineage manifest in output"
    body = boto3.client("s3").get_object(Bucket=manifest_uri.group(1), Key=manifest_uri.group(2))["Body"].read()
    return json.loads(body)


@pytest.fixture(scope="module")
def two_runs() -> tuple[dict, dict]:
    return _run_job(), _run_job()


def _counts(manifest: dict) -> dict:
    return {(d["layer"], d["dataset"]): (d["rows_in"], d["rows_out"], d["rows_rejected"], d["rows_dropped"])
            for d in manifest["datasets"]}


def test_both_runs_succeed_within_sla(two_runs: tuple[dict, dict]) -> None:
    for manifest in two_runs:
        assert manifest["status"] == "SUCCEEDED"
        assert [s["name"] for s in manifest["steps"]] == ["check", "bronze", "silver", "gold"]
        assert manifest["duration_seconds"] < SLA_SECONDS


def test_reruns_produce_identical_reconciliation(two_runs: tuple[dict, dict]) -> None:
    first, second = (_counts(m) for m in two_runs)
    assert first == second
    assert first[("bronze", "learning_activity")] == (29868, 29868, 0, 0)
    assert first[("silver", "learning_activity")] == (29868, 29574, 143, 151)


def test_no_duplicates_in_the_lake_after_two_runs(spark: SparkSession, two_runs: tuple[dict, dict]) -> None:
    config = make_config(f"s3a://{os.environ['LAKE_BUCKET']}")
    run_date = two_runs[1]["run_date"]
    bronze = spark.read.parquet(f"{config.bronze_path}/learning_activity").where(F.col("ingest_date") == run_date)
    assert bronze.count() == 29868
    assert bronze.select("_run_id").distinct().count() == 1           # second run replaced the first
    silver = spark.read.parquet(f"{config.silver_path}/learning_activity")
    assert silver.count() == silver.select("event_id").distinct().count() == 29574


def test_gold_tables_are_queryable_in_postgres(two_runs: tuple[dict, dict]) -> None:
    with psycopg.connect(host=os.environ["POSTGRES_HOST"], port=os.environ.get("POSTGRES_PORT", "5432"),
                         dbname=os.environ["POSTGRES_NAME"], user=os.environ["POSTGRES_USER"],
                         password=os.environ["POSTGRES_PASS"]) as conn:
        counts = {t: conn.execute(f"SELECT COUNT(*) FROM gold_{t}").fetchone()[0] for t in EXPECTED_GOLD_ROWS}  # noqa: S608
        candidates = conn.execute("SELECT COUNT(*) FROM gold_promotion_pipeline WHERE is_promotion_candidate").fetchone()[0]
    assert counts == EXPECTED_GOLD_ROWS
    assert candidates == 166   # matches the independent SQL cross-check against the source tables


def test_manifest_lists_every_dataset(two_runs: tuple[dict, dict]) -> None:
    layers = {}
    for d in two_runs[1]["datasets"]:
        layers.setdefault(d["layer"], []).append(d["dataset"])
    assert {k: len(v) for k, v in layers.items()} == {"bronze": 8, "silver": 8, "gold": 7}
