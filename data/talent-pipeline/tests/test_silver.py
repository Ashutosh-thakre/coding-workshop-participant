"""Silver layer component test: a tiny Bronze with one example of every data problem.

Runs the real run_silver() against a temporary lake and checks where every row ends up:
Silver, quarantine (with stage + reason) or dropped as a duplicate - and that a rerun
produces exactly the same result (idempotency).
"""

from __future__ import annotations

import json
from datetime import date
from decimal import Decimal

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import types as T

import job
from conftest import make_config

FOLDER = "s3a://bucket/landing/learning_activity/year=2024/month=08/day=02/events-000.jsonl"


def _event(event_id: str, **overrides: object) -> str:
    record = {"event_id": event_id, "employee_id": "1", "event_date": "2024-08-02T10:00:00",
              "event_type": "enrollment", "course_name": "SQL", "text": "started", "score": None}
    record.update(overrides)
    return json.dumps({k: v for k, v in record.items() if v is not ...})


BRONZE: dict[str, tuple[str, list[tuple]]] = {
    "teams": ("team_id int, name string, department string",
              [(1, "Team A", "Engineering"), (2, "Team B", "Sales")]),
    "competencies": ("competency_id int, name string, category string", [(10, "Python", "Technical")]),
    "employees": ("employee_id int, first_name string, last_name string, email string, team_id int, job_level string, hire_date date",
                  [(1, "Ann", "Lee", "ann@acme.example", 1, "IC2", date(2020, 1, 15)),
                   (2, "Bob", "Ray", "bob@acme.example", 99, "Manager", date(2021, 1, 15)),   # team 99 does not exist
                   (3, "Cat", "Kim", "cat@acme.example", 2, "IC1", date(2022, 1, 15))]),
    "projects": ("project_id int, name string, team_id int, outcome_status string, outcome_score decimal(5,2)",
                 [(1, "Launch", 1, "on-time", Decimal("80.50"))]),
    "performance_reviews": ("review_id int, employee_id int, review_period string, rating int, manager_comment string, submitted_date date",
                            [(1, 1, "2024-H2", 4, "Good", date(2025, 1, 10)),
                             (2, 2, "2024-H2", 3, "Ok", date(2025, 1, 10)),       # employee 2 is quarantined -> cascade
                             (3, 1, "2025-H1", 7, "Wow", date(2025, 7, 10))]),    # rating out of range
    "competency_assessments": ("assessment_id int, employee_id int, competency_id int, proficiency_level int, required_level int, assessed_date date",
                               [(1, 1, 10, 2, 4, date(2025, 9, 2))]),
    "talent_review": ("employee_id int, review_cycle string, potential_rating string, promotion_readiness string, attrition_risk string",
                      [(1, "2025-H1", "High", "Ready Now", "Low")]),
    "learning_activity": ("raw_line string, _source_file string", [
        (_event("EV0000000001"), FOLDER),
        (_event("EV0000000001"), FOLDER),                                          # exact duplicate
        ('{"event_id": "EV0000000003", "employee_id": "1', FOLDER),               # truncated JSON
        (_event("EV0000000004", event_date="yesterday"), FOLDER),                 # bad date
        (_event("EV0000000005", event_date="2024-08-05T10:00:00"), FOLDER),       # wrong day folder
        (_event("EV0000000006", text=..., notes="from notes"), FOLDER),           # schema drift
        (_event("EV0000000007", employee_id="999"), FOLDER),                      # unknown employee
    ]),
}


def _seed_bronze(spark: SparkSession, config: job.PipelineConfig) -> None:
    for name, (schema, rows) in BRONZE.items():
        df = job.add_audit_columns(spark.createDataFrame(rows, schema), config, "test", f"test.{name}")
        job.write_bronze(spark, df, f"{config.bronze_path}/{name}", config)


def _quarantine(spark: SparkSession, config: job.PipelineConfig, table: str) -> DataFrame:
    return spark.read.parquet(f"{config.quarantine_path}/{table}")


@pytest.fixture(scope="module")
def silver_lake(spark: SparkSession, tmp_path_factory: pytest.TempPathFactory) -> tuple[job.PipelineConfig, dict]:
    """Seed Bronze and run Silver once for the whole module (tests only read the results)."""
    config = make_config(tmp_path_factory.mktemp("silver-lake"))
    _seed_bronze(spark, config)
    return config, {m.dataset: m for m in job.run_silver(spark, config)}


@pytest.fixture
def silver_run(silver_lake: tuple[job.PipelineConfig, dict]) -> dict[str, job.DatasetMetric]:
    return silver_lake[1]


@pytest.fixture
def config(silver_lake: tuple[job.PipelineConfig, dict]) -> job.PipelineConfig:   # overrides conftest.config
    return silver_lake[0]


def test_every_table_reconciles(silver_run: dict[str, job.DatasetMetric]) -> None:
    counts = {name: (m.rows_in, m.rows_out, m.rows_rejected, m.rows_dropped) for name, m in silver_run.items()}
    assert counts == {
        "teams": (2, 2, 0, 0),
        "competencies": (1, 1, 0, 0),
        "employees": (3, 2, 1, 0),
        "projects": (1, 1, 0, 0),
        "performance_reviews": (3, 1, 2, 0),
        "competency_assessments": (1, 1, 0, 0),
        "talent_review": (1, 1, 0, 0),
        "learning_activity": (7, 2, 4, 1),   # 7 = 2 kept + 4 quarantined + 1 duplicate
    }


def test_quarantine_reasons_and_stages(spark: SparkSession, config: job.PipelineConfig, silver_run: dict) -> None:
    events = {json.loads(r._dq_raw).get("event_id") if r._dq_raw.endswith("}") else "EV0000000003": (r._dq_stage, r._dq_errors)
              for r in _quarantine(spark, config, "learning_activity").collect()}
    assert events["EV0000000003"][0] == "schema" and events["EV0000000003"][1].startswith("malformed JSON")
    assert events["EV0000000004"] == ("schema", "event_date: Input should be a valid datetime or date, input is too short")
    assert events["EV0000000005"] == ("partition", "event_date: 2024-08-05 does not match landing partition 2024-08-02")
    assert events["EV0000000007"] == ("referential", "employee_id: no matching employees.employee_id")

    [orphan_employee] = _quarantine(spark, config, "employees").collect()
    assert (orphan_employee._dq_stage, orphan_employee._dq_errors) == ("referential", "team_id: no matching teams.team_id")

    reviews = {json.loads(r._dq_raw)["review_id"]: r._dq_stage for r in _quarantine(spark, config, "performance_reviews").collect()}
    assert reviews == {2: "referential", 3: "schema"}   # review 2 cascades from its quarantined employee


def test_silver_content_types_and_lineage(spark: SparkSession, config: job.PipelineConfig, silver_run: dict) -> None:
    events = spark.read.parquet(f"{config.silver_path}/learning_activity")
    assert {r.event_id: r.text for r in events.collect()} == {"EV0000000001": "started", "EV0000000006": "from notes"}
    types = dict(events.dtypes)
    assert types["employee_id"] == "bigint" and types["event_date"] == "timestamp" and types["event_day"] == "date"
    assert {"_source_file", "_bronze_run_id", "_processed_at", "_run_id", "event_year"} <= set(events.columns)

    reviews = spark.read.parquet(f"{config.silver_path}/performance_reviews").collect()
    assert [(r.review_id, r.period_year, r.period_half) for r in reviews] == [(1, 2024, 2)]
    projects = spark.read.parquet(f"{config.silver_path}/projects")
    assert dict(projects.dtypes)["outcome_score"] == "double"


def test_learning_activity_is_partitioned_by_event_year(config: job.PipelineConfig, silver_run: dict) -> None:
    from pathlib import Path
    assert [p.name for p in Path(config.silver_path, "learning_activity").glob("event_year=*")] == ["event_year=2024"]


def test_rerun_is_idempotent(spark: SparkSession, config: job.PipelineConfig, silver_run: dict) -> None:
    second = {m.dataset: (m.rows_in, m.rows_out, m.rows_rejected) for m in job.run_silver(spark, config)}
    assert second == {name: (m.rows_in, m.rows_out, m.rows_rejected) for name, m in silver_run.items()}
    assert spark.read.parquet(f"{config.silver_path}/learning_activity").count() == 2
    assert _quarantine(spark, config, "learning_activity").count() == 4       # replaced, not appended


def test_missing_bronze_is_reported_clearly(spark: SparkSession, lake) -> None:
    with pytest.raises(job.MissingBronzeError, match="run the bronze step first"):
        job.run_silver(spark, make_config(lake))


def test_post_write_checks(spark: SparkSession) -> None:
    contract = next(c for c in job.CONTRACTS if c.name == "teams")
    schema = job.model_to_spark_schema(job.Team)
    with pytest.raises(job.SilverCheckError, match="empty"):
        job.check_silver_output(spark.createDataFrame([], schema), contract)
    with pytest.raises(job.SilverCheckError, match="null primary key"):
        job.check_silver_output(spark.createDataFrame([(None, "A", "B")], schema), contract)
    wrong_types = T.StructType([T.StructField("team_id", T.StringType()), *schema.fields[1:]])
    with pytest.raises(job.SilverCheckError, match="differs from contract"):
        job.check_silver_output(spark.createDataFrame([("1", "A", "B")], wrong_types), contract)
    job.check_silver_output(spark.createDataFrame([(1, "A", "B")], schema), contract)   # valid: no error


def test_empty_bronze_partition_for_the_run_date(spark: SparkSession, config: job.PipelineConfig) -> None:
    other_day = make_config(config.lake_root, run_date=date(2020, 1, 1))     # Bronze exists, but not for this date
    with pytest.raises(job.MissingBronzeError, match="ingest_date=2020-01-01 is empty"):
        job.run_silver(spark, other_day)


def test_conflicting_duplicates_are_reported(spark: SparkSession, lake, logs) -> None:
    config = make_config(lake)
    teams = spark.createDataFrame([(1, "Team A", "Sales"), (1, "Team A", "Design"), (2, "Team B", "Ops")],
                                  "team_id int, name string, department string")
    job.write_bronze(spark, job.add_audit_columns(teams, config, "test", "t"), f"{config.bronze_path}/teams", config)
    contract = next(c for c in job.CONTRACTS if c.name == "teams")
    metric, silver = job.process_table(spark, config, contract, {})
    assert (metric.rows_in, metric.rows_out, metric.rows_dropped) == (3, 2, 1)
    assert "1 keys have conflicting versions" in logs.readouterr().out
    silver.unpersist()
