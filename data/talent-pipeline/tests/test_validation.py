"""Validation engine on Spark DataFrames: row validation, dedup, foreign keys, partition dates."""

from __future__ import annotations

import json

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

import job


def test_validate_rows_from_columns_keeps_types_and_passthrough(spark: SparkSession) -> None:
    df = spark.createDataFrame(
        [(1, "Team A", "Sales", "src-1"), (0, "Team B", "Design", "src-2"), (3, "", "Ops", "src-3")],
        "team_id int, name string, department string, _source_object string",
    )
    out = {r["_source_object"]: r for r in job.validate_rows(df, job.Team, ["_source_object"]).collect()}

    assert out["src-1"][job.DQ_ERRORS] is None and out["src-1"]["team_id"] == 1
    assert out["src-2"][job.DQ_ERRORS] == "team_id: Input should be greater than 0"
    assert out["src-2"]["team_id"] is None                       # invalid rows carry no values...
    assert json.loads(out["src-2"][job.DQ_RAW])["team_id"] == 0  # ...but keep the original record
    assert out["src-3"][job.DQ_ERRORS].startswith("name:")
    assert dict(job.validate_rows(df, job.Team, []).dtypes)["team_id"] == "bigint"


def test_validate_rows_from_json_with_renames(spark: SparkSession) -> None:
    good = {"event_id": "EV0000000001", "employee_id": "7", "event_date": "2024-01-01T09:00:00",
            "event_type": "enrollment", "course_name": "SQL", "notes": "renamed"}
    df = spark.createDataFrame([(json.dumps(good),), ('{"broken": ',)], "raw_line string")
    rows = job.validate_rows(df, job.LearningEvent, [], "raw_line", {"notes": "text"}).orderBy("event_id").collect()

    invalid, valid = rows  # nulls sort first
    assert valid["employee_id"] == 7 and valid["text"] == "renamed" and valid[job.DQ_ERRORS] is None
    assert invalid[job.DQ_ERRORS].startswith("malformed JSON") and invalid[job.DQ_RAW] == '{"broken": '


def test_deduplicate_keeps_one_row_per_key_deterministically(spark: SparkSession) -> None:
    df = spark.createDataFrame([("a", 2), ("a", 1), ("a", 1), ("b", 5)], "key string, value int")
    kept = sorted(tuple(r) for r in job.deduplicate(df, ["key"], ["value"]).collect())
    assert kept == [("a", 1), ("b", 5)]


def test_count_conflicting_duplicates_ignores_identical_copies(spark: SparkSession) -> None:
    df = spark.createDataFrame([("a", 1), ("a", 1), ("b", 1), ("b", 2), ("c", 3)], "key string, value int")
    assert job.count_conflicting_duplicates(df, ["key"], ["value"]) == 1  # only 'b' has two versions


def test_foreign_keys_catch_orphans(spark: SparkSession) -> None:
    teams = spark.createDataFrame([(1,), (2,)], "team_id int")
    competencies = spark.createDataFrame([(10,)], "competency_id int")
    rows = spark.createDataFrame([(1, 1, 10), (2, 99, 10), (3, 2, 77)], "id int, team_id int, competency_id int")

    valid, orphans = job.check_foreign_keys(
        rows,
        {"team_id": ("teams", "team_id"), "competency_id": ("competencies", "competency_id")},
        {"teams": teams, "competencies": competencies},
    )
    assert [r.id for r in valid.collect()] == [1]
    reasons = {r.id: r[job.DQ_ERRORS] for r in orphans.collect()}
    assert reasons == {2: "team_id: no matching teams.team_id", 3: "competency_id: no matching competencies.competency_id"}


def test_no_foreign_keys_returns_everything_and_empty_rejects(spark: SparkSession) -> None:
    rows = spark.createDataFrame([(1,)], "id int")
    valid, orphans = job.check_foreign_keys(rows, {}, {})
    assert valid.count() == 1 and orphans.count() == 0 and job.DQ_ERRORS in orphans.columns


def test_partition_dates_must_match_landing_folder(spark: SparkSession) -> None:
    folder = "s3a://bucket/landing/learning_activity/year=2024/month=08/day={day}/events-000.jsonl"
    df = spark.createDataFrame(
        [("ok", "2024-08-02 10:00:00", folder.format(day="02")),
         ("moved", "2024-08-02 10:00:00", folder.format(day="03")),
         ("no-partition", "2024-08-02 10:00:00", "s3a://bucket/flat/file.jsonl"),
         ("bad-folder", "2024-08-02 10:00:00", "s3a://bucket/x/year=2024/month=13/day=45/events-000.jsonl")],
        "event_id string, event_date string, _source_file string",
    ).withColumn("event_date", F.to_timestamp("event_date"))

    matching, mismatched = job.check_partition_dates(df, "event_date")
    assert sorted(r.event_id for r in matching.collect()) == ["no-partition", "ok"]
    reasons = {r.event_id: r[job.DQ_ERRORS] for r in mismatched.collect()}
    assert reasons == {
        "moved": "event_date: 2024-08-02 does not match landing partition 2024-08-03",
        "bad-folder": "event_date: 2024-08-02 does not match landing partition 2024-13-45",  # no crash
    }


def test_schema_drift_report_flags_renamed_and_unknown_fields(spark: SparkSession, logs: pytest.CaptureFixture[str]) -> None:
    contract = next(c for c in job.CONTRACTS if c.name == "learning_activity")
    bronze = spark.createDataFrame([('{"event_id": "EV0000000001", "notes": "x", "mood": "happy"}',), ("not json",)], "raw_line string")
    job._report_schema_drift(bronze, contract, job.get_logger("silver"))
    out = logs.readouterr().out
    assert "1 rows use 'notes' instead of 'text'" in out
    assert "unknown field 'mood' in 1 rows" in out


def test_spark_schema_matches_validated_output(spark: SparkSession) -> None:
    df = spark.createDataFrame([(1, "A", "B")], "team_id int, name string, department string")
    out = job.validate_rows(df, job.Team, [])
    expected = job.model_to_spark_schema(job.Team).fields + [
        T.StructField(job.DQ_ERRORS, T.StringType()), T.StructField(job.DQ_RAW, T.StringType())]
    assert [(f.name, f.dataType) for f in out.schema.fields] == [(f.name, f.dataType) for f in expected]
