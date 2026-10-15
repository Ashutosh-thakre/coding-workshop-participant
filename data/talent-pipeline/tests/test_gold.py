"""Gold layer: every business rule against answers worked out by hand.

Three employees (as of 2026-01-15):
  Ann (team 1): ratings 3 -> 5, critical Python gap, High/Ready Now, active learner
  Bob (team 1): ratings 4 -> 2, HR attrition High, last learning 379 days ago
  Cat (team 2): single rating 4, High/Ready Now, no gaps, no learning at all
"""

from __future__ import annotations

from datetime import date

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

import job
from conftest import RUN_DATE, make_config

AS_OF = RUN_DATE  # 2026-01-15


@pytest.fixture(scope="module")
def silver(spark: SparkSession) -> dict[str, DataFrame]:
    """Silver-shaped input tables."""
    return {
        "teams": spark.createDataFrame([(1, "Team A", "Engineering"), (2, "Team B", "Sales")],
                                       "team_id long, name string, department string"),
        "employees": spark.createDataFrame(
            [(1, "Ann", "Lee", "ann@x.example", 1, "IC2", date(2020, 1, 15)),
             (2, "Bob", "Ray", "bob@x.example", 1, "Manager", date(2021, 1, 15)),
             (3, "Cat", "Kim", "cat@x.example", 2, "IC1", date(2022, 1, 15))],
            "employee_id long, first_name string, last_name string, email string, team_id long, job_level string, hire_date date"),
        "performance_reviews": spark.createDataFrame(
            [(1, 1, "2024-H2", 3, "c", date(2025, 1, 10), 2024, 2), (2, 1, "2025-H1", 5, "c", date(2025, 7, 10), 2025, 1),
             (3, 2, "2024-H2", 4, "c", date(2025, 1, 10), 2024, 2), (4, 2, "2025-H1", 2, "c", date(2025, 7, 10), 2025, 1),
             (5, 3, "2025-H1", 4, "c", date(2025, 7, 10), 2025, 1)],
            "review_id long, employee_id long, review_period string, rating long, manager_comment string, "
            "submitted_date date, period_year int, period_half int"),
        "competencies": spark.createDataFrame([(10, "Python", "Technical"), (20, "Coaching", "Leadership")],
                                              "competency_id long, name string, category string"),
        "competency_assessments": spark.createDataFrame(
            [(1, 1, 10, 2, 4, date(2025, 9, 2)),   # Ann Python: gap 2 -> critical
             (2, 1, 20, 3, 4, date(2025, 9, 2)),   # Ann Coaching: gap 1 -> moderate
             (3, 2, 10, 5, 3, date(2025, 9, 2)),   # Bob Python: gap -2 -> none
             (4, 3, 10, 2, 3, date(2025, 9, 2))],  # Cat Python: gap 1 -> moderate
            "assessment_id long, employee_id long, competency_id long, proficiency_level long, required_level long, assessed_date date"),
        "talent_review": spark.createDataFrame(
            [(1, "2025-H1", "High", "Ready Now", "Low"), (2, "2025-H1", "Medium", "1-2 Years", "High"),
             (3, "2025-H1", "High", "Ready Now", "Medium")],
            "employee_id long, review_cycle string, potential_rating string, promotion_readiness string, attrition_risk string"),
        "projects": spark.createDataFrame([(100, "P1", 1, "on-time", 80.0), (101, "P2", 1, "cancelled", 40.0)],
                                          "project_id long, name string, team_id long, outcome_status string, outcome_score double"),
        # Timestamps are built from text inside Spark (session time zone = UTC), so the test does not
        # depend on the machine's time zone. (Python naive datetimes would be read as local time.)
        "learning_activity": spark.createDataFrame(
            [("EV1", 1, "2025-12-01 09:00:00", "enrollment", "SQL", None, None),
             ("EV2", 1, "2025-12-05 09:00:00", "quiz_passed", "SQL", None, 80),
             ("EV3", 1, "2025-12-06 09:00:00", "quiz_attempted", "Python", None, 60),
             ("EV4", 1, "2025-12-10 09:00:00", "course_completed", "SQL", None, None),
             ("EV5", 1, "2025-12-20 09:00:00", "certification_earned", "SQL", None, None),
             ("EV6", 2, "2025-01-01 09:00:00", "enrollment", "Python", None, None)],
            "event_id string, employee_id long, event_date string, event_type string, course_name string, text string, score long",
        ).withColumn("event_date", F.to_timestamp("event_date")).transform(job._conform_learning_activity),
    }


@pytest.fixture(scope="module")
def built(silver: dict[str, DataFrame]) -> dict[str, dict]:
    """Every Gold builder's output, keyed by table then by grain key."""
    dim = job.build_employee_dim(silver["employees"], silver["teams"], AS_OF)
    history = job.build_performance_history(silver["performance_reviews"], dim)
    gaps = job.build_skill_gaps(silver["competency_assessments"], silver["competencies"], dim)
    learning = job.build_learning_summary(silver["learning_activity"], dim, AS_OF)
    profile = job.build_employee_profile(dim, history, silver["talent_review"], gaps, learning)

    def by(df: DataFrame, *keys: str) -> dict:
        return {tuple(r[k] for k in keys) if len(keys) > 1 else r[keys[0]]: r.asDict() for r in df.collect()}

    return {
        "dim": by(dim, "employee_id"),
        "history": by(history, "employee_id", "review_period"),
        "gaps": by(gaps, "employee_id", "competency_id"),
        "learning": by(learning, "employee_id"),
        "promotion": by(job.build_promotion_pipeline(profile), "employee_id"),
        "attrition": by(job.build_attrition_risk(profile), "employee_id"),
        "teams": by(job.build_team_outcomes(profile, silver["teams"], silver["projects"]), "team_id"),
        "skills": by(job.build_skill_distribution(gaps), "department", "competency_name", "proficiency_level"),
    }


def test_employee_dim(built: dict) -> None:
    ann = built["dim"][1]
    assert (ann["full_name"], ann["team_name"], ann["department"]) == ("Ann Lee", "Team A", "Engineering")
    assert ann["tenure_years"] == 6.0          # 2020-01-15 -> 2026-01-15


def test_q1_performance_history_trends(built: dict) -> None:
    h = built["history"]
    assert len(h) == 5
    assert (h[(1, "2024-H2")]["rating_trend"], h[(1, "2024-H2")]["review_seq"]) == ("first review", 1)
    assert {k: (h[k]["previous_rating"], h[k]["rating_change"], h[k]["rating_trend"]) for k in [(1, "2025-H1"), (2, "2025-H1"), (3, "2025-H1")]} == {
        (1, "2025-H1"): (3, 2, "improved"),
        (2, "2025-H1"): (4, -2, "declined"),
        (3, "2025-H1"): (None, None, "first review"),
    }


def test_q2_skill_gap_severity(built: dict) -> None:
    g = built["gaps"]
    assert {k: (v["gap"], v["gap_severity"], v["is_critical_gap"]) for k, v in g.items()} == {
        (1, 10): (2, "critical", True), (1, 20): (1, "moderate", False),
        (2, 10): (-2, "none", False), (3, 10): (1, "moderate", False),
    }
    assert g[(1, 10)]["competency_name"] == "Python" and g[(1, 20)]["category"] == "Leadership"


def test_q4_learning_summary(built: dict) -> None:
    ann, bob, cat = (built["learning"][i] for i in (1, 2, 3))
    assert {k: ann[k] for k in ["total_events", "courses_engaged", "courses_enrolled", "courses_completed",
                                "certifications_earned", "quiz_events", "quizzes_passed", "avg_quiz_score",
                                "completed_courses", "completion_rate", "days_since_last_activity"]} == {
        "total_events": 5, "courses_engaged": 2, "courses_enrolled": 1, "courses_completed": 1,
        "certifications_earned": 1, "quiz_events": 2, "quizzes_passed": 1, "avg_quiz_score": 70.0,
        "completed_courses": ["SQL"], "completion_rate": 0.5, "days_since_last_activity": 26,
    }
    assert (ann["first_activity_date"], ann["last_activity_date"]) == (date(2025, 12, 1), date(2025, 12, 20))
    assert (bob["completion_rate"], bob["days_since_last_activity"]) == (0.0, 379)
    # Employees with no activity still get a row: zero counts, no rate, empty list
    assert (cat["total_events"], cat["courses_completed"], cat["completed_courses"]) == (0, 0, [])
    assert (cat["completion_rate"], cat["days_since_last_activity"]) == (None, None)


def test_q3_promotion_pipeline(built: dict) -> None:
    p = built["promotion"]
    assert {i: (p[i]["is_high_potential"], p[i]["is_promotion_candidate"], p[i]["pipeline_stage"]) for i in p} == {
        1: (True, True, "Promote after closing skill gaps"),   # candidate, but 1 critical gap
        2: (False, False, "Not in pipeline"),
        3: (True, True, "Promote now"),
    }
    assert (p[1]["latest_rating"], p[1]["avg_rating"], p[1]["critical_gap_count"]) == (5, 4.0, 1)


def test_q5_attrition_risk_score_and_reasons(built: dict) -> None:
    a = built["attrition"]
    assert {i: (a[i]["risk_score"], a[i]["risk_tier"]) for i in a} == {1: (0, "Low"), 2: (7, "High"), 3: (2, "Medium")}
    assert a[1]["risk_factors"] == []
    assert a[2]["risk_factors"] == [
        "HR flagged high attrition risk", "Rating declined 4 -> 2", "Low latest rating (2)",
        "No learning activity in 180 days",
    ]
    assert a[3]["risk_factors"] == ["HR flagged medium attrition risk", "No learning activity in 180 days"]


def test_q6_team_outcomes(built: dict) -> None:
    team_a, team_b = built["teams"][1], built["teams"][2]
    assert {k: team_a[k] for k in ["headcount", "avg_latest_rating", "high_performer_share", "avg_rating_change",
                                   "avg_critical_gaps", "hr_high_risk_count", "project_count", "avg_outcome_score",
                                   "on_time_share", "troubled_projects", "has_projects"]} == {
        "headcount": 2, "avg_latest_rating": 3.5, "high_performer_share": 0.5, "avg_rating_change": 0.0,
        "avg_critical_gaps": 0.5, "hr_high_risk_count": 1, "project_count": 2, "avg_outcome_score": 60.0,
        "on_time_share": 0.5, "troubled_projects": 1, "has_projects": True,
    }
    assert (team_b["headcount"], team_b["project_count"], team_b["has_projects"], team_b["avg_outcome_score"]) == (1, 0, False, None)


def test_q7_skill_distribution(built: dict) -> None:
    s = built["skills"]
    assert {k: (v["employees"], v["critical_gaps"], v["share_of_competency"]) for k, v in s.items()} == {
        ("Engineering", "Python", 2): (1, 1, 0.5),
        ("Engineering", "Python", 5): (1, 0, 0.5),
        ("Engineering", "Coaching", 3): (1, 0, 1.0),
        ("Sales", "Python", 2): (1, 0, 1.0),
    }


def test_run_gold_end_to_end_on_local_lake(spark: SparkSession, silver: dict[str, DataFrame], lake) -> None:
    config = make_config(lake)
    for name, df in silver.items():
        df.write.mode("overwrite").parquet(f"{config.silver_path}/{name}")

    metrics = {m.dataset: m.rows_out for m in job.run_gold(spark, config)}
    assert metrics == {
        "employee_performance_history": 5, "skill_gaps": 4, "promotion_pipeline": 3, "employee_learning_summary": 3,
        "attrition_risk": 3, "team_performance_outcomes": 2, "skill_distribution": 4,
    }
    written = spark.read.parquet(f"{config.gold_path}/attrition_risk")
    assert written.count() == 3 and {"_generated_at", "_run_id"} <= set(written.columns)


def test_grain_check_rejects_duplicates_nulls_and_empty_tables(spark: SparkSession) -> None:
    schema = "id long, v string"
    job.check_grain(spark.createDataFrame([(1, "a"), (2, "b")], schema), "t", ["id"])
    with pytest.raises(job.GoldCheckError, match="grain"):
        job.check_grain(spark.createDataFrame([(1, "a"), (1, "b")], schema), "t", ["id"])
    with pytest.raises(job.GoldCheckError, match="null keys=1"):
        job.check_grain(spark.createDataFrame([(1, "a"), (None, "b")], schema), "t", ["id"])
    with pytest.raises(job.GoldCheckError, match="empty"):
        job.check_grain(spark.createDataFrame([], schema), "t", ["id"])


def test_reconciliation_check_fails_on_mismatch() -> None:
    job.check_equal("t", "rows", 5, 5)
    with pytest.raises(job.GoldCheckError, match="rows = 4, expected 5"):
        job.check_equal("t", "rows", 4, 5)
