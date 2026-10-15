"""Schema contracts and record-level validation (no Spark needed).

Every rule in section 10 of job.py has at least one passing and one failing case,
including each data-quality trap found in the sample data.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta

import pytest
from pydantic import ValidationError
from pyspark.sql import types as T

import job

TOMORROW = (date.today() + timedelta(days=1)).isoformat()

VALID_EVENT = {
    "event_id": "EV0000009083", "employee_id": "1134", "event_date": "2024-08-02T15:45:00",
    "event_type": "quiz_passed", "course_name": "Forecasting", "text": "Cleared it.", "score": 83,
}
VALID_ROWS: dict[type[job.Contract], dict] = {
    job.Team: {"team_id": 1, "name": "Team A", "department": "Sales"},
    job.Competency: {"competency_id": 1, "name": "Python", "category": "Technical"},
    job.Employee: {"employee_id": 1, "first_name": "Ann", "last_name": "Lee", "email": "ann@acme.example",
                   "team_id": 1, "job_level": "IC2", "hire_date": "2020-01-15"},
    job.Project: {"project_id": 1, "name": "Launch", "team_id": 1, "outcome_status": "on-time", "outcome_score": 56.87},
    job.PerformanceReview: {"review_id": 1, "employee_id": 1, "review_period": "2024-H2", "rating": 3,
                            "manager_comment": "Fine.", "submitted_date": "2025-01-10"},
    job.CompetencyAssessment: {"assessment_id": 1, "employee_id": 1, "competency_id": 1,
                               "proficiency_level": 3, "required_level": 4, "assessed_date": "2025-09-02"},
    job.TalentReview: {"employee_id": 1, "review_cycle": "2025-H1", "potential_rating": "High",
                       "promotion_readiness": "Ready Now", "attrition_risk": "Low"},
    job.LearningEvent: VALID_EVENT,
}


@pytest.mark.parametrize("model", list(VALID_ROWS), ids=lambda m: m.__name__)
def test_valid_row_passes(model: type[job.Contract]) -> None:
    values, error = job.validate_record(VALID_ROWS[model], model)
    assert error is None and values is not None


# (model, field, bad value, expected message fragment)
INVALID_CASES = [
    (job.Team, "team_id", 0, "greater than 0"),
    (job.Team, "name", "   ", "at least 1 character"),                  # whitespace trimmed, then empty
    (job.Competency, "category", "Cooking", "Technical"),
    (job.Employee, "email", "not-an-email", "pattern"),
    (job.Employee, "job_level", "CEO", "IC1"),
    (job.Employee, "hire_date", TOMORROW, "future"),
    (job.Employee, "hire_date", "2020-02-30", "date"),
    (job.Project, "outcome_status", "done", "on-time"),
    (job.Project, "outcome_score", 120, "less than or equal to 100"),
    (job.PerformanceReview, "rating", 7, "less than or equal to 5"),
    (job.PerformanceReview, "rating", 0, "greater than or equal to 1"),
    (job.PerformanceReview, "review_period", "2024-H3", "pattern"),
    (job.CompetencyAssessment, "proficiency_level", 6, "less than or equal to 5"),
    (job.CompetencyAssessment, "required_level", None, "integer"),
    (job.TalentReview, "potential_rating", "Huge", "Low"),
    (job.TalentReview, "promotion_readiness", "Soon", "Ready Now"),
    (job.LearningEvent, "event_id", "E123", "pattern"),
    (job.LearningEvent, "employee_id", None, "valid integer"),           # trap: 76 null employee ids
    (job.LearningEvent, "employee_id", "abc", "valid integer"),
    (job.LearningEvent, "event_date", "yesterday", "datetime"),          # trap: relative date
    (job.LearningEvent, "event_date", "N/A", "datetime"),                # trap: placeholder
    (job.LearningEvent, "event_date", "", "datetime"),                   # trap: empty
    (job.LearningEvent, "event_date", "2026-13-45", "month"),            # trap: impossible date
    (job.LearningEvent, "event_type", "dancing", "enrollment"),
    (job.LearningEvent, "score", 101, "less than or equal to 100"),
]


@pytest.mark.parametrize(("model", "field", "value", "fragment"), INVALID_CASES,
                         ids=[f"{m.__name__}.{f}={v!r}" for m, f, v, _ in INVALID_CASES])
def test_invalid_value_is_rejected_with_reason(model: type[job.Contract], field: str, value: object, fragment: str) -> None:
    values, error = job.validate_record({**VALID_ROWS[model], field: value}, model)
    assert values is None
    assert error is not None and error.startswith(field) and fragment in error


@pytest.mark.parametrize("missing", ["team_id", "name", "department"])
def test_required_field_missing(missing: str) -> None:
    row = {k: v for k, v in VALID_ROWS[job.Team].items() if k != missing}
    _, error = job.validate_record(row, job.Team)
    assert error == f"{missing}: Field required"


def test_quiz_events_require_a_score() -> None:
    _, error = job.validate_record({**VALID_EVENT, "score": None}, job.LearningEvent)
    assert error == "row: Value error, score is required for quiz_passed events"


def test_non_quiz_events_must_not_have_a_score() -> None:
    _, error = job.validate_record({**VALID_EVENT, "event_type": "enrollment"}, job.LearningEvent)
    assert error == "row: Value error, score is not allowed for enrollment events"


def test_type_coercion_and_trimming() -> None:
    values, _ = job.validate_record({**VALID_EVENT, "course_name": "  SQL  "}, job.LearningEvent)
    assert values["employee_id"] == 1134 and isinstance(values["employee_id"], int)
    assert values["event_date"] == datetime(2024, 8, 2, 15, 45)
    assert values["course_name"] == "SQL"


def test_optional_fields_default_to_none() -> None:
    row = {k: v for k, v in VALID_ROWS[job.PerformanceReview].items() if k != "manager_comment"}
    values, error = job.validate_record(row, job.PerformanceReview)
    assert error is None and values["manager_comment"] is None


def test_multiple_errors_are_all_reported() -> None:
    _, error = job.validate_record({**VALID_ROWS[job.PerformanceReview], "rating": 9, "review_period": "x"}, job.PerformanceReview)
    assert error.count(";") == 1 and "review_period" in error and "rating" in error


# --- JSON parsing and schema drift ------------------------------------------------

def test_json_string_is_parsed() -> None:
    values, error = job.validate_record(json.dumps(VALID_EVENT), job.LearningEvent)
    assert error is None and values["event_id"] == "EV0000009083"


@pytest.mark.parametrize("raw", ['{"event_id": "EV1", "employee_id": "1', '{event_id: 1}', "", None])
def test_malformed_json_is_rejected(raw: str | None) -> None:   # trap: 13 truncated / broken lines
    values, error = job.validate_record(raw, job.LearningEvent)
    assert values is None and error.startswith("malformed JSON")


def test_json_that_is_not_an_object_is_rejected() -> None:
    assert job.validate_record("[1, 2]", job.LearningEvent) == (None, "malformed JSON: not an object")


def test_known_drift_is_renamed() -> None:     # trap: 39 rows use 'notes' instead of 'text'
    row = {k: v for k, v in VALID_EVENT.items() if k != "text"} | {"notes": "From notes"}
    values, error = job.validate_record(row, job.LearningEvent, renames={"notes": "text"})
    assert error is None and values["text"] == "From notes"


def test_rename_never_overwrites_existing_field() -> None:
    values, _ = job.validate_record({**VALID_EVENT, "notes": "ignored"}, job.LearningEvent, renames={"notes": "text"})
    assert values["text"] == "Cleared it."


def test_unknown_fields_are_ignored_by_the_contract() -> None:
    values, error = job.validate_record({**VALID_EVENT, "mood": "happy"}, job.LearningEvent)
    assert error is None and "mood" not in values


def test_validation_error_formatting() -> None:
    with pytest.raises(ValidationError) as exc:
        job.Team.model_validate({"team_id": -1, "name": "A", "department": "B"})
    assert job.format_errors(exc.value) == "team_id: Input should be greater than 0"


# --- Contract -> Spark schema ---------------------------------------------------------

def test_spark_schema_is_derived_from_contract() -> None:
    schema = {f.name: f.dataType for f in job.model_to_spark_schema(job.LearningEvent).fields}
    assert schema == {
        "event_id": T.StringType(), "employee_id": T.LongType(), "event_date": T.TimestampType(),
        "event_type": T.StringType(), "course_name": T.StringType(), "text": T.StringType(), "score": T.LongType(),
    }
    assert job.model_to_spark_schema(job.Project)["outcome_score"].dataType == T.DoubleType()
    assert job.model_to_spark_schema(job.Employee)["hire_date"].dataType == T.DateType()


def test_unsupported_types_are_rejected() -> None:
    with pytest.raises(TypeError):
        job._spark_type(dict)
    with pytest.raises(TypeError):
        job._spark_type(int | str)


def test_contracts_registry_is_in_dependency_order() -> None:
    names = [c.name for c in job.CONTRACTS]
    for contract in job.CONTRACTS:
        for parent, _ in contract.foreign_keys.values():
            assert names.index(parent) < names.index(contract.name), f"{parent} must come before {contract.name}"
    assert set(names) == set(job.POSTGRES_TABLES) | set(job.LANDING_DATASETS)
