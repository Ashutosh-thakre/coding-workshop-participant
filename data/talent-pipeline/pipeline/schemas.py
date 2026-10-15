"""Schema contracts for the Silver layer.

Each source dataset has one Pydantic model describing what a *valid* row looks
like (types, required fields, allowed values, ranges, cross-field rules). The
same model is used to:
  * validate every Bronze row (invalid rows go to quarantine with the reason),
  * derive the Spark schema of the Silver table (see validation.model_to_spark_schema),
  * document the contract (this file is the source of truth).

``CONTRACTS`` adds the table-level rules a single row can't check on its own:
primary keys (for de-duplication) and foreign keys (referential integrity).
They are listed in dependency order: parents before children.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Annotated, Literal

from pydantic import AfterValidator, BaseModel, ConfigDict, Field, model_validator


# --- Reusable field rules -------------------------------------------------------

def _not_in_future(value: date) -> date:
    """Reject dates (or datetimes) later than today."""
    day = value.date() if isinstance(value, datetime) else value
    if day > date.today():
        raise ValueError(f"must not be in the future (got {value})")
    return value


PositiveId = Annotated[int, Field(gt=0)]
NonEmptyStr = Annotated[str, Field(min_length=1)]
PastDate = Annotated[date, AfterValidator(_not_in_future)]
PastDateTime = Annotated[datetime, AfterValidator(_not_in_future)]
Level1to5 = Annotated[int, Field(ge=1, le=5)]
HalfYearPeriod = Annotated[str, Field(pattern=r"^\d{4}-H[12]$")]   # e.g. 2024-H2
LowMediumHigh = Literal["Low", "Medium", "High"]


class Contract(BaseModel):
    """Base for all contracts: trim strings, ignore unknown extra fields."""

    model_config = ConfigDict(str_strip_whitespace=True, extra="ignore")


# --- HR system (PostgreSQL) -----------------------------------------------------

class Team(Contract):
    team_id: PositiveId
    name: NonEmptyStr
    department: NonEmptyStr


class Competency(Contract):
    competency_id: PositiveId
    name: NonEmptyStr
    category: Literal["Technical", "Leadership", "Domain"]


class Employee(Contract):
    employee_id: PositiveId
    first_name: NonEmptyStr
    last_name: NonEmptyStr
    email: Annotated[str, Field(pattern=r"^[^@\s]+@[^@\s]+\.[^@\s]+$")]
    team_id: PositiveId
    job_level: Literal["IC1", "IC2", "IC3", "Senior IC", "Manager", "Senior Manager", "Director"]
    hire_date: PastDate


class Project(Contract):
    project_id: PositiveId
    name: NonEmptyStr
    team_id: PositiveId
    outcome_status: Literal["on-time", "delayed", "at-risk", "cancelled"]
    outcome_score: Annotated[float, Field(ge=0, le=100)]


class PerformanceReview(Contract):
    review_id: PositiveId
    employee_id: PositiveId
    review_period: HalfYearPeriod
    rating: Level1to5
    manager_comment: str | None = None
    submitted_date: PastDate


class CompetencyAssessment(Contract):
    assessment_id: PositiveId
    employee_id: PositiveId
    competency_id: PositiveId
    proficiency_level: Level1to5
    required_level: Level1to5
    assessed_date: PastDate


class TalentReview(Contract):
    employee_id: PositiveId
    review_cycle: HalfYearPeriod
    potential_rating: LowMediumHigh
    promotion_readiness: Literal["Ready Now", "1-2 Years", "Not Ready"]
    attrition_risk: LowMediumHigh


# --- Learning platform (S3 JSONL events) ----------------------------------------

QUIZ_EVENTS = frozenset({"quiz_attempted", "quiz_passed"})


class LearningEvent(Contract):
    event_id: Annotated[str, Field(pattern=r"^EV\d{10}$")]
    employee_id: PositiveId          # arrives as a string ("324"); Pydantic converts it
    event_date: PastDateTime         # rejects "yesterday", "N/A", "", "2026-13-45"
    event_type: Literal[
        "enrollment", "module_started", "module_completed", "quiz_attempted",
        "quiz_passed", "course_completed", "certification_earned",
    ]
    course_name: NonEmptyStr
    text: str | None = None
    score: Annotated[int, Field(ge=0, le=100)] | None = None

    @model_validator(mode="after")
    def score_only_on_quizzes(self) -> "LearningEvent":
        """Quiz events must have a score; other events must not."""
        if self.event_type in QUIZ_EVENTS and self.score is None:
            raise ValueError(f"score is required for {self.event_type} events")
        if self.event_type not in QUIZ_EVENTS and self.score is not None:
            raise ValueError(f"score is not allowed for {self.event_type} events")
        return self


# --- Table-level rules ------------------------------------------------------------

@dataclass(frozen=True)
class TableContract:
    """Row contract plus table-level rules for one Silver table."""

    name: str
    model: type[Contract]
    primary_key: tuple[str, ...]
    # column -> (parent table, parent column); checked against the parent's *valid* rows
    foreign_keys: dict[str, tuple[str, str]] = field(default_factory=dict)
    # Source-to-Silver column renames applied before validation (schema drift)
    renames: dict[str, str] = field(default_factory=dict)


CONTRACTS: tuple[TableContract, ...] = (
    TableContract("teams", Team, ("team_id",)),
    TableContract("competencies", Competency, ("competency_id",)),
    TableContract("employees", Employee, ("employee_id",), {"team_id": ("teams", "team_id")}),
    TableContract("projects", Project, ("project_id",), {"team_id": ("teams", "team_id")}),
    TableContract(
        "performance_reviews", PerformanceReview, ("review_id",),
        {"employee_id": ("employees", "employee_id")},
    ),
    TableContract(
        "competency_assessments", CompetencyAssessment, ("assessment_id",),
        {"employee_id": ("employees", "employee_id"), "competency_id": ("competencies", "competency_id")},
    ),
    TableContract("talent_review", TalentReview, ("employee_id",), {"employee_id": ("employees", "employee_id")}),
    TableContract(
        "learning_activity", LearningEvent, ("event_id",),
        {"employee_id": ("employees", "employee_id")},
        renames={"notes": "text"},  # some producers send 'notes' instead of 'text'
    ),
)
