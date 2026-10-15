"""Gold layer: business-level datasets, one per analytics question.

| # | Question                                         | Gold table                    | Grain (one row per)              |
|---|--------------------------------------------------|-------------------------------|----------------------------------|
| 1 | Performance ratings and review history           | employee_performance_history  | employee x review period         |
| 2 | Critical skill gaps in key competencies          | skill_gaps                    | employee x competency            |
| 3 | High-potential employees ready for promotion     | promotion_pipeline            | employee                         |
| 4 | Training and development completed               | employee_learning_summary     | employee                         |
| 5 | Attrition risk based on performance trends       | attrition_risk                | employee                         |
| 6 | Performance vs. team and project outcomes        | team_performance_outcomes     | team                             |
| 7 | Distribution of skills across the organisation   | skill_distribution            | department x competency x level  |

Builders are pure functions (DataFrames in, DataFrame out) so they can be unit
tested; ``run_gold`` handles I/O, grain/total checks and publishing.
Every table is a full refresh. Business rules are the constants below.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from pipeline.config import PipelineConfig
from pipeline.lineage import DatasetMetric
from pipeline.logging_utils import get_logger, log_step
from pipeline.resilience import retry

logger = get_logger("gold")

# --- Business rules (assumptions - documented, easy to change) ---------------------
CRITICAL_GAP_LEVELS = 2          # required_level - proficiency_level >= 2  -> critical gap
HIGH_PERFORMER_RATING = 4        # latest rating >= 4 -> high performer
PROMOTION_MIN_RATING = 4         # promotion candidate: High potential + Ready Now + latest rating >= 4
LOW_RATING = 2                   # latest rating <= 2 -> attrition risk factor
LEARNING_INACTIVE_DAYS = 180     # no learning activity for this many days -> attrition risk factor
# Attrition risk points per factor, and tier thresholds on the total score (max 7)
RISK_POINTS = {"hr_high": 3, "hr_medium": 1, "rating_declined": 2, "low_rating": 1, "learning_inactive": 1}
RISK_TIER_HIGH = 4               # score >= 4 -> High
RISK_TIER_MEDIUM = 2             # score >= 2 -> Medium, else Low


class GoldCheckError(RuntimeError):
    """Raised when a Gold table violates its grain or doesn't reconcile with Silver."""


# --- Shared building blocks --------------------------------------------------------

def build_employee_dim(employees: DataFrame, teams: DataFrame, as_of: date) -> DataFrame:
    """One row per employee with team, department and tenure (in years, as of the run date)."""
    return employees.join(
        teams.select("team_id", F.col("name").alias("team_name"), "department"), "team_id", "left"
    ).select(
        "employee_id",
        F.concat_ws(" ", "first_name", "last_name").alias("full_name"),
        "job_level",
        "team_id",
        "team_name",
        "department",
        "hire_date",
        F.round(F.datediff(F.lit(as_of), "hire_date") / 365.25, 1).alias("tenure_years"),
    )


def _employee_context(employee_dim: DataFrame) -> DataFrame:
    """The descriptive employee columns repeated on most Gold tables."""
    return employee_dim.select("employee_id", "full_name", "department", "team_name", "job_level")


# --- 1. Performance history --------------------------------------------------------

def build_performance_history(reviews: DataFrame, employee_dim: DataFrame) -> DataFrame:
    """Each review with the previous rating, change and trend for the same employee."""
    by_period = Window.partitionBy("employee_id").orderBy("period_year", "period_half")
    change = F.col("rating") - F.col("previous_rating")
    return (
        reviews.withColumn("review_seq", F.row_number().over(by_period))
        .withColumn("previous_rating", F.lag("rating").over(by_period))
        .withColumn("rating_change", change)
        .withColumn(
            "rating_trend",
            F.when(F.col("previous_rating").isNull(), "first review")
            .when(change > 0, "improved")
            .when(change < 0, "declined")
            .otherwise("stable"),
        )
        .join(_employee_context(employee_dim), "employee_id", "inner")
        .select(
            "employee_id", "full_name", "department", "team_name", "job_level",
            "review_id", "review_period", "period_year", "period_half", "review_seq",
            "rating", "previous_rating", "rating_change", "rating_trend",
            "manager_comment", "submitted_date",
        )
    )


# --- 2. Skill gaps -----------------------------------------------------------------

def build_skill_gaps(assessments: DataFrame, competencies: DataFrame, employee_dim: DataFrame) -> DataFrame:
    """Each assessment with gap = required - proficiency and a severity label."""
    gap = F.col("required_level") - F.col("proficiency_level")
    return (
        assessments.join(
            competencies.select("competency_id", F.col("name").alias("competency_name"), "category"),
            "competency_id",
            "inner",
        )
        .withColumn("gap", gap)
        .withColumn(
            "gap_severity",
            F.when(gap >= CRITICAL_GAP_LEVELS, "critical").when(gap >= 1, "moderate").otherwise("none"),
        )
        .withColumn("is_critical_gap", gap >= CRITICAL_GAP_LEVELS)
        .join(_employee_context(employee_dim), "employee_id", "inner")
        .select(
            "employee_id", "full_name", "department", "team_name", "job_level",
            "competency_id", "competency_name", "category",
            "proficiency_level", "required_level", "gap", "gap_severity", "is_critical_gap", "assessed_date",
        )
    )


# --- 4. Learning summary -----------------------------------------------------------

def _count_if(condition: Column) -> Column:
    return F.sum(F.when(condition, 1).otherwise(0))


def build_learning_summary(events: DataFrame, employee_dim: DataFrame, as_of: date) -> DataFrame:
    """Per-employee learning activity. Every employee gets a row (zeros if no activity).

    completion_rate = courses completed / courses engaged with (any event). The event
    feed is not a strict funnel (completions without enrollment events exist), so
    'completed / enrolled' could exceed 100%.
    """
    event_type = F.col("event_type")
    course = F.col("course_name")
    per_employee = events.groupBy("employee_id").agg(
        F.count("*").alias("total_events"),
        F.countDistinct(course).alias("courses_engaged"),
        F.countDistinct(F.when(event_type == "enrollment", course)).alias("courses_enrolled"),
        F.countDistinct(F.when(event_type == "course_completed", course)).alias("courses_completed"),
        F.countDistinct(F.when(event_type == "certification_earned", course)).alias("certifications_earned"),
        _count_if(event_type.isin("quiz_attempted", "quiz_passed")).alias("quiz_events"),
        _count_if(event_type == "quiz_passed").alias("quizzes_passed"),
        F.round(F.avg("score"), 1).alias("avg_quiz_score"),
        F.min("event_day").alias("first_activity_date"),
        F.max("event_day").alias("last_activity_date"),
        F.sort_array(F.collect_set(F.when(event_type == "course_completed", course))).alias("completed_courses"),
    )
    counts = ["total_events", "courses_engaged", "courses_enrolled", "courses_completed",
              "certifications_earned", "quiz_events", "quizzes_passed"]
    return (
        _employee_context(employee_dim)
        .join(per_employee, "employee_id", "left")
        .fillna(0, subset=counts)
        .withColumn("completed_courses", F.coalesce("completed_courses", F.array().cast("array<string>")))
        .withColumn(
            "completion_rate",
            F.when(F.col("courses_engaged") > 0, F.round(F.col("courses_completed") / F.col("courses_engaged"), 2)),
        )
        .withColumn("days_since_last_activity", F.datediff(F.lit(as_of), "last_activity_date"))
    )


# --- Employee profile (shared by questions 3, 5 and 6) ------------------------------

def build_employee_profile(
    employee_dim: DataFrame,
    history: DataFrame,
    talent: DataFrame,
    skill_gaps: DataFrame,
    learning: DataFrame,
) -> DataFrame:
    """One row per employee combining latest performance, talent review, gaps and learning."""
    latest_first = Window.partitionBy("employee_id").orderBy(F.desc("period_year"), F.desc("period_half"))
    latest = (
        history.withColumn("_rank", F.row_number().over(latest_first))
        .where("_rank = 1")
        .select(
            "employee_id",
            F.col("review_period").alias("latest_period"),
            F.col("rating").alias("latest_rating"),
            "previous_rating",
            "rating_change",
            "rating_trend",
        )
    )
    averages = history.groupBy("employee_id").agg(
        F.round(F.avg("rating"), 2).alias("avg_rating"), F.count("*").alias("review_count")
    )
    gaps = skill_gaps.groupBy("employee_id").agg(
        _count_if(F.col("is_critical_gap")).alias("critical_gap_count"),
        F.count("*").alias("competencies_assessed"),
    )
    return (
        employee_dim.join(latest, "employee_id", "left")
        .join(averages, "employee_id", "left")
        .join(
            talent.select(
                "employee_id", "review_cycle", "potential_rating", "promotion_readiness",
                F.col("attrition_risk").alias("hr_attrition_risk"),
            ),
            "employee_id",
            "left",
        )
        .join(gaps, "employee_id", "left")
        .join(
            learning.select(
                "employee_id", "courses_completed", "certifications_earned",
                "last_activity_date", "days_since_last_activity",
            ),
            "employee_id",
            "left",
        )
        .fillna(0, subset=["critical_gap_count", "competencies_assessed"])
    )


# --- 3. Promotion pipeline ------------------------------------------------------------

def build_promotion_pipeline(profile: DataFrame) -> DataFrame:
    """Flag promotion candidates and give every employee a pipeline stage."""
    high_potential = F.col("potential_rating") == "High"
    candidate = (
        high_potential
        & (F.col("promotion_readiness") == "Ready Now")
        & (F.col("latest_rating") >= PROMOTION_MIN_RATING)
    )
    return (
        profile.withColumn("is_high_potential", high_potential)
        .withColumn("is_promotion_candidate", candidate)
        .withColumn(
            "pipeline_stage",
            F.when(candidate & (F.col("critical_gap_count") == 0), "Promote now")
            .when(candidate, "Promote after closing skill gaps")
            .when(high_potential, "Develop (high potential)")
            .otherwise("Not in pipeline"),
        )
        .select(
            "employee_id", "full_name", "department", "team_name", "job_level", "tenure_years",
            "potential_rating", "promotion_readiness", "latest_period", "latest_rating", "avg_rating",
            "rating_trend", "critical_gap_count", "certifications_earned",
            "is_high_potential", "is_promotion_candidate", "pipeline_stage",
        )
    )


# --- 5. Attrition risk -----------------------------------------------------------------

def build_attrition_risk(profile: DataFrame) -> DataFrame:
    """Score attrition risk from HR flag, rating trend, low rating and learning inactivity.

    Each factor adds points (RISK_POINTS); the reasons are listed in ``risk_factors``
    so every score is explainable.
    """
    hr = F.col("hr_attrition_risk")
    declined = F.col("rating_trend") == "declined"
    low_rating = F.col("latest_rating") <= LOW_RATING
    inactive = F.col("days_since_last_activity").isNull() | (F.col("days_since_last_activity") > LEARNING_INACTIVE_DAYS)

    score = (
        F.when(hr == "High", RISK_POINTS["hr_high"]).when(hr == "Medium", RISK_POINTS["hr_medium"]).otherwise(0)
        + F.when(declined, RISK_POINTS["rating_declined"]).otherwise(0)
        + F.when(low_rating, RISK_POINTS["low_rating"]).otherwise(0)
        + F.when(inactive, RISK_POINTS["learning_inactive"]).otherwise(0)
    )
    factors = F.array_compact(
        F.array(
            F.when(hr == "High", F.lit("HR flagged high attrition risk")),
            F.when(hr == "Medium", F.lit("HR flagged medium attrition risk")),
            F.when(declined, F.format_string("Rating declined %d -> %d", "previous_rating", "latest_rating")),
            F.when(low_rating, F.format_string("Low latest rating (%d)", "latest_rating")),
            F.when(inactive, F.lit(f"No learning activity in {LEARNING_INACTIVE_DAYS} days")),
        )
    )
    return (
        profile.withColumn("risk_score", score)
        .withColumn(
            "risk_tier",
            F.when(F.col("risk_score") >= RISK_TIER_HIGH, "High")
            .when(F.col("risk_score") >= RISK_TIER_MEDIUM, "Medium")
            .otherwise("Low"),
        )
        .withColumn("risk_factors", factors)
        .select(
            "employee_id", "full_name", "department", "team_name", "job_level", "tenure_years",
            "hr_attrition_risk", "latest_rating", "previous_rating", "rating_trend",
            "days_since_last_activity", "potential_rating", "risk_score", "risk_tier", "risk_factors",
        )
    )


# --- 6. Team performance vs. project outcomes ---------------------------------------------

def build_team_outcomes(profile: DataFrame, teams: DataFrame, projects: DataFrame) -> DataFrame:
    """Per team: people metrics (ratings, risk) next to project outcome metrics."""
    people = profile.groupBy("team_id").agg(
        F.count("*").alias("headcount"),
        F.round(F.avg("latest_rating"), 2).alias("avg_latest_rating"),
        F.round(F.avg(F.when(F.col("latest_rating") >= HIGH_PERFORMER_RATING, 1).otherwise(0)), 2).alias("high_performer_share"),
        F.round(F.avg("rating_change"), 2).alias("avg_rating_change"),
        F.round(F.avg("critical_gap_count"), 2).alias("avg_critical_gaps"),
        _count_if(F.col("hr_attrition_risk") == "High").alias("hr_high_risk_count"),
    )
    project_stats = projects.groupBy("team_id").agg(
        F.count("*").alias("project_count"),
        F.round(F.avg("outcome_score"), 2).alias("avg_outcome_score"),
        F.round(F.avg(F.when(F.col("outcome_status") == "on-time", 1).otherwise(0)), 2).alias("on_time_share"),
        _count_if(F.col("outcome_status").isin("at-risk", "cancelled")).alias("troubled_projects"),
    )
    return (
        teams.select("team_id", F.col("name").alias("team_name"), "department")
        .join(people, "team_id", "left")
        .join(project_stats, "team_id", "left")
        .fillna(0, subset=["headcount", "hr_high_risk_count", "project_count", "troubled_projects"])
        .withColumn("has_projects", F.col("project_count") > 0)
    )


# --- 7. Skill distribution ------------------------------------------------------------------

def build_skill_distribution(skill_gaps: DataFrame) -> DataFrame:
    """Employees per department x competency x proficiency level, with share within the competency."""
    within_competency = Window.partitionBy("department", "competency_name")
    return (
        skill_gaps.groupBy("department", "category", "competency_name", "proficiency_level")
        .agg(
            F.countDistinct("employee_id").alias("employees"),
            _count_if(F.col("is_critical_gap")).alias("critical_gaps"),
        )
        .withColumn("share_of_competency", F.round(F.col("employees") / F.sum("employees").over(within_competency), 3))
    )


# --- Checks, I/O, orchestration -----------------------------------------------------------

def check_grain(df: DataFrame, table: str, keys: list[str]) -> int:
    """Fail if any grain key is null or duplicated; return the row count."""
    rows = df.count()
    distinct = df.select(*keys).distinct().count()
    null_keys = df.where(" OR ".join(f"{k} IS NULL" for k in keys)).count()
    if distinct != rows or null_keys:
        raise GoldCheckError(f"{table}: grain {keys} violated (rows={rows}, distinct keys={distinct}, null keys={null_keys})")
    return rows


def check_equal(table: str, what: str, actual: int, expected: int) -> None:
    """Fail when a Gold total doesn't reconcile with its Silver source."""
    if actual != expected:
        raise GoldCheckError(f"{table}: {what} = {actual}, expected {expected} (from Silver)")
    logger.info("%-28s check OK: %s = %d", table, what, actual)


@retry(attempts=3, base_delay=2.0)
def publish_to_postgres(df: DataFrame, config: PipelineConfig, table: str) -> None:
    """Replace the Postgres table gold_<table> with the Gold dataset (queryable with SQL)."""
    (
        df.write.format("jdbc")
        .option("url", config.postgres.jdbc_url)
        .option("dbtable", f"gold_{table}")
        .option("user", config.postgres.user)
        .option("password", config.postgres.password)
        .option("driver", "org.postgresql.Driver")
        .mode("overwrite")
        .save()
    )


@dataclass(frozen=True)
class GoldTable:
    """A built Gold dataset with its grain."""

    name: str
    df: DataFrame
    grain: tuple[str, ...]


def run_gold(spark: SparkSession, config: PipelineConfig) -> list[DatasetMetric]:
    """Build, verify, write (and publish) every Gold table from Silver."""

    def silver(name: str) -> DataFrame:
        return spark.read.parquet(f"{config.silver_path}/{name}")

    employees, teams = silver("employees"), silver("teams")
    reviews, assessments = silver("performance_reviews"), silver("competency_assessments")
    events = silver("learning_activity")

    with log_step(logger, "build gold datasets"):
        employee_dim = build_employee_dim(employees, teams, config.run_date).cache()
        history = build_performance_history(reviews, employee_dim).cache()
        gaps = build_skill_gaps(assessments, silver("competencies"), employee_dim).cache()
        learning = build_learning_summary(events, employee_dim, config.run_date).cache()
        profile = build_employee_profile(employee_dim, history, silver("talent_review"), gaps, learning).cache()
        tables = [
            GoldTable("employee_performance_history", history, ("employee_id", "review_period")),
            GoldTable("skill_gaps", gaps, ("employee_id", "competency_id")),
            GoldTable("promotion_pipeline", build_promotion_pipeline(profile), ("employee_id",)),
            GoldTable("employee_learning_summary", learning, ("employee_id",)),
            GoldTable("attrition_risk", build_attrition_risk(profile), ("employee_id",)),
            GoldTable("team_performance_outcomes", build_team_outcomes(profile, teams, silver("projects")), ("team_id",)),
            GoldTable("skill_distribution", build_skill_distribution(gaps), ("department", "competency_name", "proficiency_level")),
        ]

    with log_step(logger, "verify grain and reconcile with silver"):
        n_employees, n_teams = employees.count(), teams.count()
        n_reviews, n_assessments, n_events = reviews.count(), assessments.count(), events.count()
        rows = {t.name: check_grain(t.df, t.name, list(t.grain)) for t in tables}
        frames = {t.name: t.df for t in tables}

        check_equal("employee_performance_history", "rows", rows["employee_performance_history"], n_reviews)
        check_equal("skill_gaps", "rows", rows["skill_gaps"], n_assessments)
        for name in ("promotion_pipeline", "employee_learning_summary", "attrition_risk"):
            check_equal(name, "rows", rows[name], n_employees)
        check_equal("employee_learning_summary", "sum(total_events)",
                    frames["employee_learning_summary"].agg(F.sum("total_events")).first()[0], n_events)
        check_equal("team_performance_outcomes", "rows", rows["team_performance_outcomes"], n_teams)
        check_equal("team_performance_outcomes", "sum(headcount)",
                    frames["team_performance_outcomes"].agg(F.sum("headcount")).first()[0], n_employees)
        check_equal("skill_distribution", "sum(employees)",
                    frames["skill_distribution"].agg(F.sum("employees")).first()[0], n_assessments)

    metrics: list[DatasetMetric] = []
    with log_step(logger, "write gold datasets"):
        for table in tables:
            target = f"{config.gold_path}/{table.name}"
            out = table.df.withColumn("_generated_at", F.current_timestamp()).withColumn("_run_id", F.lit(config.run_id))
            out.coalesce(1).write.mode("overwrite").parquet(target)
            if config.publish_gold_to_postgres:
                publish_to_postgres(out, config, table.name)
            logger.info("%-28s %6d rows -> %s%s", table.name, rows[table.name], target,
                        f" + postgres gold_{table.name}" if config.publish_gold_to_postgres else "")
            # Gold aggregates/reshapes Silver, so rows_in is the Gold row count; totals are reconciled above.
            metrics.append(DatasetMetric("gold", table.name, config.silver_path, target, rows[table.name], rows[table.name]))

    for df in (employee_dim, history, gaps, learning, profile):
        df.unpersist()
    return metrics
