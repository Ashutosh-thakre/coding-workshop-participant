"""Export the pipeline's results from the data lake into the demo website's data file.

Reads the latest successful run manifest, the Gold tables written by that run and its quarantine
from the lake (S3), computes the figures and findings shown on the website, and writes them to
frontend/src/data/insights.json (bundled into the site at build time). Every number and rule value
comes from the lake - nothing is typed in - so the website shows exactly what the deployed pipeline
produced.

Usage (cloud lake, participant credentials):
    source ENVIRONMENT.config && export AWS_REGION=ap-south-1
    .venv/bin/python data/talent-pipeline/scripts/export_site_data.py --bucket coding-workshop-datalake-d59e2ef7
Works against LocalStack too when AWS_ENDPOINT_URL is set (source env.local).
"""

from __future__ import annotations

import argparse
import io
import json
import os
import re
from datetime import UTC, datetime
from pathlib import Path

import boto3
import numpy as np
import pandas as pd
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUT = ROOT / "frontend" / "src" / "data" / "insights.json"
GITHUB_URL = "https://github.com/Ashutosh-thakre/coding-workshop-participant"
GOLD = [
    "employee_performance_history",
    "skill_gaps",
    "promotion_pipeline",
    "employee_learning_summary",
    "attrition_risk",
    "team_performance_outcomes",
    "skill_distribution",
]
TOP_N = 8


class Lake:
    """Read-only access to the data lake bucket."""

    def __init__(self, bucket: str) -> None:
        self.bucket = bucket
        self.s3 = boto3.client("s3", endpoint_url=os.environ.get("AWS_ENDPOINT_URL") or None)

    def objects(self, prefix: str) -> list[dict]:
        pages = self.s3.get_paginator("list_objects_v2").paginate(Bucket=self.bucket, Prefix=prefix)
        return [o for page in pages for o in page.get("Contents", [])]

    def read_bytes(self, key: str) -> bytes:
        return self.s3.get_object(Bucket=self.bucket, Key=key)["Body"].read()

    def read_parquet(self, prefix: str) -> pd.DataFrame:
        parts = [
            pq.read_table(io.BytesIO(self.read_bytes(o["Key"]))).to_pandas()
            for o in self.objects(prefix)
            if o["Key"].endswith(".parquet") and o["Size"] > 0
        ]
        return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()


def listing(values) -> str:
    values = [str(v) for v in values]
    return values[0] if len(values) == 1 else ", ".join(values[:-1]) + " and " + values[-1]


def direction(change: float, up: str, down: str, flat: str, tolerance: float = 0.005) -> str:
    return up if change > tolerance else down if change < -tolerance else flat


def records(df: pd.DataFrame, columns: list[str]) -> list[dict]:
    return json.loads(df[columns].to_json(orient="records", date_format="iso"))


def run_section(manifest: dict) -> dict:
    datasets = manifest["datasets"]
    return {
        "run_id": manifest["run_id"],
        "run_date": manifest["run_date"],
        "status": manifest["status"],
        "started_at": manifest["started_at"],
        "finished_at": manifest["finished_at"],
        "duration_seconds": manifest["duration_seconds"],
        "environment": "AWS (EKS + Aurora + S3)" if not manifest["environment"]["is_local"] else "Local (Postgres + LocalStack)",
        "lake": manifest["environment"]["lake_root"],
        "steps": [{"name": s["name"], "status": s["status"], "seconds": s["duration_seconds"]} for s in manifest["steps"]],
        "reconciliation": [
            {k: d[k] for k in ("layer", "dataset", "rows_in", "rows_out", "rows_rejected", "rows_dropped")} for d in datasets
        ],
        "rules": manifest["business_rules"],
    }


def quality_section(lake: Lake, manifest: dict) -> dict:
    run_date = manifest["run_date"]
    datasets = {(d["layer"], d["dataset"]): d for d in manifest["datasets"]}
    landing = [o for o in lake.objects("landing/learning_activity/") if o["Key"].endswith(".jsonl")]
    empty_days = ["-".join(re.findall(r"(?:year|month|day)=(\d+)", o["Key"])) for o in landing if o["Size"] == 0]
    bronze = lake.read_parquet(f"bronze/learning_activity/ingest_date={run_date}/")
    quarantine = lake.read_parquet(f"quarantine/learning_activity/run_date={run_date}/")
    silver = lake.read_parquet("silver/learning_activity/")

    def problem(row) -> str:
        if row["_dq_stage"] == "partition":
            return "Event date does not match its daily folder"
        if row["_dq_stage"] == "referential":
            return "Unknown employee"
        if row["_dq_errors"].startswith("malformed JSON"):
            return "Truncated / malformed JSON line"
        field = row["_dq_errors"].split(":")[0]
        return {"employee_id": "Missing employee_id", "event_date": "Unparseable or impossible date"}.get(
            field, f"Invalid {field}"
        )

    def raw_value(raw, field):
        try:
            return json.loads(raw).get(field)
        except (TypeError, ValueError, AttributeError):
            return None

    issues = [
        {"issue": "Empty daily file", "count": len(empty_days), "handling": "Warning in Bronze", "detail": ", ".join(empty_days)}
    ]
    if not quarantine.empty:
        quarantine["problem"] = quarantine.apply(problem, axis=1)
        bad_dates = sorted(
            {
                str(raw_value(r, "event_date"))
                for r in quarantine.loc[quarantine["problem"] == "Unparseable or impossible date", "_dq_raw"]
            }
        )
        for name, n in quarantine["problem"].value_counts().items():
            detail = ", ".join(f"'{v}'" for v in bad_dates) if name == "Unparseable or impossible date" else ""
            issues.append({"issue": name, "count": int(n), "handling": "Quarantined with reason", "detail": detail})
        dup_invalid = int(quarantine["_dq_raw"].map(lambda r: raw_value(r, "event_id")).dropna().duplicated().sum())
    else:
        dup_invalid = 0
    events = datasets[("silver", "learning_activity")]
    pairs = silver.groupby(["employee_id", "course_name"])["event_type"].agg(set)
    issues += [
        {"issue": "Duplicate events removed", "count": events["rows_dropped"], "handling": "One copy kept", "detail": ""},
        {"issue": "Duplicated and invalid", "count": dup_invalid, "handling": "All copies quarantined", "detail": ""},
        {
            "issue": "Field renamed by producer (notes → text)",
            "count": int(bronze["raw_line"].str.contains(r'"notes"\s*:', regex=True).sum()),
            "handling": "Renamed and kept",
            "detail": "",
        },
        {
            "issue": "Completions without an enrolment event",
            "count": int(sum("course_completed" in s and "enrollment" not in s for s in pairs)),
            "handling": "Kept; completion rate uses courses engaged",
            "detail": "",
        },
    ]
    hr = [d for (layer, name), d in datasets.items() if layer == "silver" and name != "learning_activity"]
    return {
        "issues": [i for i in issues if i["count"]],
        "lines_in": events["rows_in"],
        "kept": events["rows_out"],
        "quarantined": events["rows_rejected"],
        "duplicates": events["rows_dropped"],
        "hr_tables": len(hr),
        "hr_rejected": int(sum(d["rows_rejected"] + d["rows_dropped"] for d in hr)),
    }


def build(lake: Lake) -> dict:
    gold = {name: lake.read_parquet(f"gold/{name}/") for name in GOLD}
    gold_run = gold["promotion_pipeline"]["_run_id"].iloc[0]
    [key] = [o["Key"] for o in lake.objects("lineage/runs/") if o["Key"].endswith(f"/{gold_run}.json")]
    manifest = json.loads(lake.read_bytes(key))
    rules = manifest["business_rules"]
    hist, gaps, promo = gold["employee_performance_history"], gold["skill_gaps"], gold["promotion_pipeline"]
    learn, risk, teams = gold["employee_learning_summary"], gold["attrition_risk"], gold["team_performance_outcomes"]
    n_emp = len(promo)

    # Q1 ------------------------------------------------------------------------------------
    periods = sorted(hist["review_period"].unique())
    first, last = periods[0], periods[-1]
    avg = hist.groupby("review_period")["rating"].mean()
    dist = hist.pivot_table(index="rating", columns="review_period", values="employee_id", aggfunc="count").fillna(0).astype(int)
    latest = hist[hist["review_period"] == last]
    trend = latest["rating_trend"].value_counts()
    high = rules["high_performer_rating"]
    unused = [int(r) for r in range(1, 6) if dist[first].get(r, 0) == 0]
    change = avg[last] - avg[first]
    q1 = {
        "periods": periods,
        "ratings": [int(r) for r in dist.index],
        "series": [{"name": p, "values": [int(v) for v in dist[p]]} for p in periods],
        "trend": {k: int(trend.get(k, 0)) for k in ["improved", "stable", "declined", "first review"] if trend.get(k, 0)},
        "finding": (
            f"Average rating {direction(change, 'rose', 'fell', 'stayed level')} from {avg[first]:.2f} to {avg[last]:.2f}; "
            f"employees rated {high}+ went from {(hist.loc[hist.review_period == first, 'rating'] >= high).mean():.0%} to "
            f"{(latest['rating'] >= high).mean():.0%}. {int(trend.get('declined', 0)):,} employees declined."
            + (
                f" In {first} no one was rated {' or '.join(map(str, unused))}, so part of the change may be calibration."
                if unused
                else ""
            )
        ),
    }

    # Q2 ------------------------------------------------------------------------------------
    by_comp = (
        gaps.groupby(["competency_name", "category"])
        .agg(assessed=("employee_id", "count"), critical=("is_critical_gap", "sum"))
        .assign(rate=lambda d: d["critical"] / d["assessed"])
        .sort_values(["critical", "rate"], ascending=False)
        .reset_index()
    )
    dept_cat = gaps.pivot_table(index="department", columns="category", values="is_critical_gap", aggfunc="mean")
    people = (
        gaps[gaps["is_critical_gap"]]
        .groupby(["full_name", "department"])
        .agg(gaps=("competency_name", "count"), competencies=("competency_name", lambda s: ", ".join(sorted(s))))
        .reset_index()
        .sort_values(["gaps", "full_name"], ascending=[False, True])
    )
    n_critical, n_people = int(gaps["is_critical_gap"].sum()), people.shape[0]
    q2 = {
        "rule": rules["critical_gap_levels"],
        "top": [
            {"name": r.competency_name, "category": r.category, "critical": int(r.critical), "rate": round(r.rate, 3)}
            for r in by_comp.head(10).itertuples()
        ],
        "heatmap": {
            "rows": list(dept_cat.index),
            "cols": list(dept_cat.columns),
            "values": [[round(float(v), 3) for v in row] for row in dept_cat.values],
        },
        "people": records(people.head(TOP_N), ["full_name", "department", "gaps", "competencies"]),
        "finding": (
            f"{n_critical} critical gaps across {n_people} employees ({n_people / n_emp:.0%}). Most affected: "
            f"{by_comp.iloc[0]['competency_name']} ({int(by_comp.iloc[0]['critical'])} people); highest rate: "
            f"{dept_cat.stack().idxmax()[1]} skills in {dept_cat.stack().idxmax()[0]} ({dept_cat.stack().max():.0%})."
        ),
    }

    # Q3 ------------------------------------------------------------------------------------
    candidates = promo[promo["is_promotion_candidate"]]
    stages = promo["pipeline_stage"].value_counts()
    by_dept = candidates.pivot_table(
        index="department", columns="pipeline_stage", values="employee_id", aggfunc="count", fill_value=0
    ).reindex(columns=["Promote now", "Promote after closing skill gaps"], fill_value=0)
    by_dept = by_dept.assign(total=by_dept.sum(axis=1)).sort_values("total", ascending=False)
    shortlist = candidates.sort_values(
        ["critical_gap_count", "latest_rating", "avg_rating", "certifications_earned"], ascending=[True, False, False, False]
    )
    corr = promo["latest_rating"].corr(promo["potential_rating"].map({"Low": 1, "Medium": 2, "High": 3}))
    high_pot = promo[promo["potential_rating"] == "High"]
    q3 = {
        "rule": rules["promotion_min_rating"],
        "stages": {k: int(v) for k, v in stages.items()},
        "by_department": [
            {"department": d, "now": int(r["Promote now"]), "after": int(r["Promote after closing skill gaps"])}
            for d, r in by_dept.iterrows()
        ],
        "shortlist": records(
            shortlist.head(TOP_N),
            ["full_name", "department", "job_level", "latest_rating", "critical_gap_count", "pipeline_stage"],
        ),
        "finding": (
            f"{len(candidates)} candidates: {int(stages.get('Promote now', 0))} ready now, "
            f"{int(stages.get('Promote after closing skill gaps', 0))} after closing a skill gap. Potential and performance "
            f"correlate at {corr:.2f}: {int((high_pot['latest_rating'] < rules['promotion_min_rating']).sum())} of "
            f"{len(high_pot)} high-potential employees are rated below {rules['promotion_min_rating']}."
        ),
    }

    # Q4 ------------------------------------------------------------------------------------
    inactive_days = rules["learning_inactive_days"]
    completed = learn["courses_completed"].value_counts().sort_index()
    dept_learn = learn.groupby("department")["courses_completed"].mean().sort_values(ascending=False)
    n_inactive = int((learn["days_since_last_activity"] > inactive_days).sum())
    q4 = {
        "completed": [{"label": str(int(k)), "value": int(v)} for k, v in completed.items()],
        "by_department": [{"label": d, "value": round(float(v), 2)} for d, v in dept_learn.items()],
        "finding": (
            f"{learn['courses_completed'].mean():.1f} courses and {learn['certifications_earned'].mean():.1f} certifications "
            f"per employee on average (quiz score {learn['avg_quiz_score'].mean():.0f}). {n_inactive} employees "
            f"({n_inactive / n_emp:.0%}) had no learning activity in {inactive_days} days."
        ),
    }

    # Q5 ------------------------------------------------------------------------------------
    tiers = risk["risk_tier"].value_counts()
    share = pd.crosstab(risk["department"], risk["risk_tier"], normalize="index").reindex(
        columns=["High", "Medium", "Low"], fill_value=0
    )
    share = share.sort_values("High", ascending=False)

    def factor_group(reason: str) -> str:
        if reason.startswith("Rating declined"):
            return "Rating declined"
        if reason.startswith("Low latest rating"):
            return f"Latest rating ≤ {rules['low_rating']}"
        if reason.startswith("No learning activity"):
            return f"No learning in {inactive_days} days"
        return reason

    factors = risk.explode("risk_factors").dropna(subset=["risk_factors"])["risk_factors"].map(factor_group).value_counts()
    watch = risk[risk["risk_tier"] == "High"].sort_values(
        ["risk_score", "latest_rating", "full_name"], ascending=[False, True, True]
    )
    watch = watch.assign(reasons=watch["risk_factors"].map("; ".join))
    q5 = {
        "rules": {"points": rules["risk_points"], "high": rules["risk_tier_high"], "medium": rules["risk_tier_medium"]},
        "tiers": {k: int(tiers.get(k, 0)) for k in ["High", "Medium", "Low"]},
        "by_department": [
            {"department": d, **{k: round(float(r[k]), 3) for k in ["High", "Medium", "Low"]}} for d, r in share.iterrows()
        ],
        "factors": [{"label": k, "value": int(v)} for k, v in factors.items()],
        "watchlist": records(watch.head(TOP_N), ["full_name", "department", "potential_rating", "risk_score", "reasons"]),
        "finding": (
            f"{int(tiers.get('High', 0))} employees ({tiers.get('High', 0) / n_emp:.0%}) are high risk and "
            f"{int(tiers.get('Medium', 0))} medium. "
            f"{int(((risk.risk_tier == 'High') & (risk.potential_rating == 'High')).sum())} high-risk employees are "
            f"high-potential: talk to them first."
        ),
    }

    # Q6 ------------------------------------------------------------------------------------
    with_projects = teams[teams["has_projects"]]
    r = with_projects["avg_latest_rating"].corr(with_projects["avg_outcome_score"])
    slope, intercept = np.polyfit(with_projects["avg_latest_rating"], with_projects["avg_outcome_score"], 1)
    strength = "weak" if abs(r) < 0.3 else "moderate" if abs(r) < 0.6 else "strong"
    q6 = {
        "points": [
            {
                "team": t.team_name,
                "x": round(t.avg_latest_rating, 2),
                "y": round(t.avg_outcome_score, 2),
                "projects": int(t.project_count),
            }
            for t in with_projects.itertuples()
        ],
        "r": round(float(r), 3),
        "slope": round(float(slope), 2),
        "intercept": round(float(intercept), 2),
        "teams_total": len(teams),
        "finding": (
            f"A {strength} {'positive' if r > 0 else 'negative'} relationship (r = {r:.2f}): about {abs(slope):.0f} outcome "
            f"points per rating point. Only {len(with_projects)} of {len(teams)} teams have projects, so this is indicative."
        ),
    }

    # Q7 ------------------------------------------------------------------------------------
    cat_level = (
        gold["skill_distribution"]
        .pivot_table(index="category", columns="proficiency_level", values="employees", aggfunc="sum")
        .fillna(0)
    )
    cat_share = cat_level.div(cat_level.sum(axis=1), axis=0)
    comp = gaps.groupby("competency_name")["proficiency_level"].mean().sort_values()
    total = cat_level.values.sum()
    strong = cat_level[[c for c in cat_level.columns if c >= 4]].sum().sum() / total
    developing = cat_level[[c for c in cat_level.columns if c <= 2]].sum().sum() / total
    q7 = {
        "heatmap": {
            "rows": list(cat_share.index),
            "cols": [f"Level {int(c)}" for c in cat_share.columns],
            "values": [[round(float(v), 3) for v in row] for row in cat_share.values],
        },
        "weakest": [{"label": k, "value": round(float(v), 2)} for k, v in comp.head(5).items()],
        "strongest": [{"label": k, "value": round(float(v), 2)} for k, v in comp.tail(5).iloc[::-1].items()],
        "finding": (
            f"{strong:.0%} of assessments are at level 4-5 and {developing:.0%} at level 1-2. Weakest: {comp.index[0]} "
            f"({comp.iloc[0]:.2f}); strongest: {comp.index[-1]} ({comp.iloc[-1]:.2f})."
        ),
    }

    kpis = [
        {
            "label": "Employees",
            "value": f"{n_emp:,}",
            "note": f"{len(teams)} teams · {promo['department'].nunique()} departments",
        },
        {
            "label": "Average rating",
            "value": f"{avg[last]:.2f}",
            "note": f"{last} · {direction(change, 'up', 'down', 'same')} from {avg[first]:.2f}",
        },
        {
            "label": "Promotion candidates",
            "value": str(len(candidates)),
            "note": f"{int(stages.get('Promote now', 0))} ready now",
        },
        {
            "label": "High attrition risk",
            "value": str(int(tiers.get("High", 0))),
            "note": f"{tiers.get('High', 0) / n_emp:.0%} of employees",
        },
        {"label": "Critical skill gaps", "value": str(n_critical), "note": f"{n_people} employees"},
        {"label": "Inactive learners", "value": str(n_inactive), "note": f"none in {inactive_days} days"},
    ]

    return {
        "generated_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "source": f"s3://{lake.bucket}",
        "github": GITHUB_URL,
        "run": run_section(manifest),
        "quality": quality_section(lake, manifest),
        "kpis": kpis,
        "questions": {"q1": q1, "q2": q2, "q3": q3, "q4": q4, "q5": q5, "q6": q6, "q7": q7},
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--bucket", default=os.environ.get("LAKE_BUCKET"), required=not os.environ.get("LAKE_BUCKET"))
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    args = parser.parse_args()
    data = build(Lake(args.bucket))
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(data, indent=1, ensure_ascii=False))
    run = data["run"]
    print(
        f"wrote {args.out} ({args.out.stat().st_size / 1024:.0f} KiB) from run {run['run_id']} "
        f"({run['status']}, {run['environment']})"
    )


if __name__ == "__main__":
    main()
