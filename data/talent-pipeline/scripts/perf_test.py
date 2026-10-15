"""Performance test: Silver + Gold at 1x, 5x and 10x the sample volume.

For each scale factor k, the real Bronze partition is copied k times with new,
non-colliding keys (employee_id -> employee_id * 100000 + copy, etc.), so joins and
foreign keys still hold and every data-quality problem is replicated k times.
The job then runs `--steps silver,gold` against a temporary lake (no Postgres publish),
and the script reports runtime, throughput, CPU time and peak memory, and checks
that the outputs scale exactly (k x the 1x counts).

Usage (from the repo root, after `source data/talent-pipeline/env.local` and a full pipeline run):
    python data/talent-pipeline/scripts/perf_test.py              # scales 1,5,10
    python data/talent-pipeline/scripts/perf_test.py --scales 1,2
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import threading
import time
from pathlib import Path

JOB_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(JOB_DIR))

import job  # noqa: E402
from pyspark.sql import DataFrame, SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

PERF_ROOT = Path(os.environ.get("PERF_ROOT", "/tmp/talent-perf"))
SLA_SECONDS = 600
# Key columns re-numbered per copy: new_id = id * 100000 + copy (copy < 100)
KEY_COLUMNS = {
    "teams": ["team_id"],
    "employees": ["employee_id", "team_id"],
    "projects": ["project_id", "team_id"],
    "performance_reviews": ["review_id", "employee_id"],
    "competency_assessments": ["assessment_id", "employee_id"],
    "talent_review": ["employee_id"],
}
SHARED_TABLES = {"competencies"}   # reference data: not copied


def scale_table(spark: SparkSession, df: DataFrame, name: str, k: int) -> DataFrame:
    """Return k copies of a Bronze table with unique keys per copy."""
    if name in SHARED_TABLES:
        return df
    copies = df.crossJoin(F.broadcast(spark.range(k).withColumnRenamed("id", "_copy")))
    if name == "learning_activity":
        copy2 = F.lpad(F.col("_copy").cast("string"), 2, "0")
        copy5 = F.lpad(F.col("_copy").cast("string"), 5, "0")
        copies = copies.withColumn(  # EV00xxxxxxxx -> EV<copy>xxxxxxxx (still 10 digits)
            "raw_line", F.regexp_replace("raw_line", F.lit('"EV00'), F.concat(F.lit('"EV'), copy2))
        ).withColumn(                # "employee_id": "324" -> "employee_id": "32400003" (= 324*100000 + 3)
            "raw_line",
            F.regexp_replace("raw_line", F.lit(r'("employee_id": ")(\d+)(")'), F.concat(F.lit("$1$2"), copy5, F.lit("$3"))),
        )
    for column in KEY_COLUMNS.get(name, []):
        dtype = dict(df.dtypes)[column]
        copies = copies.withColumn(column, (F.col(column) * 100000 + F.col("_copy")).cast(dtype))
    return copies.drop("_copy")


def build_scaled_lakes(scales: list[int]) -> str:
    """Write a scaled copy of the real Bronze partition for each scale; return the run date used."""
    config = job.PipelineConfig.from_env()
    job.setup_logging("perf-setup")
    spark = job.build_spark(config)
    bronze_dates = spark.read.parquet(f"{config.bronze_path}/employees").select("ingest_date").distinct().collect()
    run_date = str(max(r.ingest_date for r in bronze_dates))
    for k in scales:
        lake = PERF_ROOT / f"x{k}"
        shutil.rmtree(lake, ignore_errors=True)
        for name in [*job.POSTGRES_TABLES, *job.LANDING_DATASETS]:
            source = spark.read.parquet(f"{config.bronze_path}/{name}").where(F.col("ingest_date") == run_date)
            scale_table(spark, source, name, k).coalesce(1).write.partitionBy("ingest_date").parquet(str(lake / "bronze" / name))
        print(f"  built x{k} Bronze in {lake}", flush=True)
    spark.stop()
    return run_date


def _process_tree(root_pid: int) -> dict[int, tuple[int, int]]:
    """{pid: (rss_kb, cpu_seconds)} for a process and all its descendants (driver + JVM + Python workers)."""
    out = subprocess.run(["ps", "-eo", "pid=,ppid=,rss=,times="], capture_output=True, text=True, check=False).stdout
    children: dict[int, list[int]] = {}
    stats: dict[int, tuple[int, int]] = {}
    for line in out.splitlines():
        pid, ppid, kb, cpu = (int(x) for x in line.split())
        children.setdefault(ppid, []).append(pid)
        stats[pid] = (kb, cpu)
    tree, stack = {}, [root_pid]
    while stack:
        pid = stack.pop()
        if pid in stats:
            tree[pid] = stats[pid]
        stack += children.get(pid, [])
    return tree


def run_scale(k: int, run_date: str, extra_env: dict[str, str]) -> dict:
    """Run silver+gold on the x{k} lake; return measurements."""
    lake = PERF_ROOT / f"x{k}"
    shutil.rmtree(lake / "lineage", ignore_errors=True)
    env = {**os.environ, "LAKE_ROOT": str(lake), "RUN_DATE": run_date, "PUBLISH_GOLD_TO_POSTGRES": "false", **extra_env}
    started = time.perf_counter()
    command = [sys.executable, str(JOB_DIR / "job.py"), "--steps", "silver,gold"]
    proc = subprocess.Popen(command, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    peak = [0]
    cpu_by_pid: dict[int, int] = {}   # last seen cumulative CPU seconds of every process in the tree

    def sample() -> None:
        while proc.poll() is None:
            tree = _process_tree(proc.pid)
            peak[0] = max(peak[0], sum(kb for kb, _ in tree.values()))
            for pid, (_, cpu) in tree.items():
                cpu_by_pid[pid] = max(cpu, cpu_by_pid.get(pid, 0))
            time.sleep(0.5)

    sampler = threading.Thread(target=sample, daemon=True)
    sampler.start()
    stdout, stderr = proc.communicate()
    wall = time.perf_counter() - started
    sampler.join()
    if proc.returncode != 0:
        raise RuntimeError(f"x{k} failed:\n{stdout[-2000:]}\n{stderr[-2000:]}")

    [manifest_path] = (lake / "lineage" / "runs" / f"run_date={run_date}").glob("*.json")
    manifest = json.loads(manifest_path.read_text())
    steps = {s["name"]: s["duration_seconds"] for s in manifest["steps"]}
    silver = {d["dataset"]: d for d in manifest["datasets"] if d["layer"] == "silver"}
    rows_in = sum(d["rows_in"] for d in silver.values())
    return {
        "scale": k, "wall_seconds": round(wall, 1), "silver_seconds": steps["silver"], "gold_seconds": steps["gold"],
        "rows_in": rows_in, "rows_per_minute": round(rows_in / ((steps["silver"] + steps["gold"]) / 60)),
        "cpu_seconds": sum(cpu_by_pid.values()), "peak_memory_mb": round(peak[0] / 1024),
        "events": silver["learning_activity"]["rows_in"], "events_out": silver["learning_activity"]["rows_out"],
        "quarantined": silver["learning_activity"]["rows_rejected"], "duplicates": silver["learning_activity"]["rows_dropped"],
        "gold_rows": {d["dataset"]: d["rows_out"] for d in manifest["datasets"] if d["layer"] == "gold"},
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--scales", default="1,5,10")
    parser.add_argument("--env", action="append", default=[], metavar="KEY=VALUE",
                        help="extra settings for the job, e.g. --env SPARK_CORES=1 --env SPARK_DRIVER_MEMORY=768m")
    parser.add_argument("--reuse", action="store_true", help="reuse the scaled Bronze built by a previous run")
    args = parser.parse_args()
    scales = [int(s) for s in args.scales.split(",")]
    extra_env = dict(item.split("=", 1) for item in args.env)

    if args.reuse:
        run_date = json.loads((PERF_ROOT / "run_date.json").read_text())
    else:
        print(f"Building scaled Bronze copies {scales} under {PERF_ROOT} ...", flush=True)
        run_date = build_scaled_lakes(scales)
        (PERF_ROOT / "run_date.json").write_text(json.dumps(run_date))
    print(f"Job settings: {extra_env or 'defaults'}", flush=True)
    results = []
    for k in scales:
        print(f"Running silver+gold at x{k} ...", flush=True)
        results.append(run_scale(k, run_date, extra_env))

    base = results[0]
    print("\n| Scale | Rows in | Silver (s) | Gold (s) | Total (s) | Rows/min | CPU (s) | Peak memory (MB) | Within SLA |")
    print("|------:|--------:|-----------:|---------:|----------:|---------:|--------:|-----------------:|:----------:|")
    for r in results:
        within_sla = "yes" if r["wall_seconds"] < SLA_SECONDS else "NO"
        print(f"| x{r['scale']} | {r['rows_in']:,} | {r['silver_seconds']} | {r['gold_seconds']} | {r['wall_seconds']} | "
              f"{r['rows_per_minute']:,} | {r['cpu_seconds']} | {r['peak_memory_mb']:,} | {within_sla} |")

    print("\nScaling checks (outputs must be exactly k x the x1 counts; skill_distribution aggregates, so it doesn't scale):")
    ok = True
    for r in results:
        factor = r["scale"] // base["scale"]
        checks = {
            "events": r["events"] == base["events"] * factor,
            "events_out": r["events_out"] == base["events_out"] * factor,
            "quarantined": r["quarantined"] == base["quarantined"] * factor,
            "duplicates": r["duplicates"] == base["duplicates"] * factor,
            **{f"gold.{t}": n == base["gold_rows"][t] * factor
               for t, n in r["gold_rows"].items() if t != "skill_distribution"},
        }
        ok &= all(checks.values())
        failed = [name for name, passed in checks.items() if not passed]
        status = "OK" if not failed else "MISMATCH " + ", ".join(failed)
        print(f"  x{r['scale']}: {status}  (events {r['events']:,} -> {r['events_out']:,} kept, "
              f"{r['quarantined']:,} quarantined, {r['duplicates']:,} duplicates)")

    name = "results" + "".join(f"_{k}-{v}" for k, v in sorted(extra_env.items())) + ".json"
    (PERF_ROOT / name).write_text(json.dumps({"settings": extra_env, "results": results}, indent=2))
    print(f"\nRaw results: {PERF_ROOT / name}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
