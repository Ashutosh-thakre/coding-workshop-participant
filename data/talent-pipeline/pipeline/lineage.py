"""Lineage and run observability.

Three levels of lineage:
  * Row level     - every Bronze/Silver/Gold row carries _source_*, _bronze_run_id, _run_id.
  * Run level     - ``RunRecorder`` captures each step's timing/status and every dataset's
                    row counts, then writes a JSON manifest per run (even failed runs).
  * Column level  - documented in docs/source_to_target.md.

Layout:
  <lake>/lineage/runs/run_date=YYYY-MM-DD/<run_id>.json      one manifest per run
  <lake>/lineage/dataset_metrics/run_date=YYYY-MM-DD/*.parquet row counts, appended per run

Unlike the data layers, lineage is an append-only audit log: every run (including
reruns) is an event worth keeping, and each record is keyed by its unique run_id.
"""

from __future__ import annotations

import json
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import types as T

from pipeline.config import PipelineConfig
from pipeline.storage import write_text


@dataclass(frozen=True)
class DatasetMetric:
    """Row counts for one dataset moving through one layer."""

    layer: str          # bronze | silver | gold
    dataset: str        # e.g. employees
    source: str         # where it was read from
    target: str         # where it was written to
    rows_in: int
    rows_out: int
    rows_rejected: int = 0  # quarantined (Silver) - always 0 in Bronze

    @property
    def rows_dropped(self) -> int:
        """Rows neither written nor quarantined (e.g. removed duplicates)."""
        return self.rows_in - self.rows_out - self.rows_rejected

    def as_dict(self) -> dict[str, Any]:
        """Plain dict, ready for JSON serialisation."""
        return {**asdict(self), "rows_dropped": self.rows_dropped}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


@dataclass
class StepRecord:
    """Timing and outcome of one pipeline step."""

    name: str
    started_at: str
    finished_at: str | None = None
    duration_seconds: float | None = None
    status: str = "RUNNING"
    error: str | None = None


@dataclass
class RunRecorder:
    """Collects everything that happened in one run, for the run manifest."""

    config: PipelineConfig
    started_at: str = field(default_factory=_utc_now)
    steps: list[StepRecord] = field(default_factory=list)
    datasets: list[DatasetMetric] = field(default_factory=list)
    status: str = "RUNNING"
    finished_at: str | None = None
    _clock_start: float = field(default_factory=time.perf_counter, repr=False)

    @contextmanager
    def step(self, name: str) -> Iterator[None]:
        """Record a step's start, end, duration and status (errors are re-raised)."""
        record = StepRecord(name=name, started_at=_utc_now())
        self.steps.append(record)
        started = time.perf_counter()
        try:
            yield
            record.status = "SUCCEEDED"
        except Exception as exc:
            record.status = "FAILED"
            record.error = f"{type(exc).__name__}: {exc}"[:2000]
            raise
        finally:
            record.finished_at = _utc_now()
            record.duration_seconds = round(time.perf_counter() - started, 2)

    def finish(self, succeeded: bool) -> None:
        """Mark the run as finished."""
        self.status = "SUCCEEDED" if succeeded else "FAILED"
        self.finished_at = _utc_now()

    def manifest(self) -> dict[str, Any]:
        """The full run manifest as a JSON-ready dict (no secrets)."""
        cfg = self.config
        return {
            "run_id": cfg.run_id,
            "run_date": cfg.run_date.isoformat(),
            "status": self.status,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "duration_seconds": round(time.perf_counter() - self._clock_start, 2),
            "environment": {
                "is_local": cfg.is_local,
                "lake_root": cfg.lake_root,
                "postgres_host": cfg.postgres.host,
                "postgres_database": cfg.postgres.database,
                "publish_gold_to_postgres": cfg.publish_gold_to_postgres,
            },
            "steps": [asdict(s) for s in self.steps],
            "datasets": [m.as_dict() for m in self.datasets],
        }


DATASET_METRICS_SCHEMA = T.StructType([
    T.StructField("run_id", T.StringType()),
    T.StructField("layer", T.StringType()),
    T.StructField("dataset", T.StringType()),
    T.StructField("source", T.StringType()),
    T.StructField("target", T.StringType()),
    T.StructField("rows_in", T.LongType()),
    T.StructField("rows_out", T.LongType()),
    T.StructField("rows_rejected", T.LongType()),
    T.StructField("rows_dropped", T.LongType()),
    T.StructField("run_status", T.StringType()),
    T.StructField("recorded_at", T.StringType()),
    T.StructField("run_date", T.DateType()),
])


def write_run_lineage(spark: SparkSession, recorder: RunRecorder) -> str:
    """Write the run manifest (JSON) and append dataset metrics (Parquet). Returns the manifest path."""
    cfg = recorder.config
    manifest_path = f"{cfg.lineage_path}/runs/run_date={cfg.run_date}/{cfg.run_id}.json"
    write_text(spark, manifest_path, json.dumps(recorder.manifest(), indent=2))

    if recorder.datasets:
        recorded_at = _utc_now()
        rows = [
            (cfg.run_id, m.layer, m.dataset, m.source, m.target, m.rows_in, m.rows_out,
             m.rows_rejected, m.rows_dropped, recorder.status, recorded_at, cfg.run_date)
            for m in recorder.datasets
        ]
        (
            spark.createDataFrame(rows, DATASET_METRICS_SCHEMA)
            .coalesce(1)
            .write.mode("append")
            .partitionBy("run_date")
            .parquet(f"{cfg.lineage_path}/dataset_metrics")
        )
    return manifest_path
