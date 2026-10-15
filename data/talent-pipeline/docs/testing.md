# Test Evidence

How the talent pipeline is tested, how to run the tests, what they showed, and what
we concluded. Mapped to the *Testing Expectations* in
[`docs/data-engineer.md`](../../../docs/data-engineer.md).

## 1. How to run

From `data/talent-pipeline/`, after `source env.local` (Postgres + LocalStack running and
seeded with `scripts/seed_sources.sh`, and one full pipeline run done):

```sh
pytest -m "not integration and not e2e"          # unit tests only, no services needed (~3 min)
pytest -m integration                            # PostgreSQL + S3 read/write (~1 min)
pytest -m e2e                                    # real job twice: smoke + idempotency (~4 min)
pytest --cov=job --cov-report=term-missing       # everything, with coverage
ruff check .                                     # lint
python scripts/perf_test.py                      # performance: 1x, 5x, 10x volume (~6 min)
```

## 2. Test suite overview

| File | Kind | What it proves |
|------|------|----------------|
| `test_config.py` | unit | env vars → typed config; fail fast on missing/invalid values; SSL in cloud; password never printed |
| `test_retry.py` | unit | retry succeeds after transient failures, backs off 2s → 4s, gives up after N attempts, logs WARNING then ERROR |
| `test_contracts.py` | unit | **every schema rule** (valid + invalid case each), including all 7 data traps; JSON parsing; schema-drift rename; contract → Spark schema |
| `test_validation.py` | unit (Spark) | row validation in Spark workers, dedup, conflicting duplicates, **foreign-key orphans**, landing partition-date rule, drift report |
| `test_bronze.py` | unit (Spark) | audit columns; rerun same date = no duplicates; other dates kept; empty source halts; write mismatch detected; empty landing files warned |
| `test_silver.py` | component (Spark) | tiny Bronze with one example of every problem → each row lands in Silver / quarantine (right stage + reason) / dropped duplicate; reconciliation; idempotent rerun; post-write checks |
| `test_gold.py` | unit (Spark) | **every business rule** against answers worked out by hand (trends, gap severity, learning summary, promotion stage, risk score + reasons, team outcomes, skill shares); grain / reconciliation checks fail on bad tables |
| `test_lineage.py` | unit | step recording, manifest content without secrets, metrics appended per run, manifest written for failed runs |
| `test_job.py` | unit | step parsing/order, exit codes 0 / 1 / 2, lineage written on success and failure, lineage failure doesn't hide the result |
| `test_integration.py` | integration | PostgreSQL reads match independent SQL counts, types preserved, **unreachable DB retried with backoff then fails**, Gold publish round-trip; S3 Parquet round-trip, empty objects listed, landing zone = source export (files + bytes + lines), health check |
| `test_e2e.py` | end-to-end | real `job.py` run twice: exit 0, identical reconciliation, no duplicates in Bronze/Silver, Gold queryable in Postgres, within SLA |

## 3. Results (2026-10-15, workshop VDI: 2 vCPU, 7 GB RAM)

### 3.1 Functional tests

```
$ pytest --cov=job --cov-report=term-missing
153 passed in 290.09s (0:04:50)
job.py     778     18    98%
```

| File | Tests | | File | Tests |
|------|------:|-|------|------:|
| test_config | 20 | | test_gold | 11 |
| test_retry | 7 | | test_lineage | 5 |
| test_contracts | 54 | | test_job | 7 |
| test_validation | 9 | | test_integration | 17 |
| test_bronze | 9 | | test_e2e | 5 |
| test_silver | 9 | | **Total** | **153** |

Functional testing expectations from the guide:

| Expectation | Evidence |
|-------------|----------|
| Source ingestion validation | `test_bronze`, `test_integration::test_run_bronze_ingests_postgres_and_landing` (Bronze counts = independent SQL counts), `test_landing_zone_matches_the_source_export` (783 files, 7,302,325 bytes, 29,868 lines) |
| Transformation validation (cleaning, casting, filtering, dedup) | `test_contracts`, `test_validation`, `test_silver` (types bigint/timestamp/date/double, drift rename, dedup, cascade) |
| Aggregation validation | `test_gold` (hand-computed expected values); the e2e Gold count of 166 promotion candidates matches an independent SQL query on the source tables |
| Data quality validation | `test_silver::test_quarantine_reasons_and_stages` (schema / partition / referential stages with reasons) |
| Idempotency validation | `test_bronze::test_rerunning_the_same_date_replaces_the_partition`, `test_silver::test_rerun_is_idempotent`, `test_e2e::test_reruns_produce_identical_reconciliation` + `test_no_duplicates_in_the_lake_after_two_runs` |

### 3.2 Coverage against the guide's goals

| Goal | Sections of `job.py` | Coverage |
|------|----------------------|---------:|
| Core transformation functions ≥ 80% | 9 Bronze / 12 Silver / 13 Gold | 100% / 99% / 100% ✅ |
| Schema and validation rules ≥ 90% | 10 Schema contracts / 11 Validation engine | 100% / 85%* ✅ |
| Error-handling and retry paths ≥ 85% | 3 Resilience + error branches of every layer | 96% ✅ |
| Critical business metric calculations ≥ 95% | 13 Gold | 100% ✅ |
| End-to-end smoke test 100% for one flow | `test_e2e.py` (full job, twice) | ✅ |
| **Overall** | | **98%** |

\* The uncovered lines of the validation engine (989–1003 and 961) run **inside Spark's Python worker
processes**, which the coverage tool cannot observe. They are still exercised: the per-record logic they
call (`validate_record`) is 100% covered in-process, and their output is asserted row by row in
`test_validation` and `test_silver`. Other uncovered lines are defensive branches that cannot happen in
practice (`unreachable`, lake round-trip mismatch, `if __name__ == "__main__"`).

### 3.3 Performance tests (`scripts/perf_test.py`)

The real Bronze partition is copied 1x / 5x / 10x with re-numbered keys (every data-quality problem is
copied too) and `job.py --steps silver,gold` runs on each copy. Peak memory is the whole process tree
(Python driver + JVM + Python workers); CPU is summed over all of them.

**Default settings, all cores (VDI: 2 cores):**

| Scale | Rows in | Silver (s) | Gold (s) | Total (s) | Rows/min | CPU (s) | Peak memory (MB) | Within SLA (600 s) |
|------:|--------:|-----------:|---------:|----------:|---------:|--------:|-----------------:|:------------------:|
| x1 | 42,041 | 28.11 | 7.52 | 52.8 | 70,796 | 50 | 1,917 | yes |
| x5 | 210,045 | 49.63 | 14.39 | 86.9 | 196,856 | 84 | 2,644 | yes |
| x10 | 420,050 | 66.29 | 19.33 | 103.4 | 294,359 | 116 | 2,660 | yes |

**Cloud profile, 1 core (the Kubernetes job's CPU limit), measured separately:**

| Scale | Total (s) | Peak memory (MB) | Fits 2 GiB pod limit |
|------:|----------:|-----------------:|:--------------------:|
| x1 | 53 | 1,825 | yes (223 MB headroom) |
| x10 | 105 | 1,932 | yes (116 MB headroom) |

Scaling checks: every output is exactly k × the 1x counts (events 29,868 → 298,680; kept
29,574 → 295,740; quarantined 143 → 1,430; duplicates 151 → 1,510; Gold rows × k).

### 3.4 External dependency and SLA tests

| Test | Result |
|------|--------|
| PostgreSQL unreachable (`test_unreachable_postgres_is_retried_with_backoff_then_fails`) | 3 attempts, waits 2 s then 4 s, `WARNING` per retry, final `ERROR` "failed after 3 attempt(s)", exception raised |
| Retry logic in isolation (`test_retry.py`) | exponential delays verified without real waiting (injected sleep) |
| SLA (`test_e2e::test_both_runs_succeed_within_sla`) | full pipeline (check → bronze → silver → gold) ≈ 85 s per run, limit 600 s |

## 4. Conclusions and findings

**Bugs found by the tests and fixed in `job.py`:**

1. `retry` crashed when the wrapped callable had no `__name__` (e.g. `functools.partial`).
2. The landing partition check crashed the whole job (Spark ANSI date cast) on any file path without
   `year=/month=/day=`. It now compares dates as text, so odd paths are quarantined, never fatal.
3. The log handler kept a reference to the stdout that existed at start-up, so output redirection lost
   log lines. It now writes to the current `sys.stdout`.

**Deployment risk found by the performance tests and fixed:** the job peaked at **2.3 GB**, above the
Kubernetes job's **2 GiB** memory limit (`infra/helm/values.yaml`), so it would have been OOM-killed in
the cloud. Measurement showed the JVM's non-heap overhead was the main consumer. Lean JVM defaults
(`DEFAULT_JAVA_OPTIONS`, 512 MB heap) bring the 1-core peak to **1.8 GB** with identical results and a
faster run. All of these are environment variables (`SPARK_DRIVER_MEMORY`, `SPARK_JAVA_OPTIONS`,
`SPARK_CORES`).

**Observations and limits:**

- Run time grows far slower than volume (10x data → ~2x time): fixed overhead (JVM start-up,
  per-table jobs) dominates at this size, and there is no non-linear slowdown.
- With 2 cores, memory peaks higher (2.6 GB at 5x–10x) because Spark starts a Python worker per core.
  The cloud job should run with `SPARK_CORES=1` (its CPU limit), where it stays under 2 GiB.
- Bronze writes each dataset as one file, so Silver's validation of events runs as one task (one core).
  That is right for this volume; at much larger volumes, repartition before validation and raise
  `target_files` in Bronze.
- Row validation runs Pydantic per row in Python (~290k rows/min at 10x on one core). Fine for daily
  HR data; for hundreds of millions of rows, translate the rules into native Spark expressions.
- Timestamps are stored and compared in UTC. Python's `collect()` shows them in the machine's local
  time zone. Tests build timestamps inside Spark so they pass in any time zone.
