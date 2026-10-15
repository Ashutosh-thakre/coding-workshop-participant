#!/usr/bin/env bash
# Script: Seed Source Systems
# Purpose: Load the sample dataset into the two source systems the pipeline reads from:
#            1. PostgreSQL  <- HR tables (CSV files, via the dataset's own load.sh)
#            2. S3 bucket   <- learning activity events (JSONL files, under landing/)
#          Safe to re-run: Postgres is skipped if already loaded, S3 uses `sync`.
# Usage:   source data/talent-pipeline/env.local      # or cloud env vars
#          ./data/talent-pipeline/scripts/seed_sources.sh [path/to/sample-data-dir]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd -P)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd -P)"
SAMPLE_ZIP="$PROJECT_ROOT/sampleData/data-engineering-sample-performance-scenario.zip"
SAMPLE_DIR="${1:-$PROJECT_ROOT/sampleData/data-engineering-sample-performance-scenario/data-engineering-performance-talent-management}"

# Fail fast with a clear message if a required variable is missing
for var in POSTGRES_HOST POSTGRES_PORT POSTGRES_NAME POSTGRES_USER POSTGRES_PASS LAKE_BUCKET; do
    if [ -z "${!var:-}" ]; then
        echo "ERROR: $var is not set. Did you run 'source data/talent-pipeline/env.local'?" >&2
        exit 1
    fi
done

# Extract the sample data from the committed zip if it is not there yet
if [ ! -d "$SAMPLE_DIR" ]; then
    echo "INFO: Extracting sample data from $SAMPLE_ZIP ..."
    unzip -q -o "$SAMPLE_ZIP" -d "$PROJECT_ROOT/sampleData/data-engineering-sample-performance-scenario"
fi

# ---------------------------------------------------------------------------
# 1. PostgreSQL: load HR tables (only if they do not exist yet)
# ---------------------------------------------------------------------------
SSLMODE=""
if [ "${IS_LOCAL:-true}" != "true" ]; then
    SSLMODE="sslmode=require"   # AWS Aurora requires SSL, local Postgres does not
fi
CONN="host=$POSTGRES_HOST port=$POSTGRES_PORT dbname=$POSTGRES_NAME user=$POSTGRES_USER password=$POSTGRES_PASS $SSLMODE"

if [ "$(psql "$CONN" -tAc "SELECT to_regclass('public.employees') IS NOT NULL")" = "t" ]; then
    echo "INFO: Postgres already seeded (table 'employees' exists) - skipping."
else
    echo "INFO: Seeding Postgres from $SAMPLE_DIR ..."
    "$SAMPLE_DIR/load.sh"
fi

# ---------------------------------------------------------------------------
# 2. S3: upload learning activity events to the landing zone
# ---------------------------------------------------------------------------
if ! aws s3api head-bucket --bucket "$LAKE_BUCKET" > /dev/null 2>&1; then
    echo "INFO: Creating bucket s3://$LAKE_BUCKET ..."
    aws s3 mb "s3://$LAKE_BUCKET"
fi

echo "INFO: Syncing learning activity events to s3://$LAKE_BUCKET/landing/learning_activity/ ..."
aws s3 sync "$SAMPLE_DIR/learning_activity/" "s3://$LAKE_BUCKET/landing/learning_activity/" \
    --exclude "*" --include "*.jsonl" --only-show-errors

# ---------------------------------------------------------------------------
# 3. Verify: compare source vs target counts
# ---------------------------------------------------------------------------
echo ""
echo "INFO: Verification"
for table in teams competencies employees projects performance_reviews competency_assessments talent_review; do
    csv_rows=$(( $(wc -l < "$SAMPLE_DIR/$table.csv") - 1 ))
    db_rows=$(psql "$CONN" -tAc "SELECT COUNT(*) FROM $table")
    status=$([ "$csv_rows" = "$db_rows" ] && echo "OK" || echo "MISMATCH")
    printf "  %-8s postgres.%-24s csv=%-6s db=%s\n" "$status" "$table" "$csv_rows" "$db_rows"
done

local_files=$(find "$SAMPLE_DIR/learning_activity" -name "*.jsonl" | wc -l)
s3_files=$(aws s3 ls "s3://$LAKE_BUCKET/landing/learning_activity/" --recursive | wc -l)
status=$([ "$local_files" = "$s3_files" ] && echo "OK" || echo "MISMATCH")
printf "  %-8s s3.landing/learning_activity     local=%-6s s3=%s\n" "$status" "$local_files" "$s3_files"

echo ""
echo "INFO: Source systems ready."
