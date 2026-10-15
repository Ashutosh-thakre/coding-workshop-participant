#!/usr/bin/env bash
# Script: Seed the cloud HR database (AWS Aurora) from inside the EKS cluster
# Purpose: Aurora is private - only members of the VPC security group (the EKS nodes) can reach it,
#          not the workshop VDI. This script starts a temporary postgres pod in the cluster, copies the
#          sample dataset into it, runs the dataset's own load.sh there, verifies the row counts, and
#          deletes the pod. Connection details travel in a temporary Kubernetes Secret, never as arguments.
#          Safe to re-run: skipped if the tables already exist.
# Prereqs: kubectl configured for the cluster (bin/deploy-backend.sh does this) and the Aurora settings
#          in POSTGRES_HOST/PORT/NAME/USER/PASS - or run from the repo with Terraform state access, in
#          which case they are read from the state.
# Usage:   ./data/talent-pipeline/scripts/seed_cloud_postgres.sh [path/to/sample-data-dir]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd -P)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd -P)"
SAMPLE_DIR="${1:-$PROJECT_ROOT/sampleData/data-engineering-sample-performance-scenario/data-engineering-performance-talent-management}"
POD="seed-hr-tables"
SECRET="seed-hr-tables-db"
TABLES="teams competencies employees projects performance_reviews competency_assessments talent_review"

# Aurora settings: from the environment, else from the Terraform state (read-only)
if [ -z "${POSTGRES_HOST:-}" ] || [ -z "${POSTGRES_PASS:-}" ]; then
    echo "INFO: Reading Aurora settings from the Terraform state ..."
    CLUSTER=$(terraform -chdir="$PROJECT_ROOT/infra" state pull | jq -c '.resources[] | select(.type=="aws_rds_cluster") | .instances[0].attributes')
    POSTGRES_HOST=$(jq -r .endpoint <<< "$CLUSTER")
    POSTGRES_PORT=$(jq -r .port <<< "$CLUSTER")
    POSTGRES_NAME=$(jq -r .database_name <<< "$CLUSTER")
    POSTGRES_USER=$(jq -r .master_username <<< "$CLUSTER")
    POSTGRES_PASS=$(jq -r .master_password <<< "$CLUSTER")
fi
POSTGRES_PORT="${POSTGRES_PORT:-5432}"

if [ ! -f "$SAMPLE_DIR/load.sh" ]; then
    echo "ERROR: sample data not found in $SAMPLE_DIR (run scripts/seed_sources.sh --s3-only first, it unzips it)" >&2
    exit 1
fi

cleanup() {
    # $1 = true to wait until the pod is gone (needed before re-creating it with the same name)
    kubectl delete pod "$POD" --ignore-not-found --wait="${1:-false}" > /dev/null
    kubectl delete secret "$SECRET" --ignore-not-found > /dev/null
}
trap cleanup EXIT
cleanup true   # remove leftovers of an earlier, interrupted run

echo "INFO: Starting temporary pod '$POD' (postgres client) in the cluster ..."
kubectl create secret generic "$SECRET" \
    --from-literal=POSTGRES_HOST="$POSTGRES_HOST" --from-literal=POSTGRES_PORT="$POSTGRES_PORT" \
    --from-literal=POSTGRES_NAME="$POSTGRES_NAME" --from-literal=POSTGRES_USER="$POSTGRES_USER" \
    --from-literal=POSTGRES_PASS="$POSTGRES_PASS" --from-literal=IS_LOCAL=false > /dev/null
kubectl run "$POD" --image=postgres:17 --restart=Never --overrides="$(cat <<EOF
{"spec": {"containers": [{"name": "$POD", "image": "postgres:17", "command": ["sleep", "1800"],
  "envFrom": [{"secretRef": {"name": "$SECRET"}}]}]}}
EOF
)" > /dev/null
kubectl wait --for=condition=Ready "pod/$POD" --timeout=300s > /dev/null

echo "INFO: Copying the sample dataset into the pod ..."
kubectl exec "$POD" -- mkdir -p /seed
tar -C "$SAMPLE_DIR" -cf - load.sh schema.sql $(cd "$SAMPLE_DIR" && ls ./*.csv) | kubectl exec -i "$POD" -- tar -xf - -C /seed

PSQL='psql "host=$POSTGRES_HOST port=$POSTGRES_PORT dbname=$POSTGRES_NAME user=$POSTGRES_USER password=$POSTGRES_PASS sslmode=require" -tA'
if [ "$(kubectl exec "$POD" -- bash -c "$PSQL -c \"SELECT to_regclass('public.employees') IS NOT NULL\"")" = "t" ]; then
    echo "INFO: Aurora already seeded (table 'employees' exists) - skipping load."
else
    echo "INFO: Running the dataset's load.sh against Aurora ..."
    kubectl exec "$POD" -- bash /seed/load.sh
fi

echo ""
echo "INFO: Verification (CSV rows vs Aurora rows)"
status=0
for table in $TABLES; do
    csv_rows=$(( $(wc -l < "$SAMPLE_DIR/$table.csv") - 1 ))
    db_rows=$(kubectl exec "$POD" -- bash -c "$PSQL -c 'SELECT COUNT(*) FROM $table'")
    result=$([ "$csv_rows" = "$db_rows" ] && echo "OK" || echo "MISMATCH")
    [ "$result" = "OK" ] || status=1
    printf "  %-8s aurora.%-24s csv=%-6s db=%s\n" "$result" "$table" "$csv_rows" "$db_rows"
done
exit $status
