#!/usr/bin/env bash
# Run the app locally against Lakebase using your CLI profile.
# Auth uses your own identity (WorkspaceClient(profile=...)), so your user must be able to
# reach the lakebase_snapper repo DB and have pg_monitor on the monitored target(s).
#
# Configure via env before running (see DEPLOY.md §"Configuration values"):
#   DATABRICKS_PROFILE   your CLI profile           (required)
#   REPO_ENDPOINT        projects/<p>/branches/<b>/endpoints/<e>   (required)
#   TARGET_DBNAME        database to monitor         (default: postgres)
#
# Example:
#   DATABRICKS_PROFILE=my-profile \
#   REPO_ENDPOINT=projects/my-project/branches/production/endpoints/primary \
#   ./run_local.sh
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROFILE="${DATABRICKS_PROFILE:?set DATABRICKS_PROFILE to your CLI profile}"
REPO_ENDPOINT="${REPO_ENDPOINT:?set REPO_ENDPOINT to projects/<p>/branches/<b>/endpoints/<e>}"
TARGET_DBNAME="${TARGET_DBNAME:-postgres}"

export DATABRICKS_PROFILE REPO_ENDPOINT
export PGHOST="$(databricks postgres list-endpoints "${REPO_ENDPOINT%/endpoints/*}" -p "$PROFILE" -o json | jq -r '.[0].status.hosts.host')"
export PGPORT=5432
export PGSSLMODE=require
export REPO_DBNAME="${REPO_DBNAME:-lakebase_snapper}"
export REPO_SCHEMA=snap
export PGUSER="$(databricks current-user me -p "$PROFILE" -o json | jq -r '.userName')"
# Monitor TARGET_DBNAME on the repo endpoint by default.
export SEED_TARGETS='[{"label":"local / '"$TARGET_DBNAME"'","endpoint":"'"$REPO_ENDPOINT"'","dbname":"'"$TARGET_DBNAME"'"}]'
# Fast cadence for local testing.
export SAMPLE_INTERVAL_SECS="${SAMPLE_INTERVAL_SECS:-2}"
export SNAPSHOT_INTERVAL_SECS="${SNAPSHOT_INTERVAL_SECS:-20}"
export RETENTION_DAYS="${RETENTION_DAYS:-7}"

echo "PGHOST=$PGHOST  PGUSER=$PGUSER  repo=$REPO_DBNAME.$REPO_SCHEMA"
exec "$here/.venv/bin/python" -m uvicorn app:app --host 0.0.0.0 --port "${PORT:-8000}" "$@"
