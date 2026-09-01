#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if [[ ! -f snowflake/.env ]]; then
  echo "snowflake/.env missing. cp snowflake/.env.example snowflake/.env" >&2
  exit 1
fi
set -a; source snowflake/.env; set +a

CONN="${SNOWFLAKE_CONNECTION:-tpch}"
PROJECT="dbt_pipe.control.tpch_clean"
DBT_VERSION="1.11.11"
VERIFY=0
for arg in "$@"; do
  case "$arg" in
    --verify) VERIFY=1 ;;
    *) echo "usage: snowflake/deploy.sh [--verify]" >&2; exit 2 ;;
  esac
done

run() {
  echo
  echo "=== $1"
  snow sql -c "$CONN" -f "$1" \
    -D "s3_base_url='$S3_BASE_URL'" \
    -D "aws_role_arn='$AWS_ROLE_ARN'" \
    -D "aws_external_id='$AWS_EXTERNAL_ID'"
}

run_with_retry() {
  local attempt=1
  until run "$1"; do
    if (( attempt >= 3 )); then
      echo "$1 failed $attempt times, giving up." >&2
      return 1
    fi
    echo "$1 failed (attempt $attempt), retrying..." >&2
    attempt=$(( attempt + 1 ))
    sleep 5
  done
}

run snowflake/01_platform.sql
run snowflake/02_landing.sql
run_with_retry snowflake/03_dimensions.sql

STAGE="$(mktemp -d)"
trap 'rm -rf "$STAGE"' EXIT
for p in dbt_project.yml dbt_projects_profiles.yml models macros seeds snapshots tests; do
  cp -R "$p" "$STAGE/"
done

echo
echo "=== deploy dbt project $PROJECT"
snow dbt deploy "$PROJECT" -c "$CONN" \
  --source "$STAGE" \
  --default-target dev \
  --dbt-version "$DBT_VERSION"

run snowflake/04_pipeline.sql

if (( VERIFY )); then
  run snowflake/05_verify.sql
fi
