#!/usr/bin/env bash
set -euo pipefail

# Config
PROJECT_ID="${PROJECT_ID:-notpla-rnd-tracker}"
DATASET_ID="${DATASET_ID:-formulation_app_eu}"
BQ_LOCATION="${BQ_LOCATION:-europe-west2}"

DDL_DIR="infra/bigquery/ddl"
VIEWS_SQL="infra/bigquery/views/101_views.sql"

echo "Project: ${PROJECT_ID}"
echo "Dataset: ${DATASET_ID}"
echo "Location: ${BQ_LOCATION}"
echo
# Preflight
command -v bq >/dev/null || { echo "Missing: bq (install/configure BigQuery CLI)"; exit 1; }
command -v gcloud >/dev/null || { echo "Missing: gcloud"; exit 1; }
[ -d "$DDL_DIR" ] || { echo "Missing DDL_DIR: $DDL_DIR"; exit 1; }
[ -f "$VIEWS_SQL" ] || { echo "Missing VIEWS_SQL: $VIEWS_SQL"; exit 1; }
echo "Checking required paths exist..."
ls -la "$DDL_DIR" >/dev/null
ls -la "$VIEWS_SQL" >/dev/null
echo
# Discover existing BASE TABLES
echo "Listing existing BASE TABLES..."
TABLES_CSV="$(bq --project_id="$PROJECT_ID" query --use_legacy_sql=false --format=csv --quiet <<SQL
SELECT table_name
FROM \`${PROJECT_ID}.${DATASET_ID}.INFORMATION_SCHEMA.TABLES\`
WHERE table_type = "BASE TABLE"
ORDER BY table_name
SQL
)"
TABLES="$(printf "%s\n" "$TABLES_CSV" | tail -n +2 | sed "/^$/d")"
echo "Existing BASE TABLES:"
if [ -n "$TABLES" ]; then printf "%s\n" "$TABLES"; else echo "(none)"; fi
echo
# Drop BASE TABLES
echo "Dropping BASE TABLES..."
if [ -n "$TABLES" ]; then
  while IFS= read -r T; do
    [ -z "$T" ] && continue
    echo "Dropping ${DATASET_ID}.${T}"
    bq --project_id="$PROJECT_ID" rm -f -t "${DATASET_ID}.${T}"
  done <<< "$TABLES"
else
  echo "No base tables found."
fi
echo
# Apply DDL in order
echo "Applying DDL files in order..."
for f in $(ls -1 "$DDL_DIR"/*.sql | sort -V); do
  echo "Applying $(basename "$f")"
  RENDERED="$(sed -e "s/PROJECT_ID/${PROJECT_ID}/g" -e "s/DATASET_ID/${DATASET_ID}/g" "$f")"
  bq --project_id="$PROJECT_ID" query --use_legacy_sql=false "$RENDERED"
done
echo
# Apply views
echo "Applying views..."
RENDERED_VIEWS="$(sed -e "s/PROJECT_ID/${PROJECT_ID}/g" -e "s/DATASET_ID/${DATASET_ID}/g" "$VIEWS_SQL")"
bq --project_id="$PROJECT_ID" query --use_legacy_sql=false "$RENDERED_VIEWS"
echo
# Verify rebuild
echo "Verifying rebuild (listing tables)..."
bq --project_id="$PROJECT_ID" query --use_legacy_sql=false --format=prettyjson --quiet <<SQL
SELECT table_name, table_type
FROM \`${PROJECT_ID}.${DATASET_ID}.INFORMATION_SCHEMA.TABLES\`
ORDER BY table_type, table_name
SQL
echo "Rebuild complete."
