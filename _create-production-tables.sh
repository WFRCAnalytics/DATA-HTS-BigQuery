#!/usr/bin/env bash
set -euo pipefail

# --- CONFIG ---
PROJECT_ID="${PROJECT_ID:-confidential-2023-utah-hts}"
BQ_LOCATION="${BQ_LOCATION:-US}"
TARGET_DATASET="${TARGET_DATASET:-hts_2023_tdm_proc}"   # <-- change this to point builds to a different dataset

# Run files in an explicit order (edit as needed)
ORDERED_SQL_FILES=(
  "day.sql"
  "hh.sql"
  "person.sql"
  "trip_unlinked.sql"
  "trip_linked.sql"
  "vehicle.sql"
)

# Optional: add a prefix/suffix to output table names (e.g., build_20251007_day)
TABLE_PREFIX="${TABLE_PREFIX:-}"   # e.g. "build_20251007_"
TABLE_SUFFIX="${TABLE_SUFFIX:-}"   # e.g. "_v2"

# --- RUNNER ---
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "Project: ${PROJECT_ID}"
echo "Location: ${BQ_LOCATION}"
echo "Target dataset: ${TARGET_DATASET}"
echo "SQL dir: ${DIR}"
echo

run_sql () {
  local file="$1"
  local base="${file%.sql}"                                  # trip_linked.sql -> trip_linked
  local table_name="${TABLE_PREFIX}${base}${TABLE_SUFFIX}"   # apply optional prefix/suffix

  echo "▶ Running: ${file}  →  ${TARGET_DATASET}.${table_name}"

  # Writes SELECT results into project:dataset.table, replacing if exists
  bq query \
    --use_legacy_sql=false \
    --project_id="${PROJECT_ID}" \
    --location="${BQ_LOCATION}" \
    --destination_table="${PROJECT_ID}:${TARGET_DATASET}.${table_name}" \
    --replace \
    < "${DIR}/${file}"

  echo "✔ Done: ${TARGET_DATASET}.${table_name}"
  echo
}

for file in "${ORDERED_SQL_FILES[@]}"; do
  [[ -f "${DIR}/${file}" ]] || { echo "ERROR: Missing file: ${file}" >&2; exit 1; }
  run_sql "${file}"
done

echo "✅ All SQL scripts completed successfully."
