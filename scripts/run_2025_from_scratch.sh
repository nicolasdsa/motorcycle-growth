#!/usr/bin/env bash

set -euo pipefail

YEAR="${YEAR:-2025}"
MONTH_START="${MONTH_START:-1}"
MONTH_END="${MONTH_END:-12}"

UFS=(
  RO AC AM RR PA AP TO
  MA PI CE RN PB PE AL SE BA
  MG ES RJ SP
  PR SC RS
  MS MT GO DF
)

info() {
  printf '[INFO] %s\n' "$1"
}

fail() {
  printf '[ERROR] %s\n' "$1" >&2
  exit 1
}

require_command() {
  local command_name="$1"
  if ! command -v "$command_name" >/dev/null 2>&1; then
    fail "Required command not found: ${command_name}"
  fi
}

run_cli() {
  info "Running: poetry run motorcycle-growth $*"
  poetry run motorcycle-growth "$@"
}

run_acquire_raw_data() {
  local output_file
  local status
  local summary_line

  output_file="$(mktemp)"
  info "Running: poetry run motorcycle-growth acquire-raw-data $*"

  set +e
  poetry run motorcycle-growth acquire-raw-data "$@" >"${output_file}" 2>&1
  status=$?
  set -e

  cat "${output_file}"
  summary_line="$(grep 'Raw acquisition summary:' "${output_file}" | tail -n 1 || true)"
  rm -f "${output_file}"

  if [[ ${status} -eq 0 ]]; then
    return 0
  fi

  if [[ -z "${summary_line}" ]]; then
    fail "acquire-raw-data failed without a summary line"
  fi

  if [[ "${summary_line}" =~ download_available=0 ]] \
    && [[ "${summary_line}" =~ missing=0 ]] \
    && [[ "${summary_line}" =~ failed=0 ]]; then
    info "Continuing despite acquire-raw-data exit code ${status}, because only configuration-dependent sources are still pending"
    return 0
  fi

  fail "acquire-raw-data failed with unresolved download, missing, or failed sources"
}

prepare_directories() {
  info "Preparing raw, interim, and processed directories"
  mkdir -p data/raw/senatran_motorcycle_fleet
  mkdir -p data/raw/ibge_population
  mkdir -p data/raw/sih_sus
  mkdir -p data/raw/sim_mortality
  mkdir -p data/raw/cnes_establishments
  mkdir -p data/raw/cnes_hospital_beds
  mkdir -p data/interim
  mkdir -p data/processed
}

download_low_frequency_sources() {
  info "Downloading IBGE, SIM, and SENATRAN raw sources for ${YEAR}"
  run_acquire_raw_data --ibge-year "${YEAR}"
  run_acquire_raw_data --sim-year "${YEAR}"
  run_acquire_raw_data --senatran-year "${YEAR}"
}

download_sih_sources() {
  local uf
  local month

  info "Downloading SIH raw files for all UFs and months in ${YEAR}"
  for uf in "${UFS[@]}"; do
    for month in $(seq "${MONTH_START}" "${MONTH_END}"); do
      run_acquire_raw_data \
        --sih-year "${YEAR}" \
        --sih-month "${month}" \
        --sih-uf "${uf}"
    done
  done
}

download_cnes_sources() {
  local uf
  local month

  info "Downloading CNES establishment and bed raw files for all UFs and months in ${YEAR}"
  for uf in "${UFS[@]}"; do
    for month in $(seq "${MONTH_START}" "${MONTH_END}"); do
      run_acquire_raw_data \
        --cnes-year "${YEAR}" \
        --cnes-month "${month}" \
        --cnes-uf "${uf}"
    done
  done
}

run_etl_pipeline() {
  info "Running ETL pipeline for ${YEAR}"
  run_cli etl-population --year "${YEAR}"
  run_cli etl-senatran-fleet
  run_cli etl-sih-hospitalizations
  run_cli etl-sim-mortality
  run_cli etl-cnes-infrastructure
  run_cli build-panel
}

show_outputs() {
  info "Expected final panel:"
  printf '  %s\n' "data/processed/panel/analytical_panel_municipality_year.parquet"
  info "Expected final panel metadata:"
  printf '  %s\n' "data/processed/panel/analytical_panel_municipality_year_metadata.json"
}

main() {
  require_command poetry
  require_command seq

  if [[ "${YEAR}" != "2025" ]]; then
    fail "This script is designed for a 2025-first run. Received YEAR=${YEAR}"
  fi

  if (( MONTH_START < 1 || MONTH_END > 12 || MONTH_START > MONTH_END )); then
    fail "Invalid month interval: MONTH_START=${MONTH_START}, MONTH_END=${MONTH_END}"
  fi

  info "Starting clean 2025 pipeline run"
  prepare_directories
  run_cli check-project
  download_low_frequency_sources
  download_sih_sources
  download_cnes_sources
  run_etl_pipeline
  show_outputs
  info "Pipeline run finished"
}

main "$@"
