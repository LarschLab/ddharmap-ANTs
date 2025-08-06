#!/usr/bin/env bash

# sync_scratch_to_work_and_nas.sh
# Copies registration outputs from SCRATCH to WORK and NAS
# Usage: sync_scratch_to_work_and_nas.sh <experiment_name> <fishID1> [fishID2 ...]
set -euo pipefail

# Base paths (override via env vars if needed)
SCRATCH_BASE="${SCRATCH:-/scratch/ddharmap}/experiments"
WORK_BASE="${WORK:-/work/FAC/FBM/CIG/jlarsch/default/Danin}/experiments"
NAS_BASE="${NAS:-/nas/FAC/FBM/CIG/jlarsch/default/D2c/Danin}/imaging/CIF/Analysis"

usage() {
  echo "Usage: $0 <experiment_name> <fishID1> [fishID2 ...]"
  exit 1
}
[[ $# -ge 2 ]] || usage
EXP_NAME="$1"; shift

for fish in "$@"; do
  echo "\n=== Syncing $fish for experiment $EXP_NAME ==="

  SCRATCH_SUBJ="$SCRATCH_BASE/$EXP_NAME/subjects/$fish"
  WORK_SUBJ="$WORK_BASE/$EXP_NAME/subjects/$fish"
  NAS_SUBJ="$NAS_BASE/$EXP_NAME/$fish"

  # Ensure target dirs exist
  mkdir -p "$WORK_SUBJ/reg/logs"
  mkdir -p "$NAS_SUBJ/reg/logs"

  echo "Copying reg folder from SCRATCH to WORK..."
  cp -a "$SCRATCH_SUBJ/reg/." "$WORK_SUBJ/reg/"

  echo "Copying reg folder from SCRATCH to NAS..."
  cp -a "$SCRATCH_SUBJ/reg/." "$NAS_SUBJ/reg/"

  echo "Finished syncing $fish"
done

echo "\nAll registration outputs are synced to WORK and NAS."
