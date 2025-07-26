#!/usr/bin/env bash
# sync_nas_to_work.sh
# Copies HCR/2P data from NAS into your $WORK folder,
# creating the data/raw + data/fixed layout for ANTs.

set -euo pipefail

# User-editable base paths (or rely on env vars)
NAS_BASE="${NAS:-$HOME/NAS}/imaging/CIF/Analysis"
WORK_BASE="${WORK:-$HOME/WORK}/experiments"

usage() {
  echo "Usage: $0 <experiment_name> <fishID1> [fishID2 ...]"
  exit 1
}

[[ $# -ge 2 ]] || usage
EXP_NAME="$1"
shift

for fish in "$@"; do
  echo "\n=== Processing $fish for experiment $EXP_NAME ==="

  SRC_DIR="$NAS_BASE/$EXP_NAME/$fish"
  RAW_DST="$WORK_BASE/data/subjects/$fish/raw"
  FIXED_DST="$WORK_BASE/data/subjects/$fish/fixed"

  # Create target dirs
  mkdir -p "$RAW_DST/anatomy_2P" \
           "$RAW_DST/confocal_round1" \
           "$FIXED_DST"

  # Copy raw acquisitions
  echo "Copying raw anatomy_2P..."
  cp "$SRC_DIR/anatomy_2P"/*.nrrd "$RAW_DST/anatomy_2P/"

  echo "Copying raw confocal_round1..."
  cp "$SRC_DIR/confocal_round1"/*.nrrd "$RAW_DST/confocal_round1/"

  # Setup fixed references for ANTs
  echo "Setting up fixed references..."
  cp "$RAW_DST/anatomy_2P/anatomy_2P_GCaMP.nrrd" \
     "$FIXED_DST/anatomy_2P_ref_GCaMP.nrrd"
  cp "$RAW_DST/confocal_round1/round1_GCaMP.nrrd" \
     "$FIXED_DST/round1_ref_GCaMP.nrrd"

  echo "Finished $fish"
done

echo "\nAll done! Your data/subjects folders are ready for ANTs."
