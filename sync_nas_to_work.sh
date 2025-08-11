#!/usr/bin/env bash
# sync_nas_to_work.sh
# Copies HCR/2P data from NAS into your $WORK folder,
# creating the data/raw + data/fixed layout for ANTs.

set -euo pipefail

# Base paths (override with env vars if needed)
NAS_BASE="${NAS:-$HOME/NAS}/imaging/CIF/Analysis"
WORK_BASE="${WORK:-$HOME/WORK}/experiments"

usage() {
  echo "Usage: $0 <experiment_name> <fishID1> [fishID2 ...]"
  exit 1
}

[[ $# -ge 2 ]] || usage
EXP_NAME="$1"
shift

# Define experiment directory under WORK_BASE
EXP_DIR="$WORK_BASE/$EXP_NAME"

# Create experiment folder if missing
mkdir -p "$EXP_DIR"

echo "Using experiment directory: $EXP_DIR"

for fish in "$@"; do
  echo -e "\n=== Processing $fish for experiment $EXP_NAME ==="

  SRC_DIR="$NAS_BASE/$EXP_NAME/$fish"
  if [[ ! -d "$SRC_DIR" ]]; then
    echo "Warning: source directory '$SRC_DIR' does not exist. Skipping $fish."
    continue
  fi

  # Define raw and fixed destinations under the experiment folder
  RAW_DST="$EXP_DIR/subjects/$fish/raw"
  FIXED_DST="$EXP_DIR/subjects/$fish/fixed"

  # Create target dirs
  mkdir -p "$RAW_DST/anatomy_2P" "$FIXED_DST"

  # Copy raw anatomy_2P
  echo "Copying raw anatomy_2P from $SRC_DIR/anatomy_2P to $RAW_DST/anatomy_2P..."
  if [[ -d "$SRC_DIR/anatomy_2P" ]]; then
    cp "$SRC_DIR/anatomy_2P"/*.nrrd "$RAW_DST/anatomy_2P/"
  else
    echo "Warning: anatomy_2P folder missing in $SRC_DIR."
  fi

  # Copy any confocal_round* folders
  for confocal_dir in "$SRC_DIR"/confocal_round*; do
    if [[ -d "$confocal_dir" ]]; then
      round_name=$(basename "$confocal_dir")
      mkdir -p "$RAW_DST/$round_name"
      echo "Copying raw $round_name from $confocal_dir to $RAW_DST/$round_name..."
      cp "$confocal_dir"/*.nrrd "$RAW_DST/$round_name/" || \
        echo "Warning: no .nrrd files found in $confocal_dir."
    fi
  done

  # Setup fixed references for ANTs
  echo "Setting up fixed references in $FIXED_DST..."
  if [[ -f "$RAW_DST/anatomy_2P/anatomy_2P_GCaMP.nrrd" ]]; then
    cp "$RAW_DST/anatomy_2P/anatomy_2P_GCaMP.nrrd" \
       "$FIXED_DST/anatomy_2P_ref_GCaMP.nrrd"
  else
    echo "Error: anatomy_2P_GCaMP.nrrd not found in raw anatomy_2P. Skipping fixed copy."
  fi

  if [[ -f "$RAW_DST/confocal_round1/round1_GCaMP.nrrd" ]]; then
    cp "$RAW_DST/confocal_round1/round1_GCaMP.nrrd" \
       "$FIXED_DST/round1_ref_GCaMP.nrrd"
  else
    echo "Error: round1_GCaMP.nrrd not found in raw confocal_round1. Skipping fixed copy."
  fi

  echo "Finished $fish"
done

echo -e "\nAll done! Your data/subjects folders are ready for ANTs."
