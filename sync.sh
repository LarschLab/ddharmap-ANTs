#!/usr/bin/env bash
# sync_preprocessed_to_compute.sh
# Pulls ONLY preprocessed data from NAS (02_reg/00_preprocessing)
# into WORK (mirrors 02_reg/00_preprocessing) and prepares SCRATCH
# at experiments/subjects/<FishID>/{raw,fixed,reg} for ANTs.
# Also writes a .origin.json for traceability and future sync-back.
#
# ENV in ~/.bashrc:
#   NAS=/nas/FAC/FBM/CIG/jlarsch/default/D2c/07\ Data
#   WORK=/work/FAC/FBM/CIG/jlarsch/default/Danin
#   SCRATCH=/scratch/ddharmap
#
# USAGE (updated):
#   $0 <user> <fishID1> [fishID2 ...]
# Example:
#   $0 Matilde L331_f01 L395_f10
#
# <user> must be a subfolder under $NAS (e.g., Matilde or Alejandro).

set -euo pipefail

# ---------- Config ----------
NAS_BASE="${NAS:?NAS env var is required}"                 # /nas/.../07 Data
WORK_SUBJECTS_DIR="${WORK:-$HOME/WORK}/experiments/subjects"
SCRATCH_SUBJECTS_DIR="${SCRATCH:-$HOME/SCRATCH}/experiments/subjects"

usage() {
  cat <<USAGE
Usage: $0 <user> <fishID1> [fishID2 ...]
  <user> must match a directory under \$NAS (e.g., Matilde, Alejandro)

Example:
  $0 Matilde L331_f01 L395_f10
USAGE
  exit 1
}
[[ $# -ge 2 ]] || usage

OWNER="$1"; shift
OWNER_DIR="$NAS_BASE/$OWNER"
[[ -d "$OWNER_DIR" ]] || { echo "ERROR: Owner directory not found: $OWNER_DIR"; exit 2; }

mkdir -p "$WORK_SUBJECTS_DIR" "$SCRATCH_SUBJECTS_DIR"
echo "Compute roots:"
echo "  WORK   : $WORK_SUBJECTS_DIR"
echo "  SCRATCH: $SCRATCH_SUBJECTS_DIR"
echo "NAS owner: $OWNER  ($OWNER_DIR)"

# ---------- Helpers ----------
cp_glob() {
  local pattern="$1" dest="$2"
  mkdir -p "$dest"
  shopt -s nullglob
  # space-safe glob expansion
  mapfile -t files < <(compgen -G "$pattern")
  shopt -u nullglob
  if (( ${#files[@]} )); then
    cp -a "${files[@]}" "$dest/"
  else
    echo "  (no files for pattern: $pattern)"
  fi
}

write_origin_json() {
  local fish_id="$1" nas_fish_root="$2"
  local now_utc; now_utc="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  local here
  for here in \
      "$WORK_SUBJECTS_DIR/$fish_id" \
      "$SCRATCH_SUBJECTS_DIR/$fish_id" \
      "$nas_fish_root"
  do
    mkdir -p "$here"
    cat > "$here/.origin.json" <<JSON
{
  "fish_id": "$fish_id",
  "nas_owner": "$OWNER",
  "nas_fish_root": "$nas_fish_root",
  "created_utc": "$now_utc",
  "created_by": "${USER:-unknown}"
}
JSON
  done
}

# ---------- Main ----------
for fish in "$@"; do
  echo -e "\n=== Preparing $fish (owner: $OWNER) ==="
  NAS_FISH_ROOT="$OWNER_DIR/$fish"
  if [[ ! -d "$NAS_FISH_ROOT" ]]; then
    echo "ERROR: Fish directory not found on NAS: $NAS_FISH_ROOT  (skipping)"
    continue
  fi

  PREPROC_ROOT="$NAS_FISH_ROOT/02_reg/00_preprocessing"
  if [[ ! -d "$PREPROC_ROOT" ]]; then
    echo "ERROR: Missing preprocessed root: $PREPROC_ROOT"
    echo "       Expected 2p_anatomy/, r1/, rn/ under it. Skipping $fish."
    continue
  fi

  # --- 1) Mirror preprocessed → WORK (fish-only) ---
  WORK_FISH_PREPROC="$WORK_SUBJECTS_DIR/$fish/02_reg/00_preprocessing"
  echo "Syncing preprocessed → WORK: $WORK_FISH_PREPROC"
  mkdir -p "$WORK_FISH_PREPROC"/{2p_anatomy,r1,rn}

  if [[ -d "$PREPROC_ROOT/2p_anatomy" ]]; then
    cp_glob "$PREPROC_ROOT/2p_anatomy/${fish}_*.nrrd" "$WORK_FISH_PREPROC/2p_anatomy"
  else
    echo "  WARN: no 2p_anatomy/ in $PREPROC_ROOT"
  fi

  if [[ -d "$PREPROC_ROOT/r1" ]]; then
    cp_glob "$PREPROC_ROOT/r1/${fish}_*.nrrd" "$WORK_FISH_PREPROC/r1"
  else
    echo "  WARN: no r1/ in $PREPROC_ROOT"
  fi

  if [[ -d "$PREPROC_ROOT/rn" ]]; then
    cp_glob "$PREPROC_ROOT/rn/${fish}_*.nrrd" "$WORK_FISH_PREPROC/rn"
  else
    echo "  INFO: no rn/ in $PREPROC_ROOT (single round?)"
  fi

  # --- 2) Build SCRATCH raw/fixed/reg from WORK ---
  SCRATCH_FISH_DIR="$SCRATCH_SUBJECTS_DIR/$fish"
  RAW_DIR="$SCRATCH_FISH_DIR/raw"
  FIXED_DIR="$SCRATCH_FISH_DIR/fixed"
  REG_DIR="$SCRATCH_FISH_DIR/reg"

  echo "Preparing SCRATCH layout at $SCRATCH_FISH_DIR"
  mkdir -p "$RAW_DIR/anatomy_2P" "$RAW_DIR/confocal_round1" "$RAW_DIR/confocal_round2" "$FIXED_DIR" "$REG_DIR/logs"

  cp_glob "$WORK_FISH_PREPROC/2p_anatomy/"'*.nrrd' "$RAW_DIR/anatomy_2P"
  cp_glob "$WORK_FISH_PREPROC/r1/"'*.nrrd'         "$RAW_DIR/confocal_round1"
  cp_glob "$WORK_FISH_PREPROC/rn/"'*.nrrd'         "$RAW_DIR/confocal_round2"

  # keep SCRATCH/raw pristine: only files for this fish
  find "$RAW_DIR" -type f ! -name "${fish}_*.nrrd" -delete || true

  # Fixed references
  if [[ -f "$RAW_DIR/anatomy_2P/anatomy_2P_GCaMP.nrrd" ]]; then
    cp -a "$RAW_DIR/anatomy_2P/anatomy_2P_GCaMP.nrrd" "$FIXED_DIR/anatomy_2P_ref_GCaMP.nrrd"
  else
    shopt -s nullglob
    gc=( "$RAW_DIR/anatomy_2P/"*GCaMP*.nrrd )
    shopt -u nullglob
    if (( ${#gc[@]} )); then
      cp -a "${gc[0]}" "$FIXED_DIR/anatomy_2P_ref_GCaMP.nrrd"
    else
      echo "  WARN: no GCaMP anatomy found for fixed ref"
    fi
  fi

  if compgen -G "$RAW_DIR/confocal_round1/"'*channel1*_GCaMP*.nrrd' > /dev/null; then
    first_r1="$(ls "$RAW_DIR/confocal_round1/"*channel1*_GCaMP*.nrrd | head -n1)"
    cp -a "$first_r1" "$FIXED_DIR/round1_ref_GCaMP.nrrd"
  elif compgen -G "$RAW_DIR/confocal_round1/"'*GCaMP*.nrrd' > /dev/null; then
    first_r1="$(ls "$RAW_DIR/confocal_round1/"*GCaMP*.nrrd | head -n1)"
    cp -a "$first_r1" "$FIXED_DIR/round1_ref_GCaMP.nrrd"
  else
    echo "  WARN: no GCaMP r1 file found for fixed ref"
  fi

  # --- 3) Traceability & convenience ---
  write_origin_json "$fish" "$NAS_FISH_ROOT"
  ln -snf "$NAS_FISH_ROOT" "$SCRATCH_FISH_DIR/nas" || true
  ln -snf "$NAS_FISH_ROOT" "$WORK_SUBJECTS_DIR/$fish/nas" || true

  echo "Finished $fish"
done

echo -e "\nDone. Preprocessed data mirrored to WORK and staged for ANTs on SCRATCH."
