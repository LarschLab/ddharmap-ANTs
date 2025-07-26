#!/usr/bin/env bash
# ants_round_align.sh
# Registers per-fish, per-round GCaMP stacks using ANTs on Curnagl
# Entirely operates off $SCRATCH with mirrored experiment folder structure.

set -euo pipefail

# ANTs installation
ANTSPATH="/users/${USER}/ANTs/antsInstallExample/install/bin"
export ANTSPATH
ANTSBIN="$ANTSPATH"

# Cluster settings
WALL_TIME="3-00:00:00"
CORES=48
MEM="256G"

# Base scratch path (override with env var if needed)
SCRATCH_BASE="${SCRATCH:-/scratch/ddharmap}/experiments"

usage() {
  echo "Usage: $0 <experiment> <fishID> <round> <partition>"
  echo "  <round> = 1,2,3,...; first round aligns to 2P; subsequent to previous round"
  exit 1
}

[[ $# -eq 4 ]] || usage
EXP_NAME="$1"
FISH="$2"
ROUND="$3"
PARTITION="$4"

# Paths on scratch (data copied there beforehand)
INPUT_BASE="$SCRATCH_BASE/$EXP_NAME/data/subjects/$FISH"
RAW_DIR="$INPUT_BASE/raw/confocal_round${ROUND}"
FIXED_DIR="$INPUT_BASE/fixed"
OUT_DIR="$SCRATCH_BASE/$EXP_NAME/subjects/$FISH/reg/round${ROUND}"

mkdir -p "$OUT_DIR"

# Determine template and reference
TEMPLATE_GCAMP="$RAW_DIR/round${ROUND}_GCaMP.nrrd"
if [[ "$ROUND" -eq 1 ]]; then
  REF_GCAMP="$FIXED_DIR/anatomy_2P_ref_GCaMP.nrrd"
else
  PREV=$((ROUND-1))
  REF_GCAMP="$FIXED_DIR/round${PREV}_ref_GCaMP.nrrd"
fi

# Sanity checks
for f in "$TEMPLATE_GCAMP" "$REF_GCAMP"; do
  if [[ ! -f "$f" ]]; then
    echo "Error: missing file: $f" >&2
    exit 1
  fi
done

# Base name for outputs
BASENAME="round${ROUND}_GCaMP"
PREFIX_OUT="$OUT_DIR/$BASENAME"

# 21dpf specialized parameters (default)
WINSOR="--winsorize-image-intensities [0,100]"
SYN_PARAMS="-t SyN[0.25,6,0.20]"
SHRINK_FACTORS="--shrink-factors 12x8x4x2"
SMOOTH_SIGMAS="--smoothing-sigmas 4x3x2x1vox"
CC_C="-m CC[${TEMPLATE_GCAMP},${REF_GCAMP},1,4] -c [200x200x200x200x10,1e-6,10]"

# antsRegistration command
ANTSCALL="${ANTSBIN}/antsRegistration \
  -d 3 --float 1 --verbose 1 \
  -o [${PREFIX_OUT},${PREFIX_OUT}_aligned] \
  --interpolation WelchWindowedSinc \
  ${WINSOR} \
  --use-histogram-matching 1 \
  -r [${TEMPLATE_GCAMP},${REF_GCAMP},1] \
  -t rigid[0.1] \
  -m MI[${TEMPLATE_GCAMP},${REF_GCAMP},1,32,Regular,0.25] \
  -c [200x200x200x200,1e-6,10] \
  ${SHRINK_FACTORS} \
  ${SMOOTH_SIGMAS} \
  -t Affine[0.1] \
  -m MI[${TEMPLATE_GCAMP},${REF_GCAMP},1,32,Regular,0.25] \
  -c [200x200x200x200,1e-6,10] \
  ${SHRINK_FACTORS} \
  ${SMOOTH_SIGMAS} \
  ${SYN_PARAMS} \
  ${CC_C}"

# Submit job
sbatch -p ${PARTITION} -N1 -n1 -c${CORES} --mem=${MEM} -t ${WALL_TIME} \
       -J ants_${EXP_NAME}_${FISH}_r${ROUND} \
       --wrap="${ANTSCALL} && \
         ${ANTSBIN}/antsApplyTransforms -d 3 --verbose 1 \
           -r ${REF_GCAMP} \
           -i ${TEMPLATE_GCAMP} \
           -o ${PREFIX_OUT}_aligned_ref.nrrd \
           -t [${PREFIX_OUT}0GenericAffine.mat,1] \
           -t ${PREFIX_OUT}1InverseWarp.nii.gz"
