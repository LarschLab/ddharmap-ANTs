#!/usr/bin/env bash
# ants_cluster_adapt.sh
# Interactive ANTs registration script all on $SCRATCH
set -euo pipefail

# ANTs installation
ANTSPATH="/users/${USER}/ANTs/antsInstallExample/install/bin"
export ANTSPATH
ANTSBIN="$ANTSPATH"

# Job resources
WALL_TIME="3-00:00:00"
CORES=48
MEM="256G"

# Base scratch experiments path
SCRATCH_BASE="${SCRATCH:-/scratch/ddharmap}/experiments"

# Interactive or CLI inputs
if [[ $# -lt 4 ]]; then
  echo "Please fill in the following values:"
  read -p "Experiment name: " EXP_NAME
  read -p "Fish ID: " FISH
  read -p "HCR round number: " ROUND
  read -p "Partition (cpu/gpu/...): " PARTITION
else
  EXP_NAME="$1"
  FISH="$2"
  ROUND="$3"
  PARTITION="$4"
fi

echo "Experiment: $EXP_NAME, Fish: $FISH, Round: $ROUND, Partition: $PARTITION"

# Paths within scratch
RAW_DIR="$SCRATCH_BASE/$EXP_NAME/subjects/$FISH/raw/confocal_round${ROUND}"
FIXED_DIR="$SCRATCH_BASE/$EXP_NAME/subjects/$FISH/fixed"
OUT_DIR="$SCRATCH_BASE/$EXP_NAME/subjects/$FISH/reg/round${ROUND}"
mkdir -p "$OUT_DIR"

# Define template & reference
TEMPLATE_GCAMP="$RAW_DIR/round${ROUND}_GCaMP.nrrd"
if [[ "$ROUND" -eq 1 ]]; then
  REF_GCAMP="$FIXED_DIR/anatomy_2P_ref_GCaMP.nrrd"
else
  PREV=$((ROUND-1))
  REF_GCAMP="$FIXED_DIR/round${PREV}_ref_GCaMP.nrrd"
fi

# Verify inputs exist
for f in "$TEMPLATE_GCAMP" "$REF_GCAMP"; do
  if [[ ! -f "$f" ]]; then
    echo "Error: required file not found: $f" >&2
    exit 1
  fi
done

# Output basename
BASENAME="round${ROUND}_GCaMP"
PREFIX_OUT="$OUT_DIR/$BASENAME"

# Registration parameters (21dpf defaults)
WINSOR="--winsorize-image-intensities [0,100]"
SYN_PARAMS="-t SyN[0.25,6,0.20]"
SHRINK_FACTORS="--shrink-factors 12x8x4x2"
SMOOTH_SIGMAS="--smoothing-sigmas 4x3x2x1vox"
CC_C="-m CC[${TEMPLATE_GCAMP},${REF_GCAMP},1,4] -c [200x200x200x200x10,1e-6,10]"

# Build commands
CMD1="${ANTSBIN}/antsRegistration -d 3 --float 1 --verbose 1 -o [${PREFIX_OUT},${PREFIX_OUT}_aligned] --interpolation WelchWindowedSinc ${WINSOR} --use-histogram-matching 1 -r [${TEMPLATE_GCAMP},${REF_GCAMP},1] -t rigid[0.1] -m MI[${TEMPLATE_GCAMP},${REF_GCAMP},1,32,Regular,0.25] -c [200x200x200x200,1e-6,10] --shrink-factors 12x8x4x2 --smoothing-sigmas 4x3x2x1vox -t Affine[0.1] -m MI[${TEMPLATE_GCAMP},${REF_GCAMP},1,32,Regular,0.25] -c [200x200x200x200,1e-6,10] --shrink-factors 12x8x4x2 --smoothing-sigmas 4x3x2x1vox ${SYN_PARAMS} ${CC_C} ${SHRINK_FACTORS} ${SMOOTH_SIGMAS}"
CMD2="${ANTSBIN}/antsApplyTransforms -d 3 --verbose 1 -r ${REF_GCAMP} -i ${TEMPLATE_GCAMP} -o ${PREFIX_OUT}_aligned_ref.nrrd -t [${PREFIX_OUT}0GenericAffine.mat,1] -t ${PREFIX_OUT}1InverseWarp.nii.gz"

# Submit to SLURM
sbatch -p ${PARTITION} -N1 -n1 -c${CORES} --mem=${MEM} -t ${WALL_TIME} \
       -J ants_${EXP_NAME}_${FISH}_r${ROUND} \
       --mail-type=END,FAIL \
       --mail-user=danin.dharmaperwira@unil.ch \
       --wrap="${CMD1} && ${CMD2}"
