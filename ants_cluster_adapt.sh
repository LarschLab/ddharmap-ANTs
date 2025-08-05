#!/usr/bin/env bash

# minimal adaptations for new folder structure on Curnagl HPC
# usage: bash ants_cluster_adapt.sh [Experiment] [FishID] [Round] [Partition]

# --- user settings & defaults ---
ANTSPATH="$HOME/ANTs/antsInstallExample/install/bin"
export ANTSPATH
ANTSBIN="$ANTSPATH"
WALL_TIME="24:00:00"

# SBATCH mail notifications
MAIL_TYPE="END,FAIL"
MAIL_USER="danin.dharmaperwira@unil.ch"

# parse args or prompt
if [ $# -lt 4 ]; then
  read -p "Experiment name: " EXP
  read -p "Fish ID: " FISH
  read -p "HCR round number: " ROUND
  read -p "Partition (cpu/gpu/...): " PARTITION
else
  EXP=$1; FISH=$2; ROUND=$3; PARTITION=$4
fi

# define directories
BASE="$SCRATCH/experiments/$EXP/subjects/$FISH"
RAW_ANAT="$BASE/raw/anatomy_2P"
RAW_CONF="$BASE/raw/confocal_round${ROUND}"
FIXED="$BASE/fixed"
REGDIR="$BASE/reg"
mkdir -p "$REGDIR"

# reference & inputs
REF_GCaMP="$FIXED/round${ROUND}_ref_GCaMP.nrrd"
ANAT_GCaMP="$RAW_ANAT/anatomy_2P_GCaMP.nrrd"

# loop over channels (exclude the GCaMP bridge)
for MOV in "$RAW_CONF"/round${ROUND}_channel*.nrrd; do
  echo "\n=== aligning $MOV to $REF_GCaMP ==="
  NAME=$(basename "${MOV%.*}")
  OUT_PREFIX="$REGDIR/${NAME}_to_round${ROUND}"

  # antsRegistration call with 21dpf params
  CMD="${ANTSBIN}/antsRegistration \
    -d 3 --float 1 --verbose 1 \
    -o [${OUT_PREFIX},${OUT_PREFIX}_aligned.nrrd] \
    --interpolation WelchWindowedSinc \
    --winsorize-image-intensities [0,100] \
    --use-histogram-matching 1 \
    -r [${REF_GCaMP},${MOV},1] \
    -t rigid[0.1] \
    -m MI[${REF_GCaMP},${MOV},1,32,Regular,0.25] \
    -c [200x200x200x200,1e-6,10] \
    --shrink-factors 12x8x4x2 \
    --smoothing-sigmas 4x3x2x1vox \
    -t Affine[0.1] \
    -m MI[${REF_GCaMP},${MOV},1,32,Regular,0.25] \
    -c [200x200x200x200,1e-6,10] \
    --shrink-factors 12x8x4x2 \
    --smoothing-sigmas 4x3x2x1vox \
    -t SyN[0.25,6,0.20] \
    -m CC[${REF_GCaMP},${MOV},1,4] \
    -c [200x200x200x200x10,1e-6,10] \
    --shrink-factors 12x8x4x2x1 \
    --smoothing-sigmas 4x3x2x1x0vox"

  sbatch \
    --mail-type=$MAIL_TYPE \
    --mail-user=$MAIL_USER \
    -p $PARTITION \
    -N 1 -n 1 -c 48 --mem=256G \
    -t $WALL_TIME \
    -J ants_${EXP}_${FISH}_r${ROUND} \
    --wrap="$CMD"
done
