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
  read -p "Partition (cpu/gpu/... or test): " PARTITION
else
  EXP=$1; FISH=$2; ROUND=$3; PARTITION=$4
fi

# define directories
BASE="$SCRATCH/experiments/$EXP/subjects/$FISH"
RAW_ANAT="$BASE/raw/anatomy_2P"
RAW_CONF="$BASE/raw/confocal_round${ROUND}"
FIXED="$BASE/fixed"
REGDIR="$BASE/reg"
LOGDIR="$REGDIR/logs"
mkdir -p "$LOGDIR"

# choose reference GCaMP
if [ "$ROUND" -eq 1 ]; then
  REF_GCaMP="$FIXED/anatomy_2P_ref_GCaMP.nrrd"
else
  REF_GCaMP="$FIXED/round1_ref_GCaMP.nrrd"
fi

# sanity check
if [ ! -f "$REF_GCaMP" ]; then
  echo "Error: reference not found: $REF_GCaMP" >&2
  exit 1
fi

# moving GCaMP
MOV_GCaMP="$RAW_CONF/round${ROUND}_GCaMP.nrrd"
if [ ! -f "$MOV_GCaMP" ]; then
  echo "Error: moving GCaMP not found: $MOV_GCaMP" >&2
  exit 1
fi

echo "Registering GCaMP:"
echo "  FIXED  = $REF_GCaMP"
echo "  MOVING = $MOV_GCaMP"

OUT_PREF_G="$REGDIR/round${ROUND}_GCaMP_to_ref"

# build registration + applyTransforms command for GCaMP
CMD_REG="${ANTSBIN}/antsRegistration \
  -d 3 --float 1 --verbose 1 \
  -o [${OUT_PREF_G},${OUT_PREF_G}_aligned.nrrd] \
  --interpolation WelchWindowedSinc \
  --winsorize-image-intensities [0,100] \
  --use-histogram-matching 1 \
  -r [${REF_GCaMP},${MOV_GCaMP},1] \
  -t rigid[0.1] \
  -m MI[${REF_GCaMP},${MOV_GCaMP},1,32,Regular,0.25] \
  -c [200x200x200x200,1e-6,10] \
  --shrink-factors 12x8x4x2 \
  --smoothing-sigmas 4x3x2x1vox \
  -t Affine[0.1] \
  -m MI[${REF_GCaMP},${MOV_GCaMP},1,32,Regular,0.25] \
  -c [200x200x200x200,1e-6,10] \
  --shrink-factors 12x8x4x2 \
  --smoothing-sigmas 4x3x2x1vox \
  -t SyN[0.1,6,0.1] \
  -m CC[${REF_GCaMP},${MOV_GCaMP},1,4] \
  -c [200x200x200x200x10,1e-8,10] \
  --shrink-factors 12x8x4x2x1 \
  --smoothing-sigmas 4x3x2x1x0vox && \
${ANTSBIN}/antsApplyTransforms \
  -d 3 --verbose 1 \
  -r ${REF_GCaMP} \
  -i ${MOV_GCaMP} \
  -o ${OUT_PREF_G}_aligned.nrrd \
  -t ${OUT_PREF_G}1Warp.nii.gz \
  -t ${OUT_PREF_G}0GenericAffine.mat"

if [ "$PARTITION" = "test" ]; then
  echo "==> TEST mode: running interactively on node"
  eval "$CMD_REG"
else
  sbatch \
    --mail-type="$MAIL_TYPE" \
    --mail-user="$MAIL_USER" \
    -p "$PARTITION" \
    -N 1 -n 1 -c 48 --mem=256G \
    -t "$WALL_TIME" \
    -J ants_${EXP}_${FISH}_r${ROUND}_GCaMP \
    --wrap="$CMD_REG" \
    2> "$LOGDIR/GCaMP.err" \
    1> "$LOGDIR/GCaMP.out"
fi

# now apply transforms to the HCR channels
echo
echo "Applying transforms to HCR channels:"
for MOV in "$RAW_CONF"/round${ROUND}_channel*.nrrd; do
  [ -f "$MOV" ] || continue
  NAME=$(basename "${MOV%.*}")
  echo "  $MOV → $REF_GCaMP"
  OUT_PREF_C="$REGDIR/${NAME}_to_round${ROUND}"

  CMD_APP="${ANTSBIN}/antsApplyTransforms \
    -d 3 --verbose 1 \
    -r ${REF_GCaMP} \
    -i ${MOV} \
    -o ${OUT_PREF_C}_aligned.nrrd \
    -t ${OUT_PREF_G}1Warp.nii.gz \
    -t ${OUT_PREF_G}0GenericAffine.mat"

  if [ "$PARTITION" = "test" ]; then
    echo "==> TEST apply: $CMD_APP"
    eval "$CMD_APP"
  else
    sbatch \
      --mail-type="$MAIL_TYPE" \
      --mail-user="$MAIL_USER" \
      -p "$PARTITION" \
      -N 1 -n 1 -c 48 --mem=256G \
      -t "$WALL_TIME" \
      -J ants_${EXP}_${FISH}_r${ROUND}_${NAME} \
      --wrap="$CMD_APP" \
      2> "$LOGDIR/${NAME}.err" \
      1> "$LOGDIR/${NAME}.out"
  fi
done
