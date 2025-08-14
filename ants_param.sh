#!/usr/bin/env bash

# minimal adaptations for new folder structure on Curnagl HPC
# usage: bash ants_cluster_adapt.sh EXP ROUND PARTITION FISH1 [FISH2 ...]

ANTSPATH="$HOME/ANTs/antsInstallExample/install/bin"
export ANTSPATH
ANTSBIN="$ANTSPATH"
WALL_TIME="24:00:00"
MAIL_TYPE="END,FAIL"
MAIL_USER="danin.dharmaperwira@unil.ch"

if [ $# -lt 4 ]; then
  echo "Usage: $0 EXP ROUND PARTITION FISH1 [FISH2 ...]" >&2
  read -p "EXP: " EXP
  read -p "ROUND: " ROUND
  read -p "PARTITION: " PARTITION
  read -p "FISH IDs (space-separated): " FISH_IDS
  exit 1
else
  EXP="$1"; ROUND="$2"; PARTITION="$3"; FISH_IDS=( "${@:4}" )
fi

echo "Will register these fish in experiment $EXP, round $ROUND on partition $PARTITION:"
printf "  %s\n" "${FISH_IDS[@]}"

# SyN sweep values (value:tag) -> tags avoid dots in filenames
SYN_STEPS=("0.10:gs0100" "0.15:gs0150" "0.20:gs0200" "0.25:gs0250")

for FISH in "${FISH_IDS[@]}"; do
  echo "===== Processing subject $FISH ====="

  if [ "$PARTITION" = "test" ]; then
    QUEUE="interactive"; CPUS=1; MEM="8G"; TIME="1:00:00"
    echo "==> TEST mode: interactive (1 CPU, 8G, 1h)"
  else
    QUEUE="$PARTITION"; CPUS=48; MEM="256G"; TIME="$WALL_TIME"
  fi

  BASE="$SCRATCH/experiments/$EXP/subjects/$FISH"
  RAW_ANAT="$BASE/raw/anatomy_2P"
  RAW_CONF="$BASE/raw/confocal_round${ROUND}"
  FIXED="$BASE/fixed"
  REGDIR="$BASE/reg"
  LOGDIR="$REGDIR/logs"
  mkdir -p "$REGDIR" "$LOGDIR"

  # choose reference GCaMP: anatomy for round1, round1_ref for later
  if [ "$ROUND" -eq 1 ]; then
    REF_GCaMP="$FIXED/anatomy_2P_ref_GCaMP.nrrd"
  else
    REF_GCaMP="$FIXED/round1_ref_GCaMP.nrrd"
  fi

  # sanity check
  if [ ! -f "$REF_GCaMP" ]; then
    echo "Error: reference not found: $REF_GCaMP" >&2; exit 1; fi
  MOV_GCaMP="$RAW_CONF/round${ROUND}_GCaMP.nrrd"
  if [ ! -f "$MOV_GCaMP" ]; then
    echo "Error: moving GCaMP not found: $MOV_GCaMP" >&2; exit 1; fi

  echo "Registering GCaMP (two-phase: affine, then SyN sweep):"
  echo "  FIXED  = $REF_GCaMP"
  echo "  MOVING = $MOV_GCaMP"

  OUT_PREFIX="$REGDIR/round${ROUND}_GCaMP_to_ref"

  ##############################################################################
  # Build one big command string to run inside a single job
  ##############################################################################

  CMD_ALL="set -euo pipefail

  echo '--- Phase A: rigid+affine (linear only) ---'
  ${ANTSBIN}/antsRegistration \
    -d 3 --float 1 --verbose 1 \
    -o [${OUT_PREFIX},${OUT_PREFIX}_affineAligned.nrrd] \
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
    --smoothing-sigmas 4x3x2x1vox

  if [ ! -f ${OUT_PREFIX}0GenericAffine.mat ]; then
    echo 'ERROR: Missing affine file ${OUT_PREFIX}0GenericAffine.mat' >&2; exit 2
  fi

  echo '--- Phase B: SyN sweep (reusing affine) ---'"

  # loop over SyN steps; for each, run SyN-only then apply to GCaMP + channels
  for PAIR in "${SYN_STEPS[@]}"; do
    GS="${PAIR%%:*}"
    TAG="${PAIR##*:}"

    # prefixes per sweep
    SWEEP_PREFIX="${OUT_PREFIX}_${TAG}"

    CMD_ALL+="

    echo '>> SyN gradientStep=${GS} (${TAG})'
    ${ANTSBIN}/antsRegistration \
      -d 3 --float 1 --verbose 1 \
      -o [${SWEEP_PREFIX},${SWEEP_PREFIX}_synAligned.nrrd] \
      --interpolation WelchWindowedSinc \
      --winsorize-image-intensities [0,100] \
      --use-histogram-matching 1 \
      -r [${OUT_PREFIX}0GenericAffine.mat] \
      -t SyN[${GS},6,0.1] \
      -m CC[${REF_GCaMP},${MOV_GCaMP},1,4] \
      -c [200x200x200x200x10,1e-8,10] \
      --shrink-factors 12x8x4x2x1 \
      --smoothing-sigmas 4x3x2x1x0vox

    # Apply (warp, affine) to the GCaMP itself for consistency
    ${ANTSBIN}/antsApplyTransforms \
      -d 3 --verbose 1 \
      -r ${REF_GCaMP} \
      -i ${MOV_GCaMP} \
      -o ${SWEEP_PREFIX}_aligned.nrrd \
      -t ${SWEEP_PREFIX}0Warp.nii.gz \
      -t ${OUT_PREFIX}0GenericAffine.mat

    # Apply to all HCR channels in this round
    for MOV in ${RAW_CONF}/round${ROUND}_HCR_channel*.nrrd; do
      [ -f \"\$MOV\" ] || continue
      NAME=\$(basename \"\${MOV%.*}\")
      ${ANTSBIN}/antsApplyTransforms \
        -d 3 --verbose 1 \
        -r ${REF_GCaMP} \
        -i \"\$MOV\" \
        -o ${REGDIR}/\${NAME}_${TAG}_aligned.nrrd \
        -t ${SWEEP_PREFIX}0Warp.nii.gz \
        -t ${OUT_PREFIX}0GenericAffine.mat
    done
    "
  done

  # submit or run
  if [ "$QUEUE" = "interactive" ]; then
    eval "$CMD_ALL"
  else
    sbatch \
      --mail-type="$MAIL_TYPE" \
      --mail-user="$MAIL_USER" \
      -p "$QUEUE" \
      -N 1 -n 1 -c "$CPUS" --mem="$MEM" \
      -t "$TIME" \
      -J ants_${EXP}_${FISH}_r${ROUND}_sweep \
      --wrap="$CMD_ALL" \
      2> "$LOGDIR/registration_and_transform.err" \
      1> "$LOGDIR/registration_and_transform.out"
  fi

done
