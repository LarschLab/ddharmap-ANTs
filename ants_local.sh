#!/usr/bin/env zsh
# Local (non-SLURM) ANTs runner for registration + HCR transform application
# usage: ./ants_local.zsh EXP ROUND FISH1 [FISH2 ...]
# If you omit args, you'll be prompted interactively.

set -euo pipefail

### --- Find ANTs binaries ---
if command -v antsRegistration >/dev/null 2>&1; then
  ANTSBIN="$(dirname "$(command -v antsRegistration)")"
elif [[ -n "${ANTSPATH:-}" && -x "$ANTSPATH/antsRegistration" ]]; then
  ANTSBIN="$ANTSPATH"; export PATH="$ANTSPATH:$PATH"
elif [[ -x "/opt/homebrew/bin/antsRegistration" ]]; then
  ANTSBIN="/opt/homebrew/bin"; export PATH="$ANTSBIN:$PATH"
else
  print -u2 "Could not find antsRegistration. Put ANTs on PATH or set ANTSPATH."
  exit 1
fi

### --- Args (interactive fallback) ---
typeset EXP ROUND
typeset -a FISH_IDS
if (( $# < 3 )); then
  print -u2 "Usage: $0 EXP ROUND FISH1 [FISH2 ...]"
  vared -p "EXP: " -c EXP
  vared -p "ROUND (integer): " -c ROUND
  print -n "FISH IDs (space-separated): "; read -rA FISH_IDS
else
  EXP="$1"; ROUND="$2"; shift 2
  FISH_IDS=("$@")
fi

### --- Threads (favor performance cores on Apple Silicon) ---
# Try to read perf/eff core counts; fall back to logical cores.
PERF_CORES=$(sysctl -n hw.perflevel0.physicalcpu_max 2>/dev/null || echo 0)
LOG_CORES=$(sysctl -n hw.logicalcpu_max 2>/dev/null || echo 4)
# Default: use perf cores if available, else (logical - 2), but at least 1.
if (( PERF_CORES > 0 )); then
  USE_THREADS=$PERF_CORES
else
  USE_THREADS=$(( LOG_CORES > 2 ? LOG_CORES - 2 : 1 ))
fi
export ITK_GLOBAL_DEFAULT_NUMBER_OF_THREADS="${ITK_THREADS:-$USE_THREADS}"
export OMP_NUM_THREADS="${OMP_THREADS:-$USE_THREADS}"
# If Accelerate is used anywhere, keep it from oversubscribing:
export VECLIB_MAXIMUM_THREADS="${VECLIB_MAXIMUM_THREADS:-$USE_THREADS}"

### --- Root dir ---
ROOT_DIR="${ROOT_DIR:-$PWD}"

print "Mode: LOCAL | ANTs: $ANTSBIN | Threads: $ITK_GLOBAL_DEFAULT_NUMBER_OF_THREADS"
print "Root: $ROOT_DIR"
print "Experiment: $EXP | Round: $ROUND | Fish: ${FISH_IDS[*]}"

for FISH in "${FISH_IDS[@]}"; do
  print "\n===== Processing subject: $FISH ====="

  BASE="$ROOT_DIR/experiments/$EXP/subjects/$FISH"
  RAW_ANAT="$BASE/raw/anatomy_2P"
  RAW_CONF="$BASE/raw/confocal_round${ROUND}"
  FIXED="$BASE/fixed"
  REGDIR="$BASE/reg"
  LOGDIR="$REGDIR/logs"
  mkdir -p "$REGDIR" "$LOGDIR"

  # Reference: anatomy ref for round1, otherwise round1 ref
  if [[ "$ROUND" == "1" ]]; then
    REF_GCaMP="$FIXED/anatomy_2P_ref_GCaMP.nrrd"
  else
    REF_GCaMP="$FIXED/round1_ref_GCaMP.nrrd"
  fi

  # Inputs
  if [[ ! -f "$REF_GCaMP" ]]; then print -u2 "Missing $REF_GCaMP"; exit 1; fi
  MOV_GCaMP="$RAW_CONF/round${ROUND}_GCaMP.nrrd"
  if [[ ! -f "$MOV_GCaMP" ]]; then print -u2 "Missing $MOV_GCaMP"; exit 1; fi

  OUT_PREFIX="$REGDIR/round${ROUND}_GCaMP_to_ref"

  # Build composite command: register + apply to GCaMP, then all HCR channels
  CMD_ALL="$ANTSBIN/antsRegistration \
    -d 3 --float 1 --verbose 1 \
    -o [${OUT_PREFIX},${OUT_PREFIX}_aligned.nrrd] \
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
  $ANTSBIN/antsApplyTransforms \
    -d 3 --verbose 1 \
    -r ${REF_GCaMP} \
    -i ${MOV_GCaMP} \
    -o ${OUT_PREFIX}_aligned.nrrd \
    -t ${OUT_PREFIX}1Warp.nii.gz \
    -t ${OUT_PREFIX}0GenericAffine.mat"

  for MOV in "$RAW_CONF"/round${ROUND}_HCR_channel*.nrrd; do
    [[ -f "$MOV" ]] || continue
    NAME="${${MOV##*/}%.*}"
    CMD_ALL+=" && $ANTSBIN/antsApplyTransforms \
      -d 3 --verbose 1 \
      -r ${REF_GCaMP} \
      -i ${MOV} \
      -o ${REGDIR}/${NAME}_aligned.nrrd \
      -t ${OUT_PREFIX}1Warp.nii.gz \
      -t ${OUT_PREFIX}0GenericAffine.mat"
  done

  JOBNAME="ants_${EXP}_${FISH}_r${ROUND}_all"
  print "Running: $JOBNAME"
  time bash -lc "$CMD_ALL" \
    1> "$LOGDIR/${JOBNAME}.out" \
    2> "$LOGDIR/${JOBNAME}.err"
  print "Done: $JOBNAME"
done
