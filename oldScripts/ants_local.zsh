#!/usr/bin/env zsh
# Local (non-SLURM) ANTs runner for your flat layout:
#   ~/ANTs/experiments/<EXP>/<FISH>/{anatomy_2P, confocal_round1, confocal_round2, ...}
# usage: ./ants_local.zsh EXP ROUND FISH1 [FISH2 ...]
set -euo pipefail

# ---------- Find ANTs binaries ----------
export ANTSPATH="/Users/ddharmap/ANTs/antsInstallExample/install/bin"
export PATH="$ANTSPATH:$PATH"
ANTSBIN="$ANTSPATH"

# ---------- Args (interactive fallback) ----------
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

# ---------- Threads (favor Apple Silicon perf cores) ----------
PERF_CORES=$(sysctl -n hw.perflevel0.physicalcpu_max 2>/dev/null || echo 0)
LOG_CORES=$(sysctl -n hw.logicalcpu_max 2>/dev/null || echo 4)
if (( PERF_CORES > 0 )); then USE_THREADS=$PERF_CORES
else USE_THREADS=$(( LOG_CORES > 2 ? LOG_CORES - 2 : 1 ))
fi
export ITK_GLOBAL_DEFAULT_NUMBER_OF_THREADS="${ITK_THREADS:-$USE_THREADS}"
export OMP_NUM_THREADS="${OMP_THREADS:-$USE_THREADS}"
export VECLIB_MAXIMUM_THREADS="${VECLIB_MAXIMUM_THREADS:-$USE_THREADS}"

# ---------- Root dir ----------
ROOT_DIR="${ROOT_DIR:-$HOME/ANTs}"   # <- matches your setup
print "Mode: LOCAL | ANTs: $ANTSBIN | Threads: $ITK_GLOBAL_DEFAULT_NUMBER_OF_THREADS"
print "Root: $ROOT_DIR"
print "Experiment: $EXP | Round: $ROUND | Fish: ${FISH_IDS[*]}"

for FISH in "${FISH_IDS[@]}"; do
  print "\n===== Processing subject: $FISH ====="

  BASE="$ROOT_DIR/experiments/$EXP/$FISH"
  ANAT_DIR="$BASE/anatomy_2P"
  CONF_DIR="$BASE/confocal_round${ROUND}"
  FIXED="$BASE/fixed"
  REGDIR="$BASE/reg"
  LOGDIR="$REGDIR/logs"
  mkdir -p "$FIXED" "$REGDIR" "$LOGDIR"

  # --------- Determine reference for this round ----------
  if [[ "$ROUND" == "1" ]]; then
    # For round 1, keep using the anatomy reference as FIXED
    REF_GCaMP="$FIXED/anatomy_2P_ref_GCaMP.nrrd"
    if [[ ! -f "$REF_GCaMP" ]]; then
      # Make sure anatomy_2P/anatomy_2P_GCaMP.nrrd exists, then link it as the round1 fixed
      if [[ -f "$ANAT_DIR/anatomy_2P_GCaMP.nrrd" ]]; then
        ln -sf "$ANAT_DIR/anatomy_2P_GCaMP.nrrd" "$REF_GCaMP"
        print "Linked anatomy ref: $REF_GCaMP -> $ANAT_DIR/anatomy_2P_GCaMP.nrrd"
      else
        print -u2 "Missing anatomy ref: $ANAT_DIR/anatomy_2P_GCaMP.nrrd"; exit 1
      fi
    fi
  else
    # For later rounds, FIXED is the RAW round1 GCaMP (not the aligned one)
    REF_GCaMP="$FIXED/round1_ref_GCaMP.nrrd"
    if [[ ! -f "$REF_GCaMP" ]]; then
      SRC="$BASE/confocal_round1/round1_GCaMP.nrrd"
      if [[ -f "$SRC" ]]; then
        ln -sf "$SRC" "$REF_GCaMP"
        print "Linked round1 ref: $REF_GCaMP -> $SRC"
      else
        print -u2 "Missing round1 GCaMP to use as ref: $SRC"; exit 1
      fi
    fi
  fi


  # --------- Moving image for this round ----------
  MOV_GCaMP="$CONF_DIR/round${ROUND}_GCaMP.nrrd"
  if [[ ! -f "$MOV_GCaMP" ]]; then
    print -u2 "Missing moving GCaMP: $MOV_GCaMP"
    exit 1
  fi

  OUT_PREFIX="$REGDIR/round${ROUND}_GCaMP_to_ref"

  # --------- Build composite command ----------
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

  for MOV in "$CONF_DIR"/round${ROUND}_HCR_channel*.nrrd; do
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

done
