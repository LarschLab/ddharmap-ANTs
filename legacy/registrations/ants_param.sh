#!/usr/bin/env bash
# usage: bash ants_sweep_only_gc.sh EXP ROUND PARTITION FISH1 [FISH2 ...]
# SyN-only parameter sweep reusing an existing affine; GCaMP only (no HCR applies).
# Expects: reg/round${ROUND}_GCaMP_to_ref0GenericAffine.mat

set -euo pipefail

ANTSPATH="$HOME/ANTs/antsInstallExample/install/bin"
export ANTSPATH
ANTSBIN="$ANTSPATH"

WALL_TIME="24:00:00"
MAIL_TYPE="END,FAIL"
MAIL_USER="danin.dharmaperwira@unil.ch"

# --- args (prompt if missing; no early exit) ---
if [ $# -lt 4 ]; then
  echo "Usage: $0 EXP ROUND PARTITION FISH1 [FISH2 ...]" >&2
  read -r -p "EXP: " EXP
  read -r -p "ROUND: " ROUND
  read -r -p "PARTITION (e.g., cpu or test): " PARTITION
  read -r -p "FISH IDs (space-separated): " FISH_LINE
  read -r -a FISH_IDS <<< "$FISH_LINE"
else
  EXP="$1"; ROUND="$2"; PARTITION="$3"; shift 3
  FISH_IDS=( "$@" )
fi

echo "SyN-only (GCaMP only) for experiment $EXP, round $ROUND on partition $PARTITION:"
printf "  %s\n" "${FISH_IDS[@]}"

# resources per fish/job
if [ "$PARTITION" = "test" ]; then
  QUEUE="interactive"; CPUS=1; MEM="8G"; TIME="1:00:00"
  echo "==> TEST mode: interactive (1 CPU, 8G, 1h)"
else
  QUEUE="$PARTITION"; CPUS=48; MEM="256G"; TIME="$WALL_TIME"
fi

# Sweep updateFieldSigma; fixed gradStep=0.25, totalFieldSigma=0.1
UFIELD_STEPS=("2.0:uf0200" "4.0:uf0400")
# add "5.0:uf0500" if you want X=5.0 as well

for FISH in "${FISH_IDS[@]}"; do
  echo "===== Processing subject $FISH ====="

  BASE="$SCRATCH/experiments/$EXP/subjects/$FISH"
  RAW_CONF="$BASE/raw/confocal_round${ROUND}"
  FIXED="$BASE/fixed"
  REGDIR="$BASE/reg"
  LOGDIR="$REGDIR/logs"
  mkdir -p "$REGDIR" "$LOGDIR"

  # fixed selection: round1 uses anatomy ref; later uses round1 ref
  if [ "$ROUND" -eq 1 ]; then
    REF_GCaMP="$FIXED/anatomy_2P_ref_GCaMP.nrrd"
  else
    REF_GCaMP="$FIXED/round1_ref_GCaMP.nrrd"
  fi
  MOV_GCaMP="$RAW_CONF/round${ROUND}_GCaMP.nrrd"
  AFFINE="${REGDIR}/round${ROUND}_GCaMP_to_ref0GenericAffine.mat"

  # sanity checks
  missing=0
  [ -f "$REF_GCaMP" ] || { echo "Missing fixed:  $REF_GCaMP" >&2; missing=1; }
  [ -f "$MOV_GCaMP" ] || { echo "Missing moving: $MOV_GCaMP" >&2; missing=1; }
  [ -f "$AFFINE"   ] || { echo "Missing affine: $AFFINE (run Phase A first)" >&2; missing=1; }
  [ $missing -eq 0 ] || { echo "Skipping $FISH due to missing inputs."; continue; }

  OUT_PREFIX="$REGDIR/round${ROUND}_GCaMP_to_ref"
  JOBNAME="ants_${EXP}_${FISH}_r${ROUND}_sweepOnly_GC"
  JOBSCRIPT="$LOGDIR/${JOBNAME}.sh"

  # ---------- per-fish job script ----------
  cat > "$JOBSCRIPT" <<EOF
#!/usr/bin/env bash
set -euo pipefail
echo "Host: \$(hostname)"
echo "Start: \$(date)"
echo "Using affine: ${AFFINE}"

EOF

  for PAIR in "${UFIELD_STEPS[@]}"; do
    UF="${PAIR%%:*}"       # updateFieldSigma
    TAG="${PAIR##*:}"      # uf0200, uf0400
    SWEEP_PREFIX="${OUT_PREFIX}_${TAG}"

    cat >> "$JOBSCRIPT" <<EOF
echo ">> SyN[0.25, ${UF}, 0.1]  (${TAG})"
"${ANTSBIN}/antsRegistration" \
  -d 3 --float 1 --verbose 1 \
  --initial-moving-transform ${AFFINE} \
  -o [${SWEEP_PREFIX},${SWEEP_PREFIX}_synAligned.nrrd] \
  --interpolation WelchWindowedSinc \
  --winsorize-image-intensities [0,100] \
  --use-histogram-matching 1 \
  -t SyN[0.25,${UF},0.1] \
  -m CC[${REF_GCaMP},${MOV_GCaMP},1,4] \
  -c [200x200x200x200x10,1e-8,10] \
  --shrink-factors 12x8x4x2x1 \
  --smoothing-sigmas 4x3x2x1x0vox

# Keep a conventional name for the GCaMP without re-resampling:
ln -sf "${SWEEP_PREFIX}_synAligned.nrrd" "${SWEEP_PREFIX}_aligned.nrrd"
EOF
  done

  cat >> "$JOBSCRIPT" <<'EOF'
echo "End: $(date)"
EOF

  chmod +x "$JOBSCRIPT"

  # ---------- submit / run ----------
  if [ "$QUEUE" = "interactive" ]; then
    bash "$JOBSCRIPT" \
      1> "$LOGDIR/${JOBNAME}.out" \
      2> "$LOGDIR/${JOBNAME}.err"
    status=$?
    if [ $status -eq 0 ]; then
      echo "✅ Finished $JOBNAME (interactive)."
    else
      echo "❌ Failed $JOBNAME (interactive). See logs in: $LOGDIR"
    fi
  else
    sbatch \
      --mail-type="$MAIL_TYPE" --mail-user="$MAIL_USER" \
      -p "$QUEUE" -N 1 -n 1 -c "$CPUS" --mem="$MEM" \
      -t "$TIME" \
      -J "$JOBNAME" \
      --output="$LOGDIR/%x.%j.out" \
      --error="$LOGDIR/%x.%j.err" \
      "$JOBSCRIPT"
    echo "Submitted $JOBNAME. Logs: $LOGDIR/%x.%j.{out,err}"
  fi
done
