#!/usr/bin/env bash
# usage: bash ants_param.sh EXP ROUND PARTITION FISH1 [FISH2 ...]
# Runs a 2-phase registration: (A) rigid+affine once, (B) SyN sweeps reusing the affine.
# Submits one SLURM job per fish and writes clean per-job logs under reg/logs/.

ANTSPATH="$HOME/ANTs/antsInstallExample/install/bin"
export ANTSPATH
ANTSBIN="$ANTSPATH"

WALL_TIME="24:00:00"
MAIL_TYPE="END,FAIL"
MAIL_USER="danin.dharmaperwira@unil.ch"

# --- args (interactive fallback; no early exit) ---
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

echo "Will register these fish in experiment $EXP, round $ROUND on partition $PARTITION:"
printf "  %s\n" "${FISH_IDS[@]}"

# Sweep updateFieldSigma with gradStep fixed at 0.25
UFIELD_STEPS=("2.0:uf0200" "4.0:uf0400")

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
    echo "Error: reference not found: $REF_GCaMP" >&2; continue; fi
  MOV_GCaMP="$RAW_CONF/round${ROUND}_GCaMP.nrrd"
  if [ ! -f "$MOV_GCaMP" ]; then
    echo "Error: moving GCaMP not found: $MOV_GCaMP" >&2; continue; fi

  echo "Registering GCaMP (two-phase: affine, then SyN sweep):"
  echo "  FIXED  = $REF_GCaMP"
  echo "  MOVING = $MOV_GCaMP"

  OUT_PREFIX="$REGDIR/round${ROUND}_GCaMP_to_ref"

  # ---------- Build a per-fish job script (robust logging & shell) ----------
  JOBNAME="ants_${EXP}_${FISH}_r${ROUND}_sweep"
  JOBSCRIPT="$LOGDIR/${JOBNAME}.sh"

  cat > "$JOBSCRIPT" <<EOF
#!/usr/bin/env bash
set -euo pipefail

echo "Host: \$(hostname)"
echo "Start: \$(date)"


echo "--- Phase A: rigid+affine ---"
"${ANTSBIN}/antsRegistration" \
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

if [ ! -f "${OUT_PREFIX}0GenericAffine.mat" ]; then
  echo "ERROR: Missing affine file ${OUT_PREFIX}0GenericAffine.mat" >&2
  exit 2
fi

echo "--- Phase B: SyN sweep (reusing affine) ---"
EOF

  # append SyN sweeps
  for PAIR in "${UFIELD_STEPS[@]}"; do
  UF="${PAIR%%:*}"         # updateFieldSigma (e.g., 2.0, 4.0)
  TAG="${PAIR##*:}"        # tag for filenames (uf0200, uf0400)
  SWEEP_PREFIX="${OUT_PREFIX}_${TAG}"

  cat >> "$JOBSCRIPT" <<EOF
echo ">> SyN gradStep=0.25, updateFieldSigma=${UF} (${TAG})"
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

# Pick the warp file emitted when using an initial transform (usually 1Warp)
WARP="${SWEEP_PREFIX}1Warp.nii.gz"
if [ ! -f "\$WARP" ]; then WARP="${SWEEP_PREFIX}0Warp.nii.gz"; fi
if [ ! -f "\$WARP" ]; then
  echo "ERROR: missing warp for ${SWEEP_PREFIX} (tried 1Warp/0Warp)" >&2
  exit 3
fi

# For GCaMP, avoid redundant reslice: link our conventional name to synAligned
ln -sf "${SWEEP_PREFIX}_synAligned.nrrd" "${SWEEP_PREFIX}_aligned.nrrd"

# Apply to all HCR channels (with high-quality interpolation)
had_hcr=0
for MOV in ${RAW_CONF}/round${ROUND}_HCR_channel*.nrrd; do
  [ -f "\$MOV" ] || continue
  had_hcr=1
  NAME=\$(basename "\${MOV%.*}")
  "${ANTSBIN}/antsApplyTransforms" \
    -d 3 --verbose 1 \
    --interpolation WelchWindowedSinc \
    -r ${REF_GCaMP} \
    -i "\$MOV" \
    -o ${REGDIR}/\${NAME}_${TAG}_aligned.nrrd \
    -t "\$WARP" \
    -t ${AFFINE}
done
if [ "\$had_hcr" -eq 0 ]; then
  echo "Note: no HCR channels found in ${RAW_CONF}"
fi
EOF
done

  cat >> "$JOBSCRIPT" <<'EOF'
echo "End: $(date)"
EOF

  chmod +x "$JOBSCRIPT"

  # ---------- Submit or run ----------
  if [ "$QUEUE" = "interactive" ]; then
    # Run synchronously (test partition)
    bash "$JOBSCRIPT" \
      1> "$LOGDIR/${JOBNAME}.out" \
      2> "$LOGDIR/${JOBNAME}.err"
    status=$?
    if [ $status -eq 0 ]; then
      echo "✅ Finished $JOBNAME (interactive). Logs: $LOGDIR/${JOBNAME}.out"
    else
      echo "❌ Failed $JOBNAME (interactive). See: $LOGDIR/${JOBNAME}.err"
    fi
  else
    # Submit to SLURM with proper job log files
    sbatch \
      --mail-type="$MAIL_TYPE" \
      --mail-user="$MAIL_USER" \
      -p "$QUEUE" \
      -N 1 -n 1 -c "$CPUS" --mem="$MEM" \
      -t "$TIME" \
      -J "$JOBNAME" \
      --output="$LOGDIR/%x.%j.out" \
      --error="$LOGDIR/%x.%j.err" \
      "$JOBSCRIPT"
    echo "Submitted $JOBNAME. Logs will be in $LOGDIR/%x.%j.{out,err}"
  fi

done