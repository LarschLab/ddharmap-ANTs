#!/usr/bin/env bash
# usage: bash ants_sweep_rigid_gc_r1_to_2p.sh PARTITION FISH1 [FISH2 ...]
# Rigid-only parameter sweep (GCaMP), mapping R1 -> 2P.
# Sweeps: Rigid[gradStep] with gradStep in {0.15, 0.20, 0.25}

set -euo pipefail
shopt -s nullglob

ANTSPATH="$HOME/ANTs/antsInstallExample/install/bin"
export ANTSPATH
ANTSBIN="$ANTSPATH"

WALL_TIME="24:00:00"
MAIL_TYPE="END,FAIL"
MAIL_USER="danin.dharmaperwira@unil.ch"

# --- args (prompt if missing; no early exit) ---
if [ $# -lt 2 ]; then
  echo "Usage: $0 PARTITION FISH1 [FISH2 ...]" >&2
  read -r -p "PARTITION (e.g., cpu or test): " PARTITION
  read -r -p "FISH IDs (space-separated): " FISH_LINE
  read -r -a FISH_IDS <<< "$FISH_LINE"
else
  PARTITION="$1"; shift
  FISH_IDS=( "$@" )
fi

echo "Rigid sweep (GCaMP; R1 -> 2P) on partition $PARTITION:"
printf "  %s\n" "${FISH_IDS[@]}"

# resources per fish/job
if [ "$PARTITION" = "test" ]; then
  QUEUE="interactive"; CPUS=1; MEM="8G"; TIME="1:00:00"
  echo "==> TEST mode: interactive (1 CPU, 8G, 1h)"
else
  QUEUE="$PARTITION"; CPUS=8; MEM="32G"; TIME="$WALL_TIME"
fi

# Sweep rigid gradient step
RIGID_STEPS=("0.15:gs015" "0.20:gs020" "0.25:gs025")

for FISH in "${FISH_IDS[@]}"; do
  echo "===== Processing subject $FISH ====="

  BASE="${SCRATCH:-$HOME/SCRATCH}/subjects/$FISH"
  RAW_CONF="$BASE/raw/confocal_round1"
  FIXED="$BASE/fixed"
  REGDIR="$BASE/reg"
  LOGDIR="$REGDIR/logs"
  mkdir -p "$REGDIR" "$LOGDIR"

  # Fixed = 2P anatomy ref
  REF_GCaMP="$FIXED/anatomy_2P_ref_GCaMP.nrrd"

  # Moving = round1 GCaMP, try common names, then unique glob
  if [ -f "$RAW_CONF/${FISH}_round1_channel1_GCaMP.nrrd" ]; then
    MOV_GCaMP="$RAW_CONF/${FISH}_round1_channel1_GCaMP.nrrd"
  elif [ -f "$RAW_CONF/${FISH}_round1_GCaMP.nrrd" ]; then
    MOV_GCaMP="$RAW_CONF/${FISH}_round1_GCaMP.nrrd"
  else
    candidates=( "$RAW_CONF"/*round1*GCaMP*.nrrd )
    if [ ${#candidates[@]} -eq 1 ]; then
      MOV_GCaMP="${candidates[0]}"
    elif [ ${#candidates[@]} -gt 1 ]; then
      echo "ERROR: Multiple possible moving files for $FISH:"
      printf '  - %s\n' "${candidates[@]}"
      echo "       Please standardize or remove extras."
      continue
    else
      echo "ERROR: No round1 GCaMP found for $FISH under $RAW_CONF"
      continue
    fi
  fi

  # sanity checks
  missing=0
  [ -f "$REF_GCaMP" ] || { echo "Missing fixed:  $REF_GCaMP" >&2; missing=1; }
  [ -f "$MOV_GCaMP" ] || { echo "Missing moving: $MOV_GCaMP" >&2; missing=1; }
  [ $missing -eq 0 ] || { echo "Skipping $FISH due to missing inputs."; continue; }

  OUT_PREFIX_BASE="$REGDIR/round1_GCaMP_to_ref_rigid"
  JOBNAME="ants_${FISH}_r1_sweepRigid_GC"
  JOBSCRIPT="$LOGDIR/${JOBNAME}.sh"

  # ---------- per-fish job script ----------
  cat > "$JOBSCRIPT" <<EOF
#!/usr/bin/env bash
set -euo pipefail
echo "Host: \$(hostname)"
echo "Start: \$(date)"
echo "Fixed : ${REF_GCaMP}"
echo "Moving: ${MOV_GCaMP}"

EOF

  for PAIR in "${RIGID_STEPS[@]}"; do
    GS="\${PAIR%%:*}"     # gradient step
    TAG="\${PAIR##*:}"    # gs015, gs020, gs025
    SWEEP_PREFIX="\${OUT_PREFIX_BASE}_\${TAG}"

    cat >> "$JOBSCRIPT" <<EOF
echo ">> Rigid[\${GS}]  (\${TAG})"
"${ANTSBIN}/antsRegistration" \
  -d 3 --float 1 --verbose 1 \
  -o [\${SWEEP_PREFIX},\${SWEEP_PREFIX}_rigidAligned.nrrd] \
  --interpolation WelchWindowedSinc \
  --winsorize-image-intensities [0,100] \
  --use-histogram-matching 1 \
  -t Rigid[\${GS}] \
  -m CC[${REF_GCaMP},${MOV_GCaMP},1,4] \
  -c [1000x500x250,1e-8,10] \
  --shrink-factors 8x4x2 \
  --smoothing-sigmas 3x2x1vox

ln -sf "\${SWEEP_PREFIX}_rigidAligned.nrrd" "\${SWEEP_PREFIX}_aligned.nrrd"
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
