#!/usr/bin/env bash
# ants_rigidSweep.sh
# usage: bash ants_rigidSweep.sh PARTITION FISH1 [FISH2 ...]
#
# Purpose
#   Rigid-only parameter sweep (GCaMP), mapping Round 1 (confocal) -> 2P anatomy.
#   Sweeps Rigid[gradStep] over: 0.15, 0.20, 0.25
#
# Assumed layout (SCRATCH may vary; script will detect with/without "experiments/"):
#   $SCRATCH/experiments/subjects/<FISH>/fixed/anatomy_2P_ref_GCaMP.nrrd
#   $SCRATCH/experiments/subjects/<FISH>/raw/confocal_round1/<FISH>_round1_channel1_GCaMP.nrrd
#
# Output (per fish)
#   $BASE/reg/round1_GCaMP_to_ref_rigid_<gsXXX>_rigidAligned.nrrd
#   + symlink: ..._aligned.nrrd
#   + transform: ..._0GenericAffine.mat (ANTs standard)

set -euo pipefail

ANTSPATH="${ANTSPATH:-$HOME/ANTs/antsInstallExample/install/bin}"
export ANTSPATH
ANTSBIN="$ANTSPATH"

WALL_TIME="24:00:00"
MAIL_TYPE="END,FAIL"
MAIL_USER="danin.dharmaperwira@unil.ch"

# -------------------- args --------------------
if [ $# -lt 2 ]; then
  echo "Usage: $0 PARTITION FISH1 [FISH2 ...]" >&2
  read -r -p "PARTITION (e.g., cpu or test): " PARTITION
  read -r -p "FISH IDs (space-separated): " FISH_LINE
  read -r -a FISH_IDS <<< "$FISH_LINE"
else
  PARTITION="$1"; shift
  FISH_IDS=( "$@" )
fi

echo "Rigid sweep (GCaMP; R1 -> 2P) on partition: $PARTITION"
printf "  Subjects: %s\n" "${FISH_IDS[@]}"
echo

# -------------------- resources --------------------
if [ "$PARTITION" = "test" ]; then
  QUEUE="interactive"; CPUS=1; MEM="8G"; TIME="1:00:00"
  echo "==> TEST mode: interactive (1 CPU, 8G, 1h)"
else
  QUEUE="$PARTITION"; CPUS=48; MEM="256G"; TIME="$WALL_TIME"
fi

# -------------------- sweep --------------------
RIGID_STEPS=("0.15:gs015" "0.20:gs020" "0.25:gs025")

# -------------------- per fish --------------------
for FISH in "${FISH_IDS[@]}"; do
  echo "===== Processing $FISH ====="

  # Resolve subject base (support both with/without 'experiments/')
  SCR="${SCRATCH:-$HOME/SCRATCH}"
  if   [ -d "$SCR/experiments/subjects/$FISH" ]; then
    BASE="$SCR/experiments/subjects/$FISH"
  elif [ -d "$SCR/subjects/$FISH" ]; then
    BASE="$SCR/subjects/$FISH"
  else
    echo "ERROR: Could not find subject folder for $FISH
  Tried:
    $SCR/experiments/subjects/$FISH
    $SCR/subjects/$FISH" >&2
    continue
  fi
  echo "Using subject base: $BASE"

  RAW_CONF="$BASE/raw/confocal_round1"
  FIXED="$BASE/fixed"
  REGDIR="$BASE/reg"
  LOGDIR="$REGDIR/logs"
  mkdir -p "$REGDIR" "$LOGDIR"

  # Fixed (2P anatomy) and Moving (R1 GCaMP — gold-standard name)
  REF_GCaMP="$FIXED/anatomy_2P_ref_GCaMP.nrrd"
  MOV_GCaMP="$RAW_CONF/${FISH}_round1_channel1_GCaMP.nrrd"

  missing=0
  [ -f "$REF_GCaMP" ] || { echo "Missing fixed:  $REF_GCaMP" >&2; missing=1; }
  if [ ! -f "$MOV_GCaMP" ]; then
    echo "ERROR: Missing moving (must follow gold-standard name): $MOV_GCaMP" >&2
    echo "       Rename your file to: ${FISH}_round1_channel1_GCaMP.nrrd"
    missing=1
  fi
  [ $missing -eq 0 ] || { echo "Skipping $FISH due to missing inputs."; echo; continue; }

  OUT_PREFIX_BASE="$REGDIR/round1_GCaMP_to_ref_rigid"
  JOBNAME="ants_${FISH}_r1_sweepRigid_GC"
  JOBSCRIPT="$LOGDIR/${JOBNAME}.sh"

  # ---------- write per-fish job script ----------
  cat > "$JOBSCRIPT" <<EOF
#!/usr/bin/env bash
set -euo pipefail
echo "Host: \$(hostname)"
echo "Start: \$(date)"
echo "Fixed : ${REF_GCaMP}"
echo "Moving: ${MOV_GCaMP}"
EOF

  for PAIR in "${RIGID_STEPS[@]}"; do
    GS="${PAIR%%:*}"       # 0.15 / 0.20 / 0.25
    TAG="${PAIR##*:}"      # gs015 / gs020 / gs025
    SWEEP_PREFIX="${OUT_PREFIX_BASE}_${TAG}"

    cat >> "$JOBSCRIPT" <<EOF
echo ">> antsRegistration Rigid[${GS}]  (${TAG})"
"${ANTSBIN}/antsRegistration" \
  -d 3 --float 1 --verbose 1 \
  -o [${SWEEP_PREFIX},${SWEEP_PREFIX}_rigidAligned.nrrd] \
  --interpolation WelchWindowedSinc \
  --winsorize-image-intensities [0,100] \
  --use-histogram-matching 1 \
  -t Rigid[${GS}] \
  -m CC[${REF_GCaMP},${MOV_GCaMP},1,4] \
  -c [1000x500x250,1e-8,10] \
  --shrink-factors 8x4x2 \
  --smoothing-sigmas 3x2x1vox

ln -sf "${SWEEP_PREFIX}_rigidAligned.nrrd" "${SWEEP_PREFIX}_aligned.nrrd"
EOF
  done

  cat >> "$JOBSCRIPT" <<'EOF'
echo "End: $(date)"
EOF

  chmod +x "$JOBSCRIPT"

  # ---------- submit or run ----------
  if [ "$QUEUE" = "interactive" ]; then
    bash "$JOBSCRIPT" \
      1> "$LOGDIR/${JOBNAME}.out" \
      2> "$LOGDIR/${JOBNAME}.err" || {
        echo "❌ Failed $JOBNAME (interactive). See logs in: $LOGDIR"
        echo
        continue
      }
    echo "✅ Finished $JOBNAME (interactive)."
  else
    sbatch \
      --mail-type="$MAIL_TYPE" --mail-user="$MAIL_USER" \
      -p "$QUEUE" -N 1 -n 1 -c "$CPUS" --mem="$MEM" \
      -t "$TIME" \
      -J "$JOBNAME" \
      --output="$LOGDIR/%x.%j.out" \
      --error="$LOGDIR/%x.%j.err" \
      "$JOBSCRIPT"
    echo "Submitted $JOBNAME. Logs will land in: $LOGDIR/%x.%j.{out,err}"
  fi

  echo
done
