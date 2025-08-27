#!/usr/bin/env bash
# ants_register.sh  (interactive)
#
# Uses compute-side layout:
#   $SCRATCH/experiments/subjects/<FishID>/{raw,fixed,reg}
#
# Prompts for: ROUND, PARTITION, FISH IDs (space-separated).
# ROUND=1  -> fixed/anatomy_2P_ref_GCaMP.nrrd
# ROUND>1  -> fixed/round1_ref_GCaMP.nrrd
#
# Moving GCaMP auto-detect in raw/confocal_round<ROUND>/:
#   prefer: round<ROUND}_GCaMP.nrrd
#   else:   *channel*GCaMP*.nrrd
#   else:   *GCaMP*.nrrd
#
# All HCR channels in that folder are transformed:
#   round<ROUND>_HCR_channel*.nrrd, round<ROUND>_channel*.nrrd, round<ROUND>_HCR_*.nrrd
#
# ENV you likely already have:
#   SCRATCH=/scratch/<user>
#   ANTSPATH=$HOME/ANTs/antsInstallExample/install/bin
# Optional:
#   MAIL_USER=<your@email>   MAIL_TYPE="END,FAIL"
#   CPUS=48  MEM=256G        (cluster defaults when not in test mode)

set -euo pipefail

# -------- ANTs path --------
ANTSPATH="${ANTSPATH:-$HOME/ANTs/antsInstallExample/install/bin}"
export ANTSPATH
ANTSBIN="$ANTSPATH"

# -------- Defaults --------
WALL_TIME="24:00:00"
MAIL_TYPE="${MAIL_TYPE:-END,FAIL}"
MAIL_USER="${MAIL_USER:-$(whoami)@example.com}"

# -------- Prompts --------
read -rp "ROUND (e.g., 1 or 2): " ROUND
read -rp "PARTITION (e.g., test | normal | gpu | other): " PARTITION
read -rp "Fish IDs (space-separated): " FISH_LINE
# shellcheck disable=SC2206
FISH_IDS=( $FISH_LINE )

if [[ -z "${SCRATCH:-}" ]]; then
  echo "ERROR: SCRATCH env not set."; exit 2
fi

# SLURM resources
if [[ "$PARTITION" == "test" ]]; then
  QUEUE="interactive"; CPUS=1; MEM="8G"; TIME="00:30:00"
  echo "==> TEST mode: interactive (1 CPU, 8G, 30m)"
else
  QUEUE="$PARTITION"
  CPUS="${CPUS:-48}"
  MEM="${MEM:-256G}"
  TIME="$WALL_TIME"
fi

# -------- Per-fish jobs --------
for FISH in "${FISH_IDS[@]}"; do
  [[ -n "$FISH" ]] || continue
  echo "===== Processing subject $FISH (round $ROUND, partition $PARTITION) ====="

  BASE="$SCRATCH/experiments/subjects/$FISH"
  RAW_ANAT="$BASE/raw/anatomy_2P"
  RAW_CONF="$BASE/raw/confocal_round${ROUND}"
  FIXED="$BASE/fixed"
  REGDIR="$BASE/reg"
  LOGDIR="$REGDIR/logs"

  mkdir -p "$REGDIR" "$LOGDIR"

  # Choose fixed reference
  if [[ "$ROUND" -eq 1 ]]; then
    REF_GC="$FIXED/anatomy_2P_ref_GCaMP.nrrd"
  else
    REF_GC="$FIXED/round1_ref_GCaMP.nrrd"
  fi
  if [[ ! -f "$REF_GC" ]]; then
    echo "ERROR: Missing fixed reference: $REF_GC"; continue
  fi

  # Pick moving GCaMP robustly
  pick_moving_gcamp() {
    local d="$1" r="$2"
    if [[ -f "$d/round${r}_GCaMP.nrrd" ]]; then
      printf "%s" "$d/round${r}_GCaMP.nrrd"; return 0
    fi
    shopt -s nullglob
    local cands=( "$d"/round${r}_*channel*GCaMP*.nrrd "$d"/round${r}_*GCaMP*.nrrd )
    shopt -u nullglob
    if (( ${#cands[@]} )); then
      printf "%s" "${cands[0]}"; return 0
    fi
    return 1
  }
  if ! MOV_GC="$(pick_moving_gcamp "$RAW_CONF" "$ROUND")"; then
    echo "ERROR: Could not find moving GCaMP in $RAW_CONF"; continue
  fi

  OUT_PREFIX="$REGDIR/round${ROUND}_GCaMP_to_ref"

  # Write per-fish job script (space-safe)
  JOB="$REGDIR/job_r${ROUND}.sh"
  cat > "$JOB" <<EOF
#!/usr/bin/env bash
set -euo pipefail
export ANTSPATH="$ANTSBIN"
export ITK_GLOBAL_DEFAULT_NUMBER_OF_THREADS="\${SLURM_CPUS_PER_TASK:-1}"
export OMP_NUM_THREADS="\${SLURM_CPUS_PER_TASK:-1}"

echo "ANTs bin: \$ANTSPATH"
echo "Threads : \${SLURM_CPUS_PER_TASK:-1}"

REF_GC="$REF_GC"
MOV_GC="$MOV_GC"
OUT_PREFIX="$OUT_PREFIX"
RAW_CONF="$RAW_CONF"
REGDIR="$REGDIR"
ROUND="$ROUND"

# 1) Register GCaMP (moving) to fixed reference and write aligned GCaMP
"\$ANTSPATH/antsRegistration" \\
  -d 3 --float 1 --verbose 1 \\
  -o ["\$OUT_PREFIX","\${OUT_PREFIX}_aligned.nrrd"] \\
  --interpolation WelchWindowedSinc \\
  --winsorize-image-intensities [0,100] \\
  --use-histogram-matching 1 \\
  -r ["\$REF_GC","\$MOV_GC",1] \\
  -t rigid[0.1] \\
  -m MI["\$REF_GC","\$MOV_GC",1,32,Regular,0.25] \\
  -c [200x200x200x200,1e-6,10] \\
  --shrink-factors 12x8x4x2 \\
  --smoothing-sigmas 4x3x2x1vox \\
  -t Affine[0.1] \\
  -m MI["\$REF_GC","\$MOV_GC",1,32,Regular,0.25] \\
  -c [200x200x200x200,1e-6,10] \\
  --shrink-factors 12x8x4x2 \\
  --smoothing-sigmas 4x3x2x1vox \\
  -t SyN[0.1,6,0.1] \\
  -m CC["\$REF_GC","\$MOV_GC",1,4] \\
  -c [200x200x200x200x10,1e-8,10] \\
  --shrink-factors 12x8x4x2x1 \\
  --smoothing-sigmas 4x3x2x1x0vox

"\$ANTSPATH/antsApplyTransforms" \\
  -d 3 --verbose 1 \\
  -r "\$REF_GC" \\
  -i "\$MOV_GC" \\
  -o "\${OUT_PREFIX}_aligned.nrrd" \\
  -t "\${OUT_PREFIX}1Warp.nii.gz" \\
  -t "\${OUT_PREFIX}0GenericAffine.mat"

# 2) Apply transforms to all HCR channels for this round
shopt -s nullglob
channels=( "\$RAW_CONF"/round\${ROUND}_HCR_channel*.nrrd "\$RAW_CONF"/round\${ROUND}_channel*.nrrd "\$RAW_CONF"/round\${ROUND}_HCR_*.nrrd )
shopt -u nullglob

for MOV in "\${channels[@]}"; do
  [ -f "\$MOV" ] || continue
  base=\$(basename "\$MOV")
  out="\$REGDIR/\${base%.*}_aligned.nrrd"
  echo "Transforming: \$base -> \$(basename "\$out")"
  "\$ANTSPATH/antsApplyTransforms" \\
    -d 3 --verbose 1 \\
    -r "\$REF_GC" \\
    -i "\$MOV" \\
    -o "\$out" \\
    -t "\${OUT_PREFIX}1Warp.nii.gz" \\
    -t "\${OUT_PREFIX}0GenericAffine.mat"
done

echo "Done."
EOF
  chmod +x "$JOB"

  # Submit or run
  if [[ "$QUEUE" == "interactive" ]]; then
    echo "Running interactively for $FISH ..."
    bash "$JOB"
  else
    sbatch \
      --mail-type="$MAIL_TYPE" \
      --mail-user="$MAIL_USER" \
      -p "$QUEUE" \
      -N 1 -n 1 -c "$CPUS" --mem="$MEM" \
      -t "$TIME" \
      -J "ants_${FISH}_r${ROUND}" \
      -o "$LOGDIR/ants_${FISH}_r${ROUND}.out" \
      -e "$LOGDIR/ants_${FISH}_r${ROUND}.err" \
      "$JOB"
  fi

done
