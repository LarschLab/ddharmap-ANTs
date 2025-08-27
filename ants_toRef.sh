#!/usr/bin/env bash
# ants_register_to_avg2p.sh  (interactive; single job for multiple fish)
#
# Goal:
#   For each fish, register the 2P anatomy and ALL rounds to an average 2P reference.
#
# Reference (FIXED):
#   Default: /scratch/ddharmap/refBrains/ref_05_LB_Perrino_2p/average_2p.nrrd
#   Override with: export REF_AVG_2P=/path/to/average_2p.nrrd
#
# SCRATCH layout (assumed):
#   $SCRATCH/experiments/subjects/<FishID>/{raw,fixed,reg}
#
# Moving discovery:
#   - 2P anatomy: fixed/anatomy_2P_ref_GCaMP.nrrd  (required)
#   - Auto-detect all rounds as: raw/confocal_round*/
#   - Per-round GCaMP (most-specific first, then fallbacks):
#       <Fish>_round<R>_channel1_*GCaMP*.nrrd
#       <Fish>_round<R>_*GCaMP*.nrrd
#       round<R>_GCaMP.nrrd
#       round<R>_*channel*GCaMP*.nrrd
#       round<R>_*GCaMP*.nrrd
#   - Per-round non-GCaMP (HCR/others), exclude *GCaMP*:
#       <Fish>_round<R>_channel[2-9]_*.nrrd
#       round<R>_HCR_channel*.nrrd
#       round<R>_channel*.nrrd
#       round<R>_HCR_*.nrrd
#
# Registration:
#   antsRegistration: Rigid + Affine (MI), SyN (CC) with SyN step = 0.25
#   antsApplyTransforms: apply computed transforms to GCaMP + other channels
#
# ENV you likely already have:
#   SCRATCH=/scratch/<user>
#   ANTSPATH=$HOME/ANTs/antsInstallExample/install/bin
# Optional:
#   REF_AVG_2P, MAIL_USER, MAIL_TYPE, CPUS, MEM

set -euo pipefail

ANTSPATH="${ANTSPATH:-$HOME/ANTs/antsInstallExample/install/bin}"
export ANTSPATH
ANTSBIN="$ANTSPATH"

REF_AVG_2P="${REF_AVG_2P:-/scratch/ddharmap/refBrains/ref_05_LB_Perrino_2p/average_2p.nrrd}"

WALL_TIME="24:00:00"
MAIL_TYPE="${MAIL_TYPE:-END,FAIL}"
MAIL_USER="${MAIL_USER:-danin.dharmaperwira@unil.ch}"

# -------- Prompts --------
read -rp "PARTITION (e.g., test | cpu | normal | gpu | other): " PARTITION
read -rp "Fish IDs (space-separated): " FISH_LINE
# shellcheck disable=SC2206
FISH_IDS=( $FISH_LINE )

[[ -n "${SCRATCH:-}" ]] || { echo "ERROR: SCRATCH env not set."; exit 2; }
[[ -f "$REF_AVG_2P" ]] || { echo "ERROR: Average 2P reference not found: $REF_AVG_2P"; exit 2; }

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

# -------- Build ONE job script that loops over all fish --------
JOBDIR="$SCRATCH/experiments/_jobs"
mkdir -p "$JOBDIR"
STAMP="$(date +%Y%m%d_%H%M%S)"
JOB="$JOBDIR/ants_to_avg2p_${STAMP}.sh"

# Serialize fish list with literal newlines
FISH_SERIALIZED=""
for f in "${FISH_IDS[@]}"; do
  [[ -n "$f" ]] || continue
  FISH_SERIALIZED+="$f"$'\n'
done

# Make a shell-escaped $'...' string so newlines survive in the job
FISH_ESCAPED=$'('"$(printf "%q" "$FISH_SERIALIZED")"')'   # yields $'L331_f01\nL395_f06\n...'

cat > "$JOB" <<EOF
#!/usr/bin/env bash
set -euo pipefail

ANTSPATH="\${ANTSPATH:-\$HOME/ANTs/antsInstallExample/install/bin}"
export ANTSPATH
export ITK_GLOBAL_DEFAULT_NUMBER_OF_THREADS="\${SLURM_CPUS_PER_TASK:-1}"
export OMP_NUM_THREADS="\${SLURM_CPUS_PER_TASK:-1}"

SCRATCH_BASE="\${SCRATCH:?SCRATCH env not set}"
REF_AVG_2P="$REF_AVG_2P"
FISH_LIST=$FISH_ESCAPED

echo "ANTs bin : \$ANTSPATH"
echo "Threads  : \${SLURM_CPUS_PER_TASK:-1}"
echo "FIXED    : \$REF_AVG_2P"

# --- helpers ---
$(declare -f pick_moving_gcamp)
$(declare -f gather_hcr_channels)
$(declare -f register_pair)
$(declare -f apply_to_image)

# ---- main loop over fish ----
while IFS= read -r FISH; do
  [[ -n "\$FISH" ]] || continue
  echo "===== Processing \$FISH ====="

  BASE="\$SCRATCH_BASE/experiments/subjects/\$FISH"
  FIXEDDIR="\$BASE/fixed"
  REGDIR="\$BASE/reg_to_avg2p"
  LOGDIR="\$REGDIR/logs"
  mkdir -p "\$REGDIR" "\$LOGDIR"

  MOV_2P="\$FIXEDDIR/anatomy_2P_ref_GCaMP.nrrd"
  if [[ ! -f "\$MOV_2P" ]]; then
    echo "ERROR: Missing 2P anatomy (moving): \$MOV_2P"
    ls -1 "\$FIXEDDIR" 2>/dev/null | sed 's/^/  - /' || true
    echo "Skipping \$FISH."
    continue
  fi
  echo "  FIXED (avg 2P): \$REF_AVG_2P"
  echo "  MOVING (2P)   : \$MOV_2P"

  OP_2P="\$REGDIR/2P_to_avg2p_"
  register_pair "\$REF_AVG_2P" "\$MOV_2P" "\$OP_2P"

  RAW_BASE="\$BASE/raw"
  shopt -s nullglob
  ROUND_DIRS=( "\$RAW_BASE"/confocal_round* )
  shopt -u nullglob
  if (( \${#ROUND_DIRS[@]} == 0 )); then
    echo "  (no confocal_round* directories found under \$RAW_BASE)"
  fi

  for RDIR in "\${ROUND_DIRS[@]}"; do
    RNAME="\$(basename "\$RDIR")"
    if [[ "\$RNAME" =~ ^confocal_round([0-9]+)\$ ]]; then
      R="\${BASH_REMATCH[1]}"
    else
      echo "  Skipping nonstandard round dir: \$RDIR"
      continue
    fi
    echo "---- Round \$R ----"

    if ! MOV_GC="\$(pick_moving_gcamp "\$RDIR" "\$R")"; then
      echo "  ERROR: Could not find moving GCaMP in \$RDIR"
      ls -1 "\$RDIR" 2>/dev/null | sed 's/^/    - /' || true
      continue
    fi
    echo "  MOVING (GCaMP): \$MOV_GC"
    OP_R="\$REGDIR/round\${R}_GCaMP_to_avg2p_"
    register_pair "\$REF_AVG_2P" "\$MOV_GC" "\$OP_R"

    mapfile -t CHANNELS < <(gather_hcr_channels "\$RDIR" "\$R")
    if (( \${#CHANNELS[@]} == 0 )); then
      echo "  (no non-GCaMP channels found in \$RDIR)"
    fi
    for MOV in "\${CHANNELS[@]}"; do
      base="\$(basename "\$MOV")"
      out="\$REGDIR/\${base%.*}_in_avg2p.nrrd"
      echo "  Transforming: \$base -> \$(basename "\$out")"
      apply_to_image "\$REF_AVG_2P" "\$OP_R" "\$MOV" "\$out"
    done
  done

  if [[ -f "\${OP_2P}_aligned.nrrd" ]]; then
    cp -f "\${OP_2P}_aligned.nrrd" "\$REGDIR/anatomy_2P_in_avg2p.nrrd"
  fi

  echo "===== Done \$FISH ====="
done <<< "\$FISH_LIST"
EOF

chmod +x "$JOB"


# -------- Submit or run --------
if [[ "$QUEUE" == "interactive" ]]; then
  echo "Running interactively..."
  bash "$JOB"
else
  sbatch \
    --mail-type="$MAIL_TYPE" \
    --mail-user="$MAIL_USER" \
    -p "$QUEUE" \
    -N 1 -n 1 -c "$CPUS" --mem="$MEM" \
    -t "$TIME" \
    -J "ants_to_avg2p" \
    -o "$JOBDIR/ants_to_avg2p_${STAMP}.out" \
    -e "$JOBDIR/ants_to_avg2p_${STAMP}.err" \
    "$JOB"
  echo "Submitted single job for fish: ${FISH_IDS[*]}"
  echo "  Job script: $JOB"
  echo "  Logs: $JOBDIR/ants_to_avg2p_${STAMP}.{out,err}"
fi
