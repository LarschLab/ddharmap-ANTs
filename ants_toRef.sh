#!/usr/bin/env bash
# ants_register_to_avg2p.sh  (interactive; single job for multiple fish)
#
# Goal:
#   For each fish, register the 2P anatomy AND all rounds to an average 2P reference.
#
# Reference (FIXED):
#   Default: /scratch/ddharmap/refBrains/ref_05_LB_Perrino_2p/average_2p.nrrd
#   Override with: export REF_AVG_2P=/path/to/average_2p.nrrd
#
# Expected layout:
#   $SCRATCH/experiments/subjects/<FishID>/{raw,fixed,reg_to_avg2p}
#   Rounds are auto-detected under: raw/confocal_round*/
#
# Registration (antsRegistration): Rigid + Affine (MI), SyN (CC) with SyN step = 0.25
# Transforms are then applied to: per-round GCaMP + non-GCaMP (HCR/other) channels
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

# Default average 2P reference (can be overridden via env)
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

cat > "$JOB" <<EOF
#!/usr/bin/env bash
set -euo pipefail

ANTSPATH="\${ANTSPATH:-\$HOME/ANTs/antsInstallExample/install/bin}"
export ANTSPATH
export ITK_GLOBAL_DEFAULT_NUMBER_OF_THREADS="\${SLURM_CPUS_PER_TASK:-1}"
export OMP_NUM_THREADS="\${SLURM_CPUS_PER_TASK:-1}"

SCRATCH_BASE="\${SCRATCH:?SCRATCH env not set}"
REF_AVG_2P="$REF_AVG_2P"

echo "ANTs bin : \$ANTSPATH"
echo "Threads  : \${SLURM_CPUS_PER_TASK:-1}"
echo "FIXED    : \$REF_AVG_2P"

# --- helpers (embedded) ---
# 1) Find the round's GCaMP (lenient fallback at end)
pick_moving_gcamp() {
  local d="\$1" r="\$2"
  shopt -s nullglob
  local cands=(
    "\$d"/*_round\${r}_channel1_*GCaMP*.nrrd
    "\$d"/*_round\${r}_*GCaMP*.nrrd
    "\$d"/round\${r}_GCaMP.nrrd
    "\$d"/round\${r}_*channel*GCaMP*.nrrd
    "\$d"/round\${r}_*GCaMP*.nrrd
    # lenient: any GCaMP in the round dir (for misnamed round index)
    "\$d"/*GCaMP*.nrrd
  )
  shopt -u nullglob
  local f
  for f in "\${cands[@]}"; do
    [[ -f "\$f" ]] && { printf "%s" "\$f"; return 0; }
  done
  return 1
}

# 2) Collect non-GCaMP channels (HCR/others), robust patterns
gather_hcr_channels() {
  local d="\$1" r="\$2"
  local out=()
  shopt -s nullglob
  # New-format (allow channel1..9 and HCR prefix)
  out+=( "\$d"/*_round\${r}_channel[1-9]_*.nrrd )
  out+=( "\$d"/*_round\${r}_HCR_channel*.nrrd )
  # Old-format fallbacks
  out+=( "\$d"/round\${r}_HCR_channel*.nrrd "\$d"/round\${r}_channel*.nrrd "\$d"/round\${r}_HCR_*.nrrd )
  shopt -u nullglob
  # Exclude any GCaMP
  local keep=()
  local f
  for f in "\${out[@]}"; do
    [[ -f "\$f" ]] || continue
    [[ "\$f" == *GCaMP* ]] && continue
    keep+=( "\$f" )
  done
  printf "%s\n" "\${keep[@]}"
}

# 3) Register a pair and tee output to a log (returns nonzero on failure)
register_pair() {
  local fx="\$1" mv="\$2" op="\$3" log="\${4:-/dev/null}"
  {
    echo "antsRegistration -> \$op"
    "\$ANTSPATH/antsRegistration" \
      -d 3 --float 1 --verbose 1 \
      -o ["\$op","\${op}_aligned.nrrd"] \
      --interpolation WelchWindowedSinc \
      --winsorize-image-intensities [0,100] \
      --use-histogram-matching 1 \
      -r ["\$fx","\$mv",1] \
      -t Rigid[0.1] \
        -m MI["\$fx","\$mv",1,32,Regular,0.25] \
        -c [200x200x100,1e-8,10] \
        --shrink-factors 8x4x2 \
        --smoothing-sigmas 3x2x1vox \
      -t Affine[0.1] \
        -m MI["\$fx","\$mv",1,32,Regular,0.25] \
        -c [200x200x100,1e-8,10] \
        --shrink-factors 8x4x2 \
        --smoothing-sigmas 3x2x1vox \
      -t SyN[0.25,6,0.1] \
        -m CC["\$fx","\$mv",1,4] \
        -c [200x200x100x20,1e-8,10] \
        --shrink-factors 8x4x2x1 \
        --smoothing-sigmas 3x2x1x0vox
  } >"\$log" 2>&1
}

# 4) Apply transforms to an image
apply_to_image() {
  local ref="\$1" op="\$2" mv="\$3" out="\$4"
  "\$ANTSPATH/antsApplyTransforms" \
    -d 3 --verbose 1 \
    -r "\$ref" \
    -i "\$mv" \
    -o "\$out" \
    -t "\${op}1Warp.nii.gz" \
    -t "\${op}0GenericAffine.mat"
}

# ---- main loop over fish ----
while IFS= read -r FISH; do
  [[ -n "\$FISH" ]] || continue
  echo "===== Processing \$FISH ====="

  BASE="\$SCRATCH_BASE/experiments/subjects/\$FISH"
  FIXEDDIR="\$BASE/fixed"
  REGDIR="\$BASE/reg_to_avg2p"
  LOGDIR="\$REGDIR/logs"
  mkdir -p "\$REGDIR" "\$LOGDIR"

  # 1) 2P anatomy -> average 2P
  MOV_2P="\$FIXEDDIR/anatomy_2P_ref_GCaMP.nrrd"
  if [[ ! -f "\$MOV_2P" ]]; then
    echo "ERROR: Missing 2P anatomy (moving): \$MOV_2P" | tee -a "\$LOGDIR/errors.log"
    ls -1 "\$FIXEDDIR" 2>/dev/null | sed 's/^/  - /' || true
    echo "Skipping \$FISH."
    continue
  fi
  echo "  FIXED (avg 2P): \$REF_AVG_2P"
  echo "  MOVING (2P)   : \$MOV_2P"

  OP_2P="\$REGDIR/2P_to_avg2p_"
  if ! register_pair "\$REF_AVG_2P" "\$MOV_2P" "\$OP_2P" "\$LOGDIR/2P_to_avg2p.log"; then
    echo "ERROR: 2P->avg registration failed for \$FISH (see \$LOGDIR/2P_to_avg2p.log). Skipping fish." | tee -a "\$LOGDIR/errors.log"
    continue
  fi

  # 2) All rounds -> average 2P
  RAW_BASE="\$BASE/raw"
  shopt -s nullglob
  ROUND_DIRS=( "\$RAW_BASE"/confocal_round* )
  shopt -u nullglob
  if (( \${#ROUND_DIRS[@]} == 0 )); then
    echo "  (no confocal_round* directories found under \$RAW_BASE)"
  fi

  for RDIR in "\${ROUND_DIRS[@]}"; do
    RNAME="\$(basename "\$RDIR")"
    if [[ "\$RNAME" =~ ^confocal_round([0-9]+)$ ]]; then
      R="\${BASH_REMATCH[1]}"
    else
      echo "  Skipping nonstandard round dir: \$RDIR"
      continue
    fi
    echo "---- Round \$R ----"

    if ! MOV_GC="\$(pick_moving_gcamp "\$RDIR" "\$R")"; then
      echo "  ERROR: Could not find moving GCaMP in \$RDIR" | tee -a "\$LOGDIR/errors.log"
      ls -1 "\$RDIR" 2>/dev/null | sed 's/^/    - /' || true
      continue
    fi
    echo "  MOVING (GCaMP): \$MOV_GC"
    OP_R="\$REGDIR/round\${R}_GCaMP_to_avg2p_"
    if ! register_pair "\$REF_AVG_2P" "\$MOV_GC" "\$OP_R" "\$LOGDIR/round\${R}_GCaMP_to_avg2p.log"; then
      echo "  ERROR: round \$R registration failed (see log). Skipping round." | tee -a "\$LOGDIR/errors.log"
      continue
    fi

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

  # 3) Canonical name for 2P in avg space
  if [[ -f "\${OP_2P}_aligned.nrrd" ]]; then
    cp -f "\${OP_2P}_aligned.nrrd" "\$REGDIR/anatomy_2P_in_avg2p.nrrd"
  fi

  echo "===== Done \$FISH ====="
# The fish list will be appended below via a literal heredoc
done <<'FISH_EOF'
EOF

# Append fish IDs as the heredoc payload (avoids sed/escaping issues)
{
  for f in "${FISH_IDS[@]}"; do
    [[ -n "$f" ]] && printf '%s\n' "$f"
  done
  echo "FISH_EOF"
} >> "$JOB"

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
