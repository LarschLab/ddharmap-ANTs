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

# Serialize fish list safely
FISH_SERIALIZED=""
for f in "${FISH_IDS[@]}"; do
  [[ -n "$f" ]] || continue
  FISH_SERIALIZED+="$f"$'\n'
done

cat > "$JOB" <<'EOS'
#!/usr/bin/env bash
set -euo pipefail

# ---- Runtime env (in-job) ----
ANTSPATH="${ANTSPATH:-$HOME/ANTs/antsInstallExample/install/bin}"
export ANTSPATH
export ITK_GLOBAL_DEFAULT_NUMBER_OF_THREADS="${SLURM_CPUS_PER_TASK:-1}"
export OMP_NUM_THREADS="${SLURM_CPUS_PER_TASK:-1}"

SCRATCH_BASE="${SCRATCH:?SCRATCH env not set}"
REF_AVG_2P="__REF_AVG_2P__"
FISH_LIST=$'__FISH_LIST__'

echo "ANTs bin : $ANTSPATH"
echo "Threads  : ${SLURM_CPUS_PER_TASK:-1}"
echo "FIXED    : $REF_AVG_2P"

# --- helpers ---
pick_moving_gcamp() {
  # args: dir round
  local d="$1" r="$2"
  shopt -s nullglob
  local cands=(
    "$d"/*_round${r}_channel1_*GCaMP*.nrrd
    "$d"/*_round${r}_*GCaMP*.nrrd
    "$d"/round${r}_GCaMP.nrrd
    "$d"/round${r}_*channel*GCaMP*.nrrd
    "$d"/round${r}_*GCaMP*.nrrd
  )
  shopt -u nullglob
  local f
  for f in "${cands[@]}"; do
    [[ -f "$f" ]] && { printf "%s" "$f"; return 0; }
  done
  return 1
}

gather_hcr_channels() {
  # args: dir round
  local d="$1" r="$2"
  local out=()
  shopt -s nullglob
  local nfmt=( "$d"/*_round${r}_channel[2-9]_*.nrrd )
  out+=( "${nfmt[@]}" )
  local ofmt=( "$d"/round${r}_HCR_channel*.nrrd "$d"/round${r}_channel*.nrrd "$d"/round${r}_HCR_*.nrrd )
  local f
  for f in "${ofmt[@]}"; do
    [[ -f "$f" ]] || continue
    [[ "$f" == *GCaMP* ]] && continue
    out+=( "$f" )
  done
  shopt -u nullglob
  printf "%s\n" "${out[@]}"
}

register_pair() {
  # args: fixed moving out_prefix
  local fx="$1" mv="$2" op="$3"
  echo "  antsRegistration -> $op"
  "$ANTSPATH/antsRegistration" \
    -d 3 --float 1 --verbose 1 \
    -o ["$op","${op}_aligned.nrrd"] \
    --interpolation WelchWindowedSinc \
    --winsorize-image-intensities [0,100] \
    --use-histogram-matching 1 \
    -r ["$fx","$mv",1] \
    -t Rigid[0.1] \
      -m MI["$fx","$mv",1,32,Regular,0.25] \
      -c [200x200x100,1e-6,10] \
      --shrink-factors 8x4x2 \
      --smoothing-sigmas 3x2x1vox \
    -t Affine[0.1] \
      -m MI["$fx","$mv",1,32,Regular,0.25] \
      -c [200x200x100,1e-6,10] \
      --shrink-factors 8x4x2 \
      --smoothing-sigmas 3x2x1vox \
    -t SyN[0.25,6,0.1] \
      -m CC["$fx","$mv",1,4] \
      -c [200x200x100x20,1e-8,10] \
      --shrink-factors 8x4x2x1 \
      --smoothing-sigmas 3x2x1x0vox
}

apply_to_image() {
  # args: ref fixed outprefix moving outpath
  local ref="$1" op="$2" mv="$3" out="$4"
  "$ANTSPATH/antsApplyTransforms" \
    -d 3 --verbose 1 \
    -r "$ref" \
    -i "$mv" \
    -o "$out" \
    -t "${op}1Warp.nii.gz" \
    -t "${op}0GenericAffine.mat"
}

# ---- main loop over fish ----
while IFS= read -r FISH; do
  [[ -n "$FISH" ]] || continue
  echo "===== Processing $FISH ====="

  BASE="$SCRATCH_BASE/experiments/subjects/$FISH"
  FIXEDDIR="$BASE/fixed"
  REGDIR="$BASE/reg_to_avg2p"
  LOGDIR="$REGDIR/logs"
  mkdir -p "$REGDIR" "$LOGDIR"

  # 1) Register the 2P anatomy reference to the average 2P
  MOV_2P="$FIXEDDIR/anatomy_2P_ref_GCaMP.nrrd"
  if [[ ! -f "$MOV_2P" ]]; then
    echo "ERROR: Missing 2P anatomy (moving): $MOV_2P"
    echo "Present in $FIXEDDIR:"
    ls -1 "$FIXEDDIR" 2>/dev/null | sed 's/^/  - /' || true
    echo "Skipping $FISH."
    continue
  fi
  echo "  FIXED (avg 2P): $REF_AVG_2P"
  echo "  MOVING (2P)   : $MOV_2P"

  OP_2P="$REGDIR/2P_to_avg2p_"
  register_pair "$REF_AVG_2P" "$MOV_2P" "$OP_2P"

  # 2) Detect all rounds under raw/
  RAW_BASE="$BASE/raw"
  shopt -s nullglob
  ROUND_DIRS=( "$RAW_BASE"/confocal_round* )
  shopt -u nullglob
  if (( ${#ROUND_DIRS[@]} == 0 )); then
    echo "  (no confocal_round* directories found under $RAW_BASE)"
  fi

  for RDIR in "${ROUND_DIRS[@]}"; do
    RNAME="$(basename "$RDIR")"            # e.g., confocal_round1
    if [[ "$RNAME" =~ ^confocal_round([0-9]+)$ ]]; then
      R="${BASH_REMATCH[1]}"
    else
      echo "  Skipping nonstandard round dir: $RDIR"
      continue
    fi
    echo "---- Round $R ----"

    # 2a) Register the round's GCaMP directly to average 2P
    if ! MOV_GC="$(pick_moving_gcamp "$RDIR" "$R")"; then
      echo "  ERROR: Could not find moving GCaMP in $RDIR"
      echo "  Contents:"
      ls -1 "$RDIR" 2>/dev/null | sed 's/^/    - /' || true
      continue
    fi
    echo "  MOVING (GCaMP): $MOV_GC"
    OP_R="$REGDIR/round${R}_GCaMP_to_avg2p_"
    register_pair "$REF_AVG_2P" "$MOV_GC" "$OP_R"

    # 2b) Apply the round's transforms to non-GCaMP channels
    mapfile -t CHANNELS < <(gather_hcr_channels "$RDIR" "$R")
    if (( ${#CHANNELS[@]} == 0 )); then
      echo "  (no non-GCaMP channels found in $RDIR)"
    fi
    for MOV in "${CHANNELS[@]}"; do
      base="$(basename "$MOV")"
      out="$REGDIR/${base%.*}_in_avg2p.nrrd"
      echo "  Transforming: $base -> $(basename "$out")"
      apply_to_image "$REF_AVG_2P" "$OP_R" "$MOV" "$out"
    done
  done

  # 3) Also save the 2P anatomy aligned (already output by antsRegistration), but ensure a canonical name:
  if [[ -f "${OP_2P}_aligned.nrrd" ]]; then
    cp -f "${OP_2P}_aligned.nrrd" "$REGDIR/anatomy_2P_in_avg2p.nrrd"
  fi

  echo "===== Done $FISH ====="
done
EOS

# inject variables (REF_AVG_2P + fish list) safely
# shellcheck disable=SC2001
REF_ESCAPED="$(printf "%s" "$REF_AVG_2P" | sed 's/[&/\]/\\&/g')"
sed -i "s|__REF_AVG_2P__|$REF_ESCAPED|g" "$JOB"

# Escape backslashes and slashes in the fish list
FISH_ESCAPED="$(printf "%s" "$FISH_SERIALIZED" | sed -e 's/[&/\]/\\&/g')"
sed -i "s|__FISH_LIST__|$FISH_ESCAPED|g" "$JOB"

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
