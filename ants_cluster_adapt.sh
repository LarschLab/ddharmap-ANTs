#!/usr/bin/env bash
# ants_register_multi.sh  (interactive; single job for multiple fish)
#
# SCRATCH layout:
#   $SCRATCH/experiments/subjects/<FishID>/{raw,fixed,reg}
#
# Prompts for: ROUND, PARTITION, Fish IDs (space-separated)
#
# Fixed reference:
#   ROUND=1  -> fixed/anatomy_2P_ref_GCaMP.nrrd
#   ROUND>1  -> fixed/round1_ref_GCaMP.nrrd
#
# Moving GCaMP detection (new format first, then fallbacks):
#   <Fish>_round<R>_channel1_*GCaMP*.nrrd
#   <Fish>_round<R>_*GCaMP*.nrrd
#   round<R>_GCaMP.nrrd
#   round<R>_*channel*GCaMP*.nrrd
#   round<R>_*GCaMP*.nrrd
#
# HCR/other channels to transform (exclude GCaMP):
#   <Fish>_round<R>_channel[2-9]_*.nrrd
#   + fallbacks: round<R>_HCR_channel*.nrrd, round<R>_channel*.nrrd, round<R>_HCR_*.nrrd (minus *GCaMP*)
#
# ENV you likely already have:
#   SCRATCH=/scratch/<user>
#   ANTSPATH=$HOME/ANTs/antsInstallExample/install/bin
# Optional:
#   MAIL_USER, MAIL_TYPE, CPUS, MEM

set -euo pipefail

ANTSPATH="${ANTSPATH:-$HOME/ANTs/antsInstallExample/install/bin}"
export ANTSPATH
ANTSBIN="$ANTSPATH"

WALL_TIME="24:00:00"
MAIL_TYPE="${MAIL_TYPE:-END,FAIL}"
MAIL_USER="danin.dharmaperwira@unil.ch"

# -------- Prompts --------
read -rp "ROUND (e.g., 1 or 2): " ROUND
read -rp "PARTITION (e.g., test | cpu | normal | gpu | other): " PARTITION
read -rp "Fish IDs (space-separated): " FISH_LINE
# shellcheck disable=SC2206
FISH_IDS=( $FISH_LINE )

[[ -n "${SCRATCH:-}" ]] || { echo "ERROR: SCRATCH env not set."; exit 2; }

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
JOB="$JOBDIR/ants_r${ROUND}_${STAMP}.sh"

FISH_SERIALIZED=""
for f in "${FISH_IDS[@]}"; do
  [[ -n "$f" ]] || continue
  FISH_SERIALIZED+="$f"$'\n'
done

# NEW: save fish list to a sidecar file next to the job script
FISH_FILE="$JOB.fish"
printf '%s' "$FISH_SERIALIZED" > "$FISH_FILE"

cat > "$JOB" <<'EOS'
#!/usr/bin/env bash
set -euo pipefail

ANTSPATH="${ANTSPATH:-$HOME/ANTs/antsInstallExample/install/bin}"
export ANTSPATH
export ITK_GLOBAL_DEFAULT_NUMBER_OF_THREADS="${SLURM_CPUS_PER_TASK:-1}"
export OMP_NUM_THREADS="${SLURM_CPUS_PER_TASK:-1}"

ROUND="__ROUND__"
SCRATCH_BASE="${SCRATCH:?SCRATCH env not set}"
FISH_FILE="__FISH_FILE__"

echo "ANTs bin : $ANTSPATH"
echo "Threads  : ${SLURM_CPUS_PER_TASK:-1}"
echo "ROUND    : $ROUND"

# --- helpers ---
pick_moving_gcamp() {
  # args: dir round
  local d="$1" r="$2"
  shopt -s nullglob
  # New-format, most specific first
  local cands=(
    "$d"/*_round${r}_channel1_*GCaMP*.nrrd
    "$d"/*_round${r}_*GCaMP*.nrrd
    # Old-format fallbacks
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
    if [[ "$f" == *GCaMP* ]]; then
      continue
    fi
    out+=( "$f" )
  done
  shopt -u nullglob
  printf "%s\n" "${out[@]}"
}

# ---- main loop over fish ----
while IFS= read -r FISH; do
  [[ -n "$FISH" ]] || continue
  echo "===== Processing $FISH ====="

  BASE="$SCRATCH_BASE/experiments/subjects/$FISH"
  RAW_CONF="$BASE/raw/confocal_round${ROUND}"
  FIXED="$BASE/fixed"
  REGDIR="$BASE/reg"
  LOGDIR="$REGDIR/logs"
  mkdir -p "$REGDIR" "$LOGDIR"

  # choose fixed reference
  if [[ "$ROUND" -eq 1 ]]; then
    REF_GC="$FIXED/anatomy_2P_ref_GCaMP.nrrd"
  else
    REF_GC="$FIXED/round1_ref_GCaMP.nrrd"
  fi
  if [[ ! -f "$REF_GC" ]]; then
    echo "ERROR: Missing fixed reference: $REF_GC"
    echo "Present in $FIXED/:"
    ls -1 "$FIXED" 2>/dev/null | sed 's/^/  - /' || true
    continue
  fi

  # moving GCaMP detection (new/old formats)
  if ! MOV_GC="$(pick_moving_gcamp "$RAW_CONF" "$ROUND")"; then
    echo "ERROR: Could not find moving GCaMP in $RAW_CONF"
    echo "Contents of $RAW_CONF:"
    ls -1 "$RAW_CONF" 2>/dev/null | sed 's/^/  - /' || true
    continue
  fi
  echo "  FIXED  = $REF_GC"
  echo "  MOVING = $MOV_GC"

  OUT_PREFIX="$REGDIR/round${ROUND}_GCaMP_to_ref"

  # 1) Registration (writes transforms + aligned GCaMP)
  "$ANTSPATH/antsRegistration" \
    -d 3 --float 1 --verbose 1 \
    -o ["$OUT_PREFIX","${OUT_PREFIX}_aligned.nrrd"] \
    --interpolation WelchWindowedSinc \
    --winsorize-image-intensities [0.025,0.975] \
    --use-histogram-matching 1 \
    -r ["$REF_GC","$MOV_GC",1] \
    -t rigid[0.1] \
    -m MI["$REF_GC","$MOV_GC",1,32,Regular,0.25] \
    -c [200x200x200x200,1e-6,10] \
    --shrink-factors 12x8x4x2 \
    --smoothing-sigmas 4x3x2x1vox \
    -t Affine[0.1] \
    -m MI["$REF_GC","$MOV_GC",1,32,Regular,0.25] \
    -c [200x200x200x200,1e-6,10] \
    --shrink-factors 12x8x4x2 \
    --smoothing-sigmas 4x3x2x1vox \
    -t SyN[0.1,6,0.1] \
    -m CC["$REF_GC","$MOV_GC",1,4] \
    -c [200x200x200x200x10,1e-8,10] \
    --shrink-factors 12x8x4x2x1 \
    --smoothing-sigmas 4x3x2x1x0vox

  # (optional) second write is redundant, but kept as a no-op confirmation pattern:
  "$ANTSPATH/antsApplyTransforms" \
    -d 3 --verbose 1 \
    -r "$REF_GC" \
    -i "$MOV_GC" \
    -o "${OUT_PREFIX}_aligned.nrrd" \
    -t "${OUT_PREFIX}1Warp.nii.gz" \
    -t "${OUT_PREFIX}0GenericAffine.mat"

  # 2) Apply transforms to all non-GCaMP channels for this round
  mapfile -t CHANNELS < <(gather_hcr_channels "$RAW_CONF" "$ROUND")
  if (( ${#CHANNELS[@]} == 0 )); then
    echo "  (no non-GCaMP channels found to transform in $RAW_CONF)"
  fi
  for MOV in "${CHANNELS[@]}"; do
    base="$(basename "$MOV")"
    out="$REGDIR/${base%.*}_aligned.nrrd"
    echo "  Transforming: $base -> $(basename "$out")"
    "$ANTSPATH/antsApplyTransforms" \
      -d 3 --verbose 1 \
      -r "$REF_GC" \
      -i "$MOV" \
      -o "$out" \
      -t "${OUT_PREFIX}1Warp.nii.gz" \
      -t "${OUT_PREFIX}0GenericAffine.mat"
  done

  echo "===== Done $FISH ====="
done < "$FISH_FILE"
EOS

# Inject ROUND and the fish file path
sed -i "s|__ROUND__|$ROUND|g" "$JOB"
sed -i "s|__FISH_FILE__|$FISH_FILE|g" "$JOB"

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
    -J "ants_multi_r${ROUND}" \
    -o "$JOBDIR/ants_multi_r${ROUND}_${STAMP}.out" \
    -e "$JOBDIR/ants_multi_r${ROUND}_${STAMP}.err" \
    "$JOB"
  echo "Submitted single job for fish: ${FISH_IDS[*]}"
  echo "  Job script: $JOB"
  echo "  Logs: $JOBDIR/ants_multi_r${ROUND}_${STAMP}.{out,err}"
fi
