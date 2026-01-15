#!/usr/bin/env bash
# ants_apply_transforms.sh  (interactive)
# Batch apply precomputed transforms to zebrafish HCR channels.
# Modes:
#   1) R1  -> 2P anatomy
#   2) Rn  -> R1
#   3) R2…N (already in R1) -> 2P anatomy   [apply only R1->2P]

set -o pipefail

ANTSPATH="$HOME/ANTs/antsInstallExample/install/bin"
export ANTSPATH
ANTSBIN="$ANTSPATH"

die() { echo "ERROR: $*" >&2; exit 1; }

confirm_exists() {
  local label="$1"; shift
  local f
  for f in "$@"; do
    [[ -e "$f" ]] || die "$label missing: $f"
  done
}

normalize_mode() {
  local m="$1"
  case "${m,,}" in
    1|'r1-2p'|'r1->2p'|'r1_to_2p'|'r1to2p') echo 1 ;;
    2|'rn-r1'|'rn->r1'|'rn_to_r1'|'rntor1') echo 2 ;;
    3|'all-2p'|'all->2p'|'all_to_2p'|'r2..n->2p'|'r2-n->2p'|'r2plus_to_2p') echo 3 ;;
    *) echo "" ;;
  esac
}

detect_rounds_in_raw() {
  local raw_base="$BASE/raw"
  shopt -s nullglob
  local arr=()
  for d in "$raw_base"/confocal_round*; do
    local bn; bn=$(basename "$d")
    if [[ $bn =~ ^confocal_round([0-9]+)$ ]]; then
      arr+=("${BASH_REMATCH[1]}")
    fi
  done
  shopt -u nullglob
  if ((${#arr[@]})); then
    printf "%s\n" "${arr[@]}" | awk '!seen[$0]++' | sort -n
  fi
}

# Choose a glob for “already-in-R1” files for a given round.
select_r1space_glob() {
  local rk="$1"
  local candidates=(
    "$REGDIR/round${rk}_HCR_channel*_aligned.nrrd"     # from this script’s Mode 2 outputs
    "$REGDIR/round${rk}_HCR_channel*R1*.nrrd"          # generic “R1” hint in name
    "$REGDIR/round${rk}_HCR_channel*.nrrd"             # last-resort fallback
  )
  for pat in "${candidates[@]}"; do
    if compgen -G "$pat" > /dev/null; then
      echo "$pat"
      return 0
    fi
  done
  return 1
}

apply_for_glob() {
  # $1 = glob for MOV files, $2 = fixed image, $3.. = transforms (files)
  local pattern="$1"; shift
  local fixed="$1"; shift
  local transforms=( "$@" )

  # Build proper (-t "$t") args
  local TARGS=()
  for t in "${transforms[@]}"; do
    TARGS+=(-t "$t")
  done

  echo
  echo "  reference:  $fixed"
  echo "  transforms: ${transforms[*]}"
  echo

  shopt -s nullglob
  local any=0
  for MOV in $pattern; do
    any=1
    local base; base=$(basename "${MOV%.*}")
    local OUT="$REGDIR/${base}_2P.nrrd"
    echo ">>> ${base} → $OUT"
    "${ANTSBIN}/antsApplyTransforms" \
      -d 3 \
      -i "$MOV" \
      -r "$fixed" \
      -o "$OUT" \
      "${TARGS[@]}" \
      --interpolation WelchWindowedSinc
    if [[ $? -ne 0 ]]; then
      echo "!! Failed on $MOV" >&2
    fi
  done
  shopt -u nullglob

  if [[ $any -eq 0 ]]; then
    echo ">> no files matching $pattern"
  fi
}


# ——————————————————————————————————————————————
# 1) parse args or prompt
# ——————————————————————————————————————————————
if [[ $# -lt 2 ]]; then
  read -p "Experiment name: " EXP
  read -p "Fish ID: " FISH
else
  EXP=$1; FISH=$2
fi

RAW_MODE=""
if [[ $# -ge 4 ]]; then RAW_MODE="$4"; fi
MODE="$(normalize_mode "$RAW_MODE")"
while [[ -z "$MODE" ]]; do
  echo
  echo "Mapping mode:"
  echo "  [1] R1  -> 2P anatomy"
  echo "  [2] Rn  -> R1"
  echo "  [3] R2…N (already in R1) -> 2P anatomy"
  read -p "Choose 1/2/3: " RAW_MODE
  MODE="$(normalize_mode "$RAW_MODE")"
done

ROUND=""
if [[ "$MODE" == "2" ]]; then
  if [[ $# -ge 3 ]]; then ROUND="$3"; fi
  while ! [[ "$ROUND" =~ ^[0-9]+$ ]] || [[ "$ROUND" -eq 1 ]]; do
    [[ -n "$ROUND" ]] && echo "Please enter a round number >= 2."
    read -p "HCR round number to map to R1: " ROUND
  done
fi

# ——————————————————————————————————————————————
# 2) layout & references
# ——————————————————————————————————————————————
[[ -n "$SCRATCH" ]] || die "\$SCRATCH is not set."
BASE="$SCRATCH/experiments/$EXP/subjects/$FISH"
FIXED="$BASE/fixed"
REGDIR="$BASE/reg"
mkdir -p "$REGDIR"

REF_R1="$FIXED/round1_ref_GCaMP.nrrd"
REF_2P="$FIXED/anatomy_2P_ref_GCaMP.nrrd"
AFFINE_R1_2P="$REGDIR/round1_GCaMP_to_ref0GenericAffine.mat"
WARP_R1_2P="$REGDIR/round1_GCaMP_to_ref1Warp.nii.gz"

confirm_exists "Fixed image (R1 ref)" "$REF_R1"
confirm_exists "Fixed image (2P)" "$REF_2P"
confirm_exists "R1->2P transforms" "$AFFINE_R1_2P" "$WARP_R1_2P"

echo
echo "Experiment: $EXP"
echo "Fish:       $FISH"
echo "Mode:       $MODE"
[[ "$MODE" == "2" ]] && echo "Round:      $ROUND"
echo

# ——————————————————————————————————————————————
# 3) run requested mode
# ——————————————————————————————————————————————
case "$MODE" in
  1)
    # R1 raw → 2P
    local_glob="$BASE/raw/confocal_round1/round1_HCR_channel*.nrrd"
    echo "R1 → 2P anatomy"
    apply_for_glob "$local_glob" "$REF_2P" "$WARP_R1_2P" "$AFFINE_R1_2P"
    ;;

  2)
    # Rn raw → R1
    AFFINE_RK_R1="$REGDIR/round${ROUND}_GCaMP_to_ref0GenericAffine.mat"
    WARP_RK_R1="$REGDIR/round${ROUND}_GCaMP_to_ref1Warp.nii.gz"
    confirm_exists "R${ROUND}->R1 transforms" "$AFFINE_RK_R1" "$WARP_RK_R1"

    local_glob="$BASE/raw/confocal_round${ROUND}/round${ROUND}_HCR_channel*.nrrd"
    echo "R${ROUND} → R1"
    apply_for_glob "$local_glob" "$REF_R1" "$WARP_RK_R1" "$AFFINE_RK_R1"
    ;;

  3)
    # R2…N (already R1) → 2P using ONLY R1->2P transforms
    echo "[R2…N] (already in R1) → 2P anatomy"
    mapfile -t ALL_ROUNDS < <(detect_rounds_in_raw)
    if ((${#ALL_ROUNDS[@]} == 0)); then
      die "Could not find any confocal_round* folders under $BASE/raw"
    fi
    echo "Found rounds: ${ALL_ROUNDS[*]}"
    for rk in "${ALL_ROUNDS[@]}"; do
      (( rk >= 2 )) || continue
      mov_glob="$(select_r1space_glob "$rk")" || {
        echo "!! Skipping round ${rk}: no R1-space files found in $REGDIR" >&2
        continue
      }
      echo
      echo "Round ${rk}: applying R1->2P to files in R1 space"
      apply_for_glob "$mov_glob" "$REF_2P" "$WARP_R1_2P" "$AFFINE_R1_2P"
    done
    ;;
esac

echo
echo "Done. Outputs in $REGDIR."
