#!/usr/bin/env bash

# ants_apply_transforms.sh
#
# Prompt-driven wrapper around antsApplyTransforms
# Transforms all round${ROUND}_channel*.nrrd files into your reg/ folder
#
# Usage: 
#   bash ants_apply_transforms.sh [Experiment] [FishID] [Round]
# or just
#   bash ants_apply_transforms.sh
#   (then you’ll be prompted)

ANTSPATH="$HOME/ANTs/antsInstallExample/install/bin"
export ANTSPATH
ANTSBIN="$ANTSPATH"

# ——————————————————————————————————————————————
# 1) parse args or prompt
# ——————————————————————————————————————————————
if [ $# -lt 3 ]; then
  read -p "Experiment name: " EXP
  read -p "Fish ID: " FISH
  read -p "HCR round number: " ROUND
else
  EXP=$1; FISH=$2; ROUND=$3
fi

# ——————————————————————————————————————————————
# 2) define folder layout (all under $SCRATCH)
# ——————————————————————————————————————————————
BASE="$SCRATCH/experiments/$EXP/subjects/$FISH"
RAW_CONF="$BASE/raw/confocal_round${ROUND}"
FIXED="$BASE/fixed"
REGDIR="$BASE/reg"
mkdir -p "$REGDIR"

# ——————————————————————————————————————————————
# 3) point to your pre‐computed transforms
#    (must already exist from your registration run)
# ——————————————————————————————————————————————
AFFINE="$REGDIR/round${ROUND}_GCaMP_to_ref0GenericAffine.mat"
WARP="$REGDIR/round${ROUND}_GCaMP_to_ref1Warp.nii.gz"
REF_GCaMP="$FIXED/$( [ $ROUND -eq 1 ] && echo anatomy_2P_ref_GCaMP.nrrd || echo round1_ref_GCaMP.nrrd )"

# sanity checks
for f in "$AFFINE" "$WARP" "$REF_GCaMP"; do
  if [ ! -e "$f" ]; then
    echo "ERROR: required file not found: $f" >&2
    exit 1
  fi
done

echo "Applying transforms to all HCR channels in:"
echo "  moving folder: $RAW_CONF"
echo "  reference:     $REF_GCaMP"
echo "  affine:        $AFFINE"
echo "  warp:          $WARP"
echo

# ——————————————————————————————————————————————
# 4) loop & transform
# ——————————————————————————————————————————————
for MOV in "$RAW_CONF"/round${ROUND}_HCR_channel*.nrrd; do
  [ -e "$MOV" ] || { echo ">> no files matching $MOV"; break; }
  NAME=$(basename "${MOV%.*}")
  OUT="$REGDIR/${NAME}_aligned.nrrd"

  echo ">>> $NAME → $OUT"
  "${ANTSBIN}/antsApplyTransforms" \
    -d 3 \
    -i "$MOV" \
    -r "$REF_GCaMP" \
    -o "$OUT" \
    -t "$WARP" \
    -t "$AFFINE" \
    --interpolation WelchWindowedSinc

  if [ $? -ne 0 ]; then
    echo "!! Failed on $MOV" >&2
  fi
done

echo; echo "Done.  All transformed files are in $REGDIR."
