#!/usr/bin/env bash
# sync_scratch_to_work_and_nas.sh
# Copies REGISTRATION OUTPUTS from SCRATCH to WORK (mirror-format) and to NAS (curated into 02_reg stages).
#
# New structure support:
#   - No experiment arg. We auto-locate subjects under:
#       1) $SCRATCH/experiments/subjects/<fish> (preferred)
#       2) $SCRATCH/experiments/*/subjects/<fish> (legacy projects)
#     If multiple matches, prefer ones containing reg_to_avg2p; if multiple remain,
#     pick the most recently modified reg_to_avg2p.
#   - WORK mirrors the relative subject path under $SCRATCH/experiments/…
#   - NAS path is discovered from the subject's `nas` symlink or `.nas_path` file;
#     fallback can be provided with --nas-project-root (or NAS_PROJECT_ROOT env).
#
# Usage:
#   sync_scratch_to_work_and_nas.sh <fishID1> [fishID2 ...]
#       [--dry-run] [--all] [--nas-project-root PATH]
#
# Notes:
# - We DO NOT copy anything to NAS/02_reg/00_preprocessing. Those inputs already exist there.
# - We DO copy aligned volumes to NAS (02_reg/06_total-ref/aligned/roundN/...) and 08_2pa-ref/aligned/.
# - Requires: bash, rsync, readlink, stat, sed
# - NAS filenames are prefixed with the fishID (e.g., L331_f01_round2_GCaMP_to_ref_1Warp.nii.gz). Existing names that already start with fishID_ are not double-prefixed.

set -euo pipefail
IFS=$'
	'

# ---------- Base paths (can be overridden via env) ----------
SCRATCH_BASE="${SCRATCH:-/scratch/$USER}/experiments"
WORK_BASE="${WORK:-/work/FAC/FBM/CIG/jlarsch/default/Danin}/experiments"
NAS_PROJECT_ROOT_ENV="${NAS_PROJECT_ROOT:-}"

# ---------- Args ----------
usage() {
  cat <<USAGE
Usage: $0 <fishID1> [fishID2 ...] [--dry-run] [--all] [--nas-project-root PATH]
  --dry-run            : print what would be copied (no writes)
  --all                : mirror the ENTIRE subject folder from SCRATCH -> WORK (not just reg_to_avg2p)
  --nas-project-root P : fallback NAS project root to use when SCRATCH subject lacks nas/.nas_path
                         (e.g., "/nas/FAC/FBM/CIG/jlarsch/default/D2c/07 Data/Matilde")
USAGE
  exit 1
}
[[ $# -ge 1 ]] || usage

DRY_RUN=0
COPY_ALL_TO_WORK=0
FALLBACK_NAS_ROOT="${NAS_PROJECT_ROOT_ENV}"
FISH_IDS=()

# Collect fish IDs and flags
while (( "$#" )); do
  case "$1" in
    --dry-run) DRY_RUN=1; shift ;;
    --all) COPY_ALL_TO_WORK=1; shift ;;
    --nas-project-root) shift; FALLBACK_NAS_ROOT="${1:-}"; [[ -n "$FALLBACK_NAS_ROOT" ]] || { echo "ERROR: --nas-project-root requires a path" >&2; exit 2; }; shift ;;
    -h|--help) usage ;;
    --*) echo "Unknown option: $1" >&2; usage ;;
    *) FISH_IDS+=("$1"); shift ;;
  esac
done

[[ ${#FISH_IDS[@]} -ge 1 ]] || usage

# ---------- Helpers ----------
log() { echo -e "$*"; }
run() { if (( DRY_RUN )); then printf 'DRY:'; printf ' %q' "$@"; echo; else "$@"; fi }
ensure_dir() { if (( DRY_RUN )); then echo "DRY: mkdir -p \"$1\""; else mkdir -p "$1"; fi }

# Ensure every file in a directory starts with the fishID_ prefix
ensure_fish_prefix_in_dir() {
  local dir="$1" fish="$2"
  shopt -s nullglob
  for p in "$dir"/*; do
    [[ -f "$p" ]] || continue
    local b="$(basename "$p")"
    [[ "$b" == ${fish}_* ]] && continue
    run rsync -a --protect-args "$p" "$dir/${fish}_$b"
    if (( ! DRY_RUN )); then rm -f -- "$p"; fi
  done
  shopt -u nullglob
}

# Prefer top-level subjects/<fish>, else find legacy matches and pick best.
find_subject_dir() {
  local fish="$1"
  local cand top
  top="$SCRATCH_BASE/subjects/$fish"
  if [[ -d "$top" ]]; then printf '%s' "$top"; return 0; fi
  # legacy search
  mapfile -t cand < <(ls -d "$SCRATCH_BASE"/*/subjects/"$fish" 2>/dev/null || true)
  if (( ${#cand[@]} == 0 )); then return 1; fi
  if (( ${#cand[@]} == 1 )); then printf '%s' "${cand[0]}"; return 0; fi
  # reduce to those containing reg_to_avg2p
  local withreg=()
  local c
  for c in "${cand[@]}"; do [[ -d "$c/reg_to_avg2p" ]] && withreg+=("$c"); done
  if (( ${#withreg[@]} == 1 )); then printf '%s' "${withreg[0]}"; return 0; fi
  if (( ${#withreg[@]} > 1 )); then
    # pick most recent reg_to_avg2p mtime
    local best="" best_m=0 m
    for c in "${withreg[@]}"; do
      m=$(stat -c %Y "$c/reg_to_avg2p" 2>/dev/null || echo 0)
      if (( m > best_m )); then best_m=$m; best="$c"; fi
    done
    if [[ -n "$best" ]]; then printf '%s' "$best"; return 0; fi
  fi
  # fallback: first candidate
  printf '%s' "${cand[0]}"; return 0
}

# Resolve NAS subject root from subject folder
resolve_nas_root() {
  local subj="$1" fish="$2"
  local p_symlink="$subj/nas"
  local p_file="$subj/.nas_path"
  local dest
  if [[ -L "$p_symlink" ]]; then dest="$(readlink -f "$p_symlink")"; [[ -n "$dest" ]] && { printf '%s' "$dest"; return 0; }; fi
  if [[ -f "$p_file" ]]; then dest="$(<"$p_file")"; dest="${dest%%[$'
']*}"; [[ -n "$dest" ]] && { printf '%s' "$dest"; return 0; }; fi
  if [[ -n "$FALLBACK_NAS_ROOT" ]]; then printf '%s' "${FALLBACK_NAS_ROOT}/${fish}"; return 0; fi
  return 1
}

# Copy with rename: SRC -> DST_DIR / (rename pattern s/from/to/)
copy_rename() {
  local src="$1" dst_dir="$2" from_pat="$3" to_repl="$4"
  [[ -f "$src" ]] || { log "  (skip missing) $src"; return 0; }
  ensure_dir "$dst_dir"
  local base dst
  base="$(basename "$src")"
  dst="$(printf '%s' "$base" | sed "s/${from_pat}/${to_repl}/")"
  run rsync -a --protect-args "$src" "$dst_dir/$dst"
}

# ---------- Main loop ----------
STAMP="$(date +%Y%m%d_%H%M%S)"
SUMMARY="$SCRATCH_BASE/batch_logs/sync_${STAMP}.log"
ensure_dir "$(dirname "$SUMMARY")"

for fish in "${FISH_IDS[@]}"; do
  log "
=== Syncing $fish ==="
  if ! SCRATCH_SUBJ="$(find_subject_dir "$fish")"; then
    log "  ERROR: Could not find subject for $fish under $SCRATCH_BASE" | tee -a "$SUMMARY"; continue
  fi
  # relative path under $SCRATCH_BASE -> mirror to WORK
  REL_SUBJ="${SCRATCH_SUBJ#${SCRATCH_BASE}/}"
  WORK_SUBJ="$WORK_BASE/$REL_SUBJ"

  if ! NAS_SUBJ_ROOT="$(resolve_nas_root "$SCRATCH_SUBJ" "$fish")"; then
    log "  ERROR: Could not resolve NAS path for $fish. Provide 'nas' symlink or .nas_path under $SCRATCH_SUBJ, or pass --nas-project-root. Skipping." | tee -a "$SUMMARY"
    continue
  fi
  NAS_REG_ROOT="$NAS_SUBJ_ROOT/02_reg"

  # ---- Preflight ----
  if [[ ! -d "$SCRATCH_SUBJ/reg_to_avg2p" ]]; then
    log "  WARN: No reg_to_avg2p in $SCRATCH_SUBJ; skipping." | tee -a "$SUMMARY"
    continue
  fi

  # ---- Work mirror ----
  if (( COPY_ALL_TO_WORK )); then
    log "  WORK: mirroring entire subject -> $WORK_SUBJ"
    ensure_dir "$WORK_SUBJ"
    run rsync -a --delete --exclude 'nas' --protect-args "$SCRATCH_SUBJ/" "$WORK_SUBJ/"
  else
    log "  WORK: syncing reg_to_avg2p -> $WORK_SUBJ/reg_to_avg2p"
    ensure_dir "$WORK_SUBJ/reg_to_avg2p"
    run rsync -a --delete --protect-args "$SCRATCH_SUBJ/reg_to_avg2p/" "$WORK_SUBJ/reg_to_avg2p/"
  fi

  # ---- NAS curated sync ----
  log "  NAS root: $NAS_SUBJ_ROOT"
  ensure_dir "$NAS_REG_ROOT"

  # 1) 2P anatomy -> reference (08_2pa-ref)
  TWO_P_SRC_DIR="$SCRATCH_SUBJ/reg_to_avg2p"
  TWO_P_DST_TM="$NAS_REG_ROOT/08_2pa-ref/transMatrices"
  TWO_P_DST_LOG="$NAS_REG_ROOT/08_2pa-ref/logs"
  TWO_P_DST_ALI="$NAS_REG_ROOT/08_2pa-ref/aligned"
  ensure_dir "$TWO_P_DST_TM"; ensure_dir "$TWO_P_DST_LOG"; ensure_dir "$TWO_P_DST_ALI"

  copy_rename "$TWO_P_SRC_DIR/2P_to_avg2p_0GenericAffine.mat" "$TWO_P_DST_TM" 'to_avg2p_' 'to_ref_'
  copy_rename "$TWO_P_SRC_DIR/2P_to_avg2p_1Warp.nii.gz"          "$TWO_P_DST_TM" 'to_avg2p_' 'to_ref_'
  copy_rename "$TWO_P_SRC_DIR/2P_to_avg2p_1InverseWarp.nii.gz"    "$TWO_P_DST_TM" 'to_avg2p_' 'to_ref_'
  copy_rename "$TWO_P_SRC_DIR/logs/2P_to_avg2p.log"               "$TWO_P_DST_LOG" 'to_avg2p'  'to_ref'
  copy_rename "$TWO_P_SRC_DIR/anatomy_2P_in_avg2p.nrrd"           "$TWO_P_DST_ALI" '_in_avg2p' '_in_ref'
  # Ensure fishID prefix on NAS outputs for 2P step
  ensure_fish_prefix_in_dir "$TWO_P_DST_TM" "$fish"
  ensure_fish_prefix_in_dir "$TWO_P_DST_LOG" "$fish"
  ensure_fish_prefix_in_dir "$TWO_P_DST_ALI" "$fish"

  # 2) Round-wise transforms + logs (04_r1-ref, 05_rN-ref)
  shopt -s nullglob
  mapfile -t ROUND_MATS < <(ls -1 "$SCRATCH_SUBJ/reg_to_avg2p"/round*_GCaMP_to_avg2p_0GenericAffine.mat 2>/dev/null || true)
  shopt -u nullglob
  for mat in "${ROUND_MATS[@]}"; do
    base="$(basename "$mat")"                       # roundN_GCaMP_to_avg2p_0GenericAffine.mat
    if [[ "$base" =~ ^round([0-9]+)_GCaMP ]]; then
      R="${BASH_REMATCH[1]}"
    else
      log "  WARN: could not parse round from $base" | tee -a "$SUMMARY"
      continue
    fi
    if [[ "$R" == 1 ]]; then
      DST_TM="$NAS_REG_ROOT/04_r1-ref/transMatrices"
      DST_LOG="$NAS_REG_ROOT/04_r1-ref/logs"
    else
      DST_TM="$NAS_REG_ROOT/05_r${R}-ref/transMatrices"
      DST_LOG="$NAS_REG_ROOT/05_r${R}-ref/logs"
    fi
    ensure_dir "$DST_TM"; ensure_dir "$DST_LOG"
    copy_rename "$SCRATCH_SUBJ/reg_to_avg2p/round${R}_GCaMP_to_avg2p_0GenericAffine.mat" "$DST_TM" 'to_avg2p_' 'to_ref_'
    copy_rename "$SCRATCH_SUBJ/reg_to_avg2p/round${R}_GCaMP_to_avg2p_1Warp.nii.gz"          "$DST_TM" 'to_avg2p_' 'to_ref_'
    copy_rename "$SCRATCH_SUBJ/reg_to_avg2p/round${R}_GCaMP_to_avg2p_1InverseWarp.nii.gz"    "$DST_TM" 'to_avg2p_' 'to_ref_'
    copy_rename "$SCRATCH_SUBJ/reg_to_avg2p/logs/round${R}_GCaMP_to_avg2p.log" "$DST_LOG" 'to_avg2p' 'to_ref'
    # Ensure fishID prefix on per-round transforms/logs
    ensure_fish_prefix_in_dir "$DST_TM" "$fish"
    ensure_fish_prefix_in_dir "$DST_LOG" "$fish"
  done

  # 3) Aligned volumes into 06_total-ref/aligned/roundN
  ALI_ROOT="$NAS_REG_ROOT/06_total-ref/aligned"
  ensure_dir "$ALI_ROOT"

  # 3a) All per-channel aligned (e.g., Lxxx_roundN_channelK_*.nrrd)
  shopt -s nullglob
  for f in "$SCRATCH_SUBJ/reg_to_avg2p"/*_in_avg2p.nrrd; do
    fb="$(basename "$f")"
    if [[ "$fb" =~ round([0-9]+) ]]; then RND="${BASH_REMATCH[1]}"; else RND="misc"; fi
    DST_ROUND="$ALI_ROOT/round${RND}"
    ensure_dir "$DST_ROUND"
    copy_rename "$f" "$DST_ROUND" '_in_avg2p' '_in_ref'
  done

  # 3b) Round GCaMP aligned volumes (roundN_GCaMP_to_avg2p__aligned.nrrd)
  for f in "$SCRATCH_SUBJ/reg_to_avg2p"/round*_GCaMP_to_avg2p__aligned.nrrd; do
    [[ -f "$f" ]] || continue
    fb="$(basename "$f")"
    if [[ "$fb" =~ ^round([0-9]+)_GCaMP ]]; then RND="${BASH_REMATCH[1]}"; else RND="misc"; fi
    DST_ROUND="$ALI_ROOT/round${RND}"
    ensure_dir "$DST_ROUND"
    copy_rename "$f" "$DST_ROUND" 'to_avg2p' 'to_ref'
  done
  # Pass to ensure fishID prefix in each round folder under aligned
  for d in "$ALI_ROOT"/*; do
    [[ -d "$d" ]] || continue
    ensure_fish_prefix_in_dir "$d" "$fish"
  done
  shopt -u nullglob

  # 4) WITHIN-FISH registrations from reg/ -> NAS 01/02
  REG_DIR="$SCRATCH_SUBJ/reg"
  if [[ -d "$REG_DIR" ]]; then
    # 4a) round1 -> 2P (01_r1-2p)
    R1_TM="$NAS_REG_ROOT/01_r1-2p/matrices"
    R1_LOG="$NAS_REG_ROOT/01_r1-2p/logs"
    ensure_dir "$R1_TM"; ensure_dir "$R1_LOG"
    copy_rename_prefix "$REG_DIR/round1_GCaMP_to_ref0GenericAffine.mat" "$R1_TM" "$fish" 'to_ref' 'to_2p'
    copy_rename_prefix "$REG_DIR/round1_GCaMP_to_ref1Warp.nii.gz"          "$R1_TM" "$fish" 'to_ref' 'to_2p'
    copy_rename_prefix "$REG_DIR/round1_GCaMP_to_ref1InverseWarp.nii.gz"    "$R1_TM" "$fish" 'to_ref' 'to_2p'
    # logs (copy any r1 logs)
    if [[ -d "$REG_DIR/logs" ]]; then
      run rsync -a --protect-args "$REG_DIR/logs/" "$R1_LOG/"
      ensure_fish_prefix_in_dir "$R1_LOG" "$fish"
    fi

    # 4b) roundN -> r1 (02_rn-r1)
    shopt -s nullglob
    mapfile -t RN_MATS < <(ls -1 "$REG_DIR"/round[2-9]*_GCaMP_to_ref0GenericAffine.mat 2>/dev/null || true)
    shopt -u nullglob
    for mat in "${RN_MATS[@]}"; do
      b="$(basename "$mat")"
      [[ "$b" =~ ^round([0-9]+)_ ]] || { log "  WARN: cannot parse round from $b"; continue; }
      R="${BASH_REMATCH[1]}"
      RN_TM="$NAS_REG_ROOT/02_rn-r1/transMatrices/round${R}"
      RN_LOG="$NAS_REG_ROOT/02_rn-r1/logs"
      ensure_dir "$RN_TM"; ensure_dir "$RN_LOG"
      copy_rename_prefix "$REG_DIR/round${R}_GCaMP_to_ref0GenericAffine.mat" "$RN_TM" "$fish" 'to_ref' 'to_r1'
      copy_rename_prefix "$REG_DIR/round${R}_GCaMP_to_ref1Warp.nii.gz"          "$RN_TM" "$fish" 'to_ref' 'to_r1'
      copy_rename_prefix "$REG_DIR/round${R}_GCaMP_to_ref1InverseWarp.nii.gz"    "$RN_TM" "$fish" 'to_ref' 'to_r1'
      # copy logs too
      if [[ -d "$REG_DIR/logs" ]]; then
        run rsync -a --protect-args "$REG_DIR/logs/" "$RN_LOG/"
        ensure_fish_prefix_in_dir "$RN_LOG" "$fish"
      fi
    done
  fi

  log "  Finished $fish"
  echo "$(date +%F\ %T) - $fish synced" >> "$SUMMARY"

done

log "
All done. Summary: $SUMMARY"
