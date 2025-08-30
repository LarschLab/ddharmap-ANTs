#!/usr/bin/env bash
# sync_min.sh  — minimal pull → canonicalize → publish
#
# WHAT IT DOES
#   PULL:
#     NAS/<owner>/<fish>/02_reg/00_preprocessing  → WORK mirror (same tree)
#     WORK mirror → SCRATCH/raw + SCRATCH/fixed (plus nas symlink + .origin.json)
#   CANONICALIZE:
#     Read SCRATCH/reg (+ optional SCRATCH/reg_to_avg2p)
#     Write canonical files into WORK/subjects/<fish>/02_reg/_canonical:
#       <fish>_<source>_in_<space>.nrrd, where <space> ∈ {2p, r1, ref}
#   PUBLISH:
#     Use SCRATCH/<fish>/nas symlink (truth) to find NAS/<owner>/<fish>
#     Copy canonical files → proper NAS/02_reg stage folders (no overwrite unless --force)
#
# USAGE
#   sync_min.sh [--pull|--push|--both] [--owner NAME | --nas-project-root PATH]
#               [--force] [--dry-run]
#               <fishID1> [fishID2 ...]
#
# Examples
#   Pull from Matilde and stage compute:
#     NAS="/nas/FAC/FBM/CIG/jlarsch/default/D2c/07 Data" \
#     WORK="/work/FAC/FBM/CIG/jlarsch/default/Danin" \
#     SCRATCH="/scratch/$USER" \
#     ./sync_min.sh --pull --owner Matilde L395_f10
#
#   Push results back (nas symlink already present from pull):
#     ./sync_min.sh --push L395_f10
#
# ENV
#   NAS (required for first pull if no symlink exists), WORK, SCRATCH
#
# Minimal dependencies: bash, rsync, readlink, sed

set -euo pipefail
IFS=$'\n\t'

# ---------- Base roots ----------
NAS_BASE="${NAS:-}"        # e.g. /nas/.../07 Data   (used with --owner on first pull)
WORK_BASE="${WORK:-$HOME/WORK}/experiments"
SCRATCH_BASE="${SCRATCH:-/scratch/$USER}/experiments"

# ---------- CLI ----------
MODE="both"                # pull | push | both
FORCE=0
DRY=0
OWNER=""                   # e.g. Matilde
NAS_PROJECT_ROOT=""        # e.g. "/nas/.../07 Data/Matilde" (alternative to --owner)

usage() {
  cat <<USAGE
Usage: $0 [--pull|--push|--both] [--owner NAME | --nas-project-root PATH]
          [--force] [--dry-run]
          <fishID1> [fishID2 ...]

Options:
  --pull               Only NAS → WORK → SCRATCH
  --push               Only SCRATCH → WORK/_canonical → NAS
  --both               (default) do both
  --owner NAME         When pulling first time, use \$NAS/NAME/<fish> as NAS subject
  --nas-project-root P Alternative base path like ".../07 Data/Matilde" (overrides --owner)
  --force              Allow overwrites at destinations
  --dry-run            Print actions without writing
USAGE
  exit 1
}

args=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --pull) MODE="pull"; shift ;;
    --push) MODE="push"; shift ;;
    --both) MODE="both"; shift ;;
    --owner) shift; OWNER="${1:-}"; [[ -n "$OWNER" ]] || { echo "ERR: --owner needs a name" >&2; exit 2; }; shift ;;
    --nas-project-root) shift; NAS_PROJECT_ROOT="${1:-}"; [[ -n "$NAS_PROJECT_ROOT" ]] || { echo "ERR: --nas-project-root needs a path" >&2; exit 2; }; shift ;;
    --force) FORCE=1; shift ;;
    --dry-run) DRY=1; shift ;;
    -h|--help) usage ;;
    --*) echo "ERR: unknown option $1" >&2; usage ;;
    *) args+=( "$1" ); shift ;;
  esac
done
[[ ${#args[@]} -ge 1 ]] || usage
FISH_IDS=( "${args[@]}" )

# ---------- utils ----------
log(){ echo "[$(date -Iseconds)] $*"; }
ensure_dir() {
  local p
  for p in "$@"; do
    if (( DRY )); then echo "DRY: mkdir -p $p"; else mkdir -p "$p"; fi
  done
}
rsync_cp() {
  # rsync wrapper that’s dry-run safe and respects --force
  local add=( -a --no-owner --no-group --chmod=ugo=rwX )
  (( FORCE )) || add+=( --ignore-existing )
  if (( DRY )); then
    echo "DRY: rsync ${add[*]} $*"
    return 0
  fi
  rsync "${add[@]}" "$@"
}
find_scratch_subj() {
  local fish="$1" top="$SCRATCH_BASE/subjects/$fish"
  [[ -d "$top" ]] && { echo "$top"; return 0; }
  # legacy fallback: pick most recent with reg or reg_to_avg2p
  mapfile -t cand < <(ls -d "$SCRATCH_BASE"/*/subjects/"$fish" 2>/dev/null || true)
  if (( ${#cand[@]} )); then
    local best="" best_m=0 m
    for c in "${cand[@]}"; do
      if [[ -d "$c/reg" ]]; then m=$(stat -c %Y "$c/reg" 2>/dev/null || echo 0)
      elif [[ -d "$c/reg_to_avg2p" ]]; then m=$(stat -c %Y "$c/reg_to_avg2p" 2>/dev/null || echo 0)
      else m=0; fi
      (( m > best_m )) && { best_m=$m; best="$c"; }
    done
    [[ -n "$best" ]] && { echo "$best"; return 0; }
  fi
  # default new subject path
  echo "$top"
}
resolve_nas_subject() {
  # Prefer SCRATCH/nas symlink or .origin.json → nas_subject_root
  local subj="$1" fish="$2"
  if [[ -L "$subj/nas" ]]; then
    local t; t="$(readlink -f "$subj/nas" || true)"
    [[ -n "$t" && -d "$t" ]] && { echo "$t"; return 0; }
  fi
  if [[ -f "$subj/.origin.json" ]]; then
    local t; t="$(sed -n -E 's/.*"nas_subject_root"[[:space:]]*:[[:space:]]*"([^"]+)".*/\1/p' "$subj/.origin.json" | head -n1)"
    [[ -n "$t" && -d "$t" ]] && { echo "$t"; return 0; }
  fi
  # First-time pull: use --nas-project-root or --owner + NAS
  if [[ -n "$NAS_PROJECT_ROOT" ]]; then
    echo "$NAS_PROJECT_ROOT/$fish"; return 0
  fi
  if [[ -n "$NAS_BASE" && -n "$OWNER" ]]; then
    echo "$NAS_BASE/$OWNER/$fish"; return 0
  fi
  return 1
}
# Route helper: given round number, return NAS stage folder
stage_for_round() {
  local r="$1"
  if [[ "$r" == "1" ]]; then
    printf '%s' "04_r1-ref"
  else
    printf '%s' "05_r${r}-ref"
  fi
}

# ---------- PULL ----------
pull_one() {
  local fish="$1"
  log "=== PULL $fish ==="
  local SCR_SUBJ; SCR_SUBJ="$(find_scratch_subj "$fish")"
  local NAS_SUBJ
  if ! NAS_SUBJ="$(resolve_nas_subject "$SCR_SUBJ" "$fish")"; then
    echo "ERR: cannot resolve NAS subject for $fish (provide --owner or --nas-project-root, and set \$NAS)." >&2
    return 0
  fi
  local PRE="$NAS_SUBJ/02_reg/00_preprocessing"
  if [[ ! -d "$PRE" ]]; then
    log "  WARN: missing $PRE — nothing to pull."
    return 0
  fi
  local WORK_SUBJ="$WORK_BASE/subjects/$fish"
  local WORK_PRE="$WORK_SUBJ/02_reg/00_preprocessing"
  local RAW="$SCR_SUBJ/raw" FIXED="$SCR_SUBJ/fixed"

  # 1) Mirror preprocessing to WORK
  ensure_dir "$WORK_PRE"
  for sub in 2p_anatomy r1 rn; do
    if [[ -d "$PRE/$sub" ]]; then
      ensure_dir "$WORK_PRE/$sub"
      rsync_cp "$PRE/$sub/" "$WORK_PRE/$sub/"
    else
      log "  INFO: no $sub in preprocessing"
    fi
  done

  # 2) Stage SCRATCH raw/fixed (respect names for downstream)
  ensure_dir "$RAW/anatomy_2P" "$RAW/confocal_round1" "$RAW/confocal_round2" "$FIXED"
  [[ -d "$WORK_PRE/2p_anatomy" ]] && rsync_cp "$WORK_PRE/2p_anatomy/" "$RAW/anatomy_2P/" || true
  [[ -d "$WORK_PRE/r1"         ]] && rsync_cp "$WORK_PRE/r1/"         "$RAW/confocal_round1/" || true
  [[ -d "$WORK_PRE/rn"         ]] && rsync_cp "$WORK_PRE/rn/"         "$RAW/confocal_round2/" || true

  # Fixed refs (first GCaMP found; skip if already present unless --force)
  shopt -s nullglob
  local gA=( "$RAW/anatomy_2P/"*GCaMP*.nrrd )
  local gR1=( "$RAW/confocal_round1/"*GCaMP*.nrrd )
  shopt -u nullglob
  if (( FORCE )) || [[ ! -f "$FIXED/anatomy_2P_ref_GCaMP.nrrd" ]]; then
    [[ ${#gA[@]} -gt 0 ]] && rsync_cp "${gA[0]}" "$FIXED/anatomy_2P_ref_GCaMP.nrrd"
  fi
  if (( FORCE )) || [[ ! -f "$FIXED/round1_ref_GCaMP.nrrd" ]]; then
    [[ ${#gR1[@]} -gt 0 ]] && rsync_cp "${gR1[0]}" "$FIXED/round1_ref_GCaMP.nrrd"
  fi

  # 3) Traceability on SCRATCH
  if (( DRY )); then
    echo "DRY: ln -sfn \"$NAS_SUBJ\" \"$SCR_SUBJ/nas\""
    echo "DRY: write $SCR_SUBJ/.origin.json"
  else
    ln -sfn "$NAS_SUBJ" "$SCR_SUBJ/nas"
    cat > "$SCR_SUBJ/.origin.json" <<JSON
{
  "fish_id": "$fish",
  "nas_subject_root": "$NAS_SUBJ",
  "work_subject_root": "$WORK_SUBJ",
  "scratch_subject_root": "$SCR_SUBJ",
  "created_utc": "$(date -u +"%Y-%m-%dT%H:%M:%SZ")",
  "created_by": "${USER:-unknown}",
  "script": "sync_min.sh"
}
JSON
  fi
  log "  ✔ pulled $fish"
}

# ---------- CANONICALIZE ----------
canonicalize_one() {
  local fish="$1"
  log "=== CANONICALIZE $fish ==="
  local SCR_SUBJ; SCR_SUBJ="$(find_scratch_subj "$fish")"
  local REG="$SCR_SUBJ/reg"
  local REG_AVG="$SCR_SUBJ/reg_to_avg2p"
  if [[ ! -d "$REG" && ! -d "$REG_AVG" ]]; then
    log "  INFO: no reg/ or reg_to_avg2p/ — skip."
    return 0
  fi
  local CANON="$WORK_BASE/subjects/$fish/02_reg/_canonical"
  ensure_dir "$CANON"

  # NEW (no duplicates)
  shopt -s nullglob globstar
  local files=()
  [[ -d "$REG"     ]] && files+=( "$REG"/**/*.nrrd )
  [[ -d "$REG_AVG" ]] && files+=( "$REG_AVG"/**/*.nrrd )
  shopt -u globstar
  (( ${#files[@]} )) || { log "  INFO: no nrrd files found"; return 0; }

  for src in "${files[@]}"; do
    local base="$(basename "$src")"
    local out="$base"

    # avg2p → ref (standardize)
    out="${out/_in_avg2p.nrrd/_in_ref.nrrd}"
    out="${out/_to_avg2p__aligned.nrrd/_in_ref.nrrd}"

    # explicit to_* → _in_ref/_in_2p/_in_r1
    out="${out/_to_ref__aligned/_in_ref}"
    out="${out/_to_ref_aligned/_in_ref}"
    out="$(echo "$out" | sed -E 's/_to_ref(_[^.]*)?_aligned\.nrrd$/_in_ref.nrrd/')"
    out="$(echo "$out" | sed -E 's/_aligned_2P\.nrrd$/_in_2p.nrrd/')"
    out="$(echo "$out" | sed -E 's/_to_2p([^.]*)?_aligned\.nrrd$/_in_2p.nrrd/')"
    out="$(echo "$out" | sed -E 's/_to_r1([^.]*)?_aligned\.nrrd$/_in_r1.nrrd/')"

    # simple *_aligned.nrrd from reg/ (use round to infer)
    if [[ "$out" =~ ^${fish}_round1_.*_aligned\.nrrd$ ]]; then
      out="${out/_aligned.nrrd/_in_2p.nrrd}"
    elif [[ "$out" =~ ^${fish}_round2_.*_aligned\.nrrd$ ]]; then
      out="${out/_aligned.nrrd/_in_r1.nrrd}"
    elif [[ "$out" =~ ^${fish}_round[12]_GCaMP_to_ref_aligned\.nrrd$ ]]; then
      out="${out/_aligned.nrrd/_in_ref.nrrd}"
    fi

    # Special-case: in reg/, GCaMP_to_ref_aligned is actually relative
    # r1 → in_2p, r2 → in_r1
    if [[ "$src" == */reg/round1_GCaMP_to_ref_aligned.nrrd ]]; then
      out="$(echo "$out" | sed -E 's/_in_ref\.nrrd$/_in_2p.nrrd/')"
    elif [[ "$src" == */reg/round2_GCaMP_to_ref_aligned.nrrd ]]; then
      out="$(echo "$out" | sed -E 's/_in_ref\.nrrd$/_in_r1.nrrd/')"
    fi

    # fish prefix
    [[ "$out" == ${fish}_* ]] || out="${fish}_$out"
    # last-resort guarantee of suffix
    [[ "$out" =~ _in_(ref|2p|r1)\.nrrd$ ]] || out="${out%.nrrd}_in_ref.nrrd"

    local dst="$CANON/$out"
    if (( FORCE )) || [[ ! -f "$dst" ]]; then
      rsync_cp "$src" "$dst"
      log "  + $(basename "$src") -> $(basename "$dst")"
    else
      log "  = exists: $(basename "$dst")"
    fi
  done
}

stage_mats_logs_one() {
  local fish="$1"
  local SCR_SUBJ; SCR_SUBJ="$(find_scratch_subj "$fish")"
  [[ -n "$SCR_SUBJ" ]] || { log "  WARN: no SCRATCH subject; skip mats/logs staging"; return 0; }

  local REG="$SCR_SUBJ/reg"
  local REG_AVG="$SCR_SUBJ/reg_to_avg2p"
  local REG_WORK="$WORK_BASE/subjects/$fish/02_reg"

  # --------- from reg_to_avg2p (rename to *_to_ref_* and prefix fish) ---------
  if [[ -d "$REG_AVG" ]]; then
    # 2P → ref  -> 08_2pa-ref/transMatrices
    ensure_dir "$REG_WORK/08_2pa-ref/transMatrices"
    shopt -s nullglob
    for m in "$REG_AVG"/2P_to_avg2p_*; do
      [[ -f "$m" ]] || continue
      bn="$(basename "$m")"
      bn="${bn/to_avg2p_/to_ref_}"
      rsync_cp "$m" "$REG_WORK/08_2pa-ref/transMatrices/${fish}_$bn"
    done

    # roundN GCaMP → ref -> 04_r1-ref or 05_rN-ref/transMatrices
    for m in "$REG_AVG"/round*_GCaMP_to_avg2p_*; do
      [[ -f "$m" ]] || continue
      b="$(basename "$m")"
      if [[ "$b" =~ ^round([0-9]+)_ ]]; then R="${BASH_REMATCH[1]}"; else R="1"; fi
      stg="$(stage_for_round "$R")"
      ensure_dir "$REG_WORK/$stg/transMatrices"
      bn="${b/to_avg2p_/to_ref_}"
      rsync_cp "$m" "$REG_WORK/$stg/transMatrices/${fish}_$bn"
    done
    shopt -u nullglob

    # logs from reg_to_avg2p -> 08_2pa-ref/logs (keep filenames)
    if [[ -d "$REG_AVG/logs" ]]; then
      ensure_dir "$REG_WORK/08_2pa-ref/logs"
      rsync_cp "$REG_AVG/logs/" "$REG_WORK/08_2pa-ref/logs/"
    fi
  fi

  # --------- from reg (already *_to_ref*, normalize underscore; logs → stages) ---------
  if [[ -d "$REG" ]]; then
    # roundN transforms in reg/
    shopt -s nullglob
    for m in "$REG"/round*_GCaMP_to_ref*; do
      [[ -f "$m" ]] || continue
      b="$(basename "$m")"
      if [[ "$b" =~ ^round([0-9]+)_ ]]; then R="${BASH_REMATCH[1]}"; else R="1"; fi
      stg="$(stage_for_round "$R")"
      ensure_dir "$REG_WORK/$stg/transMatrices"
      # Ensure "_to_ref_0..." (insert underscore if missing)
      bn="$(echo "$b" | sed -E 's/_to_ref([01])/_to_ref_\1/')"
      rsync_cp "$m" "$REG_WORK/$stg/transMatrices/${fish}_$bn"
    done
    shopt -u nullglob

    # logs from reg/logs -> split by _rN, default to r1
    if [[ -d "$REG/logs" ]]; then
      shopt -s nullglob
      for lf in "$REG/logs/"*; do
        bn="$(basename "$lf")"
        if [[ "$bn" =~ _r([0-9]+) ]]; then
          stg="$(stage_for_round "${BASH_REMATCH[1]}")"
        else
          stg="04_r1-ref"
        fi
        ensure_dir "$REG_WORK/$stg/logs"
        rsync_cp "$lf" "$REG_WORK/$stg/logs/$bn"
      done
      shopt -u nullglob
    fi
  fi
}

# ---------- PUBLISH ----------
publish_one() {
  local fish="$1"
  log "=== PUBLISH $fish ==="
  local SCR_SUBJ; SCR_SUBJ="$(find_scratch_subj "$fish")"
  local NAS_SUBJ
  if ! NAS_SUBJ="$(resolve_nas_subject "$SCR_SUBJ" "$fish")"; then
    echo "ERR: cannot resolve NAS target for $fish (missing nas symlink?)." >&2
    return 0
  fi
  local CANON="$WORK_BASE/subjects/$fish/02_reg/_canonical"
  [[ -d "$CANON" ]] || { log "  INFO: no _canonical — skip."; return 0; }

  local ROOT="$NAS_SUBJ/02_reg"
  ensure_dir "$ROOT"

  shopt -s nullglob
  for f in "$CANON/"*.nrrd; do
    local bn="$(basename "$f")" dest="" stage="" sub="aligned"

    # per-channel in ref → 06_total-ref/aligned/roundN
    if [[ "$bn" =~ ^${fish}_round([0-9]+)_channel[0-9]+_.*_in_ref\.nrrd$ ]]; then
      local R="${BASH_REMATCH[1]}"
      stage="06_total-ref"; ensure_dir "$ROOT/$stage/aligned/round${R}"
      dest="$ROOT/$stage/aligned/round${R}/$bn"
    # anatomy_2P in ref
    elif [[ "$bn" == "${fish}_anatomy_2P_in_ref.nrrd" ]]; then
      stage="08_2pa-ref"; ensure_dir "$ROOT/$stage/aligned"
      dest="$ROOT/$stage/aligned/$bn"
    # round1 in 2p
    elif [[ "$bn" =~ ^${fish}_round1_.*_in_2p\.nrrd$ ]]; then
      stage="01_r1-2p"; ensure_dir "$ROOT/$stage/aligned"
      dest="$ROOT/$stage/aligned/$bn"
    # round2 in r1
    elif [[ "$bn" =~ ^${fish}_round2_.*_in_r1\.nrrd$ ]]; then
      stage="02_rn-r1"; ensure_dir "$ROOT/$stage/aligned"
      dest="$ROOT/$stage/aligned/$bn"
    # round2 in 2p (if present)
    elif [[ "$bn" =~ ^${fish}_round2_.*_in_2p\.nrrd$ ]]; then
      stage="03_rn-2p"; ensure_dir "$ROOT/$stage/aligned"
      dest="$ROOT/$stage/aligned/$bn"
    # round1 ref (stage root)
    elif [[ "$bn" =~ ^${fish}_round1_.*_in_ref\.nrrd$ ]]; then
      stage="04_r1-ref"; ensure_dir "$ROOT/$stage"
      dest="$ROOT/$stage/$bn"
    # round2 ref (stage root)
    elif [[ "$bn" =~ ^${fish}_round2_.*_in_ref\.nrrd$ ]]; then
      stage="05_r2-ref"; ensure_dir "$ROOT/$stage"
      dest="$ROOT/$stage/$bn"
    else
      log "  WARN: no NAS mapping for $bn (skipped)."
      continue
    fi

    if (( FORCE )) || [[ ! -f "$dest" ]]; then
      rsync_cp "$f" "$dest"
      log "  → $stage/${dest##*/}"
    else
      log "  = exists: $stage/${dest##*/}"
    fi
  done
  shopt -u nullglob
  # Publish staged transMatrices & logs mirrored from WORK → NAS
  local WORK_02="$WORK_BASE/subjects/$fish/02_reg"
  shopt -s nullglob
  for stgdir in "$WORK_02"/*; do
    [[ -d "$stgdir" ]] || continue
    stg="$(basename "$stgdir")"

    if [[ -d "$stgdir/transMatrices" ]]; then
      ensure_dir "$ROOT/$stg/transMatrices"
      rsync_cp "$stgdir/transMatrices/" "$ROOT/$stg/transMatrices/"
    fi
    if [[ -d "$stgdir/logs" ]]; then
      ensure_dir "$ROOT/$stg/logs"
      rsync_cp "$stgdir/logs/" "$ROOT/$stg/logs/"
    fi
  done
  shopt -u nullglob
  log "  ✔ published $fish"
}

# ---------- MAIN ----------
echo "Roots:
  NAS     : ${NAS_BASE:-"(not used unless first pull + --owner)"}
  WORK    : $WORK_BASE
  SCRATCH : $SCRATCH_BASE
Mode=$MODE  Force=$FORCE  Dry=$DRY  Owner=${OWNER:-"-"}  NAS_ROOT=${NAS_PROJECT_ROOT:-"-"}"

for fish in "${FISH_IDS[@]}"; do
  [[ "$MODE" == "pull" || "$MODE" == "both" ]] && pull_one "$fish"
  canonicalize_one "$fish"
  stage_mats_logs_one "$fish"
  [[ "$MODE" == "push" || "$MODE" == "both" ]] && publish_one "$fish"
done

log "All done."
