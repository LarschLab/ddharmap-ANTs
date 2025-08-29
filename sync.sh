#!/usr/bin/env bash
# sync_nas_compute_integrated.sh
#
# One script that:
#  (A) PULL:   NAS (02_reg/00_preprocessing) -> WORK mirror -> stage SCRATCH/{raw,fixed,reg}
#  (B) PUSH:   SCRATCH/reg images -> WORK/_canonical (rename to <fish>_<source>_in_<space>.nrrd)
#              -> NAS 02_reg/<stage> (copy ONLY if the exact file is missing unless --force)
#
# It also integrates:
#  - Smart SCRATCH subject discovery (preferred .../experiments/subjects/<fish>, with legacy)
#  - NAS path resolution via SCRATCH/nas symlink or .nas_path, with --nas-project-root fallback
#  - --dry-run, --force, optional --with-matrices and --with-logs
#
# Canonical name:  <fishID>_<source>_in_<space>.nrrd   where <space> ∈ {2p, ref, r1}
# Stage mapping:
#   round1_*_in_2p.nrrd         -> 02_reg/01_r1-2p/aligned
#   round2_*_in_r1.nrrd         -> 02_reg/02_rn-r1/aligned
#   round2_*_in_2p.nrrd         -> 02_reg/03_rn-2p/aligned
#   round1_*_in_ref.nrrd        -> 02_reg/04_r1-ref/
#   round2_*_in_ref.nrrd        -> 02_reg/05_r2-ref/
#   anatomy_2P_in_ref.nrrd      -> 02_reg/08_2pa-ref/aligned
#
# Notes:
# - PULL needs a NAS subject root. We try to infer it from SCRATCH/<fish>/{nas symlink|.nas_path}.
#   If SCRATCH subject doesn't exist yet (first-time pull), provide --owner or --nas-project-root.
# - PUSH does not overwrite on NAS unless --force.
#
# Requirements: bash, rsync, readlink, sed, stat

set -euo pipefail
IFS=$'\n\t'

# ------------------------ Defaults / ENV ------------------------
NAS_DEFAULT="${NAS:-}"    # May be empty; we prefer nas symlink/.nas_path or --nas-project-root/--owner
WORK_BASE="${WORK:-$HOME/WORK}/experiments"
SCRATCH_BASE="${SCRATCH:-/scratch/$USER}/experiments"

# ------------------------ Args ------------------------
MODE="both"        # pull | push | both
FORCE=0
DRY=0
WITH_MATRICES=0
WITH_LOGS=0
NAS_PROJECT_ROOT="${NAS_PROJECT_ROOT:-}"  # optional
OWNER=""

usage() {
  cat <<USAGE
Usage: $0 [--pull|--push] [--force] [--dry-run] [--with-matrices] [--with-logs]
          [--nas-project-root PATH] [--owner NAME]
          <fishID1> [fishID2 ...]

Modes (default: both):
  --pull              Only NAS -> WORK -> SCRATCH
  --push              Only SCRATCH -> WORK/_canonical -> NAS

Options:
  --force             Overwrite existing files/folders at destinations
  --dry-run           Print actions, do not write
  --with-matrices     On push, also copy transforms into NAS/transMatrices
  --with-logs         On push, also copy logs into NAS/logs
  --nas-project-root  Fallback NAS project root (e.g. "/nas/.../07 Data/Matilde")
  --owner NAME        Use \$NAS/NAME/<fish> as NAS subject root if not resolvable from SCRATCH

USAGE
  exit 1
}

args=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --pull) MODE="pull"; shift ;;
    --push) MODE="push"; shift ;;
    --force) FORCE=1; shift ;;
    --dry-run) DRY=1; shift ;;
    --with-matrices) WITH_MATRICES=1; shift ;;
    --with-logs) WITH_LOGS=1; shift ;;
    --nas-project-root) shift; NAS_PROJECT_ROOT="${1:-}"; [[ -n "$NAS_PROJECT_ROOT" ]] || { echo "ERR: --nas-project-root needs a path" >&2; exit 2; }; shift ;;
    --owner) shift; OWNER="${1:-}"; [[ -n "$OWNER" ]] || { echo "ERR: --owner needs a name" >&2; exit 2; }; shift ;;
    -h|--help) usage ;;
    --*) echo "ERR: unknown option $1" >&2; usage ;;
    *) args+=( "$1" ); shift ;;
  esac
done
[[ ${#args[@]} -ge 1 ]] || usage
FISH_IDS=( "${args[@]}" )

STAMP="$(date +%Y%m%d_%H%M%S)"
LOGROOT="$SCRATCH_BASE/batch_logs"
MASTER_LOG="$LOGROOT/sync_integrated_${STAMP}.log"
mkdir -p "$LOGROOT"

log(){ echo "[$(date -Iseconds)] $*" | tee -a "$MASTER_LOG"; }
run(){ if (( DRY )); then printf 'DRY:'; printf ' %q' "$@"; echo; else "$@"; fi; }

# rsync helper honoring DRY and FORCE
rsync_cp() {
  local add=( -a --no-owner --no-group --chmod=ugo=rwX )
  (( DRY )) && add+=( -n )
  (( FORCE )) || add+=( --ignore-existing )
  rsync "${add[@]}" "$@"
}

dir_has_files(){ shopt -s nullglob dotglob; local a=("$1"/*); shopt -u nullglob dotglob; (( ${#a[@]} > 0 )); }
ensure_dir(){ (( DRY )) && echo "DRY: mkdir -p $1" || mkdir -p "$1"; }

# ------------------------ Discovery helpers ------------------------

# Preferred subject dir under SCRATCH: experiments/subjects/<fish>
# Fallback: experiments/*/subjects/<fish> (pick the one with newest reg or any if none)
find_subject_dir() {
  local fish="$1"
  local top="$SCRATCH_BASE/subjects/$fish"
  [[ -d "$top" ]] && { printf '%s' "$top"; return 0; }

  mapfile -t cand < <(ls -d "$SCRATCH_BASE"/*/subjects/"$fish" 2>/dev/null || true)
  (( ${#cand[@]} )) || return 1
  if (( ${#cand[@]} == 1 )); then printf '%s' "${cand[0]}"; return 0; fi

  # Choose the one with the most recently modified reg/ (or reg_to_avg2p legacy)
  local best="" best_m=0 m
  for c in "${cand[@]}"; do
    if [[ -d "$c/reg" ]]; then m=$(stat -c %Y "$c/reg" 2>/dev/null || echo 0)
    elif [[ -d "$c/reg_to_avg2p" ]]; then m=$(stat -c %Y "$c/reg_to_avg2p" 2>/dev/null || echo 0)
    else m=0; fi
    if (( m > best_m )); then best_m=$m; best="$c"; fi
  done
  [[ -n "$best" ]] && { printf '%s' "$best"; return 0; }
  printf '%s' "${cand[0]}"
}

# Resolve NAS subject root (…/<fish>) from SCRATCH subject's nas symlink or .nas_path,
# or fallback to --nas-project-root/<fish>, or $NAS/<owner>/<fish>
resolve_nas_subject_root() {
  local subj="$1" fish="$2"
  local ln="$subj/nas" file="$subj/.nas_path" dest=""
  if [[ -L "$ln" ]]; then dest="$(readlink -f "$ln" || true)"; [[ -n "$dest" ]] && { printf '%s' "$dest"; return 0; }; fi
  if [[ -f "$file" ]]; then dest="$(<"$file")"; dest="${dest%%[$'\r\n']*}"; [[ -n "$dest" ]] && { printf '%s' "$dest"; return 0; }; fi
  if [[ -n "$NAS_PROJECT_ROOT" ]]; then printf '%s' "$NAS_PROJECT_ROOT/$fish"; return 0; fi
  if [[ -n "$NAS_DEFAULT" && -n "$OWNER" ]]; then printf '%s' "$NAS_DEFAULT/$OWNER/$fish"; return 0; fi
  return 1
}

json_get_key() {
  local file="$1" key="$2"
  [[ -f "$file" ]] || return 1
  sed -n -E "s/.*\"$key\"[[:space:]]*:[[:space:]]*\"([^\"]+)\".*/\1/p" "$file" | head -n1
}

resolve_nas_from_scratch_origin() {
  local scratch_subj="$1" fish="$2"
  local p_link="$scratch_subj/nas"
  local p_json="$scratch_subj/.origin.json"
  local p_txt="$scratch_subj/.nas_path"

  if [[ -L "$p_link" ]]; then
    local t; t="$(readlink -f "$p_link" || true)"
    [[ -n "$t" && -d "$t" ]] && { printf '%s' "$t"; return 0; }
  fi
  if [[ -f "$p_json" ]]; then
    local t; t="$(json_get_key "$p_json" nas_subject_root || true)"
    [[ -z "$t" ]] && t="$(json_get_key "$p_json" nas_fish_root || true)"
    [[ -n "$t" && -d "$t" ]] && { printf '%s' "$t"; return 0; }
  fi
  if [[ -f "$p_txt" ]]; then
    local t; t="$(head -n1 "$p_txt")"
    [[ -n "$t" && -d "$t" ]] && { printf '%s' "$t"; return 0; }
  fi
  # fallbacks if you insist (won't be needed when nas symlink exists)
  if [[ -n "$NAS_PROJECT_ROOT" ]]; then printf '%s' "$NAS_PROJECT_ROOT/$fish"; return 0; fi
  if [[ -n "$NAS_DEFAULT" && -n "$OWNER" ]]; then printf '%s' "$NAS_DEFAULT/$OWNER/$fish"; return 0; fi
  return 1
}

# ------------------------ PULL (NAS -> WORK -> SCRATCH) ------------------------

pull_one() {
  local fish="$1"
  log "=== PULL: $fish ==="

  # --- Resolve SCRATCH subject + NAS subject ---
  local SCR_SUBJ NAS_SUBJ PRE
  if SCR_SUBJ="$(find_subject_dir "$fish" 2>/dev/null)"; then
    : # found existing SCRATCH subject
  else
    SCR_SUBJ="$SCRATCH_BASE/subjects/$fish"   # will be created
  fi
  # Resolve NAS using SCRATCH hints (nas symlink / .origin.json / .nas_path) or fallbacks
  NAS_SUBJ="$(resolve_nas_subject_root "$SCR_SUBJ" "$fish" 2>/dev/null || true)"
  if [[ -z "${NAS_SUBJ:-}" || ! -d "$NAS_SUBJ" ]]; then
    log "  ERROR: Cannot resolve NAS subject root for $fish. Provide --nas-project-root or --owner + NAS."
    return 0
  fi
  PRE="$NAS_SUBJ/02_reg/00_preprocessing"
  if [[ ! -d "$PRE" ]]; then
    log "  WARN: ${PRE} missing; nothing to pull."
    return 0
  fi

  # --- Target dirs ---
  local WORK_SUBJ="$WORK_BASE/subjects/$fish"
  local WORK_PRE="$WORK_SUBJ/02_reg/00_preprocessing"
  local RAW="$SCR_SUBJ/raw" FIXED="$SCR_SUBJ/fixed" REG="$SCR_SUBJ/reg"

  # Ensure the subject roots exist (safe in dry-run: printed only)
  ensure_dir "$SCR_SUBJ" "$WORK_SUBJ"

  # --- 1) Mirror preprocessing to WORK (idempotent, no overwrite unless --force) ---
  ensure_dir "$WORK_PRE"
  for sub in 2p_anatomy r1 rn; do
    if [[ -d "$PRE/$sub" ]]; then
      ensure_dir "$WORK_PRE/$sub"
      rsync_cp "$PRE/$sub/" "$WORK_PRE/$sub/"
    else
      log "  INFO: no $sub in preprocessing"
    fi
  done

  # --- 2) Stage SCRATCH raw/fixed/reg from WORK ---
  ensure_dir "$RAW/anatomy_2P";    rsync_cp "$WORK_PRE/2p_anatomy/" "$RAW/anatomy_2P/" || true
  ensure_dir "$RAW/confocal_round1"; rsync_cp "$WORK_PRE/r1/"         "$RAW/confocal_round1/" || true
  ensure_dir "$RAW/confocal_round2"; rsync_cp "$WORK_PRE/rn/"         "$RAW/confocal_round2/" || true
  ensure_dir "$FIXED" "$REG/logs"

  # Fixed references -> canonical names (skip if already present unless --force)
  shopt -s nullglob
  local g1=( "$RAW/anatomy_2P/"*GCaMP*.nrrd )
  local g2=( "$RAW/confocal_round1/"*GCaMP*.nrrd )
  shopt -u nullglob
  if (( FORCE )) || [[ ! -f "$FIXED/anatomy_2P_ref_GCaMP.nrrd" ]]; then
    [[ ${#g1[@]} -gt 0 ]] && rsync_cp "${g1[0]}" "$FIXED/anatomy_2P_ref_GCaMP.nrrd"
  fi
  if (( FORCE )) || [[ ! -f "$FIXED/round1_ref_GCaMP.nrrd" ]]; then
    [[ ${#g2[@]} -gt 0 ]] && rsync_cp "${g2[0]}" "$FIXED/round1_ref_GCaMP.nrrd"
  fi

  # --- 3) Record origin on SCRATCH for future pushes (owner-aware) ---
  if (( ! DRY )); then
    ln -sfn "$NAS_SUBJ" "$SCR_SUBJ/nas"
    cat > "$SCR_SUBJ/.origin.json" <<JSON
{
  "fish_id": "$fish",
  "nas_subject_root": "$NAS_SUBJ",
  "work_subject_root": "$WORK_SUBJ",
  "scratch_subject_root": "$SCR_SUBJ",
  "created_utc": "$(date -u +"%Y-%m-%dT%H:%M:%SZ")",
  "created_by": "${USER:-unknown}",
  "script": "sync_nas_compute_integrated.sh"
}
JSON
  fi

  log "  ✔ Pull completed for $fish"
}

# ------------------------ Canonicalize (SCRATCH -> WORK/_canonical) ------------------------

canonicalize_images_from_scratch() {
  local fish="$1"
  local SCR_SUBJ
  if ! SCR_SUBJ="$(find_subject_dir "$fish" 2>/dev/null)"; then
    log "  WARN: no SCRATCH subject for $fish; skip canonicalization"
    return 0
  fi
  local REG="$SCR_SUBJ/reg"
  if [[ ! -d "$REG" ]]; then
    log "  INFO: $REG missing; nothing to canonicalize"
    return 0
  fi

  log "=== CANONICALIZE: $fish ==="
  local CANON="$WORK_BASE/subjects/$fish/02_reg/_canonical"
  local STAGE="$WORK_BASE/subjects/$fish/02_reg/_staging"
  ensure_dir "$CANON" "$STAGE"

  # 1) Gather candidates directly from SCRATCH/reg (recursive)
  shopt -s nullglob globstar
  local imgs=( "$REG"/**/*.nrrd "$REG"/*.nrrd )
  shopt -u globstar
  if (( ${#imgs[@]} == 0 )); then
    log "  INFO: no *.nrrd under $REG (nothing to canonicalize)"
    return 0
  fi

  # 2) Optionally stage-copy (no harm if dry-run no-ops)
  rsync_cp "${imgs[@]}" "$STAGE/"

  # 3) Canonicalize from the imgs list (not from _staging)
  local made=0
  for f in "${imgs[@]}"; do
    local base out
    base="$(basename "$f")"
    out="$base"

    # already canonical? just ensure fish prefix
    if [[ "$out" =~ _in_(ref|2p|r1)\.nrrd$ ]]; then
      [[ "$out" == ${fish}_* ]] || out="${fish}_$out"
    else
      # normalize common patterns to canonical suffixes
      out="${out/_to_ref__aligned/_in_ref}"
      out="${out/_to_ref_aligned/_in_ref}"
      out="$(echo "$out" | sed -E 's/_to_ref(_[^.]*)?_aligned\.nrrd$/_in_ref.nrrd/')"
      out="$(echo "$out" | sed -E 's/_aligned_2P\.nrrd$/_in_2p.nrrd/')"
      out="$(echo "$out" | sed -E 's/_to_2p([^.]*)?_aligned\.nrrd$/_in_2p.nrrd/')"
      out="$(echo "$out" | sed -E 's/_to_r1([^.]*)?_aligned\.nrrd$/_in_r1.nrrd/')"
      [[ "$out" == ${fish}_* ]] || out="${fish}_$out"
      # last resort: make it ref
      [[ "$out" =~ _in_(ref|2p|r1)\.nrrd$ ]] || out="${out%.nrrd}_in_ref.nrrd"
    fi

    local dst="$CANON/$out"
    if (( FORCE )) || [[ ! -f "$dst" ]]; then
      rsync_cp "$f" "$dst"
      log "  + $base -> $(basename "$dst")"
      ((made++))
    else
      log "  SKIP existing canonical: $(basename "$dst")"
    fi
  done

  (( made > 0 )) || log "  INFO: canonicalization produced no new files for $fish"
}

# ------------------------ PUBLISH (WORK/_canonical -> NAS) ------------------------

publish_canonical_to_nas() {
  local fish="$1"
  local SCR_SUBJ
  if ! SCR_SUBJ="$(find_subject_dir "$fish" 2>/dev/null)"; then
    log "  WARN: cannot publish $fish (no SCRATCH subject)"; return 0
  fi

  local NAS_SUBJ
  if ! NAS_SUBJ="$(resolve_nas_from_scratch_origin "$SCR_SUBJ" "$fish")"; then
    log "  WARN: cannot resolve NAS for $fish from SCRATCH origin; skip publish"; return 0
  fi

  local CANON="$WORK_BASE/subjects/$fish/02_reg/_canonical"
  if [[ ! -d "$CANON" ]]; then
    log "  INFO: no canonical dir for $fish; skip publish"
    return 0
  fi

  log "=== PUBLISH: $fish ==="
  local ROOT="$NAS_SUBJ/02_reg"
  ensure_dir "$ROOT"

  stage_dirs() {
    local s="$1"
    ensure_dir "$ROOT/$s" "$ROOT/$s/aligned" "$ROOT/$s/transMatrices" "$ROOT/$s/logs"
  }

  shopt -s nullglob
  for f in "$CANON/"*.nrrd; do
    local bn dest stage sub="aligned"
    bn="$(basename "$f")"

    if [[ "$bn" == "${fish}_anatomy_2P_in_ref.nrrd" ]]; then
      stage="08_2pa-ref"; sub="aligned"
    elif [[ "$bn" =~ _round1_.*_in_2p\.nrrd$ ]]; then
      stage="01_r1-2p"; sub="aligned"
    elif [[ "$bn" =~ _round2_.*_in_r1\.nrrd$ ]]; then
      stage="02_rn-r1"; sub="aligned"
    elif [[ "$bn" =~ _round2_.*_in_2p\.nrrd$ ]]; then
      stage="03_rn-2p"; sub="aligned"
    elif [[ "$bn" =~ _round1_.*_in_ref\.nrrd$ ]]; then
      stage="04_r1-ref"; sub="."
    elif [[ "$bn" =~ _round2_.*_in_ref\.nrrd$ ]]; then
      stage="05_r2-ref"; sub="."
    else
      log "  WARN: no stage mapping for $bn (skipped)"; continue
    fi

    stage_dirs "$stage"
    if [[ "$sub" == "." ]]; then
      dest="$ROOT/$stage/$bn"
    else
      dest="$ROOT/$stage/$sub/$bn"
    fi

    if (( FORCE )) || [[ ! -f "$dest" ]]; then
      rsync_cp "$f" "$dest"
      log "  → $stage/${sub/./(root)}/$bn"
    else
      log "  SKIP (exists): $stage/${sub/./(root)}/$bn"
    fi
  done
  shopt -u nullglob

  # Optional: transforms/logs from SCRATCH/reg -> NAS/transMatrices|logs
  if (( WITH_MATRICES || WITH_LOGS )); then
    local REG="$SCR_SUBJ/reg"
    if [[ -d "$REG" ]]; then
      (( WITH_MATRICES )) && {
        ensure_dir "$ROOT/04_r1-ref/transMatrices"
        rsync_cp "$REG/"*GenericAffine.mat "$ROOT/04_r1-ref/transMatrices/" || true
        rsync_cp "$REG/"*Warp.nii.gz "$ROOT/04_r1-ref/transMatrices/" || true
        rsync_cp "$REG/"*InverseWarp.nii.gz "$ROOT/04_r1-ref/transMatrices/" || true
      }
      (( WITH_LOGS )) && {
        ensure_dir "$ROOT/04_r1-ref/logs"
        rsync_cp "$REG/logs/" "$ROOT/04_r1-ref/logs/" || true
      }
    fi
  fi

  log "  ✔ Publish completed for $fish"
}

# ------------------------ Main ------------------------

echo "Compute roots:
  WORK   : $WORK_BASE
  SCRATCH: $SCRATCH_BASE
Mode     : $MODE   Force=$FORCE  DryRun=$DRY  Matrices=$WITH_MATRICES Logs=$WITH_LOGS
Log file : $MASTER_LOG"

for fish in "${FISH_IDS[@]}"; do
  [[ "$MODE" == "pull" || "$MODE" == "both" ]] && pull_one "$fish"
  # Push flow (your requested order):
  # 1) copy from SCRATCH -> WORK/_staging
  # 2) rename to canonical in WORK/_canonical
  # 3) check NAS & copy if missing
  canonicalize_images_from_scratch "$fish"
  [[ "$MODE" == "push" || "$MODE" == "both" ]] && publish_canonical_to_nas "$fish"
done

log "All done."
