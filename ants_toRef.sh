#!/usr/bin/env bash
# ants_register_to_avg2p.sh  (interactive; single job for multiple fish)
set -euo pipefail

ANTSPATH="${ANTSPATH:-$HOME/ANTs/antsInstallExample/install/bin}"
export ANTSPATH

# Default average 2P reference (used only when role=avg_2p or legacy mode)
REF_AVG_2P="${REF_AVG_2P:-/scratch/ddharmap/refBrains/ref_05_LB_Perrino_2p/average_2p.nrrd}"

# Fixed manifest location (no flags)
[[ -n "${SCRATCH:-}" ]] || { echo "ERROR: SCRATCH env not set."; exit 2; }
MANIFEST_DIR="${MANIFEST_DIR:-${NAS:-}/Danin/regManifest}"
MANIFEST_CSV="${MANIFEST_CSV:-$MANIFEST_DIR/regManifest.csv}"

WALL_TIME="24:00:00"
MAIL_TYPE="${MAIL_TYPE:-END,FAIL}"
MAIL_USER="${MAIL_USER:-danin.dharmaperwira@unil.ch}"

# CLI flags (only --dry-run / -n supported; everything else is ignored)
DRY_RUN=0
for arg in "$@"; do
  case "$arg" in
    --dry-run|-n) DRY_RUN=1 ;;
    *) ;;  # ignore unknown args to keep things simple
  esac
done

read -rp "PARTITION (e.g., test | cpu | normal | gpu | other): " PARTITION

# -------- Manifest discovery (with clear feedback) --------
# Search order: explicit MANIFEST_CSV -> MANIFEST_DIR/regManifest.csv -> $SCRATCH/regManifest.csv -> $SCRATCH/experiments/_manifests/register_pairs.csv
CANDIDATES=()
[[ -n "${MANIFEST_CSV:-}" ]] && CANDIDATES+=("$MANIFEST_CSV")
[[ -n "${MANIFEST_DIR:-}" ]] && CANDIDATES+=("$MANIFEST_DIR/regManifest.csv")
CANDIDATES+=("$SCRATCH/regManifest.csv" "$SCRATCH/experiments/_manifests/register_pairs.csv")

FOUND_MANIFEST=""
for p in "${CANDIDATES[@]}"; do
  if [[ -n "$p" && -f "$p" ]]; then FOUND_MANIFEST="$p"; break; fi
done

if [[ -n "$FOUND_MANIFEST" ]]; then
  echo "Manifest: FOUND at $FOUND_MANIFEST"
else
  echo "Manifest: NOT FOUND. Looked in:"
  for p in "${CANDIDATES[@]}"; do echo "  - $p"; done
fi

# Only ask for fish if NO manifest is present (legacy behavior)
FISH_IDS=()
if [[ -z "$FOUND_MANIFEST" ]]; then
  read -rp "Fish IDs (space-separated): " FISH_LINE
  FISH_IDS=( $FISH_LINE )
  # In legacy mode we need REF_AVG_2P
  [[ -f "$REF_AVG_2P" ]] || { echo "ERROR: Average 2P reference not found: $REF_AVG_2P"; exit 2; }
fi

if [[ "$PARTITION" == "test" ]]; then
  QUEUE="interactive"; CPUS=1; MEM="8G"; TIME="00:30:00"
  echo "==> TEST mode: interactive (1 CPU, 8G, 30m)"
else
  QUEUE="$PARTITION"
  CPUS="${CPUS:-48}"
  MEM="${MEM:-256G}"
  TIME="$WALL_TIME"
fi

JOBDIR="$SCRATCH/experiments/_jobs"
mkdir -p "$JOBDIR"
STAMP="$(date +%Y%m%d_%H%M%S)"
JOB="$JOBDIR/ants_to_avg2p_${STAMP}.sh"

# If manifest exists: copy it for provenance and compute checksum
JOB_MANIFEST=""
JOB_MANIFEST_SHA=""
if [[ -n "$FOUND_MANIFEST" ]]; then
  JOB_MANIFEST="$JOBDIR/manifest_${STAMP}.csv"
  cp -f "$FOUND_MANIFEST" "$JOB_MANIFEST"
  if command -v sha256sum >/dev/null 2>&1; then
    JOB_MANIFEST_SHA="$(sha256sum "$JOB_MANIFEST" | awk '{print $1}')"
  elif command -v shasum >/dev/null 2>&1; then
    JOB_MANIFEST_SHA="$(shasum -a 256 "$JOB_MANIFEST" | awk '{print $1}')"
  fi
fi

cat > "$JOB" <<'EOS'
#!/usr/bin/env bash
set -euo pipefail

ANTSPATH="${ANTSPATH:-$HOME/ANTs/antsInstallExample/install/bin}"
export ANTSPATH
export ITK_GLOBAL_DEFAULT_NUMBER_OF_THREADS="${SLURM_CPUS_PER_TASK:-1}"
export OMP_NUM_THREADS="${SLURM_CPUS_PER_TASK:-1}"

SCRATCH_BASE="${SCRATCH:?SCRATCH env not set}"

# Inputs injected at submit time
REF_AVG_2P_DEFAULT="__REF_AVG_2P__"
REF_AVG_2P="${REF_AVG_2P:-$REF_AVG_2P_DEFAULT}"

MANIFEST_CSV="__MANIFEST_CSV__"
MANIFEST_SHA256="__MANIFEST_SHA256__"
DRY_RUN="__DRY_RUN__"

echo "ANTs bin : $ANTSPATH"
echo "Threads  : ${SLURM_CPUS_PER_TASK:-1}"
if [[ -n "${MANIFEST_CSV}" && -f "${MANIFEST_CSV}" ]]; then
  echo "Mode     : CSV"
  echo "Manifest : ${MANIFEST_CSV} (sha256: ${MANIFEST_SHA256})"
else
  echo "Mode     : Legacy interactive (2P -> avg_2p)"
fi
echo "Dry-run  : ${DRY_RUN}"

# ---------- helpers ----------
register_pair() {
  # Usage: register_pair <fixed> <moving> <outprefix> <logfile>
  local fx="$1" mv="$2" op="$3" log="${4:-/dev/null}"

  if [[ "$DRY_RUN" == "1" ]]; then
    {
      echo "[DRY-RUN] antsRegistration"
      echo "  - fixed : $fx"
      echo "  - moving: $mv"
      echo "  - out   : $op"
      echo "$ANTSPATH/antsRegistration -d 3 --float 1 --verbose 1 -o [$op,${op}_aligned.nrrd] ..."
    } >"$log" 2>&1
    return 0
  fi

  {
    echo "antsRegistration -> $op"
    "$ANTSPATH/antsRegistration" \
      -d 3 --float 1 --verbose 1 \
      -o ["$op","${op}_aligned.nrrd"] \
      --interpolation WelchWindowedSinc \
      --winsorize-image-intensities [0.05,0.95] \
      --use-histogram-matching 1 \
      -r ["$fx","$mv",1] \
      -t Rigid[0.1] \
        -m MI["$fx","$mv",1,32,Regular,0.25] \
        -c [200x200x200x0,1e-8,10] \
        --shrink-factors 12x8x4x2 \
        --smoothing-sigmas 4x3x2x1vox \
      -t Affine[0.1] \
        -m MI["$fx","$mv",1,32,Regular,0.25] \
        -c [200x200x200x0,1e-8,10] \
        --shrink-factors 12x8x4x2 \
        --smoothing-sigmas 4x3x2x1vox \
      -t SyN[0.25,6,0.1] \
        -m CC["$fx","$mv",1,4] \
        -c [200x200x200x200x10,1e-7,10] \
        --shrink-factors 12x8x4x2x1 \
        --smoothing-sigmas 4x3x2x1x0vox
  } >"$log" 2>&1
}

# Map roles to paths (minimal set; tweak confocal path if needed)
resolve_role_path() {
  local fish="$1"
  local role="${2,,}"  # lowercase

  # helper: return first existing file from a candidate list
  _first_existing() {
    local f
    for f in "$@"; do
      [[ -f "$f" ]] && { echo "$f"; return 0; }
    done
    echo ""
    return 1
  }

  case "$role" in
    anatomy_2p)
      # Prefer new raw/anatomy_2P layout, fall back to legacy fixed/
      _first_existing \
        "$SCRATCH_BASE/experiments/subjects/$fish/raw/anatomy_2P/${fish}_anatomy_2P_GCaMP.nrrd" \
        "$SCRATCH_BASE/experiments/subjects/$fish/fixed/anatomy_2P_ref_GCaMP.nrrd"
      ;;

    avg_2p)
      echo "$REF_AVG_2P"
      ;;

    confocal_r*|confocal_round*)
      # Support any round number in a consistent layout.
      local round=""
      if [[ "$role" =~ ^confocal_r([0-9]+)$ ]]; then
        round="${BASH_REMATCH[1]}"
      elif [[ "$role" =~ ^confocal_round([0-9]+)$ ]]; then
        round="${BASH_REMATCH[1]}"
      fi

      if [[ -z "$round" ]]; then
        echo ""
        return 2
      fi

      local dir="$SCRATCH_BASE/experiments/subjects/$fish/raw/confocal_round${round}"
      _first_existing \
        "$dir/${fish}_round${round}_channel1_GCaMP.nrrd" \
        "$dir/${fish}_round${round}_*GCaMP*.nrrd" \
        "$dir/round${round}_*GCaMP*.nrrd" \
        "$dir/*GCaMP*.nrrd"
      ;;

    *)
      echo ""
      return 2
      ;;
  esac
}

# Write a resolved manifest for provenance
RESOLVED="$SCRATCH_BASE/experiments/_jobs/manifest_resolved_$(date +%Y%m%d_%H%M%S).csv"
echo "row_idx,moving_fish,moving_role,fixed_fish,fixed_role,moving_path,fixed_path,output_prefix,status" > "$RESOLVED"

# ---------- CSV mode ----------
if [[ -n "${MANIFEST_CSV}" && -f "${MANIFEST_CSV}" ]]; then
  echo "===== CSV mode: reading ${MANIFEST_CSV} ====="

    row=0
  # Read raw lines so we can normalize BOM/CRLF and support tabs; also process last line w/o newline.
  while IFS= read -r raw || [[ -n "$raw" ]]; do
    # Normalize:
    #  - strip UTF-8 BOM if present
    #  - drop \r (Windows CRLF)
    #  - treat tabs as commas (just in case)
    raw="${raw#$'\ufeff'}"
    raw="${raw//$'\r'/}"
    raw="${raw//$'\t'/,}"

    # Skip blanks and comments
    [[ -z "${raw//[ ,]/}" ]] && continue
    [[ "${raw:0:1}" == "#" ]] && continue

    # Split into 6 fields
    IFS=',' read -r moving_fish moving_role fixed_fish fixed_role mov_override fix_override <<< "$raw"

    # Skip header row
    if [[ "${moving_fish,,}" == "moving_fish_id" ]]; then
      continue
    fi

    row=$((row+1))
    local_mrole="${moving_role,,}"
    local_frole="${fixed_role,,}"

    # Resolve moving
    if [[ -n "${mov_override:-}" ]]; then
      MOV="$mov_override"
    else
      MOV="$(resolve_role_path "$moving_fish" "$local_mrole")"
    fi

    # Resolve fixed
    fixed_bucket=""
    if [[ -n "${fix_override:-}" ]]; then
      FIX="$fix_override"; fixed_bucket="${fixed_fish:-override}"
    else
      if [[ "$local_frole" == "avg_2p" ]]; then
        FIX="$REF_AVG_2P"; fixed_bucket="global"
      else
        if [[ -z "${fixed_fish:-}" ]]; then fixed_fish="$moving_fish"; fi
        FIX="$(resolve_role_path "$fixed_fish" "$local_frole")"
        fixed_bucket="$fixed_fish"
      fi
    fi

    echo "----- Row $row -----"
    echo "  Moving [$local_mrole] : $moving_fish -> $MOV"
    echo "  Fixed  [$local_frole] : ${fixed_bucket} -> $FIX"

    status="OK"
    [[ -z "${MOV:-}" || ! -f "$MOV" ]] && { echo "ERROR: Missing MOVING file: $MOV" >&2; status="ERROR_MOVING"; }
    [[ -z "${FIX:-}" || ! -f "$FIX" ]] && { echo "ERROR: Missing FIXED file: $FIX" >&2; status="${status},ERROR_FIXED"; }

    # Output prefix (unchanged)
    if [[ "$local_mrole" == "anatomy_2p" && "$local_frole" == "avg_2p" ]]; then
      outprefix="$SCRATCH_BASE/experiments/subjects/$moving_fish/reg_to_avg2p/2P_to_avg2p_"
    else
      outprefix="$SCRATCH_BASE/experiments/subjects/$moving_fish/reg/${local_mrole}_to_${local_frole}/${fixed_bucket}/${moving_fish}__to__${fixed_bucket}__${local_mrole}_to_${local_frole}_"
    fi
    echo "$row,$moving_fish,$local_mrole,$fixed_bucket,$local_frole,$MOV,$FIX,$outprefix,$status" >> "$RESOLVED"

    if [[ "$status" == "OK" ]]; then
      echo "REG: ${moving_fish} ${local_mrole} -> ${fixed_bucket} ${local_frole}"
    fi

    [[ "$status" == "OK" ]] || continue

    # Run (unchanged)
    if [[ "$local_mrole" == "anatomy_2p" && "$local_frole" == "avg_2p" ]]; then
      REGDIR="$SCRATCH_BASE/experiments/subjects/$moving_fish/reg_to_avg2p"
      LOGDIR="$REGDIR/logs"; mkdir -p "$LOGDIR"
      OP="$outprefix"
      if ! register_pair "$FIX" "$MOV" "$OP" "$LOGDIR/2P_to_avg2p.log"; then
        echo "ERROR: Registration failed (2P->avg2p) for $moving_fish. See $LOGDIR/2P_to_avg2p.log" >&2
        continue
      fi
      if [[ "$DRY_RUN" != "1" && -f "${OP}_aligned.nrrd" ]]; then
        cp -f "${OP}_aligned.nrrd" "$REGDIR/anatomy_2P_in_avg2p.nrrd"
      fi
    else
      REGDIR="$(dirname "$outprefix")"
      LOGDIR="$REGDIR/logs"; mkdir -p "$LOGDIR"
      if ! register_pair "$FIX" "$MOV" "$outprefix" "$LOGDIR/main.log"; then
        echo "ERROR: Registration failed for $moving_fish ($local_mrole) → ${fixed_bucket} ($local_frole). See $LOGDIR/main.log" >&2
        continue
      fi
    fi

    echo "OK: $moving_fish ($local_mrole) → ${fixed_bucket} ($local_frole)"
  done < "$MANIFEST_CSV"


  echo "Resolved manifest written: $RESOLVED"
  exit 0
fi

# ---------- Legacy interactive fallback (2P -> avg_2p) ----------
ALIGN_ROUNDS="${ALIGN_ROUNDS:-0}"   # 0 = off (default)

while IFS= read -r FISH; do
  [[ -n "$FISH" ]] || continue
  echo "===== Processing $FISH (Legacy 2P -> avg_2p) ====="

  BASE="$SCRATCH_BASE/experiments/subjects/$FISH"
  FIXEDDIR="$BASE/fixed"
  REGDIR="$BASE/reg_to_avg2p"
  LOGDIR="$REGDIR/logs"
  mkdir -p "$REGDIR" "$LOGDIR"

  MOV="$FIXEDDIR/anatomy_2P_ref_GCaMP.nrrd"
  FIX="$REF_AVG_2P"

  echo "  Moving (anatomy_2p): $MOV"
  echo "  Fixed  (avg_2p)    : $FIX"

  OP="$REGDIR/2P_to_avg2p_"
  if ! register_pair "$FIX" "$MOV" "$OP" "$LOGDIR/2P_to_avg2p.log"; then
    echo "ERROR: 2P->avg registration failed for $FISH (see $LOGDIR/2P_to_avg2p.log). Skipping fish." >&2
    continue
  fi

  if [[ "$DRY_RUN" != "1" && -f "${OP}_aligned.nrrd" ]]; then
    cp -f "${OP}_aligned.nrrd" "$REGDIR/anatomy_2P_in_avg2p.nrrd"
  fi

  if [[ "$ALIGN_ROUNDS" == "1" ]]; then
    echo "  Rounds alignment ENABLED (not implemented here)."
  else
    echo "  Rounds alignment DISABLED."
  fi

  echo "===== Done $FISH ====="
done <<'FISH_EOF'
__FISH_LIST__
FISH_EOF
EOS

# Inject placeholders
sed -i "s|__REF_AVG_2P__|$REF_AVG_2P|g" "$JOB"
sed -i "s|__MANIFEST_CSV__|${JOB_MANIFEST}|g" "$JOB"
sed -i "s|__MANIFEST_SHA256__|${JOB_MANIFEST_SHA}|g" "$JOB"
sed -i "s|__DRY_RUN__|${DRY_RUN}|g" "$JOB"

# Inject fish list only if NO manifest (legacy)
if [[ -z "${JOB_MANIFEST}" ]]; then
  tmpfish="$(mktemp)"
  printf '%s\n' "${FISH_IDS[@]}" > "$tmpfish"
  sed -i -e "/__FISH_LIST__/{
    r $tmpfish
    d
  }" "$JOB"
  rm -f "$tmpfish"
fi

chmod +x "$JOB"

# Submit or run
if [[ "${PARTITION}" == "test" ]]; then
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
  echo "Submitted job."
  echo "  Job script: $JOB"
  if [[ -n "${JOB_MANIFEST}" ]]; then
    echo "  Manifest snapshot: $JOB_MANIFEST (sha256: ${JOB_MANIFEST_SHA})"
  fi
  echo "  Logs: $JOBDIR/ants_to_avg2p_${STAMP}.{out,err}"
fi
