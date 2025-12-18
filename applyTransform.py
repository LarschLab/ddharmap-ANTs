#!/usr/bin/env python3
"""
applyTransform.py

Two modes:
- Default scan mode: prompt for owner/fish, read best_rounds.csv to pick the anchor round, find non-GCaMP HCR .nrrd
  files in 02_reg/00_preprocessing/{rbest,rn}, and apply best->2P, rn->best, and rn->2P (chained) transforms discovered
  under 02_reg.
- Manifest mode: read an explicit CSV and apply exactly the transform chains you list (supports chaining like r1->r2->2p).

CLI:
    python applyTransform.py [--force] [--dry-run] [--mode scan|manifest] [--manifest-csv PATH]

Manifest CSV schema (headers required unless noted):
    moving,reference,transforms[,output,fish_id,label]
      moving      : path to moving image
      reference   : path to reference image (used as output grid if USE_REFERENCE_GRID is True)
      transforms  : semicolon-separated list of transforms in antsApplyTransforms order
                    (e.g., r2->2p warp; r2->2p affine; r1->r2 warp; r1->r2 affine)
      output      : optional full path for the output; if missing/blank, the result is written next to the moving file with suffix "_transformed.nrrd"
      fish_id     : optional, for logging
      label       : optional, for logging

Dependencies: antspyx (ants), tqdm, pandas
"""

import os
import sys
import getpass
import socket
import traceback
from pathlib import Path
import ants  # ANTsPy (antspyx) is required
from tqdm import tqdm
import pandas as pd
import datetime
import concurrent.futures
import multiprocessing
import argparse
import re

def prompt_owner_and_fishid(nas_root):
    """
    Prompt user for owner and fishid, print available options, and return (owner, fishids).
    """
    nas_root = Path(nas_root)
    owners = [d.name for d in nas_root.iterdir() if d.is_dir() and not d.name.startswith('.')]
    print("Available owners:")
    for o in owners:
        print(f"  - {o}")
    owner = input("Enter owner (as listed above): ").strip()
    if owner not in owners:
        print(f"[ERROR] Owner '{owner}' not found in {nas_root}. Exiting.")
        exit(1)
    fish_root = get_owner_root(nas_root, owner)
    fishids = sorted([d.name for d in fish_root.iterdir() if d.is_dir() and not d.name.startswith('.')], key=lambda x: (''.join([c.zfill(10) if c.isdigit() else c for c in x])))
    print(f"Available fish IDs for owner '{owner}':")
    for f in fishids:
        print(f"  - {f}")
    fishid_input = input("Enter fish ID(s) to process (space-separated, or 'all' for all fish): ").strip()
    if fishid_input == 'all':
        selected_fishids = fishids
    else:
        requested = fishid_input.split()
        not_found = [f for f in requested if f not in fishids]
        if not_found:
            print(f"[ERROR] Fish ID(s) not found for owner '{owner}': {', '.join(not_found)}. Exiting.")
            exit(1)
        selected_fishids = requested
    return owner, selected_fishids


# ========= USER CONFIGURABLE PARAMETERS =========
NAS_ROOT = "/Volumes/jlarsch/default/D2c/07_Data"  # Root NAS directory
MANIFEST_CSV_DEFAULT = "/Volumes/jlarsch/default/D2c/07_Data/Danin/regManifest/transformManifest.csv"
BEST_ROUNDS_CSV = "/Volumes/jlarsch/default/D2c/07_Data/Danin/best_rounds.csv"
USE_REFERENCE_GRID = True                          # Use reference image as output grid
INTERPOLATOR = "welchWindowedSinc"                # Interpolator for ANTs
# =================================================

def find_fish_dirs(nas_root, owner):
    """
    Returns a list of all fish directories for the given owner.
    Skips hidden directories.
    """
    fish_root = get_owner_root(nas_root, owner)
    return [d for d in fish_root.iterdir() if d.is_dir() and not d.name.startswith('.')]

def parse_round_val(val, default):
    """
    Convert round strings like 'r2' or '2' to an int. Return default on failure.
    """
    try:
        if isinstance(val, (int, float)):
            return int(val)
        if isinstance(val, str):
            digits = re.findall(r"\d+", val)
            if digits:
                return int(digits[0])
    except Exception:
        pass
    return default

def load_best_rounds(csv_path):
    """
    Load best_rounds.csv mapping fish_id -> {best_round, num_rounds, owner}.
    Returns a dict keyed by fish_id.
    """
    csv_path = Path(csv_path)
    if not csv_path.exists():
        print(f"[WARN] best_rounds.csv not found at {csv_path}; falling back to round1 defaults.")
        return {}
    df = pd.read_csv(csv_path, dtype=str, encoding="utf-8-sig")
    best_map = {}
    for _, row in df.iterrows():
        fish_id = str(row.get("fish_id", "")).strip()
        if not fish_id:
            continue
        best_round = parse_round_val(row.get("best_round", "1"), 1)
        num_rounds = parse_round_val(row.get("num_rounds", best_round), best_round)
        owner = str(row.get("owner", "")).strip()
        best_map[fish_id] = {
            "best_round": best_round,
            "num_rounds": num_rounds,
            "owner": owner
        }
    return best_map

def get_round_dir(preproc_dir, round_num, best_round):
    """
    Resolve the preprocessing directory for a given round under the new layout.
    Best round uses 'rbest'; others use 'rn'.
    """
    return preproc_dir / ("rbest" if round_num == best_round else "rn")

def find_hcr_channels(preproc_dir, fish_id, round_num, best_round):
    """
    Finds all non-GCaMP HCR channel .nrrd files for a given fish and round in the new layout.
    """
    round_dir = get_round_dir(preproc_dir, round_num, best_round)
    if not round_dir.exists():
        return []
    return [f for f in round_dir.glob(f"{fish_id}_round{round_num}_channel*.nrrd") if "GCaMP" not in f.name]

def find_best_to_2p_transforms(reg_dir, fish_id, best_round):
    """
    Find best->2P transforms under 01_rbest-2p/transMatrices.
    """
    stg = "01_rbest-2p"
    tm_dir = reg_dir / stg / "transMatrices"
    if not tm_dir.exists():
        return None, None, stg
    affine = list(tm_dir.glob(f"{fish_id}_round{best_round}_GCaMP_to_2p_0GenericAffine.mat"))
    warp = list(tm_dir.glob(f"{fish_id}_round{best_round}_GCaMP_to_2p_1Warp.nii.gz"))
    if affine and warp:
        return affine[0], warp[0], stg
    return None, None, stg

def find_round_to_best_transforms(reg_dir, fish_id, round_num, best_round):
    """
    Find round->best transforms under 02_rn-rbest/transMatrices.
    """
    stg = "02_rn-rbest"
    tm_dir = reg_dir / stg / "transMatrices"
    if not tm_dir.exists():
        return None, None, stg
    affine = list(tm_dir.glob(f"{fish_id}_round{round_num}_GCaMP_to_r{best_round}_0GenericAffine.mat"))
    warp = list(tm_dir.glob(f"{fish_id}_round{round_num}_GCaMP_to_r{best_round}_1Warp.nii.gz"))
    if affine and warp:
        return affine[0], warp[0], stg
    return None, None, stg

def find_reference_2p(preproc_dir, fish_id):
    """
    Anatomy 2P reference for best->2P.
    """
    ref = preproc_dir / "2p_anatomy" / f"{fish_id}_anatomy_2P_GCaMP.nrrd"
    return ref if ref.exists() else None

def find_best_round_reference(preproc_dir, fish_id, best_round):
    """
    Best-round GCaMP reference used for rn->best transforms.
    """
    round_dir = get_round_dir(preproc_dir, best_round, best_round)
    candidates = [
        round_dir / f"{fish_id}_round{best_round}_channel1_GCaMP.nrrd",
        round_dir / f"{fish_id}_round{best_round}_GCaMP.nrrd",
    ]
    chan_glob = list(round_dir.glob(f"{fish_id}_round{best_round}_channel*_GCaMP*.nrrd"))
    candidates.extend(chan_glob)
    for cand in candidates:
        if cand.exists():
            return cand
    return None

def normalize_gene(gene_raw):
    """
    Turn gene_probe into gene.probe if possible.
    """
    gene = gene_raw
    m_gene = re.match(r"(.+)_([0-9]+)$", gene)
    if m_gene:
        gene = f"{m_gene.group(1)}.{m_gene.group(2)}"
    return gene

def list_round_channels(preproc_dir, fish_id, round_num, best_round):
    """
    Enumerate expected non-GCaMP channels for a round (from preprocessing).
    Returns list of dicts with channel, gene, path, mtime.
    """
    round_dir = get_round_dir(preproc_dir, round_num, best_round)
    if not round_dir.exists():
        return []
    channels = []
    for f in round_dir.glob(f"{fish_id}_round{round_num}_channel*.nrrd"):
        if "GCaMP" in f.name:
            continue
        m = re.search(r"_channel(\d+)_", f.name)
        if not m:
            continue
        ch_num = int(m.group(1))
        base = f.stem
        try:
            gene_part = base.split(f"channel{ch_num}_", 1)[1]
        except Exception:
            gene_part = ""
        gene = normalize_gene(gene_part)
        channels.append({
            "channel": ch_num,
            "gene": gene,
            "path": f,
            "mtime": f.stat().st_mtime
        })
    # Deduplicate by channel, keep first (sorted by channel)
    seen = set()
    deduped = []
    for entry in sorted(channels, key=lambda x: x["channel"]):
        if entry["channel"] in seen:
            continue
        seen.add(entry["channel"])
        deduped.append(entry)
    return deduped

def find_output_for_channel(aligned_dir, fish_id, round_num, ch_num, suffix):
    """
    Find an aligned output for a specific channel/round/suffix (e.g., suffix='2p' or 'r2').
    Returns (Path or None, gene, mtime).
    """
    pattern = f"{fish_id}_round{round_num}_channel{ch_num}_*_in_{suffix}.nrrd"
    matches = list(aligned_dir.glob(pattern))
    if not matches:
        return None, "", None
    out_file = max(matches, key=lambda f: f.stat().st_mtime)
    base = out_file.stem
    suffix_tag = f"_in_{suffix}"
    if base.endswith(suffix_tag):
        base = base[: -len(suffix_tag)]
    gene_part = base.split(f"channel{ch_num}_", 1)[1] if f"channel{ch_num}_" in base else ""
    gene = normalize_gene(gene_part)
    return out_file, gene, out_file.stat().st_mtime

def get_owner_root(nas_root, owner):
    """
    Resolve the root directory for an owner. Special-case Matilde->Matilde/Microscopy.
    """
    base = Path(nas_root) / owner
    if owner == "Matilde":
        mic = base / "Microscopy"
        if mic.exists():
            return mic
    return base

def main():
    parser = argparse.ArgumentParser(description="Apply ANTs transforms to HCR channels (scan or manifest-driven).")
    parser.add_argument("--force", action="store_true", help="Overwrite existing outputs.")
    parser.add_argument("--dry-run", action="store_true", help="Print actions without writing outputs.")
    parser.add_argument("--mode", choices=["scan", "manifest"], default="scan", help="scan: prompt NAS owner/fish and auto-discover transforms; manifest: read CSV.")
    parser.add_argument("--manifest-csv", type=str, default="", help="Path to manifest CSV; if provided, manifest mode is assumed. Default: MANIFEST_CSV_DEFAULT")
    args = parser.parse_args()

    force = args.force
    dry_run = args.dry_run
    if force:
        print("[INFO] --force flag detected: will overwrite existing outputs.")
    if dry_run:
        print("[INFO] --dry-run flag detected: no files will be written or transformed.")

    # Mode resolution & manifest path
    manifest_csv_path = Path(args.manifest_csv or MANIFEST_CSV_DEFAULT)
    mode = args.mode
    if mode != "manifest" and args.manifest_csv:
        print(f"[INFO] --manifest-csv provided; switching mode to manifest.")
        mode = "manifest"

    # Set up metadata file paths in Danin folder
    danin_dir = Path(NAS_ROOT) / "Danin"
    danin_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = danin_dir / "transManifest.csv"  # append-only, one row per transformation event
    metadata_path = danin_dir / "transMetadata.csv"  # one row per fishid, TRUE/FALSE and date for each transformation

    best_rounds_map = load_best_rounds(BEST_ROUNDS_CSV)
    owner = None
    selected_fishids = []
    jobs = []
    manifest_rows = []  # for appending to transManifest.csv (only actual transformations)
    fish_best_info = {}  # cache best_round/num_rounds per fish for metadata

    if mode == "scan":
        owner, selected_fishids = prompt_owner_and_fishid(NAS_ROOT)
        for fish_id in selected_fishids:
            best_info = best_rounds_map.get(fish_id, {"best_round": 1, "num_rounds": 1, "owner": owner or ""})
            best_round = parse_round_val(best_info.get("best_round", 1), 1)
            num_rounds = parse_round_val(best_info.get("num_rounds", best_round), best_round)
            num_rounds = max(num_rounds, best_round)
            fish_best_info[fish_id] = {"best_round": best_round, "num_rounds": num_rounds}

            fish_dir = get_owner_root(NAS_ROOT, owner) / fish_id
            preproc_dir = fish_dir / "02_reg" / "00_preprocessing"
            reg_dir = fish_dir / "02_reg"
            if not preproc_dir.exists() or not reg_dir.exists():
                print(f"[DEBUG] Skipping {fish_id}: missing preprocessing or reg directory.")
                continue

            best_ref_2p = find_reference_2p(preproc_dir, fish_id)
            best_ref_round = find_best_round_reference(preproc_dir, fish_id, best_round)
            best_affine, best_warp, stg_best = find_best_to_2p_transforms(reg_dir, fish_id, best_round)
            best_hcr_files = find_hcr_channels(preproc_dir, fish_id, best_round, best_round)
            if best_hcr_files and best_affine and best_warp and best_ref_2p:
                aligned_dir = reg_dir / stg_best / "aligned"
                if not dry_run:
                    aligned_dir.mkdir(parents=True, exist_ok=True)
                for hcr_file in best_hcr_files:
                    jobs.append({
                        "moving": hcr_file,
                        "transformlist": [str(best_warp), str(best_affine)],
                        "reference": best_ref_2p,
                        "out_dir": aligned_dir,
                        "out_name": f"{hcr_file.stem}_in_2p.nrrd",
                        "fish_id": fish_id,
                        "round": best_round,
                        "colname": f"r{best_round}->2p",
                        "target_suffix": "2p"
                    })
            else:
                print(f"[DEBUG] Missing best->2p prerequisites for {fish_id} (round {best_round}).")

            for round_num in range(1, num_rounds + 1):
                if round_num == best_round:
                    continue
                hcr_files = find_hcr_channels(preproc_dir, fish_id, round_num, best_round)
                if not hcr_files:
                    print(f"[DEBUG] No HCR channels found for {fish_id} round {round_num}.")
                    continue

                affine_rb, warp_rb, stg_rb = find_round_to_best_transforms(reg_dir, fish_id, round_num, best_round)
                if not (affine_rb and warp_rb and best_ref_round):
                    print(f"[DEBUG] Missing rn->best prerequisites for {fish_id} round {round_num}.")
                    continue
                aligned_dir_rb = reg_dir / stg_rb / "aligned"
                if not dry_run:
                    aligned_dir_rb.mkdir(parents=True, exist_ok=True)
                for hcr_file in hcr_files:
                    jobs.append({
                        "moving": hcr_file,
                        "transformlist": [str(warp_rb), str(affine_rb)],
                        "reference": best_ref_round,
                        "out_dir": aligned_dir_rb,
                        "out_name": f"{hcr_file.stem}_in_r{best_round}.nrrd",
                        "fish_id": fish_id,
                        "round": round_num,
                        "colname": f"r{round_num}->r{best_round}",
                        "target_suffix": f"r{best_round}"
                    })

                if best_affine and best_warp and best_ref_2p:
                    aligned_dir_rn2p = reg_dir / "03_rn-2p" / "aligned"
                    if not dry_run:
                        aligned_dir_rn2p.mkdir(parents=True, exist_ok=True)
                    for hcr_file in hcr_files:
                        jobs.append({
                            "moving": hcr_file,
                            "transformlist": [str(best_warp), str(best_affine), str(warp_rb), str(affine_rb)],
                            "reference": best_ref_2p,
                            "out_dir": aligned_dir_rn2p,
                            "out_name": f"{hcr_file.stem}_in_2p.nrrd",
                            "fish_id": fish_id,
                            "round": round_num,
                            "colname": f"r{round_num}->2p",
                            "target_suffix": "2p"
                        })
                else:
                    print(f"[DEBUG] Skipping rn->2p for {fish_id} round {round_num}: missing best->2p transforms or reference.")
    else:
        if not manifest_csv_path:
            print("[ERROR] --manifest-csv is required in manifest mode.")
            sys.exit(2)
        if not manifest_csv_path.exists():
            print(f"[ERROR] Manifest CSV not found: {manifest_csv_path}")
            sys.exit(2)
        df = pd.read_csv(manifest_csv_path)
        required_cols = {"moving", "reference", "transforms"}
        missing_cols = required_cols - set(df.columns)
        if missing_cols:
            print(f"[ERROR] Manifest missing required columns: {', '.join(sorted(missing_cols))}")
            sys.exit(2)
        for _, row in df.iterrows():
            moving = Path(str(row["moving"])).expanduser()
            reference = Path(str(row["reference"])).expanduser()
            output_raw = str(row["output"]) if "output" in row else ""
            output = None
            transformlist = [t.strip() for t in str(row["transforms"]).split(";") if t.strip()]
            if not moving.exists():
                print(f"[WARN] Skipping row: missing moving file {moving}")
                continue
            if not reference.exists() and USE_REFERENCE_GRID:
                print(f"[WARN] Skipping row: missing reference {reference}")
                continue
            if not transformlist:
                print(f"[WARN] Skipping row: no transforms listed for {moving}")
                continue
            if output_raw and output_raw.lower() != "nan":
                output = Path(output_raw).expanduser()
            else:
                # Default: same folder as moving with suffix
                output = moving.parent / f"{moving.stem}_transformed.nrrd"
            if not dry_run:
                output.parent.mkdir(parents=True, exist_ok=True)
            jobs.append({
                "moving": moving,
                "transformlist": transformlist,
                "reference": reference if reference.exists() else None,
                "out_dir": output.parent,
                "out_name": output.name,
                "fish_id": str(row.get("fish_id", moving.stem)),
                "round": str(row.get("label", "manifest")),
                "colname": str(row.get("label", "manifest"))
            })

    print(f"Found {len(jobs)} files to transform (mode={mode}).")

    # Parallel processing setup
    max_workers = max(1, multiprocessing.cpu_count() - 1)
    print(f"[INFO] Using {max_workers} parallel workers.")

    def process_job(job):
        RED = "\033[91m"
        RESET = "\033[0m"
        out_name = job.get("out_name") or f"{job['moving'].stem}_transformed.nrrd"
        out_path = job["out_dir"] / out_name
        def short_path(p):
            try:
                return str(Path(p).relative_to(NAS_ROOT))
            except Exception:
                return str(p)
        now = datetime.datetime.now().isoformat(timespec='seconds')
        user = getpass.getuser()
        host = socket.gethostname()
        if out_path.exists() and not force:
            msg = f"{RED}{out_path.name} (already exists, use --force to overwrite){RESET}"
            print(f"[INFO] Skipping {msg}")
            return (job['fish_id'], job['colname'], 'skipped', '', str(out_path))
        if dry_run:
            print(f"[DRY-RUN] Would transform {short_path(job['moving'])} -> {short_path(out_path)}")
            return (job['fish_id'], job['colname'], 'dry_run', '', str(out_path))
        try:
            moving_img = ants.image_read(str(job["moving"]))
            reference_img = ants.image_read(str(job["reference"])) if job.get("reference") else moving_img
            transformlist = [str(t) for t in job["transformlist"]]
            fixed_img = reference_img if USE_REFERENCE_GRID else moving_img
            transformed = ants.apply_transforms(
                fixed=fixed_img,
                moving=moving_img,
                transformlist=transformlist,
                interpolator=INTERPOLATOR
            )
            ants.image_write(transformed, str(out_path))
            print(f"   -> saved: {short_path(out_path)}")
            manifest_rows.append({
                'fish_id': job.get('fish_id', ''),
                'transformation': job.get('colname', ''),
                'status': 'success',
                'date': now,
                'user': f"{user}@{host}",
                'input': short_path(job['moving']),
                'output': short_path(out_path),
                'error': ''
            })
            return (job['fish_id'], job['colname'], 'success', '', str(out_path))
        except Exception as e:
            print(f"[ERROR] Failed to transform {short_path(job['moving'])} for fish {job['fish_id']} round {job['round']}: {e}")
            traceback.print_exc()
            manifest_rows.append({
                'fish_id': job.get('fish_id', ''),
                'transformation': job.get('colname', ''),
                'status': 'failed',
                'date': now,
                'user': f"{user}@{host}",
                'input': short_path(job['moving']),
                'output': short_path(out_path),
                'error': str(e)
            })
            return (job['fish_id'], job['colname'], 'failed', str(e), str(out_path))

    # Run jobs in parallel
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        for res in tqdm(executor.map(process_job, jobs), total=len(jobs), desc="Transforming HCR channels", unit="file"):
            results.append(res)

    if mode == "scan":
        col_order = ['best_round', 'num_rounds', 'best_to_2p', 'best_to_2p_date']
        meta_rows = {}

        def ensure_col(col):
            if col not in col_order:
                col_order.append(col)

        for fish_id in selected_fishids:
            best_info = fish_best_info.get(fish_id, {"best_round": 1, "num_rounds": 1})
            best_round = best_info.get("best_round", 1)
            num_rounds = best_info.get("num_rounds", best_round)
            num_rounds = max(num_rounds, best_round)
            fish_dir = get_owner_root(NAS_ROOT, owner) / fish_id
            preproc_dir = fish_dir / "02_reg" / "00_preprocessing"
            reg_dir = fish_dir / "02_reg"
            if not preproc_dir.exists() or not reg_dir.exists():
                print(f"[DEBUG] Skipping metadata for {fish_id}: missing preprocessing or reg directory.")
                continue

            row = {
                "best_round": f"r{best_round}",
                "num_rounds": str(num_rounds),
                "best_to_2p": "",
                "best_to_2p_date": ""
            }

            for round_num in range(1, num_rounds + 1):
                round_label = f"r{round_num}"
                to_best_col = f"{round_label}_to_best"
                to_best_date_col = f"{to_best_col}_date"
                to_2p_col = f"{round_label}_to_2p"
                to_2p_date_col = f"{to_2p_col}_date"
                for c in [to_best_col, to_best_date_col, to_2p_col, to_2p_date_col]:
                    ensure_col(c)

                expected_channels = list_round_channels(preproc_dir, fish_id, round_num, best_round)

                # rn->best (best round uses raw channels as TRUE)
                statuses_best = []
                latest_best_mtime = None
                if round_num == best_round:
                    for ch in expected_channels:
                        col_id = f"{round_label}_best_ch{ch['channel']}_id"
                        col_status = f"{round_label}_best_ch{ch['channel']}_status"
                        ensure_col(col_id)
                        ensure_col(col_status)
                        row[col_id] = ch["gene"]
                        row[col_status] = 'TRUE'
                        statuses_best.append('TRUE')
                        latest_best_mtime = max(latest_best_mtime, ch["mtime"]) if latest_best_mtime else ch["mtime"]
                    row[to_best_col] = 'TRUE' if statuses_best else ''
                    row[to_best_date_col] = datetime.datetime.fromtimestamp(latest_best_mtime).isoformat(timespec='seconds') if latest_best_mtime else ''
                else:
                    aligned_dir = reg_dir / "02_rn-rbest" / "aligned"
                    for ch in expected_channels:
                        col_id = f"{round_label}_best_ch{ch['channel']}_id"
                        col_status = f"{round_label}_best_ch{ch['channel']}_status"
                        ensure_col(col_id)
                        ensure_col(col_status)
                        out_file, gene_out, mtime = find_output_for_channel(aligned_dir, fish_id, round_num, ch["channel"], suffix=f"r{best_round}")
                        if out_file:
                            row[col_id] = gene_out or ch["gene"]
                            row[col_status] = 'TRUE'
                            statuses_best.append('TRUE')
                            latest_best_mtime = max(latest_best_mtime, mtime) if latest_best_mtime else mtime
                        else:
                            row[col_id] = row.get(col_id, ch["gene"])
                            row[col_status] = 'FALSE'
                            statuses_best.append('FALSE')
                    if statuses_best:
                        row[to_best_col] = 'TRUE' if any(s == 'TRUE' for s in statuses_best) else 'FALSE'
                        row[to_best_date_col] = datetime.datetime.fromtimestamp(latest_best_mtime).isoformat(timespec='seconds') if latest_best_mtime else ''

                # to 2P (best round uses 01_rbest-2p, others 03_rn-2p)
                aligned_dir_2p = reg_dir / ("01_rbest-2p" if round_num == best_round else "03_rn-2p") / "aligned"
                statuses_2p = []
                latest_2p_mtime = None
                for ch in expected_channels:
                    col_id = f"{round_label}_2p_ch{ch['channel']}_id"
                    col_status = f"{round_label}_2p_ch{ch['channel']}_status"
                    ensure_col(col_id)
                    ensure_col(col_status)
                    out_file, gene_out, mtime = find_output_for_channel(aligned_dir_2p, fish_id, round_num, ch["channel"], suffix="2p")
                    if out_file:
                        row[col_id] = gene_out or ch["gene"]
                        row[col_status] = 'TRUE'
                        statuses_2p.append('TRUE')
                        latest_2p_mtime = max(latest_2p_mtime, mtime) if latest_2p_mtime else mtime
                    else:
                        row[col_id] = row.get(col_id, ch["gene"])
                        row[col_status] = 'FALSE'
                        statuses_2p.append('FALSE')
                if statuses_2p:
                    row[to_2p_col] = 'TRUE' if any(s == 'TRUE' for s in statuses_2p) else 'FALSE'
                    row[to_2p_date_col] = datetime.datetime.fromtimestamp(latest_2p_mtime).isoformat(timespec='seconds') if latest_2p_mtime else ''
                if round_num == best_round:
                    if statuses_2p:
                        row["best_to_2p"] = row[to_2p_col]
                        row["best_to_2p_date"] = row[to_2p_date_col]

            meta_rows[fish_id] = row

        if meta_rows:
            meta_df = pd.DataFrame.from_dict(meta_rows, orient="index")
            meta_df = meta_df.reindex(columns=col_order)
            meta_df.index.name = 'fish_id'
            meta_df.fillna('', inplace=True)
            meta_df.to_csv(metadata_path)
            print(f"[INFO] Metadata written to {metadata_path}")
        else:
            print("[INFO] No metadata rows to write.")

    # Append to transManifest.csv (append-only, one row per transformation event, only for actual transformations)
    manifest_df = pd.DataFrame(manifest_rows)
    if not manifest_df.empty:
        header = not manifest_path.exists()
        manifest_df.to_csv(manifest_path, mode='a', header=header, index=False)
        print(f"[INFO] Appended {len(manifest_df)} rows to {manifest_path}")

    print("All done.")

if __name__ == "__main__":
    main()
