"""
applyTransform_fromManifest.py

This script crawls a NAS directory structure for a selected owner and fish ID(s),
finds all non-GCaMP HCR channel .nrrd files in 02_reg/00_preprocessing/r1 and rn for each fish,
applies the appropriate ANTs transformation matrices for each round,
and saves the transformed output in the correct aligned subfolder for each round.

USAGE:

    python applyTransform_fromManifest.py [--force] [--dry-run]

    --force     Overwrite output files if they already exist (otherwise, skip existing outputs)
    --dry-run   Only print what would be done, do not perform any transformations or write files

The script will prompt you to select an owner and fish ID(s) to process. Use 'all' to process all fish for the selected owner.

Dependencies:
    - antspyx (import as 'ants')
    - tqdm
    - pandas
    pip install antspyx tqdm pandas
"""
#!/usr/bin/env python3

# This script crawls the NAS directory structure for a given owner (e.g., Matilde),
# finds all non-GCaMP HCR channel .nrrd files in 02_reg/00_preprocessing/r1 and rn for each fish,
# applies the appropriate ANTs transformation matrices for each round,
# and saves the transformed output in the correct aligned subfolder for each round.

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
    fish_root = nas_root / owner
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
USE_REFERENCE_GRID = True                          # Use reference image as output grid
INTERPOLATOR = "welchWindowedSinc"                # Interpolator for ANTs
# =================================================

def find_fish_dirs(nas_root, owner):
    """
    Returns a list of all fish directories for the given owner.
    Skips hidden directories.
    """
    fish_root = Path(nas_root) / owner
    return [d for d in fish_root.iterdir() if d.is_dir() and not d.name.startswith('.')]

def find_hcr_channels(preproc_dir, fish_id, round_num):
    """
    Finds all non-GCaMP HCR channel .nrrd files for a given fish and round.
    <fish_id> should include the letter prefix (e.g., 'L427_f01').
    Only files matching <fish_id>_round<round_num>_channel*.nrrd and not containing 'GCaMP'.
    """
    round_dir = preproc_dir / ('r1' if round_num == 1 else 'rn')
    if not round_dir.exists():
        return []
    return [f for f in round_dir.glob(f"{fish_id}_round{round_num}_channel*.nrrd") if "GCaMP" not in f.name]

def find_transforms(reg_dir, fish_id, round_num):
    """
    Finds the affine and warp transformation matrices for the given fish and round.
    <fish_id> should include the letter prefix (e.g., 'L427_f01').
    For r1: looks in 01_r1-2p/transMatrices
    For rn: looks in 02_rn-r1/transMatrices
    Returns (affine, warp, stage_folder_name)
    """
    stg = "01_r1-2p" if round_num == 1 else "02_rn-r1"
    tm_dir = reg_dir / stg / "transMatrices"
    if not tm_dir.exists():
        return None, None, stg
    affine = list(tm_dir.glob(f"{fish_id}_round{round_num}_GCaMP_to_ref0GenericAffine.mat"))
    warp = list(tm_dir.glob(f"{fish_id}_round{round_num}_GCaMP_to_ref1Warp.nii.gz"))
    if affine and warp:
        return affine[0], warp[0], stg
    return None, None, stg

def find_reference(fish_dir, round_num):
    """
    Finds the reference image for the given round.
    <fish_id> should include the letter prefix (e.g., 'L427_f01').
    For r1: 02_reg/00_preprocessing/2p_anatomy/<fish_id>_anatomy_2P_GCaMP.nrrd
    For rn: 02_reg/00_preprocessing/r1/<fish_id>_round1_channel1_GCaMP.nrrd
    Returns the Path if found, else None.
    """
    preproc_dir = fish_dir / "02_reg" / "00_preprocessing"
    fish_id = fish_dir.name
    if round_num == 1:
        ref = preproc_dir / "2p_anatomy" / f"{fish_id}_anatomy_2P_GCaMP.nrrd"
    else:
        ref = preproc_dir / "r1" / f"{fish_id}_round1_channel1_GCaMP.nrrd"
    return ref if ref.exists() else None

def main():
    # Parse --force and --dry-run flags
    force = '--force' in sys.argv
    dry_run = '--dry-run' in sys.argv
    if force:
        print("[INFO] --force flag detected: will overwrite existing outputs.")
    if dry_run:
        print("[INFO] --dry-run flag detected: no files will be written or transformed.")

    # Set up metadata file paths in Danin folder
    NAS_ROOT = "/Volumes/jlarsch/default/D2c/07_Data"
    danin_dir = Path(NAS_ROOT) / "Danin"
    danin_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = danin_dir / "transManifest.csv"  # append-only, one row per transformation event
    metadata_path = danin_dir / "transMetadata.csv"  # one row per fishid, TRUE/FALSE and date for each transformation

    # Read existing transMetadata.csv if present
    if metadata_path.exists():
        meta_df = pd.read_csv(metadata_path, dtype=str).set_index('fish_id')
    else:
        meta_df = pd.DataFrame(columns=[
            'fish_id', 'r1->2p', 'r1->2p_date', 'rn->r1', 'rn->r1_date'
        ]).set_index('fish_id')
    """
    Main routine: prompts for owner and fishid, finds all jobs (HCR files to transform), applies transforms, and saves output.
    """
    owner, selected_fishids = prompt_owner_and_fishid(NAS_ROOT)
    jobs = []
    # Track per-fish status for metadata
    fish_status = {fish_id: {'r1->2p': 'not_found', 'rn->r1': 'not_found', 'last_error': ''} for fish_id in selected_fishids}
    manifest_rows = []  # for appending to transManifest.csv (only actual transformations)

    # Helper: get all expected channels for a fish and round (including GCaMP)
    def get_expected_channels(preproc_dir, fish_id, round_num):
        round_dir = preproc_dir / ('r1' if round_num == 1 else 'rn')
        if not round_dir.exists():
            return []
        # All channel files for this round
        return [f.stem for f in round_dir.glob(f"{fish_id}_round{round_num}_channel*.nrrd")]

    for fish_id in selected_fishids:
        fish_dir = Path(NAS_ROOT) / owner / fish_id
        preproc_dir = fish_dir / "02_reg" / "00_preprocessing"
        reg_dir = fish_dir / "02_reg"
        if not preproc_dir.exists() or not reg_dir.exists():
            print(f"[DEBUG] Skipping {fish_id}: missing preprocessing or reg directory.")
            fish_status[fish_id]['last_error'] = 'missing preprocessing or reg directory'
            continue

        for round_num, colname in zip([1, 2], ['r1->2p', 'rn->r1']):
            hcr_files = find_hcr_channels(preproc_dir, fish_id, round_num)
            if not hcr_files:
                print(f"[DEBUG] No HCR channels found for {fish_id} round {round_num}.")
                fish_status[fish_id][colname] = 'not_found'
                continue
            affine, warp, stg = find_transforms(reg_dir, fish_id, round_num)
            ref = find_reference(fish_dir, round_num)
            if not (affine and warp and ref):
                print(f"[DEBUG] Missing transform or reference for {fish_id} round {round_num}.")
                fish_status[fish_id][colname] = 'not_found'
                continue
            aligned_dir = reg_dir / stg / "aligned"
            if not dry_run:
                aligned_dir.mkdir(parents=True, exist_ok=True)
            for hcr_file in hcr_files:
                jobs.append({
                    "moving": hcr_file,
                    "affine": affine,
                    "warp": warp,
                    "reference": ref,
                    "out_dir": aligned_dir,
                    "fish_id": fish_id,
                    "round": round_num,
                    "colname": colname
                })

    print(f"Found {len(jobs)} HCR files to transform.")

    # Parallel processing setup
    max_workers = max(1, multiprocessing.cpu_count() - 1)
    print(f"[INFO] Using {max_workers} parallel workers.")

    def process_job(job):
        RED = "\033[91m"
        RESET = "\033[0m"
        out_name = f"{job['moving'].stem}_in_{'2p' if job['round']==1 else 'r1'}.nrrd"
        out_path = job["out_dir"] / out_name
        # Shorten path for info/debug output: print from <owner>/*
        def short_path(p):
            p = str(p)
            try:
                idx = p.index(f"/{owner}/")
                return p[idx+1:]
            except Exception:
                return p
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
            reference_img = ants.image_read(str(job["reference"]))
            transformlist = [str(job["warp"]), str(job["affine"])]
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
                'fish_id': job['fish_id'],
                'transformation': job['colname'],
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
                'fish_id': job['fish_id'],
                'transformation': job['colname'],
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

    # Update fish_status and metadata for transMetadata.csv
    # For each fish, scan aligned folders for all expected output files (including GCaMP), and mark as TRUE/FALSE
    import re
    channel_id_re = re.compile(r"channel(\d+)_([^_]+)")
    # Track all channel columns for ordering, as dicts to sort by channel number
    r1_cols, r2_cols = dict(), dict()
    import re as _re
    for fish_id in fish_status:
        fish_dir = Path(NAS_ROOT) / owner / fish_id
        preproc_dir = fish_dir / "02_reg" / "00_preprocessing"
        reg_dir = fish_dir / "02_reg"
        for round_num, stg in zip([1, 2], ["01_r1-2p", "02_rn-r1"]):
            aligned_dir = reg_dir / stg / "aligned"
            round_label = f"r{round_num}"
            # For each channel number, look for any aligned file matching the pattern
            for ch_num in range(1, 10):  # assume up to 9 channels; adjust as needed
                pattern = f"{fish_id}_round{round_num}_channel{ch_num}_*_in_{'2p' if round_num==1 else 'r1'}.nrrd"
                matches = list(aligned_dir.glob(pattern))
                if matches:
                    # Use the most recent file if multiple
                    out_file = max(matches, key=lambda f: f.stat().st_mtime)
                    # Extract gene and probe from filename
                    m = _re.match(rf"{fish_id}_round{round_num}_channel{ch_num}_([A-Za-z0-9]+)_([0-9]+)_in_({'2p' if round_num==1 else 'r1'})\.nrrd", out_file.name)
                    if m:
                        gene = f"{m.group(1)}.{m.group(2)}"
                    else:
                        # fallback: try just gene
                        m2 = _re.match(rf"{fish_id}_round{round_num}_channel{ch_num}_([A-Za-z0-9]+)_in_({'2p' if round_num==1 else 'r1'})\.nrrd", out_file.name)
                        gene = m2.group(1) if m2 else ''
                    id_col = f"{round_label}_ch{ch_num}_id"
                    status_col = f"{round_label}_ch{ch_num}_status"
                    if id_col not in meta_df.columns:
                        meta_df[id_col] = ''
                    if status_col not in meta_df.columns:
                        meta_df[status_col] = ''
                    meta_df.loc[fish_id, id_col] = gene
                    meta_df.loc[fish_id, status_col] = 'TRUE'
                    # Set date column to latest mtime for this round
                    if f"{round_label}_date" not in meta_df.columns:
                        meta_df[f"{round_label}_date"] = ''
                    mtime = out_file.stat().st_mtime
                    prev = meta_df.loc[fish_id, f"{round_label}_date"]
                    if not prev or (isinstance(prev, str) and prev == '') or (isinstance(prev, float) and mtime > prev):
                        meta_df.loc[fish_id, f"{round_label}_date"] = datetime.datetime.fromtimestamp(mtime).isoformat(timespec='seconds')
                    # Track for ordering
                    if round_num == 1:
                        r1_cols[ch_num] = (id_col, status_col)
                    else:
                        r2_cols[ch_num] = (id_col, status_col)
                else:
                    id_col = f"{round_label}_ch{ch_num}_id"
                    status_col = f"{round_label}_ch{ch_num}_status"
                    if id_col not in meta_df.columns:
                        meta_df[id_col] = ''
                    if status_col not in meta_df.columns:
                        meta_df[status_col] = ''
                    meta_df.loc[fish_id, status_col] = 'FALSE'
            # GCaMP channel (not transformed by this script, just check presence)
            gcamp_patterns = [
                f"{fish_id}_round{round_num}_GCaMP*_in_{'2p' if round_num==1 else 'r1'}.nrrd",
                f"{fish_id}_round{round_num}_channel*_GCaMP*_in_{'2p' if round_num==1 else 'r1'}.nrrd"
            ]
            gcamp_files = []
            for pat in gcamp_patterns:
                gcamp_files.extend(aligned_dir.glob(pat))
            for gcamp in gcamp_files:
                # Try to extract channel number if present
                m = channel_id_re.search(gcamp.name)
                if m:
                    ch_num, gene = m.groups()
                else:
                    ch_num, gene = '1', 'GCaMP'  # fallback
                gene = gene.replace('.nrrd','')
                m_gene = _re.match(r"(.+)_([0-9]+)$", gene)
                if m_gene:
                    gene = f"{m_gene.group(1)}.{m_gene.group(2)}"
                id_col = f"{round_label}_ch{ch_num}_id"
                status_col = f"{round_label}_ch{ch_num}_status"
                if id_col not in meta_df.columns:
                    meta_df[id_col] = ''
                if status_col not in meta_df.columns:
                    meta_df[status_col] = ''
                meta_df.loc[fish_id, id_col] = gene
                meta_df.loc[fish_id, status_col] = 'TRUE'
                if f"{round_label}_date" not in meta_df.columns:
                    meta_df[f"{round_label}_date"] = ''
                mtime = gcamp.stat().st_mtime
                prev = meta_df.loc[fish_id, f"{round_label}_date"]
                if not prev or (isinstance(prev, str) and prev == '') or (isinstance(prev, float) and mtime > prev):
                    meta_df.loc[fish_id, f"{round_label}_date"] = datetime.datetime.fromtimestamp(mtime).isoformat(timespec='seconds')
                if round_num == 1:
                    r1_cols[int(ch_num)] = (id_col, status_col)
                else:
                    r2_cols[int(ch_num)] = (id_col, status_col)
            # GCaMP channel (not transformed by this script, just check presence)
            # Match both with and without channel number
            gcamp_patterns = [
                f"{fish_id}_round{round_num}_GCaMP*_in_{'2p' if round_num==1 else 'r1'}.nrrd",
                f"{fish_id}_round{round_num}_channel*_GCaMP*_in_{'2p' if round_num==1 else 'r1'}.nrrd"
            ]
            gcamp_files = []
            for pat in gcamp_patterns:
                gcamp_files.extend(aligned_dir.glob(pat))
            for gcamp in gcamp_files:
                # Try to extract channel number if present
                m = channel_id_re.search(gcamp.name)
                if m:
                    ch_num, gene = m.groups()
                else:
                    ch_num, gene = '1', 'GCaMP'  # fallback
                gene = gene.replace('.nrrd','')
                m_gene = _re.match(r"(.+)_([0-9]+)$", gene)
                if m_gene:
                    gene = f"{m_gene.group(1)}.{m_gene.group(2)}"
                id_col = f"{round_label}_ch{ch_num}_id"
                status_col = f"{round_label}_ch{ch_num}_status"
                if id_col not in meta_df.columns:
                    meta_df[id_col] = ''
                if status_col not in meta_df.columns:
                    meta_df[status_col] = ''
                meta_df.loc[fish_id, id_col] = gene
                meta_df.loc[fish_id, status_col] = 'TRUE'
                if f"{round_label}_date" not in meta_df.columns:
                    meta_df[f"{round_label}_date"] = ''
                mtime = gcamp.stat().st_mtime
                prev = meta_df.loc[fish_id, f"{round_label}_date"]
                if not prev or (isinstance(prev, str) and prev == '') or (isinstance(prev, float) and mtime > prev):
                    meta_df.loc[fish_id, f"{round_label}_date"] = datetime.datetime.fromtimestamp(mtime).isoformat(timespec='seconds')
                if round_num == 1:
                    r1_cols[int(ch_num)] = (id_col, status_col)
                else:
                    r2_cols[int(ch_num)] = (id_col, status_col)

    # Fill r1->2p and rn->r1 columns: TRUE if any *_status for that round is TRUE, FALSE if all are FALSE, else blank
    for fish_id in meta_df.index:
        for round_num, col, status_cols in zip([1,2], ['r1->2p','rn->r1'], [r1_cols, r2_cols]):
            vals = [meta_df.loc[fish_id, status] for _, status in sorted(status_cols.values()) if status in meta_df.columns]
            if any(v == 'TRUE' for v in vals):
                meta_df.loc[fish_id, col] = 'TRUE'
            elif all(v == 'FALSE' for v in vals) and vals:
                meta_df.loc[fish_id, col] = 'FALSE'
            else:
                meta_df.loc[fish_id, col] = ''

    # Order columns: fish_id, r1->2p, r1->2p_date, rn->r1, rn->r1_date, all r1_ch*_id/status, all r2_ch*_id/status, sorted by channel number
    r1_flat = [col for pair in [r1_cols[k] for k in sorted(r1_cols)] for col in pair]
    r2_flat = [col for pair in [r2_cols[k] for k in sorted(r2_cols)] for col in pair]
    ordered_cols = ['r1->2p','r1->2p_date','rn->r1','rn->r1_date'] + r1_flat + r2_flat
    # Remove duplicate fish_id column if present
    if 'fish_id' in meta_df.columns:
        meta_df = meta_df.drop(columns=['fish_id'])
    meta_df = meta_df.reindex(columns=ordered_cols)
    meta_df.index.name = 'fish_id'
    meta_df.to_csv(metadata_path)
    print(f"[INFO] Metadata written to {metadata_path}")

    # Append to transManifest.csv (append-only, one row per transformation event, only for actual transformations)
    manifest_df = pd.DataFrame(manifest_rows)
    if not manifest_df.empty:
        header = not manifest_path.exists()
        manifest_df.to_csv(manifest_path, mode='a', header=header, index=False)
        print(f"[INFO] Appended {len(manifest_df)} rows to {manifest_path}")

    print("All done.")

if __name__ == "__main__":
    main()
#!/usr/bin/env python3
