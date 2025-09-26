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
    fishids = [d.name for d in fish_root.iterdir() if d.is_dir() and not d.name.startswith('.')]
    print(f"Available fish IDs for owner '{owner}':")
    for f in fishids:
        print(f"  - {f}")
    fishid_input = input("Enter fish ID to process (or 'all' for all fish): ").strip()
    if fishid_input == 'all':
        selected_fishids = fishids
    elif fishid_input in fishids:
        selected_fishids = [fishid_input]
    else:
        print(f"[ERROR] Fish ID '{fishid_input}' not found for owner '{owner}'. Exiting.")
        exit(1)
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

    # Set up metadata file path
    NAS_ROOT = "/Volumes/jlarsch/default/D2c/07_Data"
    manifest_dir = Path(NAS_ROOT) / "Danin" / "transManifest"
    manifest_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = manifest_dir / "transManifest.csv"

    # Read existing metadata if present
    if manifest_path.exists():
        meta_df = pd.read_csv(manifest_path, dtype=str).set_index('fish_id')
    else:
        meta_df = pd.DataFrame(columns=[
            'fish_id', 'r1->2p', 'rn->r1', 'last_processed_date', 'processed_by', 'force_overwrite', 'last_error'
        ]).set_index('fish_id')
    """
    Main routine: prompts for owner and fishid, finds all jobs (HCR files to transform), applies transforms, and saves output.
    """
    owner, selected_fishids = prompt_owner_and_fishid(NAS_ROOT)
    jobs = []
    # Track per-fish status for metadata
    fish_status = {fish_id: {'r1->2p': 'not_found', 'rn->r1': 'not_found', 'last_error': ''} for fish_id in selected_fishids}

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
        out_name = f"{job['moving'].stem}_in_{'2p' if job['round']==1 else 'r1'}.nrrd"
        out_path = job["out_dir"] / out_name
        if out_path.exists() and not force:
            msg = f"[INFO] Skipping {out_path} (already exists, use --force to overwrite)"
            print(msg)
            return (job['fish_id'], job['colname'], 'skipped', '', str(out_path))
        if dry_run:
            print(f"[DRY-RUN] Would transform {job['moving']} -> {out_path}")
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
            print(f"   -> saved: {out_path}")
            return (job['fish_id'], job['colname'], 'success', '', str(out_path))
        except Exception as e:
            print(f"[ERROR] Failed to transform {job['moving']} for fish {job['fish_id']} round {job['round']}: {e}")
            traceback.print_exc()
            return (job['fish_id'], job['colname'], 'failed', str(e), str(out_path))

    # Run jobs in parallel
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        for res in tqdm(executor.map(process_job, jobs), total=len(jobs), desc="Transforming HCR channels", unit="file"):
            results.append(res)

    # Update fish_status with results
    for fish_id in fish_status:
        for col in ['r1->2p', 'rn->r1']:
            # If any job for this fish/col was success, set to success; if all skipped, set to skipped; if any failed, set to failed
            statuses = [r[2] for r in results if r[0] == fish_id and r[1] == col]
            errors = [r[3] for r in results if r[0] == fish_id and r[1] == col and r[2] == 'failed']
            if 'success' in statuses:
                fish_status[fish_id][col] = 'success'
            elif 'failed' in statuses:
                fish_status[fish_id][col] = 'failed'
            elif 'dry_run' in statuses:
                fish_status[fish_id][col] = 'dry_run'
            elif 'skipped' in statuses:
                fish_status[fish_id][col] = 'skipped'
            # Record last error if any
            if errors:
                fish_status[fish_id]['last_error'] = errors[-1]

    # Write/update metadata CSV
    now = datetime.datetime.now().isoformat(timespec='seconds')
    user = getpass.getuser()
    host = socket.gethostname()
    for fish_id, stat in fish_status.items():
        meta_df.loc[fish_id, 'fish_id'] = fish_id
        meta_df.loc[fish_id, 'r1->2p'] = stat['r1->2p']
        meta_df.loc[fish_id, 'rn->r1'] = stat['rn->r1']
        meta_df.loc[fish_id, 'last_processed_date'] = now
        meta_df.loc[fish_id, 'processed_by'] = f"{user}@{host}"
        meta_df.loc[fish_id, 'force_overwrite'] = str(force)
        meta_df.loc[fish_id, 'last_error'] = stat.get('last_error', '')
    meta_df = meta_df[list(meta_df.columns)]  # preserve column order
    meta_df.to_csv(manifest_path)
    print(f"[INFO] Metadata written to {manifest_path}")

    print("All done.")

if __name__ == "__main__":
    main()
#!/usr/bin/env python3
