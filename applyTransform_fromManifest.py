#!/usr/bin/env python3
# applyTransform_fromManifest.py
# Minimal ANTsPy script that mirrors the notebook semantics, looping over a CSV.

# ========= EDIT THESE =========
MANIFEST_PATH  = "/Volumes/jlarsch/default/D2c/07_Data/Danin/regManifest/transformManifest.csv"
STACKS_DIR     = "/Volumes/jlarsch/default/D2c/07_Data/Danin/tempTransform/tempStacks"
TRANSFORMS_DIR = "/Volumes/jlarsch/default/D2c/07_Data/Danin/tempTransform/tempMat"
OUTPUT_DIR     = "/Volumes/jlarsch/default/D2c/07_Data/Danin/tempTransform/tempOutput"

USE_REFERENCE_GRID = True          # True -> output in reference space; False -> preserve moving grid
INTERPOLATOR = "welchWindowedSinc" # or "linear", "bspline", etc.
# =============================

from pathlib import Path
import pandas as pd
import ants  # must be ANTsPy (package name: antspyx)
from tqdm import tqdm 

def main():
    manifest_path = Path(MANIFEST_PATH)
    stacks_dir    = Path(STACKS_DIR)
    xforms_dir    = Path(TRANSFORMS_DIR)
    out_dir       = Path(OUTPUT_DIR)
    out_dir.mkdir(parents=True, exist_ok=True)

    if not manifest_path.exists():
        raise FileNotFoundError(f"Manifest not found: {manifest_path}")

    df = pd.read_csv(manifest_path)
    required = ["moving_filename", "affine_mat", "warp_nl", "reference_filename"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Manifest missing columns: {missing}")

    print(f"[apply] rows={len(df)}  stacks={stacks_dir}  transforms={xforms_dir}  out={out_dir}")
    print(f"[apply] use_ref_grid={USE_REFERENCE_GRID}  interp={INTERPOLATOR}")

    # Wrap the loop with tqdm for a progress bar
    for i, row in enumerate(tqdm(df.itertuples(index=False), total=len(df), desc="Transforming")):
        moving_fn = str(row.moving_filename).strip()
        affine_fn = str(row.affine_mat).strip()
        warp_fn   = str(row.warp_nl).strip()
        ref_fn    = str(row.reference_filename).strip()

        moving_p = stacks_dir / moving_fn
        affine_p = xforms_dir / affine_fn
        warp_p   = xforms_dir / warp_fn
        ref_p    = stacks_dir / ref_fn

        if not moving_p.exists(): raise FileNotFoundError(f"Missing moving: {moving_p}")
        if not affine_p.exists(): raise FileNotFoundError(f"Missing affine: {affine_p}")
        if not warp_p.exists():   raise FileNotFoundError(f"Missing warp:   {warp_p}")
        if not ref_p.exists():    raise FileNotFoundError(f"Missing reference: {ref_p}")

        print(f"[{i+1}/{len(df)}] moving={moving_p.name}  ref={ref_p.name}")

        # Load images
        moving_img    = ants.image_read(str(moving_p))
        reference_img = ants.image_read(str(ref_p))

        # ANTs semantics: last in list applied first.
        # We want affine then warp -> provide [warp, affine].
        transformlist = [str(warp_p), str(affine_p)]

        # Choose output grid
        fixed_img = reference_img if USE_REFERENCE_GRID else moving_img

        # Apply transforms (mirrors notebook)
        transformed = ants.apply_transforms(
            fixed=fixed_img,
            moving=moving_img,
            transformlist=transformlist,
            interpolator=INTERPOLATOR
        )

        # Save
        out_name = f"{Path(moving_fn).stem}_to_{Path(ref_fn).stem}.nrrd"
        out_path = out_dir / out_name
        ants.image_write(transformed, str(out_path))
        print(f"   -> saved: {out_path}")

    print("[apply] Done.")

if __name__ == "__main__":
    main()
