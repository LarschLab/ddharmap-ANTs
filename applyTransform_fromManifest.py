#!/usr/bin/env python3
"""
apply_transforms_from_manifest_hardcoded.py

Minimal, *hardcoded paths* version using a lean manifest schema.

Manifest columns (headers in first row) — all REQUIRED:
- moving_filename        (image to transform; found in STACKS_DIR)
- affine_mat             (ANTs affine, e.g., 0GenericAffine.mat; found in TRANSFORMS_DIR)
- warp_nl                (ANTs warp field, e.g., 1Warp.nii.gz; found in TRANSFORMS_DIR)
- reference_filename     (used for output naming; if USE_REFERENCE_GRID=True, defines the target grid)

Notes:
- Output filename: "<moving_stem>_to_<reference_stub>.nrrd"
- Default behavior preserves the moving grid. Set USE_REFERENCE_GRID=True to resample onto reference grid.
"""

# =============== GLOBAL SETTINGS (EDIT THESE) ===================
MANIFEST_PATH      = "/Volumes/jlarsch/default/D2c/07_Data/Danin/regManifest/transformManifest.csv"
STACKS_DIR         = "/Volumes/jlarsch/default/D2c/07_Data/Danin/tempTransform/tempStacks"
TRANSFORMS_DIR     = "/Volumes/jlarsch/default/D2c/07_Data/Danin/tempTransform/tempMat"
OUTPUT_DIR         = "/Volumes/jlarsch/default/D2c/07_Data/Danin/tempTransform/tempOutput"
THREADS            = 16           # ITK threads
DEFAULT_VALUE      = 0            # background fill
USE_REFERENCE_GRID = True        # if True, resample onto reference_filename's grid
OUTPUT_PIXEL_TYPE = "like_moving"   # Pixel type policy: "like_moving" | "float32" | "uint16" | "uint8"
# ================================================================

import os
import pandas as pd
from pathlib import Path
import SimpleITK as sitk

def resolve_pixel_id(moving_img: sitk.Image) -> int:
    if OUTPUT_PIXEL_TYPE == "like_moving":
        return moving_img.GetPixelID()
    mapping = {
        "float32": sitk.sitkFloat32,
        "uint16":  sitk.sitkUInt16,
        "uint8":   sitk.sitkUInt8,
    }
    try:
        return mapping[OUTPUT_PIXEL_TYPE.lower()]
    except KeyError:
        raise ValueError(f"Unknown OUTPUT_PIXEL_TYPE: {OUTPUT_PIXEL_TYPE}")

def log(msg: str):
    print(f"[apply] {msg}", flush=True)

def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)

def load_image(path: Path) -> sitk.Image:
    if not path.exists():
        raise FileNotFoundError(f"Missing image: {path}")
    return sitk.ReadImage(str(path))

def save_image(img: sitk.Image, path: Path):
    ensure_dir(path.parent)
    sitk.WriteImage(img, str(path), useCompression=True)

def read_affine(path: Path) -> sitk.Transform:
    if not path.exists():
        raise FileNotFoundError(f"Missing affine: {path}")
    return sitk.ReadTransform(str(path))

def read_warp(path: Path) -> sitk.Transform:
    if not path.exists():
        raise FileNotFoundError(f"Missing warp: {path}")
    disp_img = sitk.ReadImage(str(path))

    # Sanity checks
    dim = disp_img.GetDimension()
    comps = disp_img.GetNumberOfComponentsPerPixel()
    if comps != dim:
        raise ValueError(
            f"Warp field has {comps} components but image dimension is {dim}. "
            "Expected a vector displacement field with one component per axis."
        )

    # Cast to the type SimpleITK expects: vector of 64-bit float
    if disp_img.GetPixelID() != sitk.sitkVectorFloat64:
        disp_img = sitk.Cast(disp_img, sitk.sitkVectorFloat64)

    return sitk.DisplacementFieldTransform(disp_img)

def compose_affine_then_warp(affine: sitk.Transform, warp: sitk.Transform) -> sitk.Transform:
    comp = sitk.CompositeTransform(affine.GetDimension())
    # NOTE: CompositeTransform applies transforms in reverse order of addition.
    # Add warp first, then affine -> application order = affine THEN warp (as desired).
    comp.AddTransform(warp)    # applied second
    comp.AddTransform(affine)  # applied first
    return comp

def resample_to_reference(moving, reference, transform, default_value, interpolator):
    out_pix = resolve_pixel_id(moving)
    return sitk.Resample(moving, reference, transform, interpolator, default_value, out_pix)

def resample_preserve_grid(moving, transform, default_value, interpolator):
    # Copy moving’s grid
    identity = sitk.Transform(moving.GetDimension(), sitk.sitkIdentity)
    ref = sitk.Resample(moving, moving, identity, sitk.sitkNearestNeighbor, 0, moving.GetPixelID())
    out_pix = resolve_pixel_id(moving)
    return sitk.Resample(moving, ref, transform, interpolator, default_value, out_pix)

def reference_stub_from_filename(ref_name: str) -> str:
    return Path(ref_name).stem if ref_name and ref_name.strip() else "ref"

def main():
    os.environ["ITK_GLOBAL_DEFAULT_NUMBER_OF_THREADS"] = str(THREADS)

    manifest_path = Path(MANIFEST_PATH)
    stacks_dir    = Path(STACKS_DIR)
    xforms_dir    = Path(TRANSFORMS_DIR)
    out_root      = Path(OUTPUT_DIR)

    if not manifest_path.exists():
        raise FileNotFoundError(f"Manifest not found: {manifest_path}")
    df = pd.read_csv(manifest_path)

    required_cols = ["moving_filename", "affine_mat", "warp_nl", "reference_filename"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Manifest missing required columns: {missing}")

    log(f"Loaded manifest with {len(df)} rows.")
    log(f"Stacks:     {stacks_dir}")
    log(f"Transforms: {xforms_dir}")
    log(f"Output:     {out_root}")
    log(f"Threads:    {THREADS}")
    log(f"Ref grid:   {USE_REFERENCE_GRID}")

    # Single interpolator policy (no channel column): default linear for intensity images.
    interpolator = sitk.sitkLinear

    for i, row in df.iterrows():
        moving_fn  = str(row["moving_filename"])
        affine_fn  = str(row["affine_mat"])
        warp_fn    = str(row["warp_nl"])
        ref_fn     = str(row["reference_filename"])

        moving_p   = stacks_dir / moving_fn
        affine_p   = xforms_dir / affine_fn
        warp_p     = xforms_dir / warp_fn
        ref_p      = stacks_dir / ref_fn if ref_fn else None

        log(f"Row {i+1}/{len(df)}: moving={moving_fn}, ref={ref_fn}")

        # Load images & transforms
        moving_img = load_image(moving_p)
        affine_tr  = read_affine(affine_p)
        warp_tr    = read_warp(warp_p)
        comp_tr    = compose_affine_then_warp(affine_tr, warp_tr)

        # Resample
        if USE_REFERENCE_GRID:
            if not ref_fn:
                raise ValueError("USE_REFERENCE_GRID=True but reference_filename is empty in manifest.")
            reference_img = load_image(ref_p)
            out_img = resample_to_reference(moving_img, reference_img, comp_tr, DEFAULT_VALUE, interpolator)
        else:
            out_img = resample_preserve_grid(moving_img, comp_tr, DEFAULT_VALUE, interpolator)

        # Output name: <moving_stem>_to_<reference_stub>.nrrd
        ref_stub = reference_stub_from_filename(ref_fn)
        out_name = f"{Path(moving_fn).stem}_to_{ref_stub}.nrrd"
        out_dir  = out_root
        out_path = out_dir / out_name

        save_image(out_img, out_path)
        log(f"✔ Saved: {out_path}")

    log("Done.")

if __name__ == "__main__":
    main()
