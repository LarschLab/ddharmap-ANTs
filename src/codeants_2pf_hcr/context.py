"""Context and fish-scoped stage helpers for notebook cells [4], [4a], [4b], [4c], [8], [8a], and [10]."""

from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path
import re
import time
from typing import Any

import numpy as np
import pandas as pd
from skimage import transform
import tifffile

from .spatial import _infer_voxels_nrrd, apply_func_orientation, corrcoef_img, load_or_cache_voxels


DEFAULT_RUN_CONFIG: dict[str, Any] = {
    "HIGH_CONF_ONLY": False,
    "SKIP_47": False,
    "STIM_ONSET_DELAY_SEC": 10.0,
    "TARGET_STIM_TYPES": None,
    "STRICT_PAIR_INTEGRITY": True,
    "REMOVE_INTERBLOCK_GAPS": True,
    "FORCE_RECOMPUTE_VOXELS": False,
    "FORCE_RECOMPUTE_HCR_WARP": False,
    "RECOMPUTE_WARP": False,
    "RECOMPUTE_FILTER_STATS": False,
    "RECOMPUTE_INTENSITY_WARP": False,
    "RECOMPUTE_CONF_FUNC_PAIRS": False,
    "RECOMPUTE_SUITE2P_TRACE_EXPORT": False,
    "RECOMPUTE_FUNC_WARP_EXPORT": False,
    "ANAT_SEG_FORCE_RECOMPUTE": False,
    "ANAT_SEG_SKIP_IF_EXISTS": True,
    "EXPORT_BEST_ROUND_LABELS_TO_RBEST": False,
    "HCR_WARP_SPLIT_CONNECTED_COMPONENTS": False,
    "HCR_WARP_COMPONENT_CONNECTIVITY": 3,
    "HCR_WARP_MIN_COMPONENT_VOXELS": 50,
    "HCR_WARP_MIN_COMPONENT_VOLUME_UM3": None,
    "HCR_WARP_DROP_LOW_Q": False,
    "HCR_WARP_LOW_FILTER_METRIC": "n_voxels",
    "HCR_WARP_FLAG_HIGH_Q": True,
    "HCR_WARP_HIGH_FILTER_METRIC": "equivalent_diameter_um",
    "CONF_FUNC_CSV_ANALYSIS": None,
    "GENE_ORDER": ["sst1.1", "sst1.2", "npy", "tac3b", "pth2", "cfos", "cort"],
    "GENE_COLORS": {
        "sst1.1": "#d62728",
        "sst1.2": "#d61ad2",
        "npy": "#1f9d55",
        "tac3b": "#ffd400",
        "pth2": "#00bcd4",
        "cfos": "#ff7f0e",
        "cort": "#8c564b",
    },
}

FORCE_TRUE_RUN_CONFIG_KEYS = (
    "RECOMPUTE_WARP",
    "RECOMPUTE_FILTER_STATS",
    "RECOMPUTE_CONF_FUNC_PAIRS",
    "EXPORT_BEST_ROUND_LABELS_TO_RBEST",
)

_FISH_TOKEN_RE = re.compile(r"[A-Za-z]\d+_f\d+")


@dataclass(frozen=True)
class FishContext:
    fish_id: str
    owner: str
    data_mode: str
    nas_root: Path
    local_root: Path
    data_root: Path
    fish_dir: Path
    preproc_dir: Path
    analysis_dir: Path
    outdir: Path
    out_raw: Path
    out_seg: Path
    out_reg: Path
    out_ncc: Path
    out_qa: Path
    out_derived: Path
    out_hcr: Path
    ref_dir: Path
    tmp_convert_dir: Path
    matching_metadata_csv: Path
    manifest_out: Path
    cellpose_model_root: Path
    anat_cp_model_path: str
    cp_hcr_model_path: str
    run_config: dict[str, Any]


@dataclass(frozen=True)
class ContextStageConfig:
    fish_id: str
    owner: str = "Matilde"
    data_mode: str = "local"
    matching_metadata_csv_override: Path | str | None = None
    manifest_out_override: Path | str | None = None
    cellpose_model_root_override: Path | str | None = None
    anat_cp_model_path_override: Path | str | None = None
    cp_hcr_model_path_override: Path | str | None = None
    polarity_override: Any = None
    run_config: dict[str, Any] | None = None


@dataclass(frozen=True)
class FishStateStageConfig:
    reset_always: bool = True
    verbose: bool = True


@dataclass(frozen=True)
class FinalFishAuditConfig:
    strict: bool = False
    max_rows_per_col: int = 5000
    include_internal_df: bool = False


@dataclass(frozen=True)
class VoxelStageConfig:
    force_recompute_voxels: bool = False


@dataclass(frozen=True)
class FunctionalOrientationStageConfig:
    overwrite_flipped: bool = False
    cache_version: int = 2


def default_nas_root() -> Path:
    if os.name == "nt":
        base = Path(r"\\nasdcsr.unil.ch\RECHERCHE\FAC\FBM\CIG\jlarsch\default\D2c")
        return base / "07_Data" if (base / "07_Data").exists() else base
    return Path("/Volumes/jlarsch/default/D2c/07_Data")


def default_local_root() -> Path:
    return Path("/Users/ddharmap/dataProcessing/2p_HCR/analysis/midThesis")


def owner_root(data_root: Path | str, owner: str | None = None, data_mode: str = "local") -> Path:
    root = Path(data_root)
    mode = str(data_mode).strip().lower()
    if mode == "local":
        return root
    owner_name = None if owner is None else str(owner).strip()
    if not owner_name:
        return root
    base = root / owner_name
    microscopy = base / "Microscopy"
    return microscopy if microscopy.exists() else base


def resolve_fish_dir(data_root: Path | str, owner: str, fish_id: str, data_mode: str = "local") -> Path:
    root = Path(data_root)
    fish_name = str(fish_id)
    mode = str(data_mode).strip().lower()
    if mode == "local":
        candidates = [root / fish_name]
    else:
        candidates = [owner_root(root, owner, data_mode=mode) / fish_name]
    for candidate in candidates:
        if (candidate / "03_analysis").exists():
            return candidate
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return candidates[0]


def default_matching_metadata_csv(data_root: Path | str, data_mode: str) -> Path:
    root = Path(data_root)
    if str(data_mode).strip().lower() == "local":
        return root / "matchingMetadata.csv"
    return root / "Danin" / "matchingMetadata.csv"


def default_cellpose_model_root(data_root: Path | str, data_mode: str, nas_root: Path | str) -> Path:
    root = Path(data_root)
    if str(data_mode).strip().lower() == "local":
        return root / "Cellpose" / "models"
    return Path(nas_root) / "Danin" / "Cellpose" / "models"


def normalize_run_config(run_config: dict[str, Any] | None = None) -> dict[str, Any]:
    normalized = {**DEFAULT_RUN_CONFIG, **(run_config or {})}
    for key in FORCE_TRUE_RUN_CONFIG_KEYS:
        normalized[key] = True
    normalized["GENE_ORDER"] = list(normalized["GENE_ORDER"])
    normalized["GENE_COLORS"] = dict(normalized["GENE_COLORS"])
    return normalized


def resolve_fish_context(
    *,
    fish_id: str,
    owner: str = "Matilde",
    data_mode: str = "local",
    nas_root: Path | str | None = None,
    local_root: Path | str | None = None,
    matching_metadata_csv_override: Path | str | None = None,
    manifest_out_override: Path | str | None = None,
    cellpose_model_root_override: Path | str | None = None,
    anat_cp_model_path_override: Path | str | None = None,
    cp_hcr_model_path_override: Path | str | None = None,
    run_config: dict[str, Any] | None = None,
) -> FishContext:
    mode = str(data_mode).strip().lower()
    if mode not in {"nas", "local"}:
        mode = "nas"
    nas = Path(nas_root) if nas_root is not None else default_nas_root()
    local = Path(local_root) if local_root is not None else default_local_root()
    data_root = local if mode == "local" else nas
    fish_dir = resolve_fish_dir(data_root, owner, fish_id, data_mode=mode)
    analysis_dir = fish_dir / "03_analysis"
    outdir = analysis_dir / "functional"
    out_raw = outdir / "raw"
    out_seg = outdir / "segmentation"
    out_reg = outdir / "registration"
    out_ncc = outdir / "ncc"
    out_qa = outdir / "qa"
    out_derived = outdir / "derived"
    out_hcr = outdir / "hcr_in_func"
    ref_dir = out_reg / "reference_planes"
    tmp_convert_dir = out_raw / "converted_nrrd_to_tif"
    for directory in (
        outdir,
        out_raw,
        out_seg,
        out_reg,
        out_ncc,
        out_qa,
        out_derived,
        out_hcr,
        ref_dir,
        tmp_convert_dir,
    ):
        directory.mkdir(parents=True, exist_ok=True)

    matching_metadata_csv = Path(matching_metadata_csv_override) if matching_metadata_csv_override else default_matching_metadata_csv(data_root, mode)
    manifest_out_default = data_root / "confocal_mask_manifest.csv" if mode == "local" else nas / "Danin" / "confocal_mask_manifest.csv"
    manifest_out = Path(manifest_out_override) if manifest_out_override else manifest_out_default
    cellpose_model_root = Path(cellpose_model_root_override) if cellpose_model_root_override else default_cellpose_model_root(data_root, mode, nas)
    anat_cp_default = cellpose_model_root / "2pf_cpsam_20250915_115140"
    hcr_cp_default = cellpose_model_root / "HCR_cpsam_20251006_094039"

    return FishContext(
        fish_id=str(fish_id),
        owner=str(owner),
        data_mode=mode,
        nas_root=nas,
        local_root=local,
        data_root=data_root,
        fish_dir=fish_dir,
        preproc_dir=fish_dir / "02_reg" / "00_preprocessing",
        analysis_dir=analysis_dir,
        outdir=outdir,
        out_raw=out_raw,
        out_seg=out_seg,
        out_reg=out_reg,
        out_ncc=out_ncc,
        out_qa=out_qa,
        out_derived=out_derived,
        out_hcr=out_hcr,
        ref_dir=ref_dir,
        tmp_convert_dir=tmp_convert_dir,
        matching_metadata_csv=matching_metadata_csv,
        manifest_out=manifest_out,
        cellpose_model_root=cellpose_model_root,
        anat_cp_model_path=str(Path(anat_cp_model_path_override) if anat_cp_model_path_override else anat_cp_default),
        cp_hcr_model_path=str(Path(cp_hcr_model_path_override) if cp_hcr_model_path_override else hcr_cp_default),
        run_config=normalize_run_config(run_config),
    )


def notebook_bindings_from_context(ctx: FishContext) -> dict[str, Any]:
    return {
        "FISH_ID": ctx.fish_id,
        "OWNER": ctx.owner,
        "DATA_MODE": ctx.data_mode,
        "NAS_ROOT": ctx.nas_root,
        "LOCAL_ROOT": ctx.local_root,
        "DATA_ROOT": ctx.data_root,
        "FISH_DIR": ctx.fish_dir,
        "PREPROC_DIR": ctx.preproc_dir,
        "ANALYSIS_DIR": ctx.analysis_dir,
        "OUTDIR": ctx.outdir,
        "OUT_RAW": ctx.out_raw,
        "OUT_SEG": ctx.out_seg,
        "OUT_REG": ctx.out_reg,
        "OUT_NCC": ctx.out_ncc,
        "OUT_QA": ctx.out_qa,
        "OUT_DERIVED": ctx.out_derived,
        "OUT_HCR": ctx.out_hcr,
        "REF_DIR": ctx.ref_dir,
        "TMP_CONVERT_DIR": ctx.tmp_convert_dir,
        "MATCHING_METADATA_CSV": ctx.matching_metadata_csv,
        "MANIFEST_OUT": ctx.manifest_out,
        "CELLPOSE_MODEL_ROOT": ctx.cellpose_model_root,
        "ANAT_CP_MODEL_PATH": ctx.anat_cp_model_path,
        "CP_HCR_MODEL_PATH": ctx.cp_hcr_model_path,
        "RUN_CONFIG": dict(ctx.run_config),
    }


def normalize_polarity_value(value: Any) -> str | None:
    text = "" if value is None else str(value).strip().lower()
    if text in ("", "none", "nan", "null"):
        return None
    aliases = {"n": "north", "northward": "north", "s": "south", "southward": "south"}
    return aliases.get(text, text)


def matching_metadata_cols(df: pd.DataFrame) -> tuple[str | None, str | None]:
    colmap = {str(column).strip().lower(): column for column in df.columns}
    fish_col = next((colmap[key] for key in ("fish_id", "fish", "fishid") if key in colmap), None)
    polarity_col = next((colmap[key] for key in ("polarity", "fish_polarity", "functional_polarity") if key in colmap), None)
    return fish_col, polarity_col


def read_matching_metadata_polarity(fish_id: str, metadata_csv: Path | str) -> tuple[str | None, str]:
    path = Path(metadata_csv)
    if not path.exists():
        return None, "missing metadata csv"
    frame = pd.read_csv(path)
    fish_col, polarity_col = matching_metadata_cols(frame)
    if fish_col is None or polarity_col is None:
        return None, "missing metadata columns"
    row = frame.loc[frame[fish_col].astype(str) == str(fish_id)]
    if row.empty:
        return None, f"{path.name}:{polarity_col}:missing row"
    polarity = normalize_polarity_value(row.iloc[0].get(polarity_col, None))
    return polarity, f"{path.name}:{polarity_col}"


def resolve_func_polarity(fish_id: str, metadata_csv: Path | str, polarity_override: Any = None) -> tuple[str | None, str]:
    override = normalize_polarity_value(polarity_override)
    if override in ("north", "south"):
        return override, "override"
    return read_matching_metadata_polarity(fish_id, metadata_csv)


def func_polarity_north(polarity: str | None) -> bool:
    return normalize_polarity_value(polarity) == "north"


def func_orientation_mode(polarity: str | None) -> str:
    return "rot180+flipX" if func_polarity_north(polarity) else "flipX"


def func_orientation_effective(polarity: str | None) -> str:
    return "flipY" if func_polarity_north(polarity) else "flipX"


def first_match(fish_dir: Path | str, globs: list[str], all_hits: bool = False) -> list[Path] | Path | None:
    hits_all: list[Path] = []
    root = Path(fish_dir)
    for pattern in globs:
        hits_all.extend(sorted(root.glob(pattern)))
    if all_hits:
        return hits_all
    return hits_all[0] if hits_all else None


def _scanimage_find_first(obj: Any, key: str) -> Any:
    key_lower = key.lower()
    if isinstance(obj, dict):
        for current_key, value in obj.items():
            if current_key.lower() == key_lower:
                return value
            found = _scanimage_find_first(value, key)
            if found is not None:
                return found
    elif isinstance(obj, list):
        for value in obj:
            found = _scanimage_find_first(value, key)
            if found is not None:
                return found
    return None


def _scanimage_to_pair(value: Any) -> tuple[float, float] | None:
    if isinstance(value, (list, tuple)) and len(value) >= 2:
        return float(value[0]), float(value[1])
    if isinstance(value, (int, float)):
        return float(value), float(value)
    return None


def scanimage_um_per_px_from_artist(tiff_path: Path | str) -> tuple[dict[str, float] | None, str]:
    with tifffile.TiffFile(tiff_path) as tif:
        artist_tag = tif.pages[0].tags.get("Artist")
        if artist_tag is None:
            return None, "missing Artist tag"
        raw = artist_tag.value
    if isinstance(raw, bytes):
        raw = raw.decode(errors="replace")
    raw = raw.replace("\x00", "")
    match = re.search(r"{.*}", raw, re.S)
    if match:
        raw = match.group(0)
    try:
        data = json.loads(raw)
    except Exception:
        return None, "invalid Artist JSON"
    fov_um = _scanimage_to_pair(_scanimage_find_first(data, "imagingFovUm"))
    pixel_res = _scanimage_to_pair(_scanimage_find_first(data, "pixelResolutionXY"))
    microns_per_pixel = _scanimage_to_pair(_scanimage_find_first(data, "micronsPerPixel"))
    if microns_per_pixel is None:
        microns_per_pixel = _scanimage_to_pair(_scanimage_find_first(data, "umPerPixel"))
    if microns_per_pixel:
        return {"X": microns_per_pixel[0], "Y": microns_per_pixel[1]}, "micronsPerPixel"
    if fov_um and pixel_res:
        return {"X": fov_um[0] / pixel_res[0], "Y": fov_um[1] / pixel_res[1]}, "imagingFovUm/pixelResolutionXY"
    return None, "missing ScanImage scale"


def infer_hcr_stack_paths(preproc_dir: Path | str | None, fish_id: str) -> list[Path]:
    if preproc_dir is None:
        return []
    preproc = Path(preproc_dir)
    if not preproc.exists():
        return []
    candidate_dirs = [preproc / "rbest", preproc / "rn", preproc / "confocal", preproc / "hcr"]
    hits: list[Path] = []
    for directory in candidate_dirs:
        if not directory.exists():
            continue
        for pattern in (
            f"{fish_id}_round*_channel*.nrrd",
            f"{fish_id}_round*_channel*.tif",
            f"{fish_id}_round*_channel*.tiff",
            "*HCR*.nrrd",
            "*HCR*.tif",
            "*HCR*.tiff",
        ):
            hits.extend(sorted(directory.glob(pattern)))
    out: list[Path] = []
    seen: set[Path] = set()
    for path in hits:
        if "gcamp" in path.name.lower():
            continue
        if path not in seen:
            out.append(path)
            seen.add(path)
    return out


def infer_hcr_stack_path(preproc_dir: Path | str | None, fish_id: str) -> Path | None:
    hits = infer_hcr_stack_paths(preproc_dir, fish_id)
    return hits[0] if hits else None


def infer_anat_labels_path(fish_dir: Path | str, fish_id: str) -> Path | None:
    candidate_dirs = [
        Path(fish_dir) / "03_analysis" / "structural" / "cp_masks",
        Path(fish_dir) / "03_analysis" / "functional" / "masks",
    ]
    patterns = [
        f"*{fish_id}*anatomy*cp_masks*.tif",
        f"*{fish_id}*cp_masks*.tif",
        "*anatomy*cp_masks*.tif",
        "*cp_masks*.tif",
    ]
    for directory in candidate_dirs:
        if not directory.exists():
            continue
        for pattern in patterns:
            hits = sorted(directory.glob(pattern))
            if hits:
                return hits[0]
    return None


def infer_hcr_label_paths(fish_dir: Path | str, fish_id: str) -> list[Path]:
    root = Path(fish_dir) / "03_analysis" / "confocal"
    raw_dir = root / "raw" / "cp_masks"
    aligned_dir = root / "aligned"
    if not raw_dir.exists() and not aligned_dir.exists():
        return []

    # Prefer raw cp-masks first, then aligned labels. Keep only canonical label stacks and
    # avoid downstream derivative artifacts (within-labels, overlays, matches/review tables).
    search_specs: list[tuple[Path, tuple[str, ...]]] = [
        (
            raw_dir,
            (
                f"{fish_id}_round*_channel*_cp_masks*.tif",
                f"{fish_id}_round*_cp_masks*.tif",
                f"{fish_id}_round*.tif",
                "*round*_channel*_cp_masks*.tif",
                "*round*_cp_masks*.tif",
            ),
        ),
        (
            aligned_dir,
            (
                f"{fish_id}_round*_channel*_cp_masks_in_2p_labels_uint16.tif",
                f"{fish_id}_round*_channel*_cp_masks_in_2p_labels.tif",
                f"{fish_id}_round*_channel*_cp_masks_in_2p.tif",
                f"{fish_id}_round*_cp_masks_in_2p_labels_uint16.tif",
                f"{fish_id}_round*_cp_masks_in_2p_labels.tif",
                f"{fish_id}_round*_cp_masks_in_2p.tif",
                "*round*_channel*_cp_masks_in_2p_labels_uint16.tif",
                "*round*_channel*_cp_masks_in_2p_labels.tif",
                "*round*_cp_masks_in_2p_labels_uint16.tif",
                "*round*_cp_masks_in_2p_labels.tif",
            ),
        ),
    ]
    excluded_tokens = (
        "_conf_within_",
        "_twop_within_",
        "_overlay_",
        "_matches",
        "_review",
        "_final_pairs",
        "_warp_meta",
        "debug_chain",
    )
    excluded_suffixes = {".csv", ".json", ".nrrd", ".html", ".png"}

    hits: list[Path] = []
    for directory, patterns in search_specs:
        if not directory.exists():
            continue
        for pattern in patterns:
            for hit in sorted(directory.glob(pattern)):
                name = hit.name.lower()
                if any(token in name for token in excluded_tokens):
                    continue
                if hit.suffix.lower() in excluded_suffixes:
                    continue
                hits.append(hit)

    out: list[Path] = []
    seen: set[Path] = set()
    for hit in hits:
        if hit not in seen:
            out.append(hit)
            seen.add(hit)
    return out


def infer_func_labels_path(
    fish_dir: Path | str,
    fish_id: str,
    *,
    outdir: Path | str | None = None,
    out_seg: Path | str | None = None,
) -> Path | None:
    candidate_dirs = [
        Path(path)
        for path in (
            out_seg,
            outdir,
            Path(fish_dir) / "03_analysis" / "functional" / "masks",
            Path(fish_dir) / "03_analysis" / "functional" / "segmentation",
        )
        if path is not None
    ]
    candidate_patterns = (
        f"*{fish_id}*func*label*.tif",
        f"*{fish_id}*func*mask*.tif",
        f"*{fish_id}*suite2p*label*.tif",
        f"*{fish_id}*suite2p*mask*.tif",
        f"*{fish_id}*roi*label*.tif",
        f"*{fish_id}*roi*mask*.tif",
        "*func*label*.tif",
        "*func*mask*.tif",
        "*suite2p*label*.tif",
        "*suite2p*mask*.tif",
        "*roi*label*.tif",
        "*roi*mask*.tif",
    )
    excluded_name_tokens = (
        "anatomy",
        "confocal",
        "hcr",
        "round",
        "channel",
        "cellpose",
        "cp_masks",
        "_cp_",
    )
    stack_like_hits: list[Path] = []
    seen: set[Path] = set()
    for directory in candidate_dirs:
        if not directory.exists():
            continue
        for pattern in candidate_patterns:
            for hit in sorted(directory.glob(pattern)):
                name_lower = hit.name.lower()
                if any(token in name_lower for token in excluded_name_tokens):
                    continue
                if re.search(r"plane\d+", name_lower):
                    continue
                if hit not in seen:
                    stack_like_hits.append(hit)
                    seen.add(hit)
    if len(stack_like_hits) == 1:
        return stack_like_hits[0]
    return None


def prepare_notebook_paths(ctx: FishContext, polarity_override: Any = None) -> dict[str, Any]:
    polarity, polarity_source = resolve_func_polarity(ctx.fish_id, ctx.matching_metadata_csv, polarity_override=polarity_override)
    func_raw_stack_path = first_match(ctx.fish_dir, ["01_raw/2p/functional/*.tif", "01_raw/2p/functional/*.tiff"])
    func_nonflipped_list = first_match(ctx.fish_dir, ["02_reg/00_preprocessing/2p_functional/02_motionCorrected/*mcorrected*.tif"], all_hits=True) or []
    anat_stack_path = first_match(ctx.fish_dir, ["02_reg/00_preprocessing/2p_anatomy/*_anatomy_2P_GCaMP.*"])
    hcr_stack_paths = infer_hcr_stack_paths(ctx.preproc_dir, ctx.fish_id)
    hcr_labels_paths = infer_hcr_label_paths(ctx.fish_dir, ctx.fish_id)
    anat_labels_path = infer_anat_labels_path(ctx.fish_dir, ctx.fish_id)
    func_labels_path = infer_func_labels_path(ctx.fish_dir, ctx.fish_id, outdir=ctx.outdir, out_seg=ctx.out_seg)
    vox_func_auto = None
    if func_raw_stack_path is not None:
        try:
            vox_func_auto, _ = scanimage_um_per_px_from_artist(func_raw_stack_path)
        except Exception:
            vox_func_auto = None
    return {
        "POLARITY_OVERRIDE": polarity_override,
        "POLARITY": polarity,
        "POLARITY_SOURCE": polarity_source,
        "POLARITY_NORTH": func_polarity_north(polarity),
        "FUNC_RAW_STACK_PATH": func_raw_stack_path,
        "FUNC_NONFLIPPED_LIST": func_nonflipped_list,
        "ANAT_STACK_PATH": anat_stack_path,
        "HCR_STACK_PATHS": hcr_stack_paths,
        "HCR_STACK_PATH": hcr_stack_paths[0] if hcr_stack_paths else None,
        "FLIPPED_LIST": [ctx.out_raw / f"{path.stem}_flipX.tif" for path in func_nonflipped_list],
        "FUNC_LABELS_PATH": func_labels_path,
        "ANAT_LABELS_PATH": anat_labels_path,
        "HCR_LABELS_PATHS": hcr_labels_paths,
        "HCR_LABELS_PATH": hcr_labels_paths[0] if hcr_labels_paths else None,
        "VOX_FUNC_AUTO": vox_func_auto,
    }


def resolve_notebook_context_stage(
    config: ContextStageConfig,
    *,
    nas_root: Path | str | None = None,
    local_root: Path | str | None = None,
) -> dict[str, Any]:
    ctx = resolve_fish_context(
        fish_id=config.fish_id,
        owner=config.owner,
        data_mode=config.data_mode,
        nas_root=nas_root,
        local_root=local_root,
        matching_metadata_csv_override=config.matching_metadata_csv_override,
        manifest_out_override=config.manifest_out_override,
        cellpose_model_root_override=config.cellpose_model_root_override,
        anat_cp_model_path_override=config.anat_cp_model_path_override,
        cp_hcr_model_path_override=config.cp_hcr_model_path_override,
        run_config=config.run_config,
    )
    bindings = notebook_bindings_from_context(ctx)
    paths = prepare_notebook_paths(ctx, polarity_override=config.polarity_override)
    log_lines = [
        f"[Paths] DATA_MODE={ctx.data_mode} DATA_ROOT={ctx.data_root}",
        f"[Paths] POLARITY={paths['POLARITY']} (source={paths['POLARITY_SOURCE']})",
        f"[Paths] MATCHING_METADATA_CSV={ctx.matching_metadata_csv}",
        f"[Paths] MANIFEST_OUT={ctx.manifest_out}",
        f"[Paths] CELLPOSE_MODEL_ROOT={ctx.cellpose_model_root}",
        f"[Paths] ANAT_CP_MODEL_PATH={ctx.anat_cp_model_path}",
        f"[Paths] CP_HCR_MODEL_PATH={ctx.cp_hcr_model_path}",
    ]
    return {
        "ctx": ctx,
        "bindings": bindings,
        "paths": paths,
        "run_config": dict(ctx.run_config),
        "log_lines": log_lines,
    }


def resolve_fish_state_stage(
    *,
    fish_id: str,
    current_state_fish_id: str | None,
    config: FishStateStageConfig | None = None,
) -> dict[str, Any]:
    cfg = config or FishStateStageConfig()
    state_fish_id = reset_fish_state(current_state_fish_id, fish_id, force=cfg.reset_always)
    log_lines = []
    if cfg.verbose:
        log_lines.append(f"[state] Set fish state marker to {state_fish_id}")
        if cfg.reset_always:
            log_lines.append("[state] RESET_FISH_STATE_ALWAYS=True (fish marker refreshed on every run)")
    return {
        "STATE_FISH_ID": state_fish_id,
        "ok": require_fish_state(state_fish_id, fish_id),
        "log_lines": log_lines,
    }


def build_run_config_stage(
    *,
    base_run_config: dict[str, Any] | None = None,
    user_run_config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    run_config = normalize_run_config({**(base_run_config or {}), **(user_run_config or {})})
    bindings = {
        "RUN_CONFIG": run_config,
        "HIGH_CONF_ONLY": run_config["HIGH_CONF_ONLY"],
        "SKIP_47": run_config["SKIP_47"],
        "STIM_ONSET_DELAY_SEC": run_config["STIM_ONSET_DELAY_SEC"],
        "TARGET_STIM_TYPES": run_config["TARGET_STIM_TYPES"],
        "STRICT_PAIR_INTEGRITY": run_config["STRICT_PAIR_INTEGRITY"],
        "REMOVE_INTERBLOCK_GAPS": run_config["REMOVE_INTERBLOCK_GAPS"],
        "FORCE_RECOMPUTE_VOXELS": run_config["FORCE_RECOMPUTE_VOXELS"],
        "FORCE_RECOMPUTE_HCR_WARP": run_config["FORCE_RECOMPUTE_HCR_WARP"],
        "RECOMPUTE_WARP": run_config["RECOMPUTE_WARP"],
        "RECOMPUTE_FILTER_STATS": run_config["RECOMPUTE_FILTER_STATS"],
        "RECOMPUTE_INTENSITY_WARP": run_config["RECOMPUTE_INTENSITY_WARP"],
        "RECOMPUTE_CONF_FUNC_PAIRS": run_config["RECOMPUTE_CONF_FUNC_PAIRS"],
        "RECOMPUTE_SUITE2P_TRACE_EXPORT": run_config["RECOMPUTE_SUITE2P_TRACE_EXPORT"],
        "RECOMPUTE_FUNC_WARP_EXPORT": run_config["RECOMPUTE_FUNC_WARP_EXPORT"],
        "EXPORT_BEST_ROUND_LABELS_TO_RBEST": run_config["EXPORT_BEST_ROUND_LABELS_TO_RBEST"],
        "HCR_WARP_SPLIT_CONNECTED_COMPONENTS": run_config["HCR_WARP_SPLIT_CONNECTED_COMPONENTS"],
        "HCR_WARP_COMPONENT_CONNECTIVITY": run_config["HCR_WARP_COMPONENT_CONNECTIVITY"],
        "HCR_WARP_MIN_COMPONENT_VOXELS": run_config["HCR_WARP_MIN_COMPONENT_VOXELS"],
        "HCR_WARP_MIN_COMPONENT_VOLUME_UM3": run_config["HCR_WARP_MIN_COMPONENT_VOLUME_UM3"],
        "HCR_WARP_DROP_LOW_Q": run_config["HCR_WARP_DROP_LOW_Q"],
        "HCR_WARP_LOW_FILTER_METRIC": run_config["HCR_WARP_LOW_FILTER_METRIC"],
        "HCR_WARP_FLAG_HIGH_Q": run_config["HCR_WARP_FLAG_HIGH_Q"],
        "HCR_WARP_HIGH_FILTER_METRIC": run_config["HCR_WARP_HIGH_FILTER_METRIC"],
        "CONF_FUNC_CSV_ANALYSIS": run_config["CONF_FUNC_CSV_ANALYSIS"],
        "GENE_ORDER": list(run_config["GENE_ORDER"]),
        "GENE_COLORS": dict(run_config["GENE_COLORS"]),
    }
    return {
        "bindings": bindings,
        "log_lines": [f"[config] RUN_CONFIG loaded with keys: {', '.join(sorted(run_config.keys()))}"],
    }


def build_context_audit_stage(
    *,
    fish_id: str,
    state_fish_id: str | None,
    canonical_checks: dict[str, Any],
    expected_paths: dict[str, Any] | None = None,
    strict: bool = True,
) -> dict[str, Any]:
    fish_audit_df = build_fish_state_audit_df(
        fish_id=fish_id,
        state_fish_id=state_fish_id,
        canonical_checks=canonical_checks,
        expected_paths=expected_paths,
    )
    n_fail = int((fish_audit_df["status"] == "fail").sum()) if not fish_audit_df.empty else 0
    n_warn = int((fish_audit_df["status"] == "warn").sum()) if not fish_audit_df.empty else 0
    if strict and n_fail > 0:
        raise RuntimeError(f"[fish-audit] failed with {n_fail} issue(s). Resolve rows with status='fail'.")
    return {
        "fish_audit_df": fish_audit_df,
        "n_fail": n_fail,
        "n_warn": n_warn,
        "log_lines": [f"[fish-audit] fish={fish_id} fail={n_fail} warn={n_warn}"],
    }


def _vox_complete(vox: dict[str, Any] | None) -> bool:
    try:
        return bool(vox) and all(vox.get(axis) is not None for axis in ("X", "Y", "Z"))
    except Exception:
        return False


def _read_json_dict(path: Path | str | None) -> dict[str, Any]:
    try:
        target = Path(path) if path is not None else None
        if target is None or not target.exists():
            return {}
        data = json.loads(target.read_text())
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _norm_vox(vox: Any) -> dict[str, Any]:
    if not isinstance(vox, dict):
        return {}
    out: dict[str, Any] = {}
    for axis in ("X", "Y", "Z"):
        value = vox.get(axis)
        if value is None:
            continue
        try:
            out[axis] = float(value)
        except Exception:
            out[axis] = value
    return out


def _merge_missing(primary: dict[str, Any] | None, fallback: dict[str, Any] | None) -> dict[str, Any]:
    out = dict(primary or {})
    fb = dict(fallback or {})
    for axis in ("X", "Y", "Z"):
        if out.get(axis) is None and fb.get(axis) is not None:
            out[axis] = fb.get(axis)
    return out


def _load_vox_cache(vox_cache_path: Path, legacy_vox_cache_path: Path) -> tuple[dict[str, Any], list[str]]:
    log_lines: list[str] = []
    for cache_path, label in ((vox_cache_path, "voxel cache"), (legacy_vox_cache_path, "legacy voxel cache")):
        if not Path(cache_path).exists():
            continue
        data = _read_json_dict(cache_path)
        if not data:
            continue
        if label == "legacy voxel cache":
            log_lines.append(f"[Info] Loaded legacy voxel cache: {cache_path}")
        return data, log_lines
    return {}, log_lines


def _cache_lookup(cache: dict[str, Any], path: Path | str | None = None, aliases: list[str] | tuple[str, ...] = ()) -> dict[str, Any]:
    if not isinstance(cache, dict):
        return {}
    by_path = cache.get("by_path", {}) if isinstance(cache.get("by_path"), dict) else {}
    by_alias = cache.get("by_alias", {}) if isinstance(cache.get("by_alias"), dict) else {}
    out: dict[str, Any] = {}
    if path is not None:
        path_key = str(Path(path))
        if path_key in by_path:
            out = _merge_missing(_norm_vox(by_path.get(path_key)), out)
        elif path_key in cache:
            out = _merge_missing(_norm_vox(cache.get(path_key)), out)
    for alias in aliases or ():
        if alias in by_alias:
            out = _merge_missing(out, _norm_vox(by_alias.get(alias)))
        elif alias in cache:
            out = _merge_missing(out, _norm_vox(cache.get(alias)))
    return out


def _path_from_data_root(path: Path | str | None, roots: list[Path | None]) -> str | None:
    if not path:
        return None
    target = Path(path)
    for root in roots:
        if not root:
            continue
        try:
            return str(target.relative_to(Path(root)))
        except Exception:
            continue
    return str(target)


def resolve_voxel_context_stage(
    *,
    analysis_dir: Path | str,
    outdir: Path | str,
    out_reg: Path | str,
    data_mode: str | None,
    func_stack_path: Path | str | None,
    func_raw_stack_path: Path | str | None,
    anat_stack_path: Path | str | None,
    hcr_stack_paths: list[Path | str] | None,
    hcr_stack_path: Path | str | None,
    vox_func_auto: dict[str, Any] | None,
    vox_func_manual: dict[str, Any] | None,
    vox_anat_manual: dict[str, Any] | None,
    vox_hcr_manual: dict[str, Any] | None,
    flipped_list: list[Path | str] | None,
    data_root: Path | str | None,
    local_root: Path | str | None,
    nas_root: Path | str | None,
    config: VoxelStageConfig | None = None,
) -> dict[str, Any]:
    cfg = config or VoxelStageConfig()
    analysis_dir = Path(analysis_dir)
    outdir = Path(outdir)
    out_reg = Path(out_reg)
    vox_cache_path = analysis_dir / "voxel_sizes.json"
    legacy_vox_cache_path = outdir / "voxel_sizes.json"
    run_metadata_path = out_reg / "run_metadata.json"

    data_mode_local = str(data_mode or "nas").strip().lower()
    flipped_list_local = [Path(path) for path in (flipped_list or []) if path]
    func_stack_path_local = Path(func_stack_path) if func_stack_path else None
    func_raw_stack_path_local = Path(func_raw_stack_path) if func_raw_stack_path else None
    anat_stack_path_local = Path(anat_stack_path) if anat_stack_path else None
    hcr_stack_paths_local = [Path(path) for path in (hcr_stack_paths or []) if path]
    hcr_stack_path_local = Path(hcr_stack_path) if hcr_stack_path else None
    if not hcr_stack_paths_local and hcr_stack_path_local is not None:
        hcr_stack_paths_local = [hcr_stack_path_local]

    vox_func_auto_local = dict(vox_func_auto) if isinstance(vox_func_auto, dict) else {}
    vox_func_manual_local = dict(vox_func_manual) if isinstance(vox_func_manual, dict) else {}
    vox_anat_manual_local = dict(vox_anat_manual) if isinstance(vox_anat_manual, dict) else {}
    vox_hcr_manual_local = dict(vox_hcr_manual) if isinstance(vox_hcr_manual, dict) else {}

    roots = [
        Path(data_root) if data_root else None,
        Path(local_root) if local_root else None,
        Path(nas_root) if nas_root else None,
    ]

    log_lines: list[str] = []
    cache_data, cache_logs = _load_vox_cache(vox_cache_path, legacy_vox_cache_path)
    log_lines.extend(cache_logs)
    run_meta = _read_json_dict(run_metadata_path)
    run_meta_voxels = {key: {} for key in ("func", "anat", "hcr")}
    if isinstance(run_meta.get("voxels"), dict):
        for key in ("func", "anat", "hcr"):
            run_meta_voxels[key] = _norm_vox(run_meta["voxels"].get(key))

    func_paths = flipped_list_local if flipped_list_local else ([func_stack_path_local] if func_stack_path_local else [])
    func_scale: dict[str, Any] = {}
    func_scale_source: str | None = None
    if vox_func_auto_local:
        func_scale = _norm_vox(vox_func_auto_local)
        func_scale_source = "raw ScanImage metadata"
    if not _vox_complete(func_scale) and _vox_complete(run_meta_voxels.get("func")):
        func_scale = _merge_missing(run_meta_voxels.get("func"), func_scale)
        func_scale_source = "cached run_metadata.json"
    if not _vox_complete(func_scale):
        raw_aliases: list[str] = []
        if func_raw_stack_path_local:
            raw_aliases.append(f"func_raw_{func_raw_stack_path_local.name}")
        raw_aliases.append("func")
        cached_func_scale = _cache_lookup(cache_data, path=func_raw_stack_path_local, aliases=raw_aliases)
        if _vox_complete(cached_func_scale):
            func_scale = _merge_missing(cached_func_scale, func_scale)
            func_scale_source = "cached voxel_sizes.json"
    if (not _vox_complete(func_scale)) and func_raw_stack_path_local and func_raw_stack_path_local.exists():
        try:
            raw_vox = load_or_cache_voxels(func_raw_stack_path_local, f"func_raw_{func_raw_stack_path_local.name}") or {}
            if raw_vox:
                func_scale = _merge_missing(_norm_vox(raw_vox), func_scale)
                func_scale_source = "raw functional header"
        except Exception:
            pass
    if func_scale_source is not None:
        log_lines.append(f"[Vox] Functional voxel fallback source: {func_scale_source}")

    vox_func_by_path: dict[str, dict[str, Any]] = {}
    for func_path in func_paths:
        aliases = [f"func_{func_path.name}", func_path.name, "func"]
        vox = _cache_lookup(cache_data, path=func_path, aliases=aliases)
        vox = _merge_missing(vox, run_meta_voxels.get("func"))
        vox = _merge_missing(vox, func_scale)
        if (not _vox_complete(vox)) and func_path.exists():
            try:
                inferred = load_or_cache_voxels(func_path, f"func_{func_path.name}") or {}
            except Exception:
                inferred = {}
            vox = _merge_missing(_norm_vox(inferred), vox)
        if vox.get("Z") is None and (vox.get("X") is not None or vox.get("Y") is not None):
            vox["Z"] = func_scale.get("Z", 1.0)
        vox_func_by_path[str(func_path)] = dict(vox)

    vox_anat = _cache_lookup(cache_data, path=anat_stack_path_local, aliases=["anat"])
    vox_anat = _merge_missing(vox_anat, run_meta_voxels.get("anat"))
    if (not _vox_complete(vox_anat)) and anat_stack_path_local and anat_stack_path_local.exists():
        try:
            inferred = load_or_cache_voxels(anat_stack_path_local, "anat") or {}
            vox_anat = _merge_missing(_norm_vox(inferred), vox_anat)
        except Exception:
            pass

    hcr_stack_paths_local = list(hcr_stack_paths_local)
    if not hcr_stack_paths_local and hcr_stack_path_local:
        hcr_stack_paths_local = [hcr_stack_path_local]
    vox_hcr_by_path: dict[str, dict[str, Any]] = {}
    for hcr_path in hcr_stack_paths_local:
        aliases = [f"hcr_{hcr_path.name}", hcr_path.name, "hcr"]
        vox = _cache_lookup(cache_data, path=hcr_path, aliases=aliases)
        vox = _merge_missing(vox, run_meta_voxels.get("hcr"))
        if (not _vox_complete(vox)) and hcr_path.exists():
            try:
                inferred = load_or_cache_voxels(hcr_path, f"hcr_{hcr_path.name}") or {}
                vox = _merge_missing(_norm_vox(inferred), vox)
            except Exception:
                pass
        vox_hcr_by_path[str(hcr_path)] = dict(vox)
    vox_hcr = vox_hcr_by_path.get(str(hcr_stack_path_local), {}) if hcr_stack_path_local else None

    if vox_func_manual_local:
        for vox in vox_func_by_path.values():
            for axis in ("X", "Y", "Z"):
                value = vox_func_manual_local.get(axis)
                if value is not None:
                    try:
                        vox[axis] = float(value)
                    except Exception:
                        vox[axis] = value
    if vox_anat_manual_local:
        for axis in ("X", "Y", "Z"):
            value = vox_anat_manual_local.get(axis)
            if value is not None:
                try:
                    vox_anat[axis] = float(value)
                except Exception:
                    vox_anat[axis] = value
    if vox_hcr_manual_local:
        if vox_hcr_by_path:
            for vox in vox_hcr_by_path.values():
                for axis in ("X", "Y", "Z"):
                    value = vox_hcr_manual_local.get(axis)
                    if value is not None:
                        try:
                            vox[axis] = float(value)
                        except Exception:
                            vox[axis] = value
        if vox_hcr is None:
            vox_hcr = {}
        for axis in ("X", "Y", "Z"):
            value = vox_hcr_manual_local.get(axis)
            if value is not None:
                try:
                    vox_hcr[axis] = float(value)
                except Exception:
                    vox_hcr[axis] = value

    vox_func = vox_func_by_path.get(str(func_paths[0]), {}) if func_paths else {}

    rows: list[dict[str, Any]] = []
    for func_path, vox in vox_func_by_path.items():
        rows.append(
            {
                "dataset": "func",
                "path": _path_from_data_root(func_path, roots),
                "X_um": vox.get("X"),
                "Y_um": vox.get("Y"),
                "Z_um": vox.get("Z"),
            }
        )
    rows.append(
        {
            "dataset": "anat",
            "path": _path_from_data_root(anat_stack_path_local, roots) if anat_stack_path_local else None,
            "X_um": vox_anat.get("X") if vox_anat else None,
            "Y_um": vox_anat.get("Y") if vox_anat else None,
            "Z_um": vox_anat.get("Z") if vox_anat else None,
        }
    )
    for hcr_path in hcr_stack_paths_local:
        vox = vox_hcr_by_path.get(str(hcr_path), {}) if vox_hcr_by_path else (vox_hcr or {})
        rows.append(
            {
                "dataset": "hcr",
                "path": _path_from_data_root(hcr_path, roots),
                "X_um": vox.get("X") if vox else None,
                "Y_um": vox.get("Y") if vox else None,
                "Z_um": vox.get("Z") if vox else None,
            }
        )
    df_vox = pd.DataFrame(rows)
    if not df_vox.empty:
        df_vox["complete"] = df_vox[["X_um", "Y_um", "Z_um"]].notna().all(axis=1)

    return {
        "bindings": {
            "VOX_CACHE_PATH": vox_cache_path,
            "LEGACY_VOX_CACHE_PATH": legacy_vox_cache_path,
            "RUN_METADATA_PATH": run_metadata_path,
            "FORCE_RECOMPUTE_VOXELS": bool(cfg.force_recompute_voxels),
            "HCR_STACK_PATHS": list(hcr_stack_paths_local),
            "VOX_FUNC_BY_PATH": vox_func_by_path,
            "VOX_FUNC": vox_func,
            "VOX_ANAT": dict(vox_anat or {}),
            "VOX_HCR_BY_PATH": vox_hcr_by_path,
            "VOX_HCR": vox_hcr,
            "df_vox": df_vox,
        },
        "df_vox": df_vox,
        "log_lines": log_lines,
        "cache_data": cache_data,
        "run_metadata_voxels": run_meta_voxels,
    }


def build_voxel_debug_stage(
    *,
    anat_stack_path: Path | str | None,
    voxel_cache_path: Path | str | None,
    legacy_voxel_cache_path: Path | str | None,
) -> dict[str, Any]:
    anat_path = Path(anat_stack_path) if anat_stack_path else None
    hdr_vox: dict[str, Any] | None = None
    log_lines = [f"[VoxDbg] ANAT_STACK_PATH={anat_path}"]
    if anat_path and anat_path.exists():
        try:
            raw_header = {}
            with anat_path.open("rb") as handle:
                header_bytes = b""
                for _ in range(512):
                    line = handle.readline()
                    if not line:
                        break
                    header_bytes += line
                    if line.strip() == b"" or len(header_bytes) > 65536:
                        break
            text = header_bytes.decode("latin-1", errors="replace")
            for line in text.splitlines():
                if (not line) or line.startswith("#") or ":" not in line:
                    continue
                key, value = line.split(":", 1)
                raw_header[key.strip().lower()] = value.strip()
            log_lines.append(f"[VoxDbg] nrrd space units: {raw_header.get('space units')}")
            log_lines.append(f"[VoxDbg] nrrd space directions: {raw_header.get('space directions')}")
            hdr_vox = _infer_voxels_nrrd(anat_path)
            log_lines.append(f"[VoxDbg] inferred vox (um): {hdr_vox}")
        except Exception as exc:
            log_lines.append(f"[VoxDbg] header error: {exc}")
    else:
        log_lines.append("[VoxDbg] anatomy path missing")

    cache_hits: dict[str, Any] = {}
    for label, cache_path in (
        ("VOX_CACHE_PATH", Path(voxel_cache_path) if voxel_cache_path else None),
        ("LEGACY_VOX_CACHE_PATH", Path(legacy_voxel_cache_path) if legacy_voxel_cache_path else None),
    ):
        if cache_path is None:
            continue
        value: Any = None
        if cache_path.exists() and anat_path is not None:
            try:
                data = json.loads(cache_path.read_text())
                key = str(anat_path)
                if key in data:
                    value = data[key]
                else:
                    by_path = data.get("by_path", {})
                    value = by_path.get(key) if isinstance(by_path, dict) else None
            except Exception as exc:
                value = f"read failed: {exc}"
        cache_hits[label] = value
        log_lines.append(f"[VoxDbg] cache {label}={cache_path} -> {value}")

    ratios: dict[str, dict[str, float] | None] = {}
    for label, cache_vox in cache_hits.items():
        if not isinstance(cache_vox, dict) or not isinstance(hdr_vox, dict):
            ratios[label] = None
            continue
        ratio_map: dict[str, float] = {}
        for axis in ("X", "Y", "Z"):
            cache_value = cache_vox.get(axis)
            header_value = hdr_vox.get(axis)
            if cache_value is None or header_value in (None, 0):
                continue
            ratio_map[axis] = float(cache_value) / float(header_value)
        ratios[label] = ratio_map or None
        if ratios[label]:
            log_lines.append(f"[VoxDbg] cache/header ratio for {label}: {ratios[label]}")

    return {
        "cache_hits": cache_hits,
        "hdr_vox": hdr_vox,
        "ratios": ratios,
        "log_lines": log_lines,
    }


def orient_functional_stacks_stage(
    *,
    func_nonflipped_list: list[Path | str] | None,
    flipped_list: list[Path | str] | None,
    out_raw: Path | str,
    fish_id: str | None,
    polarity: str | None,
    polarity_source: str | None,
    resolve_func_polarity_func: Any = None,
    apply_func_orientation_func: Any = None,
    config: FunctionalOrientationStageConfig | None = None,
) -> dict[str, Any]:
    cfg = config or FunctionalOrientationStageConfig()
    orient_manifest_path = Path(out_raw) / "functional_orientation_manifest.json"
    log_lines: list[str] = []

    if callable(resolve_func_polarity_func):
        try:
            resolve_func_polarity_func(force_refresh=True)
        except Exception as exc:
            log_lines.append(f"[WARN] Failed to resolve POLARITY in [10]: {exc}")

    mode = "rot180+flipX" if str(polarity or "").lower() == "north" else "flipX"
    source_paths = [Path(path) for path in (func_nonflipped_list or []) if path]
    target_paths = [Path(path) for path in (flipped_list or []) if path]

    def orient_sample(arr: Any) -> Any:
        if arr is None:
            return None
        out = np.asarray(arr)
        if out.ndim < 2:
            return out
        if callable(apply_func_orientation_func):
            try:
                return apply_func_orientation_func(out)
            except Exception:
                pass
        return apply_func_orientation(out, polarity=polarity, flip_x=True)

    def finalize_dtype(arr: Any) -> Any:
        if getattr(arr, "dtype", None) == np.int16:
            return (np.asarray(arr, dtype=np.int32) + 32768).clip(0, 65535).astype(np.uint16)
        return arr

    def load_manifest() -> dict[str, Any]:
        if not orient_manifest_path.exists():
            return {}
        try:
            data = json.loads(orient_manifest_path.read_text())
            return data if isinstance(data, dict) else {}
        except Exception as exc:
            log_lines.append(f"[WARN] Could not read orientation manifest {orient_manifest_path}: {exc}")
            return {}

    def save_manifest(manifest: dict[str, Any]) -> None:
        try:
            orient_manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True))
        except Exception as exc:
            log_lines.append(f"[WARN] Could not write orientation manifest {orient_manifest_path}: {exc}")

    def manifest_entry_current(tracked: dict[str, Any], src_path: Path, current_mode: str, current_fish_id: str) -> bool:
        if not isinstance(tracked, dict) or not tracked:
            return False
        try:
            src_stat = src_path.stat()
        except Exception:
            return False
        try:
            tracked_src_mtime_ns = int(tracked.get("src_mtime_ns", -1))
            tracked_src_size = int(tracked.get("src_size", -1))
            tracked_cache_version = int(tracked.get("cache_version", 0))
        except Exception:
            return False
        return (
            str(tracked.get("mode", "")) == str(current_mode)
            and str(tracked.get("fish_id", "")) == str(current_fish_id)
            and str(tracked.get("src", "")) == str(src_path)
            and tracked_src_mtime_ns == int(src_stat.st_mtime_ns)
            and tracked_src_size == int(src_stat.st_size)
            and tracked_cache_version >= int(cfg.cache_version)
        )

    def sample_page_indices(tif: tifffile.TiffFile) -> list[int]:
        n_pages = len(getattr(tif, "pages", []))
        if n_pages <= 1:
            return [0]
        return sorted({0, int(n_pages // 2), int(n_pages - 1)})

    def validate_existing_oriented_stack(src_path: Path, dst_path: Path, current_mode: str) -> tuple[bool, str]:
        try:
            src_stat = src_path.stat()
            dst_stat = dst_path.stat()
        except Exception as exc:
            return False, f"stat_error:{exc}"
        if int(dst_stat.st_mtime_ns) < int(src_stat.st_mtime_ns):
            return False, "dst_older_than_src"
        try:
            with tifffile.TiffFile(str(src_path)) as tif_src, tifffile.TiffFile(str(dst_path)) as tif_dst:
                src_pages = len(getattr(tif_src, "pages", []))
                dst_pages = len(getattr(tif_dst, "pages", []))
                if src_pages != dst_pages:
                    return False, f"page_count_mismatch:{src_pages}!={dst_pages}"
                page_indices = sample_page_indices(tif_src)
                for page_idx in page_indices:
                    src_page = tif_src.pages[int(page_idx)].asarray()
                    dst_page = tif_dst.pages[int(page_idx)].asarray()
                    expected = finalize_dtype(orient_sample(src_page))
                    if expected.shape != dst_page.shape:
                        return False, f"shape_mismatch_page{page_idx}"
                    if expected.dtype != dst_page.dtype:
                        try:
                            expected = expected.astype(dst_page.dtype, copy=False)
                        except Exception:
                            return False, f"dtype_mismatch_page{page_idx}"
                    if not np.array_equal(expected, dst_page):
                        return False, f"content_mismatch_page{page_idx}"
        except Exception as exc:
            return False, f"validate_error:{exc}"
        return True, f"verified_existing_stack pages={page_indices}"

    if not source_paths:
        log_lines.append("[WARN] No unflipped functional stacks found")
        return {
            "bindings": {
                "ORIENT_MANIFEST_PATH": orient_manifest_path,
                "ORIENT_CACHE_VERSION": int(cfg.cache_version),
            },
            "log_lines": log_lines,
            "mode": mode,
        }

    orient_manifest = load_manifest()
    fish_id_str = str(fish_id)
    log_lines.append(
        f"[10] mode={mode} polarity={polarity} source={polarity_source} "
        f"overwrite={cfg.overwrite_flipped} stacks={len(source_paths)}"
    )
    for idx, (src_path, dst_path) in enumerate(zip(source_paths, target_paths), start=1):
        if not src_path.exists():
            log_lines.append(f"[WARN] Non-flipped functional not found: {src_path}")
            continue
        manifest_key = str(dst_path)
        tracked = orient_manifest.get(manifest_key, {})
        tracked_mode = str(tracked.get("mode", "")) or "untracked"
        tracked_fish = str(tracked.get("fish_id", "")) or "unknown"
        cache_reason: str | None = None
        if dst_path.exists() and (not cfg.overwrite_flipped):
            if manifest_entry_current(tracked, src_path, mode, fish_id_str):
                log_lines.append(f"[INFO] Using existing oriented stack ({mode}): {dst_path}")
                continue
            adopt_ok, cache_reason = validate_existing_oriented_stack(src_path, dst_path, mode)
            if adopt_ok:
                src_stat = src_path.stat()
                orient_manifest[manifest_key] = {
                    "src": str(src_path),
                    "dst": str(dst_path),
                    "fish_id": fish_id_str,
                    "mode": mode,
                    "polarity": polarity,
                    "polarity_source": polarity_source,
                    "src_mtime_ns": int(src_stat.st_mtime_ns),
                    "src_size": int(src_stat.st_size),
                    "cache_version": int(cfg.cache_version),
                    "validation": cache_reason,
                }
                log_lines.append(f"[INFO] Using existing oriented stack ({mode}): {dst_path} [{cache_reason}]")
                continue
        if dst_path.exists() and not cfg.overwrite_flipped:
            reason_bits = [
                f"cached entry is {tracked_mode} for fish={tracked_fish}",
                f"current mode={mode} fish={fish_id_str}",
            ]
            if cache_reason:
                reason_bits.append(f"validation={cache_reason}")
            log_lines.append("[INFO] Rebuilding oriented stack because " + "; ".join(reason_bits))

        log_lines.append(f"[10] [{idx}/{len(source_paths)}] orienting {src_path} -> {dst_path}")
        read_start = time.time()
        arr_nf = tifffile.imread(src_path)
        read_elapsed = time.time() - read_start
        log_lines.append(
            f"[10] [{idx}/{len(source_paths)}] loaded shape={getattr(arr_nf, 'shape', None)} "
            f"dtype={getattr(arr_nf, 'dtype', None)} in {read_elapsed:.1f}s"
        )
        arr_or = finalize_dtype(orient_sample(arr_nf))
        write_start = time.time()
        tifffile.imwrite(dst_path, arr_or)
        write_elapsed = time.time() - write_start
        src_stat = src_path.stat()
        orient_manifest[manifest_key] = {
            "src": str(src_path),
            "dst": str(dst_path),
            "fish_id": fish_id_str,
            "mode": mode,
            "polarity": polarity,
            "polarity_source": polarity_source,
            "src_mtime_ns": int(src_stat.st_mtime_ns),
            "src_size": int(src_stat.st_size),
            "cache_version": int(cfg.cache_version),
            "validation": "written_by_[10]",
        }
        log_lines.append(
            f"[INFO] Saved oriented stack to {dst_path} "
            f"(mode={mode}, polarity={polarity}, dtype={arr_or.dtype}, write_s={write_elapsed:.1f})"
        )

    save_manifest(orient_manifest)
    return {
        "bindings": {
            "ORIENT_MANIFEST_PATH": orient_manifest_path,
            "ORIENT_CACHE_VERSION": int(cfg.cache_version),
        },
        "log_lines": log_lines,
        "mode": mode,
    }


def build_registration_helper_stage(*, polarity: str | None) -> dict[str, Any]:
    def _apply_orient(arr: Any) -> np.ndarray:
        return apply_func_orientation(arr, polarity=polarity, flip_x=True)

    def _ensure_float32(arr: Any) -> np.ndarray:
        return np.asarray(arr, dtype=np.float32)

    def _resize_like(arr: Any, out_shape: tuple[int, ...] | list[int]) -> np.ndarray:
        arr32 = _ensure_float32(arr)
        target_shape = tuple(int(v) for v in out_shape)
        if arr32.shape == target_shape:
            return arr32
        return transform.resize(
            arr32,
            target_shape,
            order=1,
            preserve_range=True,
            anti_aliasing=True,
        ).astype(np.float32, copy=False)

    def _corr2(arr_a: Any, arr_b: Any) -> float:
        return corrcoef_img(_ensure_float32(arr_a), _ensure_float32(arr_b))

    return {
        "_apply_func_orientation": _apply_orient,
        "_apply_func_orient": _apply_orient,
        "_ensure_float32": _ensure_float32,
        "_resize_like": _resize_like,
        "_corr2": _corr2,
    }


def build_final_fish_audit_stage(
    *,
    fish_id: str,
    namespace: dict[str, Any],
    config: FinalFishAuditConfig | None = None,
) -> dict[str, Any]:
    cfg = config or FinalFishAuditConfig()
    current_fish = str(fish_id)

    def _maybe(name: str) -> Any:
        return namespace.get(name)

    def _pathlike_key(name: str) -> bool:
        upper = str(name).upper()
        return any(token in upper for token in ("PATH", "DIR", "ROOT", "CSV", "OUT", "FILE"))

    def _is_pathlike_value(value: Any) -> bool:
        if isinstance(value, Path):
            return True
        if isinstance(value, str):
            return ("/" in value) or ("\\" in value) or value.endswith((".tif", ".tiff", ".nrrd", ".csv", ".json", ".npy"))
        return False

    def _is_reference_metadata_df(df: pd.DataFrame) -> bool:
        cols = {str(column) for column in df.columns}
        return ("fish_id" in cols) and ({"best_round", "num_rounds"} <= cols)

    rows: list[dict[str, Any]] = []

    def _add(scope: str, key: str, status: str, detail: str, sample: Any = None) -> None:
        rows.append(
            {
                "scope": scope,
                "key": key,
                "status": status,
                "detail": detail,
                "sample": None if sample is None else str(sample),
            }
        )

    tag_keys = [
        "STATE_FISH_ID",
        "SUITE2P_FISH_ID",
        "DF_STIM_FISH_ID",
        "MIDLINE_FISH_ID",
        "STIM_IPSI_CONTRA_FISH_ID",
        "BPI_FISH_ID",
        "BPI_ACTIVITY_FISH_ID",
        "FIG_53A_FISH_ID",
        "FIG_56_FISH_ID",
        "FIG_56H_FISH_ID",
    ]
    for key in tag_keys:
        value = _maybe(key)
        status = "ok" if value in (None, current_fish) else "fail"
        detail = f"value={value}" if status == "ok" else f"mismatch: value={value}, expected={current_fish}"
        _add("tag", key, status, detail, sample=value if status != "ok" else None)

    for key, value in namespace.items():
        if not _pathlike_key(key) and not _is_pathlike_value(value):
            continue
        if not _is_pathlike_value(value):
            continue
        tokens = _extract_fish_tokens(value)
        if tokens and any(token != current_fish for token in tokens):
            _add("global-path", key, "fail", f"stale fish token(s)={tokens}, current={current_fish}", sample=value)

    for key, value in namespace.items():
        if not isinstance(value, (list, tuple)):
            continue
        stale_items = []
        checked = 0
        for item in value:
            if not _is_pathlike_value(item):
                continue
            checked += 1
            tokens = _extract_fish_tokens(item)
            if tokens and any(token != current_fish for token in tokens):
                stale_items.append((item, tokens))
        if stale_items:
            example_item, example_tokens = stale_items[0]
            _add(
                "global-list",
                key,
                "fail",
                f"{len(stale_items)}/{checked} stale item(s), current={current_fish}, example_tokens={example_tokens}",
                sample=example_item,
            )

    if cfg.include_internal_df:
        for key, value in namespace.items():
            if not isinstance(value, pd.DataFrame):
                continue
            if _is_reference_metadata_df(value):
                continue
            if "fish_id" not in value.columns:
                continue
            fish_vals = value["fish_id"].dropna().astype(str).unique().tolist()[: max(1, int(cfg.max_rows_per_col))]
            stale = [item for item in fish_vals if item != current_fish]
            if stale:
                _add("dataframe", key, "fail", f"stale fish_id values present: {stale[:5]}", sample=stale[0])

    audit_df = pd.DataFrame(rows)
    if audit_df.empty:
        audit_df = pd.DataFrame(columns=["scope", "key", "status", "detail", "sample"])
    n_fail = int((audit_df["status"] == "fail").sum()) if not audit_df.empty else 0
    report_name = f"{current_fish}_fish_audit_final.csv"
    report_path = Path(namespace.get("OUT_QA", Path.cwd())) / report_name
    report_path.parent.mkdir(parents=True, exist_ok=True)
    audit_df.to_csv(report_path, index=False)
    if cfg.strict and n_fail > 0:
        raise RuntimeError(f"[fish-audit-final] failed with {n_fail} issue(s): {report_path}")
    return {
        "fish_audit_final_df": audit_df,
        "fish_audit_final_report": report_path,
        "n_fail": n_fail,
        "log_lines": [f"[fish-audit-final] fish={current_fish} fail={n_fail} -> {report_path}"],
    }


def reset_fish_state(current_state_fish_id: str | None, fish_id: str, *, force: bool = False) -> str:
    if force or current_state_fish_id != fish_id:
        return str(fish_id)
    return str(current_state_fish_id)


def require_fish_state(state_fish_id: str | None, fish_id: str) -> bool:
    return str(state_fish_id) == str(fish_id)


def _extract_fish_tokens(value: Any) -> list[str]:
    if value is None:
        return []
    return sorted(set(_FISH_TOKEN_RE.findall(str(value))))


def _is_stale_for_current_fish(value: Any, current_fish: str) -> bool:
    tokens = _extract_fish_tokens(value)
    return bool(tokens) and any(token != str(current_fish) for token in tokens)


def build_fish_state_audit_df(
    *,
    fish_id: str,
    state_fish_id: str | None,
    canonical_checks: dict[str, Any],
    expected_paths: dict[str, Any] | None = None,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for key, value in canonical_checks.items():
        if value is None:
            rows.append({"scope": "canonical", "key": key, "status": "fail", "value": None, "detail": "missing canonical state"})
        elif _is_stale_for_current_fish(value, fish_id):
            rows.append(
                {
                    "scope": "canonical",
                    "key": key,
                    "status": "fail",
                    "value": str(value),
                    "detail": f"stale fish token(s) {_extract_fish_tokens(value)}",
                }
            )
        else:
            rows.append({"scope": "canonical", "key": key, "status": "ok", "value": str(value), "detail": ""})

    if state_fish_id is not None:
        rows.append({"scope": "state", "key": "STATE_FISH_ID", "status": "ok" if str(state_fish_id) == str(fish_id) else "fail", "value": str(state_fish_id), "detail": ""})

    for key, payload in (expected_paths or {}).items():
        expected = payload.get("expected")
        current = payload.get("current")
        if expected is None:
            rows.append({"scope": "path", "key": key, "status": "warn", "value": str(current) if current is not None else None, "detail": "expected path unavailable"})
        elif current is None:
            rows.append({"scope": "path", "key": key, "status": "warn", "value": None, "detail": f"expected {expected}"})
        elif Path(str(current)) != Path(str(expected)):
            rows.append({"scope": "path", "key": key, "status": "warn", "value": str(current), "detail": f"expected {expected}"})
        else:
            rows.append({"scope": "path", "key": key, "status": "ok", "value": str(current), "detail": ""})

    audit_df = pd.DataFrame(rows)
    if audit_df.empty:
        return audit_df
    status_order = {"fail": 0, "warn": 1, "ok": 2}
    audit_df["_ord"] = audit_df["status"].map(status_order).fillna(9)
    return audit_df.sort_values(["_ord", "scope", "key"]).drop(columns=["_ord"]).reset_index(drop=True)


__all__ = [
    "ContextStageConfig",
    "DEFAULT_RUN_CONFIG",
    "FinalFishAuditConfig",
    "FunctionalOrientationStageConfig",
    "FORCE_TRUE_RUN_CONFIG_KEYS",
    "FishStateStageConfig",
    "FishContext",
    "VoxelStageConfig",
    "build_context_audit_stage",
    "build_final_fish_audit_stage",
    "build_fish_state_audit_df",
    "build_registration_helper_stage",
    "build_voxel_debug_stage",
    "build_run_config_stage",
    "default_cellpose_model_root",
    "default_local_root",
    "default_matching_metadata_csv",
    "default_nas_root",
    "first_match",
    "infer_anat_labels_path",
    "infer_func_labels_path",
    "infer_hcr_label_paths",
    "infer_hcr_stack_path",
    "infer_hcr_stack_paths",
    "func_orientation_effective",
    "func_orientation_mode",
    "func_polarity_north",
    "normalize_run_config",
    "normalize_polarity_value",
    "notebook_bindings_from_context",
    "owner_root",
    "prepare_notebook_paths",
    "read_matching_metadata_polarity",
    "require_fish_state",
    "reset_fish_state",
    "resolve_voxel_context_stage",
    "resolve_fish_state_stage",
    "resolve_func_polarity",
    "resolve_notebook_context_stage",
    "resolve_fish_context",
    "resolve_fish_dir",
    "orient_functional_stacks_stage",
    "scanimage_um_per_px_from_artist",
]
