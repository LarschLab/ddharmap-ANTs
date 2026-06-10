"""Context and fish-scoped stage helpers for notebook cells [4], [4a], [4b], [4c], [8], [8a], [10], [14], and [14a]."""

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

from .spatial import _infer_voxels_nrrd, _res_to_um_per_px, apply_func_orientation, corrcoef_img, load_or_cache_voxels
from .runtime import default_local_root as runtime_default_local_root


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
    local_root: Path | None
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
    local_root_override: Path | str | None = None
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
    save_oriented_stacks: bool = False
    cache_version: int = 2


@dataclass(frozen=True)
class AnatomyNormalizationStageConfig:
    force_recompute_anat_convert: bool = False


@dataclass(frozen=True)
class AnatomyUint8PreprocessingConfig:
    force_recompute_anat_uint8: bool = False
    use_source_path_orig: bool = True
    apply_func_orientation: bool = True
    flip_z_for_registration: bool = True
    target_xy_shape: tuple[int, int] | None = (750, 750)
    write_registration_nrrd: bool = True
    cache_version: int = 3


def default_nas_root() -> Path:
    if os.name == "nt":
        return Path(r"\\nasdcsr.unil.ch\RECHERCHE\FAC\FBM\CIG\jlarsch\default\D2c\07_Data")
    return Path("/Volumes/jlarsch/default/D2c/07_Data")


def default_local_root() -> Path:
    root = runtime_default_local_root(strict=True)
    assert root is not None
    return root


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
        lowercase = root / "cellpose" / "models"
        if lowercase.exists():
            return lowercase
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
    if mode == "local":
        local = Path(local_root) if local_root is not None else default_local_root()
    else:
        local = Path(local_root) if local_root is not None else None
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


def _scanimage_artist_json(raw: Any) -> str | None:
    if isinstance(raw, bytes):
        raw = raw.decode(errors="replace")
    if raw is None:
        return None
    text = str(raw).replace("\x00", "")
    marker = text.find("Artist")
    search_from = marker if marker >= 0 else 0
    start = text.find("{", search_from)
    if start < 0:
        start = text.find("{")
    if start < 0:
        return None
    depth = 0
    in_string = False
    escaped = False
    for idx in range(start, len(text)):
        char = text[idx]
        if in_string:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                in_string = False
            continue
        if char == '"':
            in_string = True
        elif char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                return text[start : idx + 1]
    return None


def scanimage_um_per_px_from_artist(tiff_path: Path | str) -> tuple[dict[str, float] | None, str]:
    with tifffile.TiffFile(tiff_path) as tif:
        artist_tag = tif.pages[0].tags.get("Artist")
        raw = artist_tag.value if artist_tag is not None else None
        if raw is None and isinstance(tif.imagej_metadata, dict):
            raw = tif.imagej_metadata.get("Info")
    raw_json = _scanimage_artist_json(raw)
    if raw_json is None:
        return None, "missing Artist JSON"
    try:
        data = json.loads(raw_json)
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


def _is_uint8_anatomy_preprocess_path(path: Path | str) -> bool:
    target = Path(path)
    return target.suffix.lower() in {".tif", ".tiff"} and target.stem.endswith("_uint8")


def _anatomy_uint8_source_stem(path: Path | str) -> str:
    stem = Path(path).stem
    return stem[:-6] if stem.endswith("_uint8") else stem


def _canonical_anatomy_registration_nrrd_path(uint8_path: Path | str) -> Path:
    path = Path(uint8_path)
    stem = path.stem
    fish_match = _FISH_TOKEN_RE.search(stem)
    if fish_match is not None:
        fish_token = fish_match.group(0)
    elif "_anatomy_" in stem:
        fish_token = stem.split("_anatomy_", 1)[0]
    else:
        fish_token = _anatomy_uint8_source_stem(path)
    return path.with_name(f"{fish_token}_anatomy_2P_GCaMP.nrrd")


def _collect_anatomy_stack_candidates(
    directory: Path,
    patterns: tuple[str, ...],
    excluded_tokens: tuple[str, ...],
) -> list[Path]:
    if not directory.exists():
        return []
    hits: list[Path] = []
    seen: set[Path] = set()
    for pattern in patterns:
        for hit in sorted(directory.glob(pattern)):
            if not hit.is_file():
                continue
            name_lower = hit.name.lower()
            if any(token in name_lower for token in excluded_tokens):
                continue
            if hit not in seen:
                hits.append(hit)
                seen.add(hit)
    return hits


def infer_anatomy_stack_path(fish_dir: Path | str, fish_id: str | None = None) -> Path | None:
    fish_path = Path(fish_dir)
    fish_token = str(fish_id or fish_path.name).strip()
    raw_patterns = (
        f"{fish_token}*.tif",
        f"{fish_token}*.tiff",
        f"{fish_token}*.nrrd",
        "*anatomy*.tif",
        "*anatomy*.tiff",
        "*anatomy*.nrrd",
        "*.tif",
        "*.tiff",
        "*.nrrd",
    )
    preproc_patterns = (
        f"{fish_token}*_anatomy_2P_GCaMP.tif",
        f"{fish_token}*_anatomy_2P_GCaMP.tiff",
        f"{fish_token}*_anatomy_2P_GCaMP.nrrd",
        "*_anatomy_2P_GCaMP.tif",
        "*_anatomy_2P_GCaMP.tiff",
        "*_anatomy_2P_GCaMP.nrrd",
        "*anatomy*.tif",
        "*anatomy*.tiff",
        "*anatomy*.nrrd",
    )
    derived_patterns = (
        f"{fish_token}*_anatomy_2P_GCaMP_uint8.tif",
        f"{fish_token}*_anatomy_2P_GCaMP_uint8.tiff",
        "*_anatomy_2P_GCaMP_uint8.tif",
        "*_anatomy_2P_GCaMP_uint8.tiff",
        "*anatomy*_uint8.tif",
        "*anatomy*_uint8.tiff",
    )
    excluded_tokens = ("cp_masks", "mask", "label", "overlay")
    raw_hits = _collect_anatomy_stack_candidates(
        fish_path / "01_raw" / "2p" / "anatomy",
        raw_patterns,
        excluded_tokens,
    )
    if raw_hits:
        return raw_hits[0]

    preproc_hits = [
        hit
        for hit in _collect_anatomy_stack_candidates(
            fish_path / "02_reg" / "00_preprocessing" / "2p_anatomy",
            preproc_patterns,
            excluded_tokens,
        )
        if not _is_uint8_anatomy_preprocess_path(hit)
    ]
    if preproc_hits:
        return preproc_hits[0]

    derived_hits = _collect_anatomy_stack_candidates(
        fish_path / "02_reg" / "00_preprocessing" / "2p_anatomy",
        derived_patterns,
        excluded_tokens,
    )
    if derived_hits:
        return derived_hits[0]
    return None


def prepare_notebook_paths(ctx: FishContext, polarity_override: Any = None) -> dict[str, Any]:
    polarity, polarity_source = resolve_func_polarity(ctx.fish_id, ctx.matching_metadata_csv, polarity_override=polarity_override)
    func_raw_stack_path = first_match(ctx.fish_dir, ["01_raw/2p/functional/*.tif", "01_raw/2p/functional/*.tiff"])
    func_nonflipped_list = first_match(ctx.fish_dir, ["02_reg/00_preprocessing/2p_functional/02_motionCorrected/*mcorrected*.tif"], all_hits=True) or []
    anat_stack_path = infer_anatomy_stack_path(ctx.fish_dir, ctx.fish_id)
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
    resolved_local_root = config.local_root_override if config.local_root_override is not None else local_root
    ctx = resolve_fish_context(
        fish_id=config.fish_id,
        owner=config.owner,
        data_mode=config.data_mode,
        nas_root=nas_root,
        local_root=resolved_local_root,
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
    n_pending = int((fish_audit_df["status"] == "pending").sum()) if not fish_audit_df.empty else 0
    if strict and n_fail > 0:
        raise RuntimeError(f"[fish-audit] failed with {n_fail} issue(s). Resolve rows with status='fail'.")
    return {
        "fish_audit_df": fish_audit_df,
        "n_fail": n_fail,
        "n_warn": n_warn,
        "n_pending": n_pending,
        "log_lines": [f"[fish-audit] fish={fish_id} fail={n_fail} warn={n_warn} pending={n_pending}"],
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


def _read_anatomy_z_metadata(metadata_dir: Path | str | None) -> tuple[float | None, list[Path]]:
    if metadata_dir is None:
        return None, []
    base = Path(metadata_dir)
    if not base.exists():
        return None, []
    values: list[tuple[Path, float]] = []
    for path in sorted(base.glob("*metadata*.csv")):
        if "experiment_log" in path.name.lower():
            continue
        try:
            df = pd.read_csv(path)
        except Exception:
            continue
        if df.empty:
            continue
        renamed = {column: column.strip().lower() for column in df.columns}
        df = df.rename(columns=renamed)
        if "parameter" not in df.columns or "value" not in df.columns:
            continue
        params = dict(zip(df["parameter"].astype(str).str.strip().str.lower(), df["value"]))
        raw = params.get("step_size_um_anatomy")
        if raw is None or pd.isna(raw):
            continue
        try:
            value = float(str(raw).strip())
        except Exception:
            cleaned = re.sub(r"[^0-9eE+\-.]", "", str(raw))
            if not cleaned:
                continue
            value = float(cleaned)
        if np.isfinite(value) and value > 0:
            values.append((path, float(value)))
    if not values:
        return None, []
    unique = sorted({round(value, 9) for _, value in values})
    if len(unique) > 1:
        details = ", ".join(f"{path.name}={value:g}" for path, value in values)
        raise RuntimeError(f"[Vox] Conflicting step_size_um_anatomy values in metadata: {details}")
    return float(values[0][1]), [path for path, _ in values]


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


def _resolve_anatomy_source_path(
    *,
    anat_stack_path: Path,
    anat_stack_path_orig: Path | None,
    tmp_convert_dir: Path | None,
    preproc_dir: Path | None,
    force_recompute: bool,
) -> Path | None:
    if anat_stack_path.suffix.lower() == ".nrrd":
        return anat_stack_path
    if not force_recompute:
        return None
    if anat_stack_path_orig is not None and anat_stack_path_orig.suffix.lower() == ".nrrd":
        return anat_stack_path_orig
    try:
        if tmp_convert_dir is None or not str(anat_stack_path).startswith(str(tmp_convert_dir)):
            return None
        stem = anat_stack_path.stem
        if stem.endswith("_converted"):
            stem = stem[:-10]
        anat_dir = preproc_dir / "2p_anatomy" if preproc_dir is not None else None
        if anat_dir is None or not anat_dir.exists():
            return None
        hits = list(anat_dir.glob(stem + ".nrrd"))
        return hits[0] if hits else None
    except Exception:
        return None


def _read_nrrd_volume(path: Path) -> tuple[np.ndarray, str]:
    try:
        import nrrd
    except Exception:  # pragma: no cover
        nrrd = None
    if nrrd is not None:
        data, _ = nrrd.read(str(path))
        return np.asarray(data), "nrrd"

    try:
        import SimpleITK as sitk
    except Exception:  # pragma: no cover
        sitk = None
    if sitk is not None:
        return np.asarray(sitk.GetArrayFromImage(sitk.ReadImage(str(path)))), "SimpleITK"

    raise ImportError("Reading .nrrd requires pynrrd or SimpleITK")


def _maybe_reorder_anatomy_stack(data: Any) -> tuple[np.ndarray, bool]:
    arr = np.asarray(data)
    reordered = bool(arr.ndim == 3 and arr.shape[-1] < min(arr.shape[0], arr.shape[1]))
    if reordered:
        arr = arr.transpose(2, 1, 0)
    return arr, reordered


def _load_anatomy_volume_for_uint8(path: Path) -> tuple[np.ndarray, str, bool]:
    if path.suffix.lower() == ".nrrd":
        data, reader = _read_nrrd_volume(path)
        data, reordered = _maybe_reorder_anatomy_stack(data)
        return np.asarray(data), reader, reordered
    return np.asarray(tifffile.imread(path)), "tifffile", False


def _read_nrrd_header(path: Path) -> dict[str, Any]:
    if path.suffix.lower() != ".nrrd":
        return {}
    try:
        import nrrd
    except Exception:  # pragma: no cover
        return {}
    try:
        _, header = nrrd.read(str(path))
    except Exception:
        return {}
    return dict(header)


def _infer_anatomy_metadata_dir(source_path: Path) -> Path | None:
    source = Path(source_path)
    for parent in source.parents:
        if parent.name == "2p":
            candidate = parent / "metadata"
            if candidate.exists():
                return candidate
        if parent.name in {"01_raw", "02_reg", "03_analysis"}:
            fish_dir = parent.parent
            candidate = fish_dir / "01_raw" / "2p" / "metadata"
            if candidate.exists():
                return candidate
    return None


def _nrrd_space_directions_from_spacing(x_um: float | None, y_um: float | None, z_um: float | None) -> np.ndarray | None:
    if not all(value is not None and np.isfinite(float(value)) and float(value) > 0 for value in (x_um, y_um, z_um)):
        return None
    return np.asarray(
        [
            [float(x_um), 0.0, 0.0],
            [0.0, float(y_um), 0.0],
            [0.0, 0.0, float(z_um)],
        ],
        dtype=float,
    )


def _scale_space_directions_xy(
    space_dirs: Any,
    *,
    source_xy_shape: tuple[int, int] | None,
    output_xy_shape: tuple[int, int] | None,
) -> Any:
    if source_xy_shape is None or output_xy_shape is None:
        return space_dirs
    try:
        dirs = np.asarray(space_dirs, dtype=float)
    except Exception:
        return space_dirs
    if dirs.shape != (3, 3) or source_xy_shape[0] <= 0 or source_xy_shape[1] <= 0:
        return space_dirs
    if output_xy_shape[0] <= 0 or output_xy_shape[1] <= 0:
        return space_dirs
    scaled = dirs.copy()
    scaled[0, :] *= float(source_xy_shape[1]) / float(output_xy_shape[1])
    scaled[1, :] *= float(source_xy_shape[0]) / float(output_xy_shape[0])
    return scaled


def _registration_nrrd_header(
    *,
    write_ndim: int,
    labels: list[str],
    source_path: Path | None,
    source_shape: tuple[int, ...] | None,
    output_shape: tuple[int, ...],
    source_xy_shape: tuple[int, int] | None,
    output_xy_shape: tuple[int, int] | None,
) -> dict[str, Any]:
    header: dict[str, Any] = {
        "encoding": "raw",
        "kinds": ["domain"] * int(write_ndim),
        "labels": labels,
    }
    if write_ndim != 3:
        return header

    header["space dimension"] = 3
    header["space units"] = ["microns", "microns", "microns"]
    source_header = _read_nrrd_header(source_path) if source_path is not None else {}
    for key in ("space", "space origin", "space measurement frame"):
        if key in source_header:
            header[key] = source_header[key]
    if "space units" in source_header:
        header["space units"] = source_header["space units"]

    source_dirs = source_header.get("space directions")
    if source_dirs is not None:
        header["space directions"] = _scale_space_directions_xy(
            source_dirs,
            source_xy_shape=source_xy_shape,
            output_xy_shape=output_xy_shape,
        )
    elif source_path is not None and source_path.suffix.lower() in {".tif", ".tiff"} and source_xy_shape and output_xy_shape:
        x_um, y_um = _scaled_tiff_spacing_um(source_path, source_xy_shape, output_xy_shape)
        z_um, _paths = _read_anatomy_z_metadata(_infer_anatomy_metadata_dir(source_path))
        directions = _nrrd_space_directions_from_spacing(x_um, y_um, z_um)
        if directions is not None:
            header["space directions"] = directions

    if source_path is not None:
        header["source_path"] = str(source_path)
        header["source_name"] = source_path.name
    if source_shape is not None:
        header["source_shape"] = "x".join(str(int(v)) for v in source_shape)
    header["source_axes"] = "ZYX"
    header["array_axes"] = "XYZ"
    header["output_shape_zyx"] = "x".join(str(int(v)) for v in output_shape)
    return header


def _write_registration_nrrd_from_zyx_uint8(
    arr: np.ndarray,
    out_path: Path,
    *,
    source_path: Path | None = None,
    source_shape: tuple[int, ...] | None = None,
    source_xy_shape: tuple[int, int] | None = None,
    output_xy_shape: tuple[int, int] | None = None,
) -> str:
    data = np.asarray(arr)
    if data.dtype != np.uint8:
        data = data.astype(np.uint8, copy=False)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        import nrrd
    except Exception:  # pragma: no cover
        nrrd = None

    if nrrd is not None:
        if data.ndim == 3:
            write_data = np.transpose(data, (2, 1, 0))
            labels = ["x", "y", "z"]
        elif data.ndim == 2:
            write_data = np.transpose(data, (1, 0))
            labels = ["x", "y"]
        else:
            write_data = data
            labels = [f"axis{i}" for i in range(data.ndim)]
        header = _registration_nrrd_header(
            write_ndim=int(write_data.ndim),
            labels=labels,
            source_path=source_path,
            source_shape=source_shape,
            output_shape=tuple(int(v) for v in data.shape),
            source_xy_shape=source_xy_shape,
            output_xy_shape=output_xy_shape,
        )
        nrrd.write(str(out_path), write_data, header=header)
        return "nrrd"

    try:
        import SimpleITK as sitk
    except Exception as exc:  # pragma: no cover
        raise ImportError("Writing registration .nrrd requires pynrrd or SimpleITK") from exc
    sitk.WriteImage(sitk.GetImageFromArray(data), str(out_path))
    return "SimpleITK"


def _signed_stack_to_uint8(arr: np.ndarray) -> tuple[np.ndarray, dict[str, int | float]]:
    data = np.asarray(arr)
    if data.size == 0:
        raise ValueError("Anatomy stack is empty.")
    if not np.issubdtype(data.dtype, np.integer):
        raise TypeError(f"Expected an integer anatomy stack, got {data.dtype}.")

    raw_min = int(np.min(data))
    raw_max = int(np.max(data))
    offset = abs(raw_min) if raw_min < 0 else 0
    corrected = data.astype(np.int32, copy=False)
    if offset:
        corrected = corrected + int(offset)
    corrected = np.clip(corrected, 0, 65535)
    corrected_min = int(np.min(corrected))
    corrected_max = int(np.max(corrected))

    if corrected_max <= corrected_min:
        out = np.zeros(corrected.shape, dtype=np.uint8)
    else:
        scaled = (corrected.astype(np.float32) - float(corrected_min)) * (255.0 / float(corrected_max - corrected_min))
        out = np.clip(np.rint(scaled), 0, 255).astype(np.uint8)

    stats: dict[str, int | float] = {
        "raw_min": raw_min,
        "raw_max": raw_max,
        "negative_offset": int(offset),
        "corrected_min": corrected_min,
        "corrected_max": corrected_max,
        "output_min": int(np.min(out)) if out.size else 0,
        "output_max": int(np.max(out)) if out.size else 0,
    }
    return out, stats


def _normalize_target_xy_shape(value: Any) -> tuple[int, int] | None:
    if value is None or value is False:
        return None
    if isinstance(value, (int, np.integer)):
        size = int(value)
        if size <= 0:
            raise ValueError("target_xy_shape must contain positive integers")
        return size, size
    if isinstance(value, (tuple, list)) and len(value) == 2:
        y_size = int(value[0])
        x_size = int(value[1])
        if y_size <= 0 or x_size <= 0:
            raise ValueError("target_xy_shape must contain positive integers")
        return y_size, x_size
    raise TypeError("target_xy_shape must be None, an integer, or a two-item tuple/list")


def _resize_uint8_xy(arr: np.ndarray, target_xy_shape: tuple[int, int] | None) -> tuple[np.ndarray, bool]:
    target = _normalize_target_xy_shape(target_xy_shape)
    data = np.asarray(arr)
    if target is None:
        return data, False
    if data.ndim < 2:
        raise ValueError("Cannot resize anatomy stack with fewer than two dimensions")
    current_xy = tuple(int(v) for v in data.shape[-2:])
    if current_xy == target:
        return data, False

    leading_shape = data.shape[:-2]
    flat = data.reshape((-1, current_xy[0], current_xy[1]))
    resized = np.empty((flat.shape[0], target[0], target[1]), dtype=np.uint8)
    for idx, plane in enumerate(flat):
        plane_resized = transform.resize(
            plane,
            target,
            order=1,
            preserve_range=True,
            anti_aliasing=True,
        )
        resized[idx] = np.clip(np.rint(plane_resized), 0, 255).astype(np.uint8)
    return resized.reshape((*leading_shape, target[0], target[1])), True


def _read_tiff_xy_resolution(path: Path) -> tuple[float | None, float | None, str | None]:
    if path.suffix.lower() not in {".tif", ".tiff"}:
        return None, None, None
    def _imagej_info_value(info: Any, key: str) -> str | None:
        if not info:
            return None
        match = re.search(rf"(?im)^\s*{re.escape(key)}\s*=\s*(.+?)\s*$", str(info))
        return match.group(1).strip() if match else None
    try:
        with tifffile.TiffFile(path) as tf:
            page0 = tf.pages[0]
            x_tag = page0.tags.get("XResolution")
            y_tag = page0.tags.get("YResolution")
            unit_tag = page0.tags.get("ResolutionUnit")
            unit_name = None
            if unit_tag is not None:
                try:
                    unit_name = unit_tag.value.name
                except Exception:
                    unit_name = str(unit_tag.value)
            x_res = x_tag.value if x_tag is not None else None
            y_res = y_tag.value if y_tag is not None else None
            if isinstance(x_res, tuple) and len(x_res) == 2:
                x_res = x_res[0] / x_res[1] if x_res[1] else None
            if isinstance(y_res, tuple) and len(y_res) == 2:
                y_res = y_res[0] / y_res[1] if y_res[1] else None
            if unit_name is None or str(unit_name).upper() in {"NONE", "RESUNIT.NONE", "1"}:
                imagej_info = tf.imagej_metadata.get("Info") if isinstance(tf.imagej_metadata, dict) else None
                info_unit = _imagej_info_value(imagej_info, "ResolutionUnit")
                info_x_res = _imagej_info_value(imagej_info, "XResolution")
                info_y_res = _imagej_info_value(imagej_info, "YResolution")
                if info_unit and info_x_res and info_y_res:
                    try:
                        x_res = float(info_x_res)
                        y_res = float(info_y_res)
                        unit_name = str(info_unit)
                    except Exception:
                        pass
            return (
                float(x_res) if x_res is not None else None,
                float(y_res) if y_res is not None else None,
                unit_name,
            )
    except Exception:
        return None, None, None


def _scaled_tiff_spacing_um(
    source_path: Path,
    source_xy_shape: tuple[int, int],
    output_xy_shape: tuple[int, int],
) -> tuple[float | None, float | None]:
    x_res, y_res, unit_name = _read_tiff_xy_resolution(source_path)
    x_um, y_um = _res_to_um_per_px((x_res, y_res), unit_name)
    if x_um is None or y_um is None:
        return None, None
    if source_xy_shape[0] <= 0 or source_xy_shape[1] <= 0:
        return None, None
    if output_xy_shape[0] <= 0 or output_xy_shape[1] <= 0:
        return None, None
    scaled_x_um = float(x_um) * (float(source_xy_shape[1]) / float(output_xy_shape[1]))
    scaled_y_um = float(y_um) * (float(source_xy_shape[0]) / float(output_xy_shape[0]))
    return scaled_x_um, scaled_y_um


def _scaled_tiff_resolution(
    source_path: Path,
    source_xy_shape: tuple[int, int],
    output_xy_shape: tuple[int, int],
) -> tuple[tuple[float, float] | None, str | None]:
    x_res, y_res, unit_name = _read_tiff_xy_resolution(source_path)
    if x_res is None or y_res is None or unit_name is None:
        return None, None
    if output_xy_shape[0] <= 0 or output_xy_shape[1] <= 0:
        return None, None
    scaled_x_res = float(x_res) * (float(output_xy_shape[1]) / float(source_xy_shape[1]))
    scaled_y_res = float(y_res) * (float(output_xy_shape[0]) / float(source_xy_shape[0]))
    return (scaled_x_res, scaled_y_res), unit_name


def _anatomy_uint8_cache_metadata_path(out_path: Path) -> Path:
    return out_path.with_name(out_path.name + ".json")


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
    metadata_dir: Path | str | None = None,
    func_source_list: list[Path | str] | None = None,
    data_root: Path | str | None = None,
    local_root: Path | str | None = None,
    nas_root: Path | str | None = None,
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
    func_source_list_local = [Path(path) for path in (func_source_list or []) if path]
    func_stack_path_local = Path(func_stack_path) if func_stack_path else None
    func_raw_stack_path_local = Path(func_raw_stack_path) if func_raw_stack_path else None
    anat_stack_path_local = Path(anat_stack_path) if anat_stack_path else None
    metadata_dir_local = Path(metadata_dir) if metadata_dir else None
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

    func_paths = func_source_list_local or flipped_list_local or ([func_stack_path_local] if func_stack_path_local else [])
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
    for path_idx, func_path in enumerate(func_paths):
        legacy_flipped_path = flipped_list_local[path_idx] if path_idx < len(flipped_list_local) else None
        aliases = [f"func_{func_path.name}", func_path.name, "func"]
        if legacy_flipped_path is not None:
            aliases.extend([f"func_{legacy_flipped_path.name}", legacy_flipped_path.name])
        vox = _cache_lookup(cache_data, path=func_path, aliases=aliases)
        if not _vox_complete(vox) and legacy_flipped_path is not None:
            vox = _cache_lookup(cache_data, path=legacy_flipped_path, aliases=aliases)
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
        if legacy_flipped_path is not None:
            vox_func_by_path[str(legacy_flipped_path)] = dict(vox)

    vox_anat = _cache_lookup(cache_data, path=anat_stack_path_local, aliases=["anat"])
    vox_anat = _merge_missing(vox_anat, run_meta_voxels.get("anat"))
    if (not _vox_complete(vox_anat)) and anat_stack_path_local and anat_stack_path_local.exists():
        try:
            inferred = load_or_cache_voxels(anat_stack_path_local, "anat") or {}
            vox_anat = _merge_missing(_norm_vox(inferred), vox_anat)
        except Exception:
            pass
    if anat_stack_path_local is not None:
        anatomy_z, anatomy_z_paths = _read_anatomy_z_metadata(metadata_dir_local)
        if anatomy_z is None:
            has_manual_z = vox_anat_manual_local.get("Z") is not None
            if not has_manual_z:
                raise RuntimeError(
                    "[Vox] Could not resolve anatomy Z from metadata field step_size_um_anatomy; "
                    "set VOX_ANAT_MANUAL['Z'] or place a metadata CSV under 01_raw/2p/metadata."
                )
        else:
            vox_anat["Z"] = float(anatomy_z)
            joined_paths = ", ".join(str(path) for path in anatomy_z_paths)
            log_lines.append(f"[Vox] Anatomy Z_um={float(anatomy_z):g} from step_size_um_anatomy in {joined_paths}")

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
    for func_path in [str(path) for path in func_paths]:
        vox = vox_func_by_path.get(func_path, {})
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


def normalize_anatomy_stack_stage(
    *,
    anat_stack_path: Path | str | None,
    tmp_convert_dir: Path | str | None,
    preproc_dir: Path | str | None,
    anat_stack_path_orig: Path | str | None = None,
    config: AnatomyNormalizationStageConfig | None = None,
) -> dict[str, Any]:
    cfg = config or AnatomyNormalizationStageConfig()
    if anat_stack_path is None:
        raise ValueError("ANAT_STACK_PATH is required for anatomy normalization")
    if tmp_convert_dir is None:
        raise ValueError("TMP_CONVERT_DIR is required for anatomy normalization")

    anat_stack_path_local = Path(anat_stack_path)
    anat_stack_path_orig_local = Path(anat_stack_path_orig) if anat_stack_path_orig is not None else anat_stack_path_local
    tmp_convert_dir_local = Path(tmp_convert_dir)
    preproc_dir_local = Path(preproc_dir) if preproc_dir is not None else None
    tmp_convert_dir_local.mkdir(parents=True, exist_ok=True)

    force_recompute = bool(cfg.force_recompute_anat_convert)
    log_lines: list[str] = []
    if force_recompute:
        log_lines.append("[INFO] FORCE_RECOMPUTE_ANAT_CONVERT=True; rebuilding anatomy conversion")

    anat_src = _resolve_anatomy_source_path(
        anat_stack_path=anat_stack_path_local,
        anat_stack_path_orig=anat_stack_path_orig_local,
        tmp_convert_dir=tmp_convert_dir_local,
        preproc_dir=preproc_dir_local,
        force_recompute=force_recompute,
    )

    final_anat_stack_path = anat_stack_path_local
    converted_anat_tif: Path | None = None
    anatomy_shape: tuple[int, ...] | None = None
    reordered_to_zxy = False
    used_cached_conversion = False
    source_reader: str | None = None
    missing_nrrd_source = force_recompute and anat_src is None

    if anat_src is not None:
        nrrd_path = Path(anat_src)
        converted_anat_tif = tmp_convert_dir_local / f"{nrrd_path.stem}_converted.tif"
        if converted_anat_tif.exists() and not force_recompute:
            data = tifffile.imread(converted_anat_tif)
            anatomy_shape = tuple(int(v) for v in np.asarray(data).shape)
            used_cached_conversion = True
            log_lines.append(f"[INFO] Using existing converted anatomy: {converted_anat_tif}")
        else:
            data, source_reader = _read_nrrd_volume(nrrd_path)
            anatomy_shape = tuple(int(v) for v in np.asarray(data).shape)
            log_lines.append(f"NRRD anatomy shape (as read): {anatomy_shape}")
            data, reordered_to_zxy = _maybe_reorder_anatomy_stack(data)
            if reordered_to_zxy:
                anatomy_shape = tuple(int(v) for v in data.shape)
                log_lines.append(f"Reordered anatomy to (Z, X, Y): {anatomy_shape}")
            tifffile.imwrite(converted_anat_tif, np.asarray(data))
            log_lines.append(f"[INFO] Converted anatomy saved to {converted_anat_tif}")
        final_anat_stack_path = converted_anat_tif
    elif anat_stack_path_local.suffix.lower() in (".tif", ".tiff"):
        if missing_nrrd_source:
            log_lines.append(
                "[INFO] FORCE_RECOMPUTE_ANAT_CONVERT=True but no NRRD source found; "
                "set ANAT_STACK_PATH_ORIG to a .nrrd to reconvert."
            )
        log_lines.append("[INFO] Anatomy already TIFF; no conversion")
    else:
        if missing_nrrd_source:
            log_lines.append(
                "[INFO] FORCE_RECOMPUTE_ANAT_CONVERT=True but no NRRD source found; "
                "set ANAT_STACK_PATH_ORIG to a .nrrd to reconvert."
            )
        log_lines.append("[WARN] Anatomy path is not NRRD/TIFF; no conversion")

    artifacts = {
        "anat_source_path": anat_src,
        "converted_anat_tif": converted_anat_tif,
        "anatomy_shape": anatomy_shape,
        "reordered_to_zxy": reordered_to_zxy,
        "used_cached_conversion": used_cached_conversion,
        "source_reader": source_reader,
    }
    return {
        "bindings": {
            "FORCE_RECOMPUTE_ANAT_CONVERT": force_recompute,
            "ANAT_STACK_PATH_ORIG": anat_stack_path_orig_local,
            "ANAT_STACK_PATH": final_anat_stack_path,
        },
        "log_lines": log_lines,
        "artifacts": artifacts,
        "anat_stack_path": final_anat_stack_path,
    }


def preprocess_anatomy_uint8_stage(
    *,
    anat_stack_path: Path | str | None,
    preproc_dir: Path | str | None,
    anat_stack_path_orig: Path | str | None = None,
    output_path: Path | str | None = None,
    polarity: str | None = None,
    polarity_source: str | None = None,
    config: AnatomyUint8PreprocessingConfig | None = None,
) -> dict[str, Any]:
    cfg = config or AnatomyUint8PreprocessingConfig()
    if anat_stack_path is None:
        raise ValueError("ANAT_STACK_PATH is required for anatomy uint8 preprocessing")
    if preproc_dir is None and output_path is None:
        raise ValueError("PREPROC_DIR or output_path is required for anatomy uint8 preprocessing")

    force_recompute = bool(cfg.force_recompute_anat_uint8)
    current_anat_path = Path(anat_stack_path)
    original_anat_path = Path(anat_stack_path_orig) if anat_stack_path_orig is not None else current_anat_path
    requested_source_path = original_anat_path if bool(cfg.use_source_path_orig) and original_anat_path.exists() else current_anat_path
    if not requested_source_path.exists():
        raise FileNotFoundError(f"Anatomy source not found: {requested_source_path}")

    requested_is_uint8 = _is_uint8_anatomy_preprocess_path(requested_source_path)
    derived_input_path = requested_source_path if requested_is_uint8 else None

    if output_path is None:
        out_dir = Path(preproc_dir) / "2p_anatomy"
        out_path = derived_input_path if derived_input_path is not None else out_dir / f"{_anatomy_uint8_source_stem(requested_source_path)}_uint8.tif"
    else:
        out_path = Path(output_path)
        out_dir = out_path.parent
    out_dir.mkdir(parents=True, exist_ok=True)
    registration_nrrd_path = _canonical_anatomy_registration_nrrd_path(out_path)

    source_path = requested_source_path
    passthrough_uint8_input = False
    if derived_input_path is not None:
        source_path = derived_input_path
        out_path = derived_input_path
        meta_source_path: Path | None = None
        meta_source = _read_json_dict(_anatomy_uint8_cache_metadata_path(derived_input_path)).get("source_path")
        if meta_source:
            meta_source_candidate = Path(meta_source)
            if meta_source_candidate.exists() and not _is_uint8_anatomy_preprocess_path(meta_source_candidate):
                meta_source_path = meta_source_candidate
        if force_recompute and meta_source_path is not None:
            source_path = meta_source_path
        else:
            passthrough_uint8_input = True

    apply_orientation = bool(cfg.apply_func_orientation)
    flip_z_for_registration = bool(cfg.flip_z_for_registration)
    target_xy_shape = _normalize_target_xy_shape(cfg.target_xy_shape)
    orient_mode = func_orientation_mode(polarity) if apply_orientation else "none"
    polarity_norm = normalize_polarity_value(polarity)
    meta_path = _anatomy_uint8_cache_metadata_path(out_path)
    log_lines: list[str] = []
    if passthrough_uint8_input and force_recompute:
        log_lines.append(
            "[INFO] Existing anatomy preprocessing input has no raw source metadata; "
            "reusing it to avoid applying orientation twice."
        )
    use_cached = bool(out_path.exists() and (not force_recompute or passthrough_uint8_input))
    if use_cached:
        with tifffile.TiffFile(out_path) as tf:
            shape = tuple(int(v) for v in tf.series[0].shape)
            dtype_name = str(tf.series[0].dtype)
        if dtype_name != "uint8":
            log_lines.append(f"[INFO] Existing anatomy preprocessing output is {dtype_name}; rebuilding uint8 TIFF.")
            use_cached = False
        elif target_xy_shape is not None and tuple(shape[-2:]) != tuple(target_xy_shape):
            if passthrough_uint8_input:
                log_lines.append(
                    "[INFO] Existing anatomy preprocessing input has "
                    f"Y/X={tuple(shape[-2:])}; reusing it to avoid applying orientation twice."
                )
                use_cached = True
            else:
                log_lines.append(
                    "[INFO] Existing anatomy preprocessing output has "
                    f"Y/X={tuple(shape[-2:])}; rebuilding for Y/X={tuple(target_xy_shape)}."
                )
                use_cached = False
        else:
            cache_meta = _read_json_dict(meta_path)
            cache_target = cache_meta.get("target_xy_shape")
            cache_target_tuple = tuple(int(v) for v in cache_target) if isinstance(cache_target, list | tuple) and len(cache_target) == 2 else None
            cache_ok = (
                passthrough_uint8_input
                or (
                    int(cache_meta.get("cache_version", 0) or 0) >= int(cfg.cache_version)
                    and bool(cache_meta.get("apply_func_orientation", False)) == apply_orientation
                    and bool(cache_meta.get("flip_z_for_registration", False)) == flip_z_for_registration
                    and str(cache_meta.get("orientation_mode", "")) == str(orient_mode)
                    and normalize_polarity_value(cache_meta.get("polarity")) == polarity_norm
                    and cache_target_tuple == target_xy_shape
                )
            )
            if not cache_ok:
                log_lines.append("[INFO] Existing anatomy preprocessing output lacks current orientation/resize metadata; rebuilding.")
                use_cached = False
    if use_cached:
        log_lines.append(f"[INFO] Using existing 8-bit anatomy preprocessing output: {out_path}")
        nrrd_writer = None
        if cfg.write_registration_nrrd and (force_recompute or not registration_nrrd_path.exists()):
            cached_uint8 = np.asarray(tifffile.imread(out_path), dtype=np.uint8)
            cache_meta = _read_json_dict(meta_path)
            cache_source_shape = cache_meta.get("source_shape")
            source_shape_for_nrrd = (
                tuple(int(v) for v in cache_source_shape)
                if isinstance(cache_source_shape, list | tuple) and cache_source_shape
                else None
            )
            cache_source_xy_shape = cache_meta.get("source_xy_shape")
            source_xy_for_nrrd = (
                tuple(int(v) for v in cache_source_xy_shape)
                if isinstance(cache_source_xy_shape, list | tuple) and len(cache_source_xy_shape) == 2
                else None
            )
            output_xy_for_nrrd = tuple(int(v) for v in cached_uint8.shape[-2:]) if cached_uint8.ndim >= 2 else None
            nrrd_writer = _write_registration_nrrd_from_zyx_uint8(
                cached_uint8,
                registration_nrrd_path,
                source_path=source_path if source_path.exists() else None,
                source_shape=source_shape_for_nrrd,
                source_xy_shape=source_xy_for_nrrd,
                output_xy_shape=output_xy_for_nrrd,
            )
            log_lines.append(f"[INFO] Saved registration-ready anatomy NRRD: {registration_nrrd_path}")
        stats: dict[str, Any] = {
            "output_shape": shape,
            "output_dtype": dtype_name,
            "used_cached_uint8": True,
            "orientation_mode": orient_mode,
            "flip_z_for_registration": flip_z_for_registration,
            "polarity": polarity_norm,
            "polarity_source": polarity_source,
            "target_xy_shape": target_xy_shape,
            "registration_nrrd_path": registration_nrrd_path,
            "registration_nrrd_writer": nrrd_writer,
        }
    else:
        vol, reader, reordered_to_zxy = _load_anatomy_volume_for_uint8(source_path)
        anatomy_shape = tuple(int(v) for v in vol.shape)
        log_lines.append(f"[INFO] Anatomy uint8 source: {source_path}")
        log_lines.append(f"[INFO] Anatomy source shape={anatomy_shape} dtype={vol.dtype}")
        if reordered_to_zxy:
            log_lines.append(f"[INFO] Reordered anatomy to (Z, X, Y): {anatomy_shape}")
        anat_u8, range_stats = _signed_stack_to_uint8(vol)
        source_xy_shape = tuple(int(v) for v in anat_u8.shape[-2:]) if anat_u8.ndim >= 2 else None
        if apply_orientation:
            anat_u8 = np.asarray(apply_func_orientation(anat_u8, polarity=polarity_norm, flip_x=True), dtype=np.uint8)
            log_lines.append(
                f"[INFO] Applied anatomy orientation mode={orient_mode} "
                f"polarity={polarity_norm} source={polarity_source}"
            )
        if flip_z_for_registration and anat_u8.ndim >= 3:
            anat_u8 = np.flip(anat_u8, axis=0).copy()
            log_lines.append("[INFO] Flipped anatomy Z axis to match bottom-to-top confocal registration convention.")
        anat_u8, resized_xy = _resize_uint8_xy(anat_u8, target_xy_shape)
        output_xy_shape = tuple(int(v) for v in anat_u8.shape[-2:]) if anat_u8.ndim >= 2 else None
        if resized_xy and source_xy_shape is not None and output_xy_shape is not None:
            log_lines.append(f"[INFO] Resized anatomy Y/X from {source_xy_shape} to {output_xy_shape}")
        resolution = None
        resolutionunit = None
        if source_xy_shape is not None and output_xy_shape is not None:
            resolution, resolutionunit = _scaled_tiff_resolution(source_path, source_xy_shape, output_xy_shape)
        imwrite_kwargs: dict[str, Any] = {
            "compression": "deflate",
            "metadata": {"axes": "ZYX"} if anat_u8.ndim == 3 else None,
        }
        if resolution is not None and resolutionunit is not None:
            imwrite_kwargs["resolution"] = resolution
            imwrite_kwargs["resolutionunit"] = resolutionunit
        tifffile.imwrite(
            out_path,
            anat_u8,
            **imwrite_kwargs,
        )
        nrrd_writer = None
        if cfg.write_registration_nrrd:
            nrrd_writer = _write_registration_nrrd_from_zyx_uint8(
                anat_u8,
                registration_nrrd_path,
                source_path=source_path,
                source_shape=anatomy_shape,
                source_xy_shape=source_xy_shape,
                output_xy_shape=output_xy_shape,
            )
        write_meta = {
            "cache_version": int(cfg.cache_version),
            "source_path": str(source_path),
            "output_path": str(out_path),
            "registration_nrrd_path": str(registration_nrrd_path) if cfg.write_registration_nrrd else None,
            "apply_func_orientation": apply_orientation,
            "flip_z_for_registration": flip_z_for_registration,
            "orientation_mode": orient_mode,
            "polarity": polarity_norm,
            "polarity_source": polarity_source,
            "target_xy_shape": list(target_xy_shape) if target_xy_shape is not None else None,
            "source_shape": list(anatomy_shape),
            "source_xy_shape": list(source_xy_shape) if source_xy_shape is not None else None,
            "output_shape": [int(v) for v in anat_u8.shape],
            "resolution": list(resolution) if resolution is not None else None,
            "resolutionunit": resolutionunit,
        }
        try:
            meta_path.write_text(json.dumps(write_meta, indent=2, sort_keys=True))
        except Exception as exc:
            log_lines.append(f"[WARN] Could not write anatomy preprocessing metadata {meta_path}: {exc}")
        log_lines.append(
            "[INFO] Signed anatomy range "
            f"[{range_stats['raw_min']}, {range_stats['raw_max']}] "
            f"offset={range_stats['negative_offset']} -> uint8 "
            f"[{range_stats['output_min']}, {range_stats['output_max']}]"
        )
        log_lines.append(f"[INFO] Saved 8-bit anatomy preprocessing output: {out_path}")
        if cfg.write_registration_nrrd:
            log_lines.append(f"[INFO] Saved registration-ready anatomy NRRD: {registration_nrrd_path}")
        stats = {
            **range_stats,
            "output_shape": tuple(int(v) for v in anat_u8.shape),
            "output_dtype": str(anat_u8.dtype),
            "source_reader": reader,
            "reordered_to_zxy": reordered_to_zxy,
            "used_cached_uint8": False,
            "apply_func_orientation": apply_orientation,
            "orientation_mode": orient_mode,
            "flip_z_for_registration": flip_z_for_registration,
            "polarity": polarity_norm,
            "polarity_source": polarity_source,
            "target_xy_shape": target_xy_shape,
            "resized_xy": resized_xy,
            "metadata_path": meta_path,
            "registration_nrrd_path": registration_nrrd_path,
            "registration_nrrd_writer": nrrd_writer,
        }

    return {
        "bindings": {
            "FORCE_RECOMPUTE_ANAT_UINT8": force_recompute,
            "ANAT_STACK_PATH_ORIG": original_anat_path,
            "ANAT_STACK_PATH_16BIT": current_anat_path,
            "ANAT_8BIT_STACK_PATH": out_path,
            "ANAT_REG_NRRD_PATH": registration_nrrd_path if cfg.write_registration_nrrd else None,
            "ANAT_STACK_PATH": out_path,
        },
        "log_lines": log_lines,
        "artifacts": {
            "anat_uint8_source_path": source_path,
            "anat_uint8_path": out_path,
            **stats,
        },
        "anat_stack_path": out_path,
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
    if len(target_paths) < len(source_paths):
        target_paths.extend(Path(out_raw) / f"{path.stem}_flipX.tif" for path in source_paths[len(target_paths) :])

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
        audit_df = pd.DataFrame()
        return {
            "bindings": {
                "ORIENT_MANIFEST_PATH": orient_manifest_path,
                "ORIENT_CACHE_VERSION": int(cfg.cache_version),
                "FUNCTIONAL_ORIENTATION_AUDIT_DF": audit_df,
            },
            "audit_df": audit_df,
            "log_lines": log_lines,
            "mode": mode,
        }

    orient_manifest = load_manifest()
    fish_id_str = str(fish_id)
    audit_rows: list[dict[str, Any]] = []
    for src_path, dst_path in zip(source_paths, target_paths):
        src_exists = src_path.exists()
        dst_exists = dst_path.exists()
        if dst_exists and not cfg.save_oriented_stacks:
            cache_status = "legacy_cache_unused"
        elif cfg.save_oriented_stacks:
            cache_status = "will_write_if_needed"
        else:
            cache_status = "no_cache"
        audit_rows.append(
            {
                "fish_id": fish_id_str,
                "source_path": str(src_path),
                "oriented_cache_path": str(dst_path),
                "source_exists": bool(src_exists),
                "oriented_cache_exists": bool(dst_exists),
                "source_size_bytes": int(src_path.stat().st_size) if src_exists else None,
                "oriented_cache_size_bytes": int(dst_path.stat().st_size) if dst_exists else None,
                "status": cache_status,
            }
        )
    audit_df = pd.DataFrame(audit_rows)
    log_lines.append(
        f"[10] mode={mode} polarity={polarity} source={polarity_source} "
        f"overwrite={cfg.overwrite_flipped} save_oriented_stacks={cfg.save_oriented_stacks} stacks={len(source_paths)}"
    )
    if not cfg.save_oriented_stacks:
        n_existing = int(audit_df["oriented_cache_exists"].sum()) if "oriented_cache_exists" in audit_df.columns else 0
        bytes_existing = int(audit_df["oriented_cache_size_bytes"].dropna().sum()) if "oriented_cache_size_bytes" in audit_df.columns else 0
        log_lines.append(
            f"[10] Not saving full oriented functional movie stacks; existing legacy caches={n_existing} "
            f"({bytes_existing / (1024 ** 3):.2f} GiB)."
        )
        return {
            "bindings": {
                "ORIENT_MANIFEST_PATH": orient_manifest_path,
                "ORIENT_CACHE_VERSION": int(cfg.cache_version),
                "FUNCTIONAL_ORIENTATION_AUDIT_DF": audit_df,
            },
            "audit_df": audit_df,
            "log_lines": log_lines,
            "mode": mode,
        }

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
            "FUNCTIONAL_ORIENTATION_AUDIT_DF": audit_df,
        },
        "audit_df": audit_df,
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
            rows.append(
                {
                    "scope": "path",
                    "key": key,
                    "status": "pending",
                    "value": None,
                    "detail": f"not initialized yet; expected {expected}",
                }
            )
        elif Path(str(current)) != Path(str(expected)):
            rows.append({"scope": "path", "key": key, "status": "warn", "value": str(current), "detail": f"expected {expected}"})
        else:
            rows.append({"scope": "path", "key": key, "status": "ok", "value": str(current), "detail": ""})

    audit_df = pd.DataFrame(rows)
    if audit_df.empty:
        return audit_df
    status_order = {"fail": 0, "warn": 1, "pending": 2, "ok": 3}
    audit_df["_ord"] = audit_df["status"].map(status_order).fillna(9)
    return audit_df.sort_values(["_ord", "scope", "key"]).drop(columns=["_ord"]).reset_index(drop=True)


__all__ = [
    "AnatomyNormalizationStageConfig",
    "AnatomyUint8PreprocessingConfig",
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
    "normalize_anatomy_stack_stage",
    "notebook_bindings_from_context",
    "owner_root",
    "prepare_notebook_paths",
    "preprocess_anatomy_uint8_stage",
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
