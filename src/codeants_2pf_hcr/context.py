"""Context and fish-scoped path helpers for notebook cells [4], [4a], [4b], [4c]."""

from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path
import re
from typing import Any

import pandas as pd
import tifffile


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
    directory = Path(fish_dir) / "03_analysis" / "confocal" / "raw" / "cp_masks"
    if not directory.exists():
        return []
    hits: list[Path] = []
    for pattern in (f"{fish_id}_round*_cp_masks*.tif", f"{fish_id}_round*.tif", "*round*_cp_masks*.tif"):
        hits.extend(sorted(directory.glob(pattern)))
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
    "DEFAULT_RUN_CONFIG",
    "FORCE_TRUE_RUN_CONFIG_KEYS",
    "FishContext",
    "build_fish_state_audit_df",
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
    "resolve_func_polarity",
    "resolve_fish_context",
    "resolve_fish_dir",
    "scanimage_um_per_px_from_artist",
]
