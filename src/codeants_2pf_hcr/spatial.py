"""Spatial/image helpers and notebook-facing stages for functional reference cells [12], [16], and [20]."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import shutil
from typing import Any

import numpy as np
import pandas as pd
from scipy import ndimage as ndi
from skimage import feature, registration, transform
from skimage.util import img_as_float32
import tifffile

try:
    import cv2
except Exception:  # pragma: no cover
    cv2 = None

try:
    import nrrd
except Exception:  # pragma: no cover
    nrrd = None

try:
    import SimpleITK as sitk
except Exception:  # pragma: no cover
    sitk = None


def imread_any(path: str | Path) -> np.ndarray:
    target = Path(path)
    if target.suffix.lower() == ".nrrd":
        if sitk is not None:
            return np.asarray(sitk.GetArrayFromImage(sitk.ReadImage(str(target))))
        if nrrd is not None:
            data, _ = nrrd.read(str(target), index_order="C")
            return np.asarray(data)
        raise ImportError("Reading .nrrd requires pynrrd or SimpleITK")
    return tifffile.imread(target)


def zproject_mean(stack: np.ndarray) -> np.ndarray:
    return np.asarray(stack).mean(axis=0)


def norm01(img: np.ndarray) -> np.ndarray:
    arr = np.asarray(img, dtype=np.float32)
    black, white = np.percentile(arr, (1, 99))
    if white <= black:
        white = float(arr.max())
        black = float(arr.min())
    return np.clip((arr - black) / (white - black + 1e-6), 0.0, 1.0)


def local_unsharp(img: np.ndarray, blur_sigma: float = 1.0, amount: float = 0.6) -> np.ndarray:
    base = ndi.gaussian_filter(img, blur_sigma)
    return np.clip(base + amount * (np.asarray(img) - base), 0.0, 1.0)


def corrcoef_img(a: np.ndarray, b: np.ndarray) -> float:
    arr_a = np.asarray(a, dtype=np.float32)
    arr_b = np.asarray(b, dtype=np.float32)
    arr_a = arr_a - arr_a.mean()
    arr_b = arr_b - arr_b.mean()
    denom = np.sqrt((arr_a * arr_a).sum() * (arr_b * arr_b).sum()) + 1e-8
    return float((arr_a * arr_b).sum() / denom)


def top_correlated_mean(stack_t: np.ndarray, take_k: int = 20, pre_smooth_sigma: float = 0.5) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    stack = np.asarray(stack_t, dtype=np.float32)
    reference = stack.mean(axis=0)
    ref_smoothed = ndi.gaussian_filter(reference, pre_smooth_sigma) if pre_smooth_sigma and pre_smooth_sigma > 0 else reference
    corrs = np.empty(stack.shape[0], dtype=np.float32)
    for idx, frame in enumerate(stack):
        compare = ndi.gaussian_filter(frame, pre_smooth_sigma) if pre_smooth_sigma and pre_smooth_sigma > 0 else frame
        corrs[idx] = corrcoef_img(compare, ref_smoothed)
    k = min(int(take_k), int(stack.shape[0]))
    top_idx = np.argsort(corrs)[-k:]
    return stack[top_idx].mean(axis=0), top_idx, corrs


def best_z_by_ncc(template: np.ndarray, anat_stack: np.ndarray, use_cv2: bool = True) -> tuple[int, np.ndarray]:
    template01 = norm01(template)
    scores: list[float] = []
    if use_cv2 and cv2 is not None:
        templ = (template01 * 255).astype(np.uint8)
        for z_idx in range(anat_stack.shape[0]):
            ref = (norm01(anat_stack[z_idx]) * 255).astype(np.uint8)
            result = cv2.matchTemplate(ref, templ, cv2.TM_CCORR_NORMED)
            scores.append(float(result.ravel()[0]) if result.size == 1 else float(result.max()))
    else:
        for z_idx in range(anat_stack.shape[0]):
            ref = norm01(anat_stack[z_idx])
            if ref.shape == template01.shape:
                scores.append(corrcoef_img(template01, ref))
            else:
                try:
                    result = feature.match_template(ref, template01, pad_input=False)
                    scores.append(float(result.max()))
                except Exception:
                    resized = transform.resize(template01, ref.shape, order=1, preserve_range=True, anti_aliasing=True).astype(np.float32)
                    scores.append(corrcoef_img(resized, ref))
    score_arr = np.asarray(scores, dtype=np.float32)
    return int(np.argmax(score_arr)), score_arr


def apply_func_orientation(arr: np.ndarray, *, polarity: str | None = None, flip_x: bool = True) -> np.ndarray:
    out = np.asarray(arr)
    if out.ndim < 2:
        return out
    value = str(polarity).strip().lower()
    if not flip_x:
        return out.copy()
    if value == "north":
        return np.flip(out, axis=-2).copy()
    if value == "south":
        return np.flip(out, axis=-1).copy()
    raise ValueError(f"Cannot orient functional data without north/south polarity, got {polarity!r}")


def _to_um(val: float | int | None, unit: str | None) -> float | None:
    if val is None:
        return None
    unit_str = "" if unit is None else str(unit).strip().lower()
    if unit_str in {"um", "micrometer", "micrometers", "micron", "microns"}:
        return float(val)
    if unit_str in {"mm", "millimeter", "millimeters"}:
        return float(val) * 1000.0
    if unit_str in {"cm", "centimeter", "centimeters"}:
        return float(val) * 10000.0
    if unit_str in {"m", "meter", "meters"}:
        return float(val) * 1_000_000.0
    return float(val)


def _res_to_um_per_px(res_tag: float | tuple[float, float] | None, unit_tag: str | None) -> tuple[float | None, float | None]:
    if res_tag is None:
        return None, None
    if isinstance(res_tag, (tuple, list)) and len(res_tag) >= 2:
        x_res, y_res = float(res_tag[0]), float(res_tag[1])
    else:
        x_res = y_res = float(res_tag)
    if x_res <= 0 or y_res <= 0:
        return None, None
    unit_str = "" if unit_tag is None else str(unit_tag).strip().lower()
    if unit_str in {"inch", "in"}:
        return 25_400.0 / x_res, 25_400.0 / y_res
    if unit_str in {"centimeter", "cm"}:
        return 10_000.0 / x_res, 10_000.0 / y_res
    return None, None


def _parse_nrrd_header_text(text: str) -> dict[str, object]:
    header: dict[str, object] = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#") or ":" not in line:
            continue
        key, value = line.split(":", 1)
        header[key.strip().lower()] = value.strip()
    return header


def _find_embedded_nrrd_header(path: str | Path, max_bytes: int = 1_000_000) -> dict[str, object]:
    with Path(path).open("rb") as handle:
        raw = handle.read(max_bytes)
    parts = raw.split(b"\n\n", 1)
    if not parts:
        return {}
    try:
        return _parse_nrrd_header_text(parts[0].decode("utf-8", errors="replace"))
    except Exception:
        return {}


def _infer_voxels_nrrd(path: str | Path) -> dict[str, float]:
    target = Path(path)
    if nrrd is not None:
        try:
            _, header = nrrd.read(str(target))
            space_dirs = header.get("space directions")
            if space_dirs is not None:
                lengths = []
                for axis in space_dirs:
                    if axis is None or str(axis).lower() == "none":
                        lengths.append(None)
                    else:
                        arr = np.asarray(axis, dtype=float)
                        lengths.append(float(np.linalg.norm(arr)))
                if len(lengths) >= 3:
                    return {"X": lengths[-1], "Y": lengths[-2], "Z": lengths[0]}
            spacings = header.get("spacings")
            if spacings is not None and len(spacings) >= 3:
                vals = [float(v) for v in spacings]
                return {"X": vals[-1], "Y": vals[-2], "Z": vals[0]}
        except Exception:
            pass
    if sitk is not None:
        try:
            image = sitk.ReadImage(str(target))
            spacing = tuple(float(v) for v in image.GetSpacing())
            if len(spacing) >= 3:
                return {"X": spacing[0], "Y": spacing[1], "Z": spacing[2]}
        except Exception:
            pass
    header = _find_embedded_nrrd_header(target)
    space_directions = header.get("space directions")
    if isinstance(space_directions, str):
        tuples = re.findall(r"\(([^)]*)\)", space_directions)
        vals = []
        for item in tuples:
            if "none" in item.lower():
                vals.append(None)
                continue
            parts = [float(piece.strip()) for piece in item.split(",")]
            vals.append(float(np.linalg.norm(np.asarray(parts, dtype=float))))
        if len(vals) >= 3:
            return {"X": vals[-1], "Y": vals[-2], "Z": vals[0]}
    return {}


def _infer_voxels_from_open_tiff(tf: tifffile.TiffFile) -> dict[str, float]:
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
    x_um, _ = _res_to_um_per_px(x_res, unit_name)
    _, y_um = _res_to_um_per_px(y_res, unit_name)
    out: dict[str, float] = {}
    if x_um is not None:
        out["X"] = x_um
    if y_um is not None:
        out["Y"] = y_um
    if len(tf.pages) > 1:
        out["Z"] = 1.0
    return out


def infer_voxels_tiff(path: str | Path) -> dict[str, float]:
    with tifffile.TiffFile(path) as tf:
        return _infer_voxels_from_open_tiff(tf)


def load_or_cache_voxels(path: str | Path, alias: str | None = None) -> dict[str, float]:
    del alias
    target = Path(path)
    suffixes = [suffix.lower() for suffix in target.suffixes]
    if suffixes and suffixes[-1] == ".nrrd":
        return _infer_voxels_nrrd(target)
    if suffixes and suffixes[-1] in {".tif", ".tiff"}:
        return infer_voxels_tiff(target)
    return {}


@dataclass(frozen=True)
class FunctionalReferenceConfig:
    use_top_corr_refs: bool = True
    top_corr_k: int = 20
    top_corr_sample: int = 20
    reuse_saved_refs: bool = True
    force_recompute_refs: bool = False
    top_corr_pre_smooth_sigma: float = 0.5
    exclude_first_block: bool = False
    preprocessing_metadata_path: str | Path | None = None


@dataclass(frozen=True)
class FunctionalPlacementConfig:
    use_ncc_placement: bool = True
    display_normalize_placed: bool = True
    use_cv2: bool = True


@dataclass(frozen=True)
class InPlaneRegistrationComparisonConfig:
    methods: tuple[str, ...] = ("ncc_xy", "ants_rigid_affine")
    active_method: str = "ants_rigid_affine"
    fallback_method: str | None = None
    save_outputs: bool = True
    output_subdir: str = "inplane_registration_comparison"
    display_normalize_placed: bool = True
    use_cv2: bool = True
    ants_aff_iterations: tuple[int, ...] = (2000, 1000, 500, 250, 100)
    ants_aff_shrink_factors: tuple[int, ...] = (12, 8, 4, 2, 1)
    ants_aff_smoothing_sigmas: tuple[int, ...] = (4, 3, 2, 1, 0)
    clip_percentiles: tuple[float, float] = (5.0, 95.0)
    ants_fixed_mask_json: str | Path | None = None
    ants_require_fixed_mask: bool = True
    ants_deterministic_seed: int | None = 0
    fail_on_active_method_error: bool = True


@dataclass(frozen=True)
class RegistrationSearchConfig:
    rescale_func_to_anat: bool = True
    force_recompute: bool = False
    manual_scale: float = 1.0
    scale_metric: str = "max_score"
    scale_coarse: tuple[float, float, float] = (0.70, 1.00, 0.05)
    scale_fine: tuple[float, float] = (0.05, 0.01)
    scale_xfine: tuple[float, float] = (0.005, 0.001)
    scale_ufine: tuple[float, float] = (0.0005, 0.0001)
    scale_refine_only: bool = False
    manual_z_limit: int = 0
    scale_per_plane: bool = True
    scale_backend: str = "threads"
    scale_workers: int | None = None
    scale_center_rel_tol: float = 0.0
    scale_center_abs_tol: float = 0.0
    scale_center_prefer_high: bool = False
    cache_version: int = 2
    use_cv2: bool = True
    sharpen_sigma: float = 1.0
    sharpen_amount: float = 0.6


def _as_bool(val: Any, default: bool = False) -> bool:
    if isinstance(val, bool):
        return val
    if val is None:
        return default
    if isinstance(val, (int, float)):
        return bool(val)
    if isinstance(val, str):
        text = val.strip().lower()
        if text in {"1", "true", "t", "yes", "y", "on"}:
            return True
        if text in {"0", "false", "f", "no", "n", "off", ""}:
            return False
    return bool(val)


def _functional_plane_ref(
    *,
    label: str,
    ref2d_raw: np.ndarray,
    ref2d: np.ndarray,
    index: int | None = None,
    vox_func: Any = None,
    frame_selection: dict[str, Any] | None = None,
) -> dict[str, Any]:
    plane_ref = {
        "label": label,
        "ref2d_raw": np.asarray(ref2d_raw, dtype=np.float32),
        "ref2d": np.asarray(ref2d, dtype=np.float32),
    }
    if index is not None:
        plane_ref["index"] = int(index)
    if vox_func is not None:
        plane_ref["vox_func"] = vox_func
    if frame_selection:
        plane_ref["functional_reference_frame_selection"] = dict(frame_selection)
    return plane_ref


def _functional_preprocessing_metadata_path(source_path: Path, configured_path: str | Path | None) -> Path:
    if configured_path not in (None, "", False):
        path = Path(configured_path)
        if not path.exists():
            raise FileNotFoundError(f"Functional preprocessing metadata not found: {path}")
        return path
    fish_id = source_path.stem.split("_plane", 1)[0]
    metadata_dir = source_path.parent.parent / "01_individualPlanes"
    exact = metadata_dir / f"{fish_id}_preprocessing_metadata.json"
    if exact.exists():
        return exact
    candidates = sorted(metadata_dir.glob("*_preprocessing_metadata.json"))
    if len(candidates) == 1:
        return candidates[0]
    raise FileNotFoundError(
        f"Could not resolve functional preprocessing metadata for {source_path}; "
        f"expected {exact}"
    )


def _functional_source_frame_count(source_path: Path) -> int:
    with tifffile.TiffFile(source_path) as tif:
        shape = tuple(int(value) for value in tif.series[0].shape)
    if len(shape) < 3 or shape[0] < 1:
        raise ValueError(f"Functional stack has unsupported shape {shape}: {source_path}")
    return int(shape[0])


def _raw_functional_block_number(path: str | Path) -> int | None:
    match = re.search(r"_(\d{5})\.tiff?$", Path(path).name, flags=re.IGNORECASE)
    return int(match.group(1)) if match else None


def _functional_reference_frame_selection(source_path: Path, cfg: FunctionalReferenceConfig) -> dict[str, Any]:
    source_frame_count = _functional_source_frame_count(source_path)
    selection: dict[str, Any] = {
        "exclude_first_block": bool(cfg.exclude_first_block),
        "frame_start": 0,
        "source_frame_count": source_frame_count,
        "reference_frame_count": source_frame_count,
        "decision": "all_frames_requested",
    }
    if not cfg.exclude_first_block:
        return selection

    metadata_path = _functional_preprocessing_metadata_path(source_path, cfg.preprocessing_metadata_path)
    payload = json.loads(metadata_path.read_text())
    selection["preprocessing_metadata_path"] = str(metadata_path)
    configured_blocks = payload.get("blocks")
    if isinstance(configured_blocks, list) and configured_blocks:
        configured_block_numbers = [int(value) for value in configured_blocks]
        selection["selected_block_numbers"] = configured_block_numbers
        if 1 not in configured_block_numbers:
            selection["decision"] = "first_block_already_excluded_upstream"
            return selection

    plane_match = re.search(r"_plane(\d+)(?:_|$)", source_path.stem)
    plane_index = int(plane_match.group(1)) if plane_match else None
    selected_tiffs: list[str] = []
    selected_session: str | None = None
    sessions = payload.get("sessions")
    if isinstance(sessions, list) and plane_index is not None:
        for session in sessions:
            output_planes = session.get("output_planes", []) if isinstance(session, dict) else []
            if plane_index in [int(value) for value in output_planes]:
                selected_tiffs = [str(path) for path in session.get("selected_tiffs", [])]
                selected_session = str(session.get("session_label", "")) or None
                break
    if not selected_tiffs and isinstance(configured_blocks, list) and configured_blocks:
        selected_tiffs = [f"block_{int(value):05d}.tif" for value in configured_blocks]
    if len(selected_tiffs) < 2:
        raise ValueError(
            f"Cannot exclude the first functional block for {source_path}: preprocessing metadata "
            "does not identify at least two selected TIFF blocks for this plane"
        )

    block_numbers = [_raw_functional_block_number(path) for path in selected_tiffs]
    if any(value is None for value in block_numbers):
        raise ValueError(
            f"Cannot parse selected functional block numbers for {source_path}: {selected_tiffs}"
        )
    selected_block_numbers = [int(value) for value in block_numbers if value is not None]
    selection["selected_block_numbers"] = selected_block_numbers
    selection["selected_tiff_count"] = len(selected_tiffs)
    if selected_session is not None:
        selection["session_label"] = selected_session
    if source_frame_count % len(selected_tiffs) != 0:
        raise ValueError(
            f"Cannot resolve an exact first-block boundary for {source_path}: {source_frame_count} frames "
            f"are not divisible by {len(selected_tiffs)} selected TIFF blocks"
        )

    frame_start = source_frame_count // len(selected_tiffs)
    selection.update(
        {
            "frame_start": frame_start,
            "reference_frame_count": source_frame_count - frame_start,
            "decision": "excluded_first_selected_tiff_block",
        }
    )
    return selection


def _load_cached_functional_ref(raw_path: Path, norm_path: Path) -> tuple[np.ndarray, np.ndarray] | None:
    if not raw_path.exists() or not norm_path.exists():
        return None
    ref2d_raw = np.asarray(imread_any(raw_path), dtype=np.float32)
    ref2d = norm01(imread_any(norm_path))
    return ref2d_raw, ref2d


def _legacy_oriented_path_for_source(source_path: Path, flipped_paths: list[Path], source_idx: int, out_raw_path: Path) -> Path:
    if source_idx < len(flipped_paths):
        return flipped_paths[source_idx]
    if source_path.stem.endswith("_flipX"):
        return source_path
    return out_raw_path / f"{source_path.stem}_flipX.tif"


def build_functional_references_stage(
    *,
    flipped_list: list[Path | str] | None,
    func_nonflipped_list: list[Path | str] | None = None,
    out_raw: Path | str,
    outdir: Path | str | None = None,
    vox_func_by_path: dict[str, Any] | None = None,
    polarity: str | None = None,
    polarity_source: str | None = None,
    input_xy_frame: str | None = None,
    config: FunctionalReferenceConfig | None = None,
) -> dict[str, Any]:
    del polarity_source
    cfg = config or FunctionalReferenceConfig()
    out_raw_path = Path(out_raw)
    outdir_path = Path(outdir) if outdir is not None else None
    out_raw_path.mkdir(parents=True, exist_ok=True)

    flipped_paths = [Path(path) for path in (flipped_list or []) if path]
    source_paths = [Path(path) for path in (func_nonflipped_list or []) if path]
    source_is_preoriented = False
    if not source_paths:
        source_paths = flipped_paths
        source_is_preoriented = True
    if not source_paths:
        raise FileNotFoundError("No functional stacks available")

    from .spatial_contract import CANONICAL_XY_FRAME, LEGACY_ACQUISITION_XY_FRAME, declared_xy_frame

    declared_frames = {declared_xy_frame(path, kind="functional") for path in source_paths}
    declared_frames.discard(None)
    if len(declared_frames) > 1:
        raise ValueError(f"Functional inputs declare contradictory XY frames: {sorted(declared_frames)}")
    declared_frame = next(iter(declared_frames), None)
    if input_xy_frame is not None and declared_frame is not None and input_xy_frame != declared_frame:
        raise ValueError(f"Requested functional XY frame {input_xy_frame!r} conflicts with manifest {declared_frame!r}")
    frame = input_xy_frame or declared_frame
    if frame is None:
        frame = CANONICAL_XY_FRAME if source_is_preoriented else LEGACY_ACQUISITION_XY_FRAME
    if frame not in {CANONICAL_XY_FRAME, LEGACY_ACQUISITION_XY_FRAME}:
        raise ValueError(f"Unknown functional XY frame: {frame!r}")
    source_is_preoriented = source_is_preoriented or frame == CANONICAL_XY_FRAME

    reuse_saved_refs = bool(cfg.reuse_saved_refs)
    if cfg.force_recompute_refs:
        reuse_saved_refs = False

    plane_refs: list[dict[str, Any]] = []
    log_lines: list[str] = []
    if cfg.force_recompute_refs:
        log_lines.append("[12] FORCE_RECOMPUTE_REFS=True -> rebuilding functional refs")

    for source_idx, fp in enumerate(source_paths):
        if not fp.exists():
            raise FileNotFoundError(f"Functional stack not found: {fp}")

        frame_selection = _functional_reference_frame_selection(fp, cfg)
        frame_start = int(frame_selection["frame_start"])
        if cfg.exclude_first_block:
            log_lines.append(
                f"[12] {fp.name}: {frame_selection['decision']} "
                f"(frames {frame_start}:{frame_selection['source_frame_count']})"
            )

        legacy_oriented_path = _legacy_oriented_path_for_source(fp, flipped_paths, source_idx, out_raw_path)
        vox_f = {}
        if vox_func_by_path:
            vox_f = vox_func_by_path.get(str(fp), {}) or vox_func_by_path.get(str(legacy_oriented_path), {}) or {}
        stem = legacy_oriented_path.stem
        raw_path = out_raw_path / f"{stem}_ref_raw.tif"
        norm_path = out_raw_path / f"{stem}_ref_norm.tif"
        legacy_raw_path = (outdir_path / f"{stem}_ref_raw.tif") if outdir_path is not None else None
        legacy_norm_path = (outdir_path / f"{stem}_ref_norm.tif") if outdir_path is not None else None

        if reuse_saved_refs and frame_start == 0:
            plane_raws = sorted(out_raw_path.glob(f"{stem}_plane*_raw.tif"))
            if (not plane_raws) and outdir_path is not None:
                plane_raws = sorted(outdir_path.glob(f"{stem}_plane*_raw.tif"))
            stack_plane_refs: list[dict[str, Any]] = []
            for rawp in plane_raws:
                match = re.search(r"plane(\d+)", rawp.stem)
                zi = int(match.group(1)) if match else None
                normp = rawp.parent / f"{stem}_plane{zi}_norm.tif"
                cached = _load_cached_functional_ref(rawp, normp)
                if cached is None:
                    continue
                ref2d_raw_i, ref2d_i = cached
                stack_plane_refs.append(
                    _functional_plane_ref(
                        label=f"{stem}_plane{zi}",
                        ref2d_raw=ref2d_raw_i,
                        ref2d=ref2d_i,
                        index=zi,
                        vox_func=vox_f,
                        frame_selection=frame_selection,
                    )
                )
            if stack_plane_refs:
                plane_refs.extend(stack_plane_refs)
                log_lines.append(f"[12] Using existing per-plane refs for {legacy_oriented_path}")
                continue

            cached = _load_cached_functional_ref(raw_path, norm_path)
            if cached is not None:
                ref2d_raw, ref2d = cached
                plane_refs.append(
                    _functional_plane_ref(
                        label=stem,
                        ref2d_raw=ref2d_raw,
                        ref2d=ref2d,
                        vox_func=vox_f,
                        frame_selection=frame_selection,
                    )
                )
                log_lines.append(f"[12] Using existing refs for {legacy_oriented_path}")
                continue

            if legacy_raw_path is not None and legacy_norm_path is not None:
                cached = _load_cached_functional_ref(legacy_raw_path, legacy_norm_path)
                if cached is not None:
                    ref2d_raw, ref2d = cached
                    plane_refs.append(
                        _functional_plane_ref(
                            label=stem,
                            ref2d_raw=ref2d_raw,
                            ref2d=ref2d,
                            vox_func=vox_f,
                            frame_selection=frame_selection,
                        )
                    )
                    log_lines.append(f"[12] Using existing refs for {legacy_oriented_path} (legacy)")
                    continue

        func = np.asarray(imread_any(fp), dtype=np.float32)
        if frame_start > 0:
            func = func[frame_start:]

        def orient_ref(arr: np.ndarray) -> np.ndarray:
            if source_is_preoriented:
                return np.asarray(arr, dtype=np.float32)
            return np.asarray(apply_func_orientation(arr, polarity=polarity, flip_x=True), dtype=np.float32)

        if cfg.use_top_corr_refs and func.ndim == 4:
            _, z_count, _, _ = func.shape
            for zi in range(z_count):
                plane_t = func[:, zi, :, :]
                k = min(int(cfg.top_corr_k), int(plane_t.shape[0]))
                ref2d_raw_i, idx, _ = top_correlated_mean(
                    plane_t,
                    take_k=k,
                    pre_smooth_sigma=float(cfg.top_corr_pre_smooth_sigma),
                )
                if int(cfg.top_corr_sample) > 0 and k > int(cfg.top_corr_sample):
                    try:
                        sel = np.random.choice(idx, size=int(cfg.top_corr_sample), replace=False)
                        ref2d_raw_i = plane_t[sel].mean(axis=0).astype(np.float32)
                    except Exception:
                        pass
                ref2d_raw_i = orient_ref(ref2d_raw_i)
                rawp = out_raw_path / f"{stem}_plane{zi}_raw.tif"
                normp = out_raw_path / f"{stem}_plane{zi}_norm.tif"
                tifffile.imwrite(rawp, np.asarray(ref2d_raw_i, dtype=np.float32))
                ref2d_i = norm01(ref2d_raw_i)
                tifffile.imwrite(normp, (ref2d_i * 65535).astype(np.uint16))
                plane_refs.append(
                    _functional_plane_ref(
                        label=f"{stem}_plane{zi}",
                        ref2d_raw=ref2d_raw_i,
                        ref2d=ref2d_i,
                        index=zi,
                        vox_func=vox_f,
                        frame_selection=frame_selection,
                    )
                )
            log_lines.append(f"[12] Built top-correlated per-plane refs for {fp} -> {legacy_oriented_path.stem}")
            continue

        if func.ndim == 3:
            ref2d_raw = func.mean(axis=0).astype(np.float32)
            ref2d_raw = orient_ref(ref2d_raw)
            tifffile.imwrite(raw_path, ref2d_raw.astype(np.float32))
            ref2d = norm01(ref2d_raw)
            tifffile.imwrite(norm_path, (ref2d * 65535).astype(np.uint16))
            plane_refs.append(
                _functional_plane_ref(
                    label=stem,
                    ref2d_raw=ref2d_raw,
                    ref2d=ref2d,
                    vox_func=vox_f,
                    frame_selection=frame_selection,
                )
            )
            log_lines.append(f"[12] Built mean reference for {fp} -> {legacy_oriented_path.stem}")
            continue

        if func.ndim != 4:
            raise ValueError(f"Unsupported functional stack ndim={func.ndim} for {fp}")

        _, z_count, _, _ = func.shape
        for zi in range(z_count):
            plane_t = func[:, zi, :, :]
            ref2d_raw_i = plane_t.mean(axis=0).astype(np.float32)
            ref2d_raw_i = orient_ref(ref2d_raw_i)
            rawp = out_raw_path / f"{stem}_plane{zi}_raw.tif"
            normp = out_raw_path / f"{stem}_plane{zi}_norm.tif"
            tifffile.imwrite(rawp, ref2d_raw_i.astype(np.float32))
            ref2d_i = norm01(ref2d_raw_i)
            tifffile.imwrite(normp, (ref2d_i * 65535).astype(np.uint16))
            plane_refs.append(
                _functional_plane_ref(
                    label=f"{stem}_plane{zi}",
                    ref2d_raw=ref2d_raw_i,
                    ref2d=ref2d_i,
                    index=zi,
                    vox_func=vox_f,
                    frame_selection=frame_selection,
                )
            )
        log_lines.append(f"[12] Built mean per-plane refs for {fp} -> {legacy_oriented_path.stem}")

    if not plane_refs:
        raise RuntimeError("No functional planes available")

    ref2d_raw = plane_refs[0]["ref2d_raw"]
    ref2d = plane_refs[0]["ref2d"]
    return {
        "plane_refs": plane_refs,
        "ref2d_raw": ref2d_raw,
        "ref2d": ref2d,
        "log_lines": log_lines,
        "bindings": {
            "plane_refs": plane_refs,
            "ref2d_raw": ref2d_raw,
            "ref2d": ref2d,
            "FUNCTIONAL_INPUT_XY_FRAME": frame,
            "FUNCTIONAL_REFERENCE_XY_FRAME": CANONICAL_XY_FRAME,
        },
    }


def ncc_xy(template: np.ndarray, image: np.ndarray, *, use_cv2: bool = True) -> tuple[int, int, float]:
    template_arr = np.asarray(template, dtype=np.float32)
    image_arr = np.asarray(image, dtype=np.float32)
    if bool(use_cv2) and cv2 is not None:
        templ = (norm01(template_arr) * 255).astype(np.uint8)
        img = (norm01(image_arr) * 255).astype(np.uint8)
        res = cv2.matchTemplate(img, templ, cv2.TM_CCORR_NORMED)
        ij = np.unravel_index(np.argmax(res), res.shape)
        y0, x0 = int(ij[0]), int(ij[1])
        return x0, y0, float(res[y0, x0])
    res = feature.match_template(norm01(image_arr), norm01(template_arr), pad_input=False)
    ij = np.unravel_index(np.argmax(res), res.shape)
    y0, x0 = int(ij[0]), int(ij[1])
    return x0, y0, float(res[y0, x0])


def _place_image_on_canvas(img: np.ndarray, output_shape: tuple[int, int], x0: int, y0: int) -> np.ndarray | None:
    placed = np.zeros(output_shape, dtype=np.float32)
    h, w = img.shape[-2], img.shape[-1]
    y0i = max(0, int(y0))
    x0i = max(0, int(x0))
    y1 = min(output_shape[0], y0i + h)
    x1 = min(output_shape[1], x0i + w)
    sy0 = max(0, -int(y0))
    sx0 = max(0, -int(x0))
    sy1 = sy0 + (y1 - y0i)
    sx1 = sx0 + (x1 - x0i)
    if y1 <= y0i or x1 <= x0i:
        return None
    placed[y0i:y1, x0i:x1] = img[sy0:sy1, sx0:sx1]
    return placed


def _clip_norm01(img: np.ndarray, pmin: float = 5.0, pmax: float = 95.0) -> np.ndarray:
    arr = np.asarray(img, dtype=np.float32)
    lo, hi = np.percentile(arr, (float(pmin), float(pmax)))
    if hi <= lo:
        return norm01(arr)
    arr = np.clip(arr, lo, hi)
    return norm01(arr)


def _xy_spacing_from_vox(vox_anat: Any = None) -> tuple[float, float]:
    vox = vox_anat if isinstance(vox_anat, dict) else {}
    try:
        x_spacing = float(vox.get("X", vox.get(2, vox.get("2", 1.0))))
        y_spacing = float(vox.get("Y", vox.get(1, vox.get("1", 1.0))))
    except Exception:
        x_spacing, y_spacing = 1.0, 1.0
    if not np.isfinite(x_spacing) or x_spacing <= 0:
        x_spacing = 1.0
    if not np.isfinite(y_spacing) or y_spacing <= 0:
        y_spacing = 1.0
    return x_spacing, y_spacing


def _set_ants_2d_metadata(image: Any, *, spacing: tuple[float, float]) -> None:
    image.set_spacing(tuple(float(v) for v in spacing))
    image.set_origin((0.0, 0.0))
    image.set_direction(np.eye(2))


def _square_bounds_from_region_spec(spec: dict[str, Any], shape: tuple[int, int]) -> tuple[int, int, int, int] | None:
    if not isinstance(spec, dict):
        return None
    h, w = int(shape[0]), int(shape[1])
    bounds = spec.get("bounds_xyxy")
    if bounds is not None:
        try:
            x0, y0, x1, y1 = [int(round(float(v))) for v in bounds[:4]]
        except Exception:
            return None
    else:
        try:
            cx = int(round(float(spec["center_x"])))
            cy = int(round(float(spec["center_y"])))
            size = max(1, int(round(float(spec["size_px"]))))
        except Exception:
            return None
        half = size / 2.0
        x0 = int(round(cx - half))
        x1 = int(round(cx + half))
        y0 = int(round(cy - half))
        y1 = int(round(cy + half))
    x0 = max(0, min(w, int(x0)))
    x1 = max(0, min(w, int(x1)))
    y0 = max(0, min(h, int(y0)))
    y1 = max(0, min(h, int(y1)))
    if x1 <= x0 or y1 <= y0:
        return None
    return x0, y0, x1, y1


def apply_square_region_mask(
    image: np.ndarray,
    spec: dict[str, Any],
) -> tuple[np.ndarray, np.ndarray, tuple[int, int, int, int]]:
    """Return a full-size image with values outside the selected anatomy-space square set to zero."""
    arr = np.asarray(image, dtype=np.float32)
    if arr.ndim != 2:
        raise ValueError(f"Expected a 2D image for square masking, got shape {arr.shape!r}")
    bounds = _square_bounds_from_region_spec(spec, tuple(arr.shape))
    if bounds is None:
        raise ValueError("Region square spec does not define a valid in-bounds square.")
    x0, y0, x1, y1 = bounds
    mask = np.zeros(tuple(arr.shape), dtype=bool)
    mask[y0:y1, x0:x1] = True
    masked = np.where(mask, arr, 0.0).astype(np.float32, copy=False)
    return masked, mask, bounds


def _load_square_region_mask_json(path: str | Path | None) -> tuple[dict[str, Any] | None, Path | None]:
    if path in (None, "", False):
        return None, None
    mask_path = Path(path)
    if not mask_path.exists():
        raise FileNotFoundError(f"ANTs fixed-region mask JSON not found: {mask_path}")
    with mask_path.open("r") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError(f"ANTs fixed-region mask JSON must contain an object: {mask_path}")
    return payload, mask_path


def _select_square_region_spec_for_plane(
    payload: dict[str, Any],
    *,
    plane_idx: int | None,
    label: str | None,
) -> dict[str, Any]:
    regions = payload.get("regions")
    if not isinstance(regions, list):
        return payload

    for region in regions:
        if not isinstance(region, dict):
            continue
        try:
            if plane_idx is not None and int(region.get("plane_idx")) == int(plane_idx):
                return region
        except Exception:
            pass

    if label not in (None, ""):
        for region in regions:
            if isinstance(region, dict) and str(region.get("plane_label", "")) == str(label):
                return region

    raise ValueError(f"ANTs fixed-region mask JSON has no region for plane_idx={plane_idx!r}, label={label!r}")


def _copy_ants_transforms(transformlist: list[str], out_dir: Path, label: str, method: str) -> list[str]:
    out_dir.mkdir(parents=True, exist_ok=True)
    copied: list[str] = []
    safe_label = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(label))
    for idx, src in enumerate(transformlist or []):
        src_path = Path(src)
        if not src_path.exists():
            copied.append(str(src))
            continue
        suffix = "".join(src_path.suffixes) or src_path.suffix
        dst = out_dir / f"{safe_label}_{method}_{idx}{suffix}"
        shutil.copy2(src_path, dst)
        copied.append(str(dst))
    return copied


def _valid_fraction(img: np.ndarray) -> float:
    arr = np.asarray(img)
    if arr.size == 0:
        return 0.0
    return float(np.count_nonzero(np.isfinite(arr) & (np.abs(arr) > 1e-8)) / arr.size)


def _post_transform_ncc(warped: np.ndarray, fixed: np.ndarray) -> float:
    warped_arr = np.asarray(warped, dtype=np.float32)
    fixed_arr = np.asarray(fixed, dtype=np.float32)
    if warped_arr.shape != fixed_arr.shape:
        return float("nan")
    mask = np.isfinite(warped_arr) & np.isfinite(fixed_arr) & (np.abs(warped_arr) > 1e-8)
    if int(mask.sum()) < 4:
        return float("nan")
    return corrcoef_img(norm01(warped_arr[mask]), norm01(fixed_arr[mask]))


def _ncc_in_plane_result(
    *,
    ref_scaled: np.ndarray,
    fixed_slice: np.ndarray,
    use_cv2: bool,
    display_normalize_placed: bool,
    initial_ncc_xy: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if isinstance(initial_ncc_xy, dict):
        x0 = int(initial_ncc_xy["x0"])
        y0 = int(initial_ncc_xy["y0"])
        score = float(initial_ncc_xy["score"])
    else:
        x0, y0, score = ncc_xy(ref_scaled, fixed_slice, use_cv2=use_cv2)
    metric_img = _place_image_on_canvas(np.asarray(ref_scaled, dtype=np.float32), tuple(fixed_slice.shape), x0, y0)
    display_img = _place_image_on_canvas(
        norm01(ref_scaled) if display_normalize_placed else np.asarray(ref_scaled, dtype=np.float32),
        tuple(fixed_slice.shape),
        x0,
        y0,
    )
    if metric_img is None:
        raise RuntimeError("NCC placement produced no overlap with anatomy canvas")
    tform = transform.SimilarityTransform(translation=(int(x0), int(y0)))
    return {
        "status": "ok",
        "method": "ncc_xy",
        "warped": metric_img,
        "display_warped": display_img if display_img is not None else metric_img,
        "transform": tform,
        "ncc_xy": {"x0": int(x0), "y0": int(y0), "score": float(score)},
        "post_ncc": _post_transform_ncc(metric_img, fixed_slice),
        "valid_fraction": _valid_fraction(metric_img),
    }


def _ants_rigid_affine_in_plane_result(
    *,
    ref_scaled: np.ndarray,
    fixed_slice: np.ndarray,
    plane_idx: int,
    label: str,
    output_dir: Path | None,
    spacing: tuple[float, float],
    config: InPlaneRegistrationComparisonConfig,
    initial_ncc_xy: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if bool(config.ants_require_fixed_mask) and config.ants_fixed_mask_json in (None, "", False):
        raise RuntimeError("ants_rigid_affine requires a saved fixed-region mask JSON; run [19a] first.")
    try:
        import ants
    except Exception as exc:  # pragma: no cover - depends on optional native package
        raise ImportError("ANTsPy is required for ants_rigid_affine in-plane registration") from exc
    if config.ants_deterministic_seed is not None:
        set_deterministic = getattr(getattr(ants, "config", None), "set_ants_deterministic", None)
        if callable(set_deterministic):
            set_deterministic(True, seed_value=int(config.ants_deterministic_seed))

    pmin, pmax = config.clip_percentiles
    ref_moving_np = _clip_norm01(ref_scaled, pmin=pmin, pmax=pmax).astype(np.float32)
    fixed_np = _clip_norm01(fixed_slice, pmin=pmin, pmax=pmax).astype(np.float32)
    if isinstance(initial_ncc_xy, dict):
        ncc_x0 = int(initial_ncc_xy["x0"])
        ncc_y0 = int(initial_ncc_xy["y0"])
        ncc_score = float(initial_ncc_xy["score"])
    else:
        ncc_x0, ncc_y0, ncc_score = ncc_xy(ref_moving_np, fixed_np, use_cv2=bool(config.use_cv2))
    moving_np = _place_image_on_canvas(ref_moving_np, tuple(fixed_np.shape), ncc_x0, ncc_y0)
    moving_support_np = _place_image_on_canvas(
        np.ones(tuple(ref_moving_np.shape), dtype=np.float32),
        tuple(fixed_np.shape),
        ncc_x0,
        ncc_y0,
    )
    if moving_np is None or moving_support_np is None:
        raise RuntimeError("NCC initialization produced no overlap with the ANTs fixed canvas")
    moving_np = np.asarray(moving_np, dtype=np.float32)
    moving_support_np = np.asarray(moving_support_np, dtype=np.float32) > 0.5
    fixed_region_spec, fixed_region_path = _load_square_region_mask_json(config.ants_fixed_mask_json)
    fixed_mask_np = None
    fixed_region_bounds = None
    if fixed_region_spec is not None:
        fixed_region_spec = _select_square_region_spec_for_plane(fixed_region_spec, plane_idx=plane_idx, label=label)
        fixed_np, fixed_mask_np, fixed_region_bounds = apply_square_region_mask(fixed_np, fixed_region_spec)
    moving = ants.from_numpy(moving_np)
    fixed = ants.from_numpy(fixed_np)
    _set_ants_2d_metadata(moving, spacing=spacing)
    _set_ants_2d_metadata(fixed, spacing=spacing)
    fixed_mask = None
    if fixed_mask_np is not None:
        fixed_mask = ants.from_numpy(fixed_mask_np.astype(np.float32, copy=False))
        _set_ants_2d_metadata(fixed_mask, spacing=spacing)

    reg_kwargs = {
        "aff_iterations": tuple(int(v) for v in config.ants_aff_iterations),
        "aff_shrink_factors": tuple(int(v) for v in config.ants_aff_shrink_factors),
        "aff_smoothing_sigmas": tuple(int(v) for v in config.ants_aff_smoothing_sigmas),
    }
    if fixed_mask is not None:
        reg_kwargs["mask"] = fixed_mask
        reg_kwargs["mask_all_stages"] = True

    reg_rigid = ants.registration(
        fixed=fixed,
        moving=moving,
        type_of_transform="Rigid",
        **reg_kwargs,
    )
    reg_affine = ants.registration(
        fixed=fixed,
        moving=moving,
        initial_transform=reg_rigid["fwdtransforms"][0],
        type_of_transform="Affine",
        **reg_kwargs,
    )
    transformlist = list(reg_affine.get("fwdtransforms", []))
    if output_dir is not None:
        transformlist = _copy_ants_transforms(transformlist, output_dir / "transforms", label, "ants_rigid_affine")

    warped = np.asarray(reg_affine["warpedmovout"].numpy(), dtype=np.float32)
    mask_img = ants.from_numpy(moving_support_np.astype(np.float32, copy=False))
    _set_ants_2d_metadata(mask_img, spacing=spacing)
    mask_warped = ants.apply_transforms(
        fixed=fixed,
        moving=mask_img,
        transformlist=transformlist,
        interpolator="nearestNeighbor",
    )
    valid_mask = np.asarray(mask_warped.numpy(), dtype=np.float32) > 0.5
    warped_masked = np.where(valid_mask, warped, 0.0).astype(np.float32)
    return {
        "status": "ok",
        "method": "ants_rigid_affine",
        "warped": warped_masked,
        "display_warped": warped_masked,
        "transform": {
            "type": "ants_transformlist",
            "method": "ants_rigid_affine",
            "transformlist": transformlist,
            "ncc_preplacement": {
                "x0": int(ncc_x0),
                "y0": int(ncc_y0),
                "score": float(ncc_score),
                "source_shape": tuple(int(v) for v in ref_moving_np.shape),
                "canvas_shape": tuple(int(v) for v in fixed_np.shape),
            },
            "fixed_spacing": tuple(float(v) for v in spacing),
            "moving_spacing": tuple(float(v) for v in spacing),
            "fixed_origin": (0.0, 0.0),
            "moving_origin": (0.0, 0.0),
            "fixed_direction": np.eye(2).tolist(),
            "moving_direction": np.eye(2).tolist(),
            "moving_shape": tuple(int(v) for v in moving_np.shape),
            "fixed_shape": tuple(int(v) for v in fixed_np.shape),
        },
        "post_ncc": _post_transform_ncc(warped_masked, fixed_np),
        "valid_fraction": float(valid_mask.mean()) if valid_mask.size else 0.0,
        "ncc_xy": {"x0": int(ncc_x0), "y0": int(ncc_y0), "score": float(ncc_score)},
        "transformlist": transformlist,
        "ants_fixed_mask_path": str(fixed_region_path) if fixed_region_path is not None else None,
        "ants_fixed_mask_bounds_xyxy": fixed_region_bounds,
        "ants_fixed_mask_fraction": float(fixed_mask_np.mean()) if fixed_mask_np is not None and fixed_mask_np.size else np.nan,
    }


def _summarize_in_plane_recommendations(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["fish_id", "plane_idx", "plane", "recommended_method", "recommended_post_ncc"])
    rows: list[dict[str, Any]] = []
    for keys, sub in df[df["status"].eq("ok")].groupby(["fish_id", "plane_idx", "plane"], dropna=False):
        fish_id, plane_idx, plane = keys
        work = sub.copy()
        work["post_ncc"] = pd.to_numeric(work["post_ncc"], errors="coerce")
        work["valid_fraction"] = pd.to_numeric(work["valid_fraction"], errors="coerce")
        work = work.sort_values(["post_ncc", "valid_fraction"], ascending=[False, False], na_position="last")
        if work.empty:
            continue
        best = work.iloc[0]
        rows.append(
            {
                "fish_id": fish_id,
                "plane_idx": int(plane_idx),
                "plane": plane,
                "recommended_method": str(best["method"]),
                "recommended_post_ncc": float(best["post_ncc"]) if pd.notna(best["post_ncc"]) else np.nan,
                "recommended_valid_fraction": float(best["valid_fraction"]) if pd.notna(best["valid_fraction"]) else np.nan,
            }
        )
    return pd.DataFrame(rows)


def run_in_plane_registration_comparison_stage(
    *,
    plane_refs: list[dict[str, Any]] | None,
    anat_f: Any,
    fish_id: str | None = None,
    out_ncc: str | Path | None = None,
    best_z: int = 0,
    vox_anat: Any = None,
    config: InPlaneRegistrationComparisonConfig | None = None,
) -> dict[str, Any]:
    cfg = config or InPlaneRegistrationComparisonConfig()
    if not plane_refs:
        raise RuntimeError("plane_refs missing; run [16] first.")
    if anat_f is None:
        raise RuntimeError("anat_f missing; run [16] first.")

    methods = tuple(str(method).strip() for method in cfg.methods if str(method).strip())
    if not methods:
        raise ValueError("At least one in-plane registration method is required.")
    active_method = str(cfg.active_method).strip()
    if active_method not in methods:
        raise ValueError(f"active_method={active_method!r} is not included in methods={methods!r}")
    fallback_method = None if cfg.fallback_method in (None, "", False) else str(cfg.fallback_method).strip()
    if fallback_method and fallback_method not in methods:
        fallback_method = None

    anat_arr = np.asarray(anat_f, dtype=np.float32)
    output_dir = None
    if cfg.save_outputs and out_ncc is not None:
        output_dir = Path(out_ncc) / str(cfg.output_subdir)
        output_dir.mkdir(parents=True, exist_ok=True)

    spacing = _xy_spacing_from_vox(vox_anat)
    rows: list[dict[str, Any]] = []
    log_lines: list[str] = []
    first_ref_warped_raw = None
    first_ref_warped = None

    for plane_idx, plane_ref in enumerate(plane_refs):
        if plane_ref is None:
            continue
        resolved_plane_idx = int(plane_ref.get("index", plane_idx))
        label = str(plane_ref.get("label", f"plane{plane_idx}"))
        bz = int(plane_ref.get("best_z", best_z))
        if bz < 0 or bz >= anat_arr.shape[0]:
            log_lines.append(f"[20] {label}: best_z out of bounds ({bz})")
            continue
        fixed_slice = anat_arr[bz]
        ref_src = plane_ref.get("ref_match", plane_ref.get("ref2d_raw", plane_ref.get("ref2d")))
        if ref_src is None:
            log_lines.append(f"[20] {label}: missing functional reference")
            continue
        ref_scaled = np.asarray(ref_src, dtype=np.float32)
        ncc_scores_arr = np.asarray(plane_ref.get("ncc_scores", []), dtype=np.float32)
        depth_metrics = registration_metric_from_scores(ncc_scores_arr)
        second_best = float(np.partition(ncc_scores_arr, -2)[-2]) if ncc_scores_arr.size >= 2 else np.nan
        plane_results: dict[str, dict[str, Any]] = {}
        plane_errors: dict[str, str] = {}
        initial_ncc_xy = plane_ref.get("ncc_xy") if isinstance(plane_ref.get("ncc_xy"), dict) else None

        for method in methods:
            try:
                if method == "ncc_xy":
                    result = _ncc_in_plane_result(
                        ref_scaled=ref_scaled,
                        fixed_slice=fixed_slice,
                        use_cv2=bool(cfg.use_cv2),
                        display_normalize_placed=bool(cfg.display_normalize_placed),
                        initial_ncc_xy=initial_ncc_xy,
                    )
                elif method == "ants_rigid_affine":
                    result = _ants_rigid_affine_in_plane_result(
                        ref_scaled=ref_scaled,
                        fixed_slice=fixed_slice,
                        plane_idx=resolved_plane_idx,
                        label=label,
                        output_dir=output_dir,
                        spacing=spacing,
                        config=cfg,
                        initial_ncc_xy=initial_ncc_xy,
                    )
                else:
                    raise ValueError(f"Unsupported in-plane registration method: {method}")
                plane_results[method] = result
                ncc_xy_record = result.get("ncc_xy", {}) if isinstance(result.get("ncc_xy"), dict) else {}
                fixed_mask_bounds = result.get("ants_fixed_mask_bounds_xyxy")
                rows.append(
                    {
                        "fish_id": fish_id,
                        "plane_idx": resolved_plane_idx,
                        "plane": label,
                        "method": method,
                        "selected": False,
                        "requested_active_method": active_method,
                        "selected_method": None,
                        "fallback_method": fallback_method,
                        "fallback_reason": None,
                        "status": "ok",
                        "best_z": int(bz),
                        "scale": plane_ref.get("scale", np.nan),
                        "bestz_max_score": None if depth_metrics is None else depth_metrics.get("max_score"),
                        "bestz_second_best": second_best,
                        "bestz_peak_delta": None if depth_metrics is None else depth_metrics.get("peak_delta"),
                        "bestz_peak_zscore": None if depth_metrics is None else depth_metrics.get("peak_zscore"),
                        "bestz_z_count": int(ncc_scores_arr.size),
                        "post_ncc": float(result.get("post_ncc", np.nan)),
                        "valid_fraction": float(result.get("valid_fraction", np.nan)),
                        "ncc_xy_score": ncc_xy_record.get("score", np.nan),
                        "ncc_xy_x0": ncc_xy_record.get("x0", np.nan),
                        "ncc_xy_y0": ncc_xy_record.get("y0", np.nan),
                        "ref_shape": tuple(plane_ref.get("ref_shape", np.asarray(ref_scaled).shape)),
                        "ref_scaled_shape": tuple(np.asarray(ref_scaled).shape),
                        "transformlist": ";".join(result.get("transformlist", [])) if result.get("transformlist") else None,
                        "ants_fixed_mask_path": result.get("ants_fixed_mask_path"),
                        "ants_fixed_mask_bounds_xyxy": json.dumps(list(fixed_mask_bounds)) if fixed_mask_bounds is not None else None,
                        "ants_fixed_mask_fraction": result.get("ants_fixed_mask_fraction", np.nan),
                        "error": None,
                    }
                )
                mask_msg = ""
                if result.get("ants_fixed_mask_path"):
                    mask_msg = (
                        f" mask_bounds={result.get('ants_fixed_mask_bounds_xyxy')} "
                        f"mask_fraction={float(result.get('ants_fixed_mask_fraction', np.nan)):.3f}"
                    )
                log_lines.append(
                    f"[20] {label} method={method} z={bz} post_ncc={float(result.get('post_ncc', np.nan)):.4f} "
                    f"valid={float(result.get('valid_fraction', np.nan)):.3f}{mask_msg}"
                )
            except Exception as exc:
                plane_errors[method] = str(exc)
                rows.append(
                    {
                        "fish_id": fish_id,
                        "plane_idx": resolved_plane_idx,
                        "plane": label,
                        "method": method,
                        "selected": False,
                        "requested_active_method": active_method,
                        "selected_method": None,
                        "fallback_method": fallback_method,
                        "fallback_reason": None,
                        "status": "error",
                        "best_z": int(bz),
                        "scale": plane_ref.get("scale", np.nan),
                        "bestz_max_score": None if depth_metrics is None else depth_metrics.get("max_score"),
                        "bestz_second_best": second_best,
                        "bestz_peak_delta": None if depth_metrics is None else depth_metrics.get("peak_delta"),
                        "bestz_peak_zscore": None if depth_metrics is None else depth_metrics.get("peak_zscore"),
                        "bestz_z_count": int(ncc_scores_arr.size),
                        "post_ncc": np.nan,
                        "valid_fraction": np.nan,
                        "ncc_xy_score": np.nan,
                        "ncc_xy_x0": np.nan,
                        "ncc_xy_y0": np.nan,
                        "ref_shape": tuple(plane_ref.get("ref_shape", np.asarray(ref_scaled).shape)),
                        "ref_scaled_shape": tuple(np.asarray(ref_scaled).shape),
                        "transformlist": None,
                        "ants_fixed_mask_path": str(cfg.ants_fixed_mask_json) if cfg.ants_fixed_mask_json not in (None, "", False) else None,
                        "ants_fixed_mask_bounds_xyxy": None,
                        "ants_fixed_mask_fraction": np.nan,
                        "error": str(exc),
                    }
                )
                log_lines.append(f"[20] {label} method={method} ERROR: {exc}")
                if method == active_method and bool(cfg.fail_on_active_method_error) and not fallback_method:
                    raise

        active_result = plane_results.get(active_method)
        selected_method = active_method
        fallback_reason = None
        if active_result is None:
            if fallback_method and fallback_method in plane_results:
                active_result = plane_results[fallback_method]
                selected_method = fallback_method
                fallback_reason = plane_errors.get(active_method, f"active method {active_method!r} unavailable")
                log_lines.append(f"[20] WARNING: {label} active method {active_method} unavailable; using {fallback_method}: {fallback_reason}")
            elif bool(cfg.fail_on_active_method_error) and active_method in plane_errors:
                raise RuntimeError(f"{label} active method {active_method} failed: {plane_errors[active_method]}")
            else:
                continue

        for row in rows:
            if int(row.get("plane_idx", -1)) == resolved_plane_idx and str(row.get("plane")) == label:
                row["selected"] = str(row.get("method")) == selected_method
                row["selected_method"] = selected_method
                row["fallback_reason"] = fallback_reason

        plane_ref.setdefault("inplane_registration", {})
        plane_ref["inplane_registration"].update({method: result for method, result in plane_results.items()})
        if isinstance(plane_results.get("ncc_xy", {}).get("ncc_xy"), dict):
            plane_ref["ncc_xy"] = plane_results["ncc_xy"]["ncc_xy"]
        plane_ref["inplane_requested_active_method"] = active_method
        plane_ref["inplane_active_method"] = selected_method
        plane_ref["inplane_fallback_method"] = fallback_method
        plane_ref["inplane_fallback_reason"] = fallback_reason
        plane_ref["ref_warped_raw"] = np.asarray(active_result["warped"], dtype=np.float32)
        plane_ref["ref_warped"] = np.asarray(active_result["display_warped"], dtype=np.float32)
        plane_ref["ref_match"] = ref_scaled
        plane_ref["tform_src"] = selected_method
        if selected_method == "ncc_xy":
            plane_ref["ncc_xy"] = active_result["ncc_xy"]
            plane_ref["tform"] = active_result["transform"]
            plane_ref.pop("ants_transform", None)
            plane_ref.pop("ants_transformlist", None)
        elif selected_method == "ants_rigid_affine":
            plane_ref["ants_transform"] = active_result["transform"]
            plane_ref["ants_transformlist"] = active_result.get("transformlist", [])
            plane_ref.pop("tform", None)
        if plane_idx == 0:
            first_ref_warped_raw = plane_ref["ref_warped_raw"]
            first_ref_warped = plane_ref["ref_warped"]

        if output_dir is not None:
            safe_label = re.sub(r"[^A-Za-z0-9_.-]+", "_", label)
            for method, result in plane_results.items():
                tifffile.imwrite(output_dir / f"{safe_label}_{method}_warped.tif", np.asarray(result["warped"], dtype=np.float32))

    comparison_df = pd.DataFrame(rows)
    recommendation_df = _summarize_in_plane_recommendations(comparison_df)
    if output_dir is not None:
        comparison_path = output_dir / "inplane_registration_comparison.csv"
        recommendation_path = output_dir / "inplane_registration_recommendation.csv"
        comparison_df.to_csv(comparison_path, index=False)
        recommendation_df.to_csv(recommendation_path, index=False)
        log_lines.append(f"[20] Wrote in-plane comparison: {comparison_path}")
        log_lines.append(f"[20] Wrote in-plane recommendation: {recommendation_path}")
    else:
        comparison_path = None
        recommendation_path = None

    bindings = {"plane_refs": plane_refs, "INPLANE_REGISTRATION_COMPARISON_DF": comparison_df}
    if first_ref_warped_raw is not None:
        bindings["ref_warped_raw"] = first_ref_warped_raw
    if first_ref_warped is not None:
        bindings["ref_warped"] = first_ref_warped
    return {
        "plane_refs": plane_refs,
        "comparison_df": comparison_df,
        "recommendation_df": recommendation_df,
        "comparison_path": comparison_path,
        "recommendation_path": recommendation_path,
        "ref_warped_raw": first_ref_warped_raw,
        "ref_warped": first_ref_warped,
        "log_lines": log_lines,
        "bindings": bindings,
    }


def run_ncc_placement_stage(
    *,
    plane_refs: list[dict[str, Any]] | None,
    anat_f: Any,
    best_z: int = 0,
    config: FunctionalPlacementConfig | None = None,
) -> dict[str, Any]:
    cfg = config or FunctionalPlacementConfig()
    if not plane_refs:
        raise RuntimeError("plane_refs missing; run [16] first.")
    if anat_f is None:
        raise RuntimeError("anat_f missing; run [16] first.")

    anat_arr = np.asarray(anat_f, dtype=np.float32)
    log_lines: list[str] = []
    first_ref_warped_raw = None
    first_ref_warped = None

    for plane_idx, plane_ref in enumerate(plane_refs):
        if plane_ref is None:
            continue
        label = plane_ref.get("label", f"plane{plane_idx}")
        bz = int(plane_ref.get("best_z", best_z))
        a_slice = anat_arr[bz]
        ref_src = plane_ref.get("ref2d_raw", plane_ref.get("ref2d"))
        if ref_src is None:
            log_lines.append(f"[20a] Missing ref for {label}")
            continue
        ref_src = np.asarray(ref_src, dtype=np.float32)
        ref_scaled = np.asarray(plane_ref.get("ref_match", ref_src), dtype=np.float32)

        if ref_scaled.shape[0] > a_slice.shape[0] or ref_scaled.shape[1] > a_slice.shape[1]:
            log_lines.append(
                f"[20a] Template larger than anatomy for {label}: {tuple(ref_scaled.shape)} vs {tuple(a_slice.shape)}"
            )
            continue

        x0, y0, score = ncc_xy(ref_scaled, a_slice, use_cv2=cfg.use_cv2)
        plane_ref["ncc_xy"] = {"x0": x0, "y0": y0, "score": score}
        try:
            plane_ref["tform"] = transform.SimilarityTransform(translation=(x0, y0))
            plane_ref["tform_src"] = "ncc_xy"
        except Exception:
            plane_ref["tform"] = None

        if cfg.use_ncc_placement:
            ref_vis = norm01(ref_scaled) if cfg.display_normalize_placed else ref_scaled
            placed = _place_image_on_canvas(ref_vis, tuple(a_slice.shape), x0, y0)
            if placed is not None:
                plane_ref["ref_warped_raw"] = placed
                plane_ref["ref_warped"] = placed
                plane_ref["ref_match"] = ref_scaled
                if plane_idx == 0:
                    first_ref_warped_raw = placed
                    first_ref_warped = placed

        log_lines.append(f"[20a] {label} z={bz} score={score:.4f} top-left=({x0},{y0})")

    bindings = {"plane_refs": plane_refs}
    if first_ref_warped_raw is not None:
        bindings["ref_warped_raw"] = first_ref_warped_raw
    if first_ref_warped is not None:
        bindings["ref_warped"] = first_ref_warped
    return {
        "plane_refs": plane_refs,
        "ref_warped_raw": first_ref_warped_raw,
        "ref_warped": first_ref_warped,
        "log_lines": log_lines,
        "bindings": bindings,
    }


def scale_image(img: np.ndarray, scale: float) -> np.ndarray:
    arr = np.asarray(img, dtype=np.float32)
    if float(scale) == 1.0:
        return arr
    height, width = arr.shape[-2], arr.shape[-1]
    out_shape = (max(1, int(round(height * float(scale)))), max(1, int(round(width * float(scale)))))
    return transform.resize(arr, out_shape, order=1, preserve_range=True, anti_aliasing=True).astype(np.float32)


def registration_metric_from_scores(scores: np.ndarray | list[float] | None) -> dict[str, float | int | None] | None:
    if scores is None or len(scores) == 0:
        return None
    arr = np.asarray(scores, dtype=np.float32)
    best_z = int(np.argmax(arr))
    max_score = float(arr[best_z])
    second = float(np.partition(arr, -2)[-2]) if arr.size >= 2 else None
    mean_score = float(np.mean(arr))
    std_score = float(np.std(arr))
    peak_delta = (max_score - second) if second is not None else None
    peak_zscore = (max_score - mean_score) / (std_score + 1e-6)
    return {
        "best_z": best_z,
        "max_score": max_score,
        "peak_delta": peak_delta,
        "peak_zscore": peak_zscore,
    }


def _load_scale_cache(path: Path, cache_version: int) -> dict[str, Any]:
    default = {"version": cache_version, "per_fish": {}}
    if not path.exists():
        return default
    try:
        raw = json.loads(path.read_text())
    except Exception:
        return default
    if isinstance(raw, dict) and raw.get("version") == cache_version and isinstance(raw.get("per_fish"), dict):
        return raw
    converted = {"version": cache_version, "per_fish": {}}
    if isinstance(raw, dict):
        for fish, val in raw.items():
            if isinstance(val, (int, float)):
                converted["per_fish"][str(fish)] = {"legacy_scale": float(val), "planes": {}}
    return converted


def _load_bestz_cache(path: Path, cache_version: int) -> dict[str, Any]:
    default = {"version": cache_version, "per_fish": {}}
    if not path.exists():
        return default
    try:
        raw = json.loads(path.read_text())
    except Exception:
        return default
    if isinstance(raw, dict) and raw.get("version") == cache_version and isinstance(raw.get("per_fish"), dict):
        return raw
    converted = {"version": cache_version, "per_fish": {}}
    if isinstance(raw, dict):
        for key, by_plane in raw.items():
            if not (isinstance(key, str) and "|" in key and isinstance(by_plane, dict)):
                continue
            fish = key.split("|", 1)[0]
            fish_map = converted["per_fish"].setdefault(str(fish), {})
            for label, row in by_plane.items():
                if not isinstance(row, dict):
                    continue
                try:
                    fish_map[str(label)] = {
                        "best_z": int(row.get("best_z", 0)),
                        "scores": list(row.get("scores", [])),
                    }
                except Exception:
                    pass
    return converted


def _coerce_cached_scale(entry: Any) -> float | None:
    if isinstance(entry, (int, float)):
        return float(entry)
    if isinstance(entry, dict):
        val = entry.get("scale")
        if isinstance(val, (int, float)):
            return float(val)
    return None


def run_registration_search_stage(
    *,
    anat_stack_path: str | Path,
    plane_refs: list[dict[str, Any]],
    fish_id: str,
    out_ncc: str | Path,
    vox_anat: Any = None,
    vox_func: Any = None,
    config: RegistrationSearchConfig | None = None,
) -> dict[str, Any]:
    cfg = config or RegistrationSearchConfig()
    if not plane_refs:
        raise RuntimeError("plane_refs missing; run [12] first.")

    log_lines: list[str] = []
    anat = np.asarray(imread_any(anat_stack_path), dtype=np.float32)
    log_lines.append(f"Anatomy shape: {anat.shape}")
    anat_f = np.stack([local_unsharp(norm01(slice_img), cfg.sharpen_sigma, cfg.sharpen_amount) for slice_img in anat], axis=0)

    if cfg.rescale_func_to_anat:
        if not vox_anat:
            log_lines.append("[16] WARNING: VOX_ANAT missing; distances will be in pixels.")
        if not vox_func:
            log_lines.append("[16] NOTE: VOX_FUNC missing/untrusted; proceeding with NCC-based scale.")

    scale_backend = str(cfg.scale_backend).strip().lower()
    if scale_backend != "threads":
        log_lines.append(f"[16] Unsupported NCC_SCALE_BACKEND='{cfg.scale_backend}'; using threads")
        scale_backend = "threads"

    if cv2 is not None and cfg.use_cv2:
        try:
            cv2.setNumThreads(1)
        except Exception:
            pass

    z_max = anat_f.shape[0]
    if int(cfg.manual_z_limit) > 0:
        z_max = min(z_max, int(cfg.manual_z_limit))
    anat_subset = anat_f[:z_max]

    scale_cache_path = Path(out_ncc) / "ncc_scale_by_fish.json"
    bestz_cache_path = Path(out_ncc) / "ncc_bestz_by_plane.json"
    scale_cache = _load_scale_cache(scale_cache_path, cfg.cache_version)
    bestz_cache = _load_bestz_cache(bestz_cache_path, cfg.cache_version)
    fish_key = str(fish_id)
    fish_scale_entry = scale_cache.get("per_fish", {}).get(fish_key, {}) if isinstance(scale_cache, dict) else {}
    cached_scales = fish_scale_entry.get("planes", {}) if isinstance(fish_scale_entry, dict) else {}
    legacy_scale = fish_scale_entry.get("legacy_scale") if isinstance(fish_scale_entry, dict) else None
    cached_bestz = bestz_cache.get("per_fish", {}).get(fish_key, {}) if isinstance(bestz_cache, dict) else {}

    def score_scale_for_plane(ref_src: np.ndarray, scale: float) -> dict[str, Any] | None:
        ref_scaled = scale_image(ref_src, scale)
        best_z, scores = best_z_by_ncc(ref_scaled, anat_subset, use_cv2=bool(cfg.use_cv2))
        metrics = registration_metric_from_scores(scores)
        if metrics is None:
            return None
        metrics.update({"scale": float(scale), "scores": np.asarray(scores, dtype=np.float32)})
        return metrics

    def select_best_result(results: list[dict[str, Any]], metric_name: str) -> dict[str, Any] | None:
        if not results:
            return None
        vals = np.asarray([float(d.get(metric_name, -np.inf)) for d in results], dtype=np.float64)
        scales = np.asarray([float(d.get("scale", np.nan)) for d in results], dtype=np.float64)
        if vals.size == 0:
            return None
        best_idx = int(np.argmax(vals))
        best_val = float(vals[best_idx])
        tol = max(float(cfg.scale_center_abs_tol), abs(best_val) * float(cfg.scale_center_rel_tol))
        candidates = np.where(vals >= (best_val - tol))[0]
        if candidates.size == 0:
            return results[best_idx]
        if cfg.scale_center_prefer_high:
            chosen = int(candidates[np.argmax(scales[candidates])])
        else:
            chosen = int(candidates[np.argmin(scales[candidates])])
        return results[chosen]

    def search_scale_for_ref(ref_src: np.ndarray, label: str, seed_scale: float | None = None, refine_only: bool = False) -> tuple[dict[str, Any] | None, list[str]]:
        logs: list[str] = []
        best_metrics: dict[str, Any] | None = None

        if not refine_only:
            c0, c1, cstep = cfg.scale_coarse
            coarse_scales = np.arange(float(c0), float(c1) + 1e-9, float(cstep))
            logs.append(f"[16:{label}] Coarse sweep: {c0}-{c1} step {cstep} (n={len(coarse_scales)})")
            coarse_results: list[dict[str, Any]] = []
            for sc in coarse_scales:
                metrics = score_scale_for_plane(ref_src, float(sc))
                if metrics is not None:
                    coarse_results.append(metrics)
            if coarse_results:
                best_metrics = select_best_result(coarse_results, cfg.scale_metric)
                logs.append(f"[16:{label}] Coarse best: {float(best_metrics['scale']):.4f} using metric {cfg.scale_metric}")
            else:
                logs.append(f"[16:{label}] Coarse sweep empty")

        if refine_only and seed_scale is not None:
            seed_metrics = score_scale_for_plane(ref_src, float(seed_scale))
            if seed_metrics is not None:
                best_metrics = seed_metrics
                logs.append(f"[16:{label}] Refine seed: {float(seed_scale):.4f}")

        if best_metrics is None:
            fallback = float(seed_scale if seed_scale is not None else cfg.manual_scale)
            fallback_metrics = score_scale_for_plane(ref_src, fallback)
            if fallback_metrics is None:
                logs.append(f"[16:{label}] Failed to score fallback scale {fallback:.4f}")
                return None, logs
            best_metrics = fallback_metrics
            logs.append(f"[16:{label}] Using fallback scale: {fallback:.4f}")

        def _refine(window: tuple[float, float], label_prefix: str, precision: int, include_refine_tag: bool) -> None:
            nonlocal best_metrics
            try:
                half_win, step = window
                if float(half_win) <= 0 or float(step) <= 0:
                    return
                center = float(best_metrics["scale"])
                lo, hi = center - float(half_win), center + float(half_win)
                sweep = np.arange(lo, hi + 1e-9, float(step))
                center_fmt = f"{{:.{precision}f}}"
                lohi_fmt = f"{{:.{precision}f}}"
                tag = "refine, " if include_refine_tag and refine_only else ""
                logs.append(
                    f"[16:{label}] {label_prefix} sweep ({tag}center={center_fmt.format(center)}): "
                    f"{lohi_fmt.format(lo)}-{lohi_fmt.format(hi)} step {step} (n={len(sweep)})"
                )
                results: list[dict[str, Any]] = []
                for sc in sweep:
                    metrics = score_scale_for_plane(ref_src, float(sc))
                    if metrics is not None:
                        results.append(metrics)
                if results:
                    best_metrics = select_best_result(results, cfg.scale_metric)
                    logs.append(f"[16:{label}] {label_prefix} best: {float(best_metrics['scale']):.{precision}f} using metric {cfg.scale_metric}")
                else:
                    logs.append(f"[16:{label}] {label_prefix} empty; keeping {float(best_metrics['scale']):.{precision}f}")
            except Exception:
                return

        if not refine_only:
            _refine(cfg.scale_fine, "Fine", 4, include_refine_tag=False)
        _refine(cfg.scale_xfine, "Extra-fine", 4, include_refine_tag=True)
        _refine(cfg.scale_ufine, "Ultra-fine", 5, include_refine_tag=True)
        return best_metrics, logs

    def solve_plane(job: tuple[int, str, np.ndarray]) -> dict[str, Any]:
        p_idx, label, ref_src = job
        logs: list[str] = []
        cached_scale = _coerce_cached_scale(cached_scales.get(label))
        if cached_scale is None and isinstance(legacy_scale, (int, float)):
            cached_scale = float(legacy_scale)

        if not cfg.force_recompute:
            cached_best = cached_bestz.get(label)
            if isinstance(cached_best, dict):
                try:
                    best_scale = cached_best.get("scale", cached_scale)
                    best_scale = float(best_scale) if isinstance(best_scale, (int, float)) else None
                    best_z = int(cached_best.get("best_z", 0))
                    scores = np.asarray(cached_best.get("scores", []), dtype=np.float32)
                    if best_scale is not None and scores.size == z_max and 0 <= best_z < scores.size:
                        metrics = registration_metric_from_scores(scores)
                        if metrics is not None:
                            metrics.update({"scale": best_scale, "scores": scores})
                            logs.append(f"[16:{label}] Using cached scale+bz: {best_scale:.5f}")
                            return {
                                "plane": p_idx,
                                "label": label,
                                "logs": logs,
                                "from_cache": True,
                                "scale": float(best_scale),
                                "best_z": int(metrics["best_z"]),
                                "scores": np.asarray(metrics["scores"], dtype=np.float32),
                                "max_score": float(metrics.get("max_score", np.nan)),
                                "peak_delta": None if metrics.get("peak_delta") is None else float(metrics["peak_delta"]),
                                "peak_zscore": float(metrics.get("peak_zscore", np.nan)),
                            }
                except Exception:
                    pass

        seed = cached_scale if cached_scale is not None else None
        refine_only = bool(cfg.scale_refine_only and seed is not None)
        best_metrics, search_logs = search_scale_for_ref(ref_src, label=label, seed_scale=seed, refine_only=refine_only)
        logs.extend(search_logs)
        if best_metrics is None:
            logs.append(f"[16:{label}] FAILED: no valid scale result")
            return {"plane": p_idx, "label": label, "logs": logs, "failed": True}
        return {
            "plane": p_idx,
            "label": label,
            "logs": logs,
            "from_cache": False,
            "scale": float(best_metrics["scale"]),
            "best_z": int(best_metrics["best_z"]),
            "scores": np.asarray(best_metrics["scores"], dtype=np.float32),
            "max_score": float(best_metrics.get("max_score", np.nan)),
            "peak_delta": None if best_metrics.get("peak_delta") is None else float(best_metrics["peak_delta"]),
            "peak_zscore": float(best_metrics.get("peak_zscore", np.nan)),
        }

    jobs: list[tuple[int, str, np.ndarray]] = []
    for p_idx, plane_ref in enumerate(plane_refs):
        label = plane_ref.get("label", f"plane{p_idx}")
        ref_src = plane_ref.get("ref2d_raw", plane_ref.get("ref2d"))
        if ref_src is None:
            log_lines.append(f"[16:{label}] Missing reference; skipping")
            continue
        jobs.append((p_idx, label, np.asarray(ref_src, dtype=np.float32)))

    resolved_workers = cfg.scale_workers
    if resolved_workers is None:
        resolved_workers = max(1, min(len(plane_refs), (os.cpu_count() or 1)))
    results: dict[int, dict[str, Any]] = {}
    if cfg.scale_per_plane:
        n_workers = min(int(resolved_workers), max(1, len(jobs)))
        log_lines.append(f"[16] Per-plane sweep enabled: workers={n_workers}, planes={len(jobs)}, z={z_max}")
        with ThreadPoolExecutor(max_workers=n_workers) as executor:
            future_map = {executor.submit(solve_plane, job): job for job in jobs}
            for future in as_completed(future_map):
                _, label, _ = future_map[future]
                try:
                    out = future.result()
                except Exception as exc:
                    log_lines.append(f"[16:{label}] ERROR: {exc}")
                    continue
                log_lines.extend(out.get("logs", []))
                if out.get("failed"):
                    continue
                results[int(out["plane"])] = out
    else:
        log_lines.append(f"[16] Per-plane sweep disabled; running serial for {len(jobs)} planes")
        for job in jobs:
            out = solve_plane(job)
            log_lines.extend(out.get("logs", []))
            if out.get("failed"):
                continue
            results[int(out["plane"])] = out

    rows: list[dict[str, Any]] = []
    scores_by_plane: dict[str, np.ndarray] = {}
    scale_by_plane: dict[str, float] = {}
    best_z_value = 0
    manual_scale_value = float(cfg.manual_scale)

    for p_idx, plane_ref in enumerate(plane_refs):
        label = plane_ref.get("label", f"plane{p_idx}")
        out = results.get(p_idx)
        if out is None:
            continue
        ref_src = plane_ref.get("ref2d_raw", plane_ref.get("ref2d"))
        if ref_src is None:
            continue
        ref_src = np.asarray(ref_src, dtype=np.float32)
        scale = float(out["scale"])
        scores = np.asarray(out["scores"], dtype=np.float32)
        best_z = int(out["best_z"])
        ref_scaled = scale_image(ref_src, scale)
        plane_ref["scale"] = scale
        plane_ref["ref_match"] = ref_scaled
        plane_ref["ncc_scores"] = scores
        plane_ref["best_z"] = best_z
        plane_ref["ref_shape"] = tuple(ref_src.shape)
        plane_ref["ref_scaled_shape"] = tuple(ref_scaled.shape)
        scores_by_plane[label] = scores
        scale_by_plane[label] = scale

        second = float(np.partition(scores, -2)[-2]) if scores.size >= 2 else None
        mean_score = float(np.mean(scores)) if scores.size else None
        std_score = float(np.std(scores)) if scores.size else None
        peak_delta = (float(out["max_score"]) - second) if (scores.size and second is not None) else None
        peak_zscore = (
            (float(out["max_score"]) - mean_score) / (std_score + 1e-6)
            if (scores.size and mean_score is not None and std_score is not None)
            else None
        )
        rows.append(
            {
                "plane": label,
                "scale": scale,
                "best_z": best_z,
                "max_score": float(out["max_score"]) if scores.size else None,
                "second_best": second,
                "peak_delta": peak_delta,
                "peak_zscore": peak_zscore,
                "z_count": int(z_max),
                "ref_shape": tuple(ref_src.shape),
                "ref_scaled_shape": tuple(ref_scaled.shape),
            }
        )

    if rows and plane_refs:
        first_plane_ref = plane_refs[0]
        if first_plane_ref.get("scale") is not None:
            manual_scale_value = float(first_plane_ref["scale"])
        if first_plane_ref.get("best_z") is not None:
            best_z_value = int(first_plane_ref["best_z"])

    now_utc = datetime.now(timezone.utc).isoformat()
    scale_cache.setdefault("version", cfg.cache_version)
    scale_cache.setdefault("per_fish", {})
    scale_cache["version"] = cfg.cache_version
    scale_cache["per_fish"][fish_key] = {
        "updated_utc": now_utc,
        "mode": "per_plane",
        "planes": {row["plane"]: {"scale": float(row["scale"])} for row in rows},
    }
    bestz_cache.setdefault("version", cfg.cache_version)
    bestz_cache.setdefault("per_fish", {})
    bestz_cache["version"] = cfg.cache_version
    bestz_cache["per_fish"][fish_key] = {
        row["plane"]: {
            "scale": float(row["scale"]),
            "best_z": int(row["best_z"]),
            "scores": np.asarray(scores_by_plane.get(row["plane"], np.asarray([], dtype=np.float32))).tolist(),
        }
        for row in rows
    }
    scale_cache_path.parent.mkdir(parents=True, exist_ok=True)
    scale_cache_path.write_text(json.dumps(scale_cache, indent=2))
    log_lines.append(f"[16] Wrote scale cache: {scale_cache_path}")
    bestz_cache_path.parent.mkdir(parents=True, exist_ok=True)
    bestz_cache_path.write_text(json.dumps(bestz_cache, indent=2))
    log_lines.append(f"[16] Wrote best-z cache: {bestz_cache_path}")

    df = pd.DataFrame(rows)
    return {
        "plane_refs": plane_refs,
        "anat": anat,
        "anat_f": anat_f,
        "best_z": best_z_value,
        "ncc_manual_scale": manual_scale_value,
        "df": df,
        "scores_by_plane": scores_by_plane,
        "scale_by_plane": scale_by_plane,
        "scale_cache_path": scale_cache_path,
        "bestz_cache_path": bestz_cache_path,
        "log_lines": log_lines,
        "bindings": {
            "plane_refs": plane_refs,
            "anat": anat,
            "anat_f": anat_f,
            "best_z": best_z_value,
            "NCC_MANUAL_SCALE": manual_scale_value,
            "df": df,
            "SCALE_CACHE_PATH": scale_cache_path,
            "BESTZ_CACHE_PATH": bestz_cache_path,
        },
    }


__all__ = [
    "InPlaneRegistrationComparisonConfig",
    "FunctionalPlacementConfig",
    "FunctionalReferenceConfig",
    "RegistrationSearchConfig",
    "_find_embedded_nrrd_header",
    "_infer_voxels_from_open_tiff",
    "_infer_voxels_nrrd",
    "_parse_nrrd_header_text",
    "_res_to_um_per_px",
    "_to_um",
    "apply_square_region_mask",
    "apply_func_orientation",
    "best_z_by_ncc",
    "corrcoef_img",
    "build_functional_references_stage",
    "imread_any",
    "infer_voxels_tiff",
    "local_unsharp",
    "load_or_cache_voxels",
    "ncc_xy",
    "norm01",
    "registration_metric_from_scores",
    "run_in_plane_registration_comparison_stage",
    "run_ncc_placement_stage",
    "run_registration_search_stage",
    "scale_image",
    "top_correlated_mean",
    "zproject_mean",
]
