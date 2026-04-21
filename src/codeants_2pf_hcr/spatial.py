"""Spatial/image helpers and notebook-facing stages for functional reference cells [12], [16], and [20]."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
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
        if nrrd is not None:
            data, _ = nrrd.read(str(target))
            return np.asarray(data)
        if sitk is not None:
            return np.asarray(sitk.GetArrayFromImage(sitk.ReadImage(str(target))))
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
    if str(polarity).strip().lower() == "north":
        out = out[..., ::-1, ::-1]
    if flip_x:
        out = out[..., ::-1]
    return out


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


@dataclass(frozen=True)
class FunctionalPlacementConfig:
    use_ncc_placement: bool = True
    display_normalize_placed: bool = True
    use_cv2: bool = True


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
    return plane_ref


def _load_cached_functional_ref(raw_path: Path, norm_path: Path) -> tuple[np.ndarray, np.ndarray] | None:
    if not raw_path.exists() or not norm_path.exists():
        return None
    ref2d_raw = np.asarray(imread_any(raw_path), dtype=np.float32)
    ref2d = norm01(imread_any(norm_path))
    return ref2d_raw, ref2d


def build_functional_references_stage(
    *,
    flipped_list: list[Path | str] | None,
    out_raw: Path | str,
    outdir: Path | str | None = None,
    vox_func_by_path: dict[str, Any] | None = None,
    config: FunctionalReferenceConfig | None = None,
) -> dict[str, Any]:
    cfg = config or FunctionalReferenceConfig()
    out_raw_path = Path(out_raw)
    outdir_path = Path(outdir) if outdir is not None else None
    out_raw_path.mkdir(parents=True, exist_ok=True)

    source_paths = [Path(path) for path in (flipped_list or []) if path]
    if not source_paths:
        raise FileNotFoundError("No flipped functional stacks available")

    reuse_saved_refs = bool(cfg.reuse_saved_refs)
    if cfg.force_recompute_refs:
        reuse_saved_refs = False

    plane_refs: list[dict[str, Any]] = []
    log_lines: list[str] = []
    if cfg.force_recompute_refs:
        log_lines.append("[12] FORCE_RECOMPUTE_REFS=True -> rebuilding functional refs")

    for fp in source_paths:
        if not fp.exists():
            raise FileNotFoundError(f"Flipped functional stack not found: {fp}")

        vox_f = vox_func_by_path.get(str(fp), {}) if vox_func_by_path else {}
        stem = fp.stem
        raw_path = out_raw_path / f"{stem}_ref_raw.tif"
        norm_path = out_raw_path / f"{stem}_ref_norm.tif"
        legacy_raw_path = (outdir_path / f"{stem}_ref_raw.tif") if outdir_path is not None else None
        legacy_norm_path = (outdir_path / f"{stem}_ref_norm.tif") if outdir_path is not None else None

        if reuse_saved_refs:
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
                    )
                )
            if stack_plane_refs:
                plane_refs.extend(stack_plane_refs)
                log_lines.append(f"[12] Using existing per-plane refs for {fp}")
                continue

            cached = _load_cached_functional_ref(raw_path, norm_path)
            if cached is not None:
                ref2d_raw, ref2d = cached
                plane_refs.append(
                    _functional_plane_ref(label=stem, ref2d_raw=ref2d_raw, ref2d=ref2d, vox_func=vox_f)
                )
                log_lines.append(f"[12] Using existing refs for {fp}")
                continue

            if legacy_raw_path is not None and legacy_norm_path is not None:
                cached = _load_cached_functional_ref(legacy_raw_path, legacy_norm_path)
                if cached is not None:
                    ref2d_raw, ref2d = cached
                    plane_refs.append(
                        _functional_plane_ref(label=stem, ref2d_raw=ref2d_raw, ref2d=ref2d, vox_func=vox_f)
                    )
                    log_lines.append(f"[12] Using existing refs for {fp} (legacy)")
                    continue

        func = np.asarray(imread_any(fp), dtype=np.float32)
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
                    )
                )
            log_lines.append(f"[12] Built top-correlated per-plane refs for {fp}")
            continue

        if func.ndim == 3:
            ref2d_raw = func.mean(axis=0).astype(np.float32)
            tifffile.imwrite(raw_path, ref2d_raw.astype(np.float32))
            ref2d = norm01(ref2d_raw)
            tifffile.imwrite(norm_path, (ref2d * 65535).astype(np.uint16))
            plane_refs.append(_functional_plane_ref(label=stem, ref2d_raw=ref2d_raw, ref2d=ref2d, vox_func=vox_f))
            log_lines.append(f"[12] Built mean reference for {fp}")
            continue

        if func.ndim != 4:
            raise ValueError(f"Unsupported functional stack ndim={func.ndim} for {fp}")

        _, z_count, _, _ = func.shape
        for zi in range(z_count):
            plane_t = func[:, zi, :, :]
            ref2d_raw_i = plane_t.mean(axis=0).astype(np.float32)
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
                )
            )
        log_lines.append(f"[12] Built mean per-plane refs for {fp}")

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
    "FunctionalPlacementConfig",
    "FunctionalReferenceConfig",
    "RegistrationSearchConfig",
    "_find_embedded_nrrd_header",
    "_infer_voxels_from_open_tiff",
    "_infer_voxels_nrrd",
    "_parse_nrrd_header_text",
    "_res_to_um_per_px",
    "_to_um",
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
    "run_ncc_placement_stage",
    "run_registration_search_stage",
    "scale_image",
    "top_correlated_mean",
    "zproject_mean",
]
