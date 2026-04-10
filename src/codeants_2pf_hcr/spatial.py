"""Spatial/image helpers for notebook utility cells such as [6] and registration prep."""

from __future__ import annotations

import json
from pathlib import Path
import re

import numpy as np
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


__all__ = [
    "_find_embedded_nrrd_header",
    "_infer_voxels_from_open_tiff",
    "_infer_voxels_nrrd",
    "_parse_nrrd_header_text",
    "_res_to_um_per_px",
    "_to_um",
    "apply_func_orientation",
    "best_z_by_ncc",
    "corrcoef_img",
    "imread_any",
    "infer_voxels_tiff",
    "local_unsharp",
    "load_or_cache_voxels",
    "norm01",
    "top_correlated_mean",
    "zproject_mean",
]
