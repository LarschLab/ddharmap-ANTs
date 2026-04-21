"""Segmentation and functional-label QA helpers for notebook cells [24], [26], and [26a]."""

from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
from typing import Any, Callable

import numpy as np
import pandas as pd
import tifffile

from .matching import _ensure_uint_labels
from .runtime import optional_dependency_error
from .spatial import imread_any, infer_voxels_tiff, norm01


@dataclass(frozen=True)
class HcrCellposeConfig:
    save_tif: bool = True
    skip_if_exists: bool = True
    use_anisotropy: bool = True
    anisotropy_override: float | None = None
    model_path_override: str | Path | None = None
    data_mode: str = "nas"
    use_gpu: bool = True
    verbose: bool = True


@dataclass(frozen=True)
class AnatomyCellposeConfig:
    force_recompute: bool = False
    skip_if_exists: bool = True
    save_8bit: bool = True
    use_anisotropy: bool = True
    use_gpu: bool | None = None
    compute_device: str | None = None
    verbose: bool = True


def resolve_hcr_cellpose_model_path(
    *,
    model_path_override: str | Path | None = None,
    cp_hcr_model_path: str | Path | None = None,
    cp_hcr_model_path_default: str | Path | None = None,
    data_mode: str = "nas",
    data_root: str | Path | None = None,
    local_root: str | Path | None = None,
    nas_root: str | Path | None = None,
) -> str:
    for candidate in (model_path_override, cp_hcr_model_path, cp_hcr_model_path_default):
        if candidate not in (None, "", False):
            return str(candidate)
    mode_is_local = str(data_mode).strip().lower() == "local"
    if mode_is_local and data_root not in (None, "", False):
        root = Path(data_root)
    elif local_root not in (None, "", False) and mode_is_local:
        root = Path(local_root)
    elif nas_root not in (None, "", False):
        root = Path(nas_root) / "Danin"
    else:
        raise RuntimeError("Cannot resolve default HCR Cellpose model path without NAS_ROOT.")
    return str(root / "Cellpose" / "models" / "HCR_cpsam_20251006_094039")


def collect_hcr_intensity_stack_paths(
    *,
    hcr_intensity_paths: list[str | Path] | None = None,
    preproc_dir: str | Path | None = None,
) -> list[Path]:
    if hcr_intensity_paths:
        intensity_paths = [Path(path) for path in hcr_intensity_paths]
    else:
        intensity_paths = []
        if preproc_dir is not None:
            base = Path(preproc_dir)
            for directory in (base / "rbest", base / "rn"):
                if directory.exists():
                    intensity_paths.extend(sorted(directory.glob("*round*_channel*.nrrd")))
                    intensity_paths.extend(sorted(directory.glob("*round*_channel*.tif")))
                    intensity_paths.extend(sorted(directory.glob("*round*_channel*.tiff")))
    intensity_paths = [path for path in intensity_paths if "fullbrain" not in path.name.lower()]
    intensity_paths = [path for path in intensity_paths if "channel1" not in path.name.lower()]
    intensity_paths = [path for path in intensity_paths if "_cp_masks" not in path.stem.lower()]
    return intensity_paths


def _src_priority(path_obj: Path) -> int:
    suffix = str(path_obj.suffix).lower()
    if suffix == ".nrrd":
        return 0
    if suffix == ".tif":
        return 1
    return 2


def deduplicate_hcr_intensity_targets(intensity_paths: list[Path], out_conf_masks: str | Path) -> tuple[list[tuple[Path, Path]], int]:
    out_masks = Path(out_conf_masks)
    candidate_pairs: list[tuple[Path, Path]] = []
    candidate_idx: dict[Path, int] = {}
    duplicate_targets = 0
    for intensity_path in intensity_paths:
        mask_path = out_masks / f"{intensity_path.stem}_cp_masks.tif"
        prev_idx = candidate_idx.get(mask_path)
        if prev_idx is None:
            candidate_idx[mask_path] = len(candidate_pairs)
            candidate_pairs.append((intensity_path, mask_path))
            continue
        duplicate_targets += 1
        prev_ip, _ = candidate_pairs[prev_idx]
        if _src_priority(intensity_path) < _src_priority(prev_ip):
            candidate_pairs[prev_idx] = (intensity_path, mask_path)
    return candidate_pairs, duplicate_targets


def _pending_pairs(candidate_pairs: list[tuple[Path, Path]], *, skip_if_exists: bool) -> tuple[list[tuple[Path, Path]], list[str]]:
    pending: list[tuple[Path, Path]] = []
    log_lines: list[str] = []
    for intensity_path, mask_path in candidate_pairs:
        if skip_if_exists and mask_path.exists():
            log_lines.append(f"[SKIP] existing masks: {mask_path}")
            continue
        pending.append((intensity_path, mask_path))
    return pending, log_lines


def _normalize_to_uint8(arr: np.ndarray) -> tuple[np.ndarray, str]:
    if arr.dtype == np.uint8 or (np.issubdtype(arr.dtype, np.integer) and arr.max() <= 255 and arr.min() >= 0):
        return arr.astype(np.uint8, copy=False), "native"
    return (norm01(arr) * 255).astype(np.uint8), f"normalized_from_{arr.dtype}"


def _compute_anisotropy(
    intensity_path: Path,
    *,
    use_anisotropy: bool,
    anisotropy_override: float | None,
) -> tuple[float | None, dict[str, Any] | None]:
    if not use_anisotropy:
        return None, None
    if anisotropy_override is not None:
        anisotropy = float(anisotropy_override)
        if not (anisotropy > 0 and np.isfinite(anisotropy)):
            raise ValueError("anisotropy override must be finite and > 0")
        return anisotropy, {"override": anisotropy}
    vox = None
    try:
        vox = infer_voxels_tiff(intensity_path)
    except Exception:
        vox = None
    if vox and vox.get("Z") and vox.get("X"):
        anisotropy = float(vox["Z"]) / float(vox["X"])
        if anisotropy > 0 and np.isfinite(anisotropy):
            return anisotropy, vox
    return None, vox


def _resolve_cellpose_gpu_flag(*, use_gpu: bool | None, compute_device: str | None) -> bool:
    device = None if compute_device in (None, "", False) else str(compute_device).strip().lower()
    if device in {"cpu"}:
        return False
    if device in {"cuda", "gpu", "mps"}:
        return True
    if use_gpu is not None:
        return bool(use_gpu)
    try:
        import torch
    except Exception:  # pragma: no cover
        return False
    try:
        if torch.cuda.is_available():
            return True
    except Exception:  # pragma: no cover
        pass
    try:
        mps_backend = getattr(getattr(torch, "backends", None), "mps", None)
        if mps_backend is not None and bool(mps_backend.is_available()):
            return True
    except Exception:  # pragma: no cover
        pass
    return False


def _load_cellpose_model(*, cp_model: Path, use_gpu: bool, stage_tag: str, log_lines: list[str]) -> Any:
    try:
        from cellpose import io, models
    except Exception as exc:  # pragma: no cover
        raise optional_dependency_error(module_name="cellpose", stage_tag=stage_tag, exc=exc) from exc

    io.logger_setup()
    try:
        return models.CellposeModel(gpu=use_gpu, pretrained_model=str(cp_model))
    except Exception as exc:  # pragma: no cover
        msg = str(exc)
        if "weights_only" not in msg and "WeightsUnpickler" not in msg and "UnpicklingError" not in msg:
            raise
        log_lines.append(f"[WARN] {stage_tag} Cellpose model load failed with weights_only=True; retrying with weights_only=False")
        try:
            import torch.serialization as torch_serialization
            from cellpose import vit_sam
        except Exception as retry_exc:  # pragma: no cover
            raise optional_dependency_error(module_name="torch/cellpose", stage_tag=stage_tag, exc=retry_exc) from retry_exc

        orig_load = vit_sam.torch.load
        try:
            vit_sam.torch.load = lambda *a, **k: torch_serialization.load(*a, **{**k, "weights_only": False})
            return models.CellposeModel(gpu=use_gpu, pretrained_model=str(cp_model))
        finally:
            vit_sam.torch.load = orig_load


def _prepare_anat_volume(path: Path, *, log_lines: list[str]) -> np.ndarray:
    vol = np.asarray(imread_any(path))
    if vol.ndim == 4 and vol.shape[-1] in (3, 4):
        vol = vol[..., 0]
    if path.suffix.lower() == ".nrrd" and vol.ndim == 3 and vol.shape[-1] < min(vol.shape[0], vol.shape[1]):
        vol = vol.transpose(2, 1, 0)
        log_lines.append(f"[ANAT CP] Reordered NRRD to (Z, Y, X): {path.name} -> {vol.shape}")
    if vol.ndim not in (2, 3):
        raise ValueError(f"Unsupported anatomy array shape for Cellpose: {vol.shape}")
    return vol


def _mask_shape(path: Path) -> tuple[int, ...]:
    arr = np.asarray(tifffile.imread(str(path)))
    if arr.ndim == 4 and arr.shape[-1] in (3, 4):
        arr = arr[..., 0]
    return tuple(int(v) for v in arr.shape)


def _to_uint8_preserve(arr: np.ndarray, *, log_lines: list[str]) -> np.ndarray:
    arr = np.asarray(arr)
    if arr.dtype == np.uint8:
        return arr
    if np.issubdtype(arr.dtype, np.integer):
        amin = int(arr.min()) if arr.size else 0
        amax = int(arr.max()) if arr.size else 0
        if amin >= 0 and amax <= 255:
            return arr.astype(np.uint8, copy=False)
        log_lines.append(f"[ANAT CP] Integer intensity range [{amin}, {amax}] -> clipping to [0,255] (no normalization).")
        return np.clip(arr, 0, 255).astype(np.uint8)

    arrf = arr.astype(np.float32, copy=False)
    finite = np.isfinite(arrf)
    if not finite.any():
        raise ValueError("Anatomy volume has no finite values.")
    fmin = float(np.nanmin(arrf[finite]))
    fmax = float(np.nanmax(arrf[finite]))
    arrf = np.nan_to_num(arrf, nan=0.0, posinf=255.0, neginf=0.0)
    if fmin >= 0.0 and fmax <= 255.0:
        return arrf.astype(np.uint8)
    log_lines.append(f"[ANAT CP] Float intensity range [{fmin:.3f}, {fmax:.3f}] -> clipping to [0,255] (no normalization).")
    return np.clip(arrf, 0.0, 255.0).astype(np.uint8)


def run_hcr_cellpose_stage(
    *,
    fish_dir: str | Path,
    preproc_dir: str | Path | None,
    hcr_intensity_paths: list[str | Path] | None = None,
    cp_hcr_model_path: str | Path | None = None,
    cp_hcr_model_path_default: str | Path | None = None,
    data_mode: str = "nas",
    data_root: str | Path | None = None,
    local_root: str | Path | None = None,
    nas_root: str | Path | None = None,
    config: HcrCellposeConfig | None = None,
) -> dict[str, Any]:
    cfg = config or HcrCellposeConfig()
    fish_path = Path(fish_dir)
    out_conf_raw = fish_path / "03_analysis" / "confocal" / "raw"
    out_conf_masks = out_conf_raw / "cp_masks"
    out_conf_convert = out_conf_raw / "converted_nrrd_to_tif"
    out_conf_raw.mkdir(parents=True, exist_ok=True)
    out_conf_masks.mkdir(parents=True, exist_ok=True)
    out_conf_convert.mkdir(parents=True, exist_ok=True)

    cp_model_path = resolve_hcr_cellpose_model_path(
        model_path_override=cfg.model_path_override,
        cp_hcr_model_path=cp_hcr_model_path,
        cp_hcr_model_path_default=cp_hcr_model_path_default,
        data_mode=cfg.data_mode or data_mode,
        data_root=data_root,
        local_root=local_root,
        nas_root=nas_root,
    )

    intensity_paths = collect_hcr_intensity_stack_paths(
        hcr_intensity_paths=hcr_intensity_paths,
        preproc_dir=preproc_dir,
    )
    if not intensity_paths:
        raise RuntimeError("No HCR intensity stacks found (rbest/rn, channel2/3 only).")

    candidate_pairs, duplicate_targets = deduplicate_hcr_intensity_targets(intensity_paths, out_conf_masks)
    pending, pending_logs = _pending_pairs(candidate_pairs, skip_if_exists=bool(cfg.skip_if_exists))

    log_lines = [f"[Cellpose] CP_MODEL_PATH={cp_model_path}"]
    if duplicate_targets > 0:
        log_lines.append(f"[Cellpose] deduplicated {duplicate_targets} duplicate input stack(s) mapping to the same output mask path")
    log_lines.append(f"[Cellpose] HCR intensity stacks: {len(candidate_pairs)}")
    log_lines.extend(pending_logs)

    rows: list[dict[str, Any]] = []
    if not pending:
        log_lines.append("[Cellpose] All mask outputs already exist; skipping Cellpose import/model load.")
        status = "cached"
    else:
        cp_model = Path(cp_model_path)
        if not cp_model.exists():
            raise FileNotFoundError(f"CP_MODEL_PATH does not exist: {cp_model}")
        model = _load_cellpose_model(cp_model=cp_model, use_gpu=bool(cfg.use_gpu), stage_tag="[24]", log_lines=log_lines)

        log_lines.append(f"[Cellpose] stacks requiring segmentation: {len(pending)}")
        for intensity_path, mask_path in pending:
            row = {
                "input_path": str(intensity_path),
                "mask_path": str(mask_path),
                "converted_path": None,
                "status": "pending",
                "anisotropy": None,
                "vox": None,
            }
            arr = imread_any(intensity_path)
            if intensity_path.suffix.lower() == ".nrrd" and arr.ndim == 3 and arr.shape[-1] < min(arr.shape[0], arr.shape[1]):
                arr = arr.transpose(2, 1, 0)
                log_lines.append(f"[INFO] Reordered NRRD to (Z, Y, X): {intensity_path.name} -> {arr.shape}")
            if arr.ndim != 3:
                row["status"] = f"skip_bad_shape:{tuple(arr.shape)}"
                rows.append(row)
                log_lines.append(f"[SKIP] expected 3D stack: {intensity_path} (shape={arr.shape})")
                continue
            arr_u8, norm_status = _normalize_to_uint8(arr)
            if norm_status == "native":
                log_lines.append(f"[INFO] Using native uint8 intensities for {intensity_path.name}")
            else:
                log_lines.append(f"[INFO] Normalized to uint8 for {intensity_path.name} (dtype={arr.dtype})")
            conv_path = out_conf_convert / f"{intensity_path.stem}_uint8.tif"
            tifffile.imwrite(conv_path, arr_u8)
            anisotropy, vox = _compute_anisotropy(
                intensity_path,
                use_anisotropy=bool(cfg.use_anisotropy),
                anisotropy_override=cfg.anisotropy_override,
            )
            row["converted_path"] = str(conv_path)
            row["anisotropy"] = anisotropy
            row["vox"] = vox
            aniso_msg = "disabled" if not cfg.use_anisotropy else anisotropy
            log_lines.append(f"[CP] {intensity_path.stem}: use_anisotropy={cfg.use_anisotropy} vox={vox} anisotropy={aniso_msg}")
            result = model.eval(arr_u8, channels=[0, 0], channel_axis=None, z_axis=0, do_3D=True, anisotropy=anisotropy)
            try:
                masks, _, _, _ = result
            except ValueError:
                masks, _, _ = result
            mask_arr = np.asarray(masks, dtype=np.uint16)
            if cfg.save_tif:
                tifffile.imwrite(mask_path, mask_arr)
            row["status"] = "saved"
            row["n_labels"] = int(len(np.unique(mask_arr)) - (1 if np.any(mask_arr == 0) else 0))
            rows.append(row)
            log_lines.append(f"[CP] {intensity_path.stem}: input={conv_path}, masks={mask_path}")
        status = "segmented"

    manifest_df = pd.DataFrame(rows)
    bindings = {
        "CP_MODEL_PATH": cp_model_path,
        "OUT_CONF_RAW": out_conf_raw,
        "OUT_CONF_MASKS": out_conf_masks,
        "OUT_CONF_CONVERT": out_conf_convert,
    }
    return {
        "status": status,
        "bindings": bindings,
        "candidate_pairs": candidate_pairs,
        "pending_pairs": pending,
        "manifest_df": manifest_df,
        "log_lines": log_lines,
    }


def run_anatomy_cellpose_stage(
    *,
    anat_seg_source_path: str | Path,
    analysis_dir: str | Path,
    anat_labels_path: str | Path | None = None,
    anat_cp_model_path: str | Path | None,
    vox_anat: dict[str, Any] | None = None,
    assert_fish_compatible: Any = None,
    fish_id: str | None = None,
    config: AnatomyCellposeConfig | None = None,
) -> dict[str, Any]:
    cfg = config or AnatomyCellposeConfig()
    log_lines: list[str] = []

    anat_src = Path(anat_seg_source_path)
    analysis_path = Path(analysis_dir)
    out_dir = analysis_path / "structural" / "cp_masks"
    convert_dir = analysis_path / "structural" / "raw" / "converted_nrrd_to_tif"
    out_dir.mkdir(parents=True, exist_ok=True)
    convert_dir.mkdir(parents=True, exist_ok=True)

    if callable(assert_fish_compatible):
        assert_fish_compatible(anat_src, key="ANAT_SEG_SOURCE_PATH", strict=True)
        assert_fish_compatible(out_dir, key="ANAT_SEG_OUT_DIR", strict=True)
        assert_fish_compatible(convert_dir, key="ANAT_SEG_CONVERT_DIR", strict=True)
    if not anat_src.exists():
        raise FileNotFoundError(f"Anatomy source not found: {anat_src}")
    if anat_cp_model_path in (None, "", False):
        raise RuntimeError("Set ANAT_CP_MODEL_PATH to the anatomy Cellpose model path before running [24a].")

    model_path = Path(anat_cp_model_path)
    if not model_path.exists():
        raise FileNotFoundError(f"ANAT_CP_MODEL_PATH does not exist: {model_path}")

    vol = _prepare_anat_volume(anat_src, log_lines=log_lines)
    expected_shape = tuple(int(v) for v in vol.shape)
    stem = anat_src.stem
    anat_8bit_path = convert_dir / f"{stem}_8bit.tif"
    anat_mask_out = out_dir / f"{stem}_8bit_cp_masks.tif"

    reuse_path = None
    existing_candidates: list[Path] = []
    if anat_labels_path not in (None, "", False):
        existing_candidates.append(Path(anat_labels_path))
    existing_candidates.append(anat_mask_out)
    fish_token = str(fish_id or "").strip()
    for pattern in (
        f"{fish_token}*anatomy*cp_masks*.tif",
        f"{fish_token}*anatomy*cp_masks*.tiff",
        f"{fish_token}*cp_masks*.tif",
        f"{fish_token}*cp_masks*.tiff",
        "*anatomy*cp_masks*.tif",
        "*anatomy*cp_masks*.tiff",
        "*cp_masks*.tif",
        "*cp_masks*.tiff",
    ):
        existing_candidates.extend(sorted(out_dir.glob(pattern)))

    if (not cfg.force_recompute) and cfg.skip_if_exists:
        seen: set[str] = set()
        for candidate in existing_candidates:
            key = str(candidate)
            if key in seen or not candidate.exists():
                continue
            seen.add(key)
            if callable(assert_fish_compatible) and not assert_fish_compatible(candidate, key="ANAT_LABELS_PATH candidate", strict=False):
                log_lines.append(f"[ANAT CP] Skipping stale-fish candidate: {candidate}")
                continue
            try:
                if _mask_shape(candidate) == expected_shape:
                    reuse_path = candidate
                    break
                log_lines.append(f"[ANAT CP] Existing mask shape mismatch ({candidate}: {_mask_shape(candidate)} vs expected {expected_shape}); recomputing.")
            except Exception as exc:
                log_lines.append(f"[ANAT CP] Could not validate existing mask {candidate}: {exc}; recomputing.")

    if reuse_path is not None:
        log_lines.append(f"[ANAT CP] Reusing existing anatomy masks: {reuse_path}")
        return {
            "status": "cached",
            "bindings": {
                "ANAT_SEG_SOURCE_PATH": anat_src,
                "ANAT_SEG_OUT_DIR": out_dir,
                "ANAT_SEG_CONVERT_DIR": convert_dir,
                "ANAT_CP_MODEL_PATH": str(model_path),
                "ANAT_LABELS_PATH": reuse_path,
            },
            "log_lines": log_lines,
        }

    anat_u8 = _to_uint8_preserve(vol, log_lines=log_lines)
    if cfg.save_8bit and (cfg.force_recompute or (not anat_8bit_path.exists())):
        tifffile.imwrite(str(anat_8bit_path), anat_u8, imagej=True, compression="deflate")
        log_lines.append(f"[ANAT CP] Saved 8-bit anatomy TIFF: {anat_8bit_path}")

    use_gpu = _resolve_cellpose_gpu_flag(use_gpu=cfg.use_gpu, compute_device=cfg.compute_device)
    log_lines.append(f"[ANAT CP] device={cfg.compute_device or 'auto'} gpu={use_gpu}")
    anat_model = _load_cellpose_model(cp_model=model_path, use_gpu=use_gpu, stage_tag="[24a]", log_lines=log_lines)

    anisotropy = None
    if cfg.use_anisotropy and vol.ndim == 3 and isinstance(vox_anat, dict):
        try:
            z_um = float(vox_anat.get("Z"))
            x_um = float(vox_anat.get("X"))
            if z_um > 0 and x_um > 0:
                anisotropy = z_um / x_um
        except Exception:
            anisotropy = None

    eval_kwargs = dict(channels=[0, 0], channel_axis=None)
    if vol.ndim == 3:
        eval_kwargs.update(dict(z_axis=0, do_3D=True, anisotropy=anisotropy))
    else:
        eval_kwargs.update(dict(do_3D=False))

    result = anat_model.eval(anat_u8, **eval_kwargs)
    anat_masks = result[0] if isinstance(result, (tuple, list)) else result
    anat_masks = np.asarray(anat_masks)
    if anat_masks.ndim == 4 and anat_masks.shape[-1] in (3, 4):
        anat_masks = anat_masks[..., 0]
    if anat_masks.size == 0:
        raise RuntimeError("Cellpose returned empty anatomy mask output.")

    max_lbl = int(np.max(anat_masks)) if anat_masks.size else 0
    out_dtype = np.uint16 if max_lbl <= np.iinfo(np.uint16).max else np.uint32
    tifffile.imwrite(str(anat_mask_out), anat_masks.astype(out_dtype), imagej=True, compression="deflate")
    log_lines.append(f"[ANAT CP] Saved anatomy masks: {anat_mask_out} (max_label={max_lbl}, dtype={out_dtype})")

    return {
        "status": "segmented",
        "bindings": {
            "ANAT_SEG_SOURCE_PATH": anat_src,
            "ANAT_SEG_OUT_DIR": out_dir,
            "ANAT_SEG_CONVERT_DIR": convert_dir,
            "ANAT_CP_MODEL_PATH": str(model_path),
            "ANAT_LABELS_PATH": anat_mask_out,
        },
        "log_lines": log_lines,
    }


def _rescale_labels_to_ref(labels: np.ndarray | None, ref_shape: tuple[int, ...] | None) -> np.ndarray | None:
    if labels is None or ref_shape is None:
        return labels
    if tuple(labels.shape) == tuple(ref_shape):
        return labels
    from skimage.transform import resize

    return resize(labels.astype(np.float32), ref_shape, order=0, preserve_range=True, anti_aliasing=False).astype(labels.dtype)


def _maybe_orient_labels(arr: np.ndarray, *, apply_orient: bool, apply_func_orientation_func: Callable[..., np.ndarray] | None) -> np.ndarray:
    if not apply_orient or not callable(apply_func_orientation_func):
        return arr
    try:
        return apply_func_orientation_func(arr)
    except Exception:
        return arr


def _resolve_in_memory_labels(
    arr: Any,
    *,
    ref_shape: tuple[int, ...] | None,
    ensure_uint_labels_func: Callable[[Any], np.ndarray],
) -> np.ndarray:
    labels = ensure_uint_labels_func(arr)
    return _rescale_labels_to_ref(labels, ref_shape)


def resolve_functional_labels_for_plane(
    plane_ref: dict[str, Any],
    plane_idx: int,
    *,
    use_suite2p_labels: bool,
    func_labels: Any = None,
    out_seg: str | Path | None = None,
    func_labels_path: str | Path | None = None,
    apply_func_orientation_func: Callable[..., np.ndarray] | None = None,
    imread_func: Callable[[str | Path], np.ndarray] | None = None,
    ensure_uint_labels_func: Callable[[Any], np.ndarray] | None = None,
) -> tuple[np.ndarray | None, str | None]:
    ensure = ensure_uint_labels_func if callable(ensure_uint_labels_func) else _ensure_uint_labels
    imread_local = imread_func if callable(imread_func) else tifffile.imread
    label = plane_ref.get("label", f"plane{plane_idx}")
    ref_match = plane_ref.get("ref_match")
    ref_shape = tuple(ref_match.shape) if ref_match is not None else None

    if use_suite2p_labels:
        suite2p = plane_ref.get("suite2p")
        if isinstance(suite2p, dict) and suite2p.get("labels") is not None:
            arr = _resolve_in_memory_labels(suite2p["labels"], ref_shape=ref_shape, ensure_uint_labels_func=ensure)
            return arr, "Suite2p labels (plane_refs)"
        if isinstance(func_labels, list) and plane_idx < len(func_labels) and func_labels[plane_idx] is not None:
            arr = _resolve_in_memory_labels(func_labels[plane_idx], ref_shape=ref_shape, ensure_uint_labels_func=ensure)
            return arr, f"Suite2p labels list[{plane_idx}]"
        if func_labels is not None:
            arr = ensure(np.asarray(func_labels))
            if arr.ndim == 3:
                src_idx = int(plane_ref.get("index", plane_idx))
                if src_idx < arr.shape[0]:
                    arr = _resolve_in_memory_labels(arr[src_idx], ref_shape=ref_shape, ensure_uint_labels_func=ensure)
                    return arr, f"Suite2p labels stack[{src_idx}]"
            elif arr.ndim == 2:
                arr = _resolve_in_memory_labels(arr, ref_shape=ref_shape, ensure_uint_labels_func=ensure)
                return arr, "Suite2p labels (2D)"

    if out_seg is not None:
        cp_path = Path(out_seg) / f"{label}_cellpose_masks.tif"
        if cp_path.exists():
            arr = ensure(imread_local(cp_path))
            if arr.ndim == 3 and arr.shape[-1] in (3, 4):
                arr = arr[..., 0]
            if arr.ndim == 3 and arr.shape[0] == 1:
                arr = arr[0]
            arr = _maybe_orient_labels(arr, apply_orient=True, apply_func_orientation_func=apply_func_orientation_func)
            arr = _rescale_labels_to_ref(arr, ref_shape)
            return arr, f"Cellpose masks: {cp_path}"

    if func_labels is not None:
        if isinstance(func_labels, list) and plane_idx < len(func_labels):
            arr = func_labels[plane_idx]
            if arr is not None:
                arr = _resolve_in_memory_labels(arr, ref_shape=ref_shape, ensure_uint_labels_func=ensure)
                return arr, f"func_labels list[{plane_idx}]"
        else:
            arr = ensure(np.asarray(func_labels))
            if arr.ndim == 3:
                src_idx = int(plane_ref.get("index", plane_idx))
                if src_idx < arr.shape[0]:
                    arr = _resolve_in_memory_labels(arr[src_idx], ref_shape=ref_shape, ensure_uint_labels_func=ensure)
                    return arr, f"func_labels stack[{src_idx}]"
            elif arr.ndim == 2:
                arr = _resolve_in_memory_labels(arr, ref_shape=ref_shape, ensure_uint_labels_func=ensure)
                return arr, "func_labels (2D)"

    if func_labels_path not in (None, "", False) and os.path.exists(str(func_labels_path)):
        arr = ensure(imread_local(func_labels_path))
        if arr.ndim == 3:
            src_idx = int(plane_ref.get("index", plane_idx))
            if src_idx < arr.shape[0]:
                arr = _maybe_orient_labels(arr[src_idx], apply_orient=True, apply_func_orientation_func=apply_func_orientation_func)
                arr = _rescale_labels_to_ref(arr, ref_shape)
                return arr, f"FUNC_LABELS_PATH[{src_idx}]: {Path(func_labels_path).name}"
        elif arr.ndim == 2:
            arr = _maybe_orient_labels(arr, apply_orient=True, apply_func_orientation_func=apply_func_orientation_func)
            arr = _rescale_labels_to_ref(arr, ref_shape)
            return arr, f"FUNC_LABELS_PATH: {Path(func_labels_path).name}"

    return None, None


def _native_suite2p_ref_paths(label: str, *, out_raw: str | Path, outdir: str | Path) -> tuple[Path, Path]:
    out_raw_path = Path(out_raw)
    outdir_path = Path(outdir)
    norm_cands = [out_raw_path / f"{label}_ref_norm.tif", outdir_path / f"{label}_ref_norm.tif"]
    raw_cands = [out_raw_path / f"{label}_ref_raw.tif", outdir_path / f"{label}_ref_raw.tif"]
    norm_path = next((path for path in norm_cands if path.exists()), norm_cands[0])
    raw_path = next((path for path in raw_cands if path.exists()), raw_cands[0])
    return norm_path, raw_path


def resolve_native_suite2p_labels_for_plane(
    plane_ref: dict[str, Any],
    plane_idx: int,
    *,
    func_labels: Any = None,
    ensure_uint_labels_func: Callable[[Any], np.ndarray] | None = None,
) -> tuple[np.ndarray | None, str | None]:
    ensure = ensure_uint_labels_func if callable(ensure_uint_labels_func) else _ensure_uint_labels
    suite2p = plane_ref.get("suite2p")
    if isinstance(suite2p, dict) and suite2p.get("labels") is not None:
        return ensure(suite2p["labels"]), "plane_refs.suite2p.labels"
    if isinstance(func_labels, list) and plane_idx < len(func_labels) and func_labels[plane_idx] is not None:
        return ensure(func_labels[plane_idx]), f"func_labels[{plane_idx}]"
    return None, None


def export_suite2p_native_labels_stage(
    *,
    plane_refs: list[dict[str, Any]] | None,
    suite2p_by_ref_idx: dict[int, dict[str, Any]] | None,
    out_qa: str | Path,
    out_raw: str | Path,
    outdir: str | Path,
    func_labels: Any = None,
) -> dict[str, Any]:
    export_dir = Path(out_qa) / "suite2p_native_labels"
    export_dir.mkdir(parents=True, exist_ok=True)
    rows: list[dict[str, Any]] = []
    log_lines: list[str] = []

    if not plane_refs:
        log_lines.append("[26a] plane_refs missing; run [12] first.")
        return {"status": "missing_plane_refs", "export_dir": export_dir, "manifest_df": pd.DataFrame(rows), "log_lines": log_lines}
    if not suite2p_by_ref_idx:
        log_lines.append("[26a] suite2p_by_ref_idx missing; run [23a] first.")
        return {"status": "missing_suite2p", "export_dir": export_dir, "manifest_df": pd.DataFrame(rows), "log_lines": log_lines}

    saved = 0
    for plane_idx, plane_ref in enumerate(plane_refs):
        label = plane_ref.get("label", f"plane{plane_idx}")
        labels, src_desc = resolve_native_suite2p_labels_for_plane(plane_ref, plane_idx, func_labels=func_labels)
        ref_norm_mem = plane_ref.get("ref2d")
        ref_norm_path_existing, ref_raw_path_existing = _native_suite2p_ref_paths(label, out_raw=out_raw, outdir=outdir)
        out_labels = export_dir / f"{label}_suite2p_labels_native.tif"
        out_ref_norm = export_dir / f"{label}_ref_norm.tif"
        if labels is None:
            rows.append(
                {
                    "plane": int(plane_idx),
                    "label": label,
                    "status": "missing_labels",
                    "label_src": src_desc,
                    "labels_path": str(out_labels),
                    "ref_norm_export_path": str(out_ref_norm),
                    "ref_norm_source_path": str(ref_norm_path_existing),
                    "ref_norm_exists": bool(ref_norm_path_existing.exists()),
                    "ref_raw_source_path": str(ref_raw_path_existing),
                    "ref_raw_exists": bool(ref_raw_path_existing.exists()),
                }
            )
            log_lines.append(f"[26a] missing labels for {label}; skipping")
            continue
        labels = _ensure_uint_labels(labels)
        if labels.ndim == 3 and labels.shape[-1] in (3, 4):
            labels = labels[..., 0]
        if labels.ndim != 2:
            rows.append(
                {
                    "plane": int(plane_idx),
                    "label": label,
                    "status": f"unexpected_label_shape:{tuple(labels.shape)}",
                    "label_src": src_desc,
                    "labels_path": str(out_labels),
                    "labels_shape": tuple(labels.shape),
                    "labels_dtype": str(labels.dtype),
                    "ref_norm_export_path": str(out_ref_norm),
                    "ref_norm_source_path": str(ref_norm_path_existing),
                    "ref_norm_exists": bool(ref_norm_path_existing.exists()),
                    "ref_raw_source_path": str(ref_raw_path_existing),
                    "ref_raw_exists": bool(ref_raw_path_existing.exists()),
                }
            )
            log_lines.append(f"[26a] unexpected label shape for {label}: {labels.shape}")
            continue
        labels_out = labels.astype(np.uint16, copy=False) if int(np.max(labels)) <= np.iinfo(np.uint16).max else labels.astype(np.uint32, copy=False)
        tifffile.imwrite(out_labels, labels_out)
        ref_norm_status = "missing_ref_norm"
        ref_norm_shape = None
        if ref_norm_mem is not None:
            ref_norm_arr = norm01(np.asarray(ref_norm_mem, dtype=np.float32))
            tifffile.imwrite(out_ref_norm, (ref_norm_arr * 65535).astype(np.uint16))
            ref_norm_status = "exported_from_plane_refs"
            ref_norm_shape = tuple(ref_norm_arr.shape)
        elif ref_norm_path_existing.exists():
            ref_norm_status = "source_exists_only"
            try:
                ref_norm_shape = tuple(np.asarray(tifffile.imread(ref_norm_path_existing)).shape)
            except Exception:
                ref_norm_shape = None
        unique_labels = np.unique(labels_out)
        rows.append(
            {
                "plane": int(plane_idx),
                "label": label,
                "status": "saved",
                "label_src": src_desc,
                "labels_path": str(out_labels),
                "labels_shape": tuple(labels_out.shape),
                "labels_dtype": str(labels_out.dtype),
                "n_nonzero_labels": int(np.count_nonzero(unique_labels)),
                "max_label_value": int(labels_out.max()) if labels_out.size else 0,
                "ref_norm_export_path": str(out_ref_norm),
                "ref_norm_export_status": ref_norm_status,
                "ref_norm_shape": ref_norm_shape,
                "ref_norm_source_path": str(ref_norm_path_existing),
                "ref_norm_exists": bool(ref_norm_path_existing.exists()),
                "ref_raw_source_path": str(ref_raw_path_existing),
                "ref_raw_exists": bool(ref_raw_path_existing.exists()),
            }
        )
        saved += 1
        log_lines.append(f"[26a] saved {label}: labels -> {out_labels.name}; ref_norm -> {out_ref_norm.name}")

    manifest_csv = export_dir / "suite2p_native_label_export_manifest.csv"
    manifest_df = pd.DataFrame(rows)
    manifest_df.to_csv(manifest_csv, index=False)
    log_lines.append(f"[26a] saved {saved} plane label TIFF(s) to {export_dir}")
    log_lines.append(f"[26a] manifest -> {manifest_csv}")
    return {
        "status": "exported",
        "export_dir": export_dir,
        "manifest_csv": manifest_csv,
        "manifest_df": manifest_df,
        "log_lines": log_lines,
    }


__all__ = [
    "AnatomyCellposeConfig",
    "HcrCellposeConfig",
    "collect_hcr_intensity_stack_paths",
    "deduplicate_hcr_intensity_targets",
    "export_suite2p_native_labels_stage",
    "resolve_functional_labels_for_plane",
    "resolve_hcr_cellpose_model_path",
    "resolve_native_suite2p_labels_for_plane",
    "run_anatomy_cellpose_stage",
    "run_hcr_cellpose_stage",
]
