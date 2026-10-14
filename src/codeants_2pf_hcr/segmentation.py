"""Segmentation and functional-label QA helpers for notebook cells [24], [26], and [26a]."""

from __future__ import annotations
from codeants_2pf_hcr.paths import visible_glob, is_visible_path

from dataclasses import dataclass
import hashlib
from importlib import metadata
import os
from pathlib import Path
import platform
import sys
from typing import Any, Callable

import numpy as np
import pandas as pd
import tifffile

from .runtime import optional_dependency_error


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
    # Cellpose input preprocessing; None segments the raw stack. Default adopted
    # 2026-10-14 after the blinded ex vivo review (wikiLogbook dataset 06).
    boundary_enhancement: str | None = "hessian2d"
    hessian_sigma_px: float = 1.5
    hessian_beta: float = 0.5


HESSIAN_VALLEY_METHOD = "hessian2d"


def hessian_valley_tag(sigma_px: float, beta: float) -> str:
    """Filename tag, e.g. ``hessian2d_s1p5_b0p50``."""
    return f"{HESSIAN_VALLEY_METHOD}_s{float(sigma_px):.1f}_b{float(beta):.2f}".replace(".", "p")


def hessian_valley_subtract(volume: np.ndarray, *, sigma_px: float = 1.5, beta: float = 0.5) -> tuple[np.ndarray, float]:
    """Darken the gaps between touching nuclei before Cellpose.

    Per plane, the largest Hessian eigenvalue (positive in dark valleys) is
    clipped at 0, scaled by sigma**2, matched to the stack's intensity at the
    99th percentile, and ``beta`` times it is subtracted. Returns the uint8
    volume (same axis order; 2D or ZYX) and the matching scale.
    """
    from skimage import feature

    raw = np.asarray(volume, dtype=np.float32)
    if raw.ndim not in (2, 3):
        raise ValueError(f"Hessian valley subtraction expects a 2D or ZYX volume, got {raw.shape}")
    planes = raw[None] if raw.ndim == 2 else raw
    valley = np.empty(planes.shape, np.float32)
    for z, plane in enumerate(planes):
        hessian = feature.hessian_matrix(plane, sigma=sigma_px, order="rc", use_gaussian_derivatives=False)
        valley[z] = np.clip(feature.hessian_matrix_eigvals(hessian)[0], 0, None) * sigma_px**2
    # Keep the recipe's NumPy scalar arithmetic and type promotion unchanged (do not
    # convert scale to a Python float first): the output must stay identical to the
    # validated 2026-10-07 inputs, which it reproduces voxel for voxel.
    scale = np.percentile(planes, 99) / max(np.percentile(valley, 99), 1e-6)
    enhanced = np.clip(np.rint(planes - beta * scale * valley), 0, 255).astype(np.uint8)
    return (enhanced[0] if raw.ndim == 2 else enhanced), float(scale)


def _nrrd_array_needs_reorder(arr: np.ndarray) -> bool:
    """Legacy heuristic in _prepare_anat_volume: reorder when the last axis is the shortest."""
    return arr.ndim == 3 and arr.shape[-1] < min(arr.shape[0], arr.shape[1])


def _same_nrrd_header(a: dict[str, Any], b: dict[str, Any]) -> bool:
    def same(x: Any, y: Any) -> bool:
        try:
            return np.array_equal(np.asarray(x, dtype=float), np.asarray(y, dtype=float), equal_nan=True)
        except (TypeError, ValueError):
            return np.array_equal(np.asarray(x, dtype=object), np.asarray(y, dtype=object))

    keys = (set(a) | set(b)) - {"encoding"}
    return all(same(a.get(k), b.get(k)) for k in keys)


def _volume_content_sha256(arr: np.ndarray) -> str:
    """Hash of array content, dtype and shape (independent of file encoding)."""
    arr = np.ascontiguousarray(arr)
    digest = hashlib.sha256(f"{arr.dtype.str}|{arr.shape}|".encode())
    digest.update(arr.tobytes())
    return digest.hexdigest()


def anatomy_mask_binding_path(mask_path: str | Path) -> Path:
    mask_path = Path(mask_path)
    return mask_path.with_name(f"{mask_path.stem}.binding.json")


def write_anatomy_mask_binding(
    mask_path: str | Path,
    cellpose_input: np.ndarray,
    *,
    preprocessing: dict[str, Any],
    source_path: str | Path,
    note: str | None = None,
) -> Path:
    """Record which Cellpose input (content hash + preprocessing) produced a mask.

    Written by the anatomy Cellpose stage after segmenting, or after a verified
    import of masks segmented elsewhere from the same input.
    """
    import json
    from datetime import datetime, timezone

    mask_path = Path(mask_path)
    payload = {
        "schema_version": 1,
        "mask": mask_path.name,
        "mask_sha256": _file_sha256(mask_path),
        "cellpose_input_content_sha256": _volume_content_sha256(cellpose_input),
        "cellpose_input_shape": [int(v) for v in np.shape(cellpose_input)],
        "preprocessing": dict(preprocessing),
        "source_path": str(source_path),
        "written_at": datetime.now(timezone.utc).isoformat(),
        "note": note,
    }
    out = anatomy_mask_binding_path(mask_path)
    out.write_text(json.dumps(payload, indent=1))
    return out


def _read_mask_binding(mask_path: Path) -> dict[str, Any] | None:
    import json

    path = anatomy_mask_binding_path(mask_path)
    if not path.is_file():
        return None
    try:
        return json.loads(path.read_text())
    except Exception:
        return {"unreadable": True}


def _mask_binding_mismatch(mask_path: Path, input_sha: str, preprocessing: dict[str, Any]) -> str | None:
    """None when the mask is bound to this input content and preprocessing; else why not."""
    binding = _read_mask_binding(mask_path)
    if binding is None:
        return "no binding record"
    if binding.get("unreadable"):
        return "unreadable binding record"
    if binding.get("preprocessing") != preprocessing:
        return f"made with preprocessing {binding.get('preprocessing')}"
    if binding.get("cellpose_input_content_sha256") != input_sha:
        return "made from a different Cellpose input"
    if binding.get("mask_sha256") != _file_sha256(mask_path):
        return "mask changed since it was bound"
    return None


def _mask_preprocessing_method(mask_path: Path) -> str:
    binding = _read_mask_binding(mask_path)
    if isinstance(binding, dict) and isinstance(binding.get("preprocessing"), dict):
        return str(binding["preprocessing"].get("method", "unknown"))
    return HESSIAN_VALLEY_METHOD if f"_{HESSIAN_VALLEY_METHOD}_" in mask_path.name else "none"


def _write_cellpose_input(source: Path, volume_zyx: np.ndarray, out_path: Path, *, log_lines: list[str]) -> None:
    """Write the Cellpose input with the source's header and axis layout (NRRD) or as TIFF.

    TIFF inputs are written as plain ImageJ TIFFs; source resolution tags are not copied.
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if source.suffix.lower() == ".nrrd":
        import nrrd

        # Inverse of the reader (imread_any = C/ZYX order) plus any reorder
        # _prepare_anat_volume applied to this source; no inference from shapes.
        source_arr, header = nrrd.read(str(source), index_order="C")
        on_disk = volume_zyx.transpose(2, 1, 0) if _nrrd_array_needs_reorder(source_arr) else volume_zyx
        if out_path.exists():
            existing, existing_header = nrrd.read(str(out_path), index_order="C")
            if np.array_equal(existing, on_disk) and _same_nrrd_header(existing_header, header):
                return
            log_lines.append(f"[ANAT CP] Rewriting Cellpose input (content or header differed): {out_path}")
        nrrd.write(str(out_path), on_disk, header, index_order="C")
    else:
        if out_path.exists() and np.array_equal(np.asarray(tifffile.imread(str(out_path))), volume_zyx):
            return
        tifffile.imwrite(str(out_path), volume_zyx, imagej=True, compression="deflate")


def _publish_segmentation(
    *,
    output_base: Path,
    staged: dict[Path, Path],
    replaced: list[Path],
    log_lines: list[str],
) -> Path | None:
    """Promote staged files to their canonical paths, archiving what they replace.

    ``staged`` maps a staged file to its canonical destination; ``replaced`` lists
    canonical masks to retire. Old masks/bindings and overwritten files move to a
    unique ``superseded_masks/<UTC>/`` folder. On any failure every move is undone,
    so the previous canonical bundle is restored.
    """
    import os
    import shutil
    import tempfile
    from datetime import datetime, timezone

    to_archive: list[Path] = []
    for mask in replaced:
        to_archive += [path for path in (mask, anatomy_mask_binding_path(mask)) if path.exists()]
    to_archive += [dest for dest in staged.values() if dest.exists() and dest not in to_archive]
    archive = None
    if to_archive:
        root = output_base / "superseded_masks"
        root.mkdir(parents=True, exist_ok=True)
        archive = Path(tempfile.mkdtemp(prefix=datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ_"), dir=root))
    moves: list[tuple[Path, Path]] = []
    try:
        for path in to_archive:
            target = archive / path.name
            if target.exists():
                raise FileExistsError(f"Superseded copy already exists: {target}")
            shutil.move(str(path), target)
            moves.append((path, target))
        for src, dest in staged.items():
            dest.parent.mkdir(parents=True, exist_ok=True)
            os.replace(src, dest)
            moves.append((src, dest))
    except Exception:
        for original, moved in reversed(moves):
            if moved.exists():
                shutil.move(str(moved), original)
        raise
    for path in to_archive:
        log_lines.append(f"[ANAT CP] Superseded {path.name} -> {archive}")
    return archive


def _ensure_uint_labels_local() -> Callable[[Any], np.ndarray]:
    from .matching import _ensure_uint_labels

    return _ensure_uint_labels


def _imread_any(path: str | Path) -> np.ndarray:
    from .spatial import imread_any

    return imread_any(path)


def _infer_voxels_tiff(path: str | Path) -> dict[str, float]:
    from .spatial import infer_voxels_tiff

    return infer_voxels_tiff(path)


def _norm01(arr: np.ndarray) -> np.ndarray:
    from .spatial import norm01

    return norm01(arr)


def _installed_version(distribution: str) -> str | None:
    try:
        return metadata.version(distribution)
    except metadata.PackageNotFoundError:
        return None


def _file_sha256(path: Path) -> str | None:
    if not path.is_file():
        return None
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _cellpose_runtime_provenance(*, model_path: Path, optional_runtime_loaded: bool, use_gpu: bool | None) -> dict[str, Any]:
    provenance: dict[str, Any] = {
        "python_version": platform.python_version(),
        "python_executable": sys.executable,
        "platform": platform.platform(),
        "cellpose_version": _installed_version("cellpose"),
        "torch_version": _installed_version("torch"),
        "model_sha256": _file_sha256(model_path),
        "requested_gpu": use_gpu,
        "cuda_available": None,
        "cuda_version": None,
        "compute_device": None,
    }
    if not optional_runtime_loaded:
        return provenance
    try:
        import torch

        cuda_available = bool(torch.cuda.is_available())
        provenance["cuda_available"] = cuda_available
        provenance["cuda_version"] = getattr(getattr(torch, "version", None), "cuda", None)
        if cuda_available:
            provenance["compute_device"] = str(torch.cuda.get_device_name(0))
        else:
            mps_backend = getattr(getattr(torch, "backends", None), "mps", None)
            if mps_backend is not None and bool(mps_backend.is_available()):
                provenance["compute_device"] = "mps"
            else:
                provenance["compute_device"] = "cpu"
    except Exception:
        provenance["compute_device"] = "unknown"
    return provenance


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
    source: str = "all",
) -> list[Path]:
    if hcr_intensity_paths:
        intensity_paths = [Path(path) for path in hcr_intensity_paths]
    else:
        intensity_paths = []
        if preproc_dir is not None:
            base = Path(preproc_dir)
            source_key = str(source).strip().lower()
            if source_key in {"", "all", "both"}:
                directories = (base / "rbest", base / "rn")
            elif source_key in {"rbest", "rn"}:
                directories = (base / source_key,)
            else:
                raise ValueError("source must be one of: all, rbest, rn")
            for directory in directories:
                if directory.exists():
                    intensity_paths.extend(sorted(visible_glob(directory, "*_rbest_channel*.nrrd")))
                    intensity_paths.extend(sorted(visible_glob(directory, "*_rbest_channel*.tif")))
                    intensity_paths.extend(sorted(visible_glob(directory, "*_rbest_channel*.tiff")))
                    intensity_paths.extend(sorted(visible_glob(directory, "*_r[0-9]*_channel*.nrrd")))
                    intensity_paths.extend(sorted(visible_glob(directory, "*_r[0-9]*_channel*.tif")))
                    intensity_paths.extend(sorted(visible_glob(directory, "*_r[0-9]*_channel*.tiff")))
                    intensity_paths.extend(sorted(visible_glob(directory, "*round*_channel*.nrrd")))
                    intensity_paths.extend(sorted(visible_glob(directory, "*round*_channel*.tif")))
                    intensity_paths.extend(sorted(visible_glob(directory, "*round*_channel*.tiff")))
    ignored_names = {".DS_Store", "Thumbs.db", "desktop.ini"}
    intensity_paths = [
        path
        for path in intensity_paths
        if is_visible_path(path) and not any(part in ignored_names for part in path.parts)
    ]
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
    return (_norm01(arr) * 255).astype(np.uint8), f"normalized_from_{arr.dtype}"


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
        vox = _infer_voxels_tiff(intensity_path)
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
    vol = np.asarray(_imread_any(path))
    if vol.ndim == 4 and vol.shape[-1] in (3, 4):
        vol = vol[..., 0]
    if path.suffix.lower() == ".nrrd" and _nrrd_array_needs_reorder(vol):
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
    source: str = "all",
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
        source=source,
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
        runtime_provenance = _cellpose_runtime_provenance(
            model_path=Path(cp_model_path),
            optional_runtime_loaded=False,
            use_gpu=cfg.use_gpu,
        )
    else:
        cp_model = Path(cp_model_path)
        if not cp_model.exists():
            raise FileNotFoundError(f"CP_MODEL_PATH does not exist: {cp_model}")
        model = _load_cellpose_model(cp_model=cp_model, use_gpu=bool(cfg.use_gpu), stage_tag="[24]", log_lines=log_lines)
        runtime_provenance = _cellpose_runtime_provenance(
            model_path=cp_model,
            optional_runtime_loaded=True,
            use_gpu=cfg.use_gpu,
        )

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
            arr = _imread_any(intensity_path)
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
        "source": source,
        "candidate_pairs": candidate_pairs,
        "pending_pairs": pending,
        "manifest_df": manifest_df,
        "log_lines": log_lines,
        "runtime_provenance": runtime_provenance,
    }


def run_anatomy_cellpose_stage(
    *,
    anat_seg_source_path: str | Path,
    analysis_dir: str | Path,
    output_root: str | Path | None = None,
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
    output_base = Path(output_root) if output_root is not None else analysis_path / "structural"
    out_dir = output_base / "cp_masks"
    convert_dir = output_base / "raw" / "converted_nrrd_to_tif"
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
    enhancement: dict[str, Any] | None = None
    preprocessing: dict[str, Any] = {"method": "none"}
    cellpose_input_path = anat_src
    if cfg.boundary_enhancement not in (None, "", "none"):
        if cfg.boundary_enhancement != HESSIAN_VALLEY_METHOD:
            raise ValueError(f"Unknown anatomy boundary enhancement: {cfg.boundary_enhancement!r}")
        tag = hessian_valley_tag(cfg.hessian_sigma_px, cfg.hessian_beta)
        vol, scale = hessian_valley_subtract(vol, sigma_px=cfg.hessian_sigma_px, beta=cfg.hessian_beta)
        preprocessing = {"method": HESSIAN_VALLEY_METHOD, "sigma_px": float(cfg.hessian_sigma_px), "beta": float(cfg.hessian_beta)}
        stem = f"{stem}_{tag}"
        suffix = ".nrrd" if anat_src.suffix.lower() == ".nrrd" else ".tif"
        cellpose_input_path = output_base / "cellpose_input" / f"{stem}{suffix}"
        enhancement = {**preprocessing, "tag": tag, "scale": scale, "cellpose_input_path": str(cellpose_input_path)}
        log_lines.append(f"[ANAT CP] Hessian valley subtraction ({tag}, scale={scale:.4f})")
    anat_u8 = _to_uint8_preserve(vol, log_lines=log_lines)
    input_sha = _volume_content_sha256(anat_u8)
    anat_8bit_path = convert_dir / f"{stem}_8bit.tif"
    anat_mask_out = out_dir / f"{stem}_8bit_cp_masks.tif"

    # Masks made with different preprocessing are "foreign": they are never reused
    # and never replaced without force_recompute (in vivo labels feed accepted geometry).
    existing_masks = [p for p in visible_glob(out_dir, "*cp_masks*.tif*") if p.name != anat_mask_out.name]
    explicit = Path(anat_labels_path) if anat_labels_path not in (None, "", False) else None
    # Compare file identity, not names: an external mask that shares the canonical
    # basename is still a different label set and must be protected.
    if (
        explicit is not None
        and explicit.exists()
        and explicit.resolve() != anat_mask_out.resolve()
        and all(explicit.resolve() != p.resolve() for p in existing_masks)
    ):
        existing_masks.append(explicit)
    if enhancement is not None:
        foreign = existing_masks
    else:
        foreign = [p for p in existing_masks if _mask_preprocessing_method(p) != "none"]
        if anat_mask_out.exists() and _mask_preprocessing_method(anat_mask_out) != "none":
            foreign.append(anat_mask_out)
    if foreign and not cfg.force_recompute:
        raise RuntimeError(
            f"Masks not made with preprocessing {preprocessing['method']!r} exist ({', '.join(p.name for p in foreign)}). "
            "Segmenting now would replace labels downstream steps may depend on: rerun with force_recompute "
            "to resegment (masks inside this output directory move to superseded_masks/; an explicit labels "
            "path elsewhere is left in place, so pass the returned label path afterwards), or choose the "
            "matching boundary_enhancement."
        )

    reuse_path = None
    existing_candidates: list[Path] = []
    if enhancement is not None:
        # Reuse only the exact mask, and only when its binding record proves it was
        # made from this input content with these parameters.
        if anat_mask_out.exists() and not cfg.force_recompute:
            why = _mask_binding_mismatch(anat_mask_out, input_sha, preprocessing)
            if why is not None:
                raise RuntimeError(
                    f"{anat_mask_out.name} is not bound to the current Cellpose input ({why}); "
                    "rerun with force_recompute to resegment."
                )
            if not cfg.skip_if_exists:
                raise RuntimeError(f"skip_if_exists=False would overwrite {anat_mask_out.name}; use force_recompute.")
            existing_candidates.append(anat_mask_out)
    else:
        if explicit is not None:
            existing_candidates.append(explicit)
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
            existing_candidates.extend(sorted(visible_glob(out_dir, pattern)))
        # Legacy raw masks have no binding record; a binding that names other
        # preprocessing or another input disqualifies a candidate.
        existing_candidates = [
            p for p in existing_candidates
            if _mask_preprocessing_method(p) == "none"
            and (_read_mask_binding(p) is None or _mask_binding_mismatch(p, input_sha, preprocessing) is None)
        ]

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
    if reuse_path is None and not cfg.force_recompute:
        blocking = [p for p in existing_masks if p.exists()] + ([anat_mask_out] if anat_mask_out.exists() else [])
        if blocking:
            raise RuntimeError(
                f"Existing masks cannot be reused ({', '.join(p.name for p in blocking)}: stale binding, shape/fish "
                "check failed, or skip_if_exists=False). Replacing them needs force_recompute."
            )

    if reuse_path is not None:
        if enhancement is not None:
            _write_cellpose_input(anat_src, vol, cellpose_input_path, log_lines=log_lines)
            enhancement["cellpose_input_sha256"] = _file_sha256(cellpose_input_path)
        log_lines.append(f"[ANAT CP] Reusing existing anatomy masks: {reuse_path}")
        return {
            "status": "cached",
            "bindings": {
                "ANAT_SEG_SOURCE_PATH": anat_src,
                "ANAT_SEG_OUT_DIR": out_dir,
                "ANAT_SEG_CONVERT_DIR": convert_dir,
                "ANAT_CP_MODEL_PATH": str(model_path),
                "ANAT_LABELS_PATH": reuse_path,
                "ANAT_CELLPOSE_INPUT_PATH": cellpose_input_path,
            },
            "boundary_enhancement": enhancement,
            "log_lines": log_lines,
            "runtime_provenance": _cellpose_runtime_provenance(
                model_path=model_path,
                optional_runtime_loaded=False,
                use_gpu=cfg.use_gpu,
            ),
        }

    if cfg.save_8bit and (cfg.force_recompute or (not anat_8bit_path.exists())):
        tifffile.imwrite(str(anat_8bit_path), anat_u8, imagej=True, compression="deflate")
        log_lines.append(f"[ANAT CP] Saved 8-bit anatomy TIFF: {anat_8bit_path}")

    use_gpu = _resolve_cellpose_gpu_flag(use_gpu=cfg.use_gpu, compute_device=cfg.compute_device)
    log_lines.append(f"[ANAT CP] device={cfg.compute_device or 'auto'} gpu={use_gpu}")
    anat_model = _load_cellpose_model(cp_model=model_path, use_gpu=use_gpu, stage_tag="[24a]", log_lines=log_lines)
    runtime_provenance = _cellpose_runtime_provenance(
        model_path=model_path,
        optional_runtime_loaded=True,
        use_gpu=use_gpu,
    )

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
    # Stage the input, mask and binding, verify them, then promote them together;
    # replaced masks inside out_dir are archived, and a failure restores the old bundle.
    # An explicit anat_labels_path outside out_dir is never moved.
    import tempfile

    staging = Path(tempfile.mkdtemp(prefix=".staging_", dir=output_base))
    try:
        staged_mask = staging / anat_mask_out.name
        tifffile.imwrite(str(staged_mask), anat_masks.astype(out_dtype), imagej=True, compression="deflate")
        if _mask_shape(staged_mask) != expected_shape:
            raise RuntimeError(f"Staged mask shape {_mask_shape(staged_mask)} != input {expected_shape}")
        staged_binding = write_anatomy_mask_binding(staged_mask, anat_u8, preprocessing=preprocessing, source_path=anat_src)
        staged = {staged_mask: anat_mask_out, staged_binding: anatomy_mask_binding_path(anat_mask_out)}
        if enhancement is not None:
            staged_input = staging / cellpose_input_path.name
            _write_cellpose_input(anat_src, vol, staged_input, log_lines=log_lines)
            staged[staged_input] = cellpose_input_path
        _publish_segmentation(
            output_base=output_base,
            staged=staged,
            replaced=[p for p in existing_masks if p.parent == out_dir],
            log_lines=log_lines,
        )
    finally:
        import shutil

        shutil.rmtree(staging, ignore_errors=True)
    if enhancement is not None:
        enhancement["cellpose_input_sha256"] = _file_sha256(cellpose_input_path)
    log_lines.append(f"[ANAT CP] Saved anatomy masks: {anat_mask_out} (max_label={max_lbl}, dtype={out_dtype})")

    return {
        "status": "segmented",
        "bindings": {
            "ANAT_SEG_SOURCE_PATH": anat_src,
            "ANAT_SEG_OUT_DIR": out_dir,
            "ANAT_SEG_CONVERT_DIR": convert_dir,
            "ANAT_CP_MODEL_PATH": str(model_path),
            "ANAT_LABELS_PATH": anat_mask_out,
            "ANAT_CELLPOSE_INPUT_PATH": cellpose_input_path,
        },
        "boundary_enhancement": enhancement,
        "log_lines": log_lines,
        "runtime_provenance": runtime_provenance,
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
    ensure = ensure_uint_labels_func if callable(ensure_uint_labels_func) else _ensure_uint_labels_local()
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
    ensure = ensure_uint_labels_func if callable(ensure_uint_labels_func) else _ensure_uint_labels_local()
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
        labels = _ensure_uint_labels_local()(labels)
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
            ref_norm_arr = _norm01(np.asarray(ref_norm_mem, dtype=np.float32))
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
    "anatomy_mask_binding_path",
    "hessian_valley_subtract",
    "hessian_valley_tag",
    "resolve_functional_labels_for_plane",
    "resolve_hcr_cellpose_model_path",
    "resolve_native_suite2p_labels_for_plane",
    "run_anatomy_cellpose_stage",
    "run_hcr_cellpose_stage",
    "write_anatomy_mask_binding",
]
