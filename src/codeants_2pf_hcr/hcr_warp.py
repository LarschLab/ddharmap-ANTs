"""HCR warp helpers and notebook-facing stage wrappers for single-fish cells [43] and [43b]."""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
import re
from typing import Any

from ._hcr_warp_stage_cells import CELL_43_SOURCE, CELL_43B_SOURCE
from .single_fish_notebook_stages import (
    run_single_fish_cell_38_stage,
    run_single_fish_cell_40_stage,
    run_single_fish_cell_41_stage,
)


@dataclass(frozen=True)
class HcrDirectWarpResult:
    mask_path: str
    output_label_path: str
    output_metadata_path: str
    round_idx: int
    moving_intensity_path: str
    transformlist: tuple[str, ...]
    whichtoinvert: tuple[bool, ...]
    max_label: int


def _parse_round_from_name(path: Path | str) -> int | None:
    match = re.search(r"round(\d+)", Path(path).name.lower())
    return int(match.group(1)) if match else None


def _find_intensity_for_round(preproc_dir: Path, round_idx: int) -> Path | None:
    round_idx = int(round_idx)
    exact_hits: list[Path] = []
    round_hits: list[Path] = []
    for sub_name in ("rbest", "rn"):
        sub = preproc_dir / sub_name
        if not sub.exists():
            continue
        for path in sorted(sub.glob("*.nrrd")):
            name = path.name.lower()
            if f"round{round_idx}" not in name:
                continue
            round_hits.append(path)
            if "channel1" in name and "gcamp" in name:
                exact_hits.append(path)
    if exact_hits:
        return exact_hits[0]
    return round_hits[0] if round_hits else None


def _find_hcr_intensity_for_mask(preproc_dir: Path, mask_path: Path, round_idx: int, fish_id: str) -> Path | None:
    stem = mask_path.stem
    base = stem[:-9] if stem.endswith("_cp_masks") else stem
    ch_match = re.search(r"channel(\d+)", stem.lower())
    channel = int(ch_match.group(1)) if ch_match else None
    gene_match = (
        re.search(rf"round{int(round_idx)}_channel{channel}_(.+?)(?:_cp_masks)?$", stem.lower())
        if channel is not None
        else None
    )
    gene_token = gene_match.group(1) if gene_match else None
    token_hits: list[Path] = []
    round_hits: list[Path] = []
    for sub_name in ("rbest", "rn"):
        sub = preproc_dir / sub_name
        if not sub.exists():
            continue
        exact = sub / f"{base}.nrrd"
        if exact.exists():
            return exact
        pattern = f"{fish_id}_round{int(round_idx)}_channel{channel}_*.nrrd" if channel is not None else f"*round{int(round_idx)}*.nrrd"
        for path in sorted(sub.glob(pattern)):
            if "gcamp" in path.name.lower():
                continue
            round_hits.append(path)
            if gene_token and gene_token in path.stem.lower():
                token_hits.append(path)
    if token_hits:
        return token_hits[0]
    return round_hits[0] if round_hits else None


def _find_transform(trans_dir: Path, round_idx: int, best_round_idx: int, target_tag: str, kind: str) -> Path | None:
    if kind not in {"warp", "affine"}:
        raise ValueError(f"Unknown transform kind: {kind}")
    suffix = "1Warp.nii.gz" if kind == "warp" else "0GenericAffine.mat"
    if not trans_dir.exists():
        return None
    files = [path for path in sorted(trans_dir.glob(f"*{suffix}")) if "inverse" not in path.name.lower()]
    if not files:
        return None

    def has_source_round(name: str) -> bool:
        if re.search(rf"(^|[_-])round{int(round_idx)}(?=([_.-]|$))", name):
            return True
        if re.search(rf"(^|[_-])r{int(round_idx)}(?=([_.-]|$))", name):
            return True
        return int(round_idx) == int(best_round_idx) and bool(re.search(r"(^|[_-])rbest(?=([_.-]|$))", name))

    def target_aliases(tag: str) -> list[str]:
        tag = str(tag or "").lower().strip()
        if tag == "to_2p":
            return ["to_2p", "in_2p", "to2p", "to_ref", "in_ref"]
        match = re.match(r"to_r(\d+)$", tag)
        if match:
            target_round = int(match.group(1))
            aliases = [f"to_r{target_round}", f"in_r{target_round}"]
            if target_round == int(best_round_idx):
                aliases += ["to_rbest", "in_rbest"]
            return aliases
        return [tag] if tag else []

    source_hits = [path for path in files if has_source_round(path.name.lower())]
    aliases = target_aliases(target_tag)
    if aliases:
        both_hits = [path for path in source_hits if any(alias in path.name.lower() for alias in aliases)]
        if both_hits:
            return both_hits[0]
    if source_hits:
        return source_hits[0]
    return files[0] if len(files) == 1 else None


def build_direct_ants_hcr_transform_chain(
    *,
    round_idx: int,
    best_round_idx: int,
    rbest_to_2p_transform_dir: Path,
    rn_to_rbest_transform_dir: Path,
) -> tuple[Path, ...]:
    best_to_2p_warp = _find_transform(rbest_to_2p_transform_dir, best_round_idx, best_round_idx, "to_2p", "warp")
    best_to_2p_affine = _find_transform(rbest_to_2p_transform_dir, best_round_idx, best_round_idx, "to_2p", "affine")
    chain: list[Path | None] = [best_to_2p_warp, best_to_2p_affine]
    if int(round_idx) != int(best_round_idx):
        rn_to_best_warp = _find_transform(
            rn_to_rbest_transform_dir,
            round_idx,
            best_round_idx,
            f"to_r{int(best_round_idx)}",
            "warp",
        )
        rn_to_best_affine = _find_transform(
            rn_to_rbest_transform_dir,
            round_idx,
            best_round_idx,
            f"to_r{int(best_round_idx)}",
            "affine",
        )
        chain.extend([rn_to_best_warp, rn_to_best_affine])
    if any(path is None for path in chain):
        raise FileNotFoundError(f"Missing direct HCR->2P transform chain for round r{round_idx}: {chain}")
    return tuple(path for path in chain if path is not None)


def _ants_clone_geometry(dst_img: Any, like_img: Any) -> Any:
    dst_img.set_spacing(like_img.spacing)
    dst_img.set_origin(like_img.origin)
    dst_img.set_direction(like_img.direction)
    return dst_img


def _warp_zyx_labels_with_ants(
    *,
    labels_zyx: Any,
    moving_intensity_path: Path,
    fixed_intensity_path: Path,
    transformlist: tuple[Path, ...],
) -> Any:
    import numpy as np

    try:
        import ants  # type: ignore
    except Exception as exc:  # pragma: no cover - depends on optional native package
        raise ImportError("ANTsPy is required for direct HCR label warping") from exc

    labels_xyz = np.transpose(np.asarray(labels_zyx), (2, 1, 0)).astype(np.int32, copy=False)
    moving_label_img = ants.from_numpy(labels_xyz)
    moving_intensity_img = ants.image_read(str(moving_intensity_path))
    _ants_clone_geometry(moving_label_img, moving_intensity_img)
    if tuple(moving_label_img.shape) != tuple(moving_intensity_img.shape):
        raise RuntimeError(
            f"Label XYZ shape {tuple(moving_label_img.shape)} does not match moving intensity "
            f"shape {tuple(moving_intensity_img.shape)} for {moving_intensity_path}."
        )
    fixed_img = ants.image_read(str(fixed_intensity_path))
    warped_xyz = ants.apply_transforms(
        fixed=fixed_img,
        moving=moving_label_img,
        transformlist=[str(path) for path in transformlist],
        whichtoinvert=[False for _ in transformlist],
        interpolator="nearestNeighbor",
    ).numpy()
    return np.transpose(warped_xyz, (2, 1, 0)).astype(np.int32, copy=False), fixed_img


def _write_warped_label_tiff(path: Path, labels_zyx: Any, *, like_img: Any | None = None) -> None:
    import numpy as np
    import tifffile

    arr = np.asarray(labels_zyx)
    max_label = int(arr.max()) if arr.size else 0
    if max_label > 65535:
        raise RuntimeError(f"Label id overflow for uint16 TIFF output ({max_label} > 65535): {path}")
    kwargs: dict[str, Any] = {"compression": "deflate", "imagej": True, "metadata": {"axes": "ZYX"}}
    try:
        if like_img is not None:
            sx, sy, sz = [float(value) for value in like_img.spacing]
            if all(value > 0 for value in (sx, sy, sz)):
                kwargs["metadata"] = {"axes": "ZYX", "spacing": sz, "unit": "um"}
                kwargs["resolution"] = (1.0 / sx, 1.0 / sy)
    except Exception:
        pass
    path.parent.mkdir(parents=True, exist_ok=True)
    tifffile.imwrite(path, arr.astype(np.uint16, copy=False), **kwargs)


def run_direct_ants_hcr_label_warp(
    *,
    fish_id: str,
    mask_paths: tuple[Path, ...],
    preproc_dir: Path,
    anatomy_intensity_path: Path,
    output_dir: Path,
    best_round_idx: int,
    rbest_to_2p_transform_dir: Path,
    rn_to_rbest_transform_dir: Path,
) -> tuple[HcrDirectWarpResult, ...]:
    import numpy as np
    import tifffile

    output_dir.mkdir(parents=True, exist_ok=True)
    results: list[HcrDirectWarpResult] = []
    for mask_path in mask_paths:
        round_idx = _parse_round_from_name(mask_path)
        if round_idx is None:
            continue
        labels_zyx = tifffile.imread(mask_path)
        if np.asarray(labels_zyx).ndim != 3:
            raise ValueError(f"Expected 3D HCR mask TIFF for direct warp: {mask_path}")
        moving_intensity = _find_hcr_intensity_for_mask(preproc_dir, mask_path, round_idx, fish_id)
        if moving_intensity is None or not moving_intensity.exists():
            moving_intensity = _find_intensity_for_round(preproc_dir, round_idx)
        if moving_intensity is None or not moving_intensity.exists():
            raise FileNotFoundError(f"Missing moving HCR intensity for {mask_path.name} round r{round_idx}")
        transform_chain = build_direct_ants_hcr_transform_chain(
            round_idx=round_idx,
            best_round_idx=best_round_idx,
            rbest_to_2p_transform_dir=rbest_to_2p_transform_dir,
            rn_to_rbest_transform_dir=rn_to_rbest_transform_dir,
        )
        warped_zyx, fixed_img = _warp_zyx_labels_with_ants(
            labels_zyx=labels_zyx,
            moving_intensity_path=moving_intensity,
            fixed_intensity_path=anatomy_intensity_path,
            transformlist=transform_chain,
        )
        save_base = output_dir / f"{mask_path.stem}_in_2p"
        label_path = Path(f"{save_base}_labels_uint16.tif")
        metadata_path = Path(f"{save_base}_warp_meta.json")
        _write_warped_label_tiff(label_path, warped_zyx, like_img=fixed_img)
        max_label = int(np.asarray(warped_zyx).max()) if np.asarray(warped_zyx).size else 0
        metadata = {
            "round": int(round_idx),
            "mask": str(mask_path),
            "method": "ants_direct_to_2p",
            "output_space": "2p",
            "moving_intensity": str(moving_intensity),
            "fixed_intensity": str(anatomy_intensity_path),
            "transformlist": [str(path) for path in transform_chain],
            "whichtoinvert": [False for _ in transform_chain],
            "outputs": {"tif": str(label_path)},
            "filter_stats": {
                "n_labels_before": int(len([value for value in np.unique(labels_zyx) if int(value) != 0])),
                "n_labels_after": int(len([value for value in np.unique(warped_zyx) if int(value) != 0])),
                "low_conf_labels": [],
            },
        }
        metadata_path.write_text(json.dumps(metadata, indent=2))
        results.append(
            HcrDirectWarpResult(
                mask_path=str(mask_path),
                output_label_path=str(label_path),
                output_metadata_path=str(metadata_path),
                round_idx=int(round_idx),
                moving_intensity_path=str(moving_intensity),
                transformlist=tuple(str(path) for path in transform_chain),
                whichtoinvert=tuple(False for _ in transform_chain),
                max_label=max_label,
            )
        )
    return tuple(results)


def _exec_stage_source(*, source: str, stage_tag: str, env: dict[str, Any] | None = None) -> dict[str, Any]:
    namespace = dict(env or {})
    namespace.setdefault("__name__", f"codeants_2pf_hcr.hcr_warp.{stage_tag}")
    code = compile(source, f"<{stage_tag}>", "exec")
    exec(code, namespace, namespace)
    bindings = {key: value for key, value in namespace.items() if not key.startswith("__")}
    return {
        "status": "ok",
        "bindings": bindings,
        "log_lines": [f"{stage_tag} executed via codeants_2pf_hcr.hcr_warp"],
    }


def run_hcr_external_bigwarp_label_stage(*, env: dict[str, Any] | None = None) -> dict[str, Any]:
    return _exec_stage_source(source=CELL_43_SOURCE, stage_tag="[43]", env=env)


def run_hcr_external_bigwarp_intensity_stage(*, env: dict[str, Any] | None = None) -> dict[str, Any]:
    return _exec_stage_source(source=CELL_43B_SOURCE, stage_tag="[43b]", env=env)


__all__ = [
    "HcrDirectWarpResult",
    "build_direct_ants_hcr_transform_chain",
    "run_direct_ants_hcr_label_warp",
    "run_single_fish_cell_38_stage",
    "run_single_fish_cell_40_stage",
    "run_single_fish_cell_41_stage",
    "run_hcr_external_bigwarp_intensity_stage",
    "run_hcr_external_bigwarp_label_stage",
]
