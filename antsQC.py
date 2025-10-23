"""
Utilities for registering confocal Cellpose segmentations to a two-photon
reference with ANTs, computing centroid matches, and producing QC artifacts.

The module keeps stateful notebook logic in one place, exposes a structured API
for reuse, and removes duplicated helper snippets from the original notebook.
"""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, Literal, Sequence

import numpy as np
import pandas as pd
from skimage.measure import regionprops_table
from scipy.optimize import linear_sum_assignment
from scipy.spatial import cKDTree

try:  # ANTsPy is optional at import time; some workflows only export CSVs.
    import ants as _ants_module
except Exception:  # pragma: no cover - optional dependency
    _ants_module = None


__all__ = [
    "AntsPyNotAvailableError",
    "Spacing",
    "TransformConfig",
    "MaskDataset",
    "PipelineConfig",
    "PipelineResult",
    "load_cellpose_masks",
    "load_mask_dataset",
    "build_overview_table",
    "warp_label_mask",
    "apply_transforms_to_points",
    "match_centroids",
    "run_pipeline",
]


class AntsPyNotAvailableError(RuntimeError):
    """Raised when an operation requires ANTsPy but it is not installed."""

    def __init__(self) -> None:
        super().__init__(
            "ANTsPy is required for this operation but is not available. "
            "Install `antspyx` or inject an ANTs module via `ants_module`."
        )


@dataclass(frozen=True)
class Spacing:
    """Voxel spacing (µm) for a label volume."""

    dy: float
    dx: float
    dz: float | None = None

    @classmethod
    def from_mapping(cls, mapping: dict[str, float]) -> "Spacing":
        """Construct from notebook-style dictionaries with dx/dy(/dz) keys."""
        dz_val = mapping.get("dz")
        return cls(
            dy=float(mapping["dy"]),
            dx=float(mapping["dx"]),
            dz=float(dz_val) if dz_val is not None else None,
        )

    @property
    def ndim(self) -> int:
        return 3 if self.dz is not None else 2

    def voxel_tuple(self) -> tuple[float, ...]:
        """Return spacing ordered to match NumPy masks (z,y,x) or (y,x)."""
        if self.ndim == 3:
            assert self.dz is not None
            return (self.dz, self.dy, self.dx)
        return (self.dy, self.dx)

    def ants_spacing(self) -> tuple[float, ...]:
        """Return spacing ordered for ANTs images (x,y,z) or (x,y)."""
        if self.ndim == 3:
            assert self.dz is not None
            return (self.dx, self.dy, self.dz)
        return (self.dx, self.dy)


def fs_info(path: Path) -> dict[str, object]:
    """Return lightweight filesystem metadata for `path`."""
    path = Path(path)
    exists = path.exists()
    size_bytes = path.stat().st_size if exists else None
    modified = (
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(path.stat().st_mtime))
        if exists
        else None
    )
    return {
        "path": str(path),
        "exists": exists,
        "size_bytes": size_bytes,
        "size_MB": (size_bytes / 1e6) if size_bytes is not None else None,
        "modified": modified,
    }


def physical_extent_um(shape: tuple[int, ...], spacing: Spacing) -> tuple[float, ...]:
    """Return the physical field-of-view in microns for a mask shape."""
    if spacing.ndim == 3:
        assert spacing.dz is not None
        z, y, x = shape
        return (z * spacing.dz, y * spacing.dy, x * spacing.dx)
    y, x = shape
    return (y * spacing.dy, x * spacing.dx)


def load_cellpose_masks(seg_path: str | os.PathLike[str]) -> np.ndarray:
    """
    Load Cellpose *_seg.npy dictionaries, raw label arrays (.npy), or npz files.

    Returns a NumPy ndarray of integer labels (2D or 3D).
    """
    seg_path = Path(seg_path)
    if not seg_path.exists():
        raise FileNotFoundError(f"File not found: {seg_path}")

    try:
        obj = np.load(seg_path, allow_pickle=True)
    except Exception as exc:
        raise OSError(
            f"Failed to load {seg_path}. If the file lives on a network share, "
            f"copy it locally and retry. Original error: {exc}"
        ) from exc

    if isinstance(obj, np.lib.npyio.NpzFile):
        if "masks" not in obj.files:
            raise KeyError(f"'masks' not found in {seg_path}. Keys: {obj.files}")
        return obj["masks"]

    if hasattr(obj, "item"):  # Cellpose dict stored in .npy
        try:
            data = obj.item()
        except Exception as exc:
            raise OSError(f"{seg_path} appears corrupted (cannot unpickle dict).") from exc
        masks = data.get("masks")
        if masks is None:
            raise KeyError(f"'masks' not found in {seg_path}. Keys: {list(data.keys())}")
        return masks

    if isinstance(obj, np.ndarray):
        return obj

    raise TypeError(f"Unexpected content in {seg_path}: {type(obj)}")


@dataclass
class MaskDataset:
    """Container for a labeled mask plus metadata derived from it."""

    name: str
    path: Path
    spacing: Spacing
    mask: np.ndarray = field(repr=False)

    _centroids_idx: pd.DataFrame | None = field(default=None, init=False, repr=False)

    def summary_row(self) -> dict[str, object]:
        info = fs_info(self.path)
        unique_vals = np.unique(self.mask)
        has_background = unique_vals.size > 0 and unique_vals[0] == 0
        n_labels = int(unique_vals.size - 1) if has_background else int(unique_vals.size)
        field_um = tuple(round(v, 3) for v in physical_extent_um(self.mask.shape, self.spacing))
        nz_count = int((self.mask > 0).sum())
        frac_nz = float(nz_count / self.mask.size) if self.mask.size else 0.0
        return {
            "dataset": self.name,
            "file": self.path.name,
            "path": info["path"],
            "exists": info["exists"],
            "size_MB": round(info["size_MB"], 3) if info["size_MB"] is not None else None,
            "modified": info["modified"],
            "shape": tuple(int(x) for x in self.mask.shape),
            "ndim": int(self.mask.ndim),
            "dtype": str(self.mask.dtype),
            "min_label": int(self.mask.min()),
            "max_label": int(self.mask.max()),
            "n_cells": n_labels,
            "nonzero_voxels": nz_count,
            "frac_nonzero_pct": round(frac_nz * 100.0, 4),
            "voxel_um": self.spacing.voxel_tuple(),
            "FOV_um": field_um,
            "n_voxels": int(self.mask.size),
            "anisotropy_z_over_y": (
                float(self.spacing.dz / self.spacing.dy) if self.spacing.ndim == 3 else None
            ),
        }

    def _ensure_centroids(self) -> pd.DataFrame:
        if self._centroids_idx is None:
            props = regionprops_table(self.mask, properties=("label", "centroid"))
            df = pd.DataFrame(props)
            if self.spacing.ndim == 3:
                df = df.rename(
                    columns={"centroid-0": "z", "centroid-1": "y", "centroid-2": "x"}
                )
            else:
                df = df.rename(columns={"centroid-0": "y", "centroid-1": "x"})
            self._centroids_idx = df
        return self._centroids_idx

    def centroid_indices(self) -> pd.DataFrame:
        return self._ensure_centroids().copy()

    def centroid_positions_um(self) -> np.ndarray:
        df = self._ensure_centroids()
        if self.spacing.ndim == 3:
            assert self.spacing.dz is not None
            arr = df[["z", "y", "x"]].to_numpy(dtype=float)
            arr[:, 0] *= self.spacing.dz
            arr[:, 1] *= self.spacing.dy
            arr[:, 2] *= self.spacing.dx
            return arr
        arr = df[["y", "x"]].to_numpy(dtype=float)
        arr[:, 0] *= self.spacing.dy
        arr[:, 1] *= self.spacing.dx
        return arr

    def labels(self) -> np.ndarray:
        return self._ensure_centroids()["label"].to_numpy(dtype=int, copy=True)

    def sample_voxels(self, n: int, seed: int | None = None) -> pd.DataFrame:
        """Return up to `n` random voxels (indices + label) for quick inspection."""
        rng = np.random.default_rng(seed)
        total = self.mask.size
        if total == 0:
            return pd.DataFrame(columns=(["z", "y", "x"] if self.spacing.ndim == 3 else ["y", "x"]))
        idx = rng.choice(total, size=min(n, total), replace=False)
        coords = np.array(np.unravel_index(idx, self.mask.shape)).T
        values = self.mask.ravel()[idx]
        columns = ["z", "y", "x"] if self.spacing.ndim == 3 else ["y", "x"]
        df = pd.DataFrame(coords, columns=columns)
        df.insert(0, "dataset", self.name)
        df["value"] = values
        return df


@dataclass
class TransformConfig:
    """Configuration of ANTs transforms (path order matters)."""

    paths: Sequence[str | os.PathLike[str]]
    invert_flags: Sequence[bool] | None = None
    invert_affine: bool = True

    def as_lists(self) -> tuple[list[str], list[bool]]:
        transform_paths = [str(Path(p)) for p in self.paths]
        if self.invert_flags is not None:
            if len(self.invert_flags) != len(transform_paths):
                raise ValueError(
                    "invert_flags length must match number of transforms "
                    f"({len(self.invert_flags)} vs {len(transform_paths)})"
                )
            return transform_paths, list(self.invert_flags)

        flags: list[bool] = []
        for path in transform_paths:
            suffix = Path(path).suffix.lower()
            if suffix == ".mat":
                flags.append(bool(self.invert_affine))
            else:
                flags.append(False)
        return transform_paths, flags


def _clone_geometry(src, dst):
    """Copy spacing/origin/direction from one ANTs image to another."""
    dst.set_spacing(src.spacing)
    dst.set_origin(src.origin)
    dst.set_direction(src.direction)
    return dst


def _mask_to_ants_image(
    mask: np.ndarray,
    spacing: Spacing,
    *,
    ants_module,
    like_img=None,
    dtype=np.int32,
):
    """Return an ANTs image from a NumPy mask, optionally cloning geometry."""
    if mask.ndim == 3:
        arr = np.transpose(mask, (2, 1, 0)).astype(dtype, copy=False)
    else:
        arr = mask.astype(dtype, copy=False)
    img = ants_module.from_numpy(arr)
    if like_img is not None:
        return _clone_geometry(like_img, img)
    img.set_spacing(spacing.ants_spacing())
    return img


def warp_label_mask(
    moving_mask: np.ndarray,
    moving_spacing: Spacing,
    transform_config: TransformConfig,
    *,
    fixed_mask: np.ndarray,
    fixed_spacing: Spacing,
    ants_module=None,
    moving_reference_path: str | os.PathLike[str] | None = None,
    fixed_reference_path: str | os.PathLike[str] | None = None,
) -> np.ndarray:
    """
    Warp a confocal label mask into the fixed 2P grid using ANTs transforms.

    Parameters
    ----------
    moving_mask
        Confocal labels in NumPy (Z,Y,X) order.
    moving_spacing
        Spacing for `moving_mask`.
    transform_config
        Ordered list of ANTs transforms (warp before affine).
    fixed_mask
        Mask defining the target grid (typically the 2P segmentation).
    fixed_spacing
        Spacing for the fixed grid.
    ants_module
        Optional injected ANTs module (for easier testing). Defaults to global import.
    moving_reference_path / fixed_reference_path
        Optional intensity images used during registration; when provided the
        label ANTs images inherit their geometry to avoid spacing/origin drift.
    """
    ants_module = ants_module or _ants_module
    if ants_module is None:
        raise AntsPyNotAvailableError()

    mov_ref = ants_module.image_read(str(moving_reference_path)) if moving_reference_path else None
    fix_ref = ants_module.image_read(str(fixed_reference_path)) if fixed_reference_path else None

    mov_img = _mask_to_ants_image(moving_mask, moving_spacing, ants_module=ants_module, like_img=mov_ref)
    ref_img = _mask_to_ants_image(
        fixed_mask,
        fixed_spacing,
        ants_module=ants_module,
        like_img=fix_ref,
        dtype=np.int16,
    )

    transformlist, whichtoinvert = transform_config.as_lists()
    warped_xyz = ants_module.apply_transforms(
        fixed=ref_img,
        moving=mov_img,
        transformlist=transformlist,
        whichtoinvert=whichtoinvert,
        interpolator="nearestNeighbor",
    ).numpy()

    if moving_mask.ndim == 3:
        warped = np.transpose(warped_xyz, (2, 1, 0))
    else:
        warped = warped_xyz
    return warped.astype(moving_mask.dtype, copy=False)


def um_to_index_dataframe(points_um: np.ndarray, spacing: Spacing) -> pd.DataFrame:
    """Convert physical coordinates (µm) to ANTs index units (x,y,(z))."""
    points = np.asarray(points_um, dtype=float)
    if spacing.ndim == 3:
        if points.shape[1] != 3:
            raise ValueError("3D spacing requires points with three columns (z,y,x).")
        assert spacing.dz is not None
        df = pd.DataFrame(
            {
                "x": points[:, 2] / spacing.dx,
                "y": points[:, 1] / spacing.dy,
                "z": points[:, 0] / spacing.dz,
            }
        )
    else:
        if points.shape[1] != 2:
            raise ValueError("2D spacing requires points with two columns (y,x).")
        df = pd.DataFrame(
            {
                "x": points[:, 1] / spacing.dx,
                "y": points[:, 0] / spacing.dy,
            }
        )
    return df[["x", "y", "z"]] if "z" in df.columns else df[["x", "y"]]


def index_df_to_um(df_idx: pd.DataFrame, spacing: Spacing) -> np.ndarray:
    """Convert ANTs index units back to microns, returning (z,y,x) or (y,x)."""
    if spacing.ndim == 3:
        assert spacing.dz is not None
        x = df_idx["x"].to_numpy()
        y = df_idx["y"].to_numpy()
        z = df_idx["z"].to_numpy()
        return np.c_[z * spacing.dz, y * spacing.dy, x * spacing.dx]
    x = df_idx["x"].to_numpy()
    y = df_idx["y"].to_numpy()
    return np.c_[y * spacing.dy, x * spacing.dx]


def apply_transforms_to_points(
    points_um: np.ndarray,
    moving_spacing: Spacing,
    fixed_spacing: Spacing,
    transform_config: TransformConfig,
    *,
    ants_module=None,
) -> np.ndarray:
    """Map points from moving (confocal) space to fixed (2P) space in microns."""
    ants_module = ants_module or _ants_module
    if ants_module is None:
        raise AntsPyNotAvailableError()

    moving_idx = um_to_index_dataframe(points_um, moving_spacing)
    transformlist, whichtoinvert = transform_config.as_lists()
    dim = 3 if moving_spacing.ndim == 3 else 2
    fixed_idx = ants_module.apply_transforms_to_points(dim, moving_idx, transformlist, whichtoinvert=whichtoinvert)
    return index_df_to_um(fixed_idx, fixed_spacing)


def nearest_neighbor_match(A_um: np.ndarray, B_um: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """Return distances and indices of nearest neighbors from A to B."""
    tree = cKDTree(B_um)
    dists, nn = tree.query(A_um, k=1)
    return dists, nn


def hungarian_match(
    A_um: np.ndarray,
    B_um: np.ndarray,
    max_cost: float | None = None,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Return optimal 1–1 assignment indices using the Hungarian algorithm."""
    from scipy.spatial.distance import cdist

    C = cdist(A_um, B_um)
    if max_cost is not None and np.isfinite(max_cost):
        C = np.where(C > max_cost, max_cost * 10.0, C)
    row_ind, col_ind = linear_sum_assignment(C)
    dists = C[row_ind, col_ind]
    return dists, col_ind, row_ind


def summarize_distances(dists: np.ndarray, valid_mask: np.ndarray) -> dict[str, float]:
    """Aggregate distance statistics for matched centroids."""
    total = int(dists.size)
    within = int(valid_mask.sum())
    if within > 0:
        valid_dists = dists[valid_mask]
        return {
            "N_total": total,
            "N_within_gate": within,
            "frac_within_gate": float(within / total),
            "median_um": float(np.median(valid_dists)),
            "p90_um": float(np.percentile(valid_dists, 90)),
        }
    return {
        "N_total": total,
        "N_within_gate": 0,
        "frac_within_gate": 0.0,
        "median_um": float("nan"),
        "p90_um": float("nan"),
    }


def build_overview_table(datasets: Iterable[MaskDataset]) -> pd.DataFrame:
    """Return a summary table for a collection of datasets."""
    rows = [ds.summary_row() for ds in datasets]
    return pd.DataFrame(rows)


def match_centroids(
    conf_labels: np.ndarray,
    twop_labels: np.ndarray,
    conf_in_twop_um: np.ndarray,
    twop_um: np.ndarray,
    *,
    method: Literal["nn", "hungarian"] = "nn",
    max_distance_um: float = np.inf,
) -> tuple[pd.DataFrame, dict[str, float]]:
    """Match transformed confocal centroids to 2P centroids and summarize distances."""
    conf_labels = np.asarray(conf_labels, dtype=int)
    twop_labels = np.asarray(twop_labels, dtype=int)
    if method == "nn":
        dists, nn = nearest_neighbor_match(conf_in_twop_um, twop_um)
        matched_conf = conf_labels
        matched_twop = twop_labels[nn]
        paired_twop_coords = twop_um[nn]
    elif method == "hungarian":
        dists, col_ind, row_ind = hungarian_match(conf_in_twop_um, twop_um, max_cost=max_distance_um)
        matched_conf = conf_labels[row_ind]
        matched_twop = twop_labels[col_ind]
        paired_twop_coords = twop_um[col_ind]
    else:
        raise ValueError("method must be 'nn' or 'hungarian'")

    valid = np.asarray(dists <= max_distance_um)

    matches = pd.DataFrame(
        {
            "conf_label": matched_conf,
            "twoP_label": matched_twop,
            "distance_um": dists,
            "within_gate": valid,
        }
    )

    if conf_in_twop_um.shape[1] == 3:
        matches[["conf_z_um", "conf_y_um", "conf_x_um"]] = conf_in_twop_um[: len(matches)]
        matches[["twoP_z_um", "twoP_y_um", "twoP_x_um"]] = paired_twop_coords[: len(matches)]
    else:
        matches[["conf_y_um", "conf_x_um"]] = conf_in_twop_um[: len(matches)]
        matches[["twoP_y_um", "twoP_x_um"]] = paired_twop_coords[: len(matches)]

    matches = matches.sort_values("distance_um").reset_index(drop=True)
    summary = summarize_distances(dists, valid)
    summary.update({"method": method, "max_distance_um": float(max_distance_um)})
    return matches, summary


@dataclass
class PipelineConfig:
    """Configurable inputs for the QC pipeline."""

    confocal_seg: str | os.PathLike[str]
    twop_seg: str | os.PathLike[str]
    confocal_spacing: Spacing
    twop_spacing: Spacing
    transforms: TransformConfig
    match_method: Literal["nn", "hungarian"] = "nn"
    max_distance_um: float = 5.0
    random_seed: int = 42
    confocal_intensity: str | os.PathLike[str] | None = None
    twop_intensity: str | os.PathLike[str] | None = None

    def __post_init__(self) -> None:
        self.confocal_seg = Path(self.confocal_seg)
        self.twop_seg = Path(self.twop_seg)
        if self.confocal_intensity is not None:
            self.confocal_intensity = Path(self.confocal_intensity)
        if self.twop_intensity is not None:
            self.twop_intensity = Path(self.twop_intensity)


@dataclass
class PipelineResult:
    """Aggregate outputs from `run_pipeline` for downstream notebook use."""

    config: PipelineConfig
    overview: pd.DataFrame
    confocal_dataset: MaskDataset
    twop_dataset: MaskDataset
    warped_confocal_labels: np.ndarray
    conf_centroids_idx: pd.DataFrame
    twop_centroids_idx: pd.DataFrame
    conf_centroids_um: np.ndarray
    twop_centroids_um: np.ndarray
    conf_centroids_in_twop_um: np.ndarray
    matches: pd.DataFrame
    match_summary: dict[str, float]


def load_mask_dataset(name: str, path: str | os.PathLike[str], spacing: Spacing) -> MaskDataset:
    """Load a labeled mask and wrap it in `MaskDataset`."""
    mask = load_cellpose_masks(path)
    return MaskDataset(name=name, path=Path(path), spacing=spacing, mask=mask)


def run_pipeline(
    config: PipelineConfig,
    *,
    ants_module=None,
    save_overview_csv: str | os.PathLike[str] | None = None,
    save_matches_csv: str | os.PathLike[str] | None = None,
    save_summary_json: str | os.PathLike[str] | None = None,
    save_warped_mask: str | os.PathLike[str] | None = None,
) -> PipelineResult:
    """
    Execute the QC workflow end-to-end and optionally persist artifacts.

    Returns a `PipelineResult` for interactive notebooks or downstream scripts.
    """
    ants_module = ants_module or _ants_module

    conf_dataset = load_mask_dataset("Confocal (HCR)", config.confocal_seg, config.confocal_spacing)
    twop_dataset = load_mask_dataset("2P (mCherry)", config.twop_seg, config.twop_spacing)

    overview = build_overview_table([conf_dataset, twop_dataset])
    if save_overview_csv is not None:
        overview.to_csv(save_overview_csv, index=False)

    warped_confocal = warp_label_mask(
        moving_mask=conf_dataset.mask,
        moving_spacing=conf_dataset.spacing,
        transform_config=config.transforms,
        fixed_mask=twop_dataset.mask,
        fixed_spacing=twop_dataset.spacing,
        ants_module=ants_module,
        moving_reference_path=config.confocal_intensity,
        fixed_reference_path=config.twop_intensity,
    )
    if save_warped_mask is not None:
        np.save(save_warped_mask, warped_confocal)

    conf_centroids_idx = conf_dataset.centroid_indices()
    twop_centroids_idx = twop_dataset.centroid_indices()
    conf_centroids_um = conf_dataset.centroid_positions_um()
    twop_centroids_um = twop_dataset.centroid_positions_um()

    conf_in_twop_um = apply_transforms_to_points(
        conf_centroids_um,
        conf_dataset.spacing,
        twop_dataset.spacing,
        config.transforms,
        ants_module=ants_module,
    )

    matches, summary = match_centroids(
        conf_labels=conf_centroids_idx["label"].to_numpy(dtype=int, copy=True),
        twop_labels=twop_centroids_idx["label"].to_numpy(dtype=int, copy=True),
        conf_in_twop_um=conf_in_twop_um,
        twop_um=twop_centroids_um,
        method=config.match_method,
        max_distance_um=config.max_distance_um,
    )

    if save_matches_csv is not None:
        matches.to_csv(save_matches_csv, index=False)
    if save_summary_json is not None:
        Path(save_summary_json).write_text(json.dumps(summary, indent=2))

    return PipelineResult(
        config=config,
        overview=overview,
        confocal_dataset=conf_dataset,
        twop_dataset=twop_dataset,
        warped_confocal_labels=warped_confocal,
        conf_centroids_idx=conf_centroids_idx,
        twop_centroids_idx=twop_centroids_idx,
        conf_centroids_um=conf_centroids_um,
        twop_centroids_um=twop_centroids_um,
        conf_centroids_in_twop_um=conf_in_twop_um,
        matches=matches,
        match_summary=summary,
    )
