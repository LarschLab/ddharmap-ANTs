"""QA figure builders used by tools wrappers and notebook-adjacent workflows."""

from __future__ import annotations

import csv
import json
import os
from pathlib import Path
import tempfile
from typing import Any

_cache_root = tempfile.mkdtemp(prefix="mpl_cache_")
os.environ.setdefault("MPLCONFIGDIR", _cache_root)
os.environ.setdefault("XDG_CACHE_HOME", _cache_root)

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
from matplotlib import font_manager
from matplotlib.gridspec import GridSpec
import matplotlib.patheffects as path_effects
import numpy as np
import SimpleITK as sitk
import tifffile


DEFAULT_DATA_ROOT = Path("/Users/ddharmap/dataProcessing/2p_HCR/analysis/midThesis")
DEFAULT_FUNCTIONAL_IMAGE = DEFAULT_DATA_ROOT / "L396_f04/03_analysis/functional/derived/L396_f04_plane0_mcorrected_flipX_func_ref_in_2p_8bitnorm.tif"

MODALITY_SPECS = [
    ("anatomy", "Anatomy", np.array([1.0, 1.0, 1.0], dtype=np.float32)),
    ("functional", "Functional", np.array([1.00, 0.62, 0.12], dtype=np.float32)),
    ("round1", "Round 1 GCaMP", np.array([0.95, 0.20, 0.80], dtype=np.float32)),
    ("round2", "Round 2 GCaMP", np.array([0.10, 0.85, 0.95], dtype=np.float32)),
    ("round3", "Round 3 GCaMP", np.array([0.20, 0.85, 0.30], dtype=np.float32)),
]

PANEL_LAYOUT = [
    [(1, 1, "GCaMP"), (2, 1, "GCaMP"), (3, 1, "GCaMP")],
    [(1, 2, "sst1_1"), (2, 2, "sst1_2"), (3, 2, "npy")],
    [(1, 3, "pth2"), (2, 3, "tac3b"), (3, 3, "cfos")],
]

DISPLAY_NAME = {"sst1_1": "sst1.1", "sst1_2": "sst1.2"}
ROW_LUTS = [
    np.array([0.0, 1.0, 0.0], dtype=np.float32),
    np.array([1.0, 1.0, 0.0], dtype=np.float32),
    np.array([1.0, 0.0, 0.0], dtype=np.float32),
]


def pick_font_family() -> str:
    installed = {font.name for font in font_manager.fontManager.ttflist}
    return "Aptos" if "Aptos" in installed else "DejaVu Sans"


FONT_FAMILY = pick_font_family()


def _read_image(path: Path) -> np.ndarray:
    suffixes = [suffix.lower() for suffix in path.suffixes]
    if suffixes and suffixes[-1] in {".tif", ".tiff"}:
        return np.asarray(tifffile.imread(path), dtype=np.float32)
    if suffixes and suffixes[-1] == ".nrrd":
        return np.asarray(sitk.GetArrayFromImage(sitk.ReadImage(str(path))), dtype=np.float32)
    raise ValueError(f"Unsupported image format: {path}")


def _normalize_for_display(image: np.ndarray, clamp_negative: bool = False) -> np.ndarray:
    arr = np.asarray(image, dtype=np.float32)
    if clamp_negative:
        arr = np.maximum(arr, 0.0)
    finite = arr[np.isfinite(arr)]
    if finite.size == 0:
        return np.zeros_like(arr, dtype=np.float32)
    black = float(np.percentile(finite, 1.0))
    white = float(np.percentile(finite, 99.5))
    if white <= black:
        black = float(finite.min())
        white = float(finite.max())
    if white <= black:
        return np.zeros_like(arr, dtype=np.float32)
    return np.clip((arr - black) / (white - black + 1e-6), 0.0, 1.0).astype(np.float32)


def _pseudocolor(norm01: np.ndarray, rgb: np.ndarray) -> np.ndarray:
    return np.repeat(norm01[..., None], 3, axis=2) * rgb.reshape(1, 1, 3)


def build_best_plane_modality_merge_grid(
    *,
    fish_id: str,
    data_root: Path = DEFAULT_DATA_ROOT,
    functional_image: Path = DEFAULT_FUNCTIONAL_IMAGE,
    output: Path | None = None,
    dpi: int = 300,
) -> Path:
    fish_dir = Path(data_root) / fish_id
    qa_dir = fish_dir / "03_analysis" / "functional" / "qa"
    qa_dir.mkdir(parents=True, exist_ok=True)
    out_path = Path(output) if output else qa_dir / f"{fish_id}_best_plane_modality_merge_grid.png"
    ncc_json = fish_dir / "03_analysis" / "functional" / "ncc" / "ncc_bestz_by_plane.json"
    anatomy_path = fish_dir / "02_reg" / "00_preprocessing" / "2p_anatomy" / f"{fish_id}_anatomy_2P_GCaMP.nrrd"
    with ncc_json.open("r") as handle:
        payload = json.load(handle)
    per_fish = payload["per_fish"][fish_id]
    best_plane_label, best_z, best_peak = max(
        ((plane_label, int(entry["best_z"]), float(entry["scores"][int(entry["best_z"])])) for plane_label, entry in per_fish.items()),
        key=lambda item: item[2],
    )
    functional_path = Path(functional_image)
    if not functional_path.exists():
        functional_path = fish_dir / "03_analysis" / "functional" / "derived" / f"{best_plane_label}_func_ref_in_2p.tif"
    round_paths = [fish_dir / "03_analysis" / "confocal" / "aligned" / f"{fish_id}_round{round_idx}_channel1_GCaMP_in_2p.nrrd" for round_idx in (1, 2, 3)]
    modalities: list[dict[str, Any]] = []
    anatomy_volume = _read_image(anatomy_path)
    anatomy_slice = anatomy_volume[best_z]
    modalities.append({"key": "anatomy", "label": "Anatomy", "rgb": MODALITY_SPECS[0][2], "gray01": _normalize_for_display(anatomy_slice)})
    func_img = _read_image(functional_path)
    if func_img.ndim == 3:
        func_img = func_img[best_z]
    modalities.append({"key": "functional", "label": "Functional", "rgb": MODALITY_SPECS[1][2], "gray01": _normalize_for_display(func_img, clamp_negative=True)})
    for round_idx, (_, label, rgb) in enumerate(MODALITY_SPECS[2:], start=1):
        round_img = _read_image(round_paths[round_idx - 1])[best_z]
        modalities.append({"key": f"round{round_idx}", "label": label, "rgb": rgb, "gray01": _normalize_for_display(round_img, clamp_negative=True)})

    merged = np.zeros((*modalities[0]["gray01"].shape, 3), dtype=np.float32)
    for modality in modalities:
        merged += _pseudocolor(modality["gray01"], modality["rgb"])
    merged = np.clip(merged, 0.0, 1.0)

    plt.rcParams["font.family"] = FONT_FAMILY
    fig = plt.figure(figsize=(10.0, 8.0), constrained_layout=True)
    grid = GridSpec(2, 3, figure=fig, height_ratios=[1.0, 1.0])
    for idx, modality in enumerate(modalities):
        ax = fig.add_subplot(grid[idx // 3, idx % 3])
        ax.imshow(_pseudocolor(modality["gray01"], modality["rgb"]))
        ax.set_title(modality["label"], fontsize=11)
        ax.axis("off")
    ax = fig.add_subplot(grid[1, 2])
    ax.imshow(merged)
    title = ax.set_title(f"Merged overlay\n{best_plane_label}, z={best_z}, NCC={best_peak:.3f}", fontsize=11)
    title.set_path_effects([path_effects.withStroke(linewidth=2, foreground="white")])
    ax.axis("off")
    fig.savefig(out_path, dpi=dpi, bbox_inches="tight")
    plt.close(fig)
    return out_path


def build_round_channel_mip_grid(
    *,
    fish_id: str,
    data_root: Path = DEFAULT_DATA_ROOT,
    output: Path | None = None,
    z_min: int | None = None,
    z_max: int | None = None,
    norm_mode: str = "robust_asinh",
    norm_black_quantile: float = 10.0,
    norm_white_quantile: float = 99.5,
    norm_gain: float = 10.0,
    norm_soft_clip: float = 4.0,
    dpi: int = 300,
) -> Path:
    fish_dir = Path(data_root) / fish_id
    aligned_dir = fish_dir / "03_analysis" / "confocal" / "aligned"
    qa_dir = fish_dir / "03_analysis" / "functional" / "qa"
    qa_dir.mkdir(parents=True, exist_ok=True)
    out_path = Path(output) if output else qa_dir / f"{fish_id}_round_channel_2p_mip_grid.png"
    tforms_csv = fish_dir / "03_analysis" / "functional" / "ncc" / "tforms_by_plane.csv"

    if z_min is None or z_max is None:
        best_z_values: list[int] = []
        with tforms_csv.open("r", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                best_z_values.append(int(float(row["best_z"])))
        z_min = min(best_z_values)
        z_max = max(best_z_values)

    def robust_asinh_norm(subvolume: np.ndarray, mip: np.ndarray) -> np.ndarray:
        positive = subvolume[subvolume > 0]
        if positive.size == 0:
            return np.zeros_like(mip, dtype=np.float32)
        black = float(np.percentile(positive, norm_black_quantile))
        white = float(np.percentile(positive, norm_white_quantile))
        if white <= black:
            black = float(positive.min())
            white = float(positive.max())
        scaled = np.maximum((mip - black) / (white - black + 1e-6), 0.0)
        scaled = np.clip(scaled, 0.0, norm_soft_clip)
        return np.asarray(np.arcsinh(norm_gain * scaled) / np.arcsinh(norm_gain * norm_soft_clip), dtype=np.float32)

    plt.rcParams["font.family"] = FONT_FAMILY
    fig = plt.figure(figsize=(10.5, 10.5), constrained_layout=True)
    grid = GridSpec(3, 3, figure=fig)
    for row_idx, row in enumerate(PANEL_LAYOUT):
        for col_idx, (round_idx, channel_idx, gene_name) in enumerate(row):
            path = aligned_dir / f"{fish_id}_round{round_idx}_channel{channel_idx}_{gene_name}_in_2p.nrrd"
            arr = np.asarray(sitk.GetArrayFromImage(sitk.ReadImage(str(path))), dtype=np.float32)
            subvolume = arr[z_min : z_max + 1]
            mip = subvolume.max(axis=0)
            norm01 = robust_asinh_norm(subvolume, mip) if norm_mode == "robust_asinh" else _normalize_for_display(mip, clamp_negative=True)
            rgb = ROW_LUTS[row_idx]
            ax = fig.add_subplot(grid[row_idx, col_idx])
            ax.imshow(_pseudocolor(norm01, rgb))
            ax.set_title(f"R{round_idx} {DISPLAY_NAME.get(gene_name, gene_name)}", fontsize=11)
            ax.axis("off")
    fig.savefig(out_path, dpi=dpi, bbox_inches="tight")
    plt.close(fig)
    return out_path


__all__ = ["build_best_plane_modality_merge_grid", "build_round_channel_mip_grid"]
