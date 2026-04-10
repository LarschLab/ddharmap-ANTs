#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import os
import tempfile
from pathlib import Path

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


DEFAULT_FISH_ID = "L396_f04"
DEFAULT_DATA_ROOT = Path("/Users/ddharmap/dataProcessing/2p_HCR/analysis/midThesis")
DEFAULT_FUNCTIONAL_IMAGE = Path(
    "/Users/ddharmap/dataProcessing/2p_HCR/analysis/midThesis/"
    "L396_f04/03_analysis/functional/derived/"
    "L396_f04_plane0_mcorrected_flipX_func_ref_in_2p_8bitnorm.tif"
)

MODALITY_SPECS = [
    ("anatomy", "Anatomy", np.array([1.0, 1.0, 1.0], dtype=np.float32)),
    ("functional", "Functional", np.array([1.00, 0.62, 0.12], dtype=np.float32)),
    ("round1", "Round 1 GCaMP", np.array([0.95, 0.20, 0.80], dtype=np.float32)),
    ("round2", "Round 2 GCaMP", np.array([0.10, 0.85, 0.95], dtype=np.float32)),
    ("round3", "Round 3 GCaMP", np.array([0.20, 0.85, 0.30], dtype=np.float32)),
]


def pick_font_family() -> str:
    installed = {f.name for f in font_manager.fontManager.ttflist}
    if "Aptos" in installed:
        return "Aptos"
    return "DejaVu Sans"


FONT_FAMILY = pick_font_family()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build a standalone best-plane modality merge grid in shared 2P anatomy space "
            "from anatomy, transformed functional reference, and round 1-3 GCaMP volumes."
        )
    )
    parser.add_argument("--fish-id", default=DEFAULT_FISH_ID, help=f"Fish identifier. Default: {DEFAULT_FISH_ID}")
    parser.add_argument(
        "--data-root",
        default=str(DEFAULT_DATA_ROOT),
        help=f"Root analysis directory. Default: {DEFAULT_DATA_ROOT}",
    )
    parser.add_argument(
        "--output",
        default=None,
        help=(
            "Output image path. Defaults to "
            "<fish>/03_analysis/functional/qa/<fish>_best_plane_modality_merge_grid.png"
        ),
    )
    parser.add_argument(
        "--functional-image",
        default=str(DEFAULT_FUNCTIONAL_IMAGE),
        help=(
            "Functional XY image to render in the left column and merged overlay. "
            f"Default: {DEFAULT_FUNCTIONAL_IMAGE}"
        ),
    )
    parser.add_argument("--dpi", type=int, default=300, help="Output DPI. Default: 300")
    args = parser.parse_args()
    if args.dpi <= 0:
        raise ValueError("--dpi must be > 0.")
    return args


def fish_dir(data_root: Path, fish_id: str) -> Path:
    return data_root / fish_id


def qa_dir(data_root: Path, fish_id: str) -> Path:
    return fish_dir(data_root, fish_id) / "03_analysis" / "functional" / "qa"


def default_output_path(data_root: Path, fish_id: str) -> Path:
    return qa_dir(data_root, fish_id) / f"{fish_id}_best_plane_modality_merge_grid.png"


def ncc_bestz_json_path(data_root: Path, fish_id: str) -> Path:
    return fish_dir(data_root, fish_id) / "03_analysis" / "functional" / "ncc" / "ncc_bestz_by_plane.json"


def anatomy_nrrd_path(data_root: Path, fish_id: str) -> Path:
    return fish_dir(data_root, fish_id) / "02_reg" / "00_preprocessing" / "2p_anatomy" / f"{fish_id}_anatomy_2P_GCaMP.nrrd"


def functional_ref_path(data_root: Path, fish_id: str, plane_label: str) -> Path:
    return (
        fish_dir(data_root, fish_id)
        / "03_analysis"
        / "functional"
        / "derived"
        / f"{plane_label}_func_ref_in_2p.tif"
    )


def round_gcamp_path(data_root: Path, fish_id: str, round_idx: int) -> Path:
    return (
        fish_dir(data_root, fish_id)
        / "03_analysis"
        / "confocal"
        / "aligned"
        / f"{fish_id}_round{round_idx}_channel1_GCaMP_in_2p.nrrd"
    )


def require_file(path: Path) -> Path:
    if not path.exists():
        raise FileNotFoundError(f"Missing required file: {path}")
    return path


def load_json(path: Path) -> dict[str, object]:
    require_file(path)
    with path.open("r") as handle:
        data = json.load(handle)
    if not isinstance(data, dict):
        raise ValueError(f"Expected top-level JSON object in {path}")
    return data


def select_best_plane(ncc_json_path: Path, fish_id: str) -> tuple[str, int, float]:
    payload = load_json(ncc_json_path)
    per_fish = payload.get("per_fish")
    if not isinstance(per_fish, dict):
        raise ValueError(f"Missing or invalid 'per_fish' in {ncc_json_path}")
    fish_entry = per_fish.get(fish_id)
    if not isinstance(fish_entry, dict) or not fish_entry:
        raise ValueError(f"Missing or empty NCC entry for fish '{fish_id}' in {ncc_json_path}")

    best_plane_label: str | None = None
    best_z: int | None = None
    best_peak: float | None = None
    for plane_label, entry in fish_entry.items():
        if not isinstance(entry, dict):
            raise ValueError(f"Invalid plane entry for '{plane_label}' in {ncc_json_path}")
        raw_best_z = entry.get("best_z")
        scores = entry.get("scores")
        if raw_best_z is None:
            raise ValueError(f"Missing best_z for '{plane_label}' in {ncc_json_path}")
        if not isinstance(scores, list) or not scores:
            raise ValueError(f"Missing or empty scores for '{plane_label}' in {ncc_json_path}")
        z_idx = int(raw_best_z)
        if z_idx < 0 or z_idx >= len(scores):
            raise ValueError(
                f"best_z={z_idx} is out of bounds for '{plane_label}' with {len(scores)} scores in {ncc_json_path}"
            )
        peak = float(scores[z_idx])
        if best_peak is None or peak > best_peak:
            best_plane_label = str(plane_label)
            best_z = z_idx
            best_peak = peak

    if best_plane_label is None or best_z is None or best_peak is None:
        raise ValueError(f"Failed to resolve best plane for fish '{fish_id}' from {ncc_json_path}")
    return best_plane_label, best_z, best_peak


def read_image(path: Path) -> np.ndarray:
    suffixes = [s.lower() for s in path.suffixes]
    if suffixes and suffixes[-1] in {".tif", ".tiff"}:
        return np.asarray(tifffile.imread(path), dtype=np.float32)
    if suffixes and suffixes[-1] == ".nrrd":
        return np.asarray(sitk.GetArrayFromImage(sitk.ReadImage(str(path))), dtype=np.float32)
    raise ValueError(f"Unsupported image format: {path}")


def ensure_plane_slice(volume: np.ndarray, best_z: int, path: Path) -> np.ndarray:
    if volume.ndim != 3:
        raise ValueError(f"Expected 3D volume for slice extraction at {path}, got shape {volume.shape}")
    if best_z < 0 or best_z >= volume.shape[0]:
        raise ValueError(f"best_z={best_z} is out of bounds for {path} with shape {volume.shape}")
    return np.asarray(volume[best_z], dtype=np.float32)


def ensure_xy_image(image: np.ndarray, path: Path) -> np.ndarray:
    if image.ndim != 2:
        raise ValueError(f"Expected 2D XY image at {path}, got shape {image.shape}")
    return np.asarray(image, dtype=np.float32)


def normalize_for_display(image: np.ndarray, clamp_negative: bool = False) -> np.ndarray:
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


def pseudocolor(norm01: np.ndarray, rgb: np.ndarray) -> np.ndarray:
    out = np.zeros((norm01.shape[0], norm01.shape[1], 3), dtype=np.float32)
    out[..., 0] = norm01 * rgb[0]
    out[..., 1] = norm01 * rgb[1]
    out[..., 2] = norm01 * rgb[2]
    return out


def label_color(rgb: np.ndarray) -> tuple[float, float, float]:
    return (float(rgb[0]), float(rgb[1]), float(rgb[2]))


def add_stroked_text(
    ax: plt.Axes,
    x: float,
    y: float,
    text: str,
    color: tuple[float, float, float],
    fontsize: float = 12,
) -> None:
    artist = ax.text(
        x,
        y,
        text,
        ha="left",
        va="top",
        transform=ax.transAxes,
        fontsize=fontsize,
        fontweight="semibold",
        family=FONT_FAMILY,
        color=color,
    )
    artist.set_path_effects(
        [
            path_effects.Stroke(linewidth=1.0, foreground="black"),
            path_effects.Normal(),
        ]
    )


def build_modalities(
    data_root: Path,
    fish_id: str,
    functional_image_override: Path | None,
) -> tuple[list[dict[str, object]], str, int, float]:
    bestz_path = ncc_bestz_json_path(data_root, fish_id)
    plane_label, best_z, peak = select_best_plane(bestz_path, fish_id)

    anat_path = require_file(anatomy_nrrd_path(data_root, fish_id))
    func_path = require_file(
        functional_image_override if functional_image_override is not None else functional_ref_path(data_root, fish_id, plane_label)
    )
    round1_path = require_file(round_gcamp_path(data_root, fish_id, 1))
    round2_path = require_file(round_gcamp_path(data_root, fish_id, 2))
    round3_path = require_file(round_gcamp_path(data_root, fish_id, 3))

    paths = {
        "anatomy": anat_path,
        "functional": func_path,
        "round1": round1_path,
        "round2": round2_path,
        "round3": round3_path,
    }

    raw_xy: dict[str, np.ndarray] = {}
    raw_xy["anatomy"] = ensure_plane_slice(read_image(anat_path), best_z, anat_path)
    raw_xy["functional"] = ensure_xy_image(read_image(func_path), func_path)
    raw_xy["round1"] = ensure_plane_slice(read_image(round1_path), best_z, round1_path)
    raw_xy["round2"] = ensure_plane_slice(read_image(round2_path), best_z, round2_path)
    raw_xy["round3"] = ensure_plane_slice(read_image(round3_path), best_z, round3_path)

    shape_set = {tuple(img.shape) for img in raw_xy.values()}
    if len(shape_set) != 1:
        raise ValueError(f"Extracted XY shapes do not match: {sorted(shape_set)}")
    xy_shape = next(iter(shape_set))
    if len(xy_shape) != 2:
        raise ValueError(f"Expected common XY shape, got {xy_shape}")

    modalities: list[dict[str, object]] = []
    for key, display_name, rgb in MODALITY_SPECS:
        raw = raw_xy[key]
        norm = normalize_for_display(raw, clamp_negative=(key in {"round1", "round2", "round3"}))
        if key == "anatomy":
            single_rgb = np.repeat(norm[..., None], 3, axis=2)
        else:
            single_rgb = pseudocolor(norm, rgb)
        modalities.append(
            {
                "key": key,
                "display_name": display_name,
                "rgb": rgb,
                "path": paths[key],
                "raw_xy": raw,
                "norm01": norm,
                "single_rgb": np.asarray(single_rgb, dtype=np.float32),
            }
        )
    return modalities, plane_label, best_z, peak


def build_merged_rgb(modalities: list[dict[str, object]]) -> np.ndarray:
    if not modalities:
        raise ValueError("No modalities provided for merge")
    shape = np.asarray(modalities[0]["norm01"]).shape  # type: ignore[arg-type]
    merged = np.zeros((shape[0], shape[1], 3), dtype=np.float32)
    for modality in modalities:
        norm01 = np.asarray(modality["norm01"], dtype=np.float32)
        rgb = np.asarray(modality["rgb"], dtype=np.float32)
        if modality["key"] == "anatomy":
            merged += np.repeat(norm01[..., None], 3, axis=2)
        else:
            merged += pseudocolor(norm01, rgb)
    return np.clip(merged, 0.0, 1.0)


def render_figure(
    modalities: list[dict[str, object]],
    fish_id: str,
    plane_label: str,
    best_z: int,
    peak: float,
) -> plt.Figure:
    merged_rgb = build_merged_rgb(modalities)

    fig = plt.figure(figsize=(13.5, 11.0), constrained_layout=False)
    fig.patch.set_facecolor("white")
    gs = GridSpec(
        5,
        6,
        figure=fig,
        left=0.03,
        right=0.985,
        top=0.965,
        bottom=0.06,
        wspace=0.02,
        hspace=0.02,
    )

    for row_idx, modality in enumerate(modalities):
        ax = fig.add_subplot(gs[row_idx, 0])
        ax.imshow(np.asarray(modality["single_rgb"]), interpolation="nearest")
        ax.set_axis_off()
        add_stroked_text(
            ax,
            0.04,
            0.96,
            str(modality["display_name"]),
            label_color(np.asarray(modality["rgb"])),
        )

    merge_ax = fig.add_subplot(gs[:, 1:])
    merge_ax.imshow(merged_rgb, interpolation="nearest")
    merge_ax.set_axis_off()
    merge_ax.set_box_aspect(1)
    label_x = 0.03
    label_y0 = 0.97
    label_step = 0.057
    for line_idx, modality in enumerate(modalities):
        add_stroked_text(
            merge_ax,
            label_x,
            label_y0 - line_idx * label_step,
            str(modality["display_name"]),
            label_color(np.asarray(modality["rgb"])),
        )

    fig.text(
        0.03,
        0.02,
        f"{fish_id} | {plane_label} | z={best_z} | peak_ncc={peak:.4f}",
        ha="left",
        va="bottom",
        fontsize=9,
        family=FONT_FAMILY,
        color="#444444",
    )
    return fig


def main() -> None:
    args = parse_args()
    data_root = Path(args.data_root).expanduser().resolve()
    fish_id = args.fish_id
    functional_image_override = Path(args.functional_image).expanduser().resolve() if args.functional_image else None

    modalities, plane_label, best_z, peak = build_modalities(data_root, fish_id, functional_image_override)
    xy_shape = tuple(np.asarray(modalities[0]["raw_xy"]).shape)

    output_path = Path(args.output).expanduser() if args.output else default_output_path(data_root, fish_id)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    fig = render_figure(modalities, fish_id, plane_label, best_z, peak)
    fig.savefig(output_path, dpi=args.dpi, facecolor=fig.get_facecolor())
    plt.close(fig)

    print(f"fish_id={fish_id}")
    print(f"selected_plane={plane_label}")
    print(f"best_z={best_z}")
    print(f"peak_ncc={peak:.4f}")
    print(f"xy_shape={xy_shape}")
    print("inputs:")
    for modality in modalities:
        print(f"  {modality['key']}={modality['path']}")
    print("panel_order:")
    for modality in modalities:
        print(f"  {modality['display_name']}")
    print(f"output={output_path}")


if __name__ == "__main__":
    main()
