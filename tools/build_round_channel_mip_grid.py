#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
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
import numpy as np
import SimpleITK as sitk
import tifffile


DEFAULT_DATA_ROOT = Path("/Users/ddharmap/dataProcessing/2p_HCR/analysis/midThesis")

PANEL_LAYOUT = [
    [(1, 1, "GCaMP"), (2, 1, "GCaMP"), (3, 1, "GCaMP")],
    [(1, 2, "sst1_1"), (2, 2, "sst1_2"), (3, 2, "npy")],
    [(1, 3, "pth2"), (2, 3, "tac3b"), (3, 3, "cfos")],
]

DISPLAY_NAME = {
    "sst1_1": "sst1.1",
    "sst1_2": "sst1.2",
}

ROW_LUTS = [
    np.array([0.0, 1.0, 0.0], dtype=np.float32),
    np.array([1.0, 1.0, 0.0], dtype=np.float32),
    np.array([1.0, 0.0, 0.0], dtype=np.float32),
]


def pick_font_family() -> str:
    installed = {f.name for f in font_manager.fontManager.ttflist}
    if "Aptos" in installed:
        return "Aptos"
    return "DejaVu Sans"


FONT_FAMILY = pick_font_family()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a 3x3 round/channel 2P-space MIP composite from aligned confocal volumes."
    )
    parser.add_argument("--fish-id", required=True, help="Fish identifier, e.g. L396_f04.")
    parser.add_argument(
        "--data-root",
        default=str(DEFAULT_DATA_ROOT),
        help=f"Root analysis directory. Default: {DEFAULT_DATA_ROOT}",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Output image path. Defaults to <fish>/03_analysis/functional/qa/<fish>_round_channel_2p_mip_grid.png",
    )
    parser.add_argument("--z-min", type=int, default=None, help="Inclusive minimum Z slice.")
    parser.add_argument("--z-max", type=int, default=None, help="Inclusive maximum Z slice.")
    return parser.parse_args()


def norm01(img: np.ndarray) -> np.ndarray:
    img = np.asarray(img, dtype=np.float32)
    m, M = np.percentile(img, (1, 99))
    if M <= m:
        M = float(img.max())
        m = float(img.min())
    return np.clip((img - m) / (M - m + 1e-6), 0.0, 1.0)


def read_volume(path: Path) -> np.ndarray:
    suffixes = [s.lower() for s in path.suffixes]
    if suffixes and suffixes[-1] in {".tif", ".tiff"}:
        return np.asarray(tifffile.imread(path), dtype=np.float32)
    if suffixes and suffixes[-1] == ".nrrd":
        return np.asarray(sitk.GetArrayFromImage(sitk.ReadImage(str(path))), dtype=np.float32)
    raise ValueError(f"Unsupported volume format: {path}")


def aligned_dir_for_fish(data_root: Path, fish_id: str) -> Path:
    return data_root / fish_id / "03_analysis" / "confocal" / "aligned"


def qa_dir_for_fish(data_root: Path, fish_id: str) -> Path:
    return data_root / fish_id / "03_analysis" / "functional" / "qa"


def tforms_csv_for_fish(data_root: Path, fish_id: str) -> Path:
    return data_root / fish_id / "03_analysis" / "functional" / "ncc" / "tforms_by_plane.csv"


def default_output_path(data_root: Path, fish_id: str) -> Path:
    return qa_dir_for_fish(data_root, fish_id) / f"{fish_id}_round_channel_2p_mip_grid.png"


def aligned_volume_path(aligned_dir: Path, fish_id: str, round_idx: int, channel_idx: int, gene_name: str) -> Path:
    return aligned_dir / f"{fish_id}_round{round_idx}_channel{channel_idx}_{gene_name}_in_2p.nrrd"


def derive_z_window_from_csv(csv_path: Path) -> tuple[int, int]:
    if not csv_path.exists():
        raise FileNotFoundError(f"Missing tforms_by_plane.csv: {csv_path}")

    best_z_values: list[int] = []
    with csv_path.open("r", newline="") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise ValueError(f"Malformed CSV with no header: {csv_path}")
        if "best_z" not in reader.fieldnames:
            raise ValueError(f"Missing required 'best_z' column in {csv_path}")
        for row_idx, row in enumerate(reader, start=2):
            raw = row.get("best_z")
            if raw is None or str(raw).strip() == "":
                raise ValueError(f"Missing best_z value at {csv_path}:{row_idx}")
            try:
                best_z_values.append(int(float(raw)))
            except ValueError as exc:
                raise ValueError(f"Invalid best_z value '{raw}' at {csv_path}:{row_idx}") from exc

    if not best_z_values:
        raise ValueError(f"No best_z values found in {csv_path}")
    return min(best_z_values), max(best_z_values)


def resolve_z_window(args: argparse.Namespace, csv_path: Path) -> tuple[int, int]:
    has_override_min = args.z_min is not None
    has_override_max = args.z_max is not None
    if has_override_min != has_override_max:
        raise ValueError("Provide both --z-min and --z-max together to override the CSV-derived Z window.")
    if has_override_min and has_override_max:
        if args.z_max < args.z_min:
            raise ValueError(f"Invalid Z override: z_max ({args.z_max}) is smaller than z_min ({args.z_min}).")
        return args.z_min, args.z_max
    return derive_z_window_from_csv(csv_path)


def build_panel_specs(data_root: Path, fish_id: str) -> list[list[dict[str, object]]]:
    aligned_dir = aligned_dir_for_fish(data_root, fish_id)
    panel_specs: list[list[dict[str, object]]] = []
    for row in PANEL_LAYOUT:
        spec_row: list[dict[str, object]] = []
        for round_idx, channel_idx, gene_name in row:
            path = aligned_volume_path(aligned_dir, fish_id, round_idx, channel_idx, gene_name)
            if not path.exists():
                raise FileNotFoundError(f"Missing aligned stack: {path}")
            spec_row.append(
                {
                    "round_idx": round_idx,
                    "channel_idx": channel_idx,
                    "gene_name": gene_name,
                    "display_name": DISPLAY_NAME.get(gene_name, gene_name),
                    "path": path,
                }
            )
        panel_specs.append(spec_row)
    return panel_specs


def volume_mip(arr: np.ndarray, z_min: int, z_max: int) -> np.ndarray:
    if arr.ndim != 3:
        raise ValueError(f"Expected a 3D volume, got shape {arr.shape}")
    z_size = int(arr.shape[0])
    if z_min < 0 or z_max >= z_size:
        raise ValueError(f"Z window {z_min}..{z_max} is out of bounds for volume shape {arr.shape}")
    return np.asarray(arr[z_min : z_max + 1].max(axis=0), dtype=np.float32)


def pseudocolor(img: np.ndarray, rgb: np.ndarray) -> np.ndarray:
    out = np.zeros((img.shape[0], img.shape[1], 3), dtype=np.float32)
    out[..., 0] = img * rgb[0]
    out[..., 1] = img * rgb[1]
    out[..., 2] = img * rgb[2]
    return out


def render_figure(panel_specs: list[list[dict[str, object]]], z_min: int, z_max: int) -> tuple[plt.Figure, tuple[int, int, int]]:
    volumes: list[np.ndarray] = []
    for row in panel_specs:
        for spec in row:
            volumes.append(read_volume(spec["path"]))  # type: ignore[arg-type]

    shape_set = {tuple(v.shape) for v in volumes}
    if len(shape_set) != 1:
        raise ValueError(f"Mismatched volume shapes across aligned stacks: {sorted(shape_set)}")
    volume_shape = next(iter(shape_set))

    colored_panels: list[list[np.ndarray]] = []
    for row_idx, row in enumerate(panel_specs):
        colored_row: list[np.ndarray] = []
        for spec in row:
            arr = read_volume(spec["path"])  # type: ignore[arg-type]
            mip = volume_mip(arr, z_min, z_max)
            colored_row.append(pseudocolor(norm01(mip), ROW_LUTS[row_idx]))
        colored_panels.append(colored_row)

    fig = plt.figure(figsize=(12.0, 12.0), constrained_layout=False)
    fig.patch.set_facecolor("white")
    gs = GridSpec(
        4,
        4,
        figure=fig,
        height_ratios=[0.14, 1.0, 1.0, 1.0],
        width_ratios=[0.14, 1.0, 1.0, 1.0],
        left=0.035,
        right=0.985,
        top=0.975,
        bottom=0.03,
        wspace=0.02,
        hspace=0.02,
    )

    corner_ax = fig.add_subplot(gs[0, 0])
    corner_ax.set_axis_off()
    corner_ax.set_facecolor("#f0f0f0")

    header_face = "#f0f0f0"
    for col_idx in range(3):
        ax = fig.add_subplot(gs[0, col_idx + 1])
        ax.set_axis_off()
        ax.set_facecolor(header_face)
        ax.text(
            0.5,
            0.5,
            f"Round {col_idx + 1}",
            ha="center",
            va="center",
            fontsize=15,
            fontweight="bold",
            family=FONT_FAMILY,
        )

    for row_idx in range(3):
        ax = fig.add_subplot(gs[row_idx + 1, 0])
        ax.set_axis_off()
        ax.set_facecolor(header_face)
        ax.text(
            0.5,
            0.5,
            f"Channel {row_idx + 1}",
            ha="center",
            va="center",
            rotation=90,
            fontsize=15,
            fontweight="bold",
            family=FONT_FAMILY,
        )

    for row_idx, row in enumerate(colored_panels):
        for col_idx, rgb_img in enumerate(row):
            ax = fig.add_subplot(gs[row_idx + 1, col_idx + 1])
            ax.imshow(rgb_img, interpolation="nearest")
            ax.set_axis_off()
            spec = panel_specs[row_idx][col_idx]
            fontstyle = "normal" if row_idx == 0 else "italic"
            ax.text(
                0.03,
                0.97,
                spec["display_name"],  # type: ignore[index]
                ha="left",
                va="top",
                transform=ax.transAxes,
                fontsize=14,
                fontstyle=fontstyle,
                fontweight="semibold",
                family=FONT_FAMILY,
                color="white",
                bbox={
                    "boxstyle": "round,pad=0.2",
                    "facecolor": (0.0, 0.0, 0.0, 0.35),
                    "edgecolor": "none",
                },
            )

    return fig, volume_shape


def main() -> None:
    args = parse_args()
    data_root = Path(args.data_root).expanduser().resolve()
    fish_id = args.fish_id
    csv_path = tforms_csv_for_fish(data_root, fish_id)
    z_min, z_max = resolve_z_window(args, csv_path)
    if fish_id == "L396_f04" and args.z_min is None and args.z_max is None and (z_min, z_max) != (108, 125):
        raise ValueError(f"Expected auto-derived Z window 108..125 for {fish_id}, got {z_min}..{z_max}")

    panel_specs = build_panel_specs(data_root, fish_id)
    fig, volume_shape = render_figure(panel_specs, z_min, z_max)

    output_path = Path(args.output).expanduser() if args.output else default_output_path(data_root, fish_id)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=300, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close(fig)

    print(f"fish_id={fish_id}")
    print(f"tforms_csv={csv_path}")
    print(f"z_window={z_min}..{z_max}")
    print(f"volume_shape={volume_shape}")
    print("panel_order:")
    for row in panel_specs:
        print(
            " | ".join(
                f"round{spec['round_idx']}/channel{spec['channel_idx']}/{spec['display_name']}" for spec in row
            )
        )
    print(f"output={output_path}")


if __name__ == "__main__":
    main()
