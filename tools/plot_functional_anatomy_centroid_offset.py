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
import numpy as np
import pandas as pd
import tifffile
from skimage.measure import regionprops_table


DEFAULT_DATA_ROOT = Path("/Users/ddharmap/dataProcessing/2p_HCR/analysis/midThesis")
XY_COLOR = "#4c78a8"
Z_COLOR = "#f58518"
REF_COLOR = "#c1121f"
FIG_WIDTH = 8.5
FIG_HEIGHT = 2.75


def pick_font_family() -> str:
    installed = {f.name for f in font_manager.fontManager.ttflist}
    if "Aptos" in installed:
        return "Aptos"
    return "DejaVu Sans"


FONT_FAMILY = pick_font_family()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Plot anatomical-functional centroid offsets from fish-scoped registration outputs."
    )
    parser.add_argument("--fish-id", required=True, help="Fish identifier, e.g. L395_f11.")
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
            "<data-root>/<fish>/03_analysis/functional/qa/<fish>_functional_anatomy_centroid_offset.png"
        ),
    )
    parser.add_argument("--dpi", type=int, default=300, help="Output DPI. Default: 300.")
    args = parser.parse_args()
    if args.dpi <= 0:
        raise ValueError("--dpi must be > 0.")
    return args


def fish_root(data_root: Path, fish_id: str) -> Path:
    return data_root / fish_id / "03_analysis"


def default_output_path(data_root: Path, fish_id: str) -> Path:
    return fish_root(data_root, fish_id) / "functional" / "qa" / f"{fish_id}_functional_anatomy_centroid_offset.png"


def roi_table_path(data_root: Path, fish_id: str) -> Path:
    return fish_root(data_root, fish_id) / "functional" / "registration" / "functional_roi_activity_identity.csv"


def diameters_path(data_root: Path, fish_id: str) -> Path:
    return fish_root(data_root, fish_id) / "functional" / "qa" / "diameters_df_all.pkl"


def run_metadata_path(data_root: Path, fish_id: str) -> Path:
    return fish_root(data_root, fish_id) / "functional" / "registration" / "run_metadata.json"


def anatomy_labels_path(data_root: Path, fish_id: str) -> Path:
    return fish_root(data_root, fish_id) / "structural" / "cp_masks" / f"{fish_id}_anatomy_00001_8bit_cp_masks.tif"


def require_file(path: Path, label: str) -> Path:
    if not path.exists():
        raise FileNotFoundError(f"Missing {label}: {path}")
    return path


def as_bool_series(series: pd.Series) -> pd.Series:
    if pd.api.types.is_bool_dtype(series):
        return series.fillna(False).astype(bool)
    if pd.api.types.is_numeric_dtype(series):
        return series.fillna(0).astype(float) != 0
    return series.astype(str).str.strip().str.lower().isin({"1", "true", "t", "yes", "y"})


def load_voxels_anat_um(run_metadata_json: Path) -> dict[str, float]:
    meta = json.loads(run_metadata_json.read_text())
    voxels = meta.get("voxels", {})
    anat = voxels.get("anat", voxels)
    if not isinstance(anat, dict):
        raise ValueError(f"Malformed anatomy voxel metadata in {run_metadata_json}")
    out: dict[str, float] = {}
    for axis in ("X", "Y", "Z"):
        raw = anat.get(axis)
        if raw is None:
            raise ValueError(f"Missing voxels.anat.{axis} in {run_metadata_json}")
        out[axis] = float(raw)
    return out


def apply_modality_xy_filter(
    df_in: pd.DataFrame,
    low_quantile: float = 0.05,
    high_quantile: float = 0.95,
    low_conf_col: str = "is_low_confidence_segmentation",
) -> pd.DataFrame:
    if df_in.empty:
        return df_in.copy()
    required = {"dataset", "x_um", "y_um"}
    if not required.issubset(df_in.columns):
        raise ValueError(f"Diameter dataframe missing required columns: {sorted(required)}")

    parts: list[pd.DataFrame] = []
    for dataset in df_in["dataset"].astype(str).unique():
        sub = df_in[df_in["dataset"].astype(str) == str(dataset)].copy()
        xy_vals = sub[["x_um", "y_um"]].mean(axis=1).to_numpy(dtype=float)
        finite = np.isfinite(xy_vals)
        finite_vals = xy_vals[finite]
        keep = np.ones(len(sub), dtype=bool)
        low_conf_mask = np.zeros(len(sub), dtype=bool)
        if finite_vals.size:
            cutoff_low = float(np.quantile(finite_vals, low_quantile))
            cutoff_high = float(np.quantile(finite_vals, high_quantile))
            finite_idx = np.where(finite)[0]
            keep[finite_idx] = finite_vals >= cutoff_low
            low_conf_mask[finite_idx] = finite_vals > cutoff_high
        sub["xy_um"] = xy_vals
        sub[low_conf_col] = low_conf_mask
        parts.append(sub.iloc[keep].copy())
    return pd.concat(parts, ignore_index=True) if parts else df_in.iloc[0:0].copy()


def anatomy_reference_thresholds_um(diameter_pkl: Path) -> tuple[float, float]:
    df_raw = pd.read_pickle(diameter_pkl)
    df_filt = apply_modality_xy_filter(df_raw)
    anat = df_filt[df_filt["dataset"].astype(str) == "Anatomy"].copy()
    if anat.empty:
        raise ValueError(f"No Anatomy rows in {diameter_pkl}")

    xy = anat[["x_um", "y_um"]].mean(axis=1).to_numpy(dtype=float)
    z = pd.to_numeric(anat["z_um"], errors="coerce").to_numpy(dtype=float)
    xy = xy[np.isfinite(xy)]
    z = z[np.isfinite(z)]
    if xy.size == 0 or z.size == 0:
        raise ValueError(f"Anatomy diameter rows in {diameter_pkl} do not contain finite XY/Z values")

    return float(np.median(xy) / 2.0), float(np.median(z) / 2.0)


def load_roi_subset(roi_csv: Path) -> pd.DataFrame:
    df = pd.read_csv(roi_csv)
    required = {"plane_match_outcome", "has_unique_anat_match", "selected_dist_um", "selected_anat_label", "best_z"}
    missing = sorted(required - set(df.columns))
    if missing:
        raise ValueError(f"ROI table missing required columns {missing}: {roi_csv}")

    mask = df["plane_match_outcome"].astype(str).eq("anatomy match")
    mask &= as_bool_series(df["has_unique_anat_match"])
    matched = df.loc[mask].copy()
    if matched.empty:
        raise ValueError(f"No ROI-centric anatomy matches after filtering in {roi_csv}")
    return matched


def compute_xy_offsets_um(roi_df: pd.DataFrame) -> np.ndarray:
    xy = pd.to_numeric(roi_df["selected_dist_um"], errors="coerce").to_numpy(dtype=float)
    xy = np.abs(xy[np.isfinite(xy)])
    if xy.size == 0:
        raise ValueError("No finite XY offsets found in selected_dist_um")
    return xy


def compute_z_offsets_um(roi_df: pd.DataFrame, anat_labels_tif: Path, dz_um: float) -> np.ndarray:
    anat_vol = np.asarray(tifffile.imread(anat_labels_tif))
    props = regionprops_table(anat_vol, properties=("label", "centroid"))
    anat_df = pd.DataFrame(props)
    if anat_df.empty:
        raise ValueError(f"No labeled anatomy regions found in {anat_labels_tif}")

    z_centroids = dict(
        zip(
            anat_df["label"].astype(int),
            pd.to_numeric(anat_df["centroid-0"], errors="coerce"),
        )
    )

    matched = roi_df[["selected_anat_label", "best_z"]].copy()
    matched["selected_anat_label"] = pd.to_numeric(matched["selected_anat_label"], errors="coerce")
    matched["best_z"] = pd.to_numeric(matched["best_z"], errors="coerce")
    matched = matched.dropna(subset=["selected_anat_label", "best_z"])

    z_vals: list[float] = []
    for row in matched.itertuples(index=False):
        anat_label = int(row.selected_anat_label)
        anatomy_centroid_z = z_centroids.get(anat_label)
        if anatomy_centroid_z is None or not np.isfinite(anatomy_centroid_z):
            continue
        z_vals.append(abs(float(anatomy_centroid_z) - float(row.best_z)) * float(dz_um))

    z = np.asarray(z_vals, dtype=float)
    z = z[np.isfinite(z)]
    if z.size == 0:
        raise ValueError("No finite Z offsets computed from anatomy centroids and best_z")
    return z


def shared_ymax(values: list[np.ndarray], refs: list[float]) -> float:
    combined = [np.asarray(v, dtype=float) for v in values]
    combined_refs = [float(v) for v in refs if np.isfinite(v)]
    finite_vals = [arr[np.isfinite(arr)] for arr in combined if arr.size]
    maxima: list[float] = []
    for arr in finite_vals:
        if arr.size:
            maxima.append(float(np.max(arr)))
    maxima.extend(combined_refs)
    if not maxima:
        return 1.0
    ymax = max(maxima)
    return max(1.0, ymax * 1.16 + 0.3)


def style_axes() -> None:
    plt.rcParams["font.family"] = "sans-serif"
    plt.rcParams["font.sans-serif"] = [FONT_FAMILY, "Aptos (Body)", "Calibri", "Arial", "DejaVu Sans"]
    plt.rcParams["axes.titlesize"] = 11
    plt.rcParams["axes.labelsize"] = 10
    plt.rcParams["xtick.labelsize"] = 9
    plt.rcParams["ytick.labelsize"] = 9


def draw_panel(ax: plt.Axes, values: np.ndarray, color: str, axis_label: str, ref_r50_um: float, ymax: float) -> None:
    vp = ax.violinplot([values], positions=[1], widths=0.7, showmeans=False, showmedians=False, showextrema=False)
    for body in vp["bodies"]:
        body.set_facecolor(color)
        body.set_edgecolor("black")
        body.set_alpha(0.75)

    median = float(np.median(values))
    ax.scatter([1], [median], color="black", s=26, zorder=3)
    ax.hlines(ref_r50_um, 0.72, 1.28, colors=REF_COLOR, linestyles="--", linewidth=1.4)
    ax.text(
        1.32,
        ref_r50_um,
        f"Anat R50={ref_r50_um:.4g}",
        color=REF_COLOR,
        fontsize=8,
        va="center",
        ha="left",
    )
    ax.text(
        0.06,
        0.95,
        f"n={int(values.size)}",
        transform=ax.transAxes,
        ha="left",
        va="top",
        color=color,
        fontsize=10,
        fontweight="semibold",
    )
    ax.set_xlim(0.5, 1.65)
    ax.set_xticks([1])
    ax.set_xticklabels([axis_label])
    ax.set_ylim(0.0, ymax)
    ax.grid(axis="y", alpha=0.25)


def render_figure(
    xy_offsets_um: np.ndarray,
    z_offsets_um: np.ndarray,
    anat_r50_xy_um: float,
    anat_r50_z_um: float,
) -> plt.Figure:
    style_axes()
    ymax = shared_ymax([xy_offsets_um, z_offsets_um], [anat_r50_xy_um, anat_r50_z_um])
    fig, axes = plt.subplots(1, 2, figsize=(FIG_WIDTH, FIG_HEIGHT), sharey=True, constrained_layout=False)
    fig.patch.set_facecolor("white")

    draw_panel(axes[0], xy_offsets_um, XY_COLOR, "XY", anat_r50_xy_um, ymax)
    draw_panel(axes[1], z_offsets_um, Z_COLOR, "Z", anat_r50_z_um, ymax)
    axes[0].set_ylabel("Distance (\u00b5m)")

    fig.suptitle("Anatomical-Functional centroid offset", fontsize=13, family=FONT_FAMILY, y=0.98)
    fig.subplots_adjust(left=0.1, right=0.98, top=0.88, bottom=0.12, wspace=0.12)
    return fig


def main() -> None:
    args = parse_args()
    data_root = Path(args.data_root).expanduser().resolve()
    fish_id = args.fish_id

    roi_csv = require_file(roi_table_path(data_root, fish_id), "ROI table")
    diameter_pkl = require_file(diameters_path(data_root, fish_id), "diameter cache")
    metadata_json = require_file(run_metadata_path(data_root, fish_id), "run metadata")
    anatomy_tif = require_file(anatomy_labels_path(data_root, fish_id), "anatomy label TIFF")

    voxels_anat_um = load_voxels_anat_um(metadata_json)
    roi_df = load_roi_subset(roi_csv)
    xy_offsets_um = compute_xy_offsets_um(roi_df)
    z_offsets_um = compute_z_offsets_um(roi_df, anatomy_tif, dz_um=voxels_anat_um["Z"])
    anat_r50_xy_um, anat_r50_z_um = anatomy_reference_thresholds_um(diameter_pkl)

    fig = render_figure(
        xy_offsets_um=xy_offsets_um,
        z_offsets_um=z_offsets_um,
        anat_r50_xy_um=anat_r50_xy_um,
        anat_r50_z_um=anat_r50_z_um,
    )

    output_path = Path(args.output).expanduser() if args.output else default_output_path(data_root, fish_id)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=args.dpi, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close(fig)

    print(f"output={output_path}")
    print(
        "summary="
        f"fish_id={fish_id} "
        f"n_xy={xy_offsets_um.size} "
        f"n_z={z_offsets_um.size} "
        f"anat_r50_xy_um={anat_r50_xy_um:.10f} "
        f"anat_r50_z_um={anat_r50_z_um:.10f} "
        f"median_xy_um={float(np.median(xy_offsets_um)):.10f} "
        f"median_z_um={float(np.median(z_offsets_um)):.10f}"
    )


if __name__ == "__main__":
    main()
