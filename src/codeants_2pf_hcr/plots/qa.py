"""QA figure builders used by tools wrappers and notebook-adjacent workflows."""

from __future__ import annotations

import csv
import json
import os
from pathlib import Path
import tempfile
from typing import Any
from functools import partial

_cache_root = tempfile.mkdtemp(prefix="mpl_cache_")
os.environ.setdefault("MPLCONFIGDIR", _cache_root)
os.environ.setdefault("XDG_CACHE_HOME", _cache_root)

import matplotlib

import matplotlib.pyplot as plt
from matplotlib import font_manager
from matplotlib.gridspec import GridSpec
import matplotlib.patheffects as path_effects
import numpy as np
import SimpleITK as sitk
from skimage import color as skcolor
from skimage import transform
import tifffile

from ..matching import _ensure_uint_labels
from ..segmentation import resolve_functional_labels_for_plane
from ..spatial import norm01


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


def _apply_overlay_color(gray01: np.ndarray, rgb: tuple[float, float, float]) -> np.ndarray:
    r, g, b = rgb
    return np.stack([gray01 * r, gray01 * g, gray01 * b], axis=-1)


def _emit_figure(fig: Any) -> None:
    try:
        from IPython.display import display
    except Exception:
        display = None
    try:
        if display is not None:
            display(fig)
        else:
            fig.canvas.draw()
    finally:
        plt.close(fig)


def _render_registration_overlay(
    *,
    overlay_items: list[dict[str, Any]],
    colors: dict[str, tuple[float, float, float]],
    show_func: bool = True,
    show_anat: bool = True,
    func_color: str = "green",
    anat_color: str = "magenta",
    func_alpha: float = 1.0,
    anat_alpha: float = 1.0,
    panel_w: float = 4.0,
    panel_h: float = 4.0,
) -> None:
    n_items = len(overlay_items)
    fig_w = max(2.0, float(panel_w) * n_items)
    fig, axes = plt.subplots(1, n_items, figsize=(fig_w, float(panel_h)))
    if n_items == 1:
        axes = [axes]
    for ax, item in zip(axes, overlay_items):
        out = np.zeros((item["f_vis"].shape[0], item["f_vis"].shape[1], 3), dtype=np.float32)
        if show_anat:
            out += _apply_overlay_color(item["a_vis"], colors[anat_color]) * float(anat_alpha)
        if show_func:
            out += _apply_overlay_color(item["f_vis"], colors[func_color]) * float(func_alpha)
        out = np.clip(out, 0, 1)
        ax.imshow(out)
        ax.set_title(f"{item['label']} z={item['z']} | func src: {item['src_label']}")
        ax.axis("off")
    _emit_figure(fig)


def show_registration_overlay_stage(
    *,
    plane_refs: list[dict[str, Any]] | None,
    anat: np.ndarray | None,
    best_z: int = 0,
    apply_transform_2d_func: Any = None,
) -> dict[str, Any]:
    log_lines: list[str] = []
    try:
        import ipywidgets as widgets
        from IPython.display import display

        has_widgets = True
    except Exception:
        widgets = None
        display = None
        has_widgets = False
        log_lines.append("ipywidgets not available; skipping interactive overlay. Install ipywidgets to enable.")

    overlay_items: list[dict[str, Any]] = []
    overlay_ready = True
    try:
        if not plane_refs:
            raise RuntimeError("plane_refs missing; run previous cells first.")
        if anat is None:
            raise RuntimeError("anat missing; run [16] first.")
        anat_arr = np.asarray(anat, dtype=np.float32)
        for plane_ref in plane_refs:
            if plane_ref is None:
                continue
            bz = int(plane_ref.get("best_z", best_z))
            a_src = anat_arr[bz]
            src_label = "raw"
            f_src = None
            if plane_ref.get("ref_warped") is not None:
                f_src = plane_ref.get("ref_warped")
                src_label = "warped"
            elif plane_ref.get("tform") is not None and callable(apply_transform_2d_func):
                mov_src = plane_ref.get("ref_match", plane_ref.get("ref2d_raw", plane_ref.get("ref2d")))
                if mov_src is not None:
                    f_src = apply_transform_2d_func(mov_src, plane_ref["tform"], output_shape=a_src.shape, order=1)
                    src_label = "tform-preview"
            elif plane_ref.get("ref_warped_raw") is not None:
                f_src = plane_ref.get("ref_warped_raw")
                src_label = "warped-raw"
            else:
                f_src = plane_ref.get("ref2d", plane_ref.get("ref2d_raw"))
                src_label = "raw"
            if f_src is None:
                continue
            f_vis = norm01(f_src)
            a_vis = norm01(a_src)
            if f_vis.shape != a_vis.shape:
                f_vis = transform.resize(
                    f_vis,
                    a_vis.shape,
                    order=1,
                    mode="reflect",
                    preserve_range=True,
                    anti_aliasing=True,
                ).astype(np.float32)
            overlay_items.append(
                {
                    "f_vis": f_vis,
                    "a_vis": a_vis,
                    "label": plane_ref.get("label", "plane"),
                    "z": bz,
                    "src_label": src_label,
                }
            )
        if not overlay_items:
            raise RuntimeError("No overlay items prepared; check inputs.")
    except Exception:
        overlay_ready = False
        log_lines.append("Interactive overlay prerequisites missing (plane_refs/anat/best_z). Run previous cells first.")

    colors = {
        "green": (0.0, 1.0, 0.0),
        "magenta": (1.0, 0.0, 1.0),
        "red": (1.0, 0.0, 0.0),
        "blue": (0.0, 0.0, 1.0),
        "cyan": (0.0, 1.0, 1.0),
        "yellow": (1.0, 1.0, 0.0),
        "white": (1.0, 1.0, 1.0),
    }

    if has_widgets and overlay_ready and widgets is not None and display is not None:
        render_func = partial(_render_registration_overlay, overlay_items=overlay_items, colors=colors)
        show_func_cb = widgets.Checkbox(value=True, description="Show functional")
        show_anat_cb = widgets.Checkbox(value=True, description="Show anatomy")
        func_color_dd = widgets.Dropdown(options=list(colors.keys()), value="green", description="Func LUT")
        anat_color_dd = widgets.Dropdown(options=list(colors.keys()), value="magenta", description="Anat LUT")
        func_alpha_sl = widgets.FloatSlider(value=1.0, min=0.0, max=1.0, step=0.05, readout_format=".2f", description="Func alpha")
        anat_alpha_sl = widgets.FloatSlider(value=1.0, min=0.0, max=1.0, step=0.05, readout_format=".2f", description="Anat alpha")
        panel_w_sl = widgets.FloatSlider(value=8.0, min=2.0, max=10.0, step=0.5, readout_format=".1f", description="Panel W")
        panel_h_sl = widgets.FloatSlider(value=8.0, min=2.0, max=10.0, step=0.5, readout_format=".1f", description="Panel H")
        ui = widgets.VBox(
            [
                widgets.HBox([show_func_cb, func_color_dd, func_alpha_sl]),
                widgets.HBox([show_anat_cb, anat_color_dd, anat_alpha_sl, panel_w_sl, panel_h_sl]),
            ]
        )
        out = widgets.interactive_output(
            render_func,
            {
                "show_func": show_func_cb,
                "show_anat": show_anat_cb,
                "func_color": func_color_dd,
                "anat_color": anat_color_dd,
                "func_alpha": func_alpha_sl,
                "anat_alpha": anat_alpha_sl,
                "panel_w": panel_w_sl,
                "panel_h": panel_h_sl,
            },
        )
        display(ui, out)
    elif has_widgets and not overlay_ready:
        log_lines.append("Interactive overlay not shown: run best-Z cell first.")

    return {
        "overlay_ready": overlay_ready,
        "overlay_items": overlay_items,
        "has_widgets": has_widgets,
        "log_lines": log_lines,
    }


def show_functional_label_overlay_stage(
    *,
    plane_refs: list[dict[str, Any]] | None,
    use_suite2p_labels: bool = False,
    func_labels: Any = None,
    out_seg: str | Path | None = None,
    func_labels_path: str | Path | None = None,
    apply_func_orientation_func: Any = None,
    imread_func: Any = None,
) -> dict[str, Any]:
    if not plane_refs:
        return {"rendered": 0, "log_lines": ["No functional references to plot."]}

    log_lines: list[str] = []
    rendered = 0
    for plane_idx, plane_ref in enumerate(plane_refs):
        label = plane_ref.get("label", f"plane{plane_idx}")
        labels, src_desc = resolve_functional_labels_for_plane(
            plane_ref,
            plane_idx,
            use_suite2p_labels=bool(use_suite2p_labels),
            func_labels=func_labels,
            out_seg=out_seg,
            func_labels_path=func_labels_path,
            apply_func_orientation_func=apply_func_orientation_func,
            imread_func=imread_func,
            ensure_uint_labels_func=_ensure_uint_labels,
        )
        if labels is None:
            log_lines.append(f"[SKIP] No labels found for {label}")
            continue
        if labels.ndim == 3 and labels.shape[-1] in (3, 4):
            labels = labels[..., 0]
        if labels.ndim != 2:
            log_lines.append(f"[SKIP] Labels for {label} have unsupported shape {labels.shape}")
            continue
        ref_img = plane_ref.get("ref_match")
        if ref_img is None:
            ref_img = plane_ref.get("ref2d_raw", plane_ref.get("ref2d"))
        if ref_img is None:
            log_lines.append(f"[SKIP] No functional reference for {label}")
            continue
        if ref_img.ndim == 3 and ref_img.shape[-1] in (3, 4):
            ref_img = ref_img[..., 0]
        if labels.shape != ref_img.shape:
            log_lines.append(f"[SKIP] Label/ref shape mismatch for {label}: labels {labels.shape}, ref {ref_img.shape}")
            continue
        overlay = skcolor.label2rgb(labels, image=norm01(ref_img), bg_label=0, alpha=0.35, image_alpha=1.0)
        fig, ax = plt.subplots(figsize=(6, 6))
        ax.imshow(overlay)
        title_src = src_desc if src_desc else "labels"
        ax.set_title(f"Functional labels on reference - {label} {title_src}")
        ax.axis("off")
        _emit_figure(fig)
        rendered += 1
    return {"rendered": rendered, "log_lines": log_lines}


__all__ = [
    "build_best_plane_modality_merge_grid",
    "build_round_channel_mip_grid",
    "show_functional_label_overlay_stage",
    "show_registration_overlay_stage",
]
