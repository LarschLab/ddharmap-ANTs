"""QA figure builders used by tools wrappers and notebook-adjacent workflows."""

from __future__ import annotations

import csv
import json
import os
from datetime import datetime, timezone
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
from matplotlib import colors as mcolors
import numpy as np
import pandas as pd
import SimpleITK as sitk
from skimage import color as skcolor
from skimage import transform
import tifffile

from ..context import infer_anat_labels_path
from ..matching import _ensure_uint_labels, _regionprops_centroids_2d, build_plane_centroid_matches
from ..matching import compute_centroids
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


def _read_json_payload(path: Path) -> dict[str, Any] | None:
    try:
        if not path.exists():
            return None
        payload = json.loads(path.read_text())
        return payload if isinstance(payload, dict) else None
    except Exception:
        return None


def _square_bounds(*, cx: int, cy: int, size_px: int, shape: tuple[int, int]) -> tuple[int, int, int, int]:
    h, w = int(shape[0]), int(shape[1])
    size_px = int(max(4, min(int(size_px), min(h, w))))
    half = float(size_px) / 2.0
    x0 = int(round(float(cx) - half))
    y0 = int(round(float(cy) - half))
    x0 = max(0, min(x0, w - size_px))
    y0 = max(0, min(y0, h - size_px))
    return x0, y0, x0 + size_px, y0 + size_px


def _apply_zoom(ax: Any, *, shape: tuple[int, int], zoom: float, center_xy: tuple[float, float]) -> None:
    if (not np.isfinite(zoom)) or float(zoom) <= 1.0:
        return
    h, w = int(shape[0]), int(shape[1])
    cx, cy = float(center_xy[0]), float(center_xy[1])
    half_w = max(8.0, float(w) / (2.0 * float(zoom)))
    half_h = max(8.0, float(h) / (2.0 * float(zoom)))
    x0 = max(0.0, cx - half_w)
    x1 = min(float(w), cx + half_w)
    y0 = max(0.0, cy - half_h)
    y1 = min(float(h), cy + half_h)
    ax.set_xlim(x0, x1)
    ax.set_ylim(y1, y0)


def show_region_shift_square_selector_stage(
    *,
    plane_refs: list[dict[str, Any]] | None,
    anat_stack: np.ndarray | None,
    fish_id: str = "",
    out_reg: str | Path | None = None,
    apply_transform_2d_func: Any = None,
    save_square: bool = True,
    reuse_saved_square: bool = True,
    default_size_px: int = 120,
    func_alpha: float = 0.90,
    anat_alpha: float = 0.75,
    zoom: float = 1.0,
    primary_json_name: str = "regional_match_qa_square.json",
    legacy_json_name: str = "regional_shift_square.json",
) -> dict[str, Any]:
    log_lines: list[str] = []
    if not plane_refs:
        log_lines.append("[region-qa] plane_refs missing; run [16] first.")
        return {"ok": False, "log_lines": log_lines}
    if anat_stack is None:
        log_lines.append("[region-qa] anatomy stack missing; run [16] first.")
        return {"ok": False, "log_lines": log_lines}
    if out_reg is None:
        log_lines.append("[region-qa] OUT_REG missing; cannot resolve save path.")
        return {"ok": False, "log_lines": log_lines}

    out_reg_path = Path(out_reg)
    square_json_path = out_reg_path / primary_json_name
    legacy_json_path = out_reg_path / legacy_json_name
    loaded_spec = None
    loaded_path = None
    if reuse_saved_square:
        for cand in (square_json_path, legacy_json_path):
            payload = _read_json_payload(cand)
            if payload is not None:
                loaded_spec = payload
                loaded_path = cand
                log_lines.append(f"[region-qa] loaded existing square: {cand}")
                break

    anat_arr = np.asarray(anat_stack, dtype=np.float32)
    anat_shape = tuple(anat_arr.shape[1:])
    payload: list[dict[str, Any]] = []
    for plane_idx, plane_ref in enumerate(plane_refs):
        best_z = int(plane_ref.get("best_z", plane_idx))
        if best_z < 0 or best_z >= int(anat_arr.shape[0]):
            continue
        anat_img = norm01(anat_arr[best_z])
        func_src = plane_ref.get("ref_warped")
        src_label = "warped"
        if func_src is None:
            tform = plane_ref.get("tform")
            mov = plane_ref.get("ref_match", plane_ref.get("ref2d_raw", plane_ref.get("ref2d")))
            if tform is not None and mov is not None and callable(apply_transform_2d_func):
                try:
                    func_src = apply_transform_2d_func(mov, tform, output_shape=anat_img.shape, order=1)
                    src_label = "tform-preview"
                except Exception:
                    func_src = None
            if func_src is None:
                func_src = plane_ref.get("ref_warped_raw")
                src_label = "warped-raw"
        if func_src is None:
            func_src = plane_ref.get("ref2d", plane_ref.get("ref2d_raw"))
            src_label = "raw"
        if func_src is None:
            continue
        func_img = np.asarray(func_src, dtype=np.float32)
        if func_img.shape != anat_img.shape:
            func_img = transform.resize(
                func_img,
                anat_img.shape,
                order=1,
                mode="reflect",
                preserve_range=True,
                anti_aliasing=True,
            ).astype(np.float32)
        payload.append(
            {
                "plane_idx": int(plane_idx),
                "plane_label": str(plane_ref.get("label", f"plane{plane_idx}")),
                "best_z": int(best_z),
                "anat_img": anat_img,
                "func_img": norm01(func_img),
                "func_src": src_label,
            }
        )

    if not payload:
        log_lines.append("[region-qa] no valid plane previews available.")
        return {"ok": False, "log_lines": log_lines}

    default_plane_idx = int(len(plane_refs) // 2)
    plane_map = {int(item["plane_idx"]): item for item in payload}
    preview_plane_idx = (
        int(loaded_spec.get("preview_plane_idx", default_plane_idx)) if isinstance(loaded_spec, dict) else default_plane_idx
    )
    if preview_plane_idx not in plane_map:
        preview_plane_idx = int(payload[min(len(payload) - 1, max(0, len(payload) // 2))]["plane_idx"])
    default_cx = int(round((anat_shape[1] - 1) / 2.0))
    default_cy = int(round((anat_shape[0] - 1) / 2.0))
    square_cx = int(loaded_spec.get("center_x", default_cx)) if isinstance(loaded_spec, dict) else default_cx
    square_cy = int(loaded_spec.get("center_y", default_cy)) if isinstance(loaded_spec, dict) else default_cy
    square_size = int(loaded_spec.get("size_px", default_size_px)) if isinstance(loaded_spec, dict) else int(default_size_px)

    def _bundle(plane_idx: int, cx: int, cy: int, size_px: int) -> dict[str, Any]:
        item = plane_map[int(plane_idx)]
        x0, y0, x1, y1 = _square_bounds(cx=int(cx), cy=int(cy), size_px=int(size_px), shape=item["anat_img"].shape)
        return {
            "fish_id": fish_id,
            "method": "region_match_qa_square_selector",
            "preview_plane_idx": int(plane_idx),
            "preview_plane_label": str(item["plane_label"]),
            "preview_best_z": int(item["best_z"]),
            "center_x": int(cx),
            "center_y": int(cy),
            "size_px": int(x1 - x0),
            "bounds_xyxy": [int(x0), int(y0), int(x1), int(y1)],
            "saved_utc": datetime.now(timezone.utc).isoformat(),
        }

    def _render(plane_idx: int, cx: int, cy: int, size_px: int, func_alpha_val: float, anat_alpha_val: float, zoom_val: float) -> None:
        item = plane_map[int(plane_idx)]
        anat_img = item["anat_img"]
        func_img = item["func_img"]
        x0, y0, x1, y1 = _square_bounds(cx=int(cx), cy=int(cy), size_px=int(size_px), shape=anat_img.shape)
        rgb = np.zeros(anat_img.shape + (3,), dtype=np.float32)
        rgb[..., 0] = anat_img * float(anat_alpha_val)
        rgb[..., 2] = anat_img * float(anat_alpha_val)
        rgb[..., 1] = func_img * float(func_alpha_val)
        rgb = np.clip(rgb, 0.0, 1.0)

        fig, ax = plt.subplots(1, 1, figsize=(8.5, 8.5))
        ax.imshow(rgb)
        ax.add_patch(
            matplotlib.patches.Rectangle((x0, y0), x1 - x0, y1 - y0, fill=False, edgecolor="yellow", linewidth=2.0)
        )
        ax.scatter([cx], [cy], s=20, c="yellow")
        ax.set_title(
            f"{item['plane_label']} z={item['best_z']} | func src={item['func_src']} | center=({int(cx)}, {int(cy)}) size={int(x1-x0)}"
        )
        _apply_zoom(ax, shape=anat_img.shape, zoom=float(zoom_val), center_xy=(float(cx), float(cy)))
        ax.axis("off")
        _emit_figure(fig)
        print(f"[region-qa] bounds xyxy=({x0}, {y0}, {x1}, {y1}) | save path={square_json_path}")

    current_spec = _bundle(preview_plane_idx, square_cx, square_cy, square_size)

    try:
        import ipywidgets as widgets
        from IPython.display import display

        has_widgets = True
    except Exception:
        widgets = None
        display = None
        has_widgets = False
        log_lines.append("[region-qa] ipywidgets not available; falling back to static preview.")

    if has_widgets and widgets is not None and display is not None:
        plane_options = [(f"p{item['plane_idx']} z={item['best_z']} {item['plane_label']}", int(item["plane_idx"])) for item in payload]
        plane_dd = widgets.Dropdown(options=plane_options, value=int(preview_plane_idx), description="Plane")
        x_sl = widgets.IntSlider(value=int(square_cx), min=0, max=int(anat_shape[1] - 1), step=1, description="Center X", continuous_update=False)
        y_sl = widgets.IntSlider(value=int(square_cy), min=0, max=int(anat_shape[0] - 1), step=1, description="Center Y", continuous_update=False)
        size_sl = widgets.IntSlider(value=int(square_size), min=8, max=int(min(anat_shape)), step=2, description="Size", continuous_update=False)
        func_alpha_sl = widgets.FloatSlider(
            value=float(func_alpha),
            min=0.0,
            max=1.0,
            step=0.05,
            description="Func alpha",
            readout_format=".2f",
            continuous_update=False,
        )
        anat_alpha_sl = widgets.FloatSlider(
            value=float(anat_alpha),
            min=0.0,
            max=1.0,
            step=0.05,
            description="Anat alpha",
            readout_format=".2f",
            continuous_update=False,
        )
        zoom_sl = widgets.FloatSlider(
            value=float(zoom),
            min=1.0,
            max=6.0,
            step=0.25,
            description="Zoom",
            readout_format=".2f",
            continuous_update=False,
        )
        save_btn = widgets.Button(description="Save square", button_style="success")

        def _on_save(_btn: Any) -> None:
            spec = _bundle(plane_dd.value, x_sl.value, y_sl.value, size_sl.value)
            if save_square:
                square_json_path.parent.mkdir(parents=True, exist_ok=True)
                square_json_path.write_text(json.dumps(spec, indent=2))
                print(f"[region-qa] saved square -> {square_json_path}")
            print(json.dumps(spec, indent=2))

        save_btn.on_click(_on_save)
        out = widgets.interactive_output(
            _render,
            {
                "plane_idx": plane_dd,
                "cx": x_sl,
                "cy": y_sl,
                "size_px": size_sl,
                "func_alpha_val": func_alpha_sl,
                "anat_alpha_val": anat_alpha_sl,
                "zoom_val": zoom_sl,
            },
        )
        display(
            widgets.VBox(
                [widgets.HBox([plane_dd]), x_sl, y_sl, size_sl, func_alpha_sl, anat_alpha_sl, zoom_sl, save_btn, out]
            )
        )
    else:
        _render(preview_plane_idx, square_cx, square_cy, square_size, float(func_alpha), float(anat_alpha), float(zoom))
        if save_square:
            square_json_path.parent.mkdir(parents=True, exist_ok=True)
            square_json_path.write_text(json.dumps(current_spec, indent=2))
            log_lines.append(f"[region-qa] saved default square -> {square_json_path}")

    return {
        "ok": True,
        "log_lines": log_lines,
        "square_spec": current_spec,
        "square_json_path": square_json_path,
        "legacy_square_json_path": legacy_json_path,
        "loaded_square_path": loaded_path,
    }


def compute_anatomy_median_xy_radius_um(
    anat_labels_all: np.ndarray,
    *,
    plane_refs: list[dict[str, Any]] | None = None,
    vox_x: float = 1.0,
    vox_y: float = 1.0,
) -> dict[str, float | int]:
    anat_arr = _ensure_uint_labels(anat_labels_all)
    planes: list[np.ndarray] = []
    if anat_arr.ndim == 3 and plane_refs:
        depth = int(anat_arr.shape[0])
        for plane_idx, plane_ref in enumerate(plane_refs):
            best_z = int(plane_ref.get("best_z", plane_idx))
            if 0 <= best_z < depth:
                planes.append(anat_arr[best_z])
    elif anat_arr.ndim == 3:
        planes = [anat_arr[z] for z in range(int(anat_arr.shape[0]))]
    else:
        planes = [anat_arr]

    diameters_um: list[float] = []
    px_area_um2 = float(max(vox_x, 1e-12)) * float(max(vox_y, 1e-12))
    for slice_labels in planes:
        labels, counts = np.unique(slice_labels, return_counts=True)
        for label, count in zip(labels.tolist(), counts.tolist()):
            if int(label) <= 0 or int(count) <= 0:
                continue
            area_um2 = float(count) * px_area_um2
            diameter_um = 2.0 * np.sqrt(area_um2 / np.pi)
            diameters_um.append(float(diameter_um))
    if not diameters_um:
        return {"median_xy_diameter_um": 0.0, "median_xy_radius_um": 0.0, "n_labels": 0}
    median_diameter = float(np.median(np.asarray(diameters_um, dtype=float)))
    return {
        "median_xy_diameter_um": median_diameter,
        "median_xy_radius_um": float(median_diameter / 2.0),
        "n_labels": int(len(diameters_um)),
    }


def show_centroid_match_qa_stage(
    *,
    plane_refs: list[dict[str, Any]] | None,
    anat_labels_all: np.ndarray | None,
    anat_stack: np.ndarray | None = None,
    out_reg: str | Path | None = None,
    outdir: str | Path | None = None,
    out_seg: str | Path | None = None,
    func_labels: Any = None,
    func_labels_path: str | Path | None = None,
    vox_anat: dict[str, Any] | None = None,
    apply_func_orientation_func: Any = None,
    imread_func: Any = None,
    require_overlap: bool = True,
    min_overlap: int = 1,
    force_recompute: bool = True,
    max_link_dist_px: float = 50.0,
    qa_legend_fontsize: int = 15,
    qa_ytick_fontsize: int = 15,
    render_ui: bool = True,
    initial_threshold_um: float | None = None,
) -> dict[str, Any]:
    log_lines: list[str] = []
    if not plane_refs:
        log_lines.append("No planes available for centroid QA.")
        return {"ok": False, "log_lines": log_lines}
    if anat_labels_all is None:
        log_lines.append("Could not load anatomy labels for centroid QA.")
        return {"ok": False, "log_lines": log_lines}

    anat_labels_arr = _ensure_uint_labels(anat_labels_all)
    out_reg_path = Path(out_reg) if out_reg is not None else None
    outdir_path = Path(outdir) if outdir is not None else None
    try:
        vox_x_val = (vox_anat or {}).get("X", (vox_anat or {}).get(2, (vox_anat or {}).get("2", 1.0)))
        vox_y_val = (vox_anat or {}).get("Y", (vox_anat or {}).get(1, (vox_anat or {}).get("1", 1.0)))
        vox_x = float(vox_x_val)
        vox_y = float(vox_y_val)
    except Exception:
        vox_x, vox_y = 1.0, 1.0

    links_cache: dict[int, pd.DataFrame] = {}
    plane_data_cache: dict[int, dict[str, Any] | None] = {}
    global_data: dict[str, Any] | None = None
    all_anat_points_cache: dict[int, pd.DataFrame] = {}
    all_func_points_cache: dict[int, pd.DataFrame] = {}

    def _load_func_labels_for_plane(p_idx: int) -> tuple[np.ndarray | None, str | None, str | None]:
        p_idx = int(max(0, min(len(plane_refs) - 1, int(p_idx))))
        plane_ref = plane_refs[p_idx]
        label = plane_ref.get("label", f"plane{p_idx}")
        arr, src_desc = resolve_functional_labels_for_plane(
            plane_ref,
            p_idx,
            use_suite2p_labels=False,
            func_labels=func_labels,
            out_seg=out_seg,
            func_labels_path=func_labels_path,
            apply_func_orientation_func=apply_func_orientation_func,
            imread_func=imread_func,
            ensure_uint_labels_func=_ensure_uint_labels,
        )
        return arr, label, src_desc

    def _compute_links_for_plane(p_idx: int) -> pd.DataFrame | None:
        p_idx = int(max(0, min(len(plane_refs) - 1, int(p_idx))))
        if p_idx in links_cache:
            return links_cache[p_idx]
        plane_ref = plane_refs[p_idx]
        plane_label = str(plane_ref.get("label", f"plane{p_idx}"))
        cache_paths: list[Path] = []
        write_paths: list[Path] = []
        if plane_label:
            if out_reg_path is not None:
                out_path = out_reg_path / f"f2a_centroid_matches_{plane_label}.csv"
                cache_paths.append(out_path)
                write_paths.append(out_path)
            if outdir_path is not None:
                cache_paths.append(outdir_path / f"f2a_centroid_matches_{plane_label}.csv")
        if p_idx == 0:
            if out_reg_path is not None:
                out_path = out_reg_path / "f2a_centroid_matches.csv"
                cache_paths.append(out_path)
                write_paths.append(out_path)
            if outdir_path is not None:
                cache_paths.append(outdir_path / "f2a_centroid_matches.csv")

        for cache_path in cache_paths:
            if force_recompute:
                break
            if cache_path.exists():
                try:
                    cached = pd.read_csv(cache_path)
                    if require_overlap and "overlap_px" in cached.columns:
                        cached = cached[cached["overlap_px"] >= int(min_overlap)].reset_index(drop=True)
                    links_cache[p_idx] = cached
                    log_lines.append(f"[3.1a] Using cached links for {plane_label}: {cache_path}")
                    return cached
                except Exception:
                    continue

        func_labels_plane, _, _ = _load_func_labels_for_plane(p_idx)
        if func_labels_plane is None:
            log_lines.append(f"[3.1a] Missing functional labels for plane {plane_label}")
            return None

        best_z = int(plane_ref.get("best_z", 0))
        if anat_labels_arr.ndim == 3:
            if best_z < 0 or best_z >= int(anat_labels_arr.shape[0]):
                log_lines.append(f"[3.1a] best_z out of range for plane {plane_label}: {best_z}")
                return None
            anat_slice = anat_labels_arr[best_z]
        else:
            anat_slice = anat_labels_arr

        match_result = build_plane_centroid_matches(
            func_labels_plane,
            anat_slice,
            plane_ref=plane_ref,
            vox_x=vox_x,
            vox_y=vox_y,
            max_link_dist_px=max_link_dist_px,
            require_overlap=require_overlap,
            min_overlap=min_overlap,
            tform_for_plane_func=lambda _plane_ref: _plane_ref.get("tform"),
            resample_labels_nn_func=None,
        )
        status = str(match_result.get("status", "ok"))
        if status != "ok" and (not status.startswith("empty_labels")):
            log_lines.append(f"[3.1a] {status} for plane {plane_label}")
            return None
        links_df = match_result["links_df"].copy()
        links_cache[p_idx] = links_df
        if len(links_df):
            for write_path in (write_paths or cache_paths):
                try:
                    write_path.parent.mkdir(parents=True, exist_ok=True)
                    links_df.to_csv(write_path, index=False)
                except Exception:
                    continue
        return links_df

    def _prepare_plane_data(p_idx: int) -> dict[str, Any] | None:
        p_idx = int(max(0, min(len(plane_refs) - 1, int(p_idx))))
        if p_idx in plane_data_cache:
            return plane_data_cache[p_idx]
        links_df = _compute_links_for_plane(p_idx)
        if links_df is None:
            plane_data_cache[p_idx] = None
            return None
        dists = links_df.get("dist_um", pd.Series(dtype=float)).to_numpy(dtype=float)
        med = float(np.median(dists)) if dists.size else np.nan
        plane_ref = plane_refs[p_idx]
        label = str(plane_ref.get("label", f"plane{p_idx}"))
        best_z = int(plane_ref.get("best_z", 0))
        try:
            bg = norm01(np.asarray(anat_stack[best_z], dtype=np.float32)) if anat_stack is not None else None
        except Exception:
            bg = None
        if anat_labels_arr.ndim == 3 and 0 <= best_z < int(anat_labels_arr.shape[0]):
            anat_slice = anat_labels_arr[best_z]
        else:
            anat_slice = anat_labels_arr if anat_labels_arr.ndim == 2 else np.zeros((1, 1), dtype=np.uint32)
        all_anat_points_cache[p_idx] = _regionprops_centroids_2d(anat_slice)
        func_labels_plane, _, _ = _load_func_labels_for_plane(p_idx)
        if func_labels_plane is not None:
            warped = build_plane_centroid_matches(
                func_labels_plane,
                anat_slice,
                plane_ref=plane_ref,
                vox_x=vox_x,
                vox_y=vox_y,
                max_link_dist_px=float("inf"),
                require_overlap=False,
                min_overlap=0,
                tform_for_plane_func=lambda _plane_ref: _plane_ref.get("tform"),
                resample_labels_nn_func=None,
            ).get("func_warped")
            all_func_points_cache[p_idx] = _regionprops_centroids_2d(warped) if warped is not None else pd.DataFrame(columns=["label", "cy", "cx"])
        else:
            all_func_points_cache[p_idx] = pd.DataFrame(columns=["label", "cy", "cx"])
        plane_data_cache[p_idx] = {
            "df": links_df,
            "d": dists,
            "med": med,
            "label": label,
            "best_z": best_z,
            "bg": bg,
            "all_anat_points": all_anat_points_cache[p_idx],
            "all_func_points": all_func_points_cache[p_idx],
        }
        return plane_data_cache[p_idx]

    def _collect_global_data() -> dict[str, Any] | None:
        nonlocal global_data
        if global_data is not None:
            return global_data
        all_d: list[np.ndarray] = []
        for p_idx in range(len(plane_refs)):
            pdata = _prepare_plane_data(p_idx)
            if pdata is not None and pdata.get("d") is not None and pdata["d"].size:
                all_d.append(pdata["d"])
        if not all_d:
            global_data = None
            return None
        dcat = np.concatenate(all_d).astype(float)
        global_data = {"d": dcat, "med": float(np.median(dcat)), "N": int(dcat.size)}
        return global_data

    reference = compute_anatomy_median_xy_radius_um(anat_labels_arr, plane_refs=plane_refs, vox_x=vox_x, vox_y=vox_y)
    initial_threshold = (
        float(initial_threshold_um) if initial_threshold_um is not None else float(reference.get("median_xy_radius_um", 0.0) or 0.0)
    )

    if render_ui:
        try:
            import ipywidgets as widgets
            from IPython.display import display

            has_widgets = True
        except Exception:
            widgets = None
            display = None
            has_widgets = False
    else:
        has_widgets = False
        widgets = None
        display = None

    render_log: list[dict[str, Any]] = []
    initial_plane_snapshot: dict[str, Any] | None = None
    if has_widgets and widgets is not None and display is not None:
        plane_sl = widgets.IntSlider(
            value=0, min=0, max=max(0, len(plane_refs) - 1), step=1, description="Plane", continuous_update=False
        )
        thr_sl = widgets.FloatSlider(
            value=float(initial_threshold),
            min=0.0,
            max=max(10.0, float(initial_threshold) * 3.0 if initial_threshold > 0 else 10.0),
            step=0.1,
            description="Threshold (µm)",
            style={"description_width": "120px"},
            layout=widgets.Layout(width="420px"),
            continuous_update=False,
        )
        out = widgets.Output()
        count_html = widgets.HTML()
        ref_html = widgets.HTML(
            value=(
                f"<b>Initial threshold:</b> median anatomy XY radius = {float(reference['median_xy_radius_um']):.2f} µm "
                f"(median diameter {float(reference['median_xy_diameter_um']):.2f} µm, N={int(reference['n_labels'])})"
            )
        )
        state = {"plane": int(plane_sl.value)}

        def _render(threshold: float) -> None:
            pdata = _prepare_plane_data(state["plane"])
            if pdata is None:
                with out:
                    out.clear_output(wait=True)
                    print("No centroid matches to plot for this plane.")
                count_html.value = ""
                return
            d = pdata["d"]
            df = pdata["df"]
            med = pdata["med"]
            bg = pdata["bg"]
            label = pdata["label"]
            best_z = pdata["best_z"]
            all_anat_points = pdata["all_anat_points"]
            all_func_points = pdata["all_func_points"]
            keep_mask = d <= float(threshold) if d.size else np.zeros((len(df),), dtype=bool)
            n_keep = int(keep_mask.sum())
            gdata = _collect_global_data()
            with out:
                out.clear_output(wait=True)
                fig, (ax1, ax2, ax3) = plt.subplots(1, 3, figsize=(20, 7), gridspec_kw={"width_ratios": [1, 3, 1]})
                if d.size:
                    vp = ax1.violinplot(d, showmeans=False, showmedians=False, showextrema=False)
                    for pc in vp["bodies"]:
                        pc.set_facecolor("#88ccee")
                        pc.set_edgecolor("black")
                        pc.set_alpha(0.7)
                ax1.axhline(med, color="crimson", linestyle="--", linewidth=1.5, label=f"Median {med:.2f} µm")
                ax1.axhline(float(threshold), color="orange", linestyle=":", linewidth=1.5, label=f"Thresh {float(threshold):.2f} µm")
                ax1.set_xticks([])
                ax1.set_ylabel("Centroid distance (µm)")
                ax1.set_title(f"Centroid distances (µm) — {label}")
                ax1.legend(loc="lower right", fontsize=qa_legend_fontsize)
                ax1.tick_params(axis="y", labelsize=qa_ytick_fontsize)

                if bg is not None:
                    ax2.imshow(bg, cmap="gray")
                if not all_anat_points.empty:
                    ax2.scatter(all_anat_points["cx"], all_anat_points["cy"], s=18, c="white", alpha=0.55, label="All anat centroids")
                if not all_func_points.empty:
                    ax2.scatter(
                        all_func_points["cx"],
                        all_func_points["cy"],
                        s=18,
                        facecolors="none",
                        edgecolors="cyan",
                        alpha=0.45,
                        label="All warped func centroids",
                    )
                kept = df[keep_mask] if keep_mask.shape[0] == len(df) else df
                if len(kept):
                    ax2.scatter(kept["ax_px"], kept["ay_px"], s=30, c="magenta", label="Linked anat centroids")
                    ax2.scatter(
                        kept["fx_anat_px"],
                        kept["fy_anat_px"],
                        s=30,
                        facecolors="none",
                        edgecolors="lime",
                        label="Linked func→anat centroids",
                    )
                    for _, row in kept.iterrows():
                        ax2.plot([row["ax_px"], row["fx_anat_px"]], [row["ay_px"], row["fy_anat_px"]], "y-", alpha=0.5)
                else:
                    ax2.text(0.02, 0.98, "No links pass threshold.\nShowing centroid context.", transform=ax2.transAxes, va="top", ha="left", color="white")
                handles, labels = ax2.get_legend_handles_labels()
                if handles:
                    ax2.legend(loc="lower right", fontsize=qa_legend_fontsize)
                ax2.set_title(
                    f"Anatomy context + links ≤ threshold — {label} @ Z={best_z}\nRef radius={float(reference['median_xy_radius_um']):.2f} µm"
                )
                ax2.axis("off")

                if gdata is not None and gdata.get("d") is not None and gdata["d"].size:
                    vp_g = ax3.violinplot(gdata["d"], showmeans=False, showmedians=False, showextrema=False)
                    for pc in vp_g["bodies"]:
                        pc.set_facecolor("#b3d9ff")
                        pc.set_edgecolor("black")
                        pc.set_alpha(0.7)
                    ax3.axhline(gdata["med"], color="crimson", linestyle="--", linewidth=1.5, label=f"Global median {gdata['med']:.2f} µm")
                    ax3.axhline(float(threshold), color="orange", linestyle=":", linewidth=1.5, label=f"Thresh {float(threshold):.2f} µm")
                    ax3.set_xticks([])
                    ax3.set_ylabel("Centroid distance (µm)")
                    ax3.set_title(f"All planes (N={int(gdata['N'])})")
                    ax3.legend(loc="lower right", fontsize=qa_legend_fontsize)
                fig.tight_layout()
                _emit_figure(fig)
            count_html.value = f"<b>{label}</b>: kept {n_keep}/{len(df)} centroid links at threshold {float(threshold):.2f} µm"
            render_log.append({"plane": label, "threshold_um": float(threshold), "kept_links": int(n_keep), "total_links": int(len(df))})

        def _on_plane_change(change: dict[str, Any]) -> None:
            if change.get("name") == "value":
                state["plane"] = int(change["new"])
                _render(thr_sl.value)

        def _on_threshold_change(change: dict[str, Any]) -> None:
            if change.get("name") == "value":
                _render(change["new"])

        plane_sl.observe(_on_plane_change, names="value")
        thr_sl.observe(_on_threshold_change, names="value")
        display(widgets.VBox([widgets.HBox([plane_sl, thr_sl]), ref_html, count_html, out]))
        _render(thr_sl.value)
    else:
        for p_idx in range(len(plane_refs)):
            pdata = _prepare_plane_data(p_idx)
            if pdata is not None:
                log_lines.append(f"[34] {pdata['label']}: centroid matches={len(pdata['df'])}, median={pdata['med']:.2f} µm")
                if initial_plane_snapshot is None:
                    df = pdata["df"]
                    d = pdata["d"]
                    keep_mask = d <= float(initial_threshold) if d.size else np.zeros((len(df),), dtype=bool)
                    initial_plane_snapshot = {
                        "plane": pdata["label"],
                        "total_links": int(len(df)),
                        "links_at_initial_threshold": int(keep_mask.sum()),
                        "context_anat_centroids": int(len(pdata["all_anat_points"])),
                        "context_func_centroids": int(len(pdata["all_func_points"])),
                    }

    return {
        "ok": True,
        "log_lines": log_lines,
        "median_xy_reference": reference,
        "initial_threshold_um": float(initial_threshold),
        "max_link_dist_px": float(max_link_dist_px),
        "require_overlap": bool(require_overlap),
        "min_overlap": int(min_overlap),
        "helpers": {
            "load_func_labels_for_plane": _load_func_labels_for_plane,
            "prepare_plane_data": _prepare_plane_data,
            "collect_global_data": _collect_global_data,
            "tform_for_plane": (lambda plane_ref: plane_ref.get("tform")),
        },
        "initial_plane_snapshot": initial_plane_snapshot,
        "render_log": render_log,
    }


def _as_bool_series(values: pd.Series) -> pd.Series:
    series = pd.Series(values)
    if pd.api.types.is_bool_dtype(series):
        return series.fillna(False).astype(bool)
    if pd.api.types.is_numeric_dtype(series):
        return series.fillna(0).astype(float) != 0
    return series.astype(str).str.strip().str.lower().isin({"1", "true", "t", "yes", "y"})


def _resolve_cohort_fish_dir(*, data_root: Path, owner: str, fish_id: str, data_mode: str = "local") -> Path:
    root = Path(data_root)
    if str(data_mode).strip().lower() == "local":
        candidates = [
            root / fish_id,
            root / owner / fish_id,
            root / owner / "Microscopy" / fish_id,
        ]
        for cand in candidates:
            if cand.exists():
                return cand
        return candidates[0]
    owner_root = root / owner
    microscopy = owner_root / "Microscopy"
    return (microscopy if microscopy.exists() else owner_root) / fish_id


def _cohort_voxels_from_metadata(run_metadata_path: Path) -> dict[str, float]:
    payload = json.loads(run_metadata_path.read_text())
    voxels = payload.get("voxels", {})
    anat = voxels.get("anat", voxels) if isinstance(voxels, dict) else {}
    return {
        "X": float(anat.get("X", 1.0)),
        "Y": float(anat.get("Y", 1.0)),
        "Z": float(anat.get("Z", 1.0)),
    }


def _collect_ncc_curves_for_fish(*, fish_id: str, fish_dir: Path) -> pd.DataFrame:
    ncc_path = fish_dir / "03_analysis" / "functional" / "ncc" / "ncc_bestz_by_plane.json"
    if not ncc_path.exists():
        return pd.DataFrame(columns=["fish_id", "plane_idx", "plane_label", "z_idx", "ncc_score", "best_z", "best_score"])
    payload = json.loads(ncc_path.read_text())
    per_fish = payload.get("per_fish", {})
    fish_payload = per_fish.get(fish_id)
    if not isinstance(fish_payload, dict):
        return pd.DataFrame(columns=["fish_id", "plane_idx", "plane_label", "z_idx", "ncc_score", "best_z", "best_score"])
    rows: list[dict[str, Any]] = []
    for plane_idx, (plane_label, entry) in enumerate(fish_payload.items()):
        scores = np.asarray(entry.get("scores", []), dtype=float)
        if scores.size == 0:
            continue
        best_z = int(entry.get("best_z", int(np.argmax(scores))))
        best_score = float(scores[best_z]) if 0 <= best_z < scores.size else float(np.nanmax(scores))
        for z_idx, score in enumerate(scores.tolist()):
            rows.append(
                {
                    "fish_id": fish_id,
                    "plane_idx": int(plane_idx),
                    "plane_label": str(plane_label),
                    "z_idx": int(z_idx),
                    "ncc_score": float(score),
                    "best_z": int(best_z),
                    "best_score": float(best_score),
                }
            )
    return pd.DataFrame(rows)


def _filter_diameters_with_summary(*, fish_id: str, diam_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    if diam_df.empty:
        empty_summary = pd.DataFrame(
            columns=["fish_id", "dataset", "xy_q05_um", "xy_q95_um", "n_input", "n_kept", "n_drop_q05", "n_low_conf_q95"]
        )
        return diam_df.copy(), empty_summary
    required = {"dataset", "x_um", "y_um", "z_um"}
    if not required.issubset(set(diam_df.columns)):
        missing = sorted(required - set(diam_df.columns))
        raise ValueError(f"diameters_df_all missing columns {missing} for fish {fish_id}")
    kept_parts: list[pd.DataFrame] = []
    summary_rows: list[dict[str, Any]] = []
    for dataset in sorted(diam_df["dataset"].astype(str).unique().tolist()):
        sub = diam_df[diam_df["dataset"].astype(str) == str(dataset)].copy()
        xy_vals = pd.to_numeric(sub[["x_um", "y_um"]].mean(axis=1), errors="coerce").to_numpy(dtype=float)
        finite_mask = np.isfinite(xy_vals)
        finite_vals = xy_vals[finite_mask]
        keep_mask = np.zeros(len(sub), dtype=bool)
        low_conf_mask = np.zeros(len(sub), dtype=bool)
        q05 = np.nan
        q95 = np.nan
        if finite_vals.size:
            q05 = float(np.quantile(finite_vals, 0.05))
            q95 = float(np.quantile(finite_vals, 0.95))
            finite_idx = np.where(finite_mask)[0]
            keep_mask[finite_idx] = finite_vals >= q05
            low_conf_mask[finite_idx] = finite_vals > q95
        kept = sub.iloc[keep_mask].copy()
        kept["xy_um"] = pd.to_numeric(kept[["x_um", "y_um"]].mean(axis=1), errors="coerce")
        kept["is_low_confidence_segmentation"] = low_conf_mask[keep_mask]
        kept_parts.append(kept)
        summary_rows.append(
            {
                "fish_id": fish_id,
                "dataset": str(dataset),
                "xy_q05_um": q05,
                "xy_q95_um": q95,
                "n_input": int(len(sub)),
                "n_kept": int(len(kept)),
                "n_drop_q05": int(max(0, len(sub) - len(kept))),
                "n_low_conf_q95": int(np.count_nonzero(low_conf_mask[keep_mask])) if len(kept) else 0,
            }
        )
    kept_df = pd.concat(kept_parts, ignore_index=True) if kept_parts else diam_df.iloc[0:0].copy()
    summary_df = pd.DataFrame(summary_rows)
    return kept_df, summary_df


def _collect_func_anat_offsets_for_fish(
    *,
    fish_id: str,
    fish_dir: Path,
    vox_anat: dict[str, float],
    roi_df: pd.DataFrame,
) -> pd.DataFrame:
    required = {"plane_match_outcome", "has_unique_anat_match", "selected_dist_um", "selected_anat_label", "best_z"}
    if not required.issubset(set(roi_df.columns)):
        return pd.DataFrame(columns=["fish_id", "axis", "offset_um"])
    mask = roi_df["plane_match_outcome"].astype(str).eq("anatomy match")
    mask &= _as_bool_series(roi_df["has_unique_anat_match"])
    matched = roi_df.loc[mask].copy()
    if matched.empty:
        return pd.DataFrame(columns=["fish_id", "axis", "offset_um"])
    matched["selected_dist_um"] = pd.to_numeric(matched["selected_dist_um"], errors="coerce")
    matched["selected_anat_label"] = pd.to_numeric(matched["selected_anat_label"], errors="coerce").astype("Int64")
    matched["best_z"] = pd.to_numeric(matched["best_z"], errors="coerce")
    matched = matched.dropna(subset=["selected_dist_um", "selected_anat_label", "best_z"]).copy()
    if matched.empty:
        return pd.DataFrame(columns=["fish_id", "axis", "offset_um"])
    anat_labels_path = infer_anat_labels_path(fish_dir, fish_id)
    if anat_labels_path is None or not Path(anat_labels_path).exists():
        z_offsets = np.full((len(matched),), np.nan, dtype=float)
    else:
        anat_labels = np.asarray(tifffile.imread(str(anat_labels_path)))
        anat_centroids = compute_centroids(anat_labels)
        z_lookup = dict(zip(anat_centroids["label"].astype(int), pd.to_numeric(anat_centroids["z"], errors="coerce")))
        dz = float(vox_anat.get("Z", 1.0))
        z_vals: list[float] = []
        for row in matched.itertuples(index=False):
            anat_label = int(row.selected_anat_label)
            anat_z = z_lookup.get(anat_label)
            if anat_z is None or not np.isfinite(anat_z):
                z_vals.append(np.nan)
                continue
            z_vals.append(abs(float(anat_z) - float(row.best_z)) * dz)
        z_offsets = np.asarray(z_vals, dtype=float)
    xy_offsets = np.abs(pd.to_numeric(matched["selected_dist_um"], errors="coerce").to_numpy(dtype=float))
    xy_rows = pd.DataFrame({"fish_id": fish_id, "axis": "xy", "offset_um": xy_offsets})
    z_rows = pd.DataFrame({"fish_id": fish_id, "axis": "z", "offset_um": z_offsets})
    out = pd.concat([xy_rows, z_rows], ignore_index=True)
    out["offset_um"] = pd.to_numeric(out["offset_um"], errors="coerce")
    out = out[np.isfinite(out["offset_um"])].reset_index(drop=True)
    return out


def _resolve_conf_mask_path(*, fish_dir: Path, conf_mask_value: Any) -> Path | None:
    if conf_mask_value is None or (isinstance(conf_mask_value, float) and np.isnan(conf_mask_value)):
        return None
    cand = Path(str(conf_mask_value))
    if cand.exists():
        return cand
    joined = fish_dir / cand
    if joined.exists():
        return joined
    return None


IN_PLANE_HCR_QC_STATUSES = {
    "in-plane responsive ROI",
    "in-plane low-activity ROI",
    "in-plane response unavailable",
    "in-plane no functional ROI candidate",
}


def _select_in_plane_hcr_status_like_53a(status_df: pd.DataFrame) -> pd.DataFrame:
    if status_df.empty:
        return status_df.copy()
    work = status_df.copy()
    work["gene"] = work.get("gene", pd.Series(["unknown"] * len(work), index=work.index)).astype(str).str.strip()
    work["anat_label"] = pd.to_numeric(work.get("anat_label", np.nan), errors="coerce").astype("Int64")
    represented = _as_bool_series(work.get("represented_on_func_plane", pd.Series(False, index=work.index)))
    functional_status = work.get("functional_status", pd.Series("", index=work.index)).astype(str).str.strip()
    work = work[represented & functional_status.isin(IN_PLANE_HCR_QC_STATUSES)].copy()
    work = work.dropna(subset=["gene", "anat_label"]).copy()
    if work.empty:
        return work
    work["dist_conf"] = pd.to_numeric(work.get("dist_conf_anat_um", work.get("dist_conf", np.nan)), errors="coerce")
    work["dist_func"] = pd.to_numeric(work.get("selected_dist_um", np.nan), errors="coerce")
    work["overlap"] = pd.to_numeric(work.get("selected_overlap_px", work.get("overlap_px_func_anat", np.nan)), errors="coerce")
    work["plane_sort"] = pd.to_numeric(work.get("selected_plane", np.nan), errors="coerce")
    work["func_sort"] = pd.to_numeric(work.get("selected_func_label", np.nan), errors="coerce")
    work = work.sort_values(
        ["gene", "anat_label", "dist_conf", "dist_func", "overlap", "plane_sort", "func_sort"],
        ascending=[True, True, True, True, False, True, True],
        na_position="last",
    ).reset_index(drop=True)
    work = work.drop_duplicates(subset=["gene", "anat_label"], keep="first").reset_index(drop=True)
    return work


def _collect_hcr_offsets_for_fish(
    *,
    fish_id: str,
    fish_dir: Path,
    vox_anat: dict[str, float],
    hcr_df: pd.DataFrame,
) -> pd.DataFrame:
    if hcr_df.empty:
        return pd.DataFrame(columns=["fish_id", "gene", "anat_label", "xy_um", "abs_dz_um", "distance_um"])
    anat_labels_path = infer_anat_labels_path(fish_dir, fish_id)
    if anat_labels_path is None or not Path(anat_labels_path).exists():
        return pd.DataFrame(columns=["fish_id", "gene", "anat_label", "xy_um", "abs_dz_um", "distance_um"])
    anat_labels = np.asarray(tifffile.imread(str(anat_labels_path)))
    anat_centroids = compute_centroids(anat_labels)
    anat_centroids["label"] = pd.to_numeric(anat_centroids["label"], errors="coerce").astype("Int64")
    anat_centroids = anat_centroids.dropna(subset=["label"])
    anat_lookup = {
        int(row.label): (float(row.x), float(row.y), float(row.z)) for row in anat_centroids.itertuples(index=False)
    }
    dx = float(vox_anat.get("X", 1.0))
    dy = float(vox_anat.get("Y", 1.0))
    dz = float(vox_anat.get("Z", 1.0))
    rows: list[dict[str, Any]] = []
    for row in hcr_df.itertuples(index=False):
        anat_label = int(row.anat_label)
        anat_xyz = anat_lookup.get(anat_label)
        if anat_xyz is None:
            continue
        # Prefer geometry distances already computed in the matching pipeline.
        # They are in shared anatomy space and avoid mixing raw confocal indices.
        xy_from_table = np.nan
        for col in ("dist_conf_anat_um", "dist_conf", "dist_um", "distance_um"):
            raw = getattr(row, col, np.nan)
            val = pd.to_numeric(pd.Series([raw]), errors="coerce").iloc[0]
            if pd.notna(val) and np.isfinite(float(val)):
                xy_from_table = float(abs(val))
                break
        abs_dz_from_table = np.nan
        for col in ("abs_dz_um", "dz_um"):
            raw = getattr(row, col, np.nan)
            val = pd.to_numeric(pd.Series([raw]), errors="coerce").iloc[0]
            if pd.notna(val) and np.isfinite(float(val)):
                abs_dz_from_table = float(abs(val))
                break

        dx_um = np.nan
        dy_um = np.nan
        dz_um = np.nan
        xy_from_centroids = np.nan
        abs_dz_from_centroids = np.nan
        dist_from_centroids = np.nan
        conf_mask_path = _resolve_conf_mask_path(
            fish_dir=fish_dir,
            conf_mask_value=getattr(row, "conf_mask", getattr(row, "mask_path", None)),
        )
        conf_label_val = getattr(row, "conf_label", getattr(row, "primary_conf_label", np.nan))
        conf_label = pd.to_numeric(pd.Series([conf_label_val]), errors="coerce").iloc[0]
        if conf_mask_path is not None and not pd.isna(conf_label):
            conf_labels = np.asarray(tifffile.imread(str(conf_mask_path)))
            if conf_labels.shape == anat_labels.shape:
                conf_centroids = compute_centroids(conf_labels)
                if not conf_centroids.empty:
                    conf_centroids["label"] = pd.to_numeric(conf_centroids["label"], errors="coerce").astype("Int64")
                    conf_hit = conf_centroids[conf_centroids["label"] == int(conf_label)]
                    if not conf_hit.empty:
                        conf_xyz = (
                            float(conf_hit.iloc[0]["x"]),
                            float(conf_hit.iloc[0]["y"]),
                            float(conf_hit.iloc[0]["z"]),
                        )
                        dx_um = (conf_xyz[0] - anat_xyz[0]) * dx
                        dy_um = (conf_xyz[1] - anat_xyz[1]) * dy
                        dz_um = (conf_xyz[2] - anat_xyz[2]) * dz
                        xy_from_centroids = float(np.hypot(dx_um, dy_um))
                        abs_dz_from_centroids = float(abs(dz_um))
                        dist_from_centroids = float(np.sqrt(dx_um * dx_um + dy_um * dy_um + dz_um * dz_um))

        xy_um = float(xy_from_table) if np.isfinite(xy_from_table) else float(xy_from_centroids)
        abs_dz_um = float(abs_dz_from_table) if np.isfinite(abs_dz_from_table) else float(abs_dz_from_centroids)
        distance_um = float(
            np.sqrt(xy_um * xy_um + abs_dz_um * abs_dz_um)
            if np.isfinite(xy_um) and np.isfinite(abs_dz_um)
            else (xy_um if np.isfinite(xy_um) else dist_from_centroids)
        )
        if not np.isfinite(xy_um) and not np.isfinite(abs_dz_um) and not np.isfinite(distance_um):
            continue
        rows.append(
            {
                "fish_id": fish_id,
                "gene": str(getattr(row, "gene", "unknown")),
                "anat_label": int(anat_label),
                "dx_um": float(dx_um),
                "dy_um": float(dy_um),
                "dz_um": float(dz_um),
                "xy_um": xy_um,
                "abs_dz_um": abs_dz_um,
                "distance_um": distance_um,
            }
        )
    return pd.DataFrame(rows)


def collect_cohort_53a_tables(
    *,
    fish_specs: list[dict[str, Any]],
    data_root: str | Path,
    data_mode: str = "local",
) -> dict[str, pd.DataFrame]:
    """Collect pooled cohort tables for the [53a-cohort] summary figure."""
    data_root_path = Path(data_root)
    ncc_parts: list[pd.DataFrame] = []
    diam_parts: list[pd.DataFrame] = []
    diam_summary_parts: list[pd.DataFrame] = []
    func_parts: list[pd.DataFrame] = []
    hcr_parts: list[pd.DataFrame] = []
    for spec in fish_specs:
        fish_id = str(spec.get("fish_id", "")).strip()
        owner = str(spec.get("owner", "")).strip()
        if not fish_id:
            continue
        fish_dir = _resolve_cohort_fish_dir(data_root=data_root_path, owner=owner, fish_id=fish_id, data_mode=data_mode)
        if not fish_dir.exists():
            continue
        analysis_dir = fish_dir / "03_analysis"
        out_reg = analysis_dir / "functional" / "registration"
        out_qa = analysis_dir / "functional" / "qa"
        run_metadata = out_reg / "run_metadata.json"
        vox_anat = _cohort_voxels_from_metadata(run_metadata) if run_metadata.exists() else {"X": 1.0, "Y": 1.0, "Z": 1.0}

        ncc_df = _collect_ncc_curves_for_fish(fish_id=fish_id, fish_dir=fish_dir)
        if not ncc_df.empty:
            ncc_parts.append(ncc_df)

        diam_path = out_qa / "diameters_df_all.pkl"
        if diam_path.exists():
            raw_diam = pd.read_pickle(diam_path)
            filt_diam, filt_summary = _filter_diameters_with_summary(fish_id=fish_id, diam_df=raw_diam)
            if not filt_diam.empty:
                filt_diam = filt_diam.copy()
                filt_diam["fish_id"] = fish_id
                diam_parts.append(filt_diam)
            if not filt_summary.empty:
                diam_summary_parts.append(filt_summary)

        roi_path = out_reg / "functional_roi_activity_identity.csv"
        if roi_path.exists():
            roi_df = pd.read_csv(roi_path)
            func_df = _collect_func_anat_offsets_for_fish(fish_id=fish_id, fish_dir=fish_dir, vox_anat=vox_anat, roi_df=roi_df)
            if not func_df.empty:
                func_parts.append(func_df)

        status_path = out_reg / "hcr_activity_status.csv"
        if status_path.exists():
            status_df = pd.read_csv(status_path)
            if "fish_id" in status_df.columns:
                status_df = status_df[status_df["fish_id"].astype(str) == fish_id].copy()
            status_subset = _select_in_plane_hcr_status_like_53a(status_df)
            hcr_df = _collect_hcr_offsets_for_fish(
                fish_id=fish_id,
                fish_dir=fish_dir,
                vox_anat=vox_anat,
                hcr_df=status_subset,
            )
            if not hcr_df.empty:
                hcr_parts.append(hcr_df)

    ncc_curves_df = (
        pd.concat(ncc_parts, ignore_index=True)
        if ncc_parts
        else pd.DataFrame(columns=["fish_id", "plane_idx", "plane_label", "z_idx", "ncc_score", "best_z", "best_score"])
    )
    diameters_df = (
        pd.concat(diam_parts, ignore_index=True)
        if diam_parts
        else pd.DataFrame(columns=["fish_id", "dataset", "x_um", "y_um", "z_um", "xy_um", "is_low_confidence_segmentation"])
    )
    diameter_filter_summary_df = (
        pd.concat(diam_summary_parts, ignore_index=True)
        if diam_summary_parts
        else pd.DataFrame(columns=["fish_id", "dataset", "xy_q05_um", "xy_q95_um", "n_input", "n_kept", "n_drop_q05", "n_low_conf_q95"])
    )
    func_anat_offsets_df = (
        pd.concat(func_parts, ignore_index=True)
        if func_parts
        else pd.DataFrame(columns=["fish_id", "axis", "offset_um"])
    )
    hcr_offsets_df = (
        pd.concat(hcr_parts, ignore_index=True)
        if hcr_parts
        else pd.DataFrame(columns=["fish_id", "gene", "anat_label", "xy_um", "abs_dz_um", "distance_um"])
    )

    anat_df = diameters_df[diameters_df.get("dataset", pd.Series(dtype=str)).astype(str) == "Anatomy"].copy()
    anat_xy = pd.to_numeric(anat_df.get("xy_um", pd.Series(dtype=float)), errors="coerce").to_numpy(dtype=float)
    anat_z = pd.to_numeric(anat_df.get("z_um", pd.Series(dtype=float)), errors="coerce").to_numpy(dtype=float)
    anat_xy = anat_xy[np.isfinite(anat_xy)]
    anat_z = anat_z[np.isfinite(anat_z)]
    thresholds_df = pd.DataFrame(
        [
            {
                "anat_r50_xy_um": float(np.median(anat_xy) / 2.0) if anat_xy.size else np.nan,
                "anat_r50_z_um": float(np.median(anat_z) / 2.0) if anat_z.size else np.nan,
            }
        ]
    )

    return {
        "ncc_curves_df": ncc_curves_df,
        "diameters_df": diameters_df,
        "diameter_filter_summary_df": diameter_filter_summary_df,
        "func_anat_offsets_df": func_anat_offsets_df,
        "hcr_offsets_df": hcr_offsets_df,
        "thresholds_df": thresholds_df,
    }


def render_cohort_53a_summary(
    *,
    ncc_curves_df: pd.DataFrame,
    diameters_df: pd.DataFrame,
    diameter_filter_summary_df: pd.DataFrame,
    func_anat_offsets_df: pd.DataFrame,
    hcr_offsets_df: pd.DataFrame,
    thresholds_df: pd.DataFrame,
    gene_order: list[str] | None = None,
    gene_colors: dict[str, str] | None = None,
) -> matplotlib.figure.Figure:
    """Render a 2x2 pooled cohort analogue of single-fish [53a]."""
    fig, axes = plt.subplots(2, 2, figsize=(15, 10), constrained_layout=True)
    ax_ncc, ax_diam = axes[0, 0], axes[0, 1]
    ax_func, ax_hcr = axes[1, 0], axes[1, 1]

    def _annotate_sample_sizes_no_overlap(
        ax: plt.Axes,
        labels: list[tuple[float, np.ndarray]],
        *,
        fontsize: int = 7,
    ) -> None:
        """Place n-labels with small y-offset collision avoidance for nearby x positions."""
        if not labels:
            return
        finite_arrays = [arr[np.isfinite(arr)] for _, arr in labels]
        finite_arrays = [arr for arr in finite_arrays if arr.size]
        if not finite_arrays:
            return
        y_min_data = float(np.min([float(np.nanmin(arr)) for arr in finite_arrays]))
        y_max_data = float(np.max([float(np.nanmax(arr)) for arr in finite_arrays]))
        y_span = max(1e-6, y_max_data - y_min_data)
        y_pad = 0.03 * y_span
        min_sep = 0.05 * y_span
        x_neighbor_thresh = 1.1
        placed: list[tuple[float, float]] = []
        top_used = y_max_data
        for xpos, arr in sorted(labels, key=lambda item: float(item[0])):
            arr_f = arr[np.isfinite(arr)]
            if arr_f.size == 0:
                continue
            y = float(np.nanmax(arr_f)) + y_pad
            while any(abs(float(xpos) - px) <= x_neighbor_thresh and abs(y - py) < min_sep for px, py in placed):
                y += min_sep
            ax.text(float(xpos), y, f"n={int(arr_f.size)}", ha="center", va="bottom", fontsize=fontsize)
            placed.append((float(xpos), y))
            top_used = max(top_used, y)
        bottom, top = ax.get_ylim()
        target_top = max(float(top), top_used + 0.08 * y_span)
        if target_top > float(top):
            ax.set_ylim(float(bottom), target_top)

    if not ncc_curves_df.empty:
        fish_ids = sorted(ncc_curves_df["fish_id"].astype(str).unique().tolist())
        fish_palette = {fid: plt.cm.tab10(i % 10) for i, fid in enumerate(fish_ids)}
        for (fish_id, plane_label), sub in ncc_curves_df.groupby(["fish_id", "plane_label"], dropna=False):
            ordered = sub.sort_values("z_idx")
            ax_ncc.plot(
                ordered["z_idx"].to_numpy(dtype=float),
                ordered["ncc_score"].to_numpy(dtype=float),
                color=fish_palette[str(fish_id)],
                linewidth=1.0,
                alpha=0.70,
            )
            peak = ordered.loc[(ordered["z_idx"] == ordered["best_z"])]
            if peak.empty and not ordered.empty:
                peak = ordered.iloc[[int(np.nanargmax(pd.to_numeric(ordered["ncc_score"], errors="coerce").to_numpy(dtype=float)))]]
            if not peak.empty:
                ax_ncc.scatter(
                    peak["z_idx"].to_numpy(dtype=float),
                    peak["ncc_score"].to_numpy(dtype=float),
                    color=fish_palette[str(fish_id)],
                    s=16,
                    alpha=0.95,
                )
        handles = [plt.Line2D([0], [0], color=fish_palette[fid], lw=2, label=fid) for fid in fish_ids]
        ax_ncc.legend(handles=handles, title="Fish", frameon=False, fontsize=8, title_fontsize=9)
        ax_ncc.set_xlabel("Z index")
        ax_ncc.set_ylabel("NCC score")
        ax_ncc.set_title("Plane Registration Match Quality")
        ax_ncc.grid(alpha=0.2, axis="y")
    else:
        ax_ncc.text(0.5, 0.5, "No NCC curves found", ha="center", va="center", transform=ax_ncc.transAxes)
        ax_ncc.set_axis_off()

    if not diameters_df.empty:
        dataset_order = ["Anatomy", "Functional", "HCR"]
        fish_ids = sorted(diameters_df["fish_id"].astype(str).unique().tolist())
        fish_palette = {fid: plt.cm.tab10(i % 10) for i, fid in enumerate(fish_ids)}
        metric_specs = [(d, axis) for d in dataset_order for axis in ("XY", "Z")]
        block_width = max(1, len(fish_ids)) + 1
        positions: list[float] = []
        values: list[np.ndarray] = []
        colors: list[Any] = []
        metric_centers: list[float] = []
        metric_labels: list[str] = []
        for metric_idx, (dataset, axis_name) in enumerate(metric_specs):
            start = metric_idx * block_width + 1
            center = start + (max(1, len(fish_ids)) - 1) / 2.0
            metric_centers.append(center)
            metric_labels.append(f"{dataset}\n{axis_name}")
            for fish_idx, fish_id in enumerate(fish_ids):
                sub = diameters_df[
                    (diameters_df["dataset"].astype(str) == dataset) & (diameters_df["fish_id"].astype(str) == str(fish_id))
                ].copy()
                col = "xy_um" if axis_name == "XY" else "z_um"
                arr = pd.to_numeric(sub.get(col, pd.Series(dtype=float)), errors="coerce").to_numpy(dtype=float)
                arr = arr[np.isfinite(arr)]
                if arr.size == 0:
                    continue
                xpos = float(start + fish_idx)
                positions.append(xpos)
                values.append(arr)
                colors.append(fish_palette[str(fish_id)])
        if values:
            vp = ax_diam.violinplot(values, positions=positions, widths=0.85, showmeans=False, showmedians=False, showextrema=False)
            for body, color in zip(vp["bodies"], colors):
                body.set_facecolor(color)
                body.set_edgecolor("black")
                body.set_alpha(0.65)
            _annotate_sample_sizes_no_overlap(ax_diam, list(zip(positions, values, strict=False)), fontsize=7)
            q95_df = diameter_filter_summary_df.copy()
            if not q95_df.empty and {"fish_id", "dataset", "xy_q95_um"}.issubset(q95_df.columns):
                for metric_idx, (dataset, axis_name) in enumerate(metric_specs):
                    if axis_name != "XY":
                        continue
                    start = metric_idx * block_width + 1
                    for fish_idx, fish_id in enumerate(fish_ids):
                        qsub = q95_df[
                            (q95_df["fish_id"].astype(str) == str(fish_id)) & (q95_df["dataset"].astype(str) == dataset)
                        ]
                        if qsub.empty:
                            continue
                        q95_val = pd.to_numeric(qsub["xy_q95_um"], errors="coerce").dropna()
                        if q95_val.empty:
                            continue
                        y = float(np.median(q95_val.to_numpy(dtype=float)))
                        xpos = float(start + fish_idx)
                        ax_diam.hlines(y, xpos - 0.35, xpos + 0.35, colors=fish_palette[str(fish_id)], linestyles="--", linewidth=1.0)
            ax_diam.set_xticks(metric_centers)
            ax_diam.set_xticklabels(metric_labels, fontsize=8)
            ax_diam.set_ylabel("Diameter (µm)")
            ax_diam.set_title("Segmentation Size Quality")
            handles = [plt.Line2D([0], [0], color=fish_palette[fid], lw=2, label=fid) for fid in fish_ids]
            ax_diam.legend(handles=handles, title="Fish", frameon=False, fontsize=8, title_fontsize=9, loc="upper right")
            ax_diam.grid(alpha=0.2, axis="y")
        else:
            ax_diam.text(0.5, 0.5, "No diameter values found", ha="center", va="center", transform=ax_diam.transAxes)
            ax_diam.set_axis_off()
    else:
        ax_diam.text(0.5, 0.5, "No diameter table found", ha="center", va="center", transform=ax_diam.transAxes)
        ax_diam.set_axis_off()

    r50_xy = float(pd.to_numeric(thresholds_df.get("anat_r50_xy_um", pd.Series([np.nan])), errors="coerce").iloc[0]) if not thresholds_df.empty else np.nan
    r50_z = float(pd.to_numeric(thresholds_df.get("anat_r50_z_um", pd.Series([np.nan])), errors="coerce").iloc[0]) if not thresholds_df.empty else np.nan

    if not func_anat_offsets_df.empty:
        fish_ids = sorted(func_anat_offsets_df["fish_id"].astype(str).unique().tolist())
        fish_palette = {fid: plt.cm.tab10(i % 10) for i, fid in enumerate(fish_ids)}
        axis_specs = [("xy", "XY"), ("z", "Z")]
        block_width = max(1, len(fish_ids)) + 1
        positions: list[float] = []
        values: list[np.ndarray] = []
        colors: list[Any] = []
        centers: list[float] = []
        labels: list[str] = []
        for axis_idx, (axis_key, axis_label) in enumerate(axis_specs):
            start = axis_idx * block_width + 1
            centers.append(start + (max(1, len(fish_ids)) - 1) / 2.0)
            labels.append(axis_label)
            for fish_idx, fish_id in enumerate(fish_ids):
                sub = func_anat_offsets_df[
                    (func_anat_offsets_df["fish_id"].astype(str) == str(fish_id))
                    & (func_anat_offsets_df["axis"].astype(str).str.lower() == axis_key)
                ]
                arr = pd.to_numeric(sub.get("offset_um", pd.Series(dtype=float)), errors="coerce").to_numpy(dtype=float)
                arr = arr[np.isfinite(arr)]
                if arr.size == 0:
                    continue
                xpos = float(start + fish_idx)
                positions.append(xpos)
                values.append(arr)
                colors.append(fish_palette[str(fish_id)])
        if values:
            vp = ax_func.violinplot(values, positions=positions, widths=0.85, showmeans=False, showmedians=False, showextrema=False)
            for body, color in zip(vp["bodies"], colors):
                body.set_facecolor(color)
                body.set_edgecolor("black")
                body.set_alpha(0.75)
            _annotate_sample_sizes_no_overlap(ax_func, list(zip(positions, values, strict=False)), fontsize=7)
            # Show per-fish anatomy R50 references derived from per-fish anatomy diameters.
            if not diameters_df.empty and {"fish_id", "dataset", "xy_um", "z_um"}.issubset(diameters_df.columns):
                anat_by_fish = diameters_df[diameters_df["dataset"].astype(str) == "Anatomy"].copy()
                for axis_idx, (axis_key, _axis_label) in enumerate(axis_specs):
                    start = axis_idx * block_width + 1
                    for fish_idx, fish_id in enumerate(fish_ids):
                        sub = anat_by_fish[anat_by_fish["fish_id"].astype(str) == str(fish_id)]
                        if sub.empty:
                            continue
                        source_col = "xy_um" if axis_key == "xy" else "z_um"
                        vals = pd.to_numeric(sub[source_col], errors="coerce").to_numpy(dtype=float)
                        vals = vals[np.isfinite(vals)]
                        if vals.size == 0:
                            continue
                        r50_val = float(np.median(vals) / 2.0)
                        xpos = float(start + fish_idx)
                        ax_func.hlines(r50_val, xpos - 0.35, xpos + 0.35, colors=fish_palette[str(fish_id)], linestyles="--", linewidth=1.0)
            elif np.isfinite(r50_xy) or np.isfinite(r50_z):
                if np.isfinite(r50_xy):
                    start = 1
                    ax_func.hlines(
                        float(r50_xy),
                        start - 0.4,
                        start + max(0, len(fish_ids) - 1) + 0.4,
                        colors="#c1121f",
                        linestyles="--",
                        linewidth=1.1,
                    )
                if np.isfinite(r50_z):
                    start = block_width + 1
                    ax_func.hlines(
                        float(r50_z),
                        start - 0.4,
                        start + max(0, len(fish_ids) - 1) + 0.4,
                        colors="#7f1d1d",
                        linestyles=":",
                        linewidth=1.1,
                    )
            ax_func.set_xticks(centers)
            ax_func.set_xticklabels(labels)
            ax_func.set_ylabel("Distance (µm)")
            ax_func.set_title("Functional→Anatomy Match Quality")
            handles = [plt.Line2D([0], [0], color=fish_palette[fid], lw=2, label=fid) for fid in fish_ids]
            ax_func.legend(handles=handles, title="Fish", frameon=False, fontsize=8, title_fontsize=9, loc="upper right")
            ax_func.grid(alpha=0.2, axis="y")
        else:
            ax_func.text(0.5, 0.5, "No functional→anatomy offsets found", ha="center", va="center", transform=ax_func.transAxes)
            ax_func.set_axis_off()
    else:
        ax_func.text(0.5, 0.5, "No functional→anatomy offsets found", ha="center", va="center", transform=ax_func.transAxes)
        ax_func.set_axis_off()

    if not hcr_offsets_df.empty:
        order = list(gene_order or [])
        if not order:
            order = sorted(hcr_offsets_df["gene"].astype(str).unique().tolist())
        fish_ids = sorted(hcr_offsets_df["fish_id"].astype(str).unique().tolist())
        fish_palette = {fid: plt.cm.tab10(i % 10) for i, fid in enumerate(fish_ids)}
        anat_r50_by_fish: dict[str, float] = {}
        if not diameters_df.empty and {"fish_id", "dataset", "xy_um"}.issubset(diameters_df.columns):
            anat_by_fish = diameters_df[diameters_df["dataset"].astype(str) == "Anatomy"].copy()
            for fish_id in fish_ids:
                sub = anat_by_fish[anat_by_fish["fish_id"].astype(str) == str(fish_id)]
                vals = pd.to_numeric(sub.get("xy_um", pd.Series(dtype=float)), errors="coerce").to_numpy(dtype=float)
                vals = vals[np.isfinite(vals)]
                if vals.size:
                    anat_r50_by_fish[str(fish_id)] = float(np.median(vals) / 2.0)
        axis_specs = [("xy_um", "XY")]
        block_width = max(1, len(fish_ids)) + 1
        xticks: list[float] = []
        xticklabels: list[str] = []
        hcr_sample_labels: list[tuple[float, np.ndarray]] = []
        for gene_idx, gene in enumerate(order):
            start = gene_idx * block_width + 1
            xticks.append(start + (max(1, len(fish_ids)) - 1) / 2.0)
            xticklabels.append(str(gene))
            gene_sub = hcr_offsets_df[hcr_offsets_df["gene"].astype(str) == str(gene)].copy()
            if gene_sub.empty:
                continue
            for fish_idx, fish_id in enumerate(fish_ids):
                sub = gene_sub[gene_sub["fish_id"].astype(str) == str(fish_id)].copy()
                if sub.empty:
                    continue
                arr = pd.to_numeric(sub.get("xy_um", pd.Series(dtype=float)), errors="coerce").to_numpy(dtype=float)
                arr = arr[np.isfinite(arr)]
                if arr.size == 0:
                    continue
                xpos = float(start + fish_idx)
                hcr_sample_labels.append((xpos, arr))
                xvals = np.full(arr.shape, xpos)
                jitter = np.random.default_rng(0).uniform(-0.12, 0.12, size=arr.shape[0]) if arr.size else np.array([], dtype=float)
                ax_hcr.scatter(
                    xvals + jitter,
                    arr,
                    s=16,
                    alpha=0.70,
                    color=fish_palette[str(fish_id)],
                    edgecolors="black",
                    linewidths=0.3,
                )
                bp = ax_hcr.boxplot([arr], positions=[xpos], widths=0.5, patch_artist=True, showfliers=False)
                for patch in bp["boxes"]:
                    patch.set_facecolor(fish_palette[str(fish_id)])
                    patch.set_alpha(0.30)
                    patch.set_edgecolor("black")
                r50_val = anat_r50_by_fish.get(str(fish_id))
                if r50_val is not None and np.isfinite(r50_val):
                    ax_hcr.hlines(float(r50_val), xpos - 0.28, xpos + 0.28, colors=fish_palette[str(fish_id)], linestyles="--", linewidth=1.0)
        _annotate_sample_sizes_no_overlap(ax_hcr, hcr_sample_labels, fontsize=7)
        max_gene_blocks = max(1, len(order))
        x_left = 1.0 - 0.5
        x_right = float((max_gene_blocks - 1) * block_width + max(1, len(fish_ids))) + 0.5
        ax_hcr.set_xlim(x_left, x_right)
        ax_hcr.set_xticks(xticks)
        ax_hcr.set_xticklabels(xticklabels, fontsize=8)
        ax_hcr.set_ylabel("Distance (µm)")
        ax_hcr.set_title("In-Plane HCR→Anatomy Match Quality")
        ax_hcr.grid(alpha=0.2, axis="y")
        handles = [plt.Line2D([0], [0], color=fish_palette[fid], lw=2, label=fid) for fid in fish_ids]
        ref_handles, ref_labels = ax_hcr.get_legend_handles_labels()
        ax_hcr.legend(handles=handles + ref_handles, labels=[*fish_ids, *ref_labels], title="Fish", frameon=False, fontsize=8, title_fontsize=9, loc="upper right")
    else:
        ax_hcr.text(0.5, 0.5, "No HCR offsets found", ha="center", va="center", transform=ax_hcr.transAxes)
        ax_hcr.set_axis_off()

    fig.suptitle("Cohort [53a] summary", fontsize=14)
    return fig


__all__ = [
    "build_best_plane_modality_merge_grid",
    "collect_cohort_53a_tables",
    "compute_anatomy_median_xy_radius_um",
    "build_round_channel_mip_grid",
    "render_cohort_53a_summary",
    "show_centroid_match_qa_stage",
    "show_functional_label_overlay_stage",
    "show_region_shift_square_selector_stage",
    "show_registration_overlay_stage",
]
