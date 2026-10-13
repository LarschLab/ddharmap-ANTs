"""Read-only per-plane registration review from persisted writer products."""
from __future__ import annotations

import json
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from ..context import infer_anat_labels_path
from ..native_paths import native_product_path
from ..matching import validate_anatomy_label_z_provenance, resolve_anatomy_label_z
from .qa import _read_tiff, _normalize_for_display, _label_boundary_overlay, _plane_label_path


def registration_review_product_paths(context):
    """Resolve the same products as make-functional-registration-qc, read-only."""
    product = context.product_path
    return dict(
        fish_id=context.fish_id,
        plane_refs_summary_path=product("make-functional-registration-qc", "plane_refs_summary_qc.json"),
        # Anatomy preparation writes the canonical fish tree even for staged runs
        # (prepared_in_vivo_anatomy_path); historical freezes omit that stage.
        anatomy_stack_path=native_product_path(context.fish_dir, "prepare-in-vivo-anatomy-stack",
                                               f"{context.fish_id}_anatomy_2P_GCaMP.nrrd"),
        anatomy_labels_path=infer_anat_labels_path(context.fish_dir, context.fish_id),
        suite2p_label_dir=product("transform-functional-rois-to-anatomy", "functional/native"),
        functional_reference_dir=product("prepare-functional-reference-stacks", "functional/raw"),
        positioned_reference_dir=product("register-functional-to-anatomy", "ncc/inplane_registration_comparison"),
    )


def _registered_reference_display(warped, method):
    # NCC's persisted metric TIFF contains the native intensity offset and zero
    # canvas padding. Its writer display normalizes before placement. Apply the
    # same review percentiles within the placed FOV so padding is not black level.
    if method == "ncc_xy":
        yy, xx = np.nonzero(warped)
        if len(yy):
            bounds = (slice(yy.min(), yy.max() + 1), slice(xx.min(), xx.max() + 1))
            display = np.zeros(warped.shape, dtype=np.float32)
            display[bounds] = _normalize_for_display(warped[bounds])
            return display
    return _normalize_for_display(warped)


def render_registration_plane_review(
    *, fish_id, plane_refs_summary_path, anatomy_stack_path, anatomy_labels_path,
    suite2p_label_dir, functional_reference_dir, positioned_reference_dir,
):
    """Four panels per saved plane; no registration, scoring, or product writes.

    Uses the writer's 1–99.5 percentile normalization and thin label boundaries.
    The registered image is selected by the saved active method, including NCC-xy.
    For NCC metric TIFFs, contrast is normalized within the placed FOV, as in
    the writer method-comparison display. Anatomy panels share the full canonical
    grid, without cropping or resampling.
    """
    refs = json.loads(Path(plane_refs_summary_path).read_text())
    if not isinstance(refs, list) or not refs:
        raise ValueError("Registration QC plane summary has no planes")
    refs = sorted(refs, key=lambda row: int(row["index"]))
    indices = [int(row["index"]) for row in refs]
    if len(set(indices)) != len(indices):
        raise ValueError("Duplicate registration QC planes")
    if not all(str(row["label"]).startswith(fish_id + "_plane") for row in refs):
        raise ValueError("Registration QC plane summary belongs to another fish")
    anatomy = _read_tiff(anatomy_stack_path)
    if anatomy_labels_path is None:
        raise FileNotFoundError(f"No anatomy label stack found for {fish_id}")
    labels = _read_tiff(anatomy_labels_path)
    errors = validate_anatomy_label_z_provenance(refs, labels.shape[0])
    if errors:
        raise ValueError("Invalid persisted anatomy-label Z: " + "; ".join(errors))
    positioned_dir = Path(positioned_reference_dir)
    comparison_path = positioned_dir / "inplane_registration_comparison.csv"
    comparison = pd.read_csv(comparison_path) if comparison_path.is_file() else pd.DataFrame()

    fig = plt.figure(figsize=(16, 3.8 * len(refs) + .5), constrained_layout=True)
    fig.suptitle(f"{fish_id}: functional-to-anatomy alignment on every plane", fontsize=15, fontweight="bold")
    rows = np.atleast_1d(fig.subfigures(len(refs), 1, squeeze=False)).ravel()
    for subfig, ref in zip(rows, refs):
        plane, label, z = int(ref["index"]), str(ref["label"]), int(ref["best_z"])
        label_z = resolve_anatomy_label_z(ref, labels.shape[0])
        method = str(ref.get("inplane_active_method") or ref.get("tform_src") or "")
        if method not in {"ants_rigid_affine", "ncc_xy"}:
            raise ValueError(f"Unsupported saved registration method for plane {plane}: {method}")
        warped_path = positioned_dir / f"{label}_{method}_warped.tif"
        # Prefer same-root references; historical summaries may name frozen paths.
        reference_path = Path(functional_reference_dir) / f"{label}_ref_norm.tif"
        if not reference_path.is_file():
            reference_path = Path(ref["reference_norm_path"])
        native = _read_tiff(reference_path)
        roi_path = _plane_label_path(Path(suite2p_label_dir), fish_id, plane, label)
        if roi_path is None:
            raise FileNotFoundError(f"Native Suite2p labels missing for plane {plane}")
        rois = _read_tiff(roi_path)
        anat = anatomy[z]
        anat_labels = labels[label_z]
        warped = _read_tiff(warped_path)
        if warped.shape != anat.shape or anat_labels.shape != anat.shape or rois.shape != native.shape:
            raise ValueError(f"Persisted image/label grid mismatch on plane {plane}")
        anat_norm, warped_norm = _normalize_for_display(anat), _registered_reference_display(warped, method)
        overlay = np.stack([anat_norm, warped_norm, anat_norm], axis=-1)
        metric = []
        if not comparison.empty:
            saved = comparison.loc[comparison["plane_idx"].eq(plane) & comparison["method"].eq(method)]
            if len(saved) != 1:
                raise ValueError(f"Ambiguous or missing saved comparison for plane {plane}, {method}")
            for key, caption in (("post_ncc", "post NCC"), ("bestz_max_score", "Z-selection NCC")):
                value = saved.iloc[0].get(key)
                if pd.notna(value) and np.isfinite(float(value)):
                    metric.append(f"{caption}={float(value):.3f}")
        elif np.isfinite(float((ref.get("ncc_xy") or {}).get("score", np.nan))):
            metric.append(f"NCC placement={float(ref['ncc_xy']['score']):.3f}")
        method_title = {"ants_rigid_affine": "ANTs rigid+affine", "ncc_xy": "NCC-xy"}[method]
        subfig.suptitle(f"Plane {plane} · best Z={z} · {method_title}" + (" · " + " · ".join(metric) if metric else ""),
                         fontsize=11, x=.02, ha="left")
        axes = subfig.subplots(1, 4)
        panels = (
            (_label_boundary_overlay(native, rois, color=(.05, .95, 1), alpha=.95), None,
             f"Native functional + Suite2p ROIs\nn={len(np.unique(rois[rois > 0]))}"),
            (_label_boundary_overlay(anat, anat_labels, color=(1, .15, .05), alpha=.95), None,
             f"In vivo anatomy + label outlines\nlabel Z={label_z}"),
            (warped_norm, "gray", "Registered functional reference\n" +
             ("contrast within placed FOV" if method == "ncc_xy" else "in anatomy space")),
            (overlay, None, "Overlay\nmagenta=anatomy · green=functional"),
        )
        for col, (ax, (image, cmap, title)) in enumerate(zip(axes, panels)):
            extent = None if col == 0 else (-.5, anat.shape[1] - .5, anat.shape[0] - .5, -.5)
            ax.imshow(image, cmap=cmap, extent=extent, interpolation="nearest")
            ax.set_title(title, fontsize=11)
            ax.axis("off")
    return fig
