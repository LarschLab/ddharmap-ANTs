"""Read-only ROI/anatomy geometry review views for QC notebook 03.

The helpers in this module consume the geometry-only outputs written by
``match-roi-to-anatomy``.  They never recompute matching, attach identity or
activity, promote outputs, or mutate the reviewed tables.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any, Iterable

import matplotlib.pyplot as plt
from matplotlib.collections import LineCollection
from matplotlib.lines import Line2D
import numpy as np
import pandas as pd
import tifffile

from ..matching import resolve_anatomy_label_z
from ..spatial import imread_any, norm01


REQUIRED_GEOMETRY_COLUMNS = (
    "plane_idx",
    "func_label",
    "selected_anat_label",
    "centroid_x_func_anat",
    "centroid_y_func_anat",
    "has_unique_anat_match",
    "plane_match_outcome",
    "claim_outcome",
)

FORBIDDEN_POST_GEOMETRY_COLUMNS = {
    "identity_label",
    "has_identity_assigned",
    "response_class",
    "response_summary_class",
    "response_is_active",
    "bpi",
    "bpi_category",
    "gene",
}


@dataclass(frozen=True)
class GeometryReviewBundle:
    """Persisted geometry tables and provenance for one staged run."""

    fish_id: str
    geometry_root: Path
    matches_path: Path
    summary_path: Path
    plane_meta_path: Path
    matches: pd.DataFrame
    summary: pd.DataFrame
    plane_meta: pd.DataFrame
    anatomy_stack_path: Path | None = None
    anatomy_labels_path: Path | None = None
    plane_refs_path: Path | None = None
    transformed_labels: pd.DataFrame | None = None


def _as_bool(values: pd.Series) -> pd.Series:
    if pd.api.types.is_bool_dtype(values):
        return values.fillna(False).astype(bool)
    return values.astype(str).str.strip().str.lower().isin({"true", "1", "yes", "y"})


def _fish_ids(df: pd.DataFrame) -> set[str]:
    if "fish_id" not in df.columns:
        return set()
    return {str(value) for value in df["fish_id"].dropna().astype(str) if str(value).strip()}


def load_geometry_review_bundle(
    geometry_root: str | Path,
    *,
    expected_fish_id: str | None = None,
    anatomy_stack_path: str | Path | None = None,
    anatomy_labels_path: str | Path | None = None,
    plane_refs_path: str | Path | None = None,
    transformed_roi_root: str | Path | None = None,
) -> GeometryReviewBundle:
    """Load and validate the three outputs of ``match-roi-to-anatomy``."""

    root = Path(geometry_root).expanduser().resolve()
    matches_path = root / "functional_roi_anatomy_matches.csv"
    summary_path = root / "functional_roi_anatomy_match_by_plane.csv"
    plane_meta_path = root / "functional_roi_anatomy_match_plane_meta.csv"
    missing_files = [path.name for path in (matches_path, summary_path, plane_meta_path) if not path.is_file()]
    if missing_files:
        raise FileNotFoundError(f"Geometry review inputs missing under {root}: {', '.join(missing_files)}")

    matches = pd.read_csv(matches_path)
    summary = pd.read_csv(summary_path)
    plane_meta = pd.read_csv(plane_meta_path)
    missing_columns = [column for column in REQUIRED_GEOMETRY_COLUMNS if column not in matches.columns]
    if missing_columns:
        raise ValueError(f"Geometry table is missing required columns: {', '.join(missing_columns)}")
    forbidden = sorted(FORBIDDEN_POST_GEOMETRY_COLUMNS.intersection(matches.columns))
    if forbidden:
        raise ValueError(
            "Geometry review requires the geometry-only stage output; post-geometry columns found: "
            + ", ".join(forbidden)
        )
    if matches.duplicated(["plane_idx", "func_label"]).any():
        raise ValueError("Geometry table has duplicate (plane_idx, func_label) keys.")

    ids = _fish_ids(matches)
    if len(ids) > 1:
        raise ValueError(f"Geometry table contains multiple fish IDs: {sorted(ids)}")
    observed_id = next(iter(ids), "")
    if expected_fish_id is not None and observed_id and observed_id != str(expected_fish_id):
        raise ValueError(f"Geometry fish mismatch: expected {expected_fish_id}, found {observed_id}")
    fish_id = str(expected_fish_id or observed_id)
    if not fish_id:
        raise ValueError("Fish identity is absent from both the table and expected_fish_id.")

    spatial_inputs = (anatomy_stack_path, anatomy_labels_path, plane_refs_path, transformed_roi_root)
    if any(value is not None for value in spatial_inputs) and any(value is None for value in spatial_inputs):
        raise ValueError("Geometry spatial review requires anatomy, anatomy labels, plane references, and transformed ROI labels together")
    transformed_labels: pd.DataFrame | None = None
    if all(value is not None for value in spatial_inputs):
        anatomy_stack = Path(anatomy_stack_path).expanduser().resolve()
        anatomy_labels = Path(anatomy_labels_path).expanduser().resolve()
        refs = Path(plane_refs_path).expanduser().resolve()
        transformed_dir = Path(transformed_roi_root).expanduser().resolve() / "functional" / "anatomy"
        for path, label in ((anatomy_stack, "anatomy stack"), (anatomy_labels, "anatomy labels"), (refs, "plane references")):
            if not path.is_file():
                raise FileNotFoundError(f"Missing {label}: {path}")
        label_paths = tuple(
            path
            for path in (tuple(sorted(transformed_dir.glob("*.tif"))) + tuple(sorted(transformed_dir.glob("*.tiff"))))
            if not path.name.startswith("._")
        )
        if not label_paths:
            raise FileNotFoundError(f"No anatomy-space transformed ROI labels found: {transformed_dir}")
        transformed_labels = pd.DataFrame(
            [{"plane_idx": _plane_from_path(path), "path": str(path)} for path in label_paths]
        )
        ref_rows = json.loads(refs.read_text())
        if not isinstance(ref_rows, list):
            raise ValueError(f"Plane references must be a JSON list: {refs}")
        refs_by_plane = {
            int(row.get("index", row.get("plane_index", -1))): row
            for row in ref_rows
            if isinstance(row, dict)
        }
        label_stack = np.asarray(imread_any(anatomy_labels))
        if label_stack.ndim != 3:
            raise ValueError(f"Anatomy labels must be a 3D stack: {anatomy_labels}")
        for plane_idx in sorted(pd.to_numeric(matches["plane_idx"], errors="coerce").dropna().astype(int).unique()):
            ref = refs_by_plane.get(int(plane_idx))
            if ref is None or ref.get("anat_label_z_mode") not in {"direct", "reverse"}:
                raise ValueError(
                    f"Geometry plane {plane_idx} lacks explicit direct/reverse anatomy-label Z provenance"
                )
            resolved_z = resolve_anatomy_label_z(ref, label_stack.shape[0])
            if ref.get("anat_label_z") in (None, "") or int(ref["anat_label_z"]) != resolved_z:
                raise ValueError(
                    f"Geometry plane {plane_idx} has inconsistent persisted anatomy-label Z provenance"
                )
            selected = pd.to_numeric(
                matches.loc[
                    pd.to_numeric(matches["plane_idx"], errors="coerce").eq(int(plane_idx))
                    & _as_bool(matches["has_unique_anat_match"]),
                    "selected_anat_label",
                ],
                errors="coerce",
            ).dropna().astype(int)
            available = set(np.unique(label_stack[resolved_z]).astype(int))
            missing_selected = sorted(set(selected[selected > 0].tolist()) - available)
            if missing_selected:
                raise ValueError(
                    f"Geometry plane {plane_idx} contains selected anatomy labels absent from resolved label Z={resolved_z}: "
                    + ", ".join(str(value) for value in missing_selected[:10])
                )

    return GeometryReviewBundle(
        fish_id=fish_id,
        geometry_root=root,
        matches_path=matches_path,
        summary_path=summary_path,
        plane_meta_path=plane_meta_path,
        matches=matches.copy(),
        summary=summary.copy(),
        plane_meta=plane_meta.copy(),
        anatomy_stack_path=Path(anatomy_stack_path).expanduser().resolve() if anatomy_stack_path is not None else None,
        anatomy_labels_path=Path(anatomy_labels_path).expanduser().resolve() if anatomy_labels_path is not None else None,
        plane_refs_path=Path(plane_refs_path).expanduser().resolve() if plane_refs_path is not None else None,
        transformed_labels=transformed_labels,
    )


def _plane_from_path(path: Path) -> int | None:
    import re

    match = re.search(r"plane(\d+)", path.name, re.IGNORECASE)
    return int(match.group(1)) if match else None


def summarize_geometry_review(matches: pd.DataFrame) -> pd.DataFrame:
    """Return decision-oriented per-plane counts from a geometry-only table."""

    df = matches.copy()
    df["_matched"] = _as_bool(df["has_unique_anat_match"])
    claim = df["claim_outcome"].fillna("").astype(str).str.lower()
    outcome = df["plane_match_outcome"].fillna("").astype(str).str.lower()
    df["_competition_lost"] = claim.str.contains("duplicate") | outcome.str.contains("lost")
    df["_no_overlap"] = outcome.str.contains("no anatomy overlap")
    df["_other_unmatched"] = (~df["_matched"]) & (~df["_competition_lost"]) & (~df["_no_overlap"])
    df["_distance"] = pd.to_numeric(df.get("selected_dist_um"), errors="coerce")
    df["_overlap"] = pd.to_numeric(df.get("selected_overlap_px"), errors="coerce")

    rows: list[dict[str, Any]] = []
    for plane_idx, group in df.groupby("plane_idx", sort=True, dropna=False):
        matched = group["_matched"]
        rows.append(
            {
                "plane_idx": int(plane_idx),
                "plane": str(group["plane"].iloc[0]) if "plane" in group.columns else f"plane{int(plane_idx)}",
                "n_rois": int(len(group)),
                "n_unique_matches": int(matched.sum()),
                "n_no_overlap": int(group["_no_overlap"].sum()),
                "n_competition_lost": int(group["_competition_lost"].sum()),
                "n_other_unmatched": int(group["_other_unmatched"].sum()),
                "unique_match_fraction": float(matched.mean()) if len(group) else np.nan,
                "median_stored_selected_distance": float(group.loc[matched, "_distance"].median()) if matched.any() else np.nan,
                "median_selected_overlap_px": float(group.loc[matched, "_overlap"].median()) if matched.any() else np.nan,
            }
        )
    return pd.DataFrame(rows)


def geometry_review_queue(
    matches: pd.DataFrame,
    *,
    plane_idx: int | None = None,
    outcomes: Iterable[str] = ("all",),
    max_overlap_px: float | None = None,
    min_distance_um: float | None = None,
) -> pd.DataFrame:
    """Filter the authoritative table into a reproducible review queue."""

    df = matches.copy()
    matched = _as_bool(df["has_unique_anat_match"])
    claim = df["claim_outcome"].fillna("").astype(str).str.lower()
    outcome = df["plane_match_outcome"].fillna("").astype(str).str.lower()
    categories = pd.Series("other-unmatched", index=df.index, dtype=object)
    categories.loc[matched] = "matched"
    categories.loc[claim.str.contains("duplicate") | outcome.str.contains("lost")] = "competition-lost"
    categories.loc[outcome.str.contains("no anatomy overlap")] = "no-overlap"
    df.insert(len(df.columns), "review_category", categories)

    if plane_idx is not None:
        df = df[pd.to_numeric(df["plane_idx"], errors="coerce").eq(int(plane_idx))]
    selected = {str(value).strip().lower() for value in outcomes}
    if selected and "all" not in selected:
        df = df[df["review_category"].isin(selected)]
    if max_overlap_px is not None and "selected_overlap_px" in df.columns:
        overlap = pd.to_numeric(df["selected_overlap_px"], errors="coerce")
        df = df[overlap.le(float(max_overlap_px)) | overlap.isna()]
    if min_distance_um is not None and "selected_dist_um" in df.columns:
        distance = pd.to_numeric(df["selected_dist_um"], errors="coerce")
        df = df[distance.ge(float(min_distance_um)) | distance.isna()]

    sort_columns = [column for column in ("plane_idx", "review_category", "selected_overlap_px", "selected_dist_um", "func_label") if column in df.columns]
    ascending = [True, True, True, False, True][: len(sort_columns)]
    return df.sort_values(sort_columns, ascending=ascending, na_position="last").reset_index(drop=True)


def render_geometry_review_summary(summary: pd.DataFrame, *, title: str = "ROI/anatomy geometry review"):
    """Render per-plane outcome counts and unique-match fractions."""

    if summary is None or summary.empty:
        raise ValueError("Geometry summary is empty.")
    plot_df = summary.sort_values("plane_idx").copy()
    x = np.arange(len(plot_df), dtype=float)
    fig, (ax_counts, ax_fraction) = plt.subplots(1, 2, figsize=(13, 4.8), gridspec_kw={"width_ratios": [2.2, 1]})
    bottom = np.zeros(len(plot_df), dtype=float)
    for column, label, color in (
        ("n_unique_matches", "unique match", "#2a9d8f"),
        ("n_no_overlap", "no overlap", "#8d99ae"),
        ("n_competition_lost", "competition lost", "#e76f51"),
        ("n_other_unmatched", "other unmatched", "#f4a261"),
    ):
        values = pd.to_numeric(plot_df[column], errors="coerce").fillna(0).to_numpy(dtype=float)
        ax_counts.bar(x, values, bottom=bottom, label=label, color=color)
        bottom += values
    labels = [str(value) for value in plot_df["plane_idx"].tolist()]
    ax_counts.set_xticks(x, labels)
    ax_counts.set_xlabel("Global plane index")
    ax_counts.set_ylabel("ROI count")
    ax_counts.legend(frameon=False, ncol=2)

    fractions = pd.to_numeric(plot_df["unique_match_fraction"], errors="coerce")
    ax_fraction.plot(x, fractions, "o-", color="#264653")
    ax_fraction.set_xticks(x, labels)
    ax_fraction.set_ylim(0, 1)
    ax_fraction.set_xlabel("Global plane index")
    ax_fraction.set_ylabel("Unique-match fraction")
    fig.suptitle(title)
    fig.tight_layout()
    return fig


def render_centroid_offset_review(matches: pd.DataFrame, *, title: str = "Centroid offsets in anatomy space") -> plt.Figure:
    """Draw matched functional-to-anatomy centroid offsets for every functional plane.

    The geometry table stores native functional centroids separately from the
    transformed functional centroids used here; both plotted endpoints are in
    anatomy-space pixels.
    This view therefore reports the accepted stage output directly and does not
    recompute transforms or matches.
    """
    required = {
        "plane_idx",
        "has_unique_anat_match",
        "centroid_x_func_anat",
        "centroid_y_func_anat",
        "centroid_x_anat",
        "centroid_y_anat",
    }
    missing = sorted(required.difference(matches.columns))
    if missing:
        raise ValueError("Centroid-offset review requires columns: " + ", ".join(missing))
    df = matches.copy()
    df = df[_as_bool(df["has_unique_anat_match"])].copy()
    for column in required.difference({"plane_idx", "has_unique_anat_match"}):
        df[column] = pd.to_numeric(df[column], errors="coerce")
    df = df.dropna(subset=list(required.difference({"has_unique_anat_match"})))
    planes = sorted(pd.to_numeric(matches["plane_idx"], errors="coerce").dropna().astype(int).unique())
    if not planes:
        raise ValueError("Centroid-offset review has no plane indices")
    ncols = 2
    nrows = int(np.ceil(len(planes) / ncols))
    fig, axes = plt.subplots(nrows, ncols, figsize=(14.0, 6.5 * nrows), squeeze=False, constrained_layout=True)
    for ax, plane in zip(axes.flat, planes):
        subset = df[df["plane_idx"].astype(int) == plane]
        if subset.empty:
            ax.text(0.5, 0.5, "No unique centroid pairs", ha="center", va="center", transform=ax.transAxes)
            ax.set_title(f"Plane {plane}")
            ax.set_axis_off()
            continue
        starts = subset[["centroid_x_func_anat", "centroid_y_func_anat"]].to_numpy(dtype=float)
        ends = subset[["centroid_x_anat", "centroid_y_anat"]].to_numpy(dtype=float)
        ax.add_collection(LineCollection(np.stack([starts, ends], axis=1), colors="#7f8c8d", linewidths=0.5, alpha=0.35))
        ax.scatter(starts[:, 0], starts[:, 1], s=8, color="#0072B2", alpha=0.65, label="Functional ROI centroid")
        ax.scatter(ends[:, 0], ends[:, 1], s=8, color="#D55E00", alpha=0.65, label="Anatomy-label centroid")
        ax.set_aspect("equal", adjustable="box")
        ax.invert_yaxis()
        ax.set_xlabel("Anatomy-space X (pixels)")
        ax.set_ylabel("Anatomy-space Y (pixels)")
        median = pd.to_numeric(subset.get("selected_dist_um"), errors="coerce").median()
        median_text = f"; stored distance {median:.2f} (verify physical units in Q0.1)" if pd.notna(median) else ""
        ax.set_title(f"Plane {plane}: {len(subset)} unique pairs{median_text}", fontsize=10)
        ax.grid(alpha=0.2)
        if plane == planes[0]:
            ax.legend(frameon=False, fontsize=8, loc="best")
    for ax in axes.flat[len(planes) :]:
        ax.axis("off")
    fig.suptitle(title, fontsize=13, fontweight="bold")
    return fig


def _spatial_plane_data(bundle: GeometryReviewBundle, plane_idx: int) -> tuple[np.ndarray, np.ndarray, np.ndarray, int, int]:
    """Load persisted anatomy, anatomy-label, and transformed-ROI data for one plane."""
    if any(value is None for value in (bundle.anatomy_stack_path, bundle.anatomy_labels_path, bundle.plane_refs_path, bundle.transformed_labels)):
        raise ValueError("This geometry bundle lacks the saved spatial inputs needed for anatomy-context review")
    refs = json.loads(bundle.plane_refs_path.read_text())
    if not isinstance(refs, list):
        raise ValueError(f"Plane references must be a JSON list: {bundle.plane_refs_path}")
    ref = next((row for row in refs if int(row.get("index", row.get("plane_index", -1))) == int(plane_idx)), None)
    if ref is None or "best_z" not in ref:
        raise KeyError(f"No best anatomy Z is recorded for plane {plane_idx}")
    best_z = int(ref["best_z"])
    anatomy = np.asarray(imread_any(bundle.anatomy_stack_path))
    anatomy_labels = np.asarray(imread_any(bundle.anatomy_labels_path))
    if anatomy.ndim != 3 or anatomy_labels.ndim != 3 or anatomy.shape != anatomy_labels.shape:
        raise ValueError("Anatomy intensity and anatomy-label volumes must be matching 3D stacks")
    if not 0 <= best_z < anatomy.shape[0]:
        raise ValueError(f"Best anatomy Z={best_z} is outside the anatomy stack")
    anatomy_label_z = resolve_anatomy_label_z(ref, anatomy_labels.shape[0], best_z=best_z)
    if not 0 <= anatomy_label_z < anatomy_labels.shape[0]:
        raise ValueError(
            f"Resolved anatomy-label Z={anatomy_label_z} is outside the label stack for plane {plane_idx}"
        )
    selected = bundle.transformed_labels[pd.to_numeric(bundle.transformed_labels["plane_idx"], errors="coerce") == int(plane_idx)]
    if selected.empty:
        raise FileNotFoundError(f"No transformed functional ROI labels found for plane {plane_idx}")
    functional_labels = np.squeeze(np.asarray(tifffile.imread(selected.iloc[0]["path"])))
    if functional_labels.ndim != 2 or functional_labels.shape != anatomy.shape[1:]:
        raise ValueError(f"Transformed functional labels do not match anatomy XY shape for plane {plane_idx}")
    return anatomy[best_z], anatomy_labels[anatomy_label_z], functional_labels, best_z, anatomy_label_z


def _label_boundaries(labels: np.ndarray) -> np.ndarray:
    image = np.asarray(labels)
    boundaries = np.zeros(image.shape, dtype=bool)
    boundaries[1:, :] |= (image[1:, :] != image[:-1, :]) & ((image[1:, :] != 0) | (image[:-1, :] != 0))
    boundaries[:, 1:] |= (image[:, 1:] != image[:, :-1]) & ((image[:, 1:] != 0) | (image[:, :-1] != 0))
    return boundaries


def _draw_label_context(ax: plt.Axes, anatomy: np.ndarray, anatomy_labels: np.ndarray, functional_labels: np.ndarray, *, title: str) -> None:
    ax.imshow(norm01(anatomy), cmap="gray", interpolation="nearest")
    for labels, color in ((anatomy_labels, "#00A651"), (functional_labels, "#D81B60")):
        boundaries = _label_boundaries(labels)
        ax.contour(boundaries, levels=[0.5], colors=[color], linewidths=0.55)
    ax.set_title(title, fontsize=10)
    ax.axis("off")


def render_geometry_placement_overview(bundle: GeometryReviewBundle) -> plt.Figure:
    """Show anatomy and transformed functional-label outlines at every saved best-Z plane."""
    if bundle.transformed_labels is None:
        raise ValueError("This geometry bundle lacks saved spatial inputs for the placement overview")
    planes = sorted(pd.to_numeric(bundle.transformed_labels["plane_idx"], errors="coerce").dropna().astype(int).unique())
    ncols = 2
    nrows = int(np.ceil(len(planes) / ncols))
    fig, axes = plt.subplots(nrows, ncols, figsize=(14.0, 6.8 * nrows), squeeze=False, constrained_layout=True)
    for ax, plane in zip(axes.flat, planes):
        anatomy, anatomy_labels, functional_labels, best_z, anatomy_label_z = _spatial_plane_data(bundle, plane)
        _draw_label_context(
            ax,
            anatomy,
            anatomy_labels,
            functional_labels,
            title=f"Functional plane {plane}: anatomy Z={best_z}, label Z={anatomy_label_z}",
        )
    for ax in axes.flat[len(planes) :]:
        ax.axis("off")
    fig.legend(
        handles=[Line2D([], [], color="#00A651", label="Anatomy-label outline"), Line2D([], [], color="#D81B60", label="Functional ROI outline")],
        loc="lower center", ncol=2, frameon=False,
    )
    fig.suptitle(f"{bundle.fish_id}: in-plane placement at each best anatomy Z", fontsize=13, fontweight="bold")
    return fig


def _anatomy_radius_reference_um(bundle: GeometryReviewBundle) -> float | None:
    """Estimate the median visible anatomy-label radius from verified image geometry."""
    matches = bundle.matches[_as_bool(bundle.matches["has_unique_anat_match"])].copy()
    columns = ("centroid_x_func_anat", "centroid_y_func_anat", "centroid_x_anat", "centroid_y_anat", "selected_dist_um")
    if any(column not in matches for column in columns):
        return None
    values = matches.loc[:, columns].apply(pd.to_numeric, errors="coerce").dropna()
    try:
        import SimpleITK as sitk

        spacing_x, spacing_y, _ = sitk.ReadImage(str(bundle.anatomy_stack_path)).GetSpacing()
        pixel_area_um2 = float(spacing_x) * float(spacing_y)
    except Exception:
        return None
    if "selected_anat_label" not in matches:
        return None
    radii_um: list[float] = []
    for plane in sorted(matches["plane_idx"].dropna().astype(int).unique()):
        _, labels, _, _, _ = _spatial_plane_data(bundle, plane)
        areas = np.bincount(np.asarray(labels, dtype=np.int64).ravel())
        selected_labels = pd.to_numeric(matches.loc[matches["plane_idx"].eq(plane), "selected_anat_label"], errors="coerce").dropna().astype(int).unique()
        for label in selected_labels:
            if 0 < label < len(areas) and areas[label] > 0:
                radii_um.append(float(np.sqrt(areas[label] * pixel_area_um2 / np.pi)))
    return float(np.median(radii_um)) if radii_um else None


def render_centroid_offset_detail(bundle: GeometryReviewBundle, *, plane_idx: int) -> plt.Figure:
    """Show one plane's anatomy context and all-plane XY-offset violins in microns."""
    anatomy, anatomy_labels, functional_labels, best_z, anatomy_label_z = _spatial_plane_data(bundle, int(plane_idx))
    matches = bundle.matches.copy()
    matched = _as_bool(matches["has_unique_anat_match"])
    selected = matches[matched & pd.to_numeric(matches["plane_idx"], errors="coerce").eq(int(plane_idx))].copy()
    fig, (context_ax, violin_ax) = plt.subplots(1, 2, figsize=(15.0, 7.5), gridspec_kw={"width_ratios": [1.25, 1]}, constrained_layout=True)
    _draw_label_context(
        context_ax,
        anatomy,
        anatomy_labels,
        functional_labels,
        title=f"Plane {plane_idx}: anatomy Z={best_z}, label Z={anatomy_label_z}",
    )
    required = ("centroid_x_func_anat", "centroid_y_func_anat", "centroid_x_anat", "centroid_y_anat")
    if all(column in selected for column in required):
        selected.loc[:, list(required)] = selected.loc[:, list(required)].apply(pd.to_numeric, errors="coerce")
        selected = selected.dropna(subset=list(required))
        segments = np.stack(
            [selected[["centroid_x_func_anat", "centroid_y_func_anat"]].to_numpy(), selected[["centroid_x_anat", "centroid_y_anat"]].to_numpy()], axis=1
        ) if not selected.empty else np.empty((0, 2, 2))
        if len(segments):
            # Keep the true segment length.  A high-contrast, foreground stroke
            # makes sub-cellular but valid offsets visible at full-plane scale.
            context_ax.add_collection(
                LineCollection(segments, colors="#FFD60A", linewidths=1.15, alpha=0.95, zorder=6)
            )
    context_ax.legend(
        handles=[
            Line2D([], [], color="#00A651", label="Anatomy-label outline"),
            Line2D([], [], color="#D81B60", label="Functional ROI outline"),
            Line2D([], [], color="#F4B400", label="Unique-match centroid offset"),
        ],
        loc="lower right", frameon=True, fontsize=8,
    )

    planes = sorted(matches["plane_idx"].dropna().astype(int).unique())
    distributions: list[np.ndarray] = []
    positions: list[int] = []
    missing_positions: list[int] = []
    for position, plane in enumerate(planes):
        values = pd.to_numeric(matches.loc[matched & matches["plane_idx"].eq(plane), "selected_dist_um"], errors="coerce").dropna().to_numpy()
        if len(values) >= 2:
            distributions.append(values)
            positions.append(position)
        else:
            missing_positions.append(position)
    if distributions:
        violin = violin_ax.violinplot(distributions, positions=positions, showmedians=True, showextrema=False)
        for body, position in zip(violin["bodies"], positions):
            body.set_facecolor("#0072B2" if planes[position] == int(plane_idx) else "#9ecae1")
            body.set_edgecolor("#264653")
            body.set_alpha(0.85)
        violin["cmedians"].set_color("#264653")
    for position in missing_positions:
        violin_ax.text(position, 0.0, "<2 pairs", ha="center", va="bottom", rotation=90, fontsize=8, color="#6c757d")
    radius = _anatomy_radius_reference_um(bundle)
    if radius is not None:
        violin_ax.axhline(radius, color="#D55E00", linestyle="--", linewidth=1.5, label=f"Median anatomy-label radius ({radius:.2f} µm)")
    violin_ax.set_xticks(np.arange(len(planes)), [str(plane) for plane in planes])
    violin_ax.set_xlabel("Functional plane")
    violin_ax.set_ylabel("Stored selected_dist_um (unit pending Q0.1 audit)")
    violin_ax.set_title("All-plane stored-distance distribution; selected plane highlighted", fontsize=10)
    violin_ax.grid(axis="y", alpha=0.2)
    if radius is not None:
        violin_ax.legend(frameon=False, fontsize=8)
    fig.suptitle(f"{bundle.fish_id}: centroid offsets with anatomy context", fontsize=13, fontweight="bold")
    return fig


def show_geometry_review_dashboard(bundle: GeometryReviewBundle, *, max_rows: int = 40) -> dict[str, Any]:
    """Display a read-only interactive queue when ipywidgets is available."""

    summary = summarize_geometry_review(bundle.matches)
    figure = render_geometry_review_summary(summary, title=f"{bundle.fish_id}: ROI/anatomy geometry")
    result: dict[str, Any] = {"summary": summary, "figure": figure, "interactive": False}
    try:
        import ipywidgets as widgets
        from IPython.display import display
    except Exception:
        return result

    planes = [("all", None)] + [(str(int(value)), int(value)) for value in sorted(bundle.matches["plane_idx"].dropna().astype(int).unique())]
    plane_widget = widgets.Dropdown(options=planes, value=None, description="Plane")
    category_widget = widgets.SelectMultiple(
        options=("matched", "no-overlap", "competition-lost", "other-unmatched"),
        value=("no-overlap", "competition-lost", "other-unmatched"),
        description="Queue",
    )
    output = widgets.Output()

    def _refresh(*_args: Any) -> None:
        queue = geometry_review_queue(
            bundle.matches,
            plane_idx=plane_widget.value,
            outcomes=category_widget.value or ("all",),
        )
        columns = [
            column
            for column in (
                "plane_idx",
                "func_label",
                "review_category",
                "plane_match_outcome",
                "claim_outcome",
                "selected_anat_label",
                "selected_overlap_px",
                "selected_dist_um",
                "n_overlap_candidates_any",
                "n_overlap_candidates_valid",
            )
            if column in queue.columns
        ]
        with output:
            output.clear_output(wait=True)
            print(f"Showing {min(len(queue), max_rows)} of {len(queue)} review rows")
            display(queue.loc[:, columns].head(int(max_rows)))

    plane_widget.observe(_refresh, names="value")
    category_widget.observe(_refresh, names="value")
    display(widgets.VBox([widgets.HBox([plane_widget, category_widget]), output]))
    _refresh()
    result.update({"interactive": True, "widgets": {"plane": plane_widget, "category": category_widget}})
    return result


def audit_xy_offset_units(
    matches: pd.DataFrame,
    *,
    dx_um: float,
    dy_um: float,
) -> pd.DataFrame:
    """Recompute physical XY offsets from frozen geometry centroids.

    This is deliberately an audit, not a replacement matcher.  It intersects
    the geometry-stage unique matches and makes the coordinate conversion
    explicit so a mislabeled pixel distance cannot be mistaken for microns.
    """
    required = {
        "plane_idx", "func_label", "has_unique_anat_match",
        "centroid_x_func_anat", "centroid_y_func_anat",
        "centroid_x_anat", "centroid_y_anat",
    }
    missing = sorted(required.difference(matches.columns))
    if missing:
        raise ValueError(f"Geometry table lacks XY-offset audit columns: {missing}")
    if not (np.isfinite(dx_um) and dx_um > 0 and np.isfinite(dy_um) and dy_um > 0):
        raise ValueError("dx_um and dy_um must be finite positive physical spacings")
    out = matches.loc[_as_bool(matches["has_unique_anat_match"])].copy()
    out["dx_px"] = pd.to_numeric(out["centroid_x_func_anat"], errors="coerce") - pd.to_numeric(out["centroid_x_anat"], errors="coerce")
    out["dy_px"] = pd.to_numeric(out["centroid_y_func_anat"], errors="coerce") - pd.to_numeric(out["centroid_y_anat"], errors="coerce")
    out["physical_xy_offset_um"] = np.hypot(out["dx_px"] * float(dx_um), out["dy_px"] * float(dy_um))
    out["reported_selected_dist_um"] = pd.to_numeric(out.get("selected_dist_um"), errors="coerce")
    out["reported_to_physical_ratio"] = out["reported_selected_dist_um"] / out["physical_xy_offset_um"]
    return out.dropna(subset=["physical_xy_offset_um"]).reset_index(drop=True)


def render_xy_offset_unit_audit(audit: pd.DataFrame, *, fish_id: str) -> plt.Figure:
    """Render reported-versus-physical offset checks for the Q0.1 review gate."""
    if audit.empty:
        raise ValueError("No unique matches with finite centroids for XY-offset audit")
    fig, axes = plt.subplots(1, 2, figsize=(11.5, 4.5), constrained_layout=True)
    reported = pd.to_numeric(audit["reported_selected_dist_um"], errors="coerce")
    physical = pd.to_numeric(audit["physical_xy_offset_um"], errors="coerce")
    finite = np.isfinite(reported) & np.isfinite(physical)
    upper = float(np.nanmax(np.r_[reported[finite], physical[finite]])) if finite.any() else 1.0
    axes[0].scatter(physical[finite], reported[finite], s=8, alpha=0.35, color="#0072B2", rasterized=True)
    axes[0].plot([0, upper], [0, upper], "--", color="#444444", linewidth=1)
    axes[0].set(
        xlabel="Physical centroid remeasurement (µm)",
        ylabel="Stored selected_dist_um\n(historically pixel-valued)",
        title="Row-level distance comparison",
    )
    axes[0].grid(alpha=0.2)
    axes[1].hist(physical[finite], bins=40, alpha=0.65, color="#009E73", label=f"physical (median {np.nanmedian(physical):.2f} µm)")
    axes[1].hist(reported[finite], bins=40, histtype="step", linewidth=1.8, color="#CC79A7", label=f"stored pixel-valued (median {np.nanmedian(reported):.2f})")
    axes[1].set(xlabel="XY centroid distance", ylabel="Unique matches", title=f"n = {int(finite.sum())}; same frozen (plane, ROI) keys")
    axes[1].legend(frameon=False, fontsize=8)
    fig.suptitle(f"{fish_id}: Q0.1 XY-offset unit audit", fontweight="bold")
    return fig


def build_stored_vs_physical_xy_offset_audit(
    stored_rows: pd.DataFrame,
    qc_rows: pd.DataFrame,
    *,
    dx_um: float,
    dy_um: float,
) -> pd.DataFrame:
    """Compare a stored centroid-distance field with a physical remeasurement.

    ``selected_dist_um`` is a historical schema name.  The caller must establish
    its units before interpreting it; this helper intentionally reports it as a
    stored value and never relabels it as micrometres.  The remeasurement uses
    the same saved anatomy-space centroids in physical units.  It applies the
    same filter to both sources, rejects duplicate keys, and inner-joins exact
    ``(plane_idx, func_label)`` keys without rematching or transforming an ROI.
    """
    if not (np.isfinite(dx_um) and dx_um > 0 and np.isfinite(dy_um) and dy_um > 0):
        raise ValueError("dx_um and dy_um must be finite positive physical spacings")

    key_columns = ("plane_idx", "func_label")
    centroid_columns = (
        "centroid_x_func_anat",
        "centroid_y_func_anat",
        "centroid_x_anat",
        "centroid_y_anat",
    )
    required = set(key_columns) | {
        "has_unique_anat_match",
        "plane_match_outcome",
        "selected_dist_um",
    } | set(centroid_columns)

    def _filtered(source: pd.DataFrame, name: str) -> pd.DataFrame:
        missing = sorted(required.difference(source.columns))
        if missing:
            raise ValueError(f"{name} XY-offset audit lacks columns: {', '.join(missing)}")
        out = source.copy()
        out = out[out["plane_match_outcome"].astype(str).eq("anatomy match")].copy()
        out = out[_as_bool(out["has_unique_anat_match"])].copy()
        for column in (*key_columns, *centroid_columns, "selected_dist_um"):
            out[column] = pd.to_numeric(out[column], errors="coerce")
        out = out.dropna(subset=[*key_columns, *centroid_columns, "selected_dist_um"])
        out["plane_idx"] = out["plane_idx"].astype(int)
        out["func_label"] = out["func_label"].astype(int)
        if out.duplicated(list(key_columns)).any():
            raise ValueError(f"{name} XY-offset audit has duplicate unique-match keys")
        return out.loc[:, [*key_columns, *centroid_columns, "selected_dist_um"]]

    stored = _filtered(stored_rows, "Stored-distance source")
    qc = _filtered(qc_rows, "Notebook 03")
    stored = stored.rename(columns={
        **{column: f"stored_{column}" for column in centroid_columns},
        "selected_dist_um": "stored_xy_offset_value",
    })
    qc = qc.rename(columns={
        **{column: f"qc_{column}" for column in centroid_columns},
        "selected_dist_um": "qc_stored_selected_dist_um",
    })
    joined = stored.merge(qc, on=list(key_columns), how="inner", validate="one_to_one")
    if joined.empty:
        raise ValueError("Stored-distance source and Notebook 03 have no intersected unique-match ROI keys")
    joined["qc_dx_px"] = joined["qc_centroid_x_func_anat"] - joined["qc_centroid_x_anat"]
    joined["qc_dy_px"] = joined["qc_centroid_y_func_anat"] - joined["qc_centroid_y_anat"]
    joined["physical_xy_offset_um"] = np.hypot(joined["qc_dx_px"] * float(dx_um), joined["qc_dy_px"] * float(dy_um))
    return joined.sort_values(list(key_columns)).reset_index(drop=True)


def summarize_stored_vs_physical_xy_offset_audit(audit: pd.DataFrame) -> pd.DataFrame:
    """Return overall and per-plane sample sizes and medians for Q0.1."""
    required = {"plane_idx", "stored_xy_offset_value", "physical_xy_offset_um"}
    missing = sorted(required.difference(audit.columns))
    if missing:
        raise ValueError(f"XY-offset parity summary lacks columns: {', '.join(missing)}")
    rows: list[dict[str, float | int | str]] = []
    for label, subset in [("overall", audit), *[(str(int(p)), g) for p, g in audit.groupby("plane_idx", sort=True)]]:
        rows.append({
            "scope": label,
            "n": int(len(subset)),
            "stored_median_xy_offset_value": float(np.median(subset["stored_xy_offset_value"])),
            "physical_median_xy_offset_um": float(np.median(subset["physical_xy_offset_um"])),
        })
    return pd.DataFrame(rows)


def render_stored_vs_physical_xy_offset_audit(audit: pd.DataFrame, *, fish_id: str) -> plt.Figure:
    """Render the stored-value and physical-distance audit without conflating units."""
    if audit.empty:
        raise ValueError("No intersected unique matches available for XY-offset parity audit")
    stored = pd.to_numeric(audit["stored_xy_offset_value"], errors="coerce").to_numpy(dtype=float)
    physical = pd.to_numeric(audit["physical_xy_offset_um"], errors="coerce").to_numpy(dtype=float)
    finite = np.isfinite(stored) & np.isfinite(physical)
    if not finite.any():
        raise ValueError("XY-offset parity audit has no finite matched distances")
    stored, physical = stored[finite], physical[finite]
    upper = max(float(np.max(np.r_[stored, physical])), 1.0)
    fig, (distribution_ax, scatter_ax) = plt.subplots(1, 2, figsize=(12.5, 4.8), constrained_layout=True)
    bins = np.linspace(0.0, upper, min(41, max(11, int(np.sqrt(len(stored))) * 3)))
    distribution_ax.hist(stored, bins=bins, alpha=0.60, color="#CC79A7", label=f"stored value (median {np.median(stored):.3f}; pixels for L765_f04)")
    distribution_ax.hist(physical, bins=bins, histtype="step", linewidth=2.0, color="#0072B2", label=f"physical remeasurement (median {np.median(physical):.3f} µm)")
    distribution_ax.set(xlabel="Centroid distance (stored values and µm are not commensurate)", ylabel="Intersected unique-match ROIs", title=f"Same keys, n={len(stored)}")
    distribution_ax.legend(frameon=False, fontsize=8)
    distribution_ax.grid(axis="y", alpha=0.2)
    scatter_ax.scatter(stored, physical, s=10, alpha=0.38, color="#0072B2", rasterized=True)
    scatter_ax.set(xlabel="Stored selected_dist_um value (pixels for L765_f04)", ylabel="Physical centroid remeasurement (µm)", title="Row-level exact-key unit audit")
    scatter_ax.set_xlim(0, upper)
    scatter_ax.set_ylim(0, upper)
    scatter_ax.grid(alpha=0.2)
    fig.suptitle(f"{fish_id}: Q0.1 stored-distance versus physical XY-offset audit", fontweight="bold")
    return fig


__all__ = [
    "FORBIDDEN_POST_GEOMETRY_COLUMNS",
    "GeometryReviewBundle",
    "REQUIRED_GEOMETRY_COLUMNS",
    "geometry_review_queue",
    "load_geometry_review_bundle",
    "render_centroid_offset_review",
    "render_centroid_offset_detail",
    "audit_xy_offset_units",
    "build_stored_vs_physical_xy_offset_audit",
    "render_xy_offset_unit_audit",
    "render_stored_vs_physical_xy_offset_audit",
    "render_geometry_placement_overview",
    "render_geometry_review_summary",
    "show_geometry_review_dashboard",
    "summarize_geometry_review",
    "summarize_stored_vs_physical_xy_offset_audit",
]
