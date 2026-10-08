"""Read-only review surfaces for molecular QC notebooks 04, 05, and 06.

The functions in this module inspect persisted artifacts only.  They never run a
registration, segmentation, matching, scoring, export, or promotion stage.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
import re
from typing import Mapping, Sequence

from matplotlib.lines import Line2D
import matplotlib.pyplot as plt
from matplotlib import patheffects
import numpy as np
import pandas as pd
import tifffile
from scipy import ndimage as ndi

from ..matching import compute_centroids, gene_from_mask, idx_to_um, resolve_anatomy_label_z, validate_anatomy_label_z_provenance
from ..spatial import anatomy_max_section_xy_radii_um, imread_any, load_or_cache_voxels, norm01

__all__ = [
    "MOLECULAR_GEOMETRY_TRACKS",
    "MolecularLabelViewerData",
    "inspect_molecular_geometry_qc",
    "plot_molecular_geometry_qc",
    "load_molecular_label_viewer",
    "load_hcr_match_overlay_viewer",
    "show_molecular_label_viewer",
    "build_hcr_anatomy_centroid_offset_table",
    "plot_hcr_anatomy_centroid_offsets",
    "inspect_molecular_identity_qc",
    "plot_molecular_identity_qc",
    "inspect_activity_export_qc",
    "plot_activity_export_qc",
    "resolve_hcr_matching_artifact_root",
    "build_hcr_anatomy_match_outcome_table",
    "plot_hcr_anatomy_match_outcome_donuts",
    "build_hcr_functional_plane_status_donut_table",
    "plot_hcr_functional_plane_status_donuts",
    "build_hcr_matching_flow_table",
    "plot_hcr_matching_flow",
    "build_hcr_matching_flow_donut_table",
    "plot_hcr_matching_flow_donuts",
    "build_cross_modality_mask_size_table",
    "plot_cross_modality_mask_sizes",
    "build_molecular_identity_fate_table",
    "plot_molecular_identity_fate",
    "plot_molecular_correspondence_tiles",
]


MOLECULAR_GEOMETRY_TRACKS = (
    "rbest_to_in_vivo",
    "rbest_to_ex_vivo",
    "ex_vivo_to_in_vivo",
    "later_round_to_ex_vivo",
)
_FISH_RE = re.compile(r"^[A-Z][0-9]{3}_f[0-9]{2}$")
_ROI_KEYS = ("plane_idx", "func_label")
_LABEL_MASK_RE = re.compile(
    r"_(?P<source>rbest|r\d+)_channel\d+_(?P<gene>.+?)_cp_masks_in_2p_labels_uint16$",
    re.IGNORECASE,
)
_VIEWER_COLORS = ("#e41a1c", "#377eb8", "#4daf4a", "#984ea3", "#ff7f00", "#a65628", "#f781bf", "#999999")
_GEOMETRY_FIELDS = (
    "selected_anat_label",
    "anat_label",
    "has_unique_anat_match",
    "plane_match_outcome",
    "claim_outcome",
    "selected_overlap_px",
    "selected_dist_um",
    "n_overlap_candidates_any",
    "n_overlap_candidates_valid",
    "match_status",
    "match_outcome",
    "overlap_px",
    "overlap_fraction",
    "dist_px",
    "dist_um",
)


@dataclass(frozen=True)
class MolecularLabelViewerData:
    """In-memory anatomy-space label masks for Notebook 04 interactive review."""

    anatomy_zyx: np.ndarray
    label_masks_zyx: Mapping[str, np.ndarray]
    label_sources: Mapping[str, Path]
    label_categories: Mapping[str, str] | None = None


def _validate_fish(fish_id: str) -> None:
    if not _FISH_RE.fullmatch(str(fish_id)):
        raise ValueError(f"Invalid fish_id {fish_id!r}; expected e.g. L765_f04")


def _path(path: str | Path | None) -> Path | None:
    return None if path is None else Path(path).expanduser().resolve()


def _inside(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


def _check_root(path: Path | None, roots: Sequence[Path], *, area: str, issues: list[dict]) -> None:
    if path is not None and not any(_inside(path, root) for root in roots):
        _issue(
            issues,
            area,
            "blocked",
            "Artifact escapes the explicitly selected fish/pipeline roots: "
            + ", ".join(str(root) for root in roots),
            path,
        )


def _review_roots(pipeline_root: str | Path, fish_dir: str | Path | None) -> tuple[Path, ...]:
    roots = [Path(pipeline_root).expanduser().resolve()]
    if fish_dir is not None:
        fish_root = Path(fish_dir).expanduser().resolve()
        if fish_root not in roots:
            roots.append(fish_root)
    return tuple(roots)


def _issue(rows: list[dict], area: str, status: str, detail: str, path: Path | None = None) -> None:
    rows.append({"area": area, "status": status, "detail": detail, "path": "" if path is None else str(path)})


def _load_csv(path: str | Path | None, *, area: str, issues: list[dict]) -> tuple[Path | None, pd.DataFrame | None]:
    resolved = _path(path)
    if resolved is None:
        _issue(issues, area, "missing", "No artifact path was configured.")
        return None, None
    if not resolved.is_file():
        _issue(issues, area, "missing", "Configured artifact does not exist; no computation was attempted.", resolved)
        return resolved, None
    try:
        table = pd.read_csv(resolved)
    except Exception as exc:
        _issue(issues, area, "blocked", f"Could not read CSV: {exc}", resolved)
        return resolved, None
    _issue(issues, area, "present", f"Loaded {len(table):,} rows.", resolved)
    return resolved, table


def _load_csv_collection(
    paths: str | Path | Sequence[str | Path] | None,
    *,
    area: str,
    issues: list[dict],
) -> tuple[tuple[Path, ...], pd.DataFrame | None]:
    """Load one or more persisted tables and retain their source filenames."""
    configured = () if paths is None else ((paths,) if isinstance(paths, (str, Path)) else tuple(paths))
    if not configured:
        _issue(issues, area, "missing", "No artifact paths were configured.")
        return (), None
    resolved: list[Path] = []
    tables: list[pd.DataFrame] = []
    for item in configured:
        path, table = _load_csv(item, area=area, issues=issues)
        if path is not None:
            resolved.append(path)
        if table is not None:
            table = table.copy()
            table["source_file"] = path.name
            tables.append(table)
    return tuple(resolved), (pd.concat(tables, ignore_index=True, sort=False) if tables else None)


def _require_columns(table: pd.DataFrame | None, required: Sequence[str], *, area: str, issues: list[dict]) -> bool:
    if table is None:
        return False
    missing = [name for name in required if name not in table.columns]
    if missing:
        _issue(issues, area, "blocked", f"Missing required columns: {missing}")
        return False
    return True


def _fish_check(table: pd.DataFrame | None, fish_id: str, *, area: str, issues: list[dict]) -> None:
    if table is None or table.empty or "fish_id" not in table:
        return
    observed = sorted(table["fish_id"].dropna().astype(str).unique())
    if observed != [fish_id]:
        _issue(issues, area, "blocked", f"Fish identity mismatch: observed {observed}, expected {fish_id}.")


def _status(issues: list[dict]) -> str:
    states = {row["status"] for row in issues}
    if "blocked" in states:
        return "blocked"
    if "missing" in states or "not_started" in states:
        return "incomplete"
    return "ready_for_review"


def _viewer_label_name(path: Path, seen: set[str]) -> str:
    match = _LABEL_MASK_RE.search(path.stem)
    if match is None:
        base = path.stem
    else:
        source = match.group("source").lower()
        gene = match.group("gene").replace("_", ".")
        base = f"{source} · {gene}"
    name = base
    index = 2
    while name in seen:
        name = f"{base} [{index}]"
        index += 1
    seen.add(name)
    return name


def _anatomy_grid_voxel_zyx_um(path: Path) -> dict[str, float]:
    """Load explicit physical spacing for an anatomy-grid label measurement."""

    vox = load_or_cache_voxels(path)
    missing = [axis for axis in ("X", "Y", "Z") if axis not in vox]
    if missing:
        raise ValueError(f"Anatomy voxel source lacks physical spacing for {missing}: {path}")
    values = {"dz": float(vox["Z"]), "dy": float(vox["Y"]), "dx": float(vox["X"])}
    invalid = [axis for axis, value in values.items() if not np.isfinite(value) or value <= 0]
    if invalid:
        raise ValueError(f"Anatomy voxel source has invalid physical spacing for {invalid}: {path}")
    return values


def load_molecular_label_viewer(
    anatomy_path: str | Path,
    label_paths: Sequence[str | Path],
) -> MolecularLabelViewerData:
    """Load same-grid in-vivo anatomy and any number of HCR label masks.

    The returned arrays use explicit ``ZYX`` display order.  This loader is
    read-only and validates that every selected label TIFF exactly shares the
    anatomy stack shape before the interactive viewer is created.
    """

    anatomy_file = _path(anatomy_path)
    if anatomy_file is None or not anatomy_file.is_file():
        raise FileNotFoundError(f"In-vivo anatomy stack does not exist: {anatomy_path}")
    anatomy = np.asarray(imread_any(anatomy_file))
    if anatomy.ndim != 3:
        raise ValueError(f"In-vivo anatomy stack must be 3D ZYX, got {anatomy.shape}: {anatomy_file}")
    if not label_paths:
        raise ValueError("Select at least one retained HCR label mask for interactive review.")

    masks: dict[str, np.ndarray] = {}
    sources: dict[str, Path] = {}
    seen: set[str] = set()
    for raw_path in label_paths:
        label_path = _path(raw_path)
        if label_path is None or not label_path.is_file():
            raise FileNotFoundError(f"HCR label mask does not exist: {raw_path}")
        labels = np.asarray(imread_any(label_path))
        if labels.ndim != 3:
            raise ValueError(f"HCR label mask must be 3D ZYX, got {labels.shape}: {label_path}")
        if labels.shape != anatomy.shape:
            raise ValueError(
                "HCR label mask does not share the in-vivo anatomy grid: "
                f"{label_path.name} is {labels.shape}, anatomy is {anatomy.shape}"
            )
        name = _viewer_label_name(label_path, seen)
        masks[name] = labels.astype(np.uint16, copy=False)
        sources[name] = label_path
    return MolecularLabelViewerData(anatomy_zyx=anatomy, label_masks_zyx=masks, label_sources=sources)


def load_hcr_match_overlay_viewer(
    anatomy_path: str | Path,
    label_paths: Sequence[str | Path],
    review_paths: Sequence[str | Path],
    final_pair_paths: Sequence[str | Path],
) -> MolecularLabelViewerData:
    """Load accepted and rejected persisted HCR labels for anatomy-space review.

    This is a display-only classification of each mask's existing label IDs:
    accepted IDs come from its saved final-pair table; rejected IDs are IDs
    recorded in its saved review table but absent from that final table.  It
    neither regenerates candidates nor changes pair membership.
    """
    if not (len(label_paths) == len(review_paths) == len(final_pair_paths)):
        raise ValueError("Provide one review and final-pair table for each selected HCR label mask.")
    base = load_molecular_label_viewer(anatomy_path, label_paths)
    masks: dict[str, np.ndarray] = {}
    sources: dict[str, Path] = {}
    categories: dict[str, str] = {}
    for name, raw_label, raw_review, raw_final in zip(base.label_masks_zyx, label_paths, review_paths, final_pair_paths):
        label_path, review_path, final_path = _path(raw_label), _path(raw_review), _path(raw_final)
        if review_path is None or not review_path.is_file():
            raise FileNotFoundError(f"Persisted HCR review table does not exist: {raw_review}")
        if final_path is None or not final_path.is_file():
            raise FileNotFoundError(f"Persisted HCR final-pair table does not exist: {raw_final}")
        review, final = pd.read_csv(review_path), pd.read_csv(final_path)
        if "conf_label" not in review or "conf_label" not in final:
            raise ValueError(f"Review and final-pair tables require conf_label: {review_path.name}, {final_path.name}")
        accepted = set(final["conf_label"].dropna().astype(int))
        reviewed = set(review["conf_label"].dropna().astype(int))
        labels = base.label_masks_zyx[name]
        for category, ids in (("accepted", accepted), ("rejected", reviewed - accepted)):
            key = f"{category} · {name}"
            masks[key] = np.where(np.isin(labels, list(ids)), labels, 0).astype(labels.dtype, copy=False)
            sources[key] = label_path
            categories[key] = category
    return MolecularLabelViewerData(
        anatomy_zyx=base.anatomy_zyx,
        label_masks_zyx=masks,
        label_sources=sources,
        label_categories=categories,
    )


def show_molecular_label_viewer(
    viewer_data: MolecularLabelViewerData,
    *,
    opacity: float = 0.55,
):
    """Return a fast ipympl/ipywidgets anatomy-plane label-overlay viewer.

    The data are preloaded before callbacks are connected.  Plane and gene
    changes only replace existing Matplotlib image layers; they never read
    files, register images, or create molecular/anatomy candidate matches.
    """

    if not 0.0 < float(opacity) <= 1.0:
        raise ValueError("opacity must be in (0, 1]")
    try:
        import ipywidgets as widgets
        from matplotlib.colors import to_rgb
        from ipympl.backend_nbagg import Canvas
    except ImportError as exc:  # pragma: no cover - environment-specific dependency
        raise ImportError(
            "Interactive molecular review requires ipywidgets and ipympl in the active kernel."
        ) from exc

    anatomy = np.asarray(viewer_data.anatomy_zyx)
    if anatomy.ndim != 3 or not viewer_data.label_masks_zyx:
        raise ValueError("Viewer data require one 3D anatomy stack and at least one label mask.")
    anatomy_display = np.stack([norm01(plane) for plane in anatomy], axis=0)
    names = tuple(viewer_data.label_masks_zyx)
    fig, ax = plt.subplots(figsize=(6.6, 6.6))
    if not isinstance(fig.canvas, Canvas):
        plt.close(fig)
        raise RuntimeError("Enable the ipympl backend before creating the viewer: run `%matplotlib widget`.")
    anatomy_artist = ax.imshow(anatomy_display[0], cmap="gray", interpolation="nearest")
    overlay_artist = ax.imshow(
        np.zeros((*anatomy.shape[1:], 4), dtype=np.float32),
        interpolation="nearest",
    )
    ax.set_axis_off()
    # The ipympl canvas otherwise adds a redundant ``Figure 1`` header above
    # the image, making the controls feel detached in a notebook.
    if hasattr(fig.canvas, "header_visible"):
        fig.canvas.header_visible = False

    plane_slider = widgets.IntSlider(
        value=0,
        min=0,
        max=anatomy.shape[0] - 1,
        step=1,
        description="Anatomy Z",
        continuous_update=True,
        readout=True,
    )
    checkboxes = {
        name: widgets.Checkbox(value=True, description=name, indent=False)
        for name in names
    }
    gene_grid = widgets.GridBox(
        list(checkboxes.values()),
        layout=widgets.Layout(
            grid_template_columns="minmax(0, 1fr)",
            grid_gap="3px",
            max_height="300px",
            overflow="auto",
            width="100%",
        ),
    )
    status = widgets.HTML()
    categories = viewer_data.label_categories or {}
    colors = {
        name: np.asarray(to_rgb({"accepted": "#00A651", "rejected": "#D55E00"}.get(categories.get(name), _VIEWER_COLORS[index % len(_VIEWER_COLORS)])), dtype=np.float32)
        for index, name in enumerate(names)
    }

    def update(_change=None) -> None:
        z_index = int(plane_slider.value)
        selected = [name for name, checkbox in checkboxes.items() if checkbox.value]
        rgb_sum = np.zeros((*anatomy.shape[1:], 3), dtype=np.float32)
        count = np.zeros(anatomy.shape[1:], dtype=np.uint16)
        for name in selected:
            present = viewer_data.label_masks_zyx[name][z_index] > 0
            if present.any():
                rgb_sum[present] += colors[name]
                count[present] += 1
        rgba = np.zeros((*anatomy.shape[1:], 4), dtype=np.float32)
        present = count > 0
        if present.any():
            rgba[present, :3] = rgb_sum[present] / count[present, None]
            rgba[present, 3] = float(opacity)
        anatomy_artist.set_data(anatomy_display[z_index])
        overlay_artist.set_data(rgba)
        ax.set_title(f"In-vivo anatomy Z {z_index + 1}/{anatomy.shape[0]} · {len(selected)} selected mask(s)")
        status.value = "<small>Loaded once; plane and checkbox changes perform no disk I/O, matching, or pair updates.</small>"
        fig.canvas.draw_idle()

    plane_slider.observe(update, names="value")
    for checkbox in checkboxes.values():
        checkbox.observe(update, names="value")
    update()
    controls = widgets.VBox(
        [
            widgets.HTML("<b>Review controls</b>"),
            plane_slider,
            widgets.HTML("<b>Visible HCR labels</b>"),
            gene_grid,
            status,
        ],
        layout=widgets.Layout(width="270px", min_width="240px", max_width="300px", align_items="stretch"),
    )
    return widgets.HBox(
        [fig.canvas, controls],
        layout=widgets.Layout(align_items="flex-start", gap="12px", width="100%"),
    )


def inspect_molecular_geometry_qc(
    *,
    fish_id: str,
    pipeline_root: str | Path,
    fish_dir: str | Path | None = None,
    track_artifacts: Mapping[str, Sequence[str | Path]],
    segmentation_table_path: str | Path | Sequence[str | Path] | None,
    pair_candidates_path: str | Path | Sequence[str | Path] | None,
    segmentation_label_paths: Sequence[str | Path] = (),
    pair_candidates_state: str = "expected",
) -> dict:
    """Inspect segmentation, four distinct registration tracks, and pair ambiguity.

    ``pair_candidates_state='not_started'`` records an intentionally pending
    geometry writer without confusing it with a missing configured artifact.
    """
    _validate_fish(fish_id)
    root = Path(pipeline_root).expanduser().resolve()
    roots = _review_roots(root, fish_dir)
    issues: list[dict] = []
    if pair_candidates_state not in {"expected", "not_started"}:
        raise ValueError("pair_candidates_state must be 'expected' or 'not_started'")
    track_rows: list[dict] = []
    unknown = sorted(set(track_artifacts) - set(MOLECULAR_GEOMETRY_TRACKS))
    if unknown:
        _issue(issues, "registration tracks", "blocked", f"Unknown track keys: {unknown}")
    for track in MOLECULAR_GEOMETRY_TRACKS:
        configured = [_path(item) for item in track_artifacts.get(track, ())]
        for item in configured:
            _check_root(item, roots, area=f"registration:{track}", issues=issues)
        present = [item for item in configured if item is not None and item.exists()]
        state = "present" if configured and len(present) == len(configured) else "missing"
        track_rows.append({
            "track": track,
            "status": state,
            "configured_artifacts": len(configured),
            "present_artifacts": len(present),
            "paths": "\n".join(str(item) for item in configured if item is not None),
        })
        detail = f"{len(present)}/{len(configured)} configured artifacts present."
        _issue(issues, f"registration:{track}", state, detail)

    label_paths = tuple(path for raw_path in segmentation_label_paths if (path := _path(raw_path)) is not None)
    label_rows: list[dict] = []
    for label_path in label_paths:
        _check_root(label_path, roots, area="molecular segmentation masks", issues=issues)
        label_rows.append({"path": str(label_path), "status": "present" if label_path.is_file() else "missing"})
    if label_rows:
        present_labels = sum(row["status"] == "present" for row in label_rows)
        _issue(
            issues,
            "molecular segmentation masks",
            "present" if present_labels == len(label_rows) else "missing",
            f"{present_labels}/{len(label_rows)} configured label masks available in anatomy space.",
        )

    segmentation_paths, segmentation = _load_csv_collection(
        segmentation_table_path, area="molecular segmentation", issues=issues
    ) if segmentation_table_path is not None else ((), None)
    if pair_candidates_path is None and pair_candidates_state == "not_started":
        pair_paths, pairs = (), None
        _issue(
            issues,
            "molecular/anatomy pair candidates",
            "not_started",
            "Candidate matching has not been run; no candidate table is expected yet.",
        )
    else:
        pair_paths, pairs = _load_csv_collection(
            pair_candidates_path, area="molecular/anatomy pair candidates", issues=issues
        )
    for segmentation_path in segmentation_paths:
        _check_root(segmentation_path, roots, area="molecular segmentation", issues=issues)
    for pair_path in pair_paths:
        _check_root(pair_path, roots, area="molecular/anatomy pair candidates", issues=issues)
    _fish_check(segmentation, fish_id, area="molecular segmentation", issues=issues)
    _fish_check(pairs, fish_id, area="molecular/anatomy pair candidates", issues=issues)

    segmentation_summary = pd.DataFrame()
    if segmentation is not None:
        round_col = next((c for c in ("round", "round_id", "hcr_round") if c in segmentation), None)
        gene_col = next((c for c in ("gene", "gene_name", "identity_label") if c in segmentation), None)
        group = [c for c in (round_col, gene_col) if c is not None]
        if group:
            segmentation_summary = segmentation.groupby(group, dropna=False).size().rename("n_segments").reset_index()
        else:
            segmentation_summary = segmentation.groupby("source_file").size().rename("n_segments").reset_index()

    ambiguity = pd.DataFrame()
    if pairs is not None:
        label_col = next((c for c in ("hcr_label", "conf_label", "molecular_label") if c in pairs), None)
        anatomy_col = next((c for c in ("anat_label", "anatomy_label", "twoP_label") if c in pairs), None)
        if label_col and anatomy_col:
            group = ["source_file", label_col] if "source_file" in pairs else [label_col]
            counts = pairs.groupby(group, dropna=False)[anatomy_col].nunique().rename("candidate_anatomy_labels")
            ambiguity = counts.reset_index()
            ambiguity["is_ambiguous"] = ambiguity["candidate_anatomy_labels"] > 1
        else:
            _issue(
                issues,
                "molecular/anatomy pair candidates",
                "blocked",
                "Cannot assess pair ambiguity without an HCR-label and anatomy-label column.",
                pair_paths[0] if pair_paths else None,
            )

    return {
        "fish_id": fish_id,
        "status": _status(issues),
        "issues": pd.DataFrame(issues),
        "tracks": pd.DataFrame(track_rows),
        "segmentation": segmentation,
        "segmentation_summary": segmentation_summary,
        "pair_candidates": pairs,
        "pair_ambiguity": ambiguity,
        "label_masks": pd.DataFrame(label_rows, columns=["path", "status"]),
        "paths": {"segmentation": segmentation_paths, "label_masks": label_paths, "pair_candidates": pair_paths},
        "read_only": True,
    }


def plot_molecular_geometry_qc(report: Mapping) -> plt.Figure:
    """Render a compact inventory; it is evidence navigation, not acceptance."""
    tracks = report["tracks"]
    ambiguity = report["pair_ambiguity"]
    fig, axes = plt.subplots(1, 2, figsize=(10, 3.8))
    colors = ["#2ca25f" if s == "present" else "#de2d26" for s in tracks["status"]]
    axes[0].barh(tracks["track"], tracks["present_artifacts"], color=colors)
    axes[0].set_xlabel("Persisted artifacts found")
    axes[0].set_title("Registration tracks remain distinct")
    if ambiguity.empty:
        axes[1].text(0.5, 0.5, "Pair ambiguity unavailable", ha="center", va="center")
        axes[1].set_axis_off()
    else:
        n_ambiguous = int(ambiguity["is_ambiguous"].sum())
        axes[1].bar(["unambiguous", "ambiguous"], [len(ambiguity) - n_ambiguous, n_ambiguous], color=["#3182bd", "#e6550d"])
        axes[1].set_ylabel("Molecular labels")
        axes[1].set_title("Candidate-pair ambiguity")
    fig.suptitle(f"{report['fish_id']} molecular geometry review")
    fig.tight_layout()
    return fig


def _mask_source_metadata(path: Path) -> tuple[str | None, str | None]:
    """Return HCR round and gene encoded by a persisted transformed mask name."""
    match = _LABEL_MASK_RE.search(path.stem)
    if match is None:
        return None, None
    return match.group("source").lower(), match.group("gene").replace("_", ".")


def _bbox_size_rows(
    path: Path,
    *,
    modality: str,
    voxel_zyx_um: Mapping[str, float],
) -> list[dict]:
    """Measure each positive integer label's inclusive bounding box, read-only."""
    labels = np.asarray(imread_any(path))
    if labels.ndim not in (2, 3):
        raise ValueError(f"Label mask must be 2D YX or 3D ZYX, got {labels.shape}: {path}")
    if labels.ndim == 2:
        labels = labels[None, ...]
    if not np.issubdtype(labels.dtype, np.integer):
        raise ValueError(f"Label mask must contain integer labels: {path}")
    dz = float(voxel_zyx_um["Z"])
    dy = float(voxel_zyx_um["Y"])
    dx = float(voxel_zyx_um["X"])
    if not all(np.isfinite(value) and value > 0 for value in (dz, dy, dx)):
        raise ValueError("voxel_zyx_um must provide finite positive Z, Y, and X spacings in µm.")
    round_name, gene = _mask_source_metadata(path) if modality == "hcr" else (None, None)
    rows: list[dict] = []
    objects = ndi.find_objects(labels)
    for label, object_slice in enumerate(objects, start=1):
        if object_slice is None:
            continue
        # ``find_objects`` returns the minimal inclusive bounding box as Python
        # slices.  The legacy [53] metric is explicitly max-index minus
        # min-index, hence one less than the slice length (and zero for a
        # one-voxel extent); do not substitute an inclusive physical width.
        z_px, y_px, x_px = (item.stop - item.start - 1 for item in object_slice)
        # ``xy_um`` is the mean of the legacy physical X/Y extents.
        x_um, y_um, z_um = x_px * dx, y_px * dy, z_px * dz
        rows.append(
            {
                "modality": modality,
                "source_path": str(path),
                "source_name": path.name,
                "round": round_name,
                "gene": gene,
                "label": int(label),
                "x_px": x_px,
                "y_px": y_px,
                "z_px": z_px,
                "x_um": x_um,
                "y_um": y_um,
                "z_um": z_um,
                "xy_um": float((x_um + y_um) / 2.0),
            }
        )
    return rows


def build_cross_modality_mask_size_table(
    *,
    anatomy_labels_path: str | Path,
    functional_label_paths: Sequence[str | Path],
    hcr_label_paths: Sequence[str | Path],
    reference_anatomy_path: str | Path,
    hcr_label_metrics_path: str | Path | None = None,
) -> pd.DataFrame:
    """Measure persisted anatomy, functional, and HCR label-mask bounding boxes.

    All selected masks must already be on the reference anatomy grid.  Physical
    spacing is read from ``reference_anatomy_path`` (normally the prepared
    anatomy NRRD), because transformed TIFF masks do not reliably preserve XY
    resolution metadata.  Q05 and Q95 are calculated independently for each
    modality from the mean X/Y ``xy_um`` extent. When a persisted HCR label
    metrics table is provided, HCR small/large classifications come from that
    backend table, never from display-only XY bounding-box quantiles.
    """
    reference = _path(reference_anatomy_path)
    if reference is None or not reference.is_file():
        raise FileNotFoundError(f"Reference anatomy does not exist: {reference_anatomy_path}")
    voxels = load_or_cache_voxels(reference)
    missing = [axis for axis in ("Z", "Y", "X") if axis not in voxels]
    if missing:
        raise ValueError(f"Reference anatomy lacks physical voxel spacing for {missing}: {reference}")
    selected: list[tuple[str, Path]] = [("anatomy", _path(anatomy_labels_path))]
    selected.extend(("functional", _path(path)) for path in functional_label_paths)
    selected.extend(("hcr", _path(path)) for path in hcr_label_paths)
    rows: list[dict] = []
    for modality, path in selected:
        if path is None or not path.is_file():
            raise FileNotFoundError(f"{modality} label mask does not exist: {path}")
        rows.extend(_bbox_size_rows(path, modality=modality, voxel_zyx_um=voxels))
    table = pd.DataFrame(rows)
    if table.empty:
        return pd.DataFrame(columns=[
            "modality", "source_path", "source_name", "round", "gene", "label",
            "x_px", "y_px", "z_px", "x_um", "y_um", "z_um", "xy_um",
            "xy_q05_um", "xy_q95_um", "is_xy_hard_drop", "is_xy_q95_flag", "included_in_distribution",
        ])
    table["xy_q05_um"] = table.groupby("modality")["xy_um"].transform(lambda values: values.quantile(0.05))
    table["xy_q95_um"] = table.groupby("modality")["xy_um"].transform(lambda values: values.quantile(0.95))
    table["is_xy_hard_drop"] = table["xy_um"] < table["xy_q05_um"]
    table["is_xy_q95_flag"] = table["xy_um"] > table["xy_q95_um"]
    table["included_in_distribution"] = ~table["is_xy_hard_drop"]
    table["is_small_speck"] = False
    table["is_low_confidence_segmentation"] = False
    table["analysis_eligible"] = True
    if hcr_label_metrics_path is not None:
        metrics_path = _path(hcr_label_metrics_path)
        if metrics_path is None or not metrics_path.is_file():
            raise FileNotFoundError(f"HCR label metrics do not exist: {hcr_label_metrics_path}")
        metrics = pd.read_csv(metrics_path)
        required_metrics = {"mask_uid", "conf_label", "is_small_speck", "is_low_confidence_segmentation", "analysis_eligible"}
        missing_metrics = sorted(required_metrics - set(metrics.columns))
        if missing_metrics:
            raise ValueError(f"HCR label metrics lack columns: {missing_metrics}")
        hcr_rows = table["modality"].eq("hcr")
        table.loc[hcr_rows, "mask_uid"] = table.loc[hcr_rows, "source_name"].str.replace("_cp_masks_in_2p_labels_uint16.tif", "", regex=False)
        keyed = metrics.loc[:, list(required_metrics)].rename(columns={"conf_label": "label"})
        hcr = table.loc[hcr_rows].drop(columns=["is_small_speck", "is_low_confidence_segmentation", "analysis_eligible"]).merge(
            keyed, on=["mask_uid", "label"], how="left", validate="one_to_one",
        )
        if hcr["analysis_eligible"].isna().any():
            raise ValueError("Some staged HCR labels lack hcr_label_metrics.csv rows")
        for column in ("is_small_speck", "is_low_confidence_segmentation", "analysis_eligible"):
            table.loc[hcr_rows, column] = hcr[column].astype(bool).to_numpy()
        table.loc[hcr_rows, "is_xy_hard_drop"] = table.loc[hcr_rows, "is_small_speck"]
        table.loc[hcr_rows, "included_in_distribution"] = ~table.loc[hcr_rows, "is_small_speck"]
    return table


def _plot_size_distribution(
    ax: plt.Axes,
    table: pd.DataFrame,
    value_col: str,
    title: str,
    ylabel: str,
    *,
    show_xy_q95: bool = False,
) -> None:
    modalities = [name for name in ("anatomy", "functional", "hcr") if name in set(table["modality"])]
    data = [table.loc[(table["modality"] == name) & table["included_in_distribution"], value_col].to_numpy(dtype=float) for name in modalities]
    violin = ax.violinplot(data, positions=np.arange(1, len(modalities) + 1), showextrema=False, showmedians=True)
    for body in violin["bodies"]:
        body.set_facecolor("#9ecae1"); body.set_edgecolor("#2171b5"); body.set_alpha(0.76)
    ax.set_xticks(np.arange(1, len(modalities) + 1), modalities)
    for idx, modality in enumerate(modalities, start=1):
        kept_values = table.loc[(table["modality"] == modality) & table["included_in_distribution"], value_col].to_numpy(dtype=float)
        kept = int(((table["modality"] == modality) & table["included_in_distribution"]).sum())
        total = int((table["modality"] == modality).sum())
        q05 = float(table.loc[table["modality"] == modality, "xy_q05_um"].iloc[0])
        q95 = float(table.loc[table["modality"] == modality, "xy_q95_um"].iloc[0])
        mean, sd = (np.mean(kept_values), np.std(kept_values, ddof=1)) if len(kept_values) > 1 else ((kept_values[0], 0.0) if len(kept_values) else (np.nan, np.nan))
        median = float(np.median(kept_values)) if len(kept_values) else np.nan
        if modality == "hcr":
            low_conf = int(((table["modality"] == modality) & table["is_low_confidence_segmentation"]).sum())
            annotation = f"n={kept}/{total}\nmed={median:.2g}; SD={sd:.2g}\nlow-conf={low_conf}"
        else:
            annotation = f"n={kept}/{total}\nmed={median:.2g}; SD={sd:.2g}\nXY q05={q05:.2g}, q95={q95:.2g}"
        ax.text(idx, 0.01, annotation, ha="center", va="bottom", transform=ax.get_xaxis_transform(), fontsize=7)
        flagged = table.loc[
            (table["modality"] == modality) & table.get("is_low_confidence_segmentation", False), value_col
        ].to_numpy(dtype=float)
        if len(flagged):
            ax.scatter(np.full(len(flagged), idx), flagged, color="#d62728", marker="x", s=34, zorder=6)
        if show_xy_q95 and modality != "hcr":
            ax.hlines(q95, idx - 0.34, idx + 0.34, color="#d62728", linewidth=1.8, zorder=5)
    ax.set_title(title)
    ax.set_ylabel(ylabel)
    ax.grid(axis="y", alpha=0.22)
    plotted = table.loc[table["included_in_distribution"], value_col].to_numpy(dtype=float)
    if len(plotted):
        span = max(float(np.ptp(plotted)), max(float(np.max(plotted)), 1.0) * 0.1)
        ax.set_ylim(float(np.min(plotted)) - 0.34 * span, float(np.max(plotted)) + 0.12 * span)
    if show_xy_q95:
        ax.plot([], [], color="#d62728", linewidth=1.8, label="Modality XY q95")
    if ax.get_legend_handles_labels()[0]:
        ax.legend(fontsize=8, frameon=False)


def plot_cross_modality_mask_sizes(size_table: pd.DataFrame, *, fish_id: str) -> plt.Figure:
    """Plot XY/Z size distributions plus HCR gene/round splits from QC-only data."""
    required = {"modality", "xy_um", "z_um", "included_in_distribution", "is_xy_q95_flag", "round", "gene"}
    size_table = size_table.copy()
    if "is_low_confidence_segmentation" not in size_table:
        size_table["is_low_confidence_segmentation"] = False
    missing = sorted(required - set(size_table.columns))
    if missing:
        raise ValueError(f"Size table lacks required columns: {missing}")
    _validate_fish(fish_id)
    fig, axes = plt.subplots(2, 2, figsize=(12, 8.5), constrained_layout=True)
    _plot_size_distribution(axes[0, 0], size_table, "xy_um", "All masks: XY bounding-box size", "Mean X/Y extent (µm)", show_xy_q95=True)
    _plot_size_distribution(axes[0, 1], size_table, "z_um", "All masks: Z bounding-box size", "Z extent (µm)")
    hcr = size_table.loc[(size_table["modality"] == "hcr") & size_table["included_in_distribution"]].copy()
    hcr["hcr_group"] = hcr[["round", "gene"]].fillna("unknown").astype(str).agg(" · ".join, axis=1)
    groups = sorted(hcr["hcr_group"].unique())
    for ax, value_col, title, ylabel in (
        (axes[1, 0], "xy_um", "HCR masks: XY by gene / round", "Mean X/Y extent (µm)"),
        (axes[1, 1], "z_um", "HCR masks: Z by gene / round", "Z extent (µm)"),
    ):
        if not groups:
            ax.text(0.5, 0.5, "No retained HCR masks", ha="center", va="center", transform=ax.transAxes)
            ax.set_axis_off()
            continue
        values = [hcr.loc[hcr["hcr_group"] == group, value_col].to_numpy(dtype=float) for group in groups]
        violin = ax.violinplot(values, positions=np.arange(1, len(groups) + 1), showextrema=False, showmedians=True)
        for body in violin["bodies"]:
            body.set_facecolor("#c7e9c0"); body.set_edgecolor("#238b45"); body.set_alpha(0.76)
        ax.set_xticks(np.arange(1, len(groups) + 1), groups)
        for idx, group in enumerate(groups, start=1):
            group_values = hcr.loc[hcr["hcr_group"] == group, value_col].to_numpy(dtype=float)
            median = float(np.median(group_values)) if len(group_values) else np.nan
            sd = float(np.std(group_values, ddof=1)) if len(group_values) > 1 else 0.0
            ax.text(idx, 0.01, f"n={len(group_values)}\nmed={median:.2g}; SD={sd:.2g}", ha="center", va="bottom", transform=ax.get_xaxis_transform(), fontsize=7)
            flagged = hcr.loc[hcr["hcr_group"].eq(group) & hcr["is_low_confidence_segmentation"], value_col].to_numpy(dtype=float)
            if len(flagged):
                ax.scatter(np.full(len(flagged), idx), flagged, color="#d62728", marker="x", s=34, zorder=6)
        ax.set_title(title)
        ax.set_ylabel(ylabel)
        ax.tick_params(axis="x", rotation=25)
        ax.grid(axis="y", alpha=0.22)
        plotted = hcr[value_col].to_numpy(dtype=float)
        span = max(float(np.ptp(plotted)), max(float(np.max(plotted)), 1.0) * 0.1)
        ax.set_ylim(float(np.min(plotted)) - 0.34 * span, float(np.max(plotted)) + 0.12 * span)
        if hcr["is_low_confidence_segmentation"].any():
            ax.scatter([], [], color="#d62728", marker="x", label="Low-confidence segmentation")
            ax.legend(fontsize=8, frameon=False)
    fig.suptitle(f"{fish_id} cross-modality mask bounding boxes · HCR small speckles excluded; low-confidence masks retained as red ×")
    return fig


def build_hcr_anatomy_centroid_offset_table(
    *,
    anatomy_labels_path: str | Path,
    anatomy_voxel_source_path: str | Path,
    hcr_label_paths: Sequence[str | Path],
    final_pair_paths: Sequence[str | Path],
) -> pd.DataFrame:
    """Return signed HCR-minus-anatomy centroid offsets for persisted HCR pairs.

    All masks must already share the in-vivo anatomy grid.  The function reads
    only persisted masks and pair tables; it never proposes or
    changes a molecular/anatomy pairing.  Physical spacing is required from an
    explicit anatomy-grid source (normally the prepared anatomy NRRD); label
    TIFF metadata is not trusted because segmentation exports may omit it.
    """

    anatomy_path = _path(anatomy_labels_path)
    if anatomy_path is None or not anatomy_path.is_file():
        raise FileNotFoundError(f"Anatomy label stack does not exist: {anatomy_labels_path}")
    voxel_source_path = _path(anatomy_voxel_source_path)
    if voxel_source_path is None or not voxel_source_path.is_file():
        raise FileNotFoundError(f"Anatomy voxel source does not exist: {anatomy_voxel_source_path}")
    if len(hcr_label_paths) != len(final_pair_paths):
        raise ValueError("Provide one final-pair table for each selected HCR label mask.")

    anatomy = np.asarray(imread_any(anatomy_path))
    if anatomy.ndim != 3:
        raise ValueError(f"Anatomy label stack must be 3D ZYX, got {anatomy.shape}: {anatomy_path}")
    anatomy_centroids = compute_centroids(anatomy)
    vox_zyx = _anatomy_grid_voxel_zyx_um(voxel_source_path)
    _, radius_summary = anatomy_max_section_xy_radii_um(
        anatomy, dx_um=vox_zyx["dx"], dy_um=vox_zyx["dy"],
    )
    median_anatomy_xy_radius_um = float(radius_summary["median_xy_radius_um"])
    anatomy_um = anatomy_centroids.loc[:, ["label", "z", "y", "x"]].copy()
    anatomy_um[["z_um", "y_um", "x_um"]] = idx_to_um(anatomy_centroids, vox_zyx)

    rows: list[pd.DataFrame] = []
    for raw_mask, raw_pairs in zip(hcr_label_paths, final_pair_paths):
        mask_path = _path(raw_mask)
        pairs_path = _path(raw_pairs)
        if mask_path is None or not mask_path.is_file():
            raise FileNotFoundError(f"HCR label mask does not exist: {raw_mask}")
        if pairs_path is None or not pairs_path.is_file():
            raise FileNotFoundError(f"Accepted HCR pair table does not exist: {raw_pairs}")
        hcr = np.asarray(imread_any(mask_path))
        if hcr.shape != anatomy.shape:
            raise ValueError(f"HCR mask {mask_path.name} is {hcr.shape}, anatomy is {anatomy.shape}")
        pairs = pd.read_csv(pairs_path)
        required = {"conf_label", "twoP_label"}
        if not required.issubset(pairs.columns):
            raise ValueError(f"{pairs_path.name} lacks required accepted-pair columns {sorted(required)}")
        hcr_centroids = compute_centroids(hcr)
        hcr_um = hcr_centroids.loc[:, ["label", "z", "y", "x"]].copy()
        hcr_um[["z_um", "y_um", "x_um"]] = idx_to_um(hcr_centroids, vox_zyx)
        pair_columns = ["conf_label", "twoP_label"] + [column for column in ("analysis_eligible", "analysis_exclusion_reason", "within_geometry_gate") if column in pairs]
        merged = pairs.loc[:, pair_columns].drop_duplicates().merge(
            hcr_um, left_on="conf_label", right_on="label", how="inner"
        ).merge(anatomy_um, left_on="twoP_label", right_on="label", how="inner", suffixes=("_hcr", "_anatomy"))
        if merged.empty:
            continue
        for axis in ("x", "y", "z"):
            rows.append(
                pd.DataFrame(
                    {
                        "gene": gene_from_mask(mask_path),
                        "axis": axis,
                        "offset_um": merged[f"{axis}_um_hcr"] - merged[f"{axis}_um_anatomy"],
                        "conf_label": merged["conf_label"].astype(int),
                        "anat_label": merged["twoP_label"].astype(int),
                        "analysis_eligible": merged["analysis_eligible"].astype(bool) if "analysis_eligible" in merged else True,
                        "analysis_exclusion_reason": merged["analysis_exclusion_reason"].astype(str) if "analysis_exclusion_reason" in merged else "",
                        "median_anatomy_xy_radius_um": median_anatomy_xy_radius_um,
                        "anatomy_radius_definition": radius_summary["definition"],
                        "anatomy_dx_um": float(vox_zyx["dx"]),
                        "anatomy_dy_um": float(vox_zyx["dy"]),
                        "anatomy_dz_um": float(vox_zyx["dz"]),
                        "anatomy_voxel_source_path": str(voxel_source_path),
                        "mask_path": str(mask_path),
                        "final_pairs_path": str(pairs_path),
                    }
                )
            )
    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame(
        columns=[
            "gene", "axis", "offset_um", "conf_label", "anat_label",
            "median_anatomy_xy_radius_um", "anatomy_radius_definition",
            "anatomy_dx_um", "anatomy_dy_um", "anatomy_dz_um",
            "anatomy_voxel_source_path", "mask_path", "final_pairs_path",
        ]
    )


def plot_hcr_anatomy_centroid_offsets(offset_table: pd.DataFrame) -> plt.Figure:
    """Plot lateral XY distance and signed axial Z displacement by gene and gate."""

    required = {"gene", "axis", "offset_um"}
    if not required.issubset(offset_table.columns):
        raise ValueError(f"Offset table lacks required columns {sorted(required)}")
    key_columns = [column for column in ("gene", "conf_label", "anat_label", "analysis_eligible", "analysis_exclusion_reason", "mask_path", "final_pairs_path") if column in offset_table]
    wide = offset_table.pivot_table(index=key_columns, columns="axis", values="offset_um", aggfunc="first").reset_index()
    if not {"x", "y", "z"}.issubset(wide.columns):
        raise ValueError("Offset table must contain X, Y, and Z values for every persisted pair.")
    wide["xy"] = np.sqrt(wide["x"] ** 2 + wide["y"] ** 2)
    genes = sorted(wide["gene"].dropna().astype(str).unique())
    figure_specs = (("xy", "XY offset", "Lateral centroid distance (µm)", False), ("z", "Z offset", "Signed axial centroid offset (µm)", True))
    fig, axes = plt.subplots(1, 2, figsize=(9, 4.8))
    rng = np.random.default_rng(0)
    for ax, (axis, title, ylabel, signed) in zip(axes, figure_specs):
        subsets = [pd.to_numeric(wide.loc[wide["gene"].astype(str) == gene, axis], errors="coerce").dropna().to_numpy(dtype=float) for gene in genes]
        nonempty = [(gene, values) for gene, values in zip(genes, subsets) if values.size]
        if not nonempty:
            ax.text(0.5, 0.5, "No persisted pairs", ha="center", va="center", transform=ax.transAxes)
            ax.set_axis_off()
            continue
        positions = np.arange(1, len(nonempty) + 1)
        violin = ax.violinplot([values for _, values in nonempty], positions=positions, showextrema=False, showmedians=True)
        for body in violin["bodies"]:
            body.set_facecolor("#756bb1")
            body.set_edgecolor("#4a1486")
            body.set_alpha(0.72)
        for position, (gene, values) in zip(positions, nonempty):
            ax.scatter(position + rng.uniform(-0.09, 0.09, size=len(values)), values,
                       color="#4a1486", s=9, alpha=0.28, linewidths=0, zorder=3)
            if "analysis_eligible" in wide:
                rejected = pd.to_numeric(wide.loc[
                    wide["gene"].astype(str).eq(gene) & ~wide["analysis_eligible"].astype(bool), axis
                ], errors="coerce").dropna().to_numpy(dtype=float)
                if len(rejected):
                    ax.scatter(np.full(len(rejected), position), rejected, color="#d62728", marker="x", s=32, zorder=5)
        ax.axhline(0.0, color="#333333", linewidth=1, linestyle="--", zorder=0)
        ax.set_xticks(positions, [f"{gene}\n(n={len(values)})" for gene, values in nonempty], rotation=0)
        ax.set_title(title)
        ax.set_xlabel("HCR gene")
        ax.set_ylabel(ylabel)
        ax.grid(axis="y", alpha=0.22)
        if not signed:
            ax.set_ylim(bottom=0.0)
            if "median_anatomy_xy_radius_um" in offset_table and not offset_table.empty:
                radius = float(pd.to_numeric(offset_table["median_anatomy_xy_radius_um"], errors="coerce").dropna().iloc[0])
                ax.axhline(radius, color="#d62728", linewidth=1.6, linestyle=":", label=f"Median all-label max-section anatomy radius gate ({radius:.2f} µm)")
                ax.legend(frameon=False, fontsize=8)
    fig.suptitle("HCR/anatomy candidates: lateral XY gate and axial Z displacement · red × excluded")
    fig.tight_layout()
    return fig


def inspect_molecular_identity_qc(
    *,
    fish_id: str,
    pipeline_root: str | Path,
    fish_dir: str | Path | None = None,
    geometry_state: str,
    frozen_geometry_table_path: str | Path | None,
    identity_table_path: str | Path | None,
) -> dict:
    """Review identity attachment and prove shared geometry fields did not change."""
    _validate_fish(fish_id)
    root = Path(pipeline_root).expanduser().resolve()
    roots = _review_roots(root, fish_dir)
    issues: list[dict] = []
    if geometry_state not in {"frozen", "accepted"}:
        _issue(issues, "geometry prerequisite", "blocked", f"Geometry state is {geometry_state!r}, not frozen/accepted.")
    geometry_path, geometry = _load_csv(frozen_geometry_table_path, area="frozen geometry", issues=issues)
    identity_path, identity = _load_csv(identity_table_path, area="identity attachment", issues=issues)
    _check_root(geometry_path, roots, area="frozen geometry", issues=issues)
    _check_root(identity_path, roots, area="identity attachment", issues=issues)
    _fish_check(geometry, fish_id, area="frozen geometry", issues=issues)
    _fish_check(identity, fish_id, area="identity attachment", issues=issues)
    keys_ok = _require_columns(geometry, _ROI_KEYS, area="frozen geometry", issues=issues)
    keys_ok &= _require_columns(identity, _ROI_KEYS, area="identity attachment", issues=issues)

    changed = pd.DataFrame()
    compared_fields: list[str] = []
    if keys_ok and geometry is not None and identity is not None:
        compared_fields = [field for field in _GEOMETRY_FIELDS if field in geometry and field in identity]
        left = geometry[list(_ROI_KEYS) + compared_fields].copy()
        right = identity[list(_ROI_KEYS) + compared_fields].copy()
        if left.duplicated(list(_ROI_KEYS)).any() or right.duplicated(list(_ROI_KEYS)).any():
            _issue(issues, "geometry preservation", "blocked", "ROI keys are not unique in one or both tables.")
        else:
            merged = left.merge(right, on=list(_ROI_KEYS), how="outer", suffixes=("_geometry", "_identity"), indicator=True)
            mismatch = merged["_merge"].ne("both")
            for field in compared_fields:
                a = merged[f"{field}_geometry"]
                b = merged[f"{field}_identity"]
                mismatch |= ~(a.eq(b) | (a.isna() & b.isna()))
            changed = merged.loc[mismatch].copy()
            if not changed.empty:
                _issue(issues, "geometry preservation", "blocked", f"Identity attachment changed geometry for {len(changed)} ROI rows.")
            elif compared_fields:
                _issue(issues, "geometry preservation", "pass", f"Geometry fields unchanged: {compared_fields}")
            else:
                _issue(issues, "geometry preservation", "blocked", "No shared geometry fields were available to compare.")

    identity_summary = pd.DataFrame()
    if identity is not None:
        label_col = next((c for c in ("identity_display_label", "identity_label", "gene") if c in identity), None)
        if label_col:
            identity_summary = identity[label_col].fillna("unassigned").value_counts(dropna=False).rename_axis("identity").rename("n_rois").reset_index()
        else:
            _issue(issues, "identity attachment", "blocked", "No identity label column found.")
    return {
        "fish_id": fish_id,
        "status": _status(issues),
        "issues": pd.DataFrame(issues),
        "identity_summary": identity_summary,
        "geometry_changes": changed,
        "compared_geometry_fields": compared_fields,
        "read_only": True,
    }


def plot_molecular_identity_qc(report: Mapping) -> plt.Figure:
    summary = report["identity_summary"]
    fig, ax = plt.subplots(figsize=(7, max(3.2, 0.35 * max(1, len(summary)))))
    if summary.empty:
        ax.text(0.5, 0.5, "Identity summary unavailable", ha="center", va="center")
        ax.set_axis_off()
    else:
        ax.barh(summary["identity"].astype(str), summary["n_rois"], color="#756bb1")
        ax.set_xlabel("Functional ROIs")
        ax.set_title("Identity attached after frozen geometry")
    fig.suptitle(f"{report['fish_id']} molecular identity review")
    fig.tight_layout()
    return fig


def _inspect_response_sessions(table: pd.DataFrame | None, *, area: str, windows: pd.DataFrame | None,
                               reference_manifest: Mapping | None, issues: list[dict]) -> None:
    """Validate existing plane/session provenance without extending canonical tables."""
    if table is None or "plane_idx" not in table:
        return
    planes = pd.to_numeric(table["plane_idx"], errors="coerce")
    if planes.isna().any() or (planes % 1).ne(0).any():
        _issue(issues, area, "blocked", "ROI plane indices are missing or nonintegral.")
        return
    required_planes = set(planes.astype(int))
    session_col = next((c for c in ("session_id", "session", "functional_session", "response_session", "session_label") if c in table), None)
    sources = []
    if session_col:
        sources.append(("ROI table", table, session_col))
    if windows is not None:
        sources.append(("scored stimulus windows", windows, "session_label"))
    if not sources:
        _issue(issues, area, "blocked", "Session-aware provenance requires explicit ROI sessions or persisted scored stimulus windows.")
        return
    mappings = []
    for name, frame, column in sources:
        if not {"plane_idx", column}.issubset(frame.columns):
            _issue(issues, area, "blocked", f"{name} lacks plane/session provenance columns.")
            return
        numeric = pd.to_numeric(frame["plane_idx"], errors="coerce")
        labels = frame[column].astype("string").str.strip()
        if numeric.isna().any() or (numeric % 1).ne(0).any() or labels.isna().any() or labels.eq("").any():
            _issue(issues, area, "blocked", f"{name} contains missing or invalid plane/session provenance.")
            return
        pairs = pd.DataFrame({"plane": numeric.astype(int), "session": labels}).drop_duplicates()
        if pairs.groupby("plane")["session"].nunique().gt(1).any():
            _issue(issues, area, "blocked", f"{name} assigns more than one session to a plane.")
            return
        mapping = dict(zip(pairs.plane, pairs.session))
        if not required_planes.issubset(mapping):
            _issue(issues, area, "blocked", f"{name} omits ROI planes {sorted(required_planes.difference(mapping))}.")
            return
        mappings.append(mapping)
    mapping = mappings[0]
    if any(any(other[plane] != mapping[plane] for plane in required_planes) for other in mappings[1:]):
        _issue(issues, area, "blocked", "ROI session provenance disagrees with scored stimulus windows.")
        return
    if reference_manifest is not None:
        selections = reference_manifest.get("parameters", {}).get("frame_selection_by_plane", {})
        reference_rows = []
        for label, record in selections.items():
            match = re.search(r"_plane(\d+)(?:_|$)", label)
            if match:
                reference_rows.append({"plane_idx": int(match.group(1)), "session_label": record.get("session_label")})
        refs = pd.DataFrame(reference_rows)
        if refs.empty or refs.session_label.isna().any() or refs.session_label.astype(str).str.strip().eq("").any():
            _issue(issues, area, "blocked", "Functional-reference manifest lacks valid per-plane session selections.")
            return
        if refs.groupby("plane_idx").session_label.nunique().gt(1).any():
            _issue(issues, area, "blocked", "Functional-reference selections assign multiple sessions to a plane.")
            return
        refmap = dict(zip(refs.plane_idx, refs.session_label))
        if not required_planes.issubset(refmap) or any(refmap[plane] != mapping[plane] for plane in required_planes):
            _issue(issues, area, "blocked", "Scored sessions disagree with functional-reference plane selections.")
            return
    if "func_source" in table:
        source_planes = table.func_source.astype(str).str.extract(r"(?:^|/)plane(\d+)(?:/|$)", expand=False)
        if source_planes.isna().any() or pd.to_numeric(source_planes).ne(planes).any():
            _issue(issues, area, "blocked", "ROI functional sources disagree with their persisted plane indices.")
            return
    _issue(issues, area, "pass", f"Unique sessions verified for {len(required_planes)} ROI planes from persisted provenance.")


def inspect_activity_export_qc(
    *,
    fish_id: str,
    pipeline_root: str | Path,
    fish_dir: str | Path | None = None,
    roi_master_path: str | Path | None,
    bpi_cells_path: str | Path | None,
    hcr_status_path: str | Path | None,
    hcr_pairs_path: str | Path | None,
    trace_meta_path: str | Path | None = None,
    require_hcr_exports: bool = True,
    require_trace_meta: bool = True,
    expected_figure_paths: Sequence[str | Path] = (),
    scored_windows_path: str | Path | None = None,
    reference_manifest_path: str | Path | None = None,
) -> dict:
    """Review response/BPI provenance and canonical ROI/HCR export scopes."""
    _validate_fish(fish_id)
    root = Path(pipeline_root).expanduser().resolve()
    roots = _review_roots(root, fish_dir)
    issues: list[dict] = []
    tables: dict[str, pd.DataFrame | None] = {}
    for name, path, required in (
        ("roi_master", roi_master_path, True),
        ("bpi_cells", bpi_cells_path, True),
        ("hcr_status", hcr_status_path, require_hcr_exports),
        ("hcr_pairs", hcr_pairs_path, require_hcr_exports),
        ("trace_meta", trace_meta_path, require_trace_meta),
    ):
        if not required and (path is None or not _path(path).is_file()):
            tables[name] = None
            _issue(issues, name, "not_applicable", "Optional downstream export was not produced for this selected run.")
            continue
        table_path, tables[name] = _load_csv(path, area=name, issues=issues)
        _check_root(table_path, roots, area=name, issues=issues)
        _fish_check(tables[name], fish_id, area=name, issues=issues)

    master = tables["roi_master"]
    bpi = tables["bpi_cells"]
    hcr_status = tables["hcr_status"]
    hcr_pairs = tables["hcr_pairs"]
    _require_columns(master, (*_ROI_KEYS, "response_is_active", "response_class", "bpi", "bpi_category"), area="roi_master", issues=issues)
    _require_columns(bpi, (*_ROI_KEYS, "response_is_active", "bpi", "bpi_category"), area="bpi_cells", issues=issues)
    if hcr_status is not None:
        _require_columns(
            hcr_status,
            ("fish_id", "gene", "anat_label", "functional_status", "represented_on_func_plane", "selection_rule", "match_policy_version"),
            area="hcr_status",
            issues=issues,
        )
    if hcr_pairs is not None:
        _require_columns(
            hcr_pairs,
            ("fish_id", "gene", "conf_label", "anat_label", "func_label", "plane", "response_is_active", "response_class", "is_selected_for_analysis", "match_policy_version"),
            area="hcr_pairs",
            issues=issues,
        )
    trace_meta = tables["trace_meta"]
    if trace_meta is not None:
        _require_columns(trace_meta, _ROI_KEYS, area="trace_meta", issues=issues)

    windows = None
    if scored_windows_path is not None:
        windows_path, windows = _load_csv(scored_windows_path, area="session provenance", issues=issues)
        _check_root(windows_path, roots, area="session provenance", issues=issues)
        _fish_check(windows, fish_id, area="session provenance", issues=issues)
    reference_manifest = None
    if reference_manifest_path is not None:
        reference_path = _path(reference_manifest_path)
        _check_root(reference_path, roots, area="reference session provenance", issues=issues)
        try:
            reference_manifest = json.loads(reference_path.read_text())
            if not isinstance(reference_manifest, dict):
                raise ValueError("Functional-reference manifest must be a JSON object.")
            parameters = reference_manifest.get("parameters", {})
            if not isinstance(parameters, dict):
                raise ValueError("Functional-reference parameters must be a JSON object.")
            selections = parameters.get("frame_selection_by_plane", {})
            if not isinstance(selections, dict) or any(not isinstance(row, dict) for row in selections.values()):
                raise ValueError("Functional-reference plane selections and rows must be JSON objects.")
            if reference_manifest.get("fish_id") != fish_id:
                raise ValueError("Functional-reference manifest fish does not match.")
        except (OSError, ValueError, TypeError) as exc:
            reference_manifest = None
            _issue(issues, "reference session provenance", "blocked", str(exc))

    for name, table in (("roi_master", master), ("bpi_cells", bpi)):
        if table is None:
            continue
        if all(c in table for c in _ROI_KEYS) and table.duplicated(list(_ROI_KEYS)).any():
            _issue(issues, name, "blocked", "Canonical ROI key (plane_idx, func_label) is not unique.")
        _inspect_response_sessions(table, area=name, windows=windows, reference_manifest=reference_manifest, issues=issues)

    scope_rows = [
        {"artifact": "functional_roi_activity_identity.csv", "scope": "ROI-centric whole population", "rows": 0 if master is None else len(master)},
        {"artifact": "functional_roi_activity_bpi_cells.csv", "scope": "ROI-centric response/BPI diagnostics", "rows": 0 if bpi is None else len(bpi)},
        {"artifact": "hcr_activity_status.csv", "scope": "HCR-centric identified-cell status", "rows": 0 if hcr_status is None else len(hcr_status)},
        {"artifact": "conf_to_func_pairs.csv", "scope": "HCR-centric identified-cell trace export", "rows": 0 if hcr_pairs is None else len(hcr_pairs)},
    ]
    if master is not None and hcr_pairs is not None and len(master) == len(hcr_pairs) and len(master) > 0:
        _issue(issues, "scope separation", "warn", "ROI- and HCR-centric tables have identical row counts; verify that one did not replace the other.")

    figure_rows = []
    for item in expected_figure_paths:
        figure = _path(item)
        _check_root(figure, roots, area="figure", issues=issues)
        present = bool(figure and figure.is_file() and figure.stat().st_size > 0)
        figure_rows.append({"path": str(figure), "status": "present" if present else "missing"})
        _issue(issues, "figure", "present" if present else "missing", "Persisted non-empty figure." if present else "Figure missing/empty; no rendering was attempted.", figure)

    response_summary = pd.DataFrame()
    if master is not None and "response_class" in master:
        response_summary = master["response_class"].fillna("missing").value_counts().rename_axis("response_class").rename("n_rois").reset_index()
    return {
        "fish_id": fish_id,
        "status": _status(issues),
        "issues": pd.DataFrame(issues),
        "scope_summary": pd.DataFrame(scope_rows),
        "response_summary": response_summary,
        "figures": pd.DataFrame(figure_rows, columns=["path", "status"]),
        "tables": tables,
        "read_only": True,
    }


def plot_activity_export_qc(report: Mapping) -> plt.Figure:
    response = report["response_summary"]
    scopes = report["scope_summary"]
    fig, axes = plt.subplots(1, 2, figsize=(11, 4))
    if response.empty:
        axes[0].text(0.5, 0.5, "Response summary unavailable", ha="center", va="center")
        axes[0].set_axis_off()
    else:
        axes[0].barh(response["response_class"].astype(str), response["n_rois"], color="#3182bd")
        axes[0].set_xlabel("ROI-centric ROIs")
        axes[0].set_title("Session-aware response classes")
    axes[1].barh(scopes["scope"], scopes["rows"], color=["#2c7fb8", "#7fcdbb", "#fdae6b", "#e6550d"])
    axes[1].set_xlabel("Rows (scopes are not interchangeable)")
    axes[1].set_title("Canonical export scopes")
    fig.suptitle(f"{report['fish_id']} activity and export review")
    fig.tight_layout()
    return fig


_BPI_CATEGORY_COLORS = {
    "bout-responsive": "#2c7fb8",
    "continuous-responsive": "#d95f0e",
    "both-responsive": "#d946ef",
    "weak-response": "#000000",
    "low activity": "#9e9e9e",
    "response unavailable": "#ececec",
}
_BPI_CATEGORY_ORDER = tuple(_BPI_CATEGORY_COLORS)
_BPI_PREFERENCE_COLORS = {
    "bout-preferential": "#2c7fb8",
    "continuous-preferential": "#d95f0e",
    "non-preferential": "#d946ef",
    "suppressed-only": "#756bb1",
}
_RESPONSE_CLASS_MARKERS = {
    "bout-responsive": "o",
    "continuous-responsive": "s",
    "both-responsive": "D",
}


def build_activity_bpi_gate_table(bpi_cells_path: str | Path) -> tuple[pd.DataFrame, dict[str, float]]:
    """Load the complete ROI table and normalize persisted response/BPI fields."""
    path = Path(bpi_cells_path)
    if not path.is_file():
        raise FileNotFoundError(f"Missing response/BPI cells table: {path}")
    table = pd.read_csv(path)
    required = {
        "mean_bout_auc_dff", "mean_cont_auc_dff", "response_magnitude_auc", "activity_mag", "bpi",
        "response_is_active", "response_class", "bpi_category", "bpi_preference",
        "bpi_zero_band", "bpi_activity_threshold",
    }
    missing = sorted(required - set(table.columns))
    if missing:
        raise ValueError(f"Response/BPI cells table lacks required columns {missing}: {path}")
    thresholds: dict[str, float] = {}
    for column in ("bpi_zero_band", "bpi_activity_threshold"):
        values = pd.to_numeric(table[column], errors="coerce").dropna().unique()
        if len(values) != 1:
            raise ValueError(f"Expected one persisted {column} value in {path}; found {values.tolist()}")
        thresholds[column] = float(values[0])
    work = table.copy()
    for column in ("mean_bout_auc_dff", "mean_cont_auc_dff", "response_magnitude_auc", "activity_mag", "bpi"):
        work[column] = pd.to_numeric(work[column], errors="coerce")
    for column, fallback in (
        ("response_class", "response unavailable"),
        ("bpi_category", "response unavailable"),
        ("bpi_preference", "unavailable"),
    ):
        work[column] = work[column].fillna(fallback).astype(str).str.strip().str.lower()
    active = work["response_is_active"]
    if pd.api.types.is_bool_dtype(active):
        work["response_is_active"] = active.fillna(False).astype(bool)
    elif pd.api.types.is_numeric_dtype(active):
        work["response_is_active"] = active.fillna(0).astype(float).ne(0)
    else:
        work["response_is_active"] = active.astype(str).str.strip().str.lower().isin({"1", "true", "t", "yes", "y"})
    work["abs_bpi"] = work["bpi"].abs()
    assessable = (
        work["response_class"].ne("response unavailable")
        & np.isfinite(work["mean_bout_auc_dff"])
        & np.isfinite(work["mean_cont_auc_dff"])
        & np.isfinite(work["response_magnitude_auc"])
        & np.isfinite(work["activity_mag"])
    )
    if not assessable.any():
        raise ValueError(f"No assessable response rows available for review: {path}")
    return work, thresholds


def plot_activity_bpi_gate(bpi_cells_path: str | Path, *, fish_id: str) -> tuple[plt.Figure, pd.DataFrame]:
    """Render response-test classes separately from BPI effect-size preference."""
    work, thresholds = build_activity_bpi_gate_table(bpi_cells_path)
    zero_band = thresholds["bpi_zero_band"]
    auc_floor = thresholds["bpi_activity_threshold"]
    assessable = work.loc[
        work["response_class"].ne("response unavailable")
        & np.isfinite(work["mean_bout_auc_dff"])
        & np.isfinite(work["mean_cont_auc_dff"])
        & np.isfinite(work["response_magnitude_auc"])
        & np.isfinite(work["activity_mag"])
    ].copy()
    responsive = assessable.loc[
        assessable["response_is_active"]
    ].copy()
    nonresponsive = assessable.loc[~assessable["response_is_active"]].copy()

    fig, axes = plt.subplots(2, 3, figsize=(15.0, 8.8), constrained_layout=True)
    ax_plane, ax_bpi, ax_bins, ax_dist, ax_strength, ax_donut = axes.flat

    ax_plane.scatter(
        nonresponsive["mean_cont_auc_dff"], nonresponsive["mean_bout_auc_dff"],
        s=9, marker="x", linewidths=0.45, alpha=0.22, color="#7f7f7f",
    )
    for response_class, marker in _RESPONSE_CLASS_MARKERS.items():
        for preference, color in _BPI_PREFERENCE_COLORS.items():
            subset = responsive.loc[
                responsive["response_class"].eq(response_class)
                & responsive["bpi_preference"].eq(preference)
            ]
            if subset.empty:
                continue
            scatter_kw = dict(s=19, marker=marker, alpha=0.75, color=color)
            ax_plane.scatter(subset["mean_cont_auc_dff"], subset["mean_bout_auc_dff"], **scatter_kw)
            ax_bpi.scatter(subset["response_magnitude_auc"], subset["bpi"], **scatter_kw)
            ax_strength.scatter(subset["activity_mag"], subset["bpi"], **scatter_kw)

    limit = 1.05 * float(np.nanmax(np.abs(assessable[["mean_bout_auc_dff", "mean_cont_auc_dff"]].to_numpy())))
    ax_plane.plot([-limit, limit], [-limit, limit], "--", color="#444444", linewidth=0.9)
    ax_plane.axvline(auc_floor, color="#777777", linestyle=":", linewidth=0.8)
    ax_plane.text(
        0.98, 0.02, "dotted: 0.05 AUC floor\nnull threshold: cell-specific",
        transform=ax_plane.transAxes, ha="right", va="bottom", fontsize=7,
        bbox={"facecolor": "white", "edgecolor": "none", "alpha": 0.8},
    )
    ax_plane.axhline(auc_floor, color="#777777", linestyle=":", linewidth=0.8)
    ax_plane.set_xscale("symlog", linthresh=0.01)
    ax_plane.set_yscale("symlog", linthresh=0.01)
    ax_plane.set(
        xlim=(-limit, limit), ylim=(-limit, limit),
        xlabel="Continuous response AUC (dF/F·s)", ylabel="Bout response AUC (dF/F·s)",
        title=f"Pooled signed condition AUCs (n={len(assessable)} assessable)\ncalls: any side; color=BPI preference / suppression; marker=response test",
    )

    preference_handles = [
        Line2D(
            [0], [0], marker="o", linestyle="", color=color, markersize=5,
            label=f"{preference} (n={int(responsive['bpi_preference'].eq(preference).sum())})",
        )
        for preference, color in _BPI_PREFERENCE_COLORS.items()
        if responsive["bpi_preference"].eq(preference).any()
    ]
    class_handles = [
        Line2D(
            [0], [0], marker=marker, linestyle="", markerfacecolor="white",
            markeredgecolor="#222222", color="#222222", markersize=5,
            label=f"{response_class} (n={int(responsive['response_class'].eq(response_class).sum())})",
        )
        for response_class, marker in _RESPONSE_CLASS_MARKERS.items()
        if responsive["response_class"].eq(response_class).any()
    ]
    nonresponse_handle = Line2D(
        [0], [0], marker="x", linestyle="", color="#7f7f7f", markersize=5,
        label=f"no response pass (n={len(nonresponsive)})",
    )

    for axis, x_label in (
        (ax_bpi, "Mean positive response AUC (dF/F·s)"),
        (ax_strength, "Mean absolute z-response"),
    ):
        axis.axhline(0, color="#444444", linewidth=0.8)
        axis.axhline(zero_band, color="#444444", linestyle=":", linewidth=1.0)
        axis.axhline(-zero_band, color="#444444", linestyle=":", linewidth=1.0)
        axis.set(xlabel=x_label, ylabel="BPI", ylim=(-1.02, 1.02))
    effect_note = (
        f"n={int(responsive['bpi'].notna().sum())} finite tuning BPIs; {int(responsive['bpi_preference'].eq('suppressed-only').sum())} suppressed-only (BPI unavailable)\n"
        f"|BPI| > {zero_band:g} is an effect-size rule, not a contrast test"
    )
    ax_bpi.set_title("BPI versus response magnitude\n" + effect_note, fontsize=9.5)
    ax_strength.set_title("BPI versus z-scored response\n" + effect_note, fontsize=9.5)

    n_bins = min(6, int(assessable["response_magnitude_auc"].nunique()))
    if n_bins >= 2:
        binned = (
            assessable.assign(
                _bin=pd.qcut(assessable["response_magnitude_auc"], q=n_bins, duplicates="drop")
            )
            .groupby("_bin", observed=False)["response_is_active"]
            .agg(["mean", "count"])
            .reset_index()
        )
        mids = np.asarray([0.5 * (item.left + item.right) for item in binned["_bin"]], dtype=float)
        ax_bins.plot(mids, binned["mean"], marker="o", color="#1f77b4")
    ax_bins.set(
        xlabel="Response AUC (dF/F·s; bin midpoint)", ylabel="Fraction response-positive",
        ylim=(0, 1.02), title=f"Response calls across all assessable ROIs (n={len(assessable)}; equal-frequency bins)",
    )
    ax_bins.legend(
        handles=[*preference_handles, *class_handles, nonresponse_handle],
        fontsize=6.4, frameon=False, loc="upper left", ncol=1,
    )

    distribution_categories = [
        category for category in _BPI_CATEGORY_ORDER
        if category != "response unavailable" and assessable["response_class"].eq(category).any()
    ]
    distribution_data = [
        assessable.loc[assessable["response_class"].eq(category), "activity_mag"].to_numpy()
        for category in distribution_categories
    ]
    boxes = ax_dist.boxplot(
        distribution_data,
        labels=[f"{category}\n(n={len(values)})" for category, values in zip(distribution_categories, distribution_data)],
        showfliers=True, patch_artist=True,
        flierprops={"marker": ".", "markersize": 2.5, "markerfacecolor": "#444444", "markeredgecolor": "none", "alpha": 0.35},
    )
    for patch, category in zip(boxes["boxes"], distribution_categories):
        patch.set_facecolor(_BPI_CATEGORY_COLORS.get(category, "#cccccc"))
        patch.set_alpha(0.45)
    ax_dist.set(ylabel="Mean absolute z-response", title=f"Z-response by response-test class (n={len(assessable)})")
    ax_dist.tick_params(axis="x", labelrotation=18, labelsize=7)

    all_counts = work["response_class"].value_counts()
    donut_categories = [
        item for item in _BPI_CATEGORY_ORDER if item in all_counts.index
    ]
    donut_categories.extend(sorted(set(all_counts.index) - set(donut_categories)))
    counts = all_counts.reindex(donut_categories, fill_value=0)
    ax_donut.pie(
        counts.to_numpy(),
        colors=[_BPI_CATEGORY_COLORS.get(item, "#666666") for item in counts.index],
        startangle=90, wedgeprops={"width": 0.42, "edgecolor": "white"},
    )
    ax_donut.text(0, 0, f"n={len(work)}\nall ROIs", ha="center", va="center", fontsize=10)
    ax_donut.legend(
        [f"{item} (n={int(count)})" for item, count in counts.items()],
        fontsize=7, loc="center left", bbox_to_anchor=(1.0, 0.5), frameon=False,
    )
    ax_donut.set_title("Population response-test classes")
    fig.suptitle(
        f"{fish_id}: response significance and BPI effect size (ROI-centric)",
        fontsize=14, fontweight="bold",
    )
    return fig, work


def _gene_round(path: str | Path) -> tuple[str, str]:
    """Return stable display keys from a persisted HCR artifact name."""
    name = Path(path).name
    match = re.search(r"_(rbest|r\d+|round\d+)_channel\d+_(.+?)_cp_masks", name, flags=re.IGNORECASE)
    if not match:
        return gene_from_mask(path), "unknown round"
    return match.group(2).replace("_", "."), match.group(1)


def resolve_hcr_matching_artifact_root(
    *, staged_root: str | Path, fish_dir: str | Path,
) -> Path:
    """Prefer staged HCR matching artifacts, with an explicit fish-local fallback.

    Legacy control fish can have persisted, reviewable HCR artifacts without a
    staged writer root. This resolver selects one existing artifact set only;
    it never combines roots or recomputes matching.
    """
    staged = Path(staged_root)
    pattern = "*_cp_masks_in_2p_labels_uint16.tif"
    if any(not path.name.startswith("._") for path in staged.glob(pattern)):
        return staged
    legacy = Path(fish_dir) / "03_analysis" / "confocal" / "aligned"
    if any(not path.name.startswith("._") for path in legacy.glob(pattern)):
        return legacy
    return staged


def build_hcr_matching_flow_table(
    *, label_paths: Sequence[str | Path], review_paths: Sequence[str | Path], final_pair_paths: Sequence[str | Path]
) -> pd.DataFrame:
    """Summarize persisted HCR→anatomy matching stages without recomputing pairs."""
    final_by_stem = {Path(p).name.replace("_final_pairs.csv", ""): Path(p) for p in final_pair_paths}
    review_by_stem = {Path(p).name.replace("_review.csv", ""): Path(p) for p in review_paths}
    rows: list[dict] = []
    for raw_path in label_paths:
        path = Path(raw_path)
        stem = path.name.replace("_labels_uint16.tif", "")
        labels = np.unique(imread_any(path)); labels = labels[labels > 0]
        review = pd.read_csv(review_by_stem[stem]) if stem in review_by_stem else pd.DataFrame()
        final = pd.read_csv(final_by_stem[stem]) if stem in final_by_stem else pd.DataFrame()
        gene, round_name = _gene_round(path)
        review_labels = set(review.get("conf_label", pd.Series(dtype=int)).dropna().astype(int))
        gate = review["within_gate"].astype(str).str.strip().str.lower().isin(("true", "1", "yes")) if "within_gate" in review else pd.Series(False, index=review.index)
        gated = set(review.loc[gate, "conf_label"].dropna().astype(int))
        ambiguous = set(review.loc[review["pair_type"].astype(str).ne("1-1"), "conf_label"].dropna().astype(int)) if "pair_type" in review else set()
        good = set(review.loc[review["quality"].astype(str).str.lower().eq("good"), "conf_label"].dropna().astype(int)) if "quality" in review else set()
        accepted_rows = final[final["pair_type"].astype(str).eq("1-1")] if "pair_type" in final else final
        accepted = set(accepted_rows.get("conf_label", pd.Series(dtype=int)).dropna().astype(int))
        candidate = review_labels | accepted
        final_gate = set(accepted_rows.loc[accepted_rows["within_gate"].astype(str).str.strip().str.lower().isin(("true", "1", "yes")), "conf_label"].dropna().astype(int)) if "within_gate" in accepted_rows else set()
        final_good = set(accepted_rows.loc[accepted_rows["quality"].astype(str).str.lower().eq("good"), "conf_label"].dropna().astype(int)) if "quality" in accepted_rows else set()
        stages = {"segmented": set(labels.astype(int)), "candidate": candidate, "within gate": gated | final_gate,
                  "ambiguous": ambiguous, "quality good": good | final_good,
                  "rejected/iffy review": review_labels - accepted, "accepted 1:1": accepted}
        for stage, values in stages.items():
            rows.append({"gene": gene, "round": round_name, "stage": stage, "n_labels": len(values)})
    return pd.DataFrame(rows)


def plot_hcr_matching_flow(flow: pd.DataFrame, *, fish_id: str) -> plt.Figure:
    """Plot a per-gene/round count flow; rejected labels remain visible by stage."""
    if flow.empty:
        raise ValueError("No persisted HCR matching artifacts were supplied")
    groups = flow[["gene", "round"]].drop_duplicates().itertuples(index=False, name=None)
    groups = list(groups)
    # A single gene/round still needs enough room for the fish-qualified
    # read-only provenance title; the old one-panel width clipped it.
    fig, axes = plt.subplots(1, len(groups), figsize=(max(6.5, 4.2 * len(groups)), 4.2), squeeze=False, constrained_layout=True)
    order = ["segmented", "candidate", "within gate", "ambiguous", "quality good", "rejected/iffy review", "accepted 1:1"]
    for ax, (gene, round_name) in zip(axes.flat, groups):
        part = flow[(flow.gene == gene) & (flow["round"] == round_name)].set_index("stage").n_labels.reindex(order, fill_value=0)
        ax.bar(range(len(order)), part, color=["#7f7f7f", "#4c78a8", "#72b7b2", "#eeca3b", "#59a14f", "#00A651"])
        ax.set_xticks(range(len(order)), order, rotation=45, ha="right", fontsize=8)
        ax.set_ylabel("HCR labels (n)"); ax.set_title(f"{gene} ({round_name})")
        for x, value in enumerate(part): ax.text(x, value, str(int(value)), ha="center", va="bottom", fontsize=8)
    fig.suptitle(f"{fish_id}: persisted HCR→anatomy matching flow", fontweight="bold")
    return fig


_HCR_FLOW_OUTER_ORDER = (
    "not candidate",
    "gate failure",
    "ambiguous relation",
    "quality/review rejection",
    "accepted 1:1",
)
_HCR_FLOW_OUTER_COLORS = {
    "not candidate": "#bdbdbd",
    "gate failure": "#e69f00",
    "ambiguous relation": "#cc79a7",
    "quality/review rejection": "#d55e00",
    "accepted 1:1": "#00A651",
}


# This is deliberately the legacy [50]-aligned vocabulary.  It represents the
# terminal anatomical outcome of every *segmented HCR label*, rather than the
# later manual-review/acceptance process.  The latter remains available via the
# flow functions above for cases where it is the question being reviewed.
_HCR_ANATOMY_OUTCOME_ORDER = (
    "in-plane anatomy match",
    "out-of-plane anatomy match",
    ">q95 large mask",
    "too far / no overlap anatomy",
    "split anatomy relation",
    "merged anatomy relation",
    "complex anatomy relation",
    "1-to-1 anatomy relation, IoU below threshold",
    "other anatomy rejection",
)
_HCR_ANATOMY_OUTCOME_COLORS = {
    "in-plane anatomy match": "#2ca02c",
    "out-of-plane anatomy match": "#ff7f0e",
    ">q95 large mask": "#8c564b",
    "too far / no overlap anatomy": "#d62728",
    "split anatomy relation": "#17becf",
    "merged anatomy relation": "#9467bd",
    "complex anatomy relation": "#7f7f7f",
    "1-to-1 anatomy relation, IoU below threshold": "#bcbd22",
    "other anatomy rejection": "#1f77b4",
}

_HCR_FUNCTIONAL_SCOPE_ORDER = ("within plane", "outside plane", "unmatched")
_HCR_FUNCTIONAL_SCOPE_COLORS = {
    "within plane": "#4daf4a",
    "outside plane": "#377eb8",
    "unmatched": "#bdbdbd",
}
_HCR_FUNCTIONAL_OUTER_ORDER = (
    "responsive ROI", "low-activity ROI", "response unavailable", "no functional match", "out of plane", "unmatched",
)
_HCR_FUNCTIONAL_OUTER_COLORS = {
    "responsive ROI": "#1b9e77",
    "low-activity ROI": "#666666",
    "response unavailable": "#fdb462",
    "no functional match": "#d73027",
    "out of plane": "#80b1d3",
    "unmatched": "#bdbdbd",
}


def _as_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if value is None or (isinstance(value, float) and not np.isfinite(value)):
        return False
    return str(value).strip().lower() in {"1", "true", "t", "yes", "y"}


def _hcr_status_plane_lookup(path: str | Path | None) -> dict[tuple[str, str, int], bool]:
    """Read legacy HCR status plane representation, if it was persisted."""
    if path is None or not Path(path).is_file():
        return {}
    status = pd.read_csv(path)
    required = {"gene", "conf_label", "represented_on_func_plane"}
    if not required.issubset(status.columns):
        return {}
    columns = ["gene", "conf_label", "represented_on_func_plane"]
    has_mask = "conf_mask" in status.columns
    if has_mask:
        columns.append("conf_mask")
    lookup: dict[tuple[str, str, int], bool] = {}
    for row in status.loc[:, columns].itertuples(index=False):
        label = pd.to_numeric(pd.Series([row.conf_label]), errors="coerce").iloc[0]
        if pd.notna(label):
            gene = str(row.gene).strip().lower()
            # The filename-specific key prevents a label ID reused by another
            # round/mask from silently inheriting the wrong plane status.  The
            # empty-name fallback supports older status tables without a path.
            mask_name = Path(str(row.conf_mask)).name if has_mask else ""
            lookup[(mask_name, gene, int(label))] = _as_bool(row.represented_on_func_plane)
    return lookup


def build_hcr_functional_plane_status_donut_table(
    *, label_paths: Sequence[str | Path], activity_status_path: str | Path,
) -> pd.DataFrame:
    """Build the legacy two-ring functional-plane representation audit.

    The inner ring partitions post-q95 HCR labels into within-plane,
    outside-plane, and unmatched.  The outer ring retains the saved functional
    status for within-plane labels, and explicitly labels the other two scope
    outcomes.  It is a read-only view of persisted labels, q95 metadata, and
    ``hcr_activity_status.csv``; it does not redo HCR/anatomy/ROI matching.
    """
    status_path = Path(activity_status_path)
    if not status_path.is_file():
        raise FileNotFoundError(f"Persisted HCR activity status is missing: {status_path}")
    status = pd.read_csv(status_path)
    required = {"conf_mask", "conf_label", "gene", "represented_on_func_plane", "functional_status"}
    missing = sorted(required.difference(status.columns))
    if missing:
        raise ValueError(f"HCR activity status lacks columns: {missing}")
    status = status.copy()
    status["conf_mask_name"] = status["conf_mask"].map(lambda value: Path(str(value)).name)
    status["conf_label"] = pd.to_numeric(status["conf_label"], errors="coerce")
    status = status.dropna(subset=["conf_label"]).copy()
    status["conf_label"] = status["conf_label"].astype(int)
    status_by_key = {
        (str(row.conf_mask_name), int(row.conf_label)): row
        for row in status.sort_values(["conf_mask_name", "conf_label"]).drop_duplicates(["conf_mask_name", "conf_label"]).itertuples(index=False)
    }

    def _within_outer(functional_status: object) -> str:
        value = str(functional_status).strip().lower()
        if "response unavailable" in value:
            return "response unavailable"
        if "low-activity" in value:
            return "low-activity ROI"
        if "responsive" in value:
            return "responsive ROI"
        return "no functional match"

    rows: list[dict] = []
    for raw_path in label_paths:
        path = Path(raw_path)
        labels = set(np.unique(imread_any(path)).astype(int)) - {0}
        meta_path = path.with_name(path.name.replace("_labels_uint16.tif", "_warp_meta.json"))
        q95_labels: set[int] = set()
        if meta_path.is_file():
            try:
                q95_labels = {int(value) for value in json.loads(meta_path.read_text()).get("filter_stats", {}).get("low_conf_labels", [])}
            except (OSError, ValueError, TypeError):
                pass
        gene, round_name = _gene_round(path)
        raw_mask_name = path.name.replace("_in_2p_labels_uint16.tif", ".tif")
        for label in sorted(labels - q95_labels):
            row = status_by_key.get((path.name, int(label)))
            if row is None:
                row = status_by_key.get((raw_mask_name, int(label)))
            if row is None:
                scope, outer = "unmatched", "unmatched"
            elif _as_bool(row.represented_on_func_plane):
                scope, outer = "within plane", _within_outer(row.functional_status)
            else:
                scope, outer = "outside plane", "out of plane"
            rows.append({"gene": gene, "round": round_name, "scope": scope, "outer_category": outer, "conf_label": int(label)})
    if not rows:
        return pd.DataFrame(columns=["panel", "gene", "round", "ring", "category", "n_labels"])
    label_rows = pd.DataFrame(rows)
    panels: list[tuple[str, pd.DataFrame]] = [("All genes", label_rows)]
    order = {"pth2": 0, "sst1.1": 1, "sst1.2": 2, "tac3b": 3}
    for gene in sorted(label_rows["gene"].unique(), key=lambda value: (order.get(str(value).lower(), len(order)), str(value))):
        panels.append((str(gene), label_rows[label_rows["gene"].eq(gene)]))
    out_rows: list[dict] = []
    for panel, subset in panels:
        panel_gene = "all" if panel == "All genes" else panel
        panel_round = "all" if panel == "All genes" else ",".join(sorted(subset["round"].astype(str).unique()))
        for category in _HCR_FUNCTIONAL_SCOPE_ORDER:
            out_rows.append({"panel": panel, "gene": panel_gene, "round": panel_round, "ring": "inner", "category": category, "n_labels": int(subset["scope"].eq(category).sum())})
        for category in _HCR_FUNCTIONAL_OUTER_ORDER:
            out_rows.append({"panel": panel, "gene": panel_gene, "round": panel_round, "ring": "outer", "category": category, "n_labels": int(subset["outer_category"].eq(category).sum())})
    return pd.DataFrame(out_rows)


def plot_hcr_functional_plane_status_donuts(donuts: pd.DataFrame, *, fish_id: str) -> plt.Figure:
    """Render the legacy inner-scope / outer-functional-status donut layout."""
    required = {"panel", "ring", "category", "n_labels"}
    missing = sorted(required.difference(donuts.columns))
    if missing:
        raise ValueError(f"HCR functional-plane donut table lacks columns: {missing}")
    panels = donuts["panel"].drop_duplicates().tolist()
    if not panels:
        raise ValueError("No persisted HCR activity-status rows were supplied")
    ncols, nrows = 3, int(np.ceil(len(panels) / 3))
    fig, axes = plt.subplots(nrows, ncols, figsize=(13.8, 5.5 * nrows), squeeze=False)
    for axis in axes.flat[len(panels):]:
        axis.set_visible(False)
    for axis, panel in zip(axes.flat, panels):
        subset = donuts[donuts["panel"].eq(panel)]
        inner = subset[subset["ring"].eq("inner")].set_index("category")["n_labels"].reindex(_HCR_FUNCTIONAL_SCOPE_ORDER, fill_value=0)
        outer = subset[subset["ring"].eq("outer")].set_index("category")["n_labels"].reindex(_HCR_FUNCTIONAL_OUTER_ORDER, fill_value=0)
        total = int(inner.sum())
        axis.pie(inner, radius=0.98, startangle=90, counterclock=False, colors=[_HCR_FUNCTIONAL_SCOPE_COLORS[item] for item in inner.index], wedgeprops={"width": 0.40, "edgecolor": "white", "linewidth": 1.0})
        axis.pie(outer, radius=1.17, startangle=90, counterclock=False, colors=[_HCR_FUNCTIONAL_OUTER_COLORS[item] for item in outer.index], wedgeprops={"width": 0.15, "edgecolor": "white", "linewidth": 1.0})
        for wedge, value in zip(axis.patches[len(inner):], outer):
            if int(value) == 0:
                continue
            angle = np.deg2rad((wedge.theta1 + wedge.theta2) / 2.0)
            axis.text(1.10 * np.cos(angle), 1.10 * np.sin(angle), str(int(value)), ha="center", va="center", fontsize=8, color="black")
        # Scope labels are useful at the scale of the legacy panels, but only
        # for non-zero sectors so they do not obscure the small outer labels.
        for wedge, category, value in zip(axis.patches[:len(inner)], inner.index, inner):
            if int(value) == 0:
                continue
            angle = np.deg2rad((wedge.theta1 + wedge.theta2) / 2.0)
            axis.text(0.56 * np.cos(angle), 0.56 * np.sin(angle), f"{category}\n{int(value)}", ha="center", va="center", fontsize=8, rotation=np.rad2deg(angle) - 90, color="white" if category == "outside plane" else "black")
        axis.text(0, 0, f"n = {total}", ha="center", va="center", fontsize=10, fontweight="bold")
        axis.set_title(str(panel), fontsize=11)
    handles = [plt.matplotlib.patches.Patch(color=_HCR_FUNCTIONAL_OUTER_COLORS[item], label=item) for item in _HCR_FUNCTIONAL_OUTER_ORDER[:-1]]
    fig.legend(handles=handles, title="Outer ring", loc="lower center", ncol=5, frameon=False, fontsize=8)
    fig.suptitle("Gene-linked anatomy labels are not equally represented on the functional planes", y=0.985, fontsize=14)
    fig.subplots_adjust(top=0.88, bottom=0.12, hspace=0.25, wspace=0.35)
    return fig


def build_hcr_anatomy_match_outcome_table(
    *,
    label_paths: Sequence[str | Path],
    match_paths: Sequence[str | Path],
    activity_status_path: str | Path | None = None,
) -> pd.DataFrame:
    """Partition persisted HCR labels by the legacy anatomical-match outcome.

    This mirrors the legacy single-fish ``hcr_anat_match_overview_by_gene_pie``:
    q95 size flags come from each mask's persisted warp metadata; match quality
    comes from the persisted ``*_matches.csv``; and in-/out-of-plane status is
    read from the existing HCR status table when present.  No matching or plane
    assignment is recomputed here.
    """
    matches_by_stem = {Path(path).name.replace("_matches.csv", ""): Path(path) for path in match_paths}
    plane_lookup = _hcr_status_plane_lookup(activity_status_path)
    rows: list[dict] = []
    for raw_path in label_paths:
        path = Path(raw_path)
        stem = path.name.replace("_labels_uint16.tif", "")
        labels = set(np.unique(imread_any(path)).astype(int)) - {0}
        match_path = matches_by_stem.get(stem)
        matches = pd.read_csv(match_path) if match_path is not None and match_path.is_file() else pd.DataFrame()
        if not matches.empty and "conf_label" in matches:
            matches["conf_label"] = pd.to_numeric(matches["conf_label"], errors="coerce")
            matches = matches.dropna(subset=["conf_label"]).copy()
            matches["conf_label"] = matches["conf_label"].astype(int)
        else:
            matches = pd.DataFrame(columns=["conf_label"])
        by_label = {int(label): group.iloc[0] for label, group in matches.groupby("conf_label", sort=False)}
        meta_path = path.with_name(path.name.replace("_labels_uint16.tif", "_warp_meta.json"))
        large_labels: set[int] = set()
        if meta_path.is_file():
            try:
                large_labels = {int(value) for value in json.loads(meta_path.read_text()).get("filter_stats", {}).get("low_conf_labels", [])}
            except (OSError, ValueError, TypeError):
                pass
        gene, round_name = _gene_round(path)
        gene_key = str(gene).strip().lower()
        raw_mask_name = path.name.replace("_in_2p_labels_uint16.tif", ".tif")
        for label in sorted(labels):
            row = by_label.get(label)
            quality = "" if row is None else str(row.get("quality", "")).strip().lower()
            pair_type = "" if row is None else str(row.get("pair_type", "")).strip().lower()
            within_gate = False if row is None else _as_bool(row.get("within_gate", False))
            if quality == "good":
                represented = plane_lookup.get((raw_mask_name, gene_key, label), plane_lookup.get(("", gene_key, label)))
                outcome = "in-plane anatomy match" if represented else "out-of-plane anatomy match"
            elif label in large_labels:
                outcome = ">q95 large mask"
            elif pair_type in {"1-many", "split", "one-to-many"}:
                outcome = "split anatomy relation"
            elif pair_type in {"many-1", "merged", "many-to-one"}:
                outcome = "merged anatomy relation"
            elif pair_type in {"many-many", "complex", "many-to-many"}:
                outcome = "complex anatomy relation"
            elif quality == "iffy" and within_gate and pair_type in {"1-1", "one-to-one"}:
                outcome = "1-to-1 anatomy relation, IoU below threshold"
            elif row is None or not within_gate or quality == "rejected" or pair_type == "rejected":
                outcome = "too far / no overlap anatomy"
            else:
                outcome = "other anatomy rejection"
            rows.append({"gene": gene, "round": round_name, "category": outcome, "conf_label": int(label)})
    if not rows:
        return pd.DataFrame(columns=["gene", "round", "category", "n_labels"])
    observed = pd.DataFrame(rows)
    grouped = observed.groupby(["gene", "round", "category"], sort=False).size().rename("n_labels").reset_index()
    groups = observed[["gene", "round"]].drop_duplicates()
    full = groups.merge(pd.DataFrame({"category": _HCR_ANATOMY_OUTCOME_ORDER}), how="cross")
    return full.merge(grouped, on=["gene", "round", "category"], how="left").fillna({"n_labels": 0}).astype({"n_labels": int})


def plot_hcr_anatomy_match_outcome_donuts(outcomes: pd.DataFrame, *, fish_id: str) -> plt.Figure:
    """Render the legacy one-ring HCR→anatomy outcome donuts by gene/round."""
    required = {"gene", "round", "category", "n_labels"}
    missing = sorted(required.difference(outcomes.columns))
    if missing:
        raise ValueError(f"HCR anatomy-outcome table lacks columns: {missing}")
    groups = list(outcomes[["gene", "round"]].drop_duplicates().itertuples(index=False, name=None))
    legacy_gene_order = {"sst1.1": 0, "sst1.2": 1, "tac3b": 2, "pth2": 3}
    groups.sort(key=lambda item: (legacy_gene_order.get(str(item[0]).lower(), len(legacy_gene_order)), str(item[0]), str(item[1])))
    if not groups:
        raise ValueError("No persisted HCR matching artifacts were supplied")
    ncols = min(2, len(groups))
    nrows = int(np.ceil(len(groups) / ncols))
    # Keep a dedicated header band: the legacy panel has a long category key
    # and letting constrained_layout negotiate it causes it to collide with
    # the top-row subplot titles.
    fig, axes = plt.subplots(nrows, ncols, figsize=(12.5, 5.8 * nrows), squeeze=False)
    for axis in axes.flat[len(groups):]:
        axis.set_visible(False)
    for axis, (gene, round_name) in zip(axes.flat, groups):
        part = outcomes[(outcomes.gene == gene) & (outcomes["round"] == round_name)].set_index("category").n_labels.reindex(_HCR_ANATOMY_OUTCOME_ORDER, fill_value=0)
        total = int(part.sum())
        nonzero = part[part.gt(0)]
        wedges, _, autotexts = axis.pie(
            nonzero.to_numpy(), startangle=90, counterclock=False,
            colors=[_HCR_ANATOMY_OUTCOME_COLORS[item] for item in nonzero.index],
            autopct=lambda pct: f"{pct:.0f}%" if pct >= 10 else (f"{pct:.1f}%" if pct > 0 else ""),
            wedgeprops={"width": 0.46, "edgecolor": "black", "linewidth": 0.8},
            textprops={"fontsize": 9, "color": "white"},
        )
        for wedge, text, value in zip(wedges, autotexts, nonzero.to_numpy()):
            text.set_path_effects([patheffects.withStroke(linewidth=2.0, foreground="black")])
            pct = 100.0 * float(value) / total if total else 0.0
            if 0 < pct <= 7.5:
                angle = np.deg2rad((wedge.theta1 + wedge.theta2) / 2.0)
                x, y = np.cos(angle), np.sin(angle)
                text.set_visible(False)
                axis.annotate(
                    f"{pct:.1f}%", xy=(0.88 * x, 0.88 * y), xytext=(1.32 * np.sign(x), 1.16 * y),
                    ha="left" if x >= 0 else "right", va="center", fontsize=9, color="white",
                    path_effects=[patheffects.withStroke(linewidth=2.0, foreground="black")],
                    arrowprops={"arrowstyle": "-", "color": "black", "connectionstyle": "angle3"},
                )
        axis.set_title(f"{gene}\n(n={total})", fontsize=12)
    handles = [plt.matplotlib.patches.Patch(color=_HCR_ANATOMY_OUTCOME_COLORS[item], label=item) for item in _HCR_ANATOMY_OUTCOME_ORDER]
    fig.legend(handles=handles, title="Fill = category", loc="upper center", bbox_to_anchor=(0.5, 0.935), ncol=3, frameon=False, fontsize=9)
    fig.suptitle(f"{fish_id}: HCR masks by gene — anatomical match outcome (legacy [50]-aligned)", y=0.995, fontsize=14)
    fig.subplots_adjust(top=0.80, hspace=0.35, wspace=0.18)
    return fig


def build_hcr_matching_flow_donut_table(
    *, label_paths: Sequence[str | Path], review_paths: Sequence[str | Path], final_pair_paths: Sequence[str | Path]
) -> pd.DataFrame:
    """Build mutually exclusive HCR label fates for the legacy-style two-ring flow view.

    The inner ring distinguishes final one-to-one acceptance from all other
    segmented labels.  The outer ring partitions the latter by the first
    persisted terminal matching outcome.  It only reads saved labels, review
    rows, and final pairs; it never recomputes candidates or assignments.
    """
    final_by_stem = {Path(p).name.replace("_final_pairs.csv", ""): Path(p) for p in final_pair_paths}
    review_by_stem = {Path(p).name.replace("_review.csv", ""): Path(p) for p in review_paths}
    rows: list[dict] = []
    for raw_path in label_paths:
        path = Path(raw_path)
        stem = path.name.replace("_labels_uint16.tif", "")
        labels = set(np.asarray(imread_any(path)).ravel().astype(int)) - {0}
        review = pd.read_csv(review_by_stem[stem]) if stem in review_by_stem else pd.DataFrame()
        final = pd.read_csv(final_by_stem[stem]) if stem in final_by_stem else pd.DataFrame()
        review_labels = set(pd.to_numeric(review.get("conf_label", pd.Series(dtype=float)), errors="coerce").dropna().astype(int))
        accepted_rows = final.loc[final["pair_type"].astype(str).eq("1-1")] if "pair_type" in final else final
        accepted = set(pd.to_numeric(accepted_rows.get("conf_label", pd.Series(dtype=float)), errors="coerce").dropna().astype(int)) & labels
        gate = review.get("within_gate", pd.Series(False, index=review.index)).astype(str).str.strip().str.lower().isin(("true", "1", "yes"))
        gated = set(pd.to_numeric(review.loc[gate, "conf_label"], errors="coerce").dropna().astype(int)) if "conf_label" in review else set()
        ambiguous = set(pd.to_numeric(review.loc[review.get("pair_type", pd.Series("", index=review.index)).astype(str).ne("1-1"), "conf_label"], errors="coerce").dropna().astype(int)) if "conf_label" in review else set()
        candidates = (review_labels | accepted) & labels
        terminal = {
            "not candidate": labels - candidates,
            "gate failure": (candidates - gated) - ambiguous - accepted,
            "ambiguous relation": (ambiguous & candidates) - accepted,
        }
        terminal["quality/review rejection"] = labels - accepted - set().union(*terminal.values())
        terminal["accepted 1:1"] = accepted
        gene, round_name = _gene_round(path)
        for ring, category, values in (
            ("inner", "not accepted", labels - accepted),
            ("inner", "accepted 1:1", accepted),
            *(("outer", category, terminal[category]) for category in _HCR_FLOW_OUTER_ORDER),
        ):
            rows.append({"gene": gene, "round": round_name, "ring": ring, "category": category, "n_labels": len(values)})
    return pd.DataFrame(rows)


def plot_hcr_matching_flow_donuts(donuts: pd.DataFrame, *, fish_id: str) -> plt.Figure:
    """Render the legacy-style two-ring persisted HCR matching flow by gene/round."""
    required = {"gene", "round", "ring", "category", "n_labels"}
    missing = sorted(required.difference(donuts.columns))
    if missing:
        raise ValueError(f"HCR matching donut table lacks columns: {missing}")
    groups = list(donuts[["gene", "round"]].drop_duplicates().itertuples(index=False, name=None))
    if not groups:
        raise ValueError("No persisted HCR matching artifacts were supplied")
    fig, axes = plt.subplots(1, len(groups), figsize=(max(7.2, 5.0 * len(groups)), 5.8), squeeze=False, constrained_layout=True)
    for ax, (gene, round_name) in zip(axes.flat, groups):
        part = donuts[(donuts.gene == gene) & (donuts["round"] == round_name)]
        inner = part[part.ring.eq("inner")].set_index("category").n_labels.reindex(["not accepted", "accepted 1:1"], fill_value=0)
        outer = part[part.ring.eq("outer")].set_index("category").n_labels.reindex(_HCR_FLOW_OUTER_ORDER, fill_value=0)
        total = int(inner.sum())
        inner_wedges, _ = ax.pie(inner, radius=0.72, startangle=90, counterclock=False,
                                 colors=["#bdbdbd", _HCR_FLOW_OUTER_COLORS["accepted 1:1"]],
                                 wedgeprops={"width": 0.28, "edgecolor": "black", "linewidth": 0.7})
        outer_wedges, _ = ax.pie(outer, radius=1.0, startangle=90, counterclock=False,
                                 colors=[_HCR_FLOW_OUTER_COLORS[item] for item in outer.index],
                                 wedgeprops={"width": 0.18, "edgecolor": "white", "linewidth": 0.7})
        for wedges, values, radius in ((inner_wedges, inner, 0.58), (outer_wedges, outer, 0.91)):
            for wedge, value in zip(wedges, values):
                if int(value) <= 0:
                    continue
                angle = np.deg2rad((wedge.theta1 + wedge.theta2) / 2.0)
                ax.text(radius * np.cos(angle), radius * np.sin(angle), str(int(value)), ha="center", va="center", fontsize=8)
        ax.text(0, 0, f"{gene}\n{round_name}\nn={total}", ha="center", va="center", fontsize=10, fontweight="bold")
        ax.set_title("inner: final acceptance\nouter: terminal persisted outcome", fontsize=9)
    handles = [plt.matplotlib.patches.Patch(color=_HCR_FLOW_OUTER_COLORS[item], label=item) for item in _HCR_FLOW_OUTER_ORDER]
    fig.legend(handles=handles, loc="lower center", ncol=min(3, len(handles)), frameon=False, fontsize=8)
    fig.suptitle(f"{fish_id}: persisted HCR→anatomy matching flow (two-ring review)", fontweight="bold")
    return fig


def _molecular_geometry_label_pages(geometry: pd.DataFrame, anatomy_labels: np.ndarray, plane_refs_path: str | Path) -> dict[int, int]:
    """Validate geometry-owned Z provenance without inferring an orientation."""
    if anatomy_labels.ndim != 3:
        raise ValueError("Molecular fate review requires a 3D anatomy-label stack")
    refs = json.loads(Path(plane_refs_path).read_text())
    if not isinstance(refs, list) or not all(isinstance(ref, dict) for ref in refs):
        raise ValueError("Geometry plane references must be a JSON list of objects")
    errors = validate_anatomy_label_z_provenance(refs, anatomy_labels.shape[0], geometry_df=geometry, anatomy_labels=anatomy_labels)
    if errors:
        raise ValueError("Invalid geometry anatomy-label Z provenance: " + "; ".join(errors))
    plane_z: dict[int, int] = {}
    for plane_idx, rows in geometry.groupby("plane_idx"):
        plane_idx = int(plane_idx)
        selected_refs = [ref for ref in refs if ref.get("index", ref.get("plane_index")) == plane_idx]
        if len(selected_refs) != 1:
            raise ValueError(f"Geometry plane {plane_idx} requires exactly one label-Z provenance record")
        ref = selected_refs[0]
        if ref.get("anat_label_z_mode") not in {"direct", "reverse"} or ref.get("anat_label_z") in (None, ""):
            raise ValueError(f"Geometry plane {plane_idx} lacks explicit anatomy-label Z provenance")
        best_z = pd.to_numeric(rows["best_z"], errors="coerce")
        if best_z.isna().any() or best_z.nunique() != 1 or not best_z.eq(best_z.astype(int)).all():
            raise ValueError(f"Geometry plane {plane_idx} has ambiguous best-Z values")
        bz = int(best_z.iloc[0])
        if ref.get("best_z") != bz:
            raise ValueError(f"Geometry plane {plane_idx} best-Z disagrees with its provenance")
        mode_ref = {key: value for key, value in ref.items() if key != "anat_label_z"}
        resolved_z = resolve_anatomy_label_z(mode_ref, anatomy_labels.shape[0], best_z=bz)
        if not 0 <= bz < anatomy_labels.shape[0] or ref["anat_label_z"] != resolved_z or not 0 <= resolved_z < anatomy_labels.shape[0]:
            raise ValueError(f"Geometry plane {plane_idx} has inconsistent anatomy-label Z provenance")
        plane_z[plane_idx] = resolved_z
    return plane_z


def build_molecular_identity_fate_table(*, label_paths: Sequence[str | Path], final_pair_paths: Sequence[str | Path], identity_table_path: str | Path, geometry_table_path: str | Path, anatomy_labels_path: str | Path, plane_refs_path: str | Path) -> pd.DataFrame:
    """Classify molecular labels using frozen tables and geometry-owned label-Z pages."""
    identity = pd.read_csv(identity_table_path)
    geometry = pd.read_csv(geometry_table_path)
    anatomy_labels = imread_any(anatomy_labels_path)
    plane_z = _molecular_geometry_label_pages(geometry, anatomy_labels, plane_refs_path)
    final_by_stem = {Path(p).name.replace("_final_pairs.csv", ""): pd.read_csv(p) for p in final_pair_paths}
    domain = ("unmatched", "out-of-plane anatomy label", "in-plane no functional ROI candidate", "in-plane response unavailable", "in-plane low-activity ROI", "in-plane responsive ROI")
    rows: list[dict] = []
    for raw_path in label_paths:
        path = Path(raw_path); stem = path.name.replace("_labels_uint16.tif", "")
        gene, round_name = _gene_round(path)
        pairs = final_by_stem.get(stem, pd.DataFrame(columns=["conf_label", "twoP_label"]))
        mapping = dict(zip(pairs.get("conf_label", []), pairs.get("twoP_label", [])))
        labels = np.unique(imread_any(path)); labels = labels[labels > 0]
        for conf_label in labels.astype(int):
            anat_label = mapping.get(conf_label)
            fate = "unmatched"
            if anat_label is not None:
                in_plane = any(0 <= z < anatomy_labels.shape[0] and np.any(anatomy_labels[z] == anat_label) for z in plane_z.values())
                linked = identity[identity.get("anat_label", pd.Series(index=identity.index)).eq(anat_label)]
                if not in_plane:
                    fate = "out-of-plane anatomy label"
                elif linked.empty:
                    fate = "in-plane no functional ROI candidate"
                elif not linked.get("has_unique_anat_match", pd.Series(False, index=linked.index)).fillna(False).any():
                    fate = "in-plane no functional ROI candidate"
                else:
                    response = linked[linked.get("has_unique_anat_match", pd.Series(False, index=linked.index)).fillna(False)]
                    if not response.get("bpi_data_available", pd.Series(False, index=response.index)).fillna(False).any(): fate = "in-plane response unavailable"
                    elif response.get("response_is_active", pd.Series(False, index=response.index)).fillna(False).any(): fate = "in-plane responsive ROI"
                    else: fate = "in-plane low-activity ROI"
            rows.append({"gene": gene, "round": round_name, "conf_label": conf_label, "anat_label": anat_label, "fate": fate})
    table = pd.DataFrame(rows)
    if table.empty: return pd.DataFrame(columns=["gene", "round", "fate", "n_labels", "proportion"])
    counts = table.groupby(["gene", "round", "fate"], observed=False).size().rename("n_labels").reset_index()
    grid = pd.MultiIndex.from_product([table[["gene", "round"]].drop_duplicates().itertuples(index=False, name=None), domain], names=["pair", "fate"])
    out = counts.set_index([list(zip(counts.gene, counts["round"])), "fate"]).reindex(grid, fill_value=0).reset_index()
    out[["gene", "round"]] = pd.DataFrame(out.pop("pair").tolist(), index=out.index)
    totals = out.groupby(["gene", "round"])["n_labels"].transform("sum")
    out["proportion"] = np.where(totals > 0, out.n_labels / totals, 0.0)
    return out


def plot_molecular_identity_fate(fates: pd.DataFrame, *, fish_id: str) -> plt.Figure:
    """Render the ordered complete HCR-centric fate domain with counts and proportions."""
    if fates.empty: raise ValueError("No molecular labels found for fate review")
    groups = list(fates[["gene", "round"]].drop_duplicates().itertuples(index=False, name=None))
    fig, axes = plt.subplots(1, len(groups), figsize=(5.2 * len(groups), 4.8), squeeze=False, constrained_layout=True)
    for ax, (gene, round_name) in zip(axes.flat, groups):
        part = fates[(fates.gene == gene) & (fates["round"] == round_name)]
        bars = ax.barh(part.fate, part.proportion, color="#4c78a8")
        for bar, row in zip(bars, part.itertuples(index=False)): ax.text(bar.get_width() + .01, bar.get_y()+bar.get_height()/2, f"n={row.n_labels}", va="center", fontsize=8)
        ax.set(xlim=(0, 1.18), xlabel="Proportion of segmented HCR labels", title=f"{gene} ({round_name}); n={int(part.n_labels.sum())}")
        ax.tick_params(axis="y", labelsize=8)
    fig.suptitle(f"{fish_id}: molecular identity fate (HCR-centric denominator)", fontweight="bold")
    return fig


def plot_molecular_correspondence_tiles(
    *,
    anatomy_path: str | Path,
    anatomy_labels_path: str | Path,
    hcr_label_paths: Sequence[str | Path],
    final_pair_paths: Sequence[str | Path],
    identity_table_path: str | Path,
    functional_label_dir: str | Path,
    plane_refs_path: str | Path,
    geometry_table_path: str | Path,
    fish_id: str,
    genes: Sequence[str] | None = None,
    response_statuses: Sequence[str] | None = None,
    flagged_only: bool = False,
    max_tiles: int | None = None,
    crop_px: int = 36,
) -> plt.Figure:
    """Render read-only tiles for persisted HCR/anatomy/functional links.

    Each tile is a frozen anatomy-space crop: green is the accepted HCR-label
    boundary, magenta is the selected functional ROI boundary, and white is
    the linked anatomy-label boundary.  ``genes``, ``response_statuses``, and
    ``flagged_only`` are display filters only.  They never write exclusions or
    modify HCR final-pair membership.  By default all persisted selected links
    are shown; ``max_tiles`` is an explicit bounded inspection subset.
    """
    _validate_fish(fish_id)
    if crop_px < 1:
        raise ValueError("crop_px must be positive")
    if max_tiles is not None and max_tiles < 1:
        raise ValueError("max_tiles must be positive when supplied")
    anatomy = np.asarray(imread_any(anatomy_path))
    anatomy_labels = np.asarray(imread_any(anatomy_labels_path))
    if anatomy.ndim != 3 or anatomy_labels.shape != anatomy.shape:
        raise ValueError("Anatomy intensity and anatomy labels must be same-grid 3D ZYX arrays")
    identity = pd.read_csv(identity_table_path)
    geometry = pd.read_csv(geometry_table_path)
    label_pages = _molecular_geometry_label_pages(geometry, anatomy_labels, plane_refs_path)
    required_identity = {"anat_label", "plane_idx", "func_label", "best_z"}
    missing_identity = sorted(required_identity.difference(identity.columns))
    if missing_identity:
        raise ValueError(f"Identity table is missing required frozen-geometry columns: {missing_identity}")
    frozen_z = geometry.groupby("plane_idx")["best_z"].first().to_dict()
    if not identity["best_z"].eq(identity["plane_idx"].map(frozen_z)).all():
        raise ValueError("Identity best-Z values disagree with frozen geometry")
    finals = {Path(p).name.replace("_final_pairs.csv", ""): pd.read_csv(p) for p in final_pair_paths}
    functional_dir = Path(functional_label_dir)
    if not functional_dir.is_dir():
        raise FileNotFoundError(f"Functional anatomy-label directory does not exist: {functional_dir}")

    def _truthy(value: object) -> bool:
        if value is None or (isinstance(value, float) and np.isnan(value)):
            return False
        if isinstance(value, (bool, np.bool_)):
            return bool(value)
        if isinstance(value, (int, np.integer, float, np.floating)):
            return bool(value)
        return str(value).strip().lower() in {"1", "true", "yes", "y", "flagged", "fail", "failed", "iffy"}

    def _flags(row: pd.Series) -> tuple[str, ...]:
        # Flag fields are optional provenance.  Absence is deliberately shown
        # as "none recorded", not interpreted as proof of high quality.
        names = [
            name for name in row.index
            if ("flag" in name.lower() or "segmentation" in name.lower() or "geometry" in name.lower())
            and name.lower() not in {"geometry_state", "segmentation_source"}
        ]
        return tuple(name for name in names if _truthy(row[name]))

    wanted_genes = None if genes is None else {str(gene) for gene in genes}
    wanted_statuses = None if response_statuses is None else {str(status) for status in response_statuses}
    functional_masks: dict[int, np.ndarray] = {}

    def _functional_mask(plane_idx: int) -> np.ndarray:
        if plane_idx not in functional_masks:
            candidates = sorted(
                path for path in functional_dir.glob(f"*plane{plane_idx}*func_mask_in_2p.tif")
                if path.is_file() and not path.name.startswith("._")
            )
            path = candidates[0] if candidates else None
            if path is None:
                raise FileNotFoundError(f"No persisted functional anatomy mask for plane {plane_idx} in {functional_dir}")
            mask = np.asarray(imread_any(path))
            if mask.ndim not in {2, 3}:
                raise ValueError(f"Functional mask must be 2D or 3D, got {mask.shape}: {path}")
            if mask.ndim == 2 and mask.shape != anatomy.shape[1:]:
                raise ValueError(f"Functional mask grid differs from anatomy XY grid: {path}")
            if mask.ndim == 3 and mask.shape != anatomy.shape:
                raise ValueError(f"Functional mask grid differs from anatomy grid: {path}")
            functional_masks[plane_idx] = mask
        return functional_masks[plane_idx]

    records: list[dict] = []
    for hcr_path in hcr_label_paths:
        path = Path(hcr_path); stem = path.name.replace("_labels_uint16.tif", "")
        hcr = np.asarray(imread_any(path)); gene, round_name = _gene_round(path)
        if hcr.shape != anatomy.shape:
            raise ValueError(f"HCR label grid differs from anatomy grid: {path}")
        for pair in finals.get(stem, pd.DataFrame()).itertuples(index=False):
            pair_row = pd.Series(pair._asdict())
            if "twoP_label" not in pair_row or "conf_label" not in pair_row:
                raise ValueError(f"Final-pair artifact needs conf_label and twoP_label: {stem}")
            anat_label = int(pair_row.twoP_label); conf_label = int(pair_row.conf_label)
            linked = identity[identity["anat_label"].eq(anat_label)].copy()
            if "has_unique_anat_match" in linked:
                linked = linked[linked["has_unique_anat_match"].fillna(False)]
            # The identity table can validly contain more than one selected
            # functional correspondence for an accepted HCR/anatomy label.
            # Keep each persisted row rather than choosing one in plot code.
            for _, row in linked.iterrows():
                response_value = row.get("response_class", row.get("response_summary_class", "response unavailable"))
                response = "response unavailable" if pd.isna(response_value) else str(response_value)
                if "bpi_data_available" in row.index:
                    trace_status = "trace/BPI available" if _truthy(row["bpi_data_available"]) else "trace/BPI unavailable"
                elif "bpi_status" in row.index:
                    trace_status = f"BPI status: {row['bpi_status']}"
                else:
                    trace_status = "trace/BPI status not recorded"
                flags = tuple(sorted(set(_flags(pair_row)).union(_flags(row))))
                records.append({
                    "gene": gene, "round": round_name, "hcr": hcr,
                    "conf_label": conf_label, "anat_label": anat_label,
                    "plane_idx": int(row.plane_idx), "func_label": int(row.func_label),
                    "best_z": int(row.best_z), "response": response,
                    "trace_status": trace_status, "flags": flags,
                })
    records.sort(key=lambda item: (item["gene"], item["round"], item["response"], item["plane_idx"], item["func_label"], item["conf_label"]))
    total_selected_links = len(records)
    if wanted_genes is not None:
        records = [record for record in records if record["gene"] in wanted_genes]
    if wanted_statuses is not None:
        records = [record for record in records if record["response"] in wanted_statuses]
    if flagged_only:
        records = [record for record in records if record["flags"]]
    if max_tiles is not None:
        records = records[:max_tiles]
    if not records: raise ValueError("No accepted molecular-to-functional correspondences available for tiles")
    cols = min(4, len(records)); rows = int(np.ceil(len(records) / cols))
    fig, axes = plt.subplots(rows, cols, figsize=(4 * cols, 4 * rows), squeeze=False, constrained_layout=True)
    for ax, record in zip(axes.flat, records):
        z = record["best_z"]
        label_z = label_pages[record["plane_idx"]]
        if not np.any(anatomy_labels[label_z] == record["anat_label"]):
            raise ValueError(f"Selected anatomy label {record['anat_label']} is absent from geometry-owned label page {label_z}")
        func = _functional_mask(record["plane_idx"])
        func_plane = func[z] if func.ndim == 3 else func
        anatomy_plane = norm01(anatomy[z]); ay, axx = np.where(anatomy_labels[label_z] == record["anat_label"])
        if len(ay) == 0: continue
        cy, cx = int(np.median(ay)), int(np.median(axx)); y0, y1 = max(0, cy-crop_px), min(anatomy_plane.shape[0], cy+crop_px); x0, x1 = max(0, cx-crop_px), min(anatomy_plane.shape[1], cx+crop_px)
        rgb = np.dstack([anatomy_plane]*3)[y0:y1, x0:x1].copy()
        ax.imshow(rgb)
        anatomy_boundary = anatomy_labels[label_z, y0:y1, x0:x1] == record["anat_label"]
        # Draw anatomy first.  The coloured, narrower top contours remain
        # visible even when all three labels coincide exactly; a white flank
        # still identifies the anatomy boundary on bright GCaMP context.
        ax.contour(anatomy_boundary, levels=[.5], colors="#202020", linewidths=3.2)
        ax.contour(anatomy_boundary, levels=[.5], colors="white", linewidths=2.1)
        ax.contour(record["hcr"][label_z, y0:y1, x0:x1] == record["conf_label"], levels=[.5], colors="#00c853", linewidths=1.45, linestyles="--")
        ax.contour(func_plane[y0:y1, x0:x1] == record["func_label"], levels=[.5], colors="#ff00ff", linewidths=.75)
        flag_text = ", ".join(record["flags"]) if record["flags"] else "none recorded"
        ax.set_title(f"{record['gene']} {record['round']} | H{record['conf_label']}→A{record['anat_label']}→R{record['func_label']}\n{record['response']} · {record['trace_status']} | flags: {flag_text}", fontsize=7); ax.axis("off")
    for ax in axes.flat[len(records):]: ax.axis("off")
    fig.suptitle(
        f"{fish_id}: persisted HCR→anatomy→functional correspondence tiles\n"
        f"HCR-centric review: showing {len(records)} of {total_selected_links} persisted selected links\n"
        "green HCR boundary · magenta ROI boundary · white anatomy-label boundary",
        fontweight="bold", fontsize=11,
    )
    return fig
