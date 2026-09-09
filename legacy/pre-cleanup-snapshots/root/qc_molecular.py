"""Read-only review surfaces for molecular QC notebooks 04, 05, and 06.

The functions in this module inspect persisted artifacts only.  They never run a
registration, segmentation, matching, scoring, export, or promotion stage.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
from typing import Mapping, Sequence

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import tifffile

from ..matching import compute_centroids, gene_from_mask, idx_to_um
from ..spatial import imread_any, infer_voxels_tiff, load_or_cache_voxels, norm01

__all__ = [
    "MOLECULAR_GEOMETRY_TRACKS",
    "MolecularLabelViewerData",
    "inspect_molecular_geometry_qc",
    "plot_molecular_geometry_qc",
    "load_molecular_label_viewer",
    "show_molecular_label_viewer",
    "build_hcr_anatomy_centroid_offset_table",
    "plot_hcr_anatomy_centroid_offsets",
    "inspect_molecular_identity_qc",
    "plot_molecular_identity_qc",
    "inspect_activity_export_qc",
    "plot_activity_export_qc",
    "build_hcr_matching_flow_table",
    "plot_hcr_matching_flow",
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


def _tiff_voxel_zyx_um(path: Path) -> dict[str, float]:
    vox = infer_voxels_tiff(path)
    dz = float(vox.get("Z", 1.0))
    with tifffile.TiffFile(path) as tiff:
        metadata = tiff.imagej_metadata or {}
    try:
        imagej_spacing = float(metadata.get("spacing", dz))
        if np.isfinite(imagej_spacing) and imagej_spacing > 0:
            dz = imagej_spacing
    except (TypeError, ValueError):
        pass
    return {"dz": dz, "dy": float(vox.get("Y", 1.0)), "dx": float(vox.get("X", 1.0))}


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
    colors = {name: np.asarray(to_rgb(_VIEWER_COLORS[index % len(_VIEWER_COLORS)]), dtype=np.float32) for index, name in enumerate(names)}

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
        status.value = "<small>Loaded once; plane and checkbox changes perform no disk I/O or matching.</small>"
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
        anatomy_col = next((c for c in ("anat_label", "anatomy_label") if c in pairs), None)
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
    for label in np.unique(labels):
        if int(label) <= 0:
            continue
        zyx = np.argwhere(labels == label)
        lower = zyx.min(axis=0)
        upper = zyx.max(axis=0)
        # Inclusive voxel extents are physical object dimensions, including a
        # single-pixel ROI's non-zero physical width.  ``xy_um`` is the
        # geometric mean of X/Y extents, an area-equivalent linear XY size.
        z_px, y_px, x_px = (upper - lower + 1).astype(int)
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
                "xy_um": float(np.sqrt(x_um * y_um)),
            }
        )
    return rows


def build_cross_modality_mask_size_table(
    *,
    anatomy_labels_path: str | Path,
    functional_label_paths: Sequence[str | Path],
    hcr_label_paths: Sequence[str | Path],
    reference_anatomy_path: str | Path,
) -> pd.DataFrame:
    """Measure persisted anatomy, functional, and HCR label-mask bounding boxes.

    All selected masks must already be on the reference anatomy grid.  Physical
    spacing is read from ``reference_anatomy_path`` (normally the prepared
    anatomy NRRD), because transformed TIFF masks do not reliably preserve XY
    resolution metadata.  Q05 and Q95 are calculated independently for each
    modality from the area-equivalent linear ``xy_um`` extent: Q05 rows are
    marked as hard drops and Q95 rows are retained but visibly flagged.
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
    return table


def _plot_size_distribution(ax: plt.Axes, table: pd.DataFrame, value_col: str, title: str, ylabel: str) -> None:
    modalities = [name for name in ("anatomy", "functional", "hcr") if name in set(table["modality"])]
    data = [table.loc[(table["modality"] == name) & table["included_in_distribution"], value_col].to_numpy(dtype=float) for name in modalities]
    ax.boxplot(data, labels=modalities, showfliers=False, patch_artist=True, boxprops={"facecolor": "#9ecae1"})
    for idx, modality in enumerate(modalities, start=1):
        flagged = table.loc[(table["modality"] == modality) & table["included_in_distribution"] & table["is_xy_q95_flag"], value_col]
        if not flagged.empty:
            ax.scatter(np.full(len(flagged), idx), flagged, color="#D55E00", marker="^", s=20, zorder=3, label="Q95 XY flag" if idx == 1 else None)
        kept = int(((table["modality"] == modality) & table["included_in_distribution"]).sum())
        total = int((table["modality"] == modality).sum())
        ax.text(idx, 0.01, f"n={kept}/{total}", ha="center", va="bottom", transform=ax.get_xaxis_transform(), fontsize=8)
    ax.set_title(title)
    ax.set_ylabel(ylabel)
    ax.grid(axis="y", alpha=0.22)
    if ax.get_legend_handles_labels()[0]:
        ax.legend(fontsize=8)


def plot_cross_modality_mask_sizes(size_table: pd.DataFrame, *, fish_id: str) -> plt.Figure:
    """Plot XY/Z size distributions plus HCR gene/round splits from QC-only data."""
    required = {"modality", "xy_um", "z_um", "included_in_distribution", "is_xy_q95_flag", "round", "gene"}
    missing = sorted(required - set(size_table.columns))
    if missing:
        raise ValueError(f"Size table lacks required columns: {missing}")
    _validate_fish(fish_id)
    fig, axes = plt.subplots(2, 2, figsize=(12, 8.5), constrained_layout=True)
    _plot_size_distribution(axes[0, 0], size_table, "xy_um", "All masks: XY bounding-box size", "XY equivalent size (µm)")
    _plot_size_distribution(axes[0, 1], size_table, "z_um", "All masks: Z bounding-box size", "Z extent (µm)")
    hcr = size_table.loc[(size_table["modality"] == "hcr") & size_table["included_in_distribution"]].copy()
    hcr["hcr_group"] = hcr[["round", "gene"]].fillna("unknown").astype(str).agg(" · ".join, axis=1)
    groups = sorted(hcr["hcr_group"].unique())
    for ax, value_col, title, ylabel in (
        (axes[1, 0], "xy_um", "HCR masks: XY by gene / round", "XY equivalent size (µm)"),
        (axes[1, 1], "z_um", "HCR masks: Z by gene / round", "Z extent (µm)"),
    ):
        if not groups:
            ax.text(0.5, 0.5, "No retained HCR masks", ha="center", va="center", transform=ax.transAxes)
            ax.set_axis_off()
            continue
        values = [hcr.loc[hcr["hcr_group"] == group, value_col].to_numpy(dtype=float) for group in groups]
        ax.boxplot(values, labels=groups, showfliers=False, patch_artist=True, boxprops={"facecolor": "#c7e9c0"})
        for idx, group in enumerate(groups, start=1):
            flagged = hcr.loc[(hcr["hcr_group"] == group) & hcr["is_xy_q95_flag"], value_col]
            if not flagged.empty:
                ax.scatter(np.full(len(flagged), idx), flagged, color="#D55E00", marker="^", s=20, zorder=3)
        ax.set_title(title)
        ax.set_ylabel(ylabel)
        ax.tick_params(axis="x", rotation=25)
        ax.grid(axis="y", alpha=0.22)
    fig.suptitle(f"{fish_id} cross-modality mask bounding boxes · Q05 XY drops excluded; Q95 XY flags shown")
    return fig


def build_hcr_anatomy_centroid_offset_table(
    *,
    anatomy_labels_path: str | Path,
    hcr_label_paths: Sequence[str | Path],
    final_pair_paths: Sequence[str | Path],
) -> pd.DataFrame:
    """Return signed HCR-minus-anatomy centroid offsets for accepted HCR pairs.

    All masks must already share the in-vivo anatomy grid.  The function reads
    only persisted masks and ``*_final_pairs.csv`` tables; it never proposes or
    changes a molecular/anatomy pairing.  Offsets are in microns when TIFF
    voxel metadata is available, otherwise in the shared image-pixel scale.
    """

    anatomy_path = _path(anatomy_labels_path)
    if anatomy_path is None or not anatomy_path.is_file():
        raise FileNotFoundError(f"Anatomy label stack does not exist: {anatomy_labels_path}")
    if len(hcr_label_paths) != len(final_pair_paths):
        raise ValueError("Provide one final-pair table for each selected HCR label mask.")

    anatomy = np.asarray(imread_any(anatomy_path))
    if anatomy.ndim != 3:
        raise ValueError(f"Anatomy label stack must be 3D ZYX, got {anatomy.shape}: {anatomy_path}")
    anatomy_centroids = compute_centroids(anatomy)
    vox_zyx = _tiff_voxel_zyx_um(anatomy_path)
    xy_pixel_area_um2 = float(vox_zyx["dy"] * vox_zyx["dx"])
    largest_xy_area_by_label: dict[int, int] = {}
    for z_slice in anatomy:
        labels, areas = np.unique(z_slice[z_slice > 0], return_counts=True)
        for label_id, area_px in zip(labels.astype(int), areas.astype(int)):
            largest_xy_area_by_label[label_id] = max(largest_xy_area_by_label.get(label_id, 0), int(area_px))
    anatomy_xy_radii_um = [
        float(np.sqrt((area_px * xy_pixel_area_um2) / np.pi))
        for area_px in largest_xy_area_by_label.values()
        if area_px
    ]
    median_anatomy_xy_radius_um = float(np.median(anatomy_xy_radii_um)) if anatomy_xy_radii_um else np.nan
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
        merged = pairs.loc[:, ["conf_label", "twoP_label"]].drop_duplicates().merge(
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
                        "median_anatomy_xy_radius_um": median_anatomy_xy_radius_um,
                        "mask_path": str(mask_path),
                        "final_pairs_path": str(pairs_path),
                    }
                )
            )
    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame(
        columns=["gene", "axis", "offset_um", "conf_label", "anat_label", "median_anatomy_xy_radius_um", "mask_path", "final_pairs_path"]
    )


def plot_hcr_anatomy_centroid_offsets(offset_table: pd.DataFrame) -> plt.Figure:
    """Plot accepted-pair lateral XY distance and signed axial Z displacement by gene."""

    required = {"gene", "axis", "offset_um"}
    if not required.issubset(offset_table.columns):
        raise ValueError(f"Offset table lacks required columns {sorted(required)}")
    key_columns = [column for column in ("gene", "conf_label", "anat_label", "mask_path", "final_pairs_path") if column in offset_table]
    wide = offset_table.pivot_table(index=key_columns, columns="axis", values="offset_um", aggfunc="first").reset_index()
    if not {"x", "y", "z"}.issubset(wide.columns):
        raise ValueError("Offset table must contain X, Y, and Z values for every accepted pair.")
    wide["xy"] = np.sqrt(wide["x"] ** 2 + wide["y"] ** 2)
    genes = sorted(wide["gene"].dropna().astype(str).unique())
    figure_specs = (("xy", "XY offset", "Lateral centroid distance (µm)", False), ("z", "Z offset", "Signed axial centroid offset (µm)", True))
    fig, axes = plt.subplots(1, 2, figsize=(9, 4.8))
    rng = np.random.default_rng(0)
    for ax, (axis, title, ylabel, signed) in zip(axes, figure_specs):
        subsets = [pd.to_numeric(wide.loc[wide["gene"].astype(str) == gene, axis], errors="coerce").dropna().to_numpy(dtype=float) for gene in genes]
        nonempty = [(gene, values) for gene, values in zip(genes, subsets) if values.size]
        if not nonempty:
            ax.text(0.5, 0.5, "No accepted pairs", ha="center", va="center", transform=ax.transAxes)
            ax.set_axis_off()
            continue
        positions = np.arange(1, len(nonempty) + 1)
        violin = ax.violinplot([values for _, values in nonempty], positions=positions, showextrema=False, showmedians=True)
        for body in violin["bodies"]:
            body.set_facecolor("#756bb1")
            body.set_edgecolor("#4a1486")
            body.set_alpha(0.72)
        for position, (_, values) in zip(positions, nonempty):
            ax.scatter(
                position + rng.uniform(-0.09, 0.09, size=len(values)),
                values,
                color="#4a1486",
                s=9,
                alpha=0.28,
                linewidths=0,
                zorder=3,
            )
        ax.axhline(0.0, color="#333333", linewidth=1, linestyle="--", zorder=0)
        ax.set_xticks(positions, [f"{gene}\n(n={len(values)})" for gene, values in nonempty], rotation=0)
        ax.set_title(title)
        ax.set_xlabel("HCR gene")
        ax.set_ylabel(ylabel)
        ax.grid(axis="y", alpha=0.22)
        if not signed:
            ax.set_ylim(bottom=0.0)
            radii = pd.to_numeric(offset_table.get("median_anatomy_xy_radius_um"), errors="coerce").dropna()
            if not radii.empty:
                radius = float(radii.median())
                ax.axhline(radius, color="#D55E00", linestyle="--", linewidth=1.5, zorder=1, label=f"Median anatomy-mask radius ({radius:.2f} µm)")
                ax.legend(loc="upper right", fontsize=8)
    fig.suptitle("Accepted HCR/anatomy pairs: lateral XY distance and axial Z displacement")
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
    expected_figure_paths: Sequence[str | Path] = (),
) -> dict:
    """Review response/BPI provenance and canonical ROI/HCR export scopes."""
    _validate_fish(fish_id)
    root = Path(pipeline_root).expanduser().resolve()
    roots = _review_roots(root, fish_dir)
    issues: list[dict] = []
    tables: dict[str, pd.DataFrame | None] = {}
    for name, path in (
        ("roi_master", roi_master_path),
        ("bpi_cells", bpi_cells_path),
        ("hcr_status", hcr_status_path),
        ("hcr_pairs", hcr_pairs_path),
        ("trace_meta", trace_meta_path),
    ):
        table_path, tables[name] = _load_csv(path, area=name, issues=issues)
        _check_root(table_path, roots, area=name, issues=issues)
        _fish_check(tables[name], fish_id, area=name, issues=issues)

    master = tables["roi_master"]
    bpi = tables["bpi_cells"]
    hcr_status = tables["hcr_status"]
    hcr_pairs = tables["hcr_pairs"]
    _require_columns(master, (*_ROI_KEYS, "response_is_active", "response_class", "bpi", "bpi_category"), area="roi_master", issues=issues)
    _require_columns(bpi, (*_ROI_KEYS, "response_is_active", "bpi", "bpi_category"), area="bpi_cells", issues=issues)
    _require_columns(
        hcr_status,
        ("fish_id", "gene", "anat_label", "functional_status", "represented_on_func_plane", "selection_rule", "match_policy_version"),
        area="hcr_status",
        issues=issues,
    )
    _require_columns(
        hcr_pairs,
        ("fish_id", "gene", "conf_label", "anat_label", "func_label", "plane", "response_is_active", "response_class", "is_selected_for_analysis", "match_policy_version"),
        area="hcr_pairs",
        issues=issues,
    )
    trace_meta = tables["trace_meta"]
    _require_columns(trace_meta, _ROI_KEYS, area="trace_meta", issues=issues)

    for name, table in (("roi_master", master), ("bpi_cells", bpi)):
        if table is None:
            continue
        if all(c in table for c in _ROI_KEYS) and table.duplicated(list(_ROI_KEYS)).any():
            _issue(issues, name, "blocked", "Canonical ROI key (plane_idx, func_label) is not unique.")
        session_col = next((c for c in ("session_id", "session", "functional_session", "response_session") if c in table), None)
        if session_col is None:
            _issue(issues, name, "blocked", "Session-aware response provenance column is missing.")

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


def build_activity_bpi_gate_table(bpi_cells_path: str | Path) -> tuple[pd.DataFrame, dict[str, float]]:
    """Load finite, response-aware ROI rows and their persisted BPI cut-offs."""
    path = Path(bpi_cells_path)
    if not path.is_file():
        raise FileNotFoundError(f"Missing response/BPI cells table: {path}")
    table = pd.read_csv(path)
    required = {
        "mean_bout_auc_dff", "mean_cont_auc_dff", "activity_mag", "bpi",
        "response_is_active", "bpi_category", "bpi_zero_band", "bpi_activity_threshold",
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
    for column in ("mean_bout_auc_dff", "mean_cont_auc_dff", "activity_mag", "bpi"):
        work[column] = pd.to_numeric(work[column], errors="coerce")
    work["bpi_category"] = work["bpi_category"].fillna("response unavailable").astype(str).str.strip().str.lower()
    work["response_is_active"] = work["response_is_active"].fillna(False).astype(bool)
    work["abs_bpi"] = work["bpi"].abs()
    work = work.loc[
        np.isfinite(work["mean_bout_auc_dff"])
        & np.isfinite(work["mean_cont_auc_dff"])
        & np.isfinite(work["activity_mag"])
        & np.isfinite(work["bpi"])
    ].copy()
    if work.empty:
        raise ValueError(f"No finite response/BPI rows available for review: {path}")
    return work, thresholds


def plot_activity_bpi_gate(bpi_cells_path: str | Path, *, fish_id: str) -> tuple[plt.Figure, pd.DataFrame]:
    """Render the read-only Q7 response/BPI gate from persisted score outputs.

    The table is ROI-centric.  The laterality-dependent global AUC panel remains
    intentionally out of scope until the midline review has been accepted.
    """
    work, thresholds = build_activity_bpi_gate_table(bpi_cells_path)
    all_rows = pd.read_csv(bpi_cells_path)
    all_rows["bpi_category"] = all_rows["bpi_category"].fillna("response unavailable").astype(str).str.strip().str.lower()
    zero_band = thresholds["bpi_zero_band"]
    activity_threshold = thresholds["bpi_activity_threshold"]
    fig, axes = plt.subplots(2, 3, figsize=(15.0, 8.8), constrained_layout=True)
    ax_plane, ax_bpi, ax_bins, ax_dist, ax_strength, ax_donut = axes.flat
    all_counts = all_rows["bpi_category"].value_counts()
    categories = [item for item in _BPI_CATEGORY_ORDER if item in set(all_counts.index)]
    categories.extend(sorted(set(all_counts.index) - set(categories)))
    responsive = work[work["response_is_active"]].copy()
    nonresponsive = work[~work["response_is_active"]].copy()
    extreme_nonresponsive = nonresponsive[nonresponsive["abs_bpi"] > zero_band]
    for category in categories:
        subset = work[work["bpi_category"] == category]
        color = _BPI_CATEGORY_COLORS.get(category, "#666666")
        label = f"{category} (n={int(all_counts[category])})"
        ax_plane.scatter(subset["mean_cont_auc_dff"], subset["mean_bout_auc_dff"], s=10, alpha=0.55, color=color, label=label)
        responsive_subset = subset[subset["response_is_active"]]
        ax_bpi.scatter(responsive_subset["activity_mag"], responsive_subset["bpi"], s=16, alpha=0.7, color=color)
        ax_strength.scatter(0.5 * (responsive_subset["mean_bout_auc_dff"] + responsive_subset["mean_cont_auc_dff"]), responsive_subset["bpi"], s=16, alpha=0.7, color=color)
    for axis, x_values in (
        (ax_bpi, nonresponsive["activity_mag"]),
        (ax_strength, 0.5 * (nonresponsive["mean_bout_auc_dff"] + nonresponsive["mean_cont_auc_dff"])),
    ):
        axis.scatter(x_values, nonresponsive["bpi"], s=9, marker="x", linewidths=0.45, alpha=0.22, color="#7f7f7f")
    limit = float(np.nanpercentile(np.abs(work[["mean_bout_auc_dff", "mean_cont_auc_dff"]].to_numpy()), 99))
    limit = max(limit, activity_threshold)
    ax_plane.plot([0, limit], [0, limit], "--", color="#444444", linewidth=0.9)
    ax_plane.set(xlim=(0, limit), ylim=(0, limit), xlabel="Continuous response AUC (dF/F·s)", ylabel="Bout response AUC (dF/F·s)", title="[56g] Bout versus continuous response")
    ax_plane.legend(fontsize=7, frameon=False, loc="upper left")
    for axis, x_label in ((ax_bpi, "Activity magnitude"), (ax_strength, "Mean bout/continuous AUC (dF/F·s)")):
        axis.axhline(0, color="#444444", linewidth=0.8)
        axis.axhline(zero_band, color="#444444", linestyle=":", linewidth=1.0)
        axis.axhline(-zero_band, color="#444444", linestyle=":", linewidth=1.0)
        axis.axvline(activity_threshold, color="#444444", linestyle="-.", linewidth=1.0)
        axis.set(xlabel=x_label, ylabel="BPI", ylim=(-1.02, 1.02))
    gate_text = (
        f"Directional call = AUC/null pass and |BPI| > {zero_band:g}\n"
        f"grey x: no response pass (n={len(nonresponsive)}; extreme |BPI| n={len(extreme_nonresponsive)})"
    )
    ax_bpi.set_title("[56g] BPI versus activity magnitude\n" + gate_text, fontsize=10)
    ax_strength.set_title("[50l] Population response strength versus BPI\n" + gate_text, fontsize=10)
    n_bins = min(6, int(work["activity_mag"].nunique()))
    if n_bins >= 2:
        binned = work.assign(_bin=pd.qcut(work["activity_mag"], q=n_bins, duplicates="drop")).groupby("_bin", observed=False)["abs_bpi"].agg(["median", "count"]).reset_index()
        mids = [0.5 * (item.left + item.right) for item in binned["_bin"]]
        ax_bins.plot(mids, binned["median"], marker="o", color="#1f77b4")
        for x, row in zip(mids, binned.itertuples(index=False)):
            ax_bins.annotate(f"n={row.count}", (x, row.median), xytext=(2, 4), textcoords="offset points", fontsize=7)
    ax_bins.set(xlabel="Activity magnitude (bin midpoint)", ylabel="Median |BPI|", ylim=(0, 1.02), title="[56g] Absolute BPI across activity bins")
    distribution_categories = [item for item in categories if (work["bpi_category"] == item).any()]
    distribution_data = [work.loc[work["bpi_category"] == category, "activity_mag"].to_numpy() for category in distribution_categories]
    ax_dist.boxplot(distribution_data, labels=[f"{category}\n(n={len(values)})" for category, values in zip(distribution_categories, distribution_data)], showfliers=False)
    ax_dist.axhline(activity_threshold, color="#444444", linestyle="-.", linewidth=1.0)
    ax_dist.set(ylabel="Activity magnitude", title="[56g] Activity by response/BPI class")
    ax_dist.tick_params(axis="x", labelrotation=20, labelsize=7)
    counts = all_counts.reindex(categories, fill_value=0)
    ax_donut.pie(counts.to_numpy(), colors=[_BPI_CATEGORY_COLORS.get(item, "#666666") for item in counts.index], startangle=90, wedgeprops={"width": 0.42, "edgecolor": "white"})
    ax_donut.text(0, 0, f"n={len(all_rows)}\nall ROIs", ha="center", va="center", fontsize=10)
    ax_donut.legend([f"{item} (n={int(count)})" for item, count in counts.items()], fontsize=7, loc="center left", bbox_to_anchor=(1.0, 0.5), frameon=False)
    ax_donut.set_title("[50l] Response/BPI population composition")
    fig.suptitle(f"{fish_id}: activity and BPI review (ROI-centric; persisted cut-offs)", fontsize=14, fontweight="bold")
    return fig, work


def _gene_round(path: str | Path) -> tuple[str, str]:
    """Return stable display keys from a persisted HCR artifact name."""
    name = Path(path).name
    match = re.search(r"_(rbest|r\d+)_channel\d+_(.+?)_cp_masks", name, flags=re.IGNORECASE)
    if not match:
        return gene_from_mask(path), "unknown round"
    return match.group(2).replace("_", "."), match.group(1)


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
    fig, axes = plt.subplots(1, len(groups), figsize=(4.2 * len(groups), 4.2), squeeze=False, constrained_layout=True)
    order = ["segmented", "candidate", "within gate", "ambiguous", "quality good", "rejected/iffy review", "accepted 1:1"]
    for ax, (gene, round_name) in zip(axes.flat, groups):
        part = flow[(flow.gene == gene) & (flow["round"] == round_name)].set_index("stage").n_labels.reindex(order, fill_value=0)
        ax.bar(range(len(order)), part, color=["#7f7f7f", "#4c78a8", "#72b7b2", "#eeca3b", "#59a14f", "#00A651"])
        ax.set_xticks(range(len(order)), order, rotation=45, ha="right", fontsize=8)
        ax.set_ylabel("HCR labels (n)"); ax.set_title(f"{gene} ({round_name})")
        for x, value in enumerate(part): ax.text(x, value, str(int(value)), ha="center", va="bottom", fontsize=8)
    fig.suptitle(f"{fish_id}: persisted HCR→anatomy matching flow", fontweight="bold")
    return fig


def build_molecular_identity_fate_table(*, label_paths: Sequence[str | Path], final_pair_paths: Sequence[str | Path], identity_table_path: str | Path, geometry_table_path: str | Path, anatomy_labels_path: str | Path) -> pd.DataFrame:
    """Classify each segmented molecular label using frozen pair and activity-identity tables."""
    identity = pd.read_csv(identity_table_path)
    geometry = pd.read_csv(geometry_table_path)
    anatomy_labels = imread_any(anatomy_labels_path)
    plane_z = geometry.groupby("plane_idx")["best_z"].first().dropna().astype(int).to_dict()
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
    *, anatomy_path: str | Path, anatomy_labels_path: str | Path, hcr_label_paths: Sequence[str | Path], final_pair_paths: Sequence[str | Path], identity_table_path: str | Path, functional_label_dir: str | Path, fish_id: str, max_tiles: int = 24, crop_px: int = 36
) -> plt.Figure:
    """Read-only tiles for accepted HCR/anatomy/functional correspondences.

    Each tile is a frozen anatomy-space crop.  Green marks the accepted HCR
    label, magenta the matched functional ROI, and white the anatomy label.
    ``max_tiles`` is an explicit inspection subset, never a selection rule.
    """
    anatomy = imread_any(anatomy_path); anatomy_labels = imread_any(anatomy_labels_path)
    identity = pd.read_csv(identity_table_path)
    finals = {Path(p).name.replace("_final_pairs.csv", ""): pd.read_csv(p) for p in final_pair_paths}
    functional_dir = Path(functional_label_dir)
    records: list[dict] = []
    for hcr_path in hcr_label_paths:
        path = Path(hcr_path); stem = path.name.replace("_labels_uint16.tif", "")
        hcr = imread_any(path); gene, round_name = _gene_round(path)
        for pair in finals.get(stem, pd.DataFrame()).itertuples(index=False):
            anat_label = int(pair.twoP_label); conf_label = int(pair.conf_label)
            linked = identity[identity.get("anat_label", pd.Series(index=identity.index)).eq(anat_label)]
            linked = linked[linked.get("has_unique_anat_match", pd.Series(False, index=linked.index)).fillna(False)]
            if linked.empty: continue
            row = linked.iloc[0]
            records.append({"gene": gene, "round": round_name, "hcr": hcr, "conf_label": conf_label, "anat_label": anat_label, "plane_idx": int(row.plane_idx), "func_label": int(row.func_label), "best_z": int(row.best_z), "response": str(row.get("response_class", "unavailable"))})
    records = records[:max_tiles]
    if not records: raise ValueError("No accepted molecular-to-functional correspondences available for tiles")
    cols = min(4, len(records)); rows = int(np.ceil(len(records) / cols))
    fig, axes = plt.subplots(rows, cols, figsize=(4 * cols, 4 * rows), squeeze=False, constrained_layout=True)
    for ax, record in zip(axes.flat, records):
        z = record["best_z"]
        if not np.any(anatomy_labels[z] == record["anat_label"]):
            anatomy_counts = np.count_nonzero(anatomy_labels == record["anat_label"], axis=(1, 2))
            hcr_counts = np.count_nonzero(record["hcr"] == record["conf_label"], axis=(1, 2))
            z = int(np.argmax(anatomy_counts if anatomy_counts.max() else hcr_counts))
        func_path = next(functional_dir.glob(f"*plane{record['plane_idx']}*func_mask_in_2p.tif"), None)
        func = imread_any(func_path) if func_path else np.zeros_like(anatomy_labels[z])
        anatomy_plane = norm01(anatomy[z]); ay, axx = np.where(anatomy_labels[z] == record["anat_label"])
        if len(ay) == 0: ay, axx = np.where(record["hcr"][z] == record["conf_label"])
        if len(ay) == 0: continue
        cy, cx = int(np.median(ay)), int(np.median(axx)); y0, y1 = max(0, cy-crop_px), min(anatomy_plane.shape[0], cy+crop_px); x0, x1 = max(0, cx-crop_px), min(anatomy_plane.shape[1], cx+crop_px)
        rgb = np.dstack([anatomy_plane]*3)[y0:y1, x0:x1].copy()
        rgb[(record["hcr"][z, y0:y1, x0:x1] == record["conf_label"])] = (0, 1, 0)
        rgb[(func[y0:y1, x0:x1] == record["func_label"])] = (1, 0, 1)
        ax.imshow(rgb); ax.contour(anatomy_labels[z, y0:y1, x0:x1] == record["anat_label"], levels=[.5], colors="white", linewidths=.8)
        ax.set_title(f"{record['gene']} {record['round']} | H{record['conf_label']}→A{record['anat_label']}→R{record['func_label']}\n{record['response']}", fontsize=7); ax.axis("off")
    for ax in axes.flat[len(records):]: ax.axis("off")
    fig.suptitle(f"{fish_id}: accepted molecular-to-functional tiles (green HCR, magenta ROI, white anatomy)", fontweight="bold")
    return fig
