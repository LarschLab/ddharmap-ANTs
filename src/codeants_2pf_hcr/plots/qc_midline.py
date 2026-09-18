"""Manual, review-only anatomical midline annotation for QC Notebook 02."""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
import pathlib
import re
from typing import Mapping, Sequence

import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.ticker import MaxNLocator
import numpy as np
import pandas as pd
import tifffile
from ..context import resolve_func_polarity
from ..spatial import apply_func_orientation, imread_any, norm01
from ..traces import annotate_midline_side
from ..midline_review import load_accepted_anatomy_midline_context
from ..midline import (
    MidlineProposal,
    estimate_anatomical_axis,
    fit_fish_midline_consensus,
    propose_fish_consensus_midline,
    propose_native_dark_corridor_midline,
)


def _line_from_points(points: list[tuple[float, float]]) -> dict[str, float]:
    if len(points) != 2:
        raise ValueError("draw exactly two points to define the midline")
    (x1, y1), (x2, y2) = points
    if np.hypot(x2 - x1, y2 - y1) < 2:
        raise ValueError("midline endpoints are too close")
    return {"x0": 0.5 * (x1 + x2), "y0": 0.5 * (y1 + y2), "theta_deg": float(np.degrees(np.arctan2(y2 - y1, x2 - x1)))}


def build_automatic_anatomy_midline_qc(
    *, anatomy_stack_path: str | Path, plane_best_z: Mapping[int, int],
) -> dict:
    """Estimate per-plane anatomy midlines and combine them into one fish proposal.

    The output is an automatic QC proposal only.  It neither writes a review
    sidecar nor assigns ROI laterality.
    """
    anatomy_path = Path(anatomy_stack_path)
    stack = np.asarray(imread_any(anatomy_path), dtype=float)
    if stack.ndim != 3:
        raise ValueError(f"Anatomy stack must be ZYX for automatic midline QC: {anatomy_path}")
    images: dict[int, np.ndarray] = {}
    local_lines = {}
    shapes = {}
    for plane, z in sorted((int(plane), int(z)) for plane, z in plane_best_z.items()):
        if z < 0 or z >= stack.shape[0]:
            raise ValueError(f"Plane {plane} has out-of-range anatomy Z {z} for {anatomy_path}")
        image = np.asarray(stack[z], dtype=float)
        try:
            proposal = estimate_anatomical_axis(image)
        except ValueError:
            height, width = image.shape
            proposal = MidlineProposal(
                x0=width / 2,
                y0=height / 2,
                theta_deg=90.0,
                method="image-centre fallback",
                confidence=0.0,
            )
        images[plane] = image
        local_lines[plane] = proposal
        shapes[plane] = image.shape
    if len(local_lines) >= 3:
        consensus = fit_fish_midline_consensus(local_lines, shapes)
        lines_by_plane = {
            plane: propose_fish_consensus_midline(plane, shapes[plane], consensus, local_line=local_lines[plane])
            for plane in local_lines
        }
    else:
        consensus = {"method": "per-plane automatic proposal; too few planes for fish consensus"}
        lines_by_plane = local_lines
    rows = [
        {
            "plane_idx": plane,
            "best_z": int(plane_best_z[plane]),
            "x0": line.x0,
            "y0": line.y0,
            "theta_deg": line.theta_deg,
            "local_theta_deg": local_lines[plane].theta_deg,
            "local_confidence": local_lines[plane].confidence,
            "method": line.method,
            "requires_manual_acceptance": True,
        }
        for plane, line in lines_by_plane.items()
    ]
    return {
        "proposals": pd.DataFrame(rows),
        "lines_by_plane": {plane: {"x0": line.x0, "y0": line.y0, "theta_deg": line.theta_deg} for plane, line in lines_by_plane.items()},
        "images": images,
        "consensus": consensus,
        "anatomy_stack_path": anatomy_path,
    }


def render_anatomy_midline_proposal_grid(
    proposals: pd.DataFrame, anatomy_zyx: np.ndarray, *,
    transformed_roi_labels: Mapping[int, str | Path] | None = None,
    accepted: bool = False,
) -> plt.Figure:
    """Render every proposed line over its anatomy Z with optional ROI outlines."""
    planes = sorted(proposals["plane_idx"].astype(int).unique())
    ncols = min(5, len(planes))
    nrows = int(np.ceil(len(planes) / ncols))
    fig, axes = plt.subplots(nrows, ncols, figsize=(3.4 * ncols, 3.7 * nrows), squeeze=False, layout="constrained")
    for ax, plane in zip(axes.flat, planes):
        row = proposals.loc[proposals["plane_idx"].eq(plane)].iloc[0]
        image = np.asarray(anatomy_zyx[int(row["best_z"])])
        ax.imshow(norm01(image), cmap="gray")
        path = (transformed_roi_labels or {}).get(plane)
        if path is not None:
            labels = np.asarray(imread_any(path))
            if labels.shape != image.shape:
                raise ValueError(f"Plane {plane} ROI labels are not on the anatomy grid")
            ax.contour(labels > 0, levels=[0.5], colors="#ff4dd2", linewidths=0.28, alpha=0.7)
        _midline_line(ax, row, image.shape)
        ax.set_xlim(-0.5, image.shape[1] - 0.5)
        ax.set_ylim(image.shape[0] - 0.5, -0.5)
        ax.set_title(f"plane {plane} · z={int(row['best_z'])} · fit RMSE={row['fit_rmse_px']:.2f}px", fontsize=8)
        ax.axis("off")
    for ax in axes.flat[len(planes):]:
        ax.axis("off")
    fig.suptitle("Accepted anatomy-grid midline — logbook gate" if accepted else "Anatomy-grid midline projection — proposal only; logbook acceptance required", fontweight="bold")
    return fig


def build_automatic_native_functional_midline_qc(
    *,
    reference_paths: Mapping[int, str | Path],
    sessions_by_plane: Mapping[int, str],
    model: Mapping[str, float] | None = None,
    symmetry_weight: float = 1.0,
) -> dict:
    """Build the review-only fixed-consensus proposal in native functional space.

    The local dark-corridor fit and per-session depth trend intentionally match
    the reviewed mounted-drive proposal run.  These coordinates are native
    functional pixels and must never be used as anatomy-space laterality data.
    """
    prior = {
        "angle_deg": -45.0,
        "angle_half_range_deg": 20.0,
        "normal_offset_fraction": 0.0,
        "normal_offset_half_range_fraction": 0.2,
    }
    if model is not None:
        prior.update({key: float(value) for key, value in model.items()})
    images: dict[int, np.ndarray] = {}
    local_lines: dict[int, MidlineProposal] = {}
    shapes: dict[int, tuple[int, int]] = {}
    sessions: dict[int, str] = {}
    for plane, path_value in sorted((int(plane), Path(path)) for plane, path in reference_paths.items()):
        if plane not in sessions_by_plane:
            raise ValueError(f"Native functional midline proposal has no session for plane {plane}")
        image = np.asarray(imread_any(path_value), dtype=float)
        if image.ndim != 2:
            raise ValueError(f"Native functional reference must be 2-D: {path_value}")
        images[plane] = image
        local_lines[plane] = propose_native_dark_corridor_midline(
            image, prior, symmetry_weight=float(symmetry_weight),
        )
        shapes[plane] = image.shape
        sessions[plane] = str(sessions_by_plane[plane])
    if len(local_lines) < 3:
        consensus = {"method": "per-plane native functional proposal; too few planes for fixed consensus"}
        lines_by_plane = local_lines
    else:
        consensus = fit_fish_midline_consensus(local_lines, shapes, sessions=sessions)
        lines_by_plane = {
            plane: propose_fish_consensus_midline(
                plane, shapes[plane], consensus, session=sessions[plane], local_line=local_lines[plane],
            )
            for plane in local_lines
        }
    rows = [
        {
            "plane_idx": plane,
            "session": sessions[plane],
            "reference_path": str(reference_paths[plane]),
            "local_x0": local_lines[plane].x0,
            "local_y0": local_lines[plane].y0,
            "local_theta_deg": local_lines[plane].theta_deg,
            "fixed_x0": line.x0,
            "fixed_y0": line.y0,
            "fixed_theta_deg": line.theta_deg,
            "requires_manual_acceptance": True,
        }
        for plane, line in lines_by_plane.items()
    ]
    return {
        "proposals": pd.DataFrame(rows),
        "lines_by_plane": {plane: {"x0": line.x0, "y0": line.y0, "theta_deg": line.theta_deg} for plane, line in lines_by_plane.items()},
        "images": images,
        "consensus": consensus,
        "reference_paths": {plane: Path(path) for plane, path in reference_paths.items()},
    }


def render_automatic_anatomy_midline_qc(
    bundle: Mapping, *, functional_label_paths: Mapping[int, Path] | None = None,
) -> plt.Figure:
    """Render automatic anatomy-midline proposals with optional ROI outlines."""
    lines = bundle["lines_by_plane"]
    images = bundle["images"]
    planes = sorted(int(plane) for plane in lines)
    ncols = 2
    nrows = int(np.ceil(len(planes) / ncols))
    fig, axes = plt.subplots(nrows, ncols, figsize=(12, 4.7 * nrows), squeeze=False, constrained_layout=True)
    for ax, plane in zip(axes.flat, planes):
        image = np.asarray(images[plane], dtype=float)
        line = lines[plane]
        ax.imshow(norm01(image), cmap="gray")
        extent = float(max(image.shape))
        theta = np.deg2rad(float(line["theta_deg"]))
        dx, dy = extent * np.cos(theta), extent * np.sin(theta)
        ax.plot([line["x0"] - dx, line["x0"] + dx], [line["y0"] - dy, line["y0"] + dy], color="#e6a817", linewidth=1.8)
        label_path = (functional_label_paths or {}).get(plane)
        if label_path is not None:
            labels = np.asarray(imread_any(label_path))
            if labels.shape == image.shape and np.any(labels):
                ax.contour(labels > 0, levels=[0.5], colors="#00A6D6", linewidths=0.25, alpha=0.65)
        ax.set_title(f"Plane {plane}: automatic consensus proposal")
        ax.axis("off")
    for ax in axes.flat[len(planes):]:
        ax.axis("off")
    fig.suptitle("Automatic anatomy-midline QC — proposal only; manual acceptance remains required", fontweight="bold")
    return fig


def render_automatic_native_functional_midline_qc(
    bundle: Mapping, *, functional_label_paths: Mapping[int, Path] | None = None,
) -> plt.Figure:
    """Render native-functional fixed-consensus proposals with native ROI outlines."""
    lines = bundle["lines_by_plane"]
    images = bundle["images"]
    planes = sorted(int(plane) for plane in lines)
    ncols = 2
    nrows = int(np.ceil(len(planes) / ncols))
    fig, axes = plt.subplots(nrows, ncols, figsize=(12, 4.7 * nrows), squeeze=False, constrained_layout=True)
    for ax, plane in zip(axes.flat, planes):
        image = np.asarray(images[plane], dtype=float)
        line = lines[plane]
        ax.imshow(norm01(image), cmap="gray")
        extent = float(max(image.shape))
        theta = np.deg2rad(float(line["theta_deg"]))
        dx, dy = extent * np.cos(theta), extent * np.sin(theta)
        ax.plot([line["x0"] - dx, line["x0"] + dx], [line["y0"] - dy, line["y0"] + dy], color="#e6a817", linewidth=1.8)
        label_path = (functional_label_paths or {}).get(plane)
        if label_path is not None:
            labels = np.asarray(imread_any(label_path))
            if labels.shape == image.shape and np.any(labels):
                ax.contour(labels > 0, levels=[0.5], colors="#00A6D6", linewidths=0.25, alpha=0.65)
        ax.set_title(f"Plane {plane}: native functional fixed-consensus proposal")
        ax.axis("off")
    for ax in axes.flat[len(planes):]:
        ax.axis("off")
    fig.suptitle("Automatic native-functional midline QC — proposal only; manual acceptance remains required", fontweight="bold")
    return fig


@dataclass
class MidlineAnnotationSession:
    """State for a user-driven annotation session; no scientific table is changed."""
    fish_id: str
    anatomy_stack_path: Path | None
    plane_best_z: Mapping[int, int]
    source_artifacts: tuple[Path, ...]
    functional_label_paths: Mapping[int, Path] = field(default_factory=dict)
    plane_image_paths: Mapping[int, Path] = field(default_factory=dict)
    plane_images: Mapping[int, np.ndarray] = field(default_factory=dict)
    coordinate_space: str = "anatomy pixel grid"
    functional_roi_centroids_by_plane: Mapping[int, np.ndarray] = field(default_factory=dict)
    lines_by_plane: dict[int, dict[str, float]] = field(default_factory=dict)
    _anatomy_stack: np.ndarray | None = field(default=None, init=False, repr=False)

    def anatomy_plane(self, plane_idx: int) -> np.ndarray:
        plane_image = self.plane_images.get(int(plane_idx))
        if plane_image is not None:
            return norm01(np.asarray(plane_image, dtype=float))
        plane_image = self.plane_image_paths.get(int(plane_idx))
        if plane_image is not None:
            return norm01(np.asarray(imread_any(plane_image), dtype=float))
        if self.anatomy_stack_path is None:
            raise ValueError("no image source is available for this functional plane")
        if self._anatomy_stack is None:
            self._anatomy_stack = np.asarray(imread_any(self.anatomy_stack_path))
        z = int(self.plane_best_z[int(plane_idx)])
        return norm01(np.asarray(self._anatomy_stack[z], dtype=float))

    def functional_labels(self, plane_idx: int) -> np.ndarray | None:
        """Return registered functional ROI labels for context, when supplied."""
        label_path = self.functional_label_paths.get(int(plane_idx))
        return None if label_path is None else np.asarray(imread_any(label_path))

    def functional_roi_centroids(self, plane_idx: int) -> np.ndarray:
        """Return anatomy-grid ``(x, y)`` functional ROI centroids for context."""
        values = np.asarray(self.functional_roi_centroids_by_plane.get(int(plane_idx), np.empty((0, 2))), dtype=float)
        if values.ndim != 2 or values.shape[1:] != (2,):
            raise ValueError(f"functional ROI centroids for plane {plane_idx} must have shape (n, 2)")
        return values

    def set_line_from_points(self, plane_idx: int, points: list[tuple[float, float]]) -> dict[str, float]:
        line = _line_from_points(points)
        self.lines_by_plane[int(plane_idx)] = line
        return line

    def copy_previous(self, plane_idx: int) -> None:
        previous = [key for key in self.lines_by_plane if key < int(plane_idx)]
        if not previous:
            raise ValueError("no earlier plane annotation is available to copy")
        self.lines_by_plane[int(plane_idx)] = dict(self.lines_by_plane[max(previous)])


def show_midline_annotation_gui(session: MidlineAnnotationSession):
    """Display a click-two-points-per-plane GUI in a widget-enabled notebook."""
    try:
        import ipywidgets as widgets
        from IPython.display import display
    except ImportError as exc:  # pragma: no cover
        raise ImportError("midline annotation GUI requires ipywidgets") from exc
    planes = sorted(session.plane_best_z)
    selector = widgets.Dropdown(options=planes, description="Plane")
    copy_button = widgets.Button(description="Copy prior line")
    status = widgets.HTML("Click two endpoints; then choose another plane.")
    fig, ax = plt.subplots(figsize=(7, 7)); points: list[tuple[float, float]] = []

    def draw(*_):
        nonlocal points
        plane = int(selector.value); image = session.anatomy_plane(plane)
        labels = session.functional_labels(plane)
        centroids = session.functional_roi_centroids(plane)
        ax.clear(); ax.imshow(image, cmap="gray")
        if labels is not None and labels.shape != image.shape:
            raise ValueError(f"functional ROI labels for plane {plane} do not share the anatomy pixel grid")
        if labels is not None and np.any(labels):
            ax.contour(labels > 0, levels=[0.5], colors="#ff4dd2", linewidths=0.35)
        if len(centroids):
            ax.scatter(centroids[:, 0], centroids[:, 1], s=4, facecolors="none", edgecolors="#ff4dd2", linewidths=0.35)
        context = "functional ROI outlines" if labels is not None else "functional ROI centroids"
        ax.set_title(f"{session.fish_id}: plane {plane}, z={session.plane_best_z[plane]} — anatomy + {context}; click two endpoints")
        line = session.lines_by_plane.get(plane)
        if line:
            length = max(image.shape); angle = np.deg2rad(line["theta_deg"])
            ax.plot([line["x0"]-length*np.cos(angle), line["x0"]+length*np.cos(angle)], [line["y0"]-length*np.sin(angle), line["y0"]+length*np.sin(angle)], color="#00d4ff", linewidth=2)
        ax.axis("off"); fig.canvas.draw_idle(); points = []

    def onclick(event):
        if event.inaxes is not ax or event.xdata is None or event.ydata is None:
            return
        points.append((float(event.xdata), float(event.ydata)))
        if len(points) == 2:
            line = session.set_line_from_points(int(selector.value), points)
            status.value = f"Saved plane {selector.value}: x={line['x0']:.1f}, y={line['y0']:.1f}, θ={line['theta_deg']:.1f}°"
            draw()

    def copy(_):
        session.copy_previous(int(selector.value)); draw(); status.value = f"Copied prior line to plane {selector.value}; adjust by clicking two endpoints if needed."
    selector.observe(draw, names="value"); copy_button.on_click(copy); fig.canvas.mpl_connect("button_press_event", onclick); draw(); display(widgets.VBox([widgets.HBox([selector, copy_button]), status])); return fig


def load_legacy_midline_annotation_session(fish_root: str | Path) -> MidlineAnnotationSession:
    """Load legacy pilot registration coordinates for review-only annotation."""
    root = Path(fish_root).resolve()
    fish_id = root.name
    if not fish_id or not re.fullmatch(r"[A-Z][0-9]{3}_f[0-9]{2}", fish_id):
        raise ValueError(f"fish root must be named like L395_f06, received {root}")
    anatomy_paths = tuple((root / "02_reg" / "00_preprocessing" / "2p_anatomy").glob("*.nrrd"))
    registration_paths = tuple((root / "02_reg" / "07_2pf-a").glob("*registration_results.csv"))
    centroid_paths = tuple(root.glob("03_analysis/functional/**/ROI_transformed/*rois_transformed.csv"))
    if len(anatomy_paths) != 1:
        raise FileNotFoundError("legacy annotation requires exactly one preprocessed 2P anatomy NRRD")
    if len(registration_paths) == 1 and len(centroid_paths) == 1:
        registration = pd.read_csv(registration_paths[0])
        centroids = pd.read_csv(centroid_paths[0])
        required_registration = {"moving_plane", "z_index"}
        required_centroids = {"plane", "x_anat", "y_anat"}
        if not required_registration.issubset(registration) or not required_centroids.issubset(centroids):
            raise ValueError("legacy registration or transformed-ROI table has an unexpected schema")
        plane_best_z = {int(row.moving_plane): int(row.z_index) for row in registration.itertuples(index=False)}
        centroid_map = {int(plane): group[["x_anat", "y_anat"]].dropna().to_numpy(dtype=float) for plane, group in centroids.groupby("plane", sort=False)}
        sources = (registration_paths[0], centroid_paths[0])
    else:
        transform_paths = tuple(root.glob("03_analysis/functional/ncc/tforms_by_plane.csv"))
        per_plane_paths = tuple(root.glob("03_analysis/functional/registration/f2a_centroid_matches_*_plane*.csv"))
        if len(transform_paths) != 1 or not per_plane_paths:
            raise FileNotFoundError("annotation needs legacy registration tables or current tforms_by_plane plus per-plane f2a centroid tables")
        transforms = pd.read_csv(transform_paths[0])
        if not {"plane_index", "best_z"}.issubset(transforms):
            raise ValueError("current tforms_by_plane table has an unexpected schema")
        plane_best_z = {int(row.plane_index): int(row.best_z) for row in transforms.itertuples(index=False)}
        centroid_map = {}
        for path in per_plane_paths:
            match = re.search(r"_plane(\d+)(?:_|\\.)", path.name)
            table = pd.read_csv(path)
            if match is None or not {"fx_anat_px", "fy_anat_px"}.issubset(table):
                raise ValueError(f"current centroid table has an unexpected schema: {path}")
            centroid_map[int(match.group(1))] = table[["fx_anat_px", "fy_anat_px"]].dropna().to_numpy(dtype=float)
        missing = set(plane_best_z).difference(centroid_map)
        if missing:
            raise ValueError(f"missing anatomy-space centroid tables for planes: {sorted(missing)}")
        sources = (transform_paths[0], *per_plane_paths)
    return MidlineAnnotationSession(
        fish_id=fish_id,
        anatomy_stack_path=anatomy_paths[0],
        plane_best_z=plane_best_z,
        source_artifacts=sources,
        functional_roi_centroids_by_plane=centroid_map,
    )


def load_native_functional_midline_annotation_session(fish_root: str | Path) -> MidlineAnnotationSession:
    """Load native functional references and ROI labels without anatomy registration."""
    root = Path(fish_root).resolve(); fish_id = root.name
    if not re.fullmatch(r"[A-Z][0-9]{3}_f[0-9]{2}", fish_id):
        raise ValueError(f"fish root must be named like L396_f03, received {root}")
    references = tuple(root.glob("03_analysis/functional/raw/*_ref_norm.tif"))
    if not references:
        raise FileNotFoundError("native annotation requires persisted functional reference images")
    def plane(path: Path) -> int:
        match = re.search(r"_plane(\d+)_", path.name)
        if match is None: raise ValueError(f"could not parse functional plane from {path}")
        return int(match.group(1))
    images = {plane(path): path for path in references}
    labels = {plane(path): path for path in root.glob("03_analysis/functional/derived/*func_mask_in_2p.tif")}
    return MidlineAnnotationSession(
        fish_id=fish_id, anatomy_stack_path=None,
        plane_best_z={index: index for index in images},
        source_artifacts=tuple(references), functional_label_paths=labels,
        plane_image_paths=images, coordinate_space="native functional pixel grid",
    )


def load_suite2p_meanimg_midline_session(fish_root: str | Path) -> MidlineAnnotationSession:
    """Load oriented native-functional Suite2p ``meanImg`` planes for review.

    This is a read-only fallback for older fish that retain Suite2p outputs but
    not persisted ``*_ref_norm.tif`` images.  The raw Suite2p image is oriented
    with the fish's metadata-derived functional polarity, matching the current
    native functional-reference convention.
    """
    root = Path(fish_root).resolve()
    fish_id = root.name
    if not re.fullmatch(r"[A-Z][0-9]{3}_f[0-9]{2}", fish_id):
        raise ValueError(f"fish root must be named like L427_f01, received {root}")
    ops_paths = tuple(sorted(root.glob("03_analysis/functional/suite2P/plane*/ops.npy")))
    if not ops_paths:
        raise FileNotFoundError("Suite2p meanImg review requires ops.npy under suite2P/plane*/")
    polarity, _ = resolve_func_polarity(fish_id, root / "matchingMetadata.csv", fish_dir=root)
    images: dict[int, np.ndarray] = {}
    for path in ops_paths:
        match = re.fullmatch(r"plane(\d+)", path.parent.name)
        if match is None:
            raise ValueError(f"could not parse Suite2p plane from {path}")
        original_windows_path = pathlib.WindowsPath
        try:
            pathlib.WindowsPath = pathlib.PosixPath
            ops = np.load(path, allow_pickle=True).item()
        finally:
            pathlib.WindowsPath = original_windows_path
        if not isinstance(ops, dict) or "meanImg" not in ops:
            raise ValueError(f"Suite2p ops is missing meanImg: {path}")
        image = np.asarray(ops["meanImg"], dtype=float)
        if image.ndim != 2 or not np.isfinite(image).any():
            raise ValueError(f"Suite2p meanImg must be a finite 2-D image: {path}")
        images[int(match.group(1))] = apply_func_orientation(image, polarity=polarity, flip_x=True)
    return MidlineAnnotationSession(
        fish_id=fish_id,
        anatomy_stack_path=None,
        plane_best_z={index: index for index in images},
        source_artifacts=ops_paths,
        plane_images=images,
        coordinate_space="native functional pixel grid",
    )


def load_motion_corrected_mean_midline_session(
    fish_root: str | Path, *, sampled_frames: int = 64, polarity_override: str | None = None
) -> MidlineAnnotationSession:
    """Build read-only native-functional references from motion-corrected movies.

    Evenly sampled movie pages make this fallback bounded for legacy fish that
    have motion correction but neither persisted references nor Suite2p output.
    The resulting mean images use the same metadata-derived orientation as the
    native functional-reference stage; they are QC inputs only. An explicit
    override is available only for a legacy cohort with validated orientation.
    """
    if sampled_frames < 3:
        raise ValueError("sampled_frames must be at least 3")
    root = Path(fish_root).resolve()
    fish_id = root.name
    if not re.fullmatch(r"[A-Z][0-9]{3}_f[0-9]{2}", fish_id):
        raise ValueError(f"fish root must be named like L395_f06, received {root}")
    paths = tuple(sorted((root / "02_reg/00_preprocessing/2p_functional/02_motionCorrected").glob("*_plane*_mcorrected.tif")))
    if not paths:
        raise FileNotFoundError("motion-corrected mean review requires per-plane *_mcorrected.tif files")
    polarity, _ = resolve_func_polarity(fish_id, root / "matchingMetadata.csv", fish_dir=root)
    if polarity_override is not None:
        polarity = str(polarity_override).strip().lower()
    if polarity not in {"north", "south"}:
        raise ValueError(
            "motion-corrected mean review needs north/south polarity metadata "
            "or an explicit validated polarity_override"
        )
    images: dict[int, np.ndarray] = {}
    for path in paths:
        match = re.search(r"_plane(\d+)_", path.name)
        if match is None:
            raise ValueError(f"could not parse motion-corrected plane from {path}")
        with tifffile.TiffFile(path) as handle:
            pages = handle.pages
            if not pages:
                raise ValueError(f"motion-corrected movie has no pages: {path}")
            indices = np.unique(np.linspace(0, len(pages) - 1, min(sampled_frames, len(pages)), dtype=int))
            selected = set(indices.tolist())
            image = np.mean(np.stack([page.asarray() for page_index, page in enumerate(pages) if page_index in selected]), axis=0)
        if image.ndim != 2 or not np.isfinite(image).any():
            raise ValueError(f"motion-corrected reference must be a finite 2-D image: {path}")
        images[int(match.group(1))] = apply_func_orientation(image, polarity=polarity, flip_x=True)
    return MidlineAnnotationSession(
        fish_id=fish_id,
        anatomy_stack_path=None,
        plane_best_z={index: index for index in images},
        source_artifacts=paths,
        plane_images=images,
        coordinate_space="native functional pixel grid",
    )


_SIDE_COLORS = {"left": "#2878b5", "right": "#d1495b", "midline": "#e6a817", "unknown": "#777777"}
_ROI_KEYS = ("plane_idx", "func_label")


def _read_roi_table(path: str | Path, fish_id: str, *, activity: bool = False) -> pd.DataFrame:
    table = pd.read_csv(path)
    required = set(_ROI_KEYS)
    if activity:
        required |= {"activity_mag"}
    else:
        required |= {"fish_id"}
    missing = sorted(required - set(table.columns))
    if missing:
        raise ValueError(f"{Path(path).name} lacks required columns {missing}")
    if "fish_id" in table:
        table = table.loc[table["fish_id"].astype(str).eq(str(fish_id))].copy()
    elif activity:
        if str(fish_id) not in Path(path).resolve().parts:
            raise ValueError("Fish-less BPI cells must come from the selected fish's staged root")
        table = table.assign(fish_id=str(fish_id))
    if table.empty:
        raise ValueError(f"{Path(path).name} has no rows for {fish_id}")
    if table.duplicated(list(_ROI_KEYS)).any():
        raise ValueError(f"{Path(path).name} has duplicate ROI keys")
    return table


def _anatomy_xy_columns(table: pd.DataFrame) -> tuple[str, str]:
    for pair in (("centroid_x_anat", "centroid_y_anat"), ("x_anat", "y_anat"), ("fx_anat_px", "fy_anat_px")):
        if set(pair).issubset(table.columns):
            return pair
    raise ValueError("ROI table lacks anatomy-space centroid columns; do not transform functional coordinates again")


def build_midline_laterality_qc_tables(
    roi_master_path: str | Path,
    bpi_cells_path: str | Path,
    accepted_sidecar_path: str | Path,
    *, fish_id: str,
) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    """Build all-ROI and persisted-response-subset laterality tables, read-only.

    The first table has one row per ROI inventory key.  The second is exactly
    the persisted response-scored BPI key set with finite activity magnitude,
    joined back only to obtain anatomy-grid coordinates.  Neither table uses
    HCR identity, match status, or a proposed midline to select rows.
    """
    all_rois = _read_roi_table(roi_master_path, fish_id)
    activity = _read_roi_table(bpi_cells_path, fish_id, activity=True)
    x_col, y_col = _anatomy_xy_columns(all_rois)
    context = load_accepted_anatomy_midline_context(
        accepted_sidecar_path, fish_id=fish_id, required_planes=sorted(all_rois["plane_idx"].unique())
    )
    all_rois = annotate_midline_side(all_rois, context, x_col=x_col, y_col=y_col)
    spatial = all_rois[list(_ROI_KEYS) + [x_col, y_col]].copy()
    activity = activity.merge(spatial, on=list(_ROI_KEYS), how="left", validate="one_to_one", indicator=True)
    if activity["_merge"].ne("both").any():
        raise ValueError("Persisted response-scored keys are absent from the ROI inventory; cannot make a spatial QC map")
    activity = activity.drop(columns="_merge")
    # ROIs without anatomy-grid centroids remain in the exact scored subset
    # and receive the explicit 'unknown' laterality rather than disappearing.
    activity["activity_mag"] = pd.to_numeric(activity["activity_mag"], errors="coerce")
    activity = activity.loc[np.isfinite(activity["activity_mag"])].copy()
    if activity.empty:
        raise ValueError("Persisted response-scored BPI table has no finite activity magnitude rows")
    activity = annotate_midline_side(activity, context, x_col=x_col, y_col=y_col)
    for table in (all_rois, activity):
        table["midline_side"] = table["midline_side"].astype(str)
    return all_rois, activity, {"x_col": x_col, "y_col": y_col, "midline_context": context}


def _midline_line(axis: plt.Axes, line: Mapping[str, float], shape: tuple[int, int], *, style: str = "-") -> None:
    angle = np.deg2rad(float(line["theta_deg"])); length = max(shape) * 1.5
    axis.plot([float(line["x0"]) - length * np.cos(angle), float(line["x0"]) + length * np.cos(angle)], [float(line["y0"]) - length * np.sin(angle), float(line["y0"]) + length * np.sin(angle)], color="#ffd23f", lw=1.5, ls=style)


def render_midline_laterality_plane_grid(
    all_rois: pd.DataFrame, context: Mapping, anatomy_zyx: np.ndarray, plane_z: Mapping[int, int], *, proposal_lines: Mapping[int, Mapping[str, float]] | None = None, roi_label_images: Mapping[int, np.ndarray | str | Path] | None = None
) -> plt.Figure:
    """Render the `[56f-qc]` all-Suite2p ROI anatomy-grid laterality review."""
    x_col, y_col = _anatomy_xy_columns(all_rois); planes = sorted(all_rois["plane_idx"].astype(int).unique())
    ncols = min(5, max(1, len(planes))); nrows = int(np.ceil(len(planes) / ncols))
    fig, axes = plt.subplots(nrows, ncols, figsize=(3.2 * ncols, 3.6 * nrows), squeeze=False, layout="constrained")
    lines = context["review"]["lines_by_plane"]
    for ax, plane in zip(axes.flat, planes):
        image = np.asarray(anatomy_zyx[int(plane_z[int(plane)])]); sub = all_rois[all_rois["plane_idx"].astype(int).eq(plane)]
        ax.imshow(norm01(image), cmap="gray")
        label_source = (roi_label_images or {}).get(plane)
        if label_source is not None:
            labels = np.asarray(imread_any(label_source) if isinstance(label_source, (str, Path)) else label_source)
            if labels.shape != image.shape:
                raise ValueError(f"ROI label image for plane {plane} does not share the anatomy display grid")
            ax.contour(labels > 0, levels=[0.5], colors="#ff4dd2", linewidths=0.3, alpha=0.75)
        for side, color in _SIDE_COLORS.items():
            rows = sub[sub["midline_side"].eq(side)]
            ax.scatter(rows[x_col], rows[y_col], s=9, c=color, label=side, linewidths=0)
        _midline_line(ax, lines[str(plane)] if str(plane) in lines else lines[plane], image.shape)
        if proposal_lines and plane in proposal_lines:
            _midline_line(ax, proposal_lines[plane], image.shape, style="--")
        ax.set_xlim(-0.5, image.shape[1] - 0.5)
        ax.set_ylim(image.shape[0] - 0.5, -0.5)
        counts = sub["midline_side"].value_counts().reindex(_SIDE_COLORS, fill_value=0)
        ax.set_title(f"plane {plane}: " + ", ".join(f"{s[0].upper()}={int(counts[s])}" for s in _SIDE_COLORS), fontsize=8)
        ax.axis("off")
    for ax in axes.flat[len(planes):]: ax.axis("off")
    fig.suptitle("All Suite2p ROI laterality — accepted manual anatomy midline", fontweight="bold")
    return fig


def render_midline_activity_plane_grid(
    activity_rois: pd.DataFrame, context: Mapping, anatomy_zyx: np.ndarray, plane_z: Mapping[int, int], *, proposal_lines: Mapping[int, Mapping[str, float]] | None = None
) -> plt.Figure:
    """Render `[56f-qc-activity]` for the exact persisted response-scored subset."""
    x_col, y_col = _anatomy_xy_columns(activity_rois); planes = sorted(activity_rois["plane_idx"].astype(int).unique())
    ncols = min(5, max(1, len(planes))); nrows = int(np.ceil(len(planes) / ncols)); vmax = float(activity_rois["activity_mag"].max())
    fig, axes = plt.subplots(nrows, ncols, figsize=(3.2 * ncols, 3.6 * nrows), squeeze=False, layout="constrained"); last = None
    lines = context["review"]["lines_by_plane"]
    for ax, plane in zip(axes.flat, planes):
        image = np.asarray(anatomy_zyx[int(plane_z[int(plane)])]); sub = activity_rois[activity_rois["plane_idx"].astype(int).eq(plane)]
        ax.imshow(norm01(image), cmap="gray")
        last = ax.scatter(sub[x_col], sub[y_col], s=18, c=sub["activity_mag"], cmap="magma", vmin=0, vmax=vmax, edgecolors=[_SIDE_COLORS.get(s, "#777777") for s in sub["midline_side"]], linewidths=0.55)
        _midline_line(ax, lines[str(plane)] if str(plane) in lines else lines[plane], image.shape)
        if proposal_lines and plane in proposal_lines: _midline_line(ax, proposal_lines[plane], image.shape, style="--")
        ax.set_xlim(-0.5, image.shape[1] - 0.5)
        ax.set_ylim(image.shape[0] - 0.5, -0.5)
        counts = sub["midline_side"].value_counts().reindex(_SIDE_COLORS, fill_value=0)
        ax.set_title(f"plane {plane}: scored n={len(sub)}; L/R/M/U=" + "/".join(str(int(counts[s])) for s in _SIDE_COLORS), fontsize=8); ax.axis("off")
    for ax in axes.flat[len(planes):]: ax.axis("off")
    if last is not None: fig.colorbar(last, ax=axes.ravel().tolist(), shrink=.7, label="Persisted activity magnitude")
    fig.suptitle("Persisted response-scored ROI activity — accepted manual anatomy midline", fontweight="bold")
    return fig



def build_left_right_index_qc_table(
    all_rois: pd.DataFrame, points_csv: str | Path, context: Mapping, *, threshold: float = 0.1,
) -> pd.DataFrame:
    """Pair persisted ipsi/contra AUCs into review-only left/right indices."""
    if not 0 < threshold < 1:
        raise ValueError("LRI threshold must lie between zero and one")
    x_col, y_col = _anatomy_xy_columns(all_rois)
    if not (set(_ROI_KEYS) | {"midline_side", "bout_response_pass", "cont_response_pass"}).issubset(all_rois):
        raise ValueError("LRI review needs accepted-midline ROI sides and persisted mode response calls")
    points = pd.read_csv(points_csv)
    required = set(_ROI_KEYS) | {"group", "laterality", "stim_mode", "auc_dff", "response_is_active", "accepted_midline_sidecar_sha256"}
    missing = sorted(required - set(points))
    if missing:
        raise ValueError(f"Motion-AUC point table lacks {missing}")
    points = points.loc[points["group"].eq("All neurons")].copy()
    if points.empty or set(points["accepted_midline_sidecar_sha256"].dropna().astype(str)) != {str(context["sidecar_sha256"])}:
        raise ValueError("Motion-AUC points do not match the accepted anatomy midline")
    points = points.loc[points["laterality"].isin({"ipsi", "contra"}) & points["stim_mode"].isin({"bout", "continuous"})].copy()
    keys = list(_ROI_KEYS) + ["stim_mode"]
    if points.duplicated(keys + ["laterality"]).any():
        raise ValueError("Motion-AUC points contain duplicate ROI/mode/laterality keys")
    if points.groupby(keys)["response_is_active"].nunique().gt(1).any():
        raise ValueError("Motion-AUC response flags disagree between stimulus sides")
    paired = points.pivot(index=keys, columns="laterality", values="auc_dff").reset_index()
    response = points.loc[points["laterality"].eq("ipsi"), keys + ["response_is_active"]]
    paired = paired.merge(response, on=keys, how="left", validate="one_to_one")
    spatial = all_rois[list(_ROI_KEYS) + [x_col, y_col, "midline_side", "bout_response_pass", "cont_response_pass"]]
    if spatial.duplicated(list(_ROI_KEYS)).any():
        raise ValueError("ROI inventory contains duplicate keys")
    paired = paired.merge(spatial, on=list(_ROI_KEYS), how="left", validate="many_to_one", indicator=True)
    if paired["_merge"].ne("both").any() or paired["midline_side"].isna().any():
        raise ValueError("Motion-AUC ROI keys are absent from the accepted-midline inventory")
    paired = paired.drop(columns="_merge")
    paired["left_auc"] = np.where(paired["midline_side"].eq("left"), paired["ipsi"], paired["contra"])
    paired["right_auc"] = np.where(paired["midline_side"].eq("right"), paired["ipsi"], paired["contra"])
    paired["auc_sum"] = paired["left_auc"] + paired["right_auc"]
    valid_sum = np.isfinite(paired["auc_sum"]) & (paired["auc_sum"].abs() > 1e-12)
    lri = np.full(len(paired), np.nan, dtype=float)
    np.divide(paired["left_auc"] - paired["right_auc"], paired["auc_sum"], out=lri, where=valid_sum)
    paired["lri"] = lri
    response_column = np.where(paired["stim_mode"].eq("bout"), paired["bout_response_pass"], paired["cont_response_pass"])
    mode_active = pd.Series(response_column, index=paired.index).astype(str).str.lower().isin({"true", "1"})
    negative = (paired["left_auc"] < 0) | (paired["right_auc"] < 0)
    complete = paired["midline_side"].isin({"left", "right"}) & paired[[x_col, y_col, "left_auc", "right_auc"]].notna().all(axis=1)
    paired["lri_qc_class"] = np.select(
        [~complete | ~valid_sum, negative, ~mode_active, paired["lri"] > threshold, paired["lri"] < -threshold],
        ["unavailable", "negative AUC review", "not mode-responsive", "left-preferring", "right-preferring"],
        default="within threshold",
    )
    return paired


def render_left_right_index_plane_grid(
    lri_rows: pd.DataFrame, context: Mapping, anatomy_zyx: np.ndarray, plane_z: Mapping[int, int], *, threshold: float = 0.1,
) -> plt.Figure:
    """Map the diagnostic LRI for response-positive ROIs on accepted anatomy."""
    x_col, y_col = _anatomy_xy_columns(lri_rows)
    planes = sorted(lri_rows["plane_idx"].astype(int).unique())
    ncols = min(5, len(planes))
    mode_rows = int(np.ceil(len(planes) / ncols))
    fig, axes = plt.subplots(2 * mode_rows, ncols, figsize=(3.6 * ncols, 3.8 * 2 * mode_rows), squeeze=False)
    fig.subplots_adjust(left=0.025, right=0.985, bottom=0.08, top=0.93, hspace=0.16, wspace=0.05)
    styles = {
        "left-preferring": ("#d1495b", "o", 24),
        "right-preferring": ("#2878b5", "o", 24),
        "within threshold": ("#f0c44f", "o", 17),
        "not mode-responsive": ("#aaaaaa", ".", 5),
        "negative AUC review": ("#9b59b6", "x", 26),
        "unavailable": ("#222222", "x", 26),
    }
    lines = context["review"]["lines_by_plane"]
    for mode_index, mode in enumerate(("bout", "continuous")):
        for plane_index, plane in enumerate(planes):
            ax = axes[mode_index * mode_rows + plane_index // ncols, plane_index % ncols]
            image = np.asarray(anatomy_zyx[int(plane_z[plane])])
            ax.imshow(norm01(image), cmap="gray")
            sub = lri_rows.loc[lri_rows["plane_idx"].eq(plane) & lri_rows["stim_mode"].eq(mode)]
            for label, (color, marker, size) in styles.items():
                rows = sub.loc[sub["lri_qc_class"].eq(label)]
                if not rows.empty:
                    ax.scatter(rows[x_col], rows[y_col], c=color, marker=marker, s=size, alpha=0.82, linewidths=0.7)
            _midline_line(ax, lines[str(plane)] if str(plane) in lines else lines[plane], image.shape)
            counts = sub["lri_qc_class"].value_counts()
            ax.set_title(f"{mode} · plane {plane} · n={len(sub)}\nL/R/neutral={counts.get('left-preferring', 0)}/{counts.get('right-preferring', 0)}/{counts.get('within threshold', 0)}", fontsize=8)
            ax.set_xlim(-0.5, image.shape[1] - 0.5)
            ax.set_ylim(image.shape[0] - 0.5, -0.5)
            ax.axis("off")
    for ax in axes.flat:
        if not ax.has_data():
            ax.axis("off")
    handles = [Line2D([0], [0], marker=marker, color="none", markerfacecolor=color, markeredgecolor=color, markersize=7, label=label) for label, (color, marker, _) in styles.items()]
    fig.legend(handles=handles, loc="lower center", ncol=3, fontsize=8)
    fig.suptitle(f"Left and right stimulus preference across anatomy planes · LRI guide ±{threshold:g}", fontweight="bold")
    return fig


def render_left_right_index_histogram(lri_rows: pd.DataFrame, *, threshold: float = 0.1) -> plt.Figure:
    """Show LRI distributions for the same response-positive rows colored on the map."""
    if not 0 < threshold < 1:
        raise ValueError("LRI threshold must lie between zero and one")
    included = {"left-preferring", "right-preferring", "within threshold"}
    fig, axes = plt.subplots(1, 2, figsize=(10, 3.6), sharex=True, layout="constrained")
    bins = np.linspace(-1, 1, 41)
    for ax, mode, color in zip(axes, ("bout", "continuous"), ("#2c7fb8", "#d95f0e")):
        rows = lri_rows.loc[lri_rows["stim_mode"].eq(mode) & lri_rows["lri_qc_class"].isin(included)]
        values = pd.to_numeric(rows["lri"], errors="coerce").dropna().to_numpy()
        ax.axvspan(-threshold, threshold, color="#f0c44f", alpha=0.25)
        ax.hist(values, bins=bins, color=color, edgecolor="white", linewidth=0.4)
        ax.axvline(0, color="#555555", lw=0.8)
        for boundary in (-threshold, threshold):
            ax.axvline(boundary, color="#a87800", lw=1, ls="--")
        ax.set_xlim(1.025, -1.025)
        ax.set_title(f"{mode.capitalize()} responses · n={len(values)}", fontsize=11)
        ax.set_xlabel("Left–right index (L−R)/(L+R)")
        ax.set_ylabel("ROI count")
        ax.yaxis.set_major_locator(MaxNLocator(integer=True))
    fig.suptitle("Left–right preference distributions among mode-responsive ROIs", fontweight="bold")
    return fig


__all__ = ["MidlineAnnotationSession", "build_automatic_anatomy_midline_qc", "render_automatic_anatomy_midline_qc", "render_anatomy_midline_proposal_grid", "build_automatic_native_functional_midline_qc", "render_automatic_native_functional_midline_qc", "load_legacy_midline_annotation_session", "load_motion_corrected_mean_midline_session", "load_native_functional_midline_annotation_session", "load_suite2p_meanimg_midline_session", "show_midline_annotation_gui", "load_accepted_anatomy_midline_context", "build_midline_laterality_qc_tables", "render_midline_laterality_plane_grid", "render_midline_activity_plane_grid", "build_left_right_index_qc_table", "render_left_right_index_plane_grid", "render_left_right_index_histogram"]
