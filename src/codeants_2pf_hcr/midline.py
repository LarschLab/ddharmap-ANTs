"""Review-only, pilot-calibrated anatomical midline proposals.

This module never replaces a saved midline or assigns ROI laterality.  It
produces a proposal and calibration diagnostics which require manual acceptance.
"""
from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Mapping, Sequence

import numpy as np
from scipy.ndimage import map_coordinates
from skimage.filters import gaussian, threshold_otsu
from skimage.measure import label, regionprops


@dataclass(frozen=True)
class MidlineProposal:
    x0: float
    y0: float
    theta_deg: float
    method: str
    confidence: float
    coordinate_space: str = "anatomy pixel grid"
    requires_manual_acceptance: bool = True


def _wrap(theta: float) -> float:
    return ((float(theta) + 90.0) % 180.0) - 90.0


def _axial_mean(angles_deg: np.ndarray) -> float:
    """Mean for unoriented lines, robust to the -90°/+90° representation cut."""
    radians = np.deg2rad(np.asarray(angles_deg, dtype=float) * 2.0)
    return _wrap(0.5 * np.degrees(np.arctan2(np.mean(np.sin(radians)), np.mean(np.cos(radians)))))


def _axial_difference(angles_deg: np.ndarray | float, reference_deg: float) -> np.ndarray:
    """Return the smallest angle difference between unoriented lines."""
    return ((np.asarray(angles_deg, dtype=float) - float(reference_deg) + 90.0) % 180.0) - 90.0


def _line_offset_fraction(line: Mapping[str, float], image_shape: tuple[int, int], normal_theta_deg: float) -> float:
    """Measure how far a line is from the image centre along a chosen normal."""
    h, w = image_shape
    radians = math.radians(normal_theta_deg)
    normal = np.array([-math.sin(radians), math.cos(radians)])
    centre_to_line = np.array([float(line["x0"]) - w / 2, float(line["y0"]) - h / 2])
    return float(np.dot(centre_to_line, normal) / math.hypot(h, w))


def _robust_line_fit(depths: np.ndarray, offsets: np.ndarray) -> tuple[float, float, np.ndarray]:
    """Fit a line while ignoring planes that disagree strongly with the others."""
    if len(depths) != len(offsets) or len(depths) < 2:
        raise ValueError("at least two matching depth and offset values are needed")
    differences = depths[:, None] - depths[None, :]
    offset_differences = offsets[:, None] - offsets[None, :]
    upper = np.triu(np.abs(differences) > np.finfo(float).eps, 1)
    slopes = offset_differences[upper] / differences[upper]
    slope = float(np.median(slopes)) if len(slopes) else 0.0
    intercept = float(np.median(offsets - slope * depths))
    residuals = offsets - (intercept + slope * depths)
    mad = float(np.median(np.abs(residuals - np.median(residuals))))
    # Exact agreement among good planes is common.  A small floor stops that
    # agreement from accidentally admitting an obviously displaced outlier.
    keep = np.abs(residuals - np.median(residuals)) <= max(3.0 * mad, 0.005)
    if int(keep.sum()) >= 2:
        kept_depths, kept_offsets = depths[keep], offsets[keep]
        differences = kept_depths[:, None] - kept_depths[None, :]
        offset_differences = kept_offsets[:, None] - kept_offsets[None, :]
        upper = np.triu(np.abs(differences) > np.finfo(float).eps, 1)
        slopes = offset_differences[upper] / differences[upper]
        slope = float(np.median(slopes)) if len(slopes) else 0.0
        intercept = float(np.median(kept_offsets - slope * kept_depths))
    return intercept, slope, keep


def estimate_anatomical_axis(image: np.ndarray) -> MidlineProposal:
    """Suggest the main long axis visible in one anatomy image.

    This is a rough starting suggestion for a person to inspect. It does not
    save a midline or decide which side of a brain an ROI belongs to.
    """
    arr = np.asarray(image, dtype=float)
    finite = np.isfinite(arr)
    if arr.ndim != 2 or finite.sum() < 100:
        raise ValueError("midline estimation needs a finite 2-D anatomy image")
    lo, hi = np.percentile(arr[finite], [1, 99])
    norm = np.clip((arr - lo) / max(hi - lo, np.finfo(float).eps), 0, 1)
    norm[~finite] = float(np.median(norm[finite]))
    smooth = gaussian(norm, sigma=2, preserve_range=True)
    threshold = threshold_otsu(smooth[np.isfinite(smooth)])
    mask = smooth > threshold
    components = regionprops(label(mask))
    if not components:
        raise ValueError("could not find anatomy component for midline proposal")
    component = max(components, key=lambda item: item.area)
    ys, xs = component.coords[:, 0], component.coords[:, 1]
    x0, y0 = float(xs.mean()), float(ys.mean())
    covariance = np.cov(np.column_stack((xs - x0, ys - y0)), rowvar=False)
    values, vectors = np.linalg.eigh(covariance)
    axis = vectors[:, int(np.argmax(values))]
    theta = _wrap(np.degrees(np.arctan2(axis[1], axis[0])))
    elongation = float((values.max() - values.min()) / max(values.max(), np.finfo(float).eps))
    return MidlineProposal(x0, y0, theta, "largest-component PCA", elongation)


def fit_pilot_calibration(images: Sequence[np.ndarray], accepted_lines: Sequence[dict]) -> dict:
    """Learn a small correction from manually accepted pilot midlines only.

    The calibration is deliberately a median correction rather than a learned
    image model: four pilot fish cannot support a general image learner.
    """
    if len(images) != len(accepted_lines) or len(images) < 3:
        raise ValueError("need matched pilot images and at least three accepted lines")
    corrections = []
    for image, target in zip(images, accepted_lines):
        base = estimate_anatomical_axis(image)
        h, w = np.asarray(image).shape
        for key in ("x0", "y0", "theta_deg"):
            if key not in target or not np.isfinite(float(target[key])):
                raise ValueError(f"accepted pilot line missing finite {key!r}")
        corrections.append(((float(target["x0"]) - base.x0) / w, (float(target["y0"]) - base.y0) / h, _wrap(float(target["theta_deg"]) - base.theta_deg)))
    values = np.asarray(corrections, dtype=float)
    median = np.array((np.median(values[:, 0]), np.median(values[:, 1]), _axial_mean(values[:, 2])))
    spread = np.median(np.abs(values - median), axis=0)
    return {"n_pilot": len(images), "dx_fraction": float(median[0]), "dy_fraction": float(median[1]), "dtheta_deg": float(median[2]), "mad_dx_fraction": float(spread[0]), "mad_dy_fraction": float(spread[1]), "mad_dtheta_deg": float(spread[2]), "method": "pilot-median correction on largest-component PCA", "coordinate_space": "anatomy pixel grid", "requires_manual_acceptance": True}


def propose_pilot_calibrated_midline(image: np.ndarray, calibration: dict) -> MidlineProposal:
    """Apply the pilot correction to one image as a line for a person to review."""
    required = ("dx_fraction", "dy_fraction", "dtheta_deg", "mad_dx_fraction", "mad_dy_fraction", "mad_dtheta_deg")
    if any(key not in calibration or not np.isfinite(float(calibration[key])) for key in required):
        raise ValueError("invalid or incomplete pilot midline calibration")
    base = estimate_anatomical_axis(image)
    h, w = np.asarray(image).shape
    spread = float(calibration["mad_dx_fraction"] + calibration["mad_dy_fraction"] + calibration["mad_dtheta_deg"] / 90.0)
    confidence = float(np.clip(base.confidence * (1.0 - min(spread, 1.0)), 0, 1))
    return MidlineProposal(base.x0 + float(calibration["dx_fraction"]) * w, base.y0 + float(calibration["dy_fraction"]) * h, _wrap(base.theta_deg + float(calibration["dtheta_deg"])), "pilot-calibrated PCA proposal", confidence)


def fit_native_dark_corridor_model(images: Sequence[np.ndarray], accepted_lines: Sequence[dict]) -> dict:
    """Learn the plausible midline area from manually accepted functional images.

    The returned prior constrains, but never accepts, a proposal.  It has no
    image-derived learned weights and is fit only on the supplied training set.
    """
    if len(images) != len(accepted_lines) or len(images) < 3:
        raise ValueError("need matched native functional images and at least three labels")
    angles, offsets = [], []
    for image, line in zip(images, accepted_lines):
        h, w = np.asarray(image).shape; theta = _wrap(float(line["theta_deg"]))
        normal = np.array([-math.sin(math.radians(theta)), math.cos(math.radians(theta))])
        offsets.append(float(np.dot(np.array([line["x0"] - w / 2, line["y0"] - h / 2]), normal) / math.hypot(h, w)))
        angles.append(theta)
    angle = _axial_mean(np.asarray(angles)); offset = float(np.median(offsets))
    return {"coordinate_space": "native functional pixel grid", "angle_deg": angle, "angle_half_range_deg": max(12.0, 3 * float(np.median(np.abs(np.asarray(angles) - angle)))), "normal_offset_fraction": offset, "normal_offset_half_range_fraction": max(0.08, 3 * float(np.median(np.abs(np.asarray(offsets) - offset)))), "n_training_planes": len(images), "requires_manual_acceptance": True}


def propose_native_dark_corridor_midline(image: np.ndarray, model: dict, *, symmetry_weight: float = 0.0) -> MidlineProposal:
    """Suggest the darkest plausible midline, optionally favouring left-right symmetry.

    The returned line is a review suggestion only. A person must still correct
    and accept it before it can be used for laterality.
    """
    arr = np.asarray(image, dtype=float); h, w = arr.shape
    lo, hi = np.percentile(arr[np.isfinite(arr)], [1, 99]); arr = np.clip((arr - lo) / max(hi - lo, np.finfo(float).eps), 0, 1)
    angles = np.linspace(float(model["angle_deg"]) - float(model["angle_half_range_deg"]), float(model["angle_deg"]) + float(model["angle_half_range_deg"]), 21)
    offsets = np.linspace(float(model["normal_offset_fraction"]) - float(model["normal_offset_half_range_fraction"]), float(model["normal_offset_fraction"]) + float(model["normal_offset_half_range_fraction"]), 25)
    length = math.hypot(h, w); tangent_samples = np.linspace(-length, length, 700); grid = np.linspace(0, 1, 32); gy, gx = np.meshgrid(grid * (h - 1), grid * (w - 1), indexing="ij")
    candidates = []
    for theta in angles:
        radians = math.radians(theta); tangent = np.array([math.cos(radians), math.sin(radians)]); normal = np.array([-tangent[1], tangent[0]])
        for offset in offsets:
            center = np.array([w / 2, h / 2]) + offset * length * normal; points = center + tangent_samples[:, None] * tangent
            keep = (points[:, 0] >= 0) & (points[:, 0] < w) & (points[:, 1] >= 0) & (points[:, 1] < h)
            dark = float(np.median(map_coordinates(arr, [points[keep, 1], points[keep, 0]], order=1)))
            symmetry = 0.0
            if symmetry_weight:
                reflected = np.stack((gx, gy), axis=-1); delta = reflected - center; reflected = center + delta - 2 * np.sum(delta * normal, axis=-1)[..., None] * normal
                valid = (reflected[..., 0] >= 0) & (reflected[..., 0] < w) & (reflected[..., 1] >= 0) & (reflected[..., 1] < h)
                source = map_coordinates(arr, [gy[valid], gx[valid]], order=1)
                mirror = map_coordinates(arr, [reflected[..., 1][valid], reflected[..., 0][valid]], order=1)
                symmetry = float(np.median(np.abs(source - mirror)))
            candidates.append((dark, symmetry, center, theta))
    dark = np.asarray([item[0] for item in candidates]); symmetry = np.asarray([item[1] for item in candidates])
    score = (dark - np.median(dark)) / max(np.std(dark), np.finfo(float).eps)
    if symmetry_weight: score += symmetry_weight * (symmetry - np.median(symmetry)) / max(np.std(symmetry), np.finfo(float).eps)
    best = candidates[int(np.argmin(score))]
    return MidlineProposal(float(best[2][0]), float(best[2][1]), float(best[3]), "native dark corridor" if not symmetry_weight else "native dark corridor + symmetry", float(np.clip(1 - np.std(score) / 5, 0, 1)), coordinate_space="native functional pixel grid")


def fit_fish_midline_consensus(
    local_lines: Mapping[float, MidlineProposal | Mapping[str, float]],
    image_shapes: Mapping[float, tuple[int, int]],
    *,
    sessions: Mapping[float, str] | None = None,
) -> dict:
    """Summarise one fish's local midline suggestions across imaging depth.

    The fish is assumed to keep one overall midline angle during a stack. Its
    position may drift steadily with depth when the fish was mounted at a small
    tilt. The fit is robust: a plane that strongly disagrees with the others is
    excluded from the depth trend. When a recording has separate focus sessions,
    provide ``sessions`` so each session receives its own depth trend. Depth
    values should be physical z positions when available; plane number is
    acceptable only for evenly spaced planes inside one session.

    This function creates a review prior, not an accepted midline.
    """
    depths = np.asarray(sorted(float(depth) for depth in local_lines), dtype=float)
    if len(depths) < 3:
        raise ValueError("at least three local plane suggestions are needed for a fish consensus")
    if set(depths) != set(float(depth) for depth in image_shapes):
        raise ValueError("each local line needs an image shape at the same depth")
    def value(line: MidlineProposal | Mapping[str, float], key: str) -> float:
        return float(getattr(line, key) if isinstance(line, MidlineProposal) else line[key])
    local_angles = np.asarray([value(local_lines[depth], "theta_deg") for depth in depths], dtype=float)
    consensus_angle = _axial_mean(local_angles)
    angle_residuals = _axial_difference(local_angles, consensus_angle)
    angle_mad = float(np.median(np.abs(angle_residuals - np.median(angle_residuals))))
    angle_keep = np.abs(angle_residuals - np.median(angle_residuals)) <= max(3.0 * angle_mad, 2.0)
    if int(angle_keep.sum()) >= 2:
        consensus_angle = _axial_mean(local_angles[angle_keep])
    offsets = np.asarray([
        _line_offset_fraction(
            {"x0": value(local_lines[depth], "x0"), "y0": value(local_lines[depth], "y0")},
            image_shapes[depth], consensus_angle,
        )
        for depth in depths
    ])
    session_by_depth = {float(depth): "all_planes" for depth in depths} if sessions is None else {float(depth): str(sessions[depth]) for depth in depths}
    if set(session_by_depth) != set(depths):
        raise ValueError("each local line needs a recording-session label")
    offset_models, offset_keep = {}, np.zeros(len(depths), dtype=bool)
    for session in sorted(set(session_by_depth.values())):
        mask = np.asarray([session_by_depth[depth] == session for depth in depths])
        if int(mask.sum()) < 2:
            raise ValueError(f"recording session {session!r} needs at least two planes")
        intercept, slope, keep = _robust_line_fit(depths[mask], offsets[mask])
        offset_models[session] = {"offset_intercept_fraction": intercept, "offset_per_depth_fraction": slope, "n_inliers": int(keep.sum())}
        offset_keep[mask] = keep
    return {
        "coordinate_space": "native functional pixel grid",
        "consensus_angle_deg": float(consensus_angle),
        "offset_models": offset_models,
        "n_planes": int(len(depths)),
        "n_angle_inliers": int(angle_keep.sum()),
        "n_offset_inliers": int(offset_keep.sum()),
        "angle_residual_deg": {str(depth): float(residual) for depth, residual in zip(depths, _axial_difference(local_angles, consensus_angle))},
        "offset_residual_fraction": {
            str(depth): float(offset - (offset_models[session_by_depth[depth]]["offset_intercept_fraction"] + offset_models[session_by_depth[depth]]["offset_per_depth_fraction"] * depth))
            for depth, offset in zip(depths, offsets)
        },
        "requires_manual_acceptance": True,
    }


def propose_fish_consensus_midline(
    depth: float,
    image_shape: tuple[int, int],
    consensus: Mapping[str, float],
    *,
    session: str | None = None,
    local_line: MidlineProposal | Mapping[str, float] | None = None,
    permitted_angle_deviation_deg: float = 0.0,
) -> MidlineProposal:
    """Place a fish-level review line at one depth.

    With the default of zero permitted deviation, every plane receives exactly
    the same angle. Supplying a small positive deviation retains local image
    evidence, but limits each plane to that many degrees from the fish-wide
    angle. This makes the fixed and flexible models directly comparable.
    """
    required = {"consensus_angle_deg", "offset_models"}
    if missing := required.difference(consensus):
        raise ValueError(f"fish consensus is missing: {sorted(missing)}")
    if permitted_angle_deviation_deg < 0:
        raise ValueError("permitted angle deviation cannot be negative")
    consensus_theta = float(consensus["consensus_angle_deg"])
    theta = consensus_theta
    if local_line is not None and permitted_angle_deviation_deg:
        local_theta = float(local_line.theta_deg if isinstance(local_line, MidlineProposal) else local_line["theta_deg"])
        theta += float(np.clip(_axial_difference(local_theta, theta), -permitted_angle_deviation_deg, permitted_angle_deviation_deg))
    offset_models = consensus["offset_models"]
    if session is None:
        if len(offset_models) != 1:
            raise ValueError("choose a recording session when the fish has more than one")
        session = next(iter(offset_models))
    if session not in offset_models:
        raise ValueError(f"recording session {session!r} is not present in this fish consensus")
    h, w = image_shape
    model = offset_models[session]
    offset = float(model["offset_intercept_fraction"]) + float(model["offset_per_depth_fraction"]) * float(depth)
    # The flexible version rotates through the same predicted centre; it must
    # not turn a small angle allowance into an unintended position change.
    radians = math.radians(consensus_theta)
    normal = np.array([-math.sin(radians), math.cos(radians)])
    centre = np.array([w / 2, h / 2]) + offset * math.hypot(h, w) * normal
    method = "fish consensus: fixed angle + linear depth shift" if not permitted_angle_deviation_deg else f"fish consensus: ±{permitted_angle_deviation_deg:g}° angle freedom + linear depth shift"
    return MidlineProposal(float(centre[0]), float(centre[1]), _wrap(theta), method, 0.0, coordinate_space="native functional pixel grid")
