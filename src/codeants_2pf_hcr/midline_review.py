"""Validation for manually accepted anatomy-space midline reviews.

This is deliberately separate from legacy midline parameter bundles.  A review
sidecar is the only provenance that may unlock laterality interpretation.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Sequence

import pandas as pd

import numpy as np


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()



def validate_logbook_midline_gate(
    logbook_path: str | Path, *, fish_id: str,
    proposal_csv_sha256: str, proposal_png_sha256: str,
) -> Path:
    """Require an exact fish-specific, proposal-hash-bound logbook decision."""
    path = Path(logbook_path).expanduser().resolve()
    if not path.is_file():
        raise ValueError(f"Midline logbook decision is missing: {path}")
    text = path.read_text(encoding="utf-8")
    sections = text.split("---", 2)
    if len(sections) < 3 or sections[0].strip():
        raise ValueError("Midline logbook decision needs YAML frontmatter")
    fields: dict[str, str] = {}
    for line in sections[1].splitlines():
        if ":" not in line or line.startswith((" ", "	")):
            continue
        key, value = line.split(":", 1)
        if key in fields:
            raise ValueError(f"Duplicate midline logbook field: {key}")
        fields[key] = value.strip().strip(chr(34)).strip(chr(39))
    expected = {
        "fish_id": str(fish_id), "midline_gate": "accepted",
        "midline_proposal_csv_sha256": str(proposal_csv_sha256),
        "midline_proposal_png_sha256": str(proposal_png_sha256),
    }
    for key, value in expected.items():
        if fields.get(key) != value:
            raise ValueError(f"Midline logbook gate {key} does not match the reviewed proposal")
    return path


def load_accepted_anatomy_midline_context(
    sidecar_path: str | Path, *, fish_id: str, required_planes: Sequence[int] | None = None
) -> dict:
    """Load a complete, accepted, hash-bound anatomy-grid review sidecar.

    Proposal files and legacy ``midline_params_func_ref.json`` bundles do not
    satisfy this contract.
    """
    path = Path(sidecar_path).expanduser().resolve()
    if not path.is_file():
        raise FileNotFoundError(f"Accepted midline review sidecar is missing: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    schema = payload.get("schema")
    if schema not in {"codeants_midline_annotation_review_v1", "codeants_anatomy_midline_materialization_v1"} or payload.get("accepted") is not True:
        raise ValueError("Laterality QC requires an explicitly accepted anatomy-midline artifact")
    if schema == "codeants_anatomy_midline_materialization_v1":
        artifacts_by_name = {Path(str(row.get("path", ""))).name: row for row in payload.get("source_artifacts", []) if isinstance(row, dict)}
        csv_hash = artifacts_by_name.get("anatomy_midline_proposals.csv", {}).get("sha256")
        png_hash = artifacts_by_name.get("anatomy_midline_proposals.png", {}).get("sha256")
        validate_logbook_midline_gate(
            payload.get("logbook_decision_path", ""), fish_id=fish_id,
            proposal_csv_sha256=csv_hash, proposal_png_sha256=png_hash,
        )
    if str(payload.get("fish_id", "")) != str(fish_id):
        raise ValueError(f"Midline sidecar fish_id does not match {fish_id}")
    if str(payload.get("coordinate_space", "")).strip().lower() != "anatomy pixel grid":
        raise ValueError("Laterality QC requires an accepted anatomy pixel grid midline; native-functional lines cannot be reused")
    artifacts = payload.get("source_artifacts")
    if not isinstance(artifacts, list) or not artifacts or not all(isinstance(row, dict) and row.get("path") and row.get("sha256") for row in artifacts):
        raise ValueError("Accepted midline sidecar is not hash-bound to review artifacts")
    for artifact in artifacts:
        artifact_path = Path(str(artifact["path"])).expanduser()
        if not artifact_path.is_file():
            raise ValueError(f"Accepted midline sidecar review artifact is missing: {artifact_path}")
        if _sha256(artifact_path) != str(artifact["sha256"]):
            raise ValueError(f"Accepted midline sidecar review artifact hash changed: {artifact_path}")
    lines = payload.get("lines_by_plane")
    if not isinstance(lines, dict) or not lines:
        raise ValueError("Accepted midline sidecar has no per-plane lines")
    observed = {int(key) for key in lines}
    expected = set(map(int, required_planes or observed))
    if observed != expected:
        raise ValueError(f"Accepted midline planes {sorted(observed)} do not match ROI planes {sorted(expected)}")
    for plane, line in lines.items():
        if not isinstance(line, dict) or not all(np.isfinite(float(line.get(key, np.nan))) for key in ("x0", "y0", "theta_deg")):
            raise ValueError(f"Accepted midline line for plane {plane} is incomplete")
    return {
        "bundle": {"fish_id": fish_id, "midline_space": "anat", "per_plane": lines, "manual": {"uncertain_band_px": 1.0}},
        "bundle_path": path,
        "midline_space": "anat",
        "review": payload,
        "sidecar_sha256": _sha256(path),
    }


def project_native_midline_to_anatomy(
    plane_ref: dict, *, x0: float, y0: float, theta_deg: float,
    anatomy_shape: tuple[int, int], n_samples: int = 41,
) -> dict[str, float | int]:
    """Project a native-functional line through the saved scale/NCC/ANTs chain."""
    from .matching import resolve_plane_transform, transform_points_between_spaces

    native_shape = tuple(int(v) for v in plane_ref["ref_shape"])
    scaled_shape = tuple(int(v) for v in plane_ref["ref_scaled_shape"])
    if len(native_shape) != 2 or len(scaled_shape) != 2:
        raise ValueError("Midline projection needs 2-D native and scaled reference shapes")
    theta = np.deg2rad(float(theta_deg))
    direction = np.array([np.cos(theta), np.sin(theta)], dtype=float)
    offsets = np.linspace(-0.35 * min(native_shape), 0.35 * min(native_shape), n_samples)
    native_x = float(x0) + offsets * direction[0]
    native_y = float(y0) + offsets * direction[1]
    ncc = plane_ref.get("ncc_xy")
    transform = resolve_plane_transform(plane_ref)
    if not isinstance(ncc, dict) or transform is None:
        raise ValueError("Midline projection needs the selected NCC placement and transform")
    x_canvas = native_x * scaled_shape[1] / native_shape[1] + float(ncc["x0"])
    y_canvas = native_y * scaled_shape[0] / native_shape[0] + float(ncc["y0"])
    if isinstance(transform, dict) and transform.get("type") == "ants_transformlist":
        paths = [Path(path) for path in transform.get("transformlist", [])]
        if not paths or not all(path.is_file() for path in paths):
            raise FileNotFoundError("Selected ANTs midline transform files are missing")
        moving_spacing = tuple(float(v) for v in transform["moving_spacing"])
        fixed_spacing = tuple(float(v) for v in transform["fixed_spacing"])
        x_canvas = x_canvas * moving_spacing[0]
        y_canvas = y_canvas * moving_spacing[1]
        fixed_x, fixed_y = transform_points_between_spaces(x_canvas, y_canvas, transform)
        fixed_x = fixed_x / fixed_spacing[0]
        fixed_y = fixed_y / fixed_spacing[1]
    else:
        fixed_x, fixed_y = transform_points_between_spaces(x_canvas, y_canvas, transform)
    points = np.column_stack([fixed_x, fixed_y])
    if not np.isfinite(points).all():
        raise ValueError("Projected midline has non-finite anatomy coordinates")
    centre = points.mean(axis=0)
    _, _, vt = np.linalg.svd(points - centre, full_matrices=False)
    vector = vt[0]
    if np.dot(vector, direction) < 0:
        vector = -vector
    residual = np.abs((points - centre) @ np.array([-vector[1], vector[0]]))
    rmse = float(np.sqrt(np.mean(residual**2)))
    if rmse > 2.0:
        raise ValueError(f"Projected anatomy midline is not adequately straight: RMSE {rmse:.3g} px")
    if not (0 <= centre[0] < anatomy_shape[1] and 0 <= centre[1] < anatomy_shape[0]):
        raise ValueError("Projected anatomy midline centre falls outside the anatomy grid")
    return {
        "plane_idx": int(plane_ref["index"]),
        "best_z": int(plane_ref["best_z"]),
        "x0": float(centre[0]), "y0": float(centre[1]),
        "theta_deg": float(np.rad2deg(np.arctan2(vector[1], vector[0]))),
        "fit_rmse_px": rmse, "n_sample_points": int(n_samples),
        "requires_human_acceptance": True,
    }


def build_anatomy_midline_proposal_table(
    plane_refs: list[dict], native_proposals: pd.DataFrame | np.ndarray, *,
    anatomy_shape: tuple[int, int],
) -> pd.DataFrame:
    """Require exact plane coverage and project saved native fixed-consensus lines."""
    proposals = pd.DataFrame(native_proposals).copy()
    required = {"plane_idx", "fixed_x0", "fixed_y0", "fixed_theta_deg"}
    if not required.issubset(proposals):
        raise ValueError(f"Native midline proposals lack {sorted(required - set(proposals))}")
    if proposals["plane_idx"].duplicated().any():
        raise ValueError("Native midline proposals have duplicate planes")
    refs = {int(ref["index"]): ref for ref in plane_refs}
    rows = {int(row.plane_idx): row for row in proposals.itertuples(index=False)}
    if set(rows) != set(refs):
        raise ValueError("Native midline proposal planes do not match registered planes")
    return pd.DataFrame([
        project_native_midline_to_anatomy(
            refs[plane], x0=rows[plane].fixed_x0, y0=rows[plane].fixed_y0,
            theta_deg=rows[plane].fixed_theta_deg, anatomy_shape=anatomy_shape,
        )
        for plane in sorted(refs)
    ])


load_anatomy_midline_context = load_accepted_anatomy_midline_context

__all__ = ["load_anatomy_midline_context", "load_accepted_anatomy_midline_context", "project_native_midline_to_anatomy", "build_anatomy_midline_proposal_table", "validate_logbook_midline_gate"]
