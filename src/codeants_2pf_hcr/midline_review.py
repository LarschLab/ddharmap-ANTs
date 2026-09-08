"""Validation for manually accepted anatomy-space midline reviews.

This is deliberately separate from legacy midline parameter bundles.  A review
sidecar is the only provenance that may unlock laterality interpretation.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Sequence

import numpy as np


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


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
    if payload.get("schema") != "codeants_midline_annotation_review_v1" or payload.get("accepted") is not True:
        raise ValueError("Laterality QC requires an explicitly accepted midline annotation sidecar")
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


__all__ = ["load_accepted_anatomy_midline_context"]
