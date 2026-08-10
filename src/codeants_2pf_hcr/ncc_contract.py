"""Consume the canonical NCC handoff produced by calcium preprocessing."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd
import tifffile

from .spatial_contract import CANONICAL_XY_FRAME, REGISTRATION_Z_FRAME


NCC_HANDOFF_SCHEMA = "functional_anatomy_ncc_handoff_v1"
NCC_MANIFEST_NAME = "functional_anatomy_qc_manifest.json"


class NCCContractError(RuntimeError):
    """Raised when the upstream NCC handoff is unsafe or incomplete."""


def preprocessing_ncc_manifest_path(fish_dir: str | Path) -> Path:
    return Path(fish_dir) / "03_analysis" / "functional" / "ncc" / NCC_MANIFEST_NAME


def _resolved_path(value: Any, *, label: str) -> Path:
    if value in (None, "", False):
        raise NCCContractError(f"NCC handoff does not declare {label}")
    path = Path(str(value))
    if not path.exists():
        raise FileNotFoundError(f"NCC handoff {label} is missing: {path}")
    return path.resolve()


def load_preprocessing_ncc_handoff(
    manifest_path: str | Path,
    *,
    fish_id: str,
    anatomy_stack_path: str | Path,
    allowed_statuses: Iterable[str] = ("pass_candidate",),
) -> dict[str, Any]:
    """Load and validate a reusable canonical scale/Z/XY placement bundle."""
    path = Path(manifest_path)
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("stage") != "functional_anatomy_ncc_qc" or int(payload.get("version", 0)) < 4:
        raise NCCContractError(f"Unsupported preprocessing NCC manifest: {path}")
    if str(payload.get("fish_id", "")) != str(fish_id):
        raise NCCContractError(
            f"NCC handoff fish mismatch: expected {fish_id}, got {payload.get('fish_id')!r}"
        )
    allowed = {str(value) for value in allowed_statuses}
    status = str(payload.get("status", ""))
    if status not in allowed:
        raise NCCContractError(
            f"NCC handoff status {status!r} does not permit static registration; allowed={sorted(allowed)}"
        )

    handoff = payload.get("downstream_handoff")
    if not isinstance(handoff, dict) or handoff.get("schema") != NCC_HANDOFF_SCHEMA:
        raise NCCContractError(f"NCC manifest has no {NCC_HANDOFF_SCHEMA} contract: {path}")
    if handoff.get("xy_frame") != CANONICAL_XY_FRAME:
        raise NCCContractError(f"NCC handoff has an unknown XY frame: {handoff.get('xy_frame')!r}")
    if handoff.get("z_frame") != REGISTRATION_Z_FRAME:
        raise NCCContractError(f"NCC handoff has an unknown Z frame: {handoff.get('z_frame')!r}")

    declared_anatomy = _resolved_path(payload.get("inputs", {}).get("canonical_anatomy"), label="canonical anatomy")
    selected_anatomy = _resolved_path(anatomy_stack_path, label="selected anatomy")
    if declared_anatomy != selected_anatomy:
        raise NCCContractError(
            f"NCC anatomy mismatch: handoff={declared_anatomy}, selected={selected_anatomy}"
        )

    placement_path = _resolved_path(handoff.get("authoritative_placement_table"), label="placement table")
    profile_path = _resolved_path(handoff.get("authoritative_anchor_profiles"), label="anchor profiles")
    reference_dir = _resolved_path(handoff.get("functional_reference_directory"), label="functional references")
    placements = pd.read_csv(placement_path)
    profiles = pd.read_csv(profile_path)
    required_placement = {
        "fish_id",
        "plane_index",
        "plane_label",
        "scale",
        "best_z",
        "best_z_subslice",
        "max_ncc",
        "placement_x",
        "placement_y",
        "reference_path",
    }
    required_profiles = {"fish_id", "plane_index", "anatomy_z", "ncc"}
    missing_placement = sorted(required_placement - set(placements.columns))
    missing_profiles = sorted(required_profiles - set(profiles.columns))
    if missing_placement or missing_profiles:
        raise NCCContractError(
            f"Incomplete NCC handoff tables: placement_missing={missing_placement}, profile_missing={missing_profiles}"
        )
    if set(placements["fish_id"].astype(str)) != {str(fish_id)}:
        raise NCCContractError("Placement table contains a different or mixed fish identity")
    if set(profiles["fish_id"].astype(str)) != {str(fish_id)}:
        raise NCCContractError("Anchor-profile table contains a different or mixed fish identity")
    if placements["plane_index"].duplicated().any():
        raise NCCContractError("Placement table contains duplicate plane indices")
    return {
        "manifest_path": path.resolve(),
        "manifest": payload,
        "placement_path": placement_path,
        "profile_path": profile_path,
        "reference_dir": reference_dir,
        "placements": placements,
        "profiles": profiles,
    }


def plane_refs_from_preprocessing_ncc_handoff(
    handoff: dict[str, Any],
    *,
    anatomy_z_count: int,
    selected_plane_indices: Iterable[int] | None = None,
) -> list[dict[str, Any]]:
    """Reconstruct registration-ready plane references without repeating NCC."""
    from .spatial import norm01, scale_image

    placements = handoff["placements"].copy()
    profiles = handoff["profiles"].copy()
    selected = None if selected_plane_indices is None else {int(value) for value in selected_plane_indices}
    available = {int(value) for value in placements["plane_index"]}
    if selected is not None:
        missing = sorted(selected - available)
        if missing:
            raise NCCContractError(f"Requested planes are absent from the NCC handoff: {missing}")
        placements = placements[placements["plane_index"].isin(selected)]

    plane_refs: list[dict[str, Any]] = []
    for row in placements.sort_values("plane_index").to_dict(orient="records"):
        plane_index = int(row["plane_index"])
        reference_path = _resolved_path(row["reference_path"], label=f"plane {plane_index} reference")
        reference = np.asarray(tifffile.imread(reference_path), dtype=np.float32)
        if reference.ndim != 2:
            raise NCCContractError(f"Plane {plane_index} reference is not 2D: {reference.shape}")
        profile = profiles[profiles["plane_index"].astype(int) == plane_index].sort_values("anatomy_z")
        expected_z = np.arange(int(anatomy_z_count), dtype=int)
        observed_z = profile["anatomy_z"].to_numpy(dtype=int)
        if not np.array_equal(observed_z, expected_z):
            raise NCCContractError(
                f"Plane {plane_index} NCC profile does not cover anatomy Z exactly: "
                f"observed={observed_z.tolist()}, expected={expected_z.tolist()}"
            )
        scores = profile["ncc"].to_numpy(dtype=np.float32)
        best_z = int(row["best_z"])
        if best_z != int(np.nanargmax(scores)):
            raise NCCContractError(
                f"Plane {plane_index} best Z disagrees with its authoritative profile: "
                f"table={best_z}, profile={int(np.nanargmax(scores))}"
            )
        scale = float(row["scale"])
        scaled = scale_image(reference, scale)
        plane_refs.append(
            {
                "label": str(row["plane_label"]),
                "index": plane_index,
                "ref2d_raw": reference,
                "ref2d": norm01(reference),
                "reference_raw_path": str(reference_path),
                "reference_norm_path": None,
                "reference_selection": str(row.get("reference_selection", "pooled_post_block0")),
                "scale": scale,
                "best_z": best_z,
                "best_z_subslice": float(row["best_z_subslice"]),
                "ncc_scores": scores,
                "ref_match": scaled,
                "ref_shape": tuple(int(value) for value in reference.shape),
                "ref_scaled_shape": tuple(int(value) for value in scaled.shape),
                "ncc_xy": {
                    "x0": int(row["placement_x"]),
                    "y0": int(row["placement_y"]),
                    "score": float(row["max_ncc"]),
                    "source": "preprocessing_ncc_handoff",
                },
                "ncc_handoff_manifest_path": str(handoff["manifest_path"]),
            }
        )
    if not plane_refs:
        raise NCCContractError("NCC handoff selected no functional planes")
    return plane_refs


__all__ = [
    "NCCContractError",
    "NCC_HANDOFF_SCHEMA",
    "NCC_MANIFEST_NAME",
    "load_preprocessing_ncc_handoff",
    "plane_refs_from_preprocessing_ncc_handoff",
    "preprocessing_ncc_manifest_path",
]
