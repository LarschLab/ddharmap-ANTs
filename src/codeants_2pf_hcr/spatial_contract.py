"""Read the upstream canonical spatial-preprocessing contract."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable


SPATIAL_MANIFEST_NAME = "spatial_preprocessing_manifest.json"
CANONICAL_XY_FRAME = "codeants_2p_canonical_xy_v1"
REGISTRATION_Z_FRAME = "codeants_confocal_registration_z_v1"
LEGACY_ACQUISITION_XY_FRAME = "two_photon_acquisition_xy"


class SpatialFrameError(RuntimeError):
    """Raised when a spatial frame is absent, unknown, or contradictory."""


def spatial_manifest_path(fish_dir: str | Path) -> Path:
    return Path(fish_dir) / "02_reg" / "00_preprocessing" / SPATIAL_MANIFEST_NAME


def load_spatial_manifest(fish_dir: str | Path, *, required: bool = False) -> dict[str, Any] | None:
    path = spatial_manifest_path(fish_dir)
    if not path.exists():
        if required:
            raise SpatialFrameError(f"Canonical spatial preprocessing manifest is missing: {path}")
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("stage") != "canonical_spatial_preprocessing" or payload.get("status") != "complete":
        raise SpatialFrameError(f"Unknown or incomplete spatial preprocessing manifest: {path}")
    frames = payload.get("coordinate_frames", {})
    if frames.get("canonical_functional_xy") != CANONICAL_XY_FRAME:
        raise SpatialFrameError(f"Unknown canonical functional XY frame in {path}")
    if frames.get("canonical_anatomy_xy") != CANONICAL_XY_FRAME:
        raise SpatialFrameError(f"Functional/anatomy canonical XY frames disagree in {path}")
    if frames.get("canonical_anatomy_z") != REGISTRATION_Z_FRAME:
        raise SpatialFrameError(f"Unknown canonical anatomy Z frame in {path}")
    return payload


def manifest_polarity(fish_dir: str | Path) -> tuple[str | None, str]:
    payload = load_spatial_manifest(fish_dir, required=False)
    if payload is None:
        return None, "missing spatial preprocessing manifest"
    value = str(payload.get("polarity", {}).get("value", "")).strip().lower()
    if value not in {"north", "south"}:
        raise SpatialFrameError(f"Canonical manifest has invalid polarity for {Path(fish_dir).name}: {value!r}")
    return value, f"{spatial_manifest_path(fish_dir).name}:polarity"


def _declared_paths(payload: dict[str, Any], key: str) -> set[Path]:
    if key == "functional":
        values: Iterable[Any] = [
            *payload.get("functional_planes", []),
            *payload.get("motion_corrected_movies", []),
        ]
        return {
            Path(str(item["output_path"])).resolve()
            for item in values
            if isinstance(item, dict) and item.get("output_path")
        }
    anatomy = payload.get("anatomy", {})
    return {Path(str(anatomy["output_path"])).resolve()} if isinstance(anatomy, dict) and anatomy.get("output_path") else set()


def path_is_canonical(path: str | Path, fish_dir: str | Path, *, kind: str) -> bool:
    payload = load_spatial_manifest(fish_dir, required=True)
    assert payload is not None
    return Path(path).resolve() in _declared_paths(payload, kind)


def fish_dir_from_product_path(path: str | Path) -> Path | None:
    target = Path(path).resolve()
    parts = target.parts
    indices = [parts.index(value) for value in ("02_reg", "03_analysis") if value in parts]
    if not indices:
        return None
    index = min(indices)
    return Path(*parts[:index])


def declared_xy_frame(path: str | Path, *, kind: str) -> str | None:
    fish_dir = fish_dir_from_product_path(path)
    if fish_dir is None or not spatial_manifest_path(fish_dir).exists():
        return None
    return CANONICAL_XY_FRAME if path_is_canonical(path, fish_dir, kind=kind) else None


def canonical_anatomy_path(fish_dir: str | Path) -> Path:
    payload = load_spatial_manifest(fish_dir, required=True)
    assert payload is not None
    paths = _declared_paths(payload, "anatomy")
    if len(paths) != 1:
        raise SpatialFrameError(f"Canonical manifest does not declare exactly one anatomy output for {fish_dir}")
    path = next(iter(paths))
    if not path.exists():
        raise FileNotFoundError(f"Canonical anatomy output is missing: {path}")
    return path


__all__ = [
    "CANONICAL_XY_FRAME",
    "LEGACY_ACQUISITION_XY_FRAME",
    "REGISTRATION_Z_FRAME",
    "SPATIAL_MANIFEST_NAME",
    "SpatialFrameError",
    "canonical_anatomy_path",
    "declared_xy_frame",
    "fish_dir_from_product_path",
    "load_spatial_manifest",
    "manifest_polarity",
    "path_is_canonical",
    "spatial_manifest_path",
]
