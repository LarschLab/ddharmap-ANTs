"""Shared read-only infrastructure for single-fish QC notebooks.

Notebook cells should contain only explicit configuration and calls into this
module.  Analysis artifacts are read-only; the sole writer in this module
creates review sidecars beneath a selected stage's ``reviews`` directory.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
from typing import Any, Mapping, Sequence

import pandas as pd

from .pipeline import (
    GRANULAR_PREPROCESSING_STAGE_NAMES,
    PIPELINE_STAGE_ORDER,
    SingleFishPipelineConfig,
    cellpose_stage_manifest_path,
    resolve_pipeline_paths,
    stage_manifest_path,
)


FISH_ID_PATTERN = re.compile(r"^[A-Z][0-9]{3}_f[0-9]{2}$")
QC_STAGE_NAMES: tuple[str, ...] = tuple(
    dict.fromkeys((*GRANULAR_PREPROCESSING_STAGE_NAMES, *PIPELINE_STAGE_ORDER))
)


@dataclass(frozen=True)
class QCNotebookConfig:
    """Explicit fish and staged-output selection for a QC notebook."""

    fish_id: str
    local_root: Path | str
    pipeline_root: Path | str | None = None


@dataclass(frozen=True)
class QCNotebookContext:
    """Resolved, fish-scoped paths and provenance for read-only QC."""

    fish_id: str
    local_root: Path
    fish_dir: Path
    pipeline_root: Path
    stage_paths: Mapping[str, Path]
    manifest_paths: Mapping[str, Path]
    provenance: Mapping[str, Any]


def _resolved(path: Path | str) -> Path:
    return Path(path).expanduser().resolve(strict=False)


def _is_within(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
    except ValueError:
        return False
    return True


def _read_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"Could not parse JSON object at {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"Expected a JSON object at {path}, got {type(payload).__name__}")
    return payload


def _manifest_matches_pipeline_root(payload: Mapping[str, Any], pipeline_root: Path) -> bool:
    parameters = payload.get("parameters")
    if not isinstance(parameters, Mapping):
        return True
    declared = parameters.get("pipeline_root")
    return declared is None or _resolved(str(declared)) == pipeline_root


def _validate_manifest_identity(
    payload: Mapping[str, Any],
    *,
    path: Path,
    fish_id: str,
    pipeline_root: Path,
    require_pipeline_root: bool,
) -> None:
    manifest_fish = payload.get("fish_id")
    if manifest_fish is not None and str(manifest_fish) != fish_id:
        raise ValueError(
            f"Manifest fish_id {manifest_fish!r} at {path} does not match requested fish {fish_id!r}"
        )
    if require_pipeline_root and not _manifest_matches_pipeline_root(payload, pipeline_root):
        declared = (payload.get("parameters") or {}).get("pipeline_root")
        raise ValueError(
            f"Manifest pipeline_root {declared!r} at {path} does not match selected root {pipeline_root}"
        )


def resolve_qc_notebook_context(config: QCNotebookConfig) -> QCNotebookContext:
    """Resolve and validate one unambiguous fish/pipeline selection.

    ``local_root`` must directly contain the fish folder.  An explicit
    ``pipeline_root`` may be outside that folder (for isolated staged runs),
    but every derived stage path remains contained by it.  Manifests stored
    inside that root are treated as identity evidence and must agree.
    """

    fish_id = str(config.fish_id).strip()
    if not FISH_ID_PATTERN.fullmatch(fish_id):
        raise ValueError(f"Invalid fish_id {config.fish_id!r}; expected e.g. L765_f04")

    local_root = _resolved(config.local_root)
    if not local_root.is_dir():
        raise FileNotFoundError(f"Local root does not exist or is not a directory: {local_root}")
    fish_dir = local_root / fish_id
    if not fish_dir.is_dir():
        raise FileNotFoundError(
            f"Fish folder must exist directly below local_root: expected {fish_dir}"
        )

    pipeline_config = SingleFishPipelineConfig(
        fish_id=fish_id,
        local_root=local_root,
        pipeline_root=config.pipeline_root,
    )
    paths = resolve_pipeline_paths(pipeline_config)
    pipeline_root = _resolved(paths.pipeline_root)
    stage_paths = {name: pipeline_root / name for name in QC_STAGE_NAMES}
    for stage_name, stage_path in stage_paths.items():
        if not _is_within(stage_path, pipeline_root):
            raise ValueError(f"Resolved stage {stage_name!r} escapes pipeline root: {stage_path}")

    manifest_paths: dict[str, Path] = {}
    for stage_name in QC_STAGE_NAMES:
        if stage_name in GRANULAR_PREPROCESSING_STAGE_NAMES:
            manifest_path = cellpose_stage_manifest_path(paths, stage_name)
        else:
            manifest_path = stage_manifest_path(paths, stage_name)
        manifest_paths[stage_name] = _resolved(manifest_path)

    # A staged root may contain stage-local manifests in addition to the
    # canonical fish manifest directory.  Any such identity evidence must be
    # internally consistent with the selected fish and root.
    root_manifests: list[Path] = []
    if pipeline_root.is_dir():
        root_manifests = sorted(
            path for path in pipeline_root.rglob("*manifest.json") if path.is_file()
        )
    for manifest_path in root_manifests:
        payload = _read_json_object(manifest_path)
        _validate_manifest_identity(
            payload,
            path=manifest_path,
            fish_id=fish_id,
            pipeline_root=pipeline_root,
            require_pipeline_root=True,
        )

    matching_manifests: list[str] = []
    for manifest_path in dict.fromkeys(manifest_paths.values()):
        if not manifest_path.is_file():
            continue
        payload = _read_json_object(manifest_path)
        if not _manifest_matches_pipeline_root(payload, pipeline_root):
            continue
        _validate_manifest_identity(
            payload,
            path=manifest_path,
            fish_id=fish_id,
            pipeline_root=pipeline_root,
            require_pipeline_root=False,
        )
        matching_manifests.append(str(manifest_path))

    provenance = {
        "fish_id": fish_id,
        "local_root": str(local_root),
        "fish_dir": str(fish_dir),
        "pipeline_root": str(pipeline_root),
        "pipeline_root_explicit": config.pipeline_root is not None,
        "pipeline_root_exists": pipeline_root.is_dir(),
        "matching_manifest_paths": tuple(matching_manifests),
    }
    return QCNotebookContext(
        fish_id=fish_id,
        local_root=local_root,
        fish_dir=fish_dir,
        pipeline_root=pipeline_root,
        stage_paths=stage_paths,
        manifest_paths=manifest_paths,
        provenance=provenance,
    )


def _artifact_records_from_manifest(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    outputs = payload.get("outputs")
    if not isinstance(outputs, list):
        return []
    records: list[dict[str, Any]] = []
    for output in outputs:
        if not isinstance(output, Mapping) or not output.get("path"):
            continue
        artifact_path = _resolved(str(output["path"]))
        records.append(
            {
                "path": str(artifact_path),
                "exists": artifact_path.exists(),
                "required": bool(output.get("required", False)),
                "label": str(output.get("label", artifact_path.name)),
            }
        )
    return records


def build_qc_stage_inventory(context: QCNotebookContext) -> pd.DataFrame:
    """Return one read-only inventory row for every declared pipeline stage."""

    rows: list[dict[str, Any]] = []
    for stage_name in QC_STAGE_NAMES:
        stage_root = context.stage_paths[stage_name]
        manifest_path = context.manifest_paths[stage_name]
        manifest_payload: dict[str, Any] | None = None
        manifest_matches = False
        if manifest_path.is_file():
            candidate = _read_json_object(manifest_path)
            manifest_matches = _manifest_matches_pipeline_root(candidate, context.pipeline_root)
            if manifest_matches:
                _validate_manifest_identity(
                    candidate,
                    path=manifest_path,
                    fish_id=context.fish_id,
                    pipeline_root=context.pipeline_root,
                    require_pipeline_root=False,
                )
                manifest_payload = candidate
        artifacts = _artifact_records_from_manifest(manifest_payload or {})
        if not artifacts and stage_root.exists():
            artifacts = [
                {
                    "path": str(path),
                    "exists": True,
                    "required": False,
                    "label": path.name,
                }
                for path in sorted(stage_root.rglob("*"))
                if path.is_file() and not path.name.startswith("._") and path.name != ".DS_Store"
            ]
        required = [record for record in artifacts if record["required"]]
        rows.append(
            {
                "stage_name": stage_name,
                "stage_root": str(stage_root),
                "stage_root_exists": stage_root.is_dir(),
                "manifest_path": str(manifest_path),
                "manifest_exists": manifest_path.is_file() and manifest_matches,
                "manifest_status": None if manifest_payload is None else manifest_payload.get("status"),
                "artifact_count": len(artifacts),
                "artifact_exists_count": sum(bool(record["exists"]) for record in artifacts),
                "required_artifacts_exist": bool(required) and all(record["exists"] for record in required),
                "artifact_paths": tuple(record["path"] for record in artifacts),
            }
        )
    return pd.DataFrame.from_records(rows)


def load_qc_csv(path: Path | str, *, expected_fish_id: str | None = None, **kwargs: Any) -> pd.DataFrame:
    """Load a CSV without mutation and optionally enforce its fish identity."""

    csv_path = _resolved(path)
    if not csv_path.is_file():
        raise FileNotFoundError(f"QC CSV does not exist: {csv_path}")
    frame = pd.read_csv(csv_path, **kwargs)
    if expected_fish_id is not None and "fish_id" in frame.columns:
        observed = set(frame["fish_id"].dropna().astype(str).unique())
        if observed and observed != {str(expected_fish_id)}:
            raise ValueError(
                f"QC CSV {csv_path} contains fish IDs {sorted(observed)}, expected {expected_fish_id!r}"
            )
    return frame


def load_qc_json(path: Path | str) -> dict[str, Any]:
    """Load a JSON object from an existing artifact."""

    json_path = _resolved(path)
    if not json_path.is_file():
        raise FileNotFoundError(f"QC JSON does not exist: {json_path}")
    return _read_json_object(json_path)


def load_qc_image(path: Path | str) -> Any:
    """Load an image into memory, closing the source file immediately."""

    image_path = _resolved(path)
    if not image_path.is_file():
        raise FileNotFoundError(f"QC image does not exist: {image_path}")
    try:
        from PIL import Image
    except ImportError as exc:  # pragma: no cover - package runtime includes Pillow
        raise ImportError("Loading QC images requires Pillow") from exc
    with Image.open(image_path) as image:
        return image.copy()


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _review_timestamp(value: datetime | str | None) -> tuple[datetime, str]:
    if value is None:
        moment = datetime.now(timezone.utc)
    elif isinstance(value, datetime):
        moment = value
    else:
        try:
            moment = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError(f"Invalid review timestamp {value!r}; expected ISO-8601") from exc
    if moment.tzinfo is None:
        raise ValueError("Review timestamp must include a timezone")
    moment = moment.astimezone(timezone.utc)
    filename_stamp = moment.strftime("%Y%m%dT%H%M%S.%fZ")
    return moment, filename_stamp


def write_qc_review_record(
    context: QCNotebookContext,
    *,
    stage_name: str,
    decision: str,
    reviewer: str,
    reviewed_artifacts: Sequence[Path | str],
    notes: str | None = None,
    timestamp: datetime | str | None = None,
) -> Path:
    """Write a non-promoting, hash-bound review sidecar for one stage."""

    if not FISH_ID_PATTERN.fullmatch(str(context.fish_id)):
        raise ValueError(f"Review context has invalid fish_id {context.fish_id!r}")
    expected_fish_dir = _resolved(context.local_root) / context.fish_id
    if _resolved(context.fish_dir) != expected_fish_dir:
        raise ValueError(
            "Review context fish_dir is ambiguous: "
            f"expected {expected_fish_dir}, got {_resolved(context.fish_dir)}"
        )
    if stage_name not in context.stage_paths:
        raise ValueError(f"Unknown or ambiguous QC stage {stage_name!r}")
    decision_value = str(decision).strip()
    reviewer_value = str(reviewer).strip()
    if not decision_value:
        raise ValueError("decision must be non-empty")
    if not reviewer_value:
        raise ValueError("reviewer must be non-empty")
    if not reviewed_artifacts:
        raise ValueError("At least one reviewed artifact is required")

    stage_root = _resolved(context.stage_paths[stage_name])
    if not _is_within(stage_root, context.pipeline_root):
        raise ValueError(f"Stage root escapes selected pipeline root: {stage_root}")
    artifacts: list[dict[str, Any]] = []
    seen: set[Path] = set()
    for raw_path in reviewed_artifacts:
        artifact_path = _resolved(raw_path)
        if artifact_path in seen:
            continue
        seen.add(artifact_path)
        if not artifact_path.is_file():
            raise FileNotFoundError(f"Reviewed artifact is not an existing file: {artifact_path}")
        if not _is_within(artifact_path, stage_root):
            raise ValueError(
                f"Reviewed artifact must belong to stage {stage_name!r}: {artifact_path}"
            )
        if _is_within(artifact_path, stage_root / "reviews"):
            raise ValueError("A review sidecar cannot itself be a reviewed analysis artifact")
        stat = artifact_path.stat()
        artifacts.append(
            {
                "path": str(artifact_path),
                "relative_to_stage_root": str(artifact_path.relative_to(stage_root)),
                "sha256": _sha256(artifact_path),
                "size_bytes": stat.st_size,
                "mtime_ns": stat.st_mtime_ns,
            }
        )

    moment, filename_stamp = _review_timestamp(timestamp)
    review_dir = stage_root / "reviews"
    if not _is_within(review_dir, stage_root):
        raise ValueError(f"Review directory escapes stage root: {review_dir}")
    review_dir.mkdir(parents=True, exist_ok=True)
    review_path = review_dir / f"review_{filename_stamp}.json"
    payload = {
        "review_schema": "codeants_single_fish_qc_review_v1",
        "fish_id": context.fish_id,
        "stage_name": stage_name,
        "pipeline_root": str(context.pipeline_root),
        "stage_root": str(stage_root),
        "reviewed_at": moment.isoformat().replace("+00:00", "Z"),
        "reviewer": reviewer_value,
        "decision": decision_value,
        "notes": None if notes is None else str(notes),
        "reviewed_artifacts": artifacts,
        "promoted_outputs": False,
    }
    try:
        with review_path.open("x", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
            handle.write("\n")
    except FileExistsError as exc:
        raise FileExistsError(f"Refusing to overwrite existing review record: {review_path}") from exc
    return review_path


__all__ = [
    "QCNotebookConfig",
    "QCNotebookContext",
    "QC_STAGE_NAMES",
    "build_qc_stage_inventory",
    "load_qc_csv",
    "load_qc_image",
    "load_qc_json",
    "resolve_qc_notebook_context",
    "write_qc_review_record",
]
