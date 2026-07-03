"""Read-only staged single-fish pipeline contracts and input audits."""

from __future__ import annotations

import csv
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import importlib.util
import json
import math
from pathlib import Path
import re
import shutil
import struct
import tempfile
from typing import Any


PIPELINE_MANIFEST_VERSION = "0.1"

PIPELINE_STAGE_ORDER: tuple[str, ...] = (
    "audit-inputs",
    "preprocess-functional",
    "preprocess-anatomy",
    "preprocess-hcr",
    "register-functional-to-anatomy",
    "register-hcr-to-anatomy",
    "match-roi-to-anatomy",
    "assign-hcr-identity",
    "score-activity-bpi",
    "export-canonical-tables",
    "make-qa-report",
    "make-figures",
)

REQUIRED_CSV_COLUMNS: dict[str, tuple[str, ...]] = {
    "tforms_by_plane.csv": ("plane_index", "label", "best_z"),
    "functional_roi_activity_identity.csv": (
        "plane_idx",
        "func_label",
        "selected_anat_label",
        "has_unique_anat_match",
        "anat_label",
        "identity_label",
        "has_identity_assigned",
        "suite2p_is_cell",
        "response_is_active",
        "response_class",
        "response_summary_class",
        "bpi",
        "bpi_category",
    ),
    "functional_roi_activity_bpi_cells.csv": (
        "plane_idx",
        "func_label",
        "anat_label",
        "response_is_active",
        "response_class",
        "response_summary_class",
        "bpi",
        "bpi_category",
    ),
    "functional_roi_activity_bpi_summary.csv": (
        "response_summary_class",
        "bpi_category",
        "n_rois",
    ),
    "hcr_activity_status.csv": (
        "gene",
        "anat_label",
        "functional_status",
        "response_is_active",
        "response_class",
        "response_summary_class",
        "selected_for_trace_export",
    ),
    "conf_to_func_pairs.csv": (
        "gene",
        "anat_label",
        "func_label",
        "plane",
        "response_is_active",
        "response_class",
        "response_summary_class",
        "selection_rule",
        "is_selected_for_analysis",
    ),
    "hcr_func_candidates.csv": (
        "gene",
        "anat_label",
        "func_label",
        "plane_idx",
        "overlap_px_func_anat",
        "dist_func_anat_um",
        "response_is_active",
        "candidate_response_bucket",
    ),
}

BOOLEAN_DOMAIN = {"True", "False", "true", "false", "1", "0", ""}

RESPONSE_CLASS_DOMAIN = {
    "",
    "bout-responsive",
    "continuous-responsive",
    "both-responsive",
    "non-responsive",
    "response unavailable",
    "low activity",
}

RESPONSE_SUMMARY_DOMAIN = {
    "",
    "Responsive neurons",
    "Non-responsive neurons",
    "Response unavailable",
    "Low activity",
}

BPI_CATEGORY_DOMAIN = {
    "",
    "bout-responsive",
    "continuous-responsive",
    "both-responsive",
    "weak-response",
    "response unavailable",
    "low activity",
}

POST_PREPROCESSING_STAGE_NAMES: tuple[str, ...] = (
    "assign-hcr-identity",
    "score-activity-bpi",
    "export-canonical-tables",
    "make-qa-report",
    "make-figures",
)

GRANULAR_PREPROCESSING_STAGE_NAMES: tuple[str, ...] = (
    "prepare-functional-reference-stacks",
    "prepare-in-vivo-anatomy-stack",
    "prepare-ex-vivo-anatomy-stack",
    "segment-hcr-cellpose",
    "segment-ex-vivo-anatomy-cellpose",
)

STAGED_REGISTRATION_CSVS: tuple[str, ...] = (
    "functional_roi_activity_identity.csv",
    "functional_roi_activity_bpi_cells.csv",
    "functional_roi_activity_bpi_summary.csv",
    "hcr_activity_status.csv",
    "hcr_activity_status_summary.csv",
    "conf_to_func_pairs.csv",
    "conf_to_func_pairs_raw.csv",
    "hcr_func_candidates.csv",
)

SCORE_ACTIVITY_BPI_CSVS: tuple[str, ...] = (
    "functional_roi_activity_identity.csv",
    "functional_roi_activity_bpi_cells.csv",
    "functional_roi_activity_bpi_summary.csv",
)

HCR_IDENTITY_ACTIVITY_CSVS: tuple[str, ...] = tuple(
    filename for filename in STAGED_REGISTRATION_CSVS if filename not in SCORE_ACTIVITY_BPI_CSVS
)

ASSIGN_HCR_IDENTITY_CSVS: tuple[str, ...] = (
    "functional_roi_activity_identity.csv",
    "anatomy_identity_lookup.csv",
    "hcr_activity_status.csv",
    "hcr_activity_status_summary.csv",
    "conf_to_func_pairs_raw.csv",
    "conf_to_func_pairs.csv",
    "hcr_func_candidates.csv",
)

ROI_ANATOMY_GEOMETRY_COLUMNS: tuple[str, ...] = (
    "fish_id",
    "plane",
    "plane_idx",
    "best_z",
    "func_source",
    "func_label",
    "roi_idx",
    "centroid_x_func",
    "centroid_y_func",
    "centroid_x_anat",
    "centroid_y_anat",
    "selected_anat_label",
    "selected_dist_um",
    "selected_overlap_px",
    "n_overlap_candidates_any",
    "n_overlap_candidates_valid",
    "matched_anat_plane",
    "plane_match_outcome",
    "claim_outcome",
    "has_unique_anat_match",
    "anat_label",
)

HCR_ALIGNED_PAIR_COLUMNS: tuple[str, ...] = (
    "conf_label",
    "twoP_label",
    "distance_um",
    "overlap_voxels",
    "within_gate",
    "conf_vol",
    "twoP_vol",
    "iou",
    "overlap_frac_conf",
    "overlap_frac_twoP",
    "pair_type",
    "quality",
)

STAGED_FIGURE_FILES: tuple[str, ...] = (
    "compound_50j_56i_unified.png",
    "bpi_all_pairs.png",
    "per_gene_stimulus_trace_with_hcr_status_56h.png",
    "single_fish_50l_responsive_identity_donut.png",
    "single_fish_hcr_anatomy_coexpression_summary.png",
)

RENDERED_FIGURE_FILES: tuple[str, ...] = (
    "single_fish_50l_responsive_identity_donut.png",
    "single_fish_hcr_anatomy_coexpression_summary.png",
)

LEGACY_COPIED_FIGURE_FILES: tuple[str, ...] = tuple(
    filename for filename in STAGED_FIGURE_FILES if filename not in RENDERED_FIGURE_FILES
)

CSV_COMPARISON_COLUMNS: dict[str, dict[str, tuple[str, ...]]] = {
    "functional_roi_activity_identity.csv": {
        "key": ("plane_idx", "func_label"),
        "exact": (
            "selected_anat_label",
            "has_unique_anat_match",
            "anat_label",
            "identity_label",
            "has_identity_assigned",
            "suite2p_is_cell",
            "response_is_active",
            "response_class",
            "response_summary_class",
            "bpi_category",
        ),
        "numeric": ("bpi",),
    },
    "functional_roi_activity_bpi_cells.csv": {
        "key": ("plane_idx", "func_label"),
        "exact": (
            "anat_label",
            "response_is_active",
            "response_class",
            "response_summary_class",
            "bpi_category",
        ),
        "numeric": ("bpi",),
    },
    "functional_roi_activity_bpi_summary.csv": {
        "key": ("response_summary_class", "bpi_category"),
        "exact": (),
        "numeric": ("n_rois",),
    },
    "hcr_activity_status.csv": {
        "key": ("gene", "anat_label"),
        "exact": (
            "functional_status",
            "response_is_active",
            "response_class",
            "response_summary_class",
            "selected_for_trace_export",
        ),
        "numeric": (),
    },
    "hcr_activity_status_summary.csv": {
        "key": ("gene", "inner_status", "outer_status"),
        "exact": (),
        "numeric": ("n_labels",),
    },
    "conf_to_func_pairs.csv": {
        "key": ("gene", "anat_label", "func_label", "plane"),
        "exact": (
            "response_is_active",
            "response_class",
            "response_summary_class",
            "selection_rule",
            "is_selected_for_analysis",
        ),
        "numeric": (),
    },
    "hcr_func_candidates.csv": {
        "key": ("gene", "anat_label", "func_label", "plane_idx"),
        "exact": ("response_is_active", "candidate_response_bucket"),
        "numeric": ("overlap_px_func_anat", "dist_func_anat_um"),
    },
}


@dataclass(frozen=True)
class SingleFishPipelineConfig:
    fish_id: str
    local_root: Path
    owner: str = "Matilde"
    data_mode: str = "local"
    strict: bool = False
    dry_run: bool = True
    write_manifest: bool = False
    pipeline_root: Path | None = None


@dataclass(frozen=True)
class PipelinePaths:
    data_root: Path
    fish_dir: Path
    raw_2p_dir: Path
    raw_2p_metadata_dir: Path
    raw_2p_functional_dir: Path
    raw_2p_anatomy_dir: Path
    preproc_dir: Path
    anatomy_preproc_dir: Path
    functional_preproc_dir: Path
    analysis_dir: Path
    functional_dir: Path
    functional_suite2p_dir: Path
    functional_registration_dir: Path
    functional_ncc_dir: Path
    functional_qa_dir: Path
    functional_segmentation_dir: Path
    functional_pipeline_manifests_dir: Path
    functional_pipeline_outputs_dir: Path
    pipeline_root: Path
    confocal_dir: Path
    confocal_raw_cp_masks_dir: Path
    confocal_aligned_dir: Path
    plots_dir: Path
    matching_metadata_csv: Path


@dataclass(frozen=True)
class StageContract:
    name: str
    order: int
    purpose: str
    depends_on: tuple[str, ...] = ()
    read_only_first_pass: bool = True


@dataclass(frozen=True)
class ManifestPathRecord:
    path: str
    exists: bool
    kind: str
    required: bool
    label: str
    count: int | None = None
    pattern: str | None = None
    size_bytes: int | None = None
    mtime: float | None = None


@dataclass(frozen=True)
class StageCheckRecord:
    label: str
    status: str
    detail: str
    expected: str | None = None
    observed: str | None = None


@dataclass(frozen=True)
class StageManifest:
    manifest_version: str
    stage_name: str
    fish_id: str
    status: str
    dry_run: bool
    generated_at: str
    inputs: tuple[ManifestPathRecord, ...]
    outputs: tuple[ManifestPathRecord, ...] = ()
    checks: tuple[StageCheckRecord, ...] = ()
    parameters: dict[str, Any] | None = None
    warnings: tuple[str, ...] = ()
    errors: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["inputs"] = [asdict(record) for record in self.inputs]
        payload["outputs"] = [asdict(record) for record in self.outputs]
        payload["checks"] = [asdict(record) for record in self.checks]
        return payload


@dataclass(frozen=True)
class PersistedManifestStatus:
    stage_name: str
    path: str
    exists: bool
    status: str
    stale_records: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class StageOutputSpec:
    label: str
    path: str
    required: bool = True
    control_path: str | None = None
    kind: str = "file"
    parity: str | None = None
    key_columns: tuple[str, ...] = ()
    exact_columns: tuple[str, ...] = ()
    numeric_columns: tuple[str, ...] = ()
    numeric_atol: float = 1e-5
    visual_thumbnail_size: int = 64
    visual_rms_warn_threshold: float = 0.02


def _stage_root(paths: PipelinePaths, stage_name: str) -> Path:
    return paths.pipeline_root / stage_name


def legacy_baseline_root(paths: PipelinePaths) -> Path:
    return paths.data_root / "pipeline_baselines" / paths.fish_dir.name / "legacy_singleFish"


def _legacy_baseline_stage_root(paths: PipelinePaths, stage_name: str) -> Path:
    return legacy_baseline_root(paths) / "stages" / stage_name


def _legacy_baseline_path(paths: PipelinePaths, stage_name: str, spec: StageOutputSpec) -> Path:
    stage_root = _stage_root(paths, stage_name)
    relative = Path(spec.path).relative_to(stage_root)
    return _legacy_baseline_stage_root(paths, stage_name) / relative


def pipeline_contracts() -> tuple[StageContract, ...]:
    purposes = {
        "audit-inputs": "Inspect required fish-scoped inputs without writing pipeline outputs.",
        "preprocess-functional": "Roadmap grouping only for functional preparation; writer commands must use concrete operation names.",
        "preprocess-anatomy": "Roadmap grouping only for anatomy preparation; writer commands must use concrete operation names.",
        "preprocess-hcr": "Roadmap grouping only for HCR preparation; writer commands must use concrete operation names.",
        "register-functional-to-anatomy": "Audit or generate functional-to-anatomy registration products.",
        "register-hcr-to-anatomy": "Audit or generate HCR-to-anatomy registration products.",
        "match-roi-to-anatomy": "Fix geometry-only ROI-to-anatomy matches.",
        "assign-hcr-identity": "Attach molecular identity after geometry is fixed.",
        "score-activity-bpi": "Attach response and BPI annotations after identity assignment.",
        "export-canonical-tables": "Export ROI-centric and HCR-centric canonical table bundles.",
        "make-qa-report": "Collect biologist-facing QA/report artifacts.",
        "make-figures": "Render final table-driven figures.",
    }
    dependencies: dict[str, tuple[str, ...]] = {
        "audit-inputs": (),
        "preprocess-functional": ("audit-inputs",),
        "preprocess-anatomy": ("audit-inputs",),
        "preprocess-hcr": ("audit-inputs",),
        "register-functional-to-anatomy": ("preprocess-functional", "preprocess-anatomy"),
        "register-hcr-to-anatomy": ("preprocess-anatomy", "preprocess-hcr"),
        "match-roi-to-anatomy": ("register-functional-to-anatomy",),
        "assign-hcr-identity": ("match-roi-to-anatomy", "register-hcr-to-anatomy"),
        "score-activity-bpi": ("assign-hcr-identity",),
        "export-canonical-tables": ("score-activity-bpi",),
        "make-qa-report": ("export-canonical-tables",),
        "make-figures": ("export-canonical-tables",),
    }
    return tuple(
        StageContract(
            name=name,
            order=index + 1,
            purpose=purposes[name],
            depends_on=dependencies[name],
        )
        for index, name in enumerate(PIPELINE_STAGE_ORDER)
    )


def resolve_pipeline_paths(config: SingleFishPipelineConfig) -> PipelinePaths:
    data_root = Path(config.local_root)
    fish_dir = data_root / config.fish_id
    raw_2p_dir = fish_dir / "01_raw" / "2p"
    preproc_dir = fish_dir / "02_reg" / "00_preprocessing"
    analysis_dir = fish_dir / "03_analysis"
    functional_dir = analysis_dir / "functional"
    confocal_dir = analysis_dir / "confocal"
    pipeline_root = Path(config.pipeline_root) if config.pipeline_root else functional_dir / "pipeline_outputs"
    return PipelinePaths(
        data_root=data_root,
        fish_dir=fish_dir,
        raw_2p_dir=raw_2p_dir,
        raw_2p_metadata_dir=raw_2p_dir / "metadata",
        raw_2p_functional_dir=raw_2p_dir / "functional",
        raw_2p_anatomy_dir=raw_2p_dir / "anatomy",
        preproc_dir=preproc_dir,
        anatomy_preproc_dir=preproc_dir / "2p_anatomy",
        functional_preproc_dir=preproc_dir / "2p_functional",
        analysis_dir=analysis_dir,
        functional_dir=functional_dir,
        functional_suite2p_dir=functional_dir / "suite2P",
        functional_registration_dir=functional_dir / "registration",
        functional_ncc_dir=functional_dir / "ncc",
        functional_qa_dir=functional_dir / "qa",
        functional_segmentation_dir=functional_dir / "segmentation",
        functional_pipeline_manifests_dir=functional_dir / "pipeline_manifests",
        functional_pipeline_outputs_dir=functional_dir / "pipeline_outputs",
        pipeline_root=pipeline_root,
        confocal_dir=confocal_dir,
        confocal_raw_cp_masks_dir=confocal_dir / "raw" / "cp_masks",
        confocal_aligned_dir=confocal_dir / "aligned",
        plots_dir=fish_dir / "04_plots",
        matching_metadata_csv=data_root / "matchingMetadata.csv",
    )


def describe_manifest_path(path: Path | str, *, required: bool = True, label: str | None = None) -> ManifestPathRecord:
    resolved = Path(path)
    exists = resolved.exists()
    stat = resolved.stat() if exists else None
    if resolved.is_file():
        kind = "file"
    elif resolved.is_dir():
        kind = "directory"
    else:
        kind = "missing"
    return ManifestPathRecord(
        path=str(resolved),
        exists=exists,
        kind=kind,
        required=required,
        label=label or resolved.name,
        size_bytes=stat.st_size if stat and resolved.is_file() else None,
        mtime=stat.st_mtime if stat else None,
    )


def describe_glob(
    base_dir: Path | str,
    pattern: str,
    *,
    required: bool = True,
    label: str | None = None,
) -> ManifestPathRecord:
    base = Path(base_dir)
    matches = tuple(path for path in sorted(base.glob(pattern)) if _is_real_match(path))
    file_matches = tuple(path for path in matches if path.is_file())
    mtimes = tuple(path.stat().st_mtime for path in file_matches)
    return ManifestPathRecord(
        path=str(base),
        exists=bool(matches),
        kind="glob",
        required=required,
        label=label or pattern,
        count=len(matches),
        pattern=pattern,
        size_bytes=sum(path.stat().st_size for path in file_matches) if file_matches else None,
        mtime=max(mtimes) if mtimes else (base.stat().st_mtime if base.exists() else None),
    )


def _is_real_match(path: Path) -> bool:
    ignored_names = {".DS_Store"}
    return not any(part.startswith("._") or part in ignored_names for part in path.parts)


def stage_manifest_to_json(manifest: StageManifest) -> str:
    return json.dumps(manifest.to_dict(), indent=2, sort_keys=True) + "\n"


def stage_manifest_path(paths: PipelinePaths, stage_name: str) -> Path:
    return paths.functional_pipeline_manifests_dir / f"{stage_name}_manifest.json"


def write_stage_manifest(manifest: StageManifest, paths: PipelinePaths) -> Path:
    path = stage_manifest_path(paths, manifest.stage_name)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(stage_manifest_to_json(manifest))
    return path


def cellpose_stage_manifest_path(paths: PipelinePaths, stage_name: str) -> Path:
    if stage_name == "prepare-in-vivo-anatomy-stack":
        return stage_manifest_path(paths, stage_name)
    if stage_name == "prepare-ex-vivo-anatomy-stack":
        return paths.analysis_dir / "structural" / "ex_vivo" / "manifests" / f"{stage_name}_manifest.json"
    if stage_name == "segment-hcr-cellpose":
        return paths.confocal_dir / "raw" / "manifests" / f"{stage_name}_manifest.json"
    if stage_name == "segment-ex-vivo-anatomy-cellpose":
        return paths.analysis_dir / "structural" / "ex_vivo" / "manifests" / f"{stage_name}_manifest.json"
    return stage_manifest_path(paths, stage_name)


def write_cellpose_stage_manifest(manifest: StageManifest, paths: PipelinePaths) -> Path:
    path = cellpose_stage_manifest_path(paths, manifest.stage_name)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(stage_manifest_to_json(manifest))
    return path


def _record_signature(record: ManifestPathRecord) -> tuple[Any, ...]:
    return (
        record.path,
        record.exists,
        record.kind,
        record.required,
        record.label,
        record.count,
        record.pattern,
        record.size_bytes,
        record.mtime,
    )


def _manifest_record_from_dict(payload: dict[str, Any]) -> ManifestPathRecord:
    return ManifestPathRecord(
        path=str(payload.get("path", "")),
        exists=bool(payload.get("exists", False)),
        kind=str(payload.get("kind", "")),
        required=bool(payload.get("required", False)),
        label=str(payload.get("label", "")),
        count=payload.get("count"),
        pattern=payload.get("pattern"),
        size_bytes=payload.get("size_bytes"),
        mtime=payload.get("mtime"),
    )


def _manifest_records_by_label(payload: dict[str, Any], key: str) -> dict[str, ManifestPathRecord]:
    return {
        record.label: record
        for record in (_manifest_record_from_dict(item) for item in payload.get(key, ()))
    }


def compare_persisted_manifest(manifest_path: Path, current_manifest: StageManifest) -> PersistedManifestStatus:
    if not manifest_path.exists():
        return PersistedManifestStatus(
            stage_name=current_manifest.stage_name,
            path=str(manifest_path),
            exists=False,
            status="missing",
        )
    try:
        payload = json.loads(manifest_path.read_text())
    except json.JSONDecodeError as exc:
        return PersistedManifestStatus(
            stage_name=current_manifest.stage_name,
            path=str(manifest_path),
            exists=True,
            status="fail",
            stale_records=(f"invalid JSON: {exc}",),
        )
    stale_records: list[str] = []
    for key in ("inputs", "outputs"):
        persisted_records = _manifest_records_by_label(payload, key)
        current_records = {record.label: record for record in getattr(current_manifest, key)}
        for label in sorted(set(persisted_records) | set(current_records)):
            persisted = persisted_records.get(label)
            current = current_records.get(label)
            if persisted is None:
                stale_records.append(f"{key}.{label}: missing from persisted manifest")
            elif current is None:
                stale_records.append(f"{key}.{label}: missing from current audit")
            elif _record_signature(persisted) != _record_signature(current):
                stale_records.append(f"{key}.{label}")
    return PersistedManifestStatus(
        stage_name=current_manifest.stage_name,
        path=str(manifest_path),
        exists=True,
        status="stale" if stale_records else "current",
        stale_records=tuple(stale_records),
    )


def build_single_fish_status(config: SingleFishPipelineConfig) -> dict[str, Any]:
    manifest = run_single_fish_audit_inputs_stage(config)
    paths = resolve_pipeline_paths(config)
    persisted_audit_status = compare_persisted_manifest(stage_manifest_path(paths, "audit-inputs"), manifest)
    downstream_stage_statuses = [
        build_single_fish_stage_status(config, stage_name)
        for stage_name in POST_PREPROCESSING_STAGE_NAMES
    ]
    missing_required = [
        record
        for record in manifest.inputs
        if record.required and not record.exists
    ]
    failed_checks = [check for check in manifest.checks if check.status == "fail"]
    status = manifest.status
    if status == "pass" and persisted_audit_status.status == "stale":
        status = "warn"
    if persisted_audit_status.status == "fail":
        status = "fail"
    active_downstream_failures = [
        stage
        for stage in downstream_stage_statuses
        if stage["status"] == "fail" and _stage_has_existing_outputs(paths, str(stage["stage_name"]))
    ]
    active_downstream_warnings = [
        stage
        for stage in downstream_stage_statuses
        if stage["status"] == "warn" and _stage_has_existing_outputs(paths, str(stage["stage_name"]))
    ]
    if active_downstream_failures:
        status = "fail"
    elif status == "pass" and active_downstream_warnings:
        status = "warn"
    return {
        "fish_id": manifest.fish_id,
        "status": status,
        "dry_run": True,
        "manifest_version": manifest.manifest_version,
        "input_records": len(manifest.inputs),
        "check_records": len(manifest.checks),
        "missing_required_inputs": [record.label for record in missing_required],
        "failed_checks": [check.label for check in failed_checks],
        "persisted_manifests": [persisted_audit_status.to_dict()],
        "stale_records": list(persisted_audit_status.stale_records),
        "downstream_stages": downstream_stage_statuses,
        "warnings": list(manifest.warnings),
        "errors": list(manifest.errors),
    }


def _csv_row_count(path: Path) -> int | None:
    if not path.exists() or not path.is_file():
        return None
    with path.open(newline="") as handle:
        reader = csv.reader(handle)
        try:
            next(reader)
        except StopIteration:
            return 0
        return sum(1 for row in reader if any(cell.strip() for cell in row))


def _csv_header(path: Path) -> tuple[str, ...] | None:
    if not path.exists() or not path.is_file():
        return None
    with path.open(newline="") as handle:
        reader = csv.reader(handle)
        try:
            return tuple(next(reader))
        except StopIteration:
            return ()


def _csv_dict_rows(path: Path) -> tuple[dict[str, str], ...] | None:
    if not path.exists() or not path.is_file():
        return None
    with path.open(newline="") as handle:
        return tuple(csv.DictReader(handle))


def _real_files_matching(directory: Path, pattern: str) -> tuple[Path, ...]:
    return tuple(path for path in sorted(directory.glob(pattern)) if path.is_file() and _is_real_match(path))


def _suite2p_plane_dirs(paths: PipelinePaths) -> tuple[Path, ...]:
    if not paths.functional_suite2p_dir.exists():
        return ()
    return tuple(
        path
        for path in sorted(paths.functional_suite2p_dir.glob("plane*"))
        if path.is_dir() and _is_real_match(path)
    )


def _string_set(values: Any) -> set[str]:
    return {str(value) for value in values if str(value) != ""}


def _plane_dir_indices(plane_dirs: tuple[Path, ...]) -> set[str]:
    return _string_set(path.name.removeprefix("plane") for path in plane_dirs)


def _column_set(rows: tuple[dict[str, str], ...] | None, column: str) -> set[str] | None:
    if rows is None:
        return None
    return _string_set(row.get(column, "") for row in rows)


def _raw_column_values(rows: tuple[dict[str, str], ...] | None, column: str) -> set[str] | None:
    if rows is None:
        return None
    return {str(row.get(column, "")) for row in rows}


def _key_set(rows: tuple[dict[str, str], ...] | None, columns: tuple[str, ...]) -> set[tuple[str, ...]] | None:
    if rows is None:
        return None
    return {tuple(str(row.get(column, "")) for column in columns) for row in rows}


def _duplicate_key_count(rows: tuple[dict[str, str], ...] | None, columns: tuple[str, ...]) -> int | None:
    if rows is None:
        return None
    keys = [tuple(str(row.get(column, "")) for column in columns) for row in rows]
    return len(keys) - len(set(keys))


def _numeric_count(rows: tuple[dict[str, str], ...] | None, column: str) -> tuple[int, int] | None:
    if rows is None:
        return None
    present = 0
    numeric = 0
    for row in rows:
        value = str(row.get(column, "")).strip()
        if value == "":
            continue
        present += 1
        try:
            float(value)
        except ValueError:
            continue
        numeric += 1
    return present, numeric


def _count_value(rows: tuple[dict[str, str], ...] | None, column: str, value: str) -> int | None:
    if rows is None:
        return None
    return sum(1 for row in rows if str(row.get(column, "")) == value)


def _bool_value(value: str) -> bool | None:
    normalized = str(value).strip().lower()
    if normalized in {"true", "1"}:
        return True
    if normalized in {"false", "0", ""}:
        return False
    return None


def _count_violations(rows: tuple[dict[str, str], ...] | None, predicate: Any) -> int | None:
    if rows is None:
        return None
    return sum(1 for row in rows if not predicate(row))


def _format_set(values: set[str] | None) -> str:
    if values is None:
        return "missing"
    return ",".join(sorted(values)) if values else "empty"


def _append_set_equality_check(
    checks: list[StageCheckRecord],
    *,
    label: str,
    left: set[str] | None,
    right: set[str] | None,
    left_name: str,
    right_name: str,
) -> None:
    checks.append(
        StageCheckRecord(
            label=label,
            status="pass" if left is not None and right is not None and left == right else "fail",
            detail=f"{left_name} should match {right_name}.",
            expected=f"{right_name}={_format_set(right)}",
            observed=f"{left_name}={_format_set(left)}",
        )
    )


def _append_key_subset_check(
    checks: list[StageCheckRecord],
    *,
    label: str,
    subset: set[tuple[str, ...]] | None,
    superset: set[tuple[str, ...]] | None,
    subset_name: str,
    superset_name: str,
) -> None:
    missing = None if subset is None or superset is None else subset - superset
    status = "pass" if missing == set() else "fail"
    checks.append(
        StageCheckRecord(
            label=label,
            status=status,
            detail=f"{subset_name} keys should be represented in {superset_name}.",
            expected="0 missing keys",
            observed="missing" if missing is None else str(len(missing)),
        )
    )


def _append_domain_check(
    checks: list[StageCheckRecord],
    *,
    label: str,
    rows: tuple[dict[str, str], ...] | None,
    column: str,
    allowed_values: set[str],
) -> None:
    values = _raw_column_values(rows, column)
    unexpected = None if values is None else values - allowed_values
    checks.append(
        StageCheckRecord(
            label=label,
            status="pass" if unexpected == set() else "fail",
            detail=f"{column} values should stay in the expected domain.",
            expected=",".join(sorted(allowed_values)),
            observed="missing" if unexpected is None else ("unexpected " + ",".join(sorted(unexpected)) if unexpected else "complete"),
        )
    )


def _append_numeric_check(
    checks: list[StageCheckRecord],
    *,
    label: str,
    rows: tuple[dict[str, str], ...] | None,
    column: str,
) -> None:
    counts = _numeric_count(rows, column)
    checks.append(
        StageCheckRecord(
            label=label,
            status="pass" if counts is not None and counts[0] == counts[1] and counts[0] > 0 else "fail",
            detail=f"Non-empty {column} values should parse as numeric.",
            expected="all present values numeric",
            observed="missing" if counts is None else f"{counts[1]}/{counts[0]}",
        )
    )


def _append_zero_violation_check(
    checks: list[StageCheckRecord],
    *,
    label: str,
    rows: tuple[dict[str, str], ...] | None,
    detail: str,
    predicate: Any,
) -> None:
    violations = _count_violations(rows, predicate)
    checks.append(
        StageCheckRecord(
            label=label,
            status="pass" if violations == 0 else "fail",
            detail=detail,
            expected="0 violations",
            observed="missing" if violations is None else str(violations),
        )
    )


def _append_optional_csv_parity_check(
    checks: list[StageCheckRecord],
    *,
    label: str,
    control_path: Path,
    staged_path: Path,
) -> None:
    if not staged_path.exists():
        checks.append(
            StageCheckRecord(
                label=label,
                status="pass",
                detail="Optional staged CSV is absent; parity check skipped.",
                expected="optional staged CSV if present",
                observed="absent",
            )
        )
        return
    control_rows = _csv_row_count(control_path)
    staged_rows = _csv_row_count(staged_path)
    control_header = _csv_header(control_path)
    staged_header = _csv_header(staged_path)
    checks.append(
        StageCheckRecord(
            label=label,
            status="pass" if control_rows == staged_rows and control_header == staged_header else "fail",
            detail="Staged CSV should match control row count and header when present.",
            expected=f"rows={control_rows}; header={len(control_header or ())}",
            observed=f"rows={staged_rows}; header={len(staged_header or ())}",
        )
    )


def _append_optional_file_parity_check(
    checks: list[StageCheckRecord],
    *,
    label: str,
    control_path: Path,
    staged_path: Path,
) -> None:
    if not staged_path.exists():
        checks.append(
            StageCheckRecord(
                label=label,
                status="pass",
                detail="Optional staged file is absent; parity check skipped.",
                expected="optional staged file if present",
                observed="absent",
            )
        )
        return
    control_exists = control_path.exists()
    staged_nonempty = staged_path.is_file() and staged_path.stat().st_size > 0
    checks.append(
        StageCheckRecord(
            label=label,
            status="pass" if control_exists and staged_nonempty else "fail",
            detail="Staged file should be non-empty when corresponding control file exists.",
            expected="control exists and staged non-empty",
            observed=f"control_exists={control_exists}; staged_size={staged_path.stat().st_size if staged_path.exists() else 'missing'}",
        )
    )


def _csv_comparison_kwargs(filename: str) -> dict[str, Any]:
    columns = CSV_COMPARISON_COLUMNS.get(filename, {})
    return {
        "key_columns": columns.get("key", ()),
        "exact_columns": columns.get("exact", ()),
        "numeric_columns": columns.get("numeric", ()),
    }


def _stage_output_specs(paths: PipelinePaths, stage_name: str) -> tuple[StageOutputSpec, ...]:
    stage_root = _stage_root(paths, stage_name)
    if stage_name == "assign-hcr-identity":
        registration_dir = stage_root / "registration"
        return (
            StageOutputSpec(
                "staged ROI identity master",
                str(registration_dir / "functional_roi_activity_identity.csv"),
                control_path=str(paths.functional_registration_dir / "functional_roi_activity_identity.csv"),
                parity="csv_shape",
                **_csv_comparison_kwargs("functional_roi_activity_identity.csv"),
            ),
            StageOutputSpec(
                "staged anatomy identity lookup",
                str(registration_dir / "anatomy_identity_lookup.csv"),
                parity="nonempty_csv",
            ),
            StageOutputSpec(
                "staged HCR activity status",
                str(registration_dir / "hcr_activity_status.csv"),
                control_path=str(paths.functional_registration_dir / "hcr_activity_status.csv"),
                parity="csv_shape",
                **_csv_comparison_kwargs("hcr_activity_status.csv"),
            ),
            StageOutputSpec(
                "staged HCR activity status summary",
                str(registration_dir / "hcr_activity_status_summary.csv"),
                control_path=str(paths.functional_registration_dir / "hcr_activity_status_summary.csv"),
                parity="csv_shape",
                **_csv_comparison_kwargs("hcr_activity_status_summary.csv"),
            ),
            StageOutputSpec(
                "staged raw HCR/function pairs",
                str(registration_dir / "conf_to_func_pairs_raw.csv"),
                control_path=str(paths.functional_registration_dir / "conf_to_func_pairs_raw.csv"),
                parity="csv_shape",
            ),
            StageOutputSpec(
                "staged responsive HCR/function pairs",
                str(registration_dir / "conf_to_func_pairs.csv"),
                control_path=str(paths.functional_registration_dir / "conf_to_func_pairs.csv"),
                parity="csv_shape",
                **_csv_comparison_kwargs("conf_to_func_pairs.csv"),
            ),
            StageOutputSpec(
                "staged HCR/function candidates",
                str(registration_dir / "hcr_func_candidates.csv"),
                control_path=str(paths.functional_registration_dir / "hcr_func_candidates.csv"),
                parity="csv_shape",
                **_csv_comparison_kwargs("hcr_func_candidates.csv"),
            ),
        )
    if stage_name == "score-activity-bpi":
        registration_dir = stage_root / "registration"
        return (
            StageOutputSpec(
                "staged scored ROI master",
                str(registration_dir / "functional_roi_activity_identity.csv"),
                control_path=str(paths.functional_registration_dir / "functional_roi_activity_identity.csv"),
                parity="csv_shape",
                **_csv_comparison_kwargs("functional_roi_activity_identity.csv"),
            ),
            StageOutputSpec(
                "staged ROI activity/BPI cells",
                str(registration_dir / "functional_roi_activity_bpi_cells.csv"),
                control_path=str(paths.functional_registration_dir / "functional_roi_activity_bpi_cells.csv"),
                parity="csv_shape",
                **_csv_comparison_kwargs("functional_roi_activity_bpi_cells.csv"),
            ),
            StageOutputSpec(
                "staged ROI activity/BPI summary",
                str(registration_dir / "functional_roi_activity_bpi_summary.csv"),
                control_path=str(paths.functional_registration_dir / "functional_roi_activity_bpi_summary.csv"),
                parity="csv_shape",
                **_csv_comparison_kwargs("functional_roi_activity_bpi_summary.csv"),
            ),
        )
    if stage_name == "export-canonical-tables":
        registration_dir = stage_root / "registration"
        return tuple(
            StageOutputSpec(
                f"staged canonical export: {filename}",
                str(registration_dir / filename),
                control_path=str(paths.functional_registration_dir / filename),
                parity="csv_shape",
                **_csv_comparison_kwargs(filename),
            )
            for filename in STAGED_REGISTRATION_CSVS
        )
    if stage_name == "make-qa-report":
        report_dir = stage_root
        return (
            StageOutputSpec(
                "staged QA report markdown",
                str(report_dir / "qa_report.md"),
                parity="nonempty_file",
            ),
            StageOutputSpec(
                "staged QA report summary JSON",
                str(report_dir / "qa_report_summary.json"),
                parity="nonempty_file",
            ),
        )
    if stage_name == "make-figures":
        plots_dir = stage_root / "04_plots"
        return tuple(
            StageOutputSpec(
                f"staged figure: {filename}",
                str(plots_dir / filename),
                control_path=str(paths.plots_dir / filename),
                parity="nonempty_file",
            )
            for filename in STAGED_FIGURE_FILES
        )
    raise ValueError(f"unsupported downstream stage: {stage_name}")


def downstream_stage_names() -> tuple[str, ...]:
    return POST_PREPROCESSING_STAGE_NAMES


def _stage_input_records(paths: PipelinePaths, stage_name: str) -> tuple[ManifestPathRecord, ...]:
    records: list[ManifestPathRecord] = []
    if stage_name == "assign-hcr-identity":
        records.extend(
            (
                describe_manifest_path(paths.functional_registration_dir / "functional_roi_activity_identity.csv", label="control ROI identity master"),
                describe_manifest_path(paths.functional_registration_dir / "hcr_activity_status.csv", label="control HCR activity status"),
                describe_manifest_path(paths.functional_registration_dir / "conf_to_func_pairs.csv", label="control responsive HCR/function pairs"),
                describe_manifest_path(paths.functional_registration_dir / "hcr_func_candidates.csv", label="control HCR/function candidates"),
            )
        )
    elif stage_name == "score-activity-bpi":
        records.extend(
            (
                describe_manifest_path(
                    _stage_root(paths, "assign-hcr-identity") / "registration" / "functional_roi_activity_identity.csv",
                    required=False,
                    label="staged identity master input",
                ),
                describe_manifest_path(paths.functional_registration_dir / "functional_roi_activity_identity.csv", label="control ROI identity master"),
                describe_manifest_path(paths.functional_registration_dir / "functional_roi_activity_bpi_cells.csv", label="control ROI activity/BPI cells"),
                describe_manifest_path(paths.functional_registration_dir / "functional_roi_activity_bpi_summary.csv", label="control ROI activity/BPI summary"),
            )
        )
    elif stage_name == "export-canonical-tables":
        records.extend(
            (
                describe_glob(
                    _stage_root(paths, "score-activity-bpi") / "registration",
                    "*.csv",
                    required=False,
                    label="staged score-activity-bpi CSV inputs",
                ),
                describe_glob(
                    _stage_root(paths, "assign-hcr-identity") / "registration",
                    "*.csv",
                    required=False,
                    label="staged assign-hcr-identity CSV inputs",
                ),
                describe_glob(paths.functional_registration_dir, "*.csv", label="control registration CSV inputs"),
            )
        )
    elif stage_name == "make-figures":
        records.extend(
            (
                describe_glob(
                    _stage_root(paths, "export-canonical-tables") / "registration",
                    "*.csv",
                    label="staged canonical export CSV inputs",
                ),
                describe_glob(paths.plots_dir, "*.png", label="control plot PNG inputs"),
            )
        )
    elif stage_name == "make-qa-report":
        records.extend(
            (
                describe_glob(
                    _stage_root(paths, "export-canonical-tables") / "registration",
                    "*.csv",
                    label="staged canonical export CSV inputs",
                ),
                describe_glob(_stage_root(paths, "assign-hcr-identity") / "registration", "*.csv", required=False, label="staged assign-hcr-identity CSVs"),
                describe_glob(_stage_root(paths, "score-activity-bpi") / "registration", "*.csv", required=False, label="staged score-activity-bpi CSVs"),
                describe_glob(_stage_root(paths, "make-figures") / "04_plots", "*.png", required=False, label="staged figure PNGs"),
            )
        )
    else:
        raise ValueError(f"unsupported downstream stage: {stage_name}")
    return tuple(records)


def _stage_output_records(paths: PipelinePaths, stage_name: str) -> tuple[ManifestPathRecord, ...]:
    return tuple(
        describe_manifest_path(spec.path, required=spec.required, label=spec.label)
        for spec in _stage_output_specs(paths, stage_name)
    )


def _build_downstream_stage_checks(paths: PipelinePaths, stage_name: str) -> tuple[StageCheckRecord, ...]:
    checks: list[StageCheckRecord] = []
    for spec in _stage_output_specs(paths, stage_name):
        output_path = Path(spec.path)
        output_exists = output_path.exists()
        checks.append(
            StageCheckRecord(
                label=f"required output exists: {spec.label}",
                status="pass" if output_exists else "fail",
                detail="Declared stage output should exist for the accepted staged baseline.",
                expected="exists",
                observed="exists" if output_exists else "missing",
            )
        )
        if not output_exists:
            continue
        if spec.parity == "csv_shape":
            control_path = Path(spec.control_path or "")
            control_rows = _csv_row_count(control_path)
            output_rows = _csv_row_count(output_path)
            control_header = _csv_header(control_path)
            output_header = _csv_header(output_path)
            checks.append(
                StageCheckRecord(
                    label=f"control CSV shape parity: {spec.label}",
                    status="pass" if control_rows == output_rows and control_header == output_header else "fail",
                    detail="Read-only staged baseline CSV should match control row count and header.",
                    expected=f"rows={control_rows}; header={len(control_header or ())}",
                    observed=f"rows={output_rows}; header={len(output_header or ())}",
                )
            )
        elif spec.parity == "nonempty_csv":
            rows = _csv_row_count(output_path)
            checks.append(
                StageCheckRecord(
                    label=f"nonempty CSV: {spec.label}",
                    status="pass" if rows is not None and rows > 0 else "fail",
                    detail="Declared staged CSV should contain data rows.",
                    expected=">0 rows",
                    observed="missing" if rows is None else str(rows),
                )
            )
        elif spec.parity == "nonempty_file":
            control_path = Path(spec.control_path or "")
            output_size = output_path.stat().st_size if output_path.is_file() else 0
            checks.append(
                StageCheckRecord(
                    label=f"control file presence: {spec.label}",
                    status="pass" if control_path.exists() and output_size > 0 else "fail",
                    detail="Read-only staged baseline figure should be non-empty when the control figure exists.",
                    expected="control exists and staged non-empty",
                    observed=f"control_exists={control_path.exists()}; staged_size={output_size}",
                )
            )
    return tuple(checks)


def _csv_rows(path: Path) -> tuple[tuple[str, ...], ...] | None:
    if not path.exists() or not path.is_file():
        return None
    with path.open(newline="") as handle:
        return tuple(tuple(row) for row in csv.reader(handle))


def _csv_key(row: dict[str, str], columns: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(str(row.get(column, "")) for column in columns)


def _csv_duplicate_key_count(rows: tuple[dict[str, str], ...], columns: tuple[str, ...]) -> int:
    keys = [_csv_key(row, columns) for row in rows]
    return len(keys) - len(set(keys))


def _csv_rows_by_key(rows: tuple[dict[str, str], ...], columns: tuple[str, ...]) -> dict[tuple[str, ...], dict[str, str]]:
    return {_csv_key(row, columns): row for row in rows}


def _stringify_table_value(value: Any) -> str:
    if value is None:
        return ""
    try:
        if isinstance(value, float) and math.isnan(value):
            return ""
    except TypeError:
        pass
    try:
        import pandas as pd

        if pd.isna(value):
            return ""
    except Exception:
        pass
    return str(value)


def _dataframe_dict_rows(dataframe: Any) -> tuple[dict[str, str], ...]:
    return tuple(
        {
            str(column): _stringify_table_value(value)
            for column, value in row.items()
        }
        for row in dataframe.to_dict(orient="records")
    )


def _numeric_cell_difference(control_value: str, staged_value: str, *, atol: float) -> tuple[bool, float | None]:
    control_text = str(control_value).strip()
    staged_text = str(staged_value).strip()
    if control_text == "" and staged_text == "":
        return False, 0.0
    if control_text == "" or staged_text == "":
        return True, None
    try:
        control_number = float(control_text)
        staged_number = float(staged_text)
    except ValueError:
        return True, None
    control_finite = math.isfinite(control_number)
    staged_finite = math.isfinite(staged_number)
    if not control_finite or not staged_finite:
        return control_text.lower() != staged_text.lower(), None
    diff = abs(control_number - staged_number)
    return diff > float(atol), diff


def _append_keyed_csv_comparison_checks(
    checks: list[StageCheckRecord],
    *,
    spec: StageOutputSpec,
    control_path: Path,
    staged_path: Path,
) -> None:
    if not spec.key_columns:
        return
    control_header = _csv_header(control_path) or ()
    staged_header = _csv_header(staged_path) or ()
    required_columns = spec.key_columns + spec.exact_columns + spec.numeric_columns
    missing_control_columns = tuple(column for column in required_columns if column not in control_header)
    missing_staged_columns = tuple(column for column in required_columns if column not in staged_header)
    if missing_control_columns or missing_staged_columns:
        checks.append(
            StageCheckRecord(
                label=f"comparison CSV semantic columns: {spec.label}",
                status="fail",
                detail="Keyed comparison columns should exist in both control and staged CSVs.",
                expected="all declared key/exact/numeric columns present",
                observed=f"control_missing={','.join(missing_control_columns) or 'none'}; staged_missing={','.join(missing_staged_columns) or 'none'}",
            )
        )
        return

    control_rows = _csv_dict_rows(control_path) or ()
    staged_rows = _csv_dict_rows(staged_path) or ()
    control_duplicate_count = _csv_duplicate_key_count(control_rows, spec.key_columns)
    staged_duplicate_count = _csv_duplicate_key_count(staged_rows, spec.key_columns)
    checks.append(
        StageCheckRecord(
            label=f"comparison CSV unique keys: {spec.label}",
            status="pass" if control_duplicate_count == 0 and staged_duplicate_count == 0 else "fail",
            detail="Declared comparison keys should identify rows uniquely in both CSVs.",
            expected="0 duplicate keys",
            observed=f"control={control_duplicate_count}; staged={staged_duplicate_count}",
        )
    )
    if control_duplicate_count or staged_duplicate_count:
        return

    control_by_key = _csv_rows_by_key(control_rows, spec.key_columns)
    staged_by_key = _csv_rows_by_key(staged_rows, spec.key_columns)
    missing_keys = set(control_by_key) - set(staged_by_key)
    extra_keys = set(staged_by_key) - set(control_by_key)
    checks.append(
        StageCheckRecord(
            label=f"comparison CSV keyed rows: {spec.label}",
            status="pass" if not missing_keys and not extra_keys else "fail",
            detail="Staged CSV should contain the same declared keys as the control CSV.",
            expected=f"missing=0; extra=0; keys={len(control_by_key)}",
            observed=f"missing={len(missing_keys)}; extra={len(extra_keys)}; keys={len(staged_by_key)}",
        )
    )

    shared_keys = sorted(set(control_by_key) & set(staged_by_key))
    exact_mismatches = 0
    for key in shared_keys:
        control_row = control_by_key[key]
        staged_row = staged_by_key[key]
        for column in spec.exact_columns:
            if str(control_row.get(column, "")) != str(staged_row.get(column, "")):
                exact_mismatches += 1
    if spec.exact_columns:
        checks.append(
            StageCheckRecord(
                label=f"comparison CSV exact cells: {spec.label}",
                status="pass" if exact_mismatches == 0 else "fail",
                detail="Declared exact-match columns should match for shared keyed rows.",
                expected=f"0 mismatched cells across {len(spec.exact_columns)} columns",
                observed=str(exact_mismatches),
            )
        )

    numeric_mismatches = 0
    numeric_invalid = 0
    max_abs_diff = 0.0
    for key in shared_keys:
        control_row = control_by_key[key]
        staged_row = staged_by_key[key]
        for column in spec.numeric_columns:
            differs, diff = _numeric_cell_difference(
                str(control_row.get(column, "")),
                str(staged_row.get(column, "")),
                atol=spec.numeric_atol,
            )
            if diff is None:
                numeric_invalid += 1
            else:
                max_abs_diff = max(max_abs_diff, float(diff))
            if differs:
                numeric_mismatches += 1
    if spec.numeric_columns:
        checks.append(
            StageCheckRecord(
                label=f"comparison CSV numeric cells: {spec.label}",
                status="pass" if numeric_mismatches == 0 and numeric_invalid == 0 else "fail",
                detail="Declared numeric columns should match within absolute tolerance for shared keyed rows.",
                expected=f"0 mismatches; atol={spec.numeric_atol:g}",
                observed=f"mismatches={numeric_mismatches}; invalid={numeric_invalid}; max_abs_diff={max_abs_diff:g}",
            )
        )


def _append_keyed_row_comparison_checks(
    checks: list[StageCheckRecord],
    *,
    spec: StageOutputSpec,
    control_rows: tuple[dict[str, str], ...],
    computed_rows: tuple[dict[str, str], ...],
) -> None:
    if not spec.key_columns:
        return
    control_header = tuple(control_rows[0].keys()) if control_rows else ()
    computed_header = tuple(computed_rows[0].keys()) if computed_rows else ()
    required_columns = spec.key_columns + spec.exact_columns + spec.numeric_columns
    missing_control_columns = tuple(column for column in required_columns if column not in control_header)
    missing_computed_columns = tuple(column for column in required_columns if column not in computed_header)
    if missing_control_columns or missing_computed_columns:
        checks.append(
            StageCheckRecord(
                label=f"recompute semantic columns: {spec.label}",
                status="fail",
                detail="Declared comparison columns should exist in both control and recomputed tables.",
                expected="all declared key/exact/numeric columns present",
                observed=f"control_missing={','.join(missing_control_columns) or 'none'}; computed_missing={','.join(missing_computed_columns) or 'none'}",
            )
        )
        return

    control_duplicate_count = _csv_duplicate_key_count(control_rows, spec.key_columns)
    computed_duplicate_count = _csv_duplicate_key_count(computed_rows, spec.key_columns)
    checks.append(
        StageCheckRecord(
            label=f"recompute unique keys: {spec.label}",
            status="pass" if control_duplicate_count == 0 and computed_duplicate_count == 0 else "fail",
            detail="Declared comparison keys should identify rows uniquely in both control and recomputed tables.",
            expected="0 duplicate keys",
            observed=f"control={control_duplicate_count}; computed={computed_duplicate_count}",
        )
    )
    if control_duplicate_count or computed_duplicate_count:
        return

    control_by_key = _csv_rows_by_key(control_rows, spec.key_columns)
    computed_by_key = _csv_rows_by_key(computed_rows, spec.key_columns)
    missing_keys = set(control_by_key) - set(computed_by_key)
    extra_keys = set(computed_by_key) - set(control_by_key)
    checks.append(
        StageCheckRecord(
            label=f"recompute keyed rows: {spec.label}",
            status="pass" if not missing_keys and not extra_keys else "fail",
            detail="Recomputed table should contain the same declared keys as the control CSV.",
            expected=f"missing=0; extra=0; keys={len(control_by_key)}",
            observed=f"missing={len(missing_keys)}; extra={len(extra_keys)}; keys={len(computed_by_key)}",
        )
    )

    shared_keys = sorted(set(control_by_key) & set(computed_by_key))
    exact_mismatches = 0
    exact_mismatches_by_column: dict[str, int] = {column: 0 for column in spec.exact_columns}
    for key in shared_keys:
        control_row = control_by_key[key]
        computed_row = computed_by_key[key]
        for column in spec.exact_columns:
            if str(control_row.get(column, "")) != str(computed_row.get(column, "")):
                exact_mismatches += 1
                exact_mismatches_by_column[column] += 1
    if spec.exact_columns:
        column_detail = ",".join(
            f"{column}={count}"
            for column, count in exact_mismatches_by_column.items()
            if count
        )
        checks.append(
            StageCheckRecord(
                label=f"recompute exact cells: {spec.label}",
                status="pass" if exact_mismatches == 0 else "fail",
                detail="Declared exact-match columns should match for shared keyed rows.",
                expected=f"0 mismatched cells across {len(spec.exact_columns)} columns",
                observed=str(exact_mismatches) if not column_detail else f"{exact_mismatches}; by_column={column_detail}",
            )
        )

    numeric_mismatches = 0
    numeric_invalid = 0
    max_abs_diff = 0.0
    numeric_mismatches_by_column: dict[str, int] = {column: 0 for column in spec.numeric_columns}
    numeric_invalid_by_column: dict[str, int] = {column: 0 for column in spec.numeric_columns}
    for key in shared_keys:
        control_row = control_by_key[key]
        computed_row = computed_by_key[key]
        for column in spec.numeric_columns:
            differs, diff = _numeric_cell_difference(
                str(control_row.get(column, "")),
                str(computed_row.get(column, "")),
                atol=spec.numeric_atol,
            )
            if diff is None:
                numeric_invalid += 1
                numeric_invalid_by_column[column] += 1
            else:
                max_abs_diff = max(max_abs_diff, float(diff))
            if differs:
                numeric_mismatches += 1
                numeric_mismatches_by_column[column] += 1
    if spec.numeric_columns:
        mismatch_detail = ",".join(
            f"{column}={count}"
            for column, count in numeric_mismatches_by_column.items()
            if count
        )
        invalid_detail = ",".join(
            f"{column}={count}"
            for column, count in numeric_invalid_by_column.items()
            if count
        )
        details = f"mismatches={numeric_mismatches}; invalid={numeric_invalid}; max_abs_diff={max_abs_diff:g}"
        if mismatch_detail:
            details += f"; mismatch_by_column={mismatch_detail}"
        if invalid_detail:
            details += f"; invalid_by_column={invalid_detail}"
        checks.append(
            StageCheckRecord(
                label=f"recompute numeric cells: {spec.label}",
                status="pass" if numeric_mismatches == 0 and numeric_invalid == 0 else "fail",
                detail="Declared numeric columns should match within absolute tolerance for shared keyed rows.",
                expected=f"0 mismatches; atol={spec.numeric_atol:g}",
                observed=details,
            )
        )


def _png_dimensions(path: Path) -> tuple[int, int] | None:
    try:
        with path.open("rb") as handle:
            header = handle.read(24)
    except OSError:
        return None
    if len(header) < 24 or header[:8] != b"\x89PNG\r\n\x1a\n" or header[12:16] != b"IHDR":
        return None
    width, height = struct.unpack(">II", header[16:24])
    return int(width), int(height)


def _image_dimensions(path: Path) -> tuple[int, int] | None:
    dimensions = _png_dimensions(path)
    if dimensions is not None:
        return dimensions
    try:
        from PIL import Image
    except Exception:
        return None
    try:
        with Image.open(path) as image:
            return tuple(int(value) for value in image.size)
    except Exception:
        return None


def _thumbnail_rgb_pixels(path: Path, *, size: int) -> tuple[tuple[int, int, int], ...] | None:
    try:
        from PIL import Image
    except Exception:
        return None
    try:
        with Image.open(path) as image:
            resampling = getattr(getattr(Image, "Resampling", Image), "LANCZOS")
            thumb = image.convert("RGB").resize((int(size), int(size)), resampling)
            pixels = tuple((int(r), int(g), int(b)) for r, g, b in thumb.getdata())
    except Exception:
        return None
    return pixels


def _append_visual_thumbnail_comparison_checks(
    checks: list[StageCheckRecord],
    *,
    spec: StageOutputSpec,
    control_path: Path,
    staged_path: Path,
) -> None:
    control_size = _image_dimensions(control_path)
    staged_size = _image_dimensions(staged_path)
    if control_size is not None and staged_size is not None:
        checks.append(
            StageCheckRecord(
                label=f"comparison figure dimensions: {spec.label}",
                status="pass" if control_size == staged_size else "warn",
                detail="Readable staged and control figures should have matching pixel dimensions.",
                expected=f"{control_size[0]}x{control_size[1]}",
                observed=f"{staged_size[0]}x{staged_size[1]}",
            )
        )
    control_pixels = _thumbnail_rgb_pixels(control_path, size=spec.visual_thumbnail_size)
    staged_pixels = _thumbnail_rgb_pixels(staged_path, size=spec.visual_thumbnail_size)
    if control_pixels is None or staged_pixels is None:
        return
    total_abs = 0.0
    total_sq = 0.0
    count = 0
    for control_pixel, staged_pixel in zip(control_pixels, staged_pixels):
        for control_value, staged_value in zip(control_pixel, staged_pixel):
            diff = float(control_value - staged_value) / 255.0
            total_abs += abs(diff)
            total_sq += diff * diff
            count += 1
    mean_abs_error = total_abs / float(count) if count else 0.0
    rms_error = math.sqrt(total_sq / float(count)) if count else 0.0
    checks.append(
        StageCheckRecord(
            label=f"comparison figure thumbnail: {spec.label}",
            status="pass" if rms_error <= float(spec.visual_rms_warn_threshold) else "warn",
            detail="Downsampled RGB thumbnails should remain visually close; drift is reported as a warning for review.",
            expected=f"rms<={spec.visual_rms_warn_threshold:g}",
            observed=f"mae={mean_abs_error:.6g}; rms={rms_error:.6g}; thumbnail={spec.visual_thumbnail_size}px",
        )
    )


def _build_staged_comparison_checks(paths: PipelinePaths, stage_name: str) -> tuple[StageCheckRecord, ...]:
    checks: list[StageCheckRecord] = []
    for spec in _stage_output_specs(paths, stage_name):
        staged_path = Path(spec.path)
        control_path = Path(spec.control_path or "")
        if spec.control_path is None:
            if spec.parity == "nonempty_csv":
                rows = _csv_row_count(staged_path)
                checks.append(
                    StageCheckRecord(
                        label=f"comparison nonempty CSV: {spec.label}",
                        status="pass" if rows is not None and rows > 0 else "fail",
                        detail="No control path is declared for this output; compare non-empty CSV presence only.",
                        expected=">0 rows",
                        observed="missing" if rows is None else str(rows),
                    )
                )
            else:
                checks.append(
                    StageCheckRecord(
                        label=f"comparison output exists: {spec.label}",
                        status="pass" if staged_path.exists() else "fail",
                        detail="No control path is declared for this output; compare output presence only.",
                        expected="exists",
                        observed="exists" if staged_path.exists() else "missing",
                    )
                )
            continue
        if not staged_path.exists() or not control_path.exists():
            checks.append(
                StageCheckRecord(
                    label=f"comparison inputs exist: {spec.label}",
                    status="fail",
                    detail="Both staged and control outputs are required for comparison.",
                    expected="staged and control exist",
                    observed=f"staged={staged_path.exists()}; control={control_path.exists()}",
                )
            )
            continue
        if spec.parity == "csv_shape":
            staged_rows = _csv_rows(staged_path)
            control_rows = _csv_rows(control_path)
            staged_header = staged_rows[0] if staged_rows else ()
            control_header = control_rows[0] if control_rows else ()
            staged_count = max(len(staged_rows or ()) - 1, 0)
            control_count = max(len(control_rows or ()) - 1, 0)
            shape_match = staged_header == control_header and staged_count == control_count
            checks.append(
                StageCheckRecord(
                    label=f"comparison CSV shape: {spec.label}",
                    status="pass" if shape_match else "fail",
                    detail="Current compare-staged contract requires matching CSV header and row count.",
                    expected=f"rows={control_count}; header={len(control_header)}",
                    observed=f"rows={staged_count}; header={len(staged_header)}",
                )
            )
            if shape_match:
                _append_keyed_csv_comparison_checks(
                    checks,
                    spec=spec,
                    control_path=control_path,
                    staged_path=staged_path,
                )
                bytes_match = staged_path.read_bytes() == control_path.read_bytes()
                checks.append(
                    StageCheckRecord(
                        label=f"comparison CSV byte parity: {spec.label}",
                        status="pass" if bytes_match else "warn",
                        detail="Byte-identical CSV output is desirable but not yet required for accepted L395_f11 staged outputs.",
                        expected="byte-identical",
                        observed="match" if bytes_match else "different",
                    )
                )
        elif spec.parity == "nonempty_file":
            staged_size = staged_path.stat().st_size if staged_path.is_file() else 0
            checks.append(
                StageCheckRecord(
                    label=f"comparison figure presence: {spec.label}",
                    status="pass" if control_path.exists() and staged_size > 0 else "fail",
                    detail="Current figure comparison checks staged non-empty presence against an existing control figure.",
                    expected="control exists and staged non-empty",
                    observed=f"control_exists={control_path.exists()}; staged_size={staged_size}",
                )
            )
            if control_path.exists() and staged_size > 0:
                _append_visual_thumbnail_comparison_checks(
                    checks,
                    spec=spec,
                    control_path=control_path,
                    staged_path=staged_path,
                )
    return tuple(checks)


def build_single_fish_compare_staged_manifest(
    config: SingleFishPipelineConfig,
    stage_name: str,
) -> StageManifest:
    if stage_name not in POST_PREPROCESSING_STAGE_NAMES:
        raise ValueError(
            f"unsupported staged comparison {stage_name!r}; expected one of {', '.join(POST_PREPROCESSING_STAGE_NAMES)}"
        )
    paths = resolve_pipeline_paths(config)
    inputs = tuple(
        describe_manifest_path(spec.control_path, label=f"control for {spec.label}")
        for spec in _stage_output_specs(paths, stage_name)
        if spec.control_path is not None
    )
    outputs = _stage_output_records(paths, stage_name)
    checks = _build_staged_comparison_checks(paths, stage_name)
    failed_checks = tuple(f"{check.label}: {check.observed}" for check in checks if check.status == "fail")
    warning_checks = tuple(f"{check.label}: {check.observed}" for check in checks if check.status == "warn")
    errors = failed_checks if config.strict else ()
    warnings = warning_checks
    if failed_checks and not config.strict:
        warnings = warning_checks + tuple(f"failed check: {check}" for check in failed_checks)
    status = "fail" if errors else ("warn" if warnings else "pass")
    return StageManifest(
        manifest_version=PIPELINE_MANIFEST_VERSION,
        stage_name=f"compare-staged-{stage_name}",
        fish_id=config.fish_id,
        status=status,
        dry_run=True,
        generated_at=datetime.now(timezone.utc).isoformat(),
        inputs=inputs,
        outputs=outputs,
        checks=checks,
        parameters={
            "local_root": str(config.local_root),
            "owner": config.owner,
            "data_mode": config.data_mode,
            "strict": config.strict,
            "write_manifest": config.write_manifest,
            "pipeline_root": str(paths.pipeline_root),
            "compared_stage": stage_name,
            "read_only_comparison": True,
            "csv_contract": "header_and_row_count_required; declared_key_exact_numeric_checks_required; byte_parity_warn_only",
            "figure_contract": "control_exists_and_staged_nonempty",
        },
        warnings=warnings,
        errors=errors,
    )


def compare_single_fish_staged_outputs(
    config: SingleFishPipelineConfig,
    stage_name: str | None = None,
) -> dict[str, Any]:
    paths = resolve_pipeline_paths(config)
    stage_names = (stage_name,) if stage_name else POST_PREPROCESSING_STAGE_NAMES
    comparisons: list[dict[str, Any]] = []
    for name in stage_names:
        if name not in POST_PREPROCESSING_STAGE_NAMES:
            raise ValueError(
                f"unsupported staged comparison {name!r}; expected one of {', '.join(POST_PREPROCESSING_STAGE_NAMES)}"
            )
        if not _stage_has_existing_outputs(paths, name):
            comparisons.append(
                {
                    "stage_name": name,
                    "status": "not_started",
                    "check_records": 0,
                    "failed_checks": [],
                    "warning_checks": [],
                    "manifest": None,
                }
            )
            continue
        manifest = build_single_fish_compare_staged_manifest(config, name)
        comparisons.append(
            {
                "stage_name": name,
                "status": manifest.status,
                "check_records": len(manifest.checks),
                "failed_checks": [check.label for check in manifest.checks if check.status == "fail"],
                "warning_checks": [check.label for check in manifest.checks if check.status == "warn"],
                "manifest": manifest.to_dict(),
            }
        )
    statuses = [comparison["status"] for comparison in comparisons]
    if any(status == "fail" for status in statuses):
        status = "fail"
    elif any(status == "warn" for status in statuses):
        status = "warn"
    elif statuses and all(status == "not_started" for status in statuses):
        status = "not_started"
    else:
        status = "pass"
    return {
        "fish_id": config.fish_id,
        "status": status,
        "dry_run": True,
        "manifest_version": PIPELINE_MANIFEST_VERSION,
        "stage_name": stage_name,
        "comparisons": comparisons,
    }


def run_single_fish_freeze_legacy_baseline_stage(
    config: SingleFishPipelineConfig,
    *,
    stage_name: str | None = None,
    overwrite: bool = False,
) -> StageManifest:
    paths = resolve_pipeline_paths(config)
    stage_names = (stage_name,) if stage_name else POST_PREPROCESSING_STAGE_NAMES
    invalid = tuple(name for name in stage_names if name not in POST_PREPROCESSING_STAGE_NAMES)
    if invalid:
        raise ValueError(
            f"unsupported legacy baseline stage {invalid[0]!r}; expected one of {', '.join(POST_PREPROCESSING_STAGE_NAMES)}"
        )

    inputs: list[ManifestPathRecord] = []
    outputs: list[ManifestPathRecord] = []
    checks: list[StageCheckRecord] = []
    copied = 0
    skipped = 0
    for name in stage_names:
        for spec in _stage_output_specs(paths, name):
            if spec.control_path is None:
                skipped += 1
                checks.append(
                    StageCheckRecord(
                        label=f"freeze legacy source declared: {name}: {spec.label}",
                        status="pass",
                        detail="This output has no legacy/control source path in the staged comparison contract and is not frozen.",
                        expected="control_path declared or explicit skip",
                        observed="skipped_no_control_path",
                    )
                )
                continue
            source_path = Path(spec.control_path)
            baseline_path = _legacy_baseline_path(paths, name, spec)
            inputs.append(describe_manifest_path(source_path, label=f"legacy source: {name}: {spec.label}"))
            existing = baseline_path.exists()
            source_exists = source_path.exists()
            can_write = source_exists and (overwrite or not existing)
            checks.append(
                StageCheckRecord(
                    label=f"freeze legacy output gate: {name}: {spec.label}",
                    status="pass" if can_write else "fail",
                    detail="Freeze copies existing legacy/control outputs and refuses to overwrite without --overwrite.",
                    expected="source exists and no existing baseline unless overwrite=True",
                    observed=f"source_exists={source_exists}; baseline_exists={existing}; overwrite={bool(overwrite)}",
                )
            )
            if can_write:
                baseline_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(source_path, baseline_path)
                copied += 1
            outputs.append(describe_manifest_path(baseline_path, label=f"legacy baseline: {name}: {spec.label}"))

    failed_checks = tuple(f"{check.label}: {check.observed}" for check in checks if check.status == "fail")
    warnings = tuple(f"skipped output without legacy source: {skipped}" for _ in (0,) if skipped)
    status = "fail" if failed_checks else ("warn" if warnings else "pass")
    return StageManifest(
        manifest_version=PIPELINE_MANIFEST_VERSION,
        stage_name="freeze-legacy-baseline",
        fish_id=config.fish_id,
        status=status,
        dry_run=False,
        generated_at=datetime.now(timezone.utc).isoformat(),
        inputs=tuple(inputs),
        outputs=tuple(outputs),
        checks=tuple(checks),
        parameters={
            "local_root": str(config.local_root),
            "owner": config.owner,
            "data_mode": config.data_mode,
            "strict": config.strict,
            "write_manifest": config.write_manifest,
            "pipeline_root": str(paths.pipeline_root),
            "baseline_root": str(legacy_baseline_root(paths)),
            "stage_name": stage_name,
            "stage_names": stage_names,
            "overwrite": bool(overwrite),
            "copied_outputs": int(copied),
            "skipped_outputs_without_control_path": int(skipped),
            "source_policy": "freeze declared legacy/control outputs from stage comparison specs into a fish-scoped legacy_singleFish baseline bundle",
        },
        warnings=warnings,
        errors=failed_checks,
    )


def _append_legacy_baseline_comparison_checks(
    checks: list[StageCheckRecord],
    *,
    spec: StageOutputSpec,
    baseline_path: Path,
    staged_path: Path,
) -> None:
    if not baseline_path.exists() or not staged_path.exists():
        checks.append(
            StageCheckRecord(
                label=f"legacy baseline comparison inputs exist: {spec.label}",
                status="fail",
                detail="Both frozen baseline and staged output are required for legacy-baseline comparison.",
                expected="baseline and staged exist",
                observed=f"baseline={baseline_path.exists()}; staged={staged_path.exists()}",
            )
        )
        return
    if spec.parity == "csv_shape":
        staged_rows = _csv_rows(staged_path)
        baseline_rows = _csv_rows(baseline_path)
        staged_header = staged_rows[0] if staged_rows else ()
        baseline_header = baseline_rows[0] if baseline_rows else ()
        staged_count = max(len(staged_rows or ()) - 1, 0)
        baseline_count = max(len(baseline_rows or ()) - 1, 0)
        shape_match = staged_header == baseline_header and staged_count == baseline_count
        checks.append(
            StageCheckRecord(
                label=f"legacy baseline CSV shape: {spec.label}",
                status="pass" if shape_match else "fail",
                detail="Frozen legacy baseline and staged CSV should match header and row count.",
                expected=f"rows={baseline_count}; header={len(baseline_header)}",
                observed=f"rows={staged_count}; header={len(staged_header)}",
            )
        )
        if shape_match:
            _append_keyed_csv_comparison_checks(
                checks,
                spec=spec,
                control_path=baseline_path,
                staged_path=staged_path,
            )
            bytes_match = staged_path.read_bytes() == baseline_path.read_bytes()
            checks.append(
                StageCheckRecord(
                    label=f"legacy baseline CSV byte parity: {spec.label}",
                    status="pass" if bytes_match else "warn",
                    detail="Byte-identical CSV output is desirable but not required for semantic staged comparisons.",
                    expected="byte-identical",
                    observed="match" if bytes_match else "different",
                )
            )
    elif spec.parity == "nonempty_file":
        staged_size = staged_path.stat().st_size if staged_path.is_file() else 0
        baseline_size = baseline_path.stat().st_size if baseline_path.is_file() else 0
        checks.append(
            StageCheckRecord(
                label=f"legacy baseline figure presence: {spec.label}",
                status="pass" if baseline_size > 0 and staged_size > 0 else "fail",
                detail="Frozen legacy baseline and staged figure should both be non-empty.",
                expected="baseline and staged non-empty",
                observed=f"baseline_size={baseline_size}; staged_size={staged_size}",
            )
        )
        if baseline_size > 0 and staged_size > 0:
            _append_visual_thumbnail_comparison_checks(
                checks,
                spec=spec,
                control_path=baseline_path,
                staged_path=staged_path,
            )


def build_single_fish_compare_legacy_baseline_manifest(
    config: SingleFishPipelineConfig,
    stage_name: str,
) -> StageManifest:
    if stage_name not in POST_PREPROCESSING_STAGE_NAMES:
        raise ValueError(
            f"unsupported legacy baseline comparison {stage_name!r}; expected one of {', '.join(POST_PREPROCESSING_STAGE_NAMES)}"
        )
    paths = resolve_pipeline_paths(config)
    inputs: list[ManifestPathRecord] = []
    checks: list[StageCheckRecord] = []
    skipped = 0
    for spec in _stage_output_specs(paths, stage_name):
        if spec.control_path is None:
            skipped += 1
            checks.append(
                StageCheckRecord(
                    label=f"legacy baseline source declared: {spec.label}",
                    status="pass",
                    detail="This output has no frozen legacy/control source in the staged comparison contract and is skipped.",
                    expected="control_path declared or explicit skip",
                    observed="skipped_no_control_path",
                )
            )
            continue
        baseline_path = _legacy_baseline_path(paths, stage_name, spec)
        staged_path = Path(spec.path)
        inputs.append(describe_manifest_path(baseline_path, label=f"legacy baseline: {stage_name}: {spec.label}"))
        _append_legacy_baseline_comparison_checks(
            checks,
            spec=spec,
            baseline_path=baseline_path,
            staged_path=staged_path,
        )
    outputs = _stage_output_records(paths, stage_name)
    failed_checks = tuple(f"{check.label}: {check.observed}" for check in checks if check.status == "fail")
    warning_checks = tuple(f"{check.label}: {check.observed}" for check in checks if check.status == "warn")
    errors = failed_checks if config.strict else ()
    warnings = warning_checks + tuple(f"skipped output without legacy source: {skipped}" for _ in (0,) if skipped)
    if failed_checks and not config.strict:
        warnings = warnings + tuple(f"failed check: {check}" for check in failed_checks)
    status = "fail" if errors else ("warn" if warnings else "pass")
    return StageManifest(
        manifest_version=PIPELINE_MANIFEST_VERSION,
        stage_name=f"compare-legacy-baseline-{stage_name}",
        fish_id=config.fish_id,
        status=status,
        dry_run=True,
        generated_at=datetime.now(timezone.utc).isoformat(),
        inputs=tuple(inputs),
        outputs=outputs,
        checks=tuple(checks),
        parameters={
            "local_root": str(config.local_root),
            "owner": config.owner,
            "data_mode": config.data_mode,
            "strict": config.strict,
            "write_manifest": config.write_manifest,
            "pipeline_root": str(paths.pipeline_root),
            "baseline_root": str(legacy_baseline_root(paths)),
            "compared_stage": stage_name,
            "read_only_comparison": True,
            "skipped_outputs_without_control_path": int(skipped),
            "csv_contract": "header_and_row_count_required; declared_key_exact_numeric_checks_required; byte_parity_warn_only",
            "figure_contract": "baseline_and_staged_nonempty_with_optional_thumbnail_warning",
        },
        warnings=warnings,
        errors=errors,
    )


def compare_single_fish_legacy_baseline(
    config: SingleFishPipelineConfig,
    stage_name: str | None = None,
) -> dict[str, Any]:
    paths = resolve_pipeline_paths(config)
    stage_names = (stage_name,) if stage_name else POST_PREPROCESSING_STAGE_NAMES
    comparisons: list[dict[str, Any]] = []
    for name in stage_names:
        if name not in POST_PREPROCESSING_STAGE_NAMES:
            raise ValueError(
                f"unsupported legacy baseline comparison {name!r}; expected one of {', '.join(POST_PREPROCESSING_STAGE_NAMES)}"
            )
        if not _stage_has_existing_outputs(paths, name):
            comparisons.append(
                {
                    "stage_name": name,
                    "status": "not_started",
                    "check_records": 0,
                    "failed_checks": [],
                    "warning_checks": [],
                    "manifest": None,
                }
            )
            continue
        manifest = build_single_fish_compare_legacy_baseline_manifest(config, name)
        comparisons.append(
            {
                "stage_name": name,
                "status": manifest.status,
                "check_records": len(manifest.checks),
                "failed_checks": [check.label for check in manifest.checks if check.status == "fail"],
                "warning_checks": [check.label for check in manifest.checks if check.status == "warn"],
                "manifest": manifest.to_dict(),
            }
        )
    statuses = [comparison["status"] for comparison in comparisons]
    if any(status == "fail" for status in statuses):
        status = "fail"
    elif any(status == "warn" for status in statuses):
        status = "warn"
    elif statuses and all(status == "not_started" for status in statuses):
        status = "not_started"
    else:
        status = "pass"
    return {
        "fish_id": config.fish_id,
        "status": status,
        "strict": config.strict,
        "pipeline_root": str(paths.pipeline_root),
        "baseline_root": str(legacy_baseline_root(paths)),
        "stage_name": stage_name,
        "comparisons": comparisons,
    }


def _score_activity_bpi_audit_inputs(paths: PipelinePaths) -> tuple[ManifestPathRecord, ...]:
    return (
        describe_manifest_path(
            paths.functional_registration_dir / "functional_roi_activity_identity.csv",
            label="ROI identity master input",
        ),
        describe_manifest_path(paths.functional_suite2p_dir, label="Suite2p root"),
        describe_glob(paths.functional_suite2p_dir, "plane*/*F.npy", label="Suite2p F traces"),
        describe_glob(paths.raw_2p_metadata_dir, "*experiment_log*.csv", label="experiment log CSVs"),
        describe_glob(paths.raw_2p_metadata_dir, "*metadata*.csv", required=False, label="experiment metadata CSVs"),
        describe_glob(paths.raw_2p_metadata_dir, "*planned_schedule*.csv", required=False, label="planned stimulus schedule CSVs"),
        describe_glob(
            paths.functional_preproc_dir,
            "**/*preprocessing_metadata.json",
            required=False,
            label="functional preprocessing session metadata",
        ),
        describe_manifest_path(
            paths.functional_registration_dir / "functional_roi_activity_identity.csv",
            label="control scored ROI master",
        ),
        describe_manifest_path(
            paths.functional_registration_dir / "functional_roi_activity_bpi_cells.csv",
            label="control ROI activity/BPI cells",
        ),
        describe_manifest_path(
            paths.functional_registration_dir / "functional_roi_activity_bpi_summary.csv",
            label="control ROI activity/BPI summary",
        ),
    )


def _append_recomputed_table_checks(
    checks: list[StageCheckRecord],
    *,
    label: str,
    control_path: Path,
    computed_table: Any,
    spec: StageOutputSpec,
) -> None:
    control_rows = _csv_dict_rows(control_path) or ()
    computed_rows = _dataframe_dict_rows(computed_table)
    checks.append(
        StageCheckRecord(
            label=f"recompute row count: {label}",
            status="pass" if len(control_rows) == len(computed_rows) else "fail",
            detail="Recomputed table row count should match the accepted control CSV.",
            expected=str(len(control_rows)),
            observed=str(len(computed_rows)),
        )
    )
    _append_keyed_row_comparison_checks(
        checks,
        spec=spec,
        control_rows=control_rows,
        computed_rows=computed_rows,
    )


def build_single_fish_score_activity_bpi_recompute_manifest(
    config: SingleFishPipelineConfig,
) -> StageManifest:
    paths = resolve_pipeline_paths(config)
    inputs = _score_activity_bpi_audit_inputs(paths)
    checks: list[StageCheckRecord] = []
    missing_required = tuple(record.path for record in inputs if record.required and not record.exists)
    recompute_error: str | None = None
    result: dict[str, Any] = {}
    missing_modules = tuple(
        module_name
        for module_name in ("pandas", "numpy")
        if importlib.util.find_spec(module_name) is None
    )
    checks.append(
        StageCheckRecord(
            label="score-activity-bpi Python dependencies",
            status="pass" if not missing_modules else "fail",
            detail="Response/BPI recompute requires the scientific Python dependencies used by activity.build_response_bpi_tables.",
            expected="pandas,numpy",
            observed="complete" if not missing_modules else "missing " + ",".join(missing_modules),
        )
    )

    if not missing_required and not missing_modules:
        try:
            import pandas as pd

            from .activity import ActivityConfig, build_response_bpi_tables

            identity_path = paths.functional_registration_dir / "functional_roi_activity_identity.csv"
            detail_df = pd.read_csv(identity_path)
            raw_required_columns = ("plane_idx", "func_label", "activity_class", "is_active")
            missing_raw_columns = tuple(column for column in raw_required_columns if column not in detail_df.columns)
            checks.append(
                StageCheckRecord(
                    label="score-activity-bpi raw ROI inputs",
                    status="pass" if not missing_raw_columns else "fail",
                    detail="Recompute requires unscored ROI identity inputs with trace-quality columns, not only scored control columns.",
                    expected="plane_idx,func_label,activity_class,is_active",
                    observed="complete" if not missing_raw_columns else "missing " + ",".join(missing_raw_columns),
                )
            )
            if missing_raw_columns:
                raise RuntimeError(f"ROI identity master missing recompute input columns: {', '.join(missing_raw_columns)}")
            result = build_response_bpi_tables(
                detail_df,
                fish_dir=paths.fish_dir,
                fish_id=config.fish_id,
                suite2p_root=paths.functional_suite2p_dir,
                precomputed_scored_bpi_df=None,
            )
            activity_config = ActivityConfig()
        except Exception as exc:
            recompute_error = str(exc)
            try:
                from .activity import ActivityConfig

                activity_config = ActivityConfig()
            except Exception:
                activity_config = None
    elif missing_modules:
        recompute_error = f"missing Python dependencies: {', '.join(missing_modules)}"
        try:
            from .activity import ActivityConfig

            activity_config = ActivityConfig()
        except Exception:
            activity_config = None
    else:
        try:
            from .activity import ActivityConfig

            activity_config = ActivityConfig()
        except Exception:
            activity_config = None

    if recompute_error is not None:
        checks.append(
            StageCheckRecord(
                label="score-activity-bpi recompute",
                status="fail",
                detail="Response/BPI tables should recompute from Suite2p traces and stimulus metadata.",
                expected="recompute completes",
                observed=recompute_error,
            )
        )
    elif not missing_required:
        stim_source = str(result.get("stim_source", ""))
        checks.append(
            StageCheckRecord(
                label="score-activity-bpi true recompute path",
                status="pass" if stim_source != "precomputed" else "fail",
                detail="Audit must not use the precomputed scored-BPI shortcut or control activity outputs.",
                expected="stim_source != precomputed",
                observed=stim_source or "missing",
            )
        )
        table_specs = (
            (
                "scored ROI master",
                paths.functional_registration_dir / "functional_roi_activity_identity.csv",
                result.get("detail_df"),
                StageOutputSpec(
                    "recomputed scored ROI master",
                    "",
                    control_path=str(paths.functional_registration_dir / "functional_roi_activity_identity.csv"),
                    **_csv_comparison_kwargs("functional_roi_activity_identity.csv"),
                ),
            ),
            (
                "ROI activity/BPI cells",
                paths.functional_registration_dir / "functional_roi_activity_bpi_cells.csv",
                result.get("scored_bpi_df"),
                StageOutputSpec(
                    "recomputed ROI activity/BPI cells",
                    "",
                    control_path=str(paths.functional_registration_dir / "functional_roi_activity_bpi_cells.csv"),
                    **_csv_comparison_kwargs("functional_roi_activity_bpi_cells.csv"),
                ),
            ),
            (
                "ROI activity/BPI summary",
                paths.functional_registration_dir / "functional_roi_activity_bpi_summary.csv",
                result.get("summary_df"),
                StageOutputSpec(
                    "recomputed ROI activity/BPI summary",
                    "",
                    control_path=str(paths.functional_registration_dir / "functional_roi_activity_bpi_summary.csv"),
                    **_csv_comparison_kwargs("functional_roi_activity_bpi_summary.csv"),
                ),
            ),
        )
        for label, control_path, computed_table, spec in table_specs:
            if computed_table is None:
                checks.append(
                    StageCheckRecord(
                        label=f"recompute table produced: {label}",
                        status="fail",
                        detail="Activity recompute should return this table.",
                        expected="table present",
                        observed="missing",
                    )
                )
                continue
            _append_recomputed_table_checks(
                checks,
                label=label,
                control_path=control_path,
                computed_table=computed_table,
                spec=spec,
            )

    failed_checks = tuple(f"{check.label}: {check.observed}" for check in checks if check.status == "fail")
    warnings = ()
    errors = missing_required + failed_checks if config.strict else ()
    if (missing_required or failed_checks) and not config.strict:
        warnings = tuple(f"missing required input: {path}" for path in missing_required) + tuple(
            f"failed check: {check}" for check in failed_checks
        )
    status = "fail" if errors else ("warn" if warnings else "pass")
    return StageManifest(
        manifest_version=PIPELINE_MANIFEST_VERSION,
        stage_name="audit-score-activity-bpi",
        fish_id=config.fish_id,
        status=status,
        dry_run=True,
        generated_at=datetime.now(timezone.utc).isoformat(),
        inputs=inputs,
        checks=tuple(checks),
        parameters={
            "local_root": str(config.local_root),
            "owner": config.owner,
            "data_mode": config.data_mode,
            "strict": config.strict,
            "write_manifest": config.write_manifest,
            "pipeline_root": str(paths.pipeline_root),
            "read_only_recompute_audit": True,
            "uses_precomputed_scored_bpi_df": False,
            "control_outputs_are_comparison_only": True,
            "csv_contract": "declared_key_exact_numeric_checks_required; no staged_outputs_written",
            "activity_config": asdict(activity_config) if activity_config is not None else None,
        },
        warnings=warnings,
        errors=errors,
    )


def _score_activity_bpi_identity_input_path(paths: PipelinePaths, identity_input_path: str | Path | None = None) -> Path:
    if identity_input_path not in (None, "", False):
        return Path(identity_input_path)
    return _stage_root(paths, "assign-hcr-identity") / "registration" / "functional_roi_activity_identity.csv"


def _assign_hcr_identity_source_root(paths: PipelinePaths, source_root: str | Path | None = None) -> Path:
    return Path(source_root) if source_root not in (None, "", False) else paths.functional_registration_dir


def _assign_hcr_identity_roi_anatomy_root(paths: PipelinePaths, roi_anatomy_root: str | Path | None = None) -> Path:
    return (
        Path(roi_anatomy_root)
        if roi_anatomy_root not in (None, "", False)
        else roi_to_anatomy_match_root(paths) / "registration"
    )


def _assign_hcr_identity_hcr_anatomy_root(paths: PipelinePaths, hcr_anatomy_root: str | Path | None = None) -> Path:
    return (
        Path(hcr_anatomy_root)
        if hcr_anatomy_root not in (None, "", False)
        else hcr_to_anatomy_registration_root(paths) / "confocal" / "aligned"
    )


def _append_assign_identity_geometry_dependency_checks(
    checks: list[StageCheckRecord],
    *,
    roi_anatomy_root: Path,
    hcr_anatomy_root: Path,
) -> None:
    roi_geometry_path = roi_anatomy_root / "functional_roi_anatomy_matches.csv"
    roi_header = _csv_header(roi_geometry_path) or ()
    roi_rows = _csv_row_count(roi_geometry_path)
    forbidden = tuple(
        column
        for column in ("identity_label", "has_identity_assigned", "response_class", "response_summary_class", "bpi", "bpi_category", "gene")
        if column in roi_header
    )
    hcr_label_count = len(tuple(path for path in hcr_anatomy_root.glob("*_cp_masks_in_2p_labels_uint16.tif") if _is_real_match(path)))
    hcr_final_pair_paths = tuple(path for path in hcr_anatomy_root.glob("*_cp_masks_in_2p_final_pairs.csv") if _is_real_match(path))
    hcr_final_pair_rows = sum((_csv_row_count(path) or 0) for path in hcr_final_pair_paths)
    final_pair_schema_status, final_pair_schema_observed = _hcr_final_pair_schema_status(hcr_final_pair_paths)
    final_pair_acceptance_status, final_pair_acceptance_observed = _hcr_final_pair_acceptance_status(hcr_final_pair_paths)
    checks.extend(
        (
            StageCheckRecord(
                label="assign-hcr-identity staged ROI/anatomy geometry",
                status="pass" if roi_rows and roi_rows > 0 else "fail",
                detail="staged ROI/anatomy geometry table exists before identity assignment",
                observed="missing" if roi_rows is None else str(roi_rows),
                expected=">0",
            ),
            StageCheckRecord(
                label="assign-hcr-identity staged ROI/anatomy geometry-only",
                status="pass" if not forbidden else "fail",
                detail="upstream ROI/anatomy geometry must not already include identity, response, BPI, or gene columns",
                observed="complete" if not forbidden else ",".join(forbidden),
                expected="no identity/response/BPI/gene columns",
            ),
            StageCheckRecord(
                label="assign-hcr-identity staged HCR/anatomy labels",
                status="pass" if hcr_label_count > 0 else "fail",
                detail="staged HCR/anatomy aligned label TIFFs exist before identity assignment",
                observed=str(hcr_label_count),
                expected=">=1",
            ),
            StageCheckRecord(
                label="assign-hcr-identity staged HCR/anatomy final pairs",
                status="pass" if hcr_final_pair_rows > 0 else "fail",
                detail="staged HCR/anatomy accepted final pairs exist before identity assignment",
                observed=str(hcr_final_pair_rows),
                expected=">0",
            ),
            StageCheckRecord(
                label="assign-hcr-identity staged HCR/anatomy final-pair schema",
                status=final_pair_schema_status,
                detail="staged HCR/anatomy final-pair CSVs use the expected schema",
                observed=final_pair_schema_observed,
                expected=",".join(HCR_ALIGNED_PAIR_COLUMNS),
            ),
            StageCheckRecord(
                label="assign-hcr-identity staged HCR/anatomy final-pair acceptance",
                status=final_pair_acceptance_status,
                detail="staged HCR/anatomy final-pair rows are accepted good 1-1 within-gate pairs",
                observed=final_pair_acceptance_observed,
                expected="quality=good,pair_type=1-1,within_gate=True",
            ),
        )
    )


def _append_anatomy_identity_lookup_checks(
    checks: list[StageCheckRecord],
    *,
    recomputed_path: Path,
    control_path: Path,
) -> None:
    recomputed_rows = _csv_dict_rows(recomputed_path) or ()
    control_rows = _csv_dict_rows(control_path) or ()
    checks.append(
        StageCheckRecord(
            label="assign-hcr-identity recomputed anatomy identity lookup",
            status="pass" if recomputed_rows else "fail",
            detail="anatomy identity lookup is recomputed from staged HCR final pairs",
            observed=str(len(recomputed_rows)),
            expected=">0",
        )
    )
    recomputed_by_label = {str(row.get("anat_label")): str(row.get("identity_label")) for row in recomputed_rows}
    control_by_label = {str(row.get("anat_label")): str(row.get("identity_label")) for row in control_rows}
    mismatches = [
        key
        for key in sorted(set(recomputed_by_label) | set(control_by_label))
        if recomputed_by_label.get(key) != control_by_label.get(key)
    ]
    checks.append(
        StageCheckRecord(
            label="assign-hcr-identity anatomy identity lookup parity",
            status="pass" if not mismatches else "fail",
            detail="recomputed anatomy identity labels match the accepted control lookup",
            observed="complete" if not mismatches else ",".join(mismatches[:10]),
            expected="same anat_label -> identity_label mapping",
        )
    )


def _append_roi_identity_master_checks(
    checks: list[StageCheckRecord],
    *,
    recomputed_path: Path,
    control_path: Path,
) -> None:
    recomputed_rows = _csv_dict_rows(recomputed_path) or ()
    control_rows = _csv_dict_rows(control_path) or ()
    checks.append(
        StageCheckRecord(
            label="assign-hcr-identity recomputed ROI identity master",
            status="pass" if recomputed_rows else "fail",
            detail="ROI identity master is recomputed from staged ROI/anatomy geometry plus staged HCR identity lookup",
            observed=str(len(recomputed_rows)),
            expected=">0",
        )
    )
    key_columns = ("plane_idx", "func_label")
    identity_columns = ("selected_anat_label", "anat_label", "identity_label", "has_identity_assigned")

    def keyed(rows: tuple[dict[str, str], ...]) -> dict[tuple[str, str], tuple[str, ...]]:
        out: dict[tuple[str, str], tuple[str, ...]] = {}
        for row in rows:
            key = tuple(str(row.get(column, "")) for column in key_columns)
            out[key] = tuple(str(row.get(column, "")) for column in identity_columns)
        return out

    recomputed_by_key = keyed(tuple(recomputed_rows))
    control_by_key = keyed(tuple(control_rows))
    mismatches = [
        "|".join(key)
        for key in sorted(set(recomputed_by_key) | set(control_by_key))
        if recomputed_by_key.get(key) != control_by_key.get(key)
    ]
    checks.append(
        StageCheckRecord(
            label="assign-hcr-identity ROI identity master parity",
            status="pass" if not mismatches else "fail",
            detail="recomputed ROI identity columns match the accepted control ROI master",
            observed="complete" if not mismatches else ",".join(mismatches[:10]),
            expected="same plane_idx,func_label -> geometry/identity mapping",
        )
    )


def _hcr_high_quality_mask_counts_by_gene(hcr_anatomy_root: Path) -> dict[str, int]:
    from .matching import gene_from_mask

    counts: dict[str, int] = {}
    for meta_path in sorted(hcr_anatomy_root.glob("*_cp_masks_in_2p_warp_meta.json")):
        if not _is_real_match(meta_path):
            continue
        try:
            payload = json.loads(meta_path.read_text())
        except Exception:
            continue
        filter_stats = payload.get("filter_stats")
        if not isinstance(filter_stats, dict):
            continue
        try:
            n_after = int(filter_stats.get("n_labels_after", 0) or 0)
        except Exception:
            n_after = 0
        low_conf = filter_stats.get("low_conf_labels", ())
        try:
            n_low_conf = len(low_conf) if isinstance(low_conf, (list, tuple, set)) else 0
        except Exception:
            n_low_conf = 0
        gene = str(payload.get("gene") or gene_from_mask(meta_path.name))
        counts[gene] = int(counts.get(gene, 0) + max(0, n_after - n_low_conf))
    return counts


def _pipeline_bool_from_any(value: Any) -> bool:
    try:
        import pandas as pd

        if pd.isna(value):
            return False
    except Exception:
        pass
    if isinstance(value, bool):
        return bool(value)
    if isinstance(value, (int, float)):
        try:
            return math.isfinite(float(value)) and float(value) != 0.0
        except Exception:
            return bool(value)
    return str(value).strip().lower() in {"1", "true", "t", "yes", "y"}


def _build_hcr_activity_status_summary_df(status_df: Any, hcr_anatomy_root: Path) -> Any:
    import pandas as pd

    columns = ["gene", "inner_status", "outer_status", "n_labels"]
    if status_df is None or getattr(status_df, "empty", True):
        return pd.DataFrame(columns=columns)
    work = pd.DataFrame(status_df).copy()
    work["gene"] = work.get("gene", pd.Series("", index=work.index)).astype(str)
    functional_status = work.get("functional_status", pd.Series("", index=work.index)).astype(str)
    represented = work.get("represented_on_func_plane", pd.Series(False, index=work.index))
    represented_bool = represented.map(_pipeline_bool_from_any) if hasattr(represented, "map") else pd.Series(False, index=work.index)
    work["inner_status"] = "within functional planes"
    work.loc[(~represented_bool.astype(bool)) | functional_status.eq("out-of-plane anatomy label"), "inner_status"] = "outside functional planes"
    work["outer_status"] = functional_status

    rows = []
    grouped = work.groupby(["gene", "inner_status", "outer_status"], dropna=False).size().reset_index(name="n_labels")
    rows.extend(grouped.loc[:, columns].to_dict("records"))

    accepted_counts = work.groupby("gene", dropna=False).size().astype(int).to_dict()
    hq_counts = _hcr_high_quality_mask_counts_by_gene(hcr_anatomy_root)
    for gene, total_hq in sorted(hq_counts.items()):
        unmatched = int(total_hq) - int(accepted_counts.get(str(gene), 0))
        if unmatched > 0:
            rows.append(
                {
                    "gene": str(gene),
                    "inner_status": "unmatched",
                    "outer_status": "unmatched",
                    "n_labels": int(unmatched),
                }
            )
    if not rows:
        return pd.DataFrame(columns=columns)
    out = pd.DataFrame(rows, columns=columns)
    out["n_labels"] = pd.to_numeric(out["n_labels"], errors="coerce").fillna(0).astype(int)
    return out.sort_values(["gene", "inner_status", "outer_status"]).reset_index(drop=True)


def run_single_fish_assign_hcr_identity_stage(
    config: SingleFishPipelineConfig,
    *,
    source_root: str | Path | None = None,
    roi_anatomy_root: str | Path | None = None,
    hcr_anatomy_root: str | Path | None = None,
    force_recompute: bool = False,
) -> StageManifest:
    paths = resolve_pipeline_paths(config)
    input_root = _assign_hcr_identity_source_root(paths, source_root)
    roi_anatomy_root_path = _assign_hcr_identity_roi_anatomy_root(paths, roi_anatomy_root)
    hcr_anatomy_root_path = _assign_hcr_identity_hcr_anatomy_root(paths, hcr_anatomy_root)
    plane_summary_path = _hcr_activity_replay_plane_refs_path(paths, None)
    from .context import infer_anat_labels_path

    anat_labels_path = infer_anat_labels_path(paths.fish_dir, config.fish_id)
    output_specs = _stage_output_specs(paths, "assign-hcr-identity")
    output_paths = tuple(Path(spec.path) for spec in output_specs)
    source_paths = {filename: input_root / filename for filename in ASSIGN_HCR_IDENTITY_CSVS}
    inputs = (
        describe_manifest_path(roi_anatomy_root_path / "functional_roi_anatomy_matches.csv", label="staged ROI/anatomy geometry matches"),
        describe_glob(roi_anatomy_root_path, "functional_roi_anatomy_match*.csv", required=False, label="staged ROI/anatomy geometry summaries"),
        describe_glob(hcr_anatomy_root_path, "*_cp_masks_in_2p_labels_uint16.tif", label="staged HCR/anatomy aligned labels"),
        describe_glob(hcr_anatomy_root_path, "*_cp_masks_in_2p_final_pairs.csv", label="staged HCR/anatomy final-pair CSVs"),
        describe_manifest_path(plane_summary_path, label="staged functional/anatomy plane refs summary"),
        describe_manifest_path(paths.functional_suite2p_dir, label="Suite2p root for HCR activity replay"),
        describe_glob(paths.functional_suite2p_dir, "plane*/*F.npy", label="Suite2p F traces for HCR activity replay"),
        _optional_manifest_path(anat_labels_path, label="anatomy labels for HCR activity replay"),
    ) + tuple(
        describe_manifest_path(source_paths[filename], label=f"assign-hcr-identity source: {filename}")
        for filename in ASSIGN_HCR_IDENTITY_CSVS
    )
    checks: list[StageCheckRecord] = []
    missing_required = tuple(record.path for record in inputs if record.required and not record.exists)
    existing_outputs = tuple(path for path in output_paths if path.exists())
    _append_assign_identity_geometry_dependency_checks(
        checks,
        roi_anatomy_root=roi_anatomy_root_path,
        hcr_anatomy_root=hcr_anatomy_root_path,
    )
    checks.append(
        StageCheckRecord(
            label="assign-hcr-identity overwrite gate",
            status="pass" if force_recompute or not existing_outputs else "fail",
            detail="Existing staged identity/HCR CSV outputs are not overwritten unless --force-recompute is set.",
            expected="no existing outputs or force_recompute=True",
            observed="none" if not existing_outputs else ",".join(str(path) for path in existing_outputs),
        )
    )
    failed_dependency_checks = tuple(check for check in checks if check.status == "fail")
    if not missing_required and not failed_dependency_checks and (force_recompute or not existing_outputs):
        import pandas as pd

        from .matching import (
            FunctionalRoiIdentityConfig,
            HcrActivityExportConfig,
            attach_identity_to_functional_roi_geometry_df,
            build_anat_identity_lookup_df,
            build_hcr_activity_tables,
            finalize_hcr_activity_export_tables,
            gene_from_mask,
            hcr_response_lookup_from_roi_master_df,
        )
        from .context import resolve_func_polarity
        from .spatial import apply_func_orientation, imread_any
        from .suite2p import Suite2pStageConfig, load_suite2p_stage

        output_by_name = {Path(spec.path).name: Path(spec.path) for spec in output_specs}
        for filename in ASSIGN_HCR_IDENTITY_CSVS:
            if filename in {
                "functional_roi_activity_identity.csv",
                "anatomy_identity_lookup.csv",
                "hcr_activity_status.csv",
                "hcr_activity_status_summary.csv",
                "conf_to_func_pairs_raw.csv",
                "conf_to_func_pairs.csv",
                "hcr_func_candidates.csv",
            }:
                continue
            source_path = source_paths[filename]
            output_path = output_by_name[filename]
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_bytes(source_path.read_bytes())
        lookup_df = build_anat_identity_lookup_df(
            _hcr_match_results_from_staged_final_pairs(hcr_anatomy_root_path),
            gene_order=list(FunctionalRoiIdentityConfig().default_gene_order),
            gene_from_mask_func=gene_from_mask,
        )
        lookup_path = output_by_name["anatomy_identity_lookup.csv"]
        lookup_path.parent.mkdir(parents=True, exist_ok=True)
        lookup_df.to_csv(lookup_path, index=False)
        _append_anatomy_identity_lookup_checks(
            checks,
            recomputed_path=lookup_path,
            control_path=source_paths["anatomy_identity_lookup.csv"],
        )
        roi_identity_df = attach_identity_to_functional_roi_geometry_df(
            pd.read_csv(roi_anatomy_root_path / "functional_roi_anatomy_matches.csv"),
            lookup_df,
            passthrough_df=pd.read_csv(source_paths["functional_roi_activity_identity.csv"]),
            identity_none=FunctionalRoiIdentityConfig().identity_none,
        )
        roi_identity_path = output_by_name["functional_roi_activity_identity.csv"]
        roi_identity_path.parent.mkdir(parents=True, exist_ok=True)
        roi_identity_df.to_csv(roi_identity_path, index=False)
        _append_roi_identity_master_checks(
            checks,
            recomputed_path=roi_identity_path,
            control_path=source_paths["functional_roi_activity_identity.csv"],
        )
        base_plane_refs = load_plane_refs_summary(plane_summary_path)
        anatomy_xy_spacing = _anatomy_xy_spacing_from_voxel_cache(paths)
        replay_plane_refs, transform_report = _overlay_selected_ants_transformlists(
            base_plane_refs,
            _selected_ants_inplane_comparison_path(paths),
            xy_spacing=anatomy_xy_spacing,
        )
        polarity, _polarity_source = resolve_func_polarity(
            config.fish_id,
            paths.matching_metadata_csv,
            fish_dir=paths.fish_dir,
        )

        def _apply_replay_func_orientation(arr: Any) -> Any:
            return apply_func_orientation(arr, polarity=polarity, flip_x=True)

        suite2p_result = load_suite2p_stage(
            plane_refs=replay_plane_refs,
            suite2p_root=paths.functional_suite2p_dir,
            fish_id=config.fish_id,
            polarity=polarity,
            polarity_source=_polarity_source,
            config=Suite2pStageConfig(verbose=False),
        )
        response_lookup = hcr_response_lookup_from_roi_master_df(
            roi_identity_df,
            fish_id=config.fish_id,
        )
        hcr_cfg = HcrActivityExportConfig()
        status_df, raw_df, _analysis_df, candidate_df, plane_meta_df = build_hcr_activity_tables(
            suite2p_result["suite2p_by_ref_idx"],
            replay_plane_refs,
            imread_any(anat_labels_path),
            _hcr_match_results_from_staged_final_pairs(hcr_anatomy_root_path),
            fish_id=config.fish_id,
            active_class=hcr_cfg.active_class,
            inactive_class=hcr_cfg.inactive_class,
            require_overlap=hcr_cfg.require_overlap_func_anat,
            min_overlap=hcr_cfg.min_overlap_func_anat,
            max_dist_um=hcr_cfg.max_dist_func_anat,
            out_of_plane=hcr_cfg.hcr_out_of_plane,
            in_plane_active=hcr_cfg.hcr_in_plane_responsive,
            in_plane_inactive=hcr_cfg.hcr_in_plane_unavailable,
            in_plane_no_func=hcr_cfg.hcr_in_plane_no_func,
            match_policy_version=hcr_cfg.hcr_activity_match_policy,
            selection_rule=hcr_cfg.selection_rule,
            dx_um=float(anatomy_xy_spacing[0]),
            dy_um=float(anatomy_xy_spacing[1]),
            gene_from_mask_func=gene_from_mask,
            response_lookup_df=response_lookup,
            apply_func_orientation_func=_apply_replay_func_orientation,
        )
        final_status_df, final_raw_df, final_analysis_df, final_candidate_df = finalize_hcr_activity_export_tables(
            status_df,
            raw_df,
            candidate_df,
            response_lookup,
            config=hcr_cfg,
            fish_id=config.fish_id,
        )
        final_summary_df = _build_hcr_activity_status_summary_df(final_status_df, hcr_anatomy_root_path)
        hcr_outputs = {
            "hcr_activity_status.csv": final_status_df,
            "hcr_activity_status_summary.csv": final_summary_df,
            "conf_to_func_pairs_raw.csv": final_raw_df,
            "conf_to_func_pairs.csv": final_analysis_df,
            "hcr_func_candidates.csv": final_candidate_df,
        }
        for filename, df in hcr_outputs.items():
            output_path = output_by_name[filename]
            output_path.parent.mkdir(parents=True, exist_ok=True)
            df.to_csv(output_path, index=False)
        checks.append(
            StageCheckRecord(
                label="assign-hcr-identity recomputed HCR activity replay",
                status="pass" if len(final_status_df) > 0 else "fail",
                detail="HCR-centric activity/status/candidate exports are recomputed from staged HCR final pairs and functional/anatomy replay inputs",
                expected=">0 status rows",
                observed=json.dumps(
                    {
                        "status_rows": int(len(final_status_df)),
                        "raw_rows": int(len(final_raw_df)),
                        "analysis_rows": int(len(final_analysis_df)),
                        "candidate_rows": int(len(final_candidate_df)),
                        "summary_rows": int(len(final_summary_df)),
                        "plane_meta_rows": int(len(plane_meta_df)),
                        "selected_ants_planes": int(transform_report.get("backend_counts", {}).get("ants_rigid_affine", 0)),
                    },
                    sort_keys=True,
                ),
            )
        )
        checks.extend(_build_staged_comparison_checks(paths, "assign-hcr-identity"))

    outputs = _stage_output_records(paths, "assign-hcr-identity")
    failed_checks = tuple(f"{check.label}: {check.observed}" for check in checks if check.status == "fail")
    warn_checks = tuple(f"{check.label}: {check.observed}" for check in checks if check.status == "warn")
    errors = missing_required + failed_checks
    warnings = warn_checks
    status = "fail" if errors else ("warn" if warnings else "pass")
    return StageManifest(
        manifest_version=PIPELINE_MANIFEST_VERSION,
        stage_name="assign-hcr-identity",
        fish_id=config.fish_id,
        status=status,
        dry_run=False,
        generated_at=datetime.now(timezone.utc).isoformat(),
        inputs=inputs,
        outputs=outputs,
        checks=tuple(checks),
        parameters={
            "local_root": str(config.local_root),
            "owner": config.owner,
            "data_mode": config.data_mode,
            "strict": config.strict,
            "write_manifest": config.write_manifest,
            "pipeline_root": str(paths.pipeline_root),
            "source_root": str(input_root),
            "source_root_is_explicit": source_root not in (None, "", False),
            "roi_anatomy_root": str(roi_anatomy_root_path),
            "roi_anatomy_root_is_explicit": roi_anatomy_root not in (None, "", False),
            "hcr_anatomy_root": str(hcr_anatomy_root_path),
            "hcr_anatomy_root_is_explicit": hcr_anatomy_root not in (None, "", False),
            "plane_refs_summary_path": str(plane_summary_path),
            "anatomy_labels_path": str(anat_labels_path),
            "force_recompute": bool(force_recompute),
            "source_policy": "functional_roi_activity_identity.csv and anatomy_identity_lookup.csv are recomputed from staged ROI/anatomy geometry and staged HCR final pairs; HCR activity/status/candidate exports are recomputed by label-first HCR replay from staged HCR final pairs, Suite2p, anatomy labels, and staged functional/anatomy plane refs; source registration CSVs are comparison controls",
        },
        warnings=warnings,
        errors=errors,
    )


def _hcr_activity_replay_plane_refs_path(paths: PipelinePaths, plane_refs_summary_path: str | Path | None) -> Path:
    if plane_refs_summary_path not in (None, "", False):
        return Path(plane_refs_summary_path)
    return functional_to_anatomy_registration_root(paths) / "plane_refs_summary.json"


def _hcr_activity_replay_hcr_root(paths: PipelinePaths, hcr_anatomy_root: str | Path | None) -> Path:
    if hcr_anatomy_root not in (None, "", False):
        return Path(hcr_anatomy_root)
    return hcr_to_anatomy_registration_root(paths) / "confocal" / "aligned"


def _hcr_activity_replay_identity_path(paths: PipelinePaths, identity_input_path: str | Path | None) -> Path:
    if identity_input_path not in (None, "", False):
        return Path(identity_input_path)
    staged_score = paths.pipeline_root / "score-activity-bpi" / "registration" / "functional_roi_activity_identity.csv"
    if staged_score.exists():
        return staged_score
    staged_assign = paths.pipeline_root / "assign-hcr-identity" / "registration" / "functional_roi_activity_identity.csv"
    if staged_assign.exists():
        return staged_assign
    return paths.functional_registration_dir / "functional_roi_activity_identity.csv"


def build_single_fish_hcr_activity_replay_manifest(
    config: SingleFishPipelineConfig,
    *,
    plane_refs_summary_path: str | Path | None = None,
    hcr_anatomy_root: str | Path | None = None,
    identity_input_path: str | Path | None = None,
    anatomy_labels_path: str | Path | None = None,
) -> StageManifest:
    paths = resolve_pipeline_paths(config)
    plane_summary_path = _hcr_activity_replay_plane_refs_path(paths, plane_refs_summary_path)
    hcr_root = _hcr_activity_replay_hcr_root(paths, hcr_anatomy_root)
    identity_path = _hcr_activity_replay_identity_path(paths, identity_input_path)

    from .context import infer_anat_labels_path, resolve_func_polarity
    from .spatial import apply_func_orientation, imread_any
    from .suite2p import Suite2pStageConfig, load_suite2p_stage

    anat_labels_path = (
        Path(anatomy_labels_path)
        if anatomy_labels_path not in (None, "", False)
        else infer_anat_labels_path(paths.fish_dir, config.fish_id)
    )
    final_pair_paths = tuple(
        path
        for path in sorted(hcr_root.glob("*_cp_masks_in_2p_final_pairs.csv"))
        if _is_real_match(path)
    )
    inputs = (
        describe_manifest_path(plane_summary_path, label="HCR replay plane refs summary"),
        describe_manifest_path(paths.functional_suite2p_dir, label="HCR replay Suite2p root"),
        describe_glob(paths.functional_suite2p_dir, "plane*/*F.npy", label="HCR replay Suite2p F traces"),
        _optional_manifest_path(anat_labels_path, label="HCR replay anatomy labels"),
        describe_manifest_path(identity_path, label="HCR replay response-aware ROI master"),
        describe_manifest_path(hcr_root, label="HCR replay staged HCR/anatomy root"),
        describe_glob(hcr_root, "*_cp_masks_in_2p_final_pairs.csv", label="HCR replay accepted HCR/anatomy final pairs"),
    )
    checks: list[StageCheckRecord] = []
    missing_required = tuple(record.path for record in inputs if record.required and not record.exists)
    run_error: str | None = None
    replay_counts: dict[str, int] = {}
    control_counts: dict[str, int] = {}
    candidate_key_gap: dict[str, int] = {}
    transform_report: dict[str, Any] = {}
    replay_variant_summaries: list[dict[str, Any]] = []

    checks.append(
        StageCheckRecord(
            label="HCR replay source policy",
            status="pass",
            detail="HCR activity replay starts from accepted HCR/anatomy labels and local Suite2p/anatomy candidates; the ROI master is used only as a response lookup.",
            expected="label-first HCR candidate replay",
            observed="hcr_final_pairs + suite2p + plane_refs + anatomy_labels + response_lookup",
        )
    )
    checks.append(
        StageCheckRecord(
            label="HCR replay final-pair count",
            status="pass" if final_pair_paths else "fail",
            detail="Accepted HCR/anatomy final-pair CSVs define the label-first identified-cell population.",
            expected=">=1 final-pair CSV",
            observed=str(len(final_pair_paths)),
        )
    )

    if not missing_required and final_pair_paths:
        try:
            import pandas as pd

            from .matching import (
                HcrActivityExportConfig,
                build_hcr_activity_tables,
                finalize_hcr_activity_export_tables,
                gene_from_mask,
                hcr_response_lookup_from_roi_master_df,
            )

            base_plane_refs = load_plane_refs_summary(plane_summary_path)
            anatomy_xy_spacing = _anatomy_xy_spacing_from_voxel_cache(paths)
            plane_refs, transform_report = _overlay_selected_ants_transformlists(
                base_plane_refs,
                _selected_ants_inplane_comparison_path(paths),
                xy_spacing=anatomy_xy_spacing,
            )
            ants_planes = int(transform_report.get("backend_counts", {}).get("ants_rigid_affine", 0))
            plane_count = int(transform_report.get("plane_count", 0))
            ants_available = importlib.util.find_spec("ants") is not None
            checks.append(
                StageCheckRecord(
                    label="HCR replay ANTsPyx availability",
                    status="pass" if ants_available or ants_planes == 0 else "fail",
                    detail="ANTs-backed HCR replay requires the AntsPyx import module `ants` when selected ANTs transformlists are used.",
                    expected="ants import available when ants_rigid_affine planes > 0",
                    observed=f"ants_available={ants_available},ants_rigid_affine_planes={ants_planes}",
                )
            )
            checks.append(
                StageCheckRecord(
                    label="HCR replay transform backend",
                    status="pass" if plane_count > 0 and ants_planes == plane_count else "warn",
                    detail="HCR replay should transform functional labels through selected ANTs in-plane transformlists, not NCC-only translation summaries.",
                    expected="all replay planes use ants_rigid_affine",
                    observed=json.dumps(transform_report, sort_keys=True),
                )
            )
            checks.append(
                StageCheckRecord(
                    label="HCR replay ANTs transform files",
                    status="pass" if int(transform_report.get("missing_transform_files", 0)) == 0 else "fail",
                    detail="Selected ANTs transformlist files must exist before replay can apply them through AntsPyx.",
                    expected="missing_transform_files=0",
                    observed=str(transform_report.get("missing_transform_files", 0)),
                )
            )
            polarity, polarity_source = resolve_func_polarity(
                config.fish_id,
                paths.matching_metadata_csv,
                fish_dir=paths.fish_dir,
            )
            def _apply_replay_func_orientation(arr: Any) -> Any:
                return apply_func_orientation(arr, polarity=polarity, flip_x=True)

            suite2p_result = load_suite2p_stage(
                plane_refs=plane_refs,
                suite2p_root=paths.functional_suite2p_dir,
                fish_id=config.fish_id,
                polarity=polarity,
                polarity_source=polarity_source,
                config=Suite2pStageConfig(verbose=False),
            )
            response_lookup = hcr_response_lookup_from_roi_master_df(
                pd.read_csv(identity_path),
                fish_id=config.fish_id,
            )
            cfg = HcrActivityExportConfig()
            anat_labels_all = imread_any(anat_labels_path)
            hcr_match_results = _hcr_match_results_from_staged_final_pairs(hcr_root)
            status_df, raw_df, _analysis_df, candidate_df, plane_meta_df = build_hcr_activity_tables(
                suite2p_result["suite2p_by_ref_idx"],
                plane_refs,
                anat_labels_all,
                hcr_match_results,
                fish_id=config.fish_id,
                active_class=cfg.active_class,
                inactive_class=cfg.inactive_class,
                require_overlap=cfg.require_overlap_func_anat,
                min_overlap=cfg.min_overlap_func_anat,
                max_dist_um=cfg.max_dist_func_anat,
                out_of_plane=cfg.hcr_out_of_plane,
                in_plane_active=cfg.hcr_in_plane_responsive,
                in_plane_inactive=cfg.hcr_in_plane_unavailable,
                in_plane_no_func=cfg.hcr_in_plane_no_func,
                match_policy_version=cfg.hcr_activity_match_policy,
                selection_rule=cfg.selection_rule,
                dx_um=float(anatomy_xy_spacing[0]),
                dy_um=float(anatomy_xy_spacing[1]),
                gene_from_mask_func=gene_from_mask,
                response_lookup_df=response_lookup,
                apply_func_orientation_func=_apply_replay_func_orientation,
            )
            final_status_df, final_raw_df, final_analysis_df, final_candidate_df = finalize_hcr_activity_export_tables(
                status_df,
                raw_df,
                candidate_df,
                response_lookup,
                config=cfg,
                fish_id=config.fish_id,
            )
            replay_tables = {
                "hcr_activity_status.csv": final_status_df,
                "conf_to_func_pairs_raw.csv": final_raw_df,
                "conf_to_func_pairs.csv": final_analysis_df,
                "hcr_func_candidates.csv": final_candidate_df,
            }
            replay_counts = {name: int(len(df)) for name, df in replay_tables.items()}
            replay_counts["plane_meta_rows"] = int(len(plane_meta_df))
            for filename in replay_tables:
                control_path = paths.functional_registration_dir / filename
                if control_path.exists():
                    control_counts[filename] = int(_csv_row_count(control_path) or 0)

            def _keys(df: Any, columns: tuple[str, ...]) -> set[tuple[str, ...]]:
                if df is None or getattr(df, "empty", True):
                    return set()
                missing = [column for column in columns if column not in df.columns]
                if missing:
                    return set()
                return {
                    tuple(str(row[column]) for column in columns)
                    for row in df.loc[:, list(columns)].fillna("").to_dict("records")
                }

            control_candidate_path = paths.functional_registration_dir / "hcr_func_candidates.csv"
            control_candidate_df = None
            if control_candidate_path.exists():
                control_candidate_df = pd.read_csv(control_candidate_path)
                replay_keys = _keys(final_candidate_df, ("gene", "anat_label", "plane_idx", "func_label"))
                control_keys = _keys(control_candidate_df, ("gene", "anat_label", "plane_idx", "func_label"))
                candidate_key_gap = {
                    "replay_keys": int(len(replay_keys)),
                    "control_keys": int(len(control_keys)),
                    "missing_control_keys": int(len(control_keys - replay_keys)),
                    "extra_replay_keys": int(len(replay_keys - control_keys)),
                }

            def _candidate_gap(df: Any) -> dict[str, int]:
                if control_candidate_df is None:
                    return {}
                replay_keys = _keys(df, ("gene", "anat_label", "plane_idx", "func_label"))
                control_keys = _keys(control_candidate_df, ("gene", "anat_label", "plane_idx", "func_label"))
                return {
                    "replay_keys": int(len(replay_keys)),
                    "control_keys": int(len(control_keys)),
                    "missing_control_keys": int(len(control_keys - replay_keys)),
                    "extra_replay_keys": int(len(replay_keys - control_keys)),
                }

            def _row_delta_total(counts: dict[str, int]) -> int:
                total = 0
                for filename, expected in control_counts.items():
                    total += abs(int(counts.get(filename, 0)) - int(expected))
                return int(total)

            replay_variant_summaries.append(
                {
                    "name": "selected_inplane_registration_ants_transformlist",
                    "counts": dict(replay_counts),
                    "candidate_key_gap": dict(candidate_key_gap),
                    "row_delta_total": _row_delta_total(replay_counts),
                    "plane_ref_report": dict(transform_report),
                    "status": "ok",
                }
            )
            for variant_name, variant_refs, variant_report in _hcr_replay_plane_ref_variants(
                paths,
                base_plane_refs,
                plane_refs,
                transform_report,
            )[1:]:
                try:
                    v_status_df, v_raw_df, _v_analysis_df, v_candidate_df, v_plane_meta_df = build_hcr_activity_tables(
                        suite2p_result["suite2p_by_ref_idx"],
                        variant_refs,
                        anat_labels_all,
                        hcr_match_results,
                        fish_id=config.fish_id,
                        active_class=cfg.active_class,
                        inactive_class=cfg.inactive_class,
                        require_overlap=cfg.require_overlap_func_anat,
                        min_overlap=cfg.min_overlap_func_anat,
                        max_dist_um=cfg.max_dist_func_anat,
                        out_of_plane=cfg.hcr_out_of_plane,
                        in_plane_active=cfg.hcr_in_plane_responsive,
                        in_plane_inactive=cfg.hcr_in_plane_unavailable,
                        in_plane_no_func=cfg.hcr_in_plane_no_func,
                        match_policy_version=cfg.hcr_activity_match_policy,
                        selection_rule=cfg.selection_rule,
                        dx_um=float(anatomy_xy_spacing[0]),
                        dy_um=float(anatomy_xy_spacing[1]),
                        gene_from_mask_func=gene_from_mask,
                        response_lookup_df=response_lookup,
                        apply_func_orientation_func=_apply_replay_func_orientation,
                    )
                    v_final_status_df, v_final_raw_df, v_final_analysis_df, v_final_candidate_df = finalize_hcr_activity_export_tables(
                        v_status_df,
                        v_raw_df,
                        v_candidate_df,
                        response_lookup,
                        config=cfg,
                        fish_id=config.fish_id,
                    )
                    v_counts = {
                        "hcr_activity_status.csv": int(len(v_final_status_df)),
                        "conf_to_func_pairs_raw.csv": int(len(v_final_raw_df)),
                        "conf_to_func_pairs.csv": int(len(v_final_analysis_df)),
                        "hcr_func_candidates.csv": int(len(v_final_candidate_df)),
                        "plane_meta_rows": int(len(v_plane_meta_df)),
                    }
                    replay_variant_summaries.append(
                        {
                            "name": variant_name,
                            "counts": v_counts,
                            "candidate_key_gap": _candidate_gap(v_final_candidate_df),
                            "row_delta_total": _row_delta_total(v_counts),
                            "plane_ref_report": dict(variant_report),
                            "status": "ok",
                        }
                    )
                except Exception as variant_exc:
                    replay_variant_summaries.append(
                        {
                            "name": variant_name,
                            "status": "fail",
                            "error": str(variant_exc),
                            "plane_ref_report": dict(variant_report),
                        }
                    )
            ok_variants = [item for item in replay_variant_summaries if item.get("status") == "ok"]
            if ok_variants:
                def _variant_sort_key(item: dict[str, Any]) -> tuple[int, int, int, str]:
                    gap = item.get("candidate_key_gap") or {}
                    missing = int(gap.get("missing_control_keys", 10**9))
                    extra = int(gap.get("extra_replay_keys", 10**9))
                    return (missing + extra, int(item.get("row_delta_total", 10**9)), missing, str(item.get("name", "")))

                best_variant = sorted(ok_variants, key=_variant_sort_key)[0]
                best_gap = best_variant.get("candidate_key_gap") or {}
                checks.append(
                    StageCheckRecord(
                        label="HCR replay best variant candidate parity",
                        status="pass" if best_gap.get("missing_control_keys") == 0 and best_gap.get("extra_replay_keys") == 0 else "warn",
                        detail="Read-only transform-variant scoreboard identifies the closest replay geometry without promoting HCR CSV outputs.",
                        expected="best missing_control_keys=0,extra_replay_keys=0",
                        observed=json.dumps(
                            {
                                "name": best_variant.get("name"),
                                "candidate_key_gap": best_gap,
                                "row_delta_total": best_variant.get("row_delta_total"),
                            },
                            sort_keys=True,
                        ),
                    )
                )

            for filename, observed_count in replay_counts.items():
                if filename == "plane_meta_rows":
                    continue
                expected_count = control_counts.get(filename)
                if expected_count is None:
                    status = "warn"
                    observed = str(observed_count)
                    expected = "control table unavailable"
                else:
                    status = "pass" if observed_count == expected_count else "warn"
                    observed = str(observed_count)
                    expected = str(expected_count)
                checks.append(
                    StageCheckRecord(
                        label=f"HCR replay row count: {filename}",
                        status=status,
                        detail="Read-only label-first HCR activity replay row count compared with accepted control output.",
                        observed=observed,
                        expected=expected,
                    )
                )
            if candidate_key_gap:
                checks.append(
                    StageCheckRecord(
                        label="HCR replay candidate key parity",
                        status="pass" if candidate_key_gap["missing_control_keys"] == 0 and candidate_key_gap["extra_replay_keys"] == 0 else "warn",
                        detail="Candidate-key parity indicates whether staged replay can safely replace copied HCR candidate outputs.",
                        observed=json.dumps(candidate_key_gap, sort_keys=True),
                        expected="missing_control_keys=0,extra_replay_keys=0",
                    )
                )
        except Exception as exc:
            run_error = str(exc)

    if run_error is not None:
        checks.append(
            StageCheckRecord(
                label="HCR activity replay recompute",
                status="fail",
                detail="Read-only HCR-centric replay should run without writing staged HCR CSV outputs.",
                expected="recompute completes",
                observed=run_error,
            )
        )
    elif not missing_required and final_pair_paths:
        checks.append(
            StageCheckRecord(
                label="HCR activity replay recompute",
                status="pass",
                detail="Read-only HCR-centric replay completed in memory; promotion remains disabled until parity is proven.",
                expected="no staged HCR CSV writes",
                observed="completed",
            )
        )

    failed_checks = tuple(f"{check.label}: {check.observed}" for check in checks if check.status == "fail")
    warn_checks = tuple(f"{check.label}: {check.observed}" for check in checks if check.status == "warn")
    errors = missing_required + failed_checks
    warnings = warn_checks
    status = "fail" if errors else ("warn" if warnings else "pass")
    return StageManifest(
        manifest_version=PIPELINE_MANIFEST_VERSION,
        stage_name="audit-hcr-activity-replay",
        fish_id=config.fish_id,
        status=status,
        dry_run=True,
        generated_at=datetime.now(timezone.utc).isoformat(),
        inputs=inputs,
        outputs=(),
        checks=tuple(checks),
        parameters={
            "local_root": str(config.local_root),
            "owner": config.owner,
            "data_mode": config.data_mode,
            "strict": config.strict,
            "write_manifest": config.write_manifest,
            "pipeline_root": str(paths.pipeline_root),
            "plane_refs_summary_path": str(plane_summary_path),
            "hcr_anatomy_root": str(hcr_root),
            "identity_input_path": str(identity_path),
            "anatomy_labels_path": str(anat_labels_path) if anat_labels_path is not None else "",
            "replay_counts": replay_counts,
            "control_counts": control_counts,
            "candidate_key_gap": candidate_key_gap,
            "transform_report": transform_report,
            "replay_variant_summaries": replay_variant_summaries,
            "promotion_enabled": False,
            "source_policy": "label-first HCR replay; ROI master is response lookup only",
        },
        warnings=warnings,
        errors=errors,
    )


def run_single_fish_score_activity_bpi_stage(
    config: SingleFishPipelineConfig,
    *,
    identity_input_path: str | Path | None = None,
    force_recompute: bool = False,
) -> StageManifest:
    paths = resolve_pipeline_paths(config)
    identity_path = _score_activity_bpi_identity_input_path(paths, identity_input_path=identity_input_path)
    output_specs = _stage_output_specs(paths, "score-activity-bpi")
    output_paths = tuple(Path(spec.path) for spec in output_specs)
    inputs = (
        describe_manifest_path(identity_path, label="ROI identity input"),
        describe_manifest_path(paths.functional_suite2p_dir, label="Suite2p root"),
        describe_glob(paths.functional_suite2p_dir, "plane*/*F.npy", label="Suite2p F traces"),
        describe_glob(paths.raw_2p_metadata_dir, "*experiment_log*.csv", label="experiment log CSVs"),
        describe_glob(paths.raw_2p_metadata_dir, "*metadata*.csv", required=False, label="experiment metadata CSVs"),
        describe_glob(paths.raw_2p_metadata_dir, "*planned_schedule*.csv", required=False, label="planned stimulus schedule CSVs"),
        describe_glob(
            paths.functional_preproc_dir,
            "**/*preprocessing_metadata.json",
            required=False,
            label="functional preprocessing session metadata",
        ),
    )
    checks: list[StageCheckRecord] = []
    missing_required = tuple(record.path for record in inputs if record.required and not record.exists)
    existing_outputs = tuple(path for path in output_paths if path.exists())
    missing_modules = tuple(
        module_name
        for module_name in ("pandas", "numpy")
        if importlib.util.find_spec(module_name) is None
    )
    checks.append(
        StageCheckRecord(
            label="score-activity-bpi Python dependencies",
            status="pass" if not missing_modules else "fail",
            detail="Response/BPI scoring requires the scientific Python dependencies used by activity.build_response_bpi_tables.",
            expected="pandas,numpy",
            observed="complete" if not missing_modules else "missing " + ",".join(missing_modules),
        )
    )
    checks.append(
        StageCheckRecord(
            label="score-activity-bpi overwrite gate",
            status="pass" if force_recompute or not existing_outputs else "fail",
            detail="Existing staged score CSV outputs are not overwritten unless --force-recompute is set.",
            expected="no existing outputs or force_recompute=True",
            observed="none" if not existing_outputs else ",".join(str(path) for path in existing_outputs),
        )
    )

    result: dict[str, Any] = {}
    activity_config: Any = None
    run_error: str | None = None
    if not missing_required and not missing_modules and (force_recompute or not existing_outputs):
        try:
            import pandas as pd

            from .activity import ActivityConfig, build_response_bpi_tables

            detail_df = pd.read_csv(identity_path)
            raw_required_columns = ("plane_idx", "func_label", "activity_class", "is_active")
            missing_raw_columns = tuple(column for column in raw_required_columns if column not in detail_df.columns)
            checks.append(
                StageCheckRecord(
                    label="score-activity-bpi raw ROI inputs",
                    status="pass" if not missing_raw_columns else "fail",
                    detail="Scoring requires unscored ROI identity inputs with trace-quality columns, not only scored control columns.",
                    expected="plane_idx,func_label,activity_class,is_active",
                    observed="complete" if not missing_raw_columns else "missing " + ",".join(missing_raw_columns),
                )
            )
            if missing_raw_columns:
                raise RuntimeError(f"ROI identity input missing recompute columns: {', '.join(missing_raw_columns)}")
            result = build_response_bpi_tables(
                detail_df,
                fish_dir=paths.fish_dir,
                fish_id=config.fish_id,
                suite2p_root=paths.functional_suite2p_dir,
                precomputed_scored_bpi_df=None,
            )
            activity_config = ActivityConfig()
        except Exception as exc:
            run_error = str(exc)
            try:
                from .activity import ActivityConfig

                activity_config = ActivityConfig()
            except Exception:
                activity_config = None
    elif missing_modules:
        run_error = f"missing Python dependencies: {', '.join(missing_modules)}"
        try:
            from .activity import ActivityConfig

            activity_config = ActivityConfig()
        except Exception:
            activity_config = None

    if run_error is not None:
        checks.append(
            StageCheckRecord(
                label="score-activity-bpi recompute",
                status="fail",
                detail="Response/BPI tables should recompute from Suite2p traces and stimulus metadata before writing staged outputs.",
                expected="recompute completes",
                observed=run_error,
            )
        )
    elif not missing_required and not missing_modules and (force_recompute or not existing_outputs):
        stim_source = str(result.get("stim_source", ""))
        checks.append(
            StageCheckRecord(
                label="score-activity-bpi true recompute path",
                status="pass" if stim_source != "precomputed" else "fail",
                detail="Writer must not use the precomputed scored-BPI shortcut or copy control activity outputs.",
                expected="stim_source != precomputed",
                observed=stim_source or "missing",
            )
        )
        if stim_source != "precomputed":
            table_outputs = (
                (result.get("detail_df"), output_paths[0]),
                (result.get("scored_bpi_df"), output_paths[1]),
                (result.get("summary_df"), output_paths[2]),
            )
            missing_tables = tuple(str(path) for table, path in table_outputs if table is None)
            if missing_tables:
                checks.append(
                    StageCheckRecord(
                        label="score-activity-bpi output tables produced",
                        status="fail",
                        detail="Activity recompute should return all three staged score CSV tables.",
                        expected="detail_df,scored_bpi_df,summary_df",
                        observed="missing " + ",".join(missing_tables),
                    )
                )
            else:
                for table, path in table_outputs:
                    path.parent.mkdir(parents=True, exist_ok=True)
                    table.to_csv(path, index=False)
                checks.extend(_build_staged_comparison_checks(paths, "score-activity-bpi"))

    outputs = _stage_output_records(paths, "score-activity-bpi")
    failed_checks = tuple(f"{check.label}: {check.observed}" for check in checks if check.status == "fail")
    warn_checks = tuple(f"{check.label}: {check.observed}" for check in checks if check.status == "warn")
    errors = missing_required + failed_checks
    warnings = warn_checks
    status = "fail" if errors else ("warn" if warnings else "pass")
    return StageManifest(
        manifest_version=PIPELINE_MANIFEST_VERSION,
        stage_name="score-activity-bpi",
        fish_id=config.fish_id,
        status=status,
        dry_run=False,
        generated_at=datetime.now(timezone.utc).isoformat(),
        inputs=inputs,
        outputs=outputs,
        checks=tuple(checks),
        parameters={
            "local_root": str(config.local_root),
            "owner": config.owner,
            "data_mode": config.data_mode,
            "strict": config.strict,
            "write_manifest": config.write_manifest,
            "pipeline_root": str(paths.pipeline_root),
            "identity_input_path": str(identity_path),
            "identity_input_is_explicit": identity_input_path not in (None, "", False),
            "force_recompute": bool(force_recompute),
            "uses_precomputed_scored_bpi_df": False,
            "control_outputs_are_comparison_only": True,
            "activity_config": asdict(activity_config) if activity_config is not None else None,
        },
        warnings=warnings,
        errors=errors,
    )


def _canonical_export_source_roots(
    paths: PipelinePaths,
    *,
    score_input_root: str | Path | None = None,
    hcr_input_root: str | Path | None = None,
) -> tuple[Path, Path]:
    score_root = (
        Path(score_input_root)
        if score_input_root not in (None, "", False)
        else _stage_root(paths, "score-activity-bpi") / "registration"
    )
    hcr_root = (
        Path(hcr_input_root)
        if hcr_input_root not in (None, "", False)
        else _stage_root(paths, "assign-hcr-identity") / "registration"
    )
    return score_root, hcr_root


def _canonical_export_source_paths(
    paths: PipelinePaths,
    *,
    score_input_root: str | Path | None = None,
    hcr_input_root: str | Path | None = None,
) -> dict[str, Path]:
    score_root, hcr_root = _canonical_export_source_roots(
        paths,
        score_input_root=score_input_root,
        hcr_input_root=hcr_input_root,
    )
    sources: dict[str, Path] = {}
    for filename in SCORE_ACTIVITY_BPI_CSVS:
        sources[filename] = score_root / filename
    for filename in HCR_IDENTITY_ACTIVITY_CSVS:
        sources[filename] = hcr_root / filename
    return sources


def run_single_fish_export_canonical_tables_stage(
    config: SingleFishPipelineConfig,
    *,
    score_input_root: str | Path | None = None,
    hcr_input_root: str | Path | None = None,
    force_recompute: bool = False,
) -> StageManifest:
    paths = resolve_pipeline_paths(config)
    score_root, hcr_root = _canonical_export_source_roots(
        paths,
        score_input_root=score_input_root,
        hcr_input_root=hcr_input_root,
    )
    source_paths = _canonical_export_source_paths(
        paths,
        score_input_root=score_input_root,
        hcr_input_root=hcr_input_root,
    )
    output_specs = _stage_output_specs(paths, "export-canonical-tables")
    output_paths = tuple(Path(spec.path) for spec in output_specs)
    inputs = tuple(
        describe_manifest_path(source_paths[filename], label=f"canonical export source: {filename}")
        for filename in STAGED_REGISTRATION_CSVS
    )
    checks: list[StageCheckRecord] = []
    missing_required = tuple(record.path for record in inputs if record.required and not record.exists)
    existing_outputs = tuple(path for path in output_paths if path.exists())
    checks.append(
        StageCheckRecord(
            label="export-canonical-tables overwrite gate",
            status="pass" if force_recompute or not existing_outputs else "fail",
            detail="Existing staged canonical CSV outputs are not overwritten unless --force-recompute is set.",
            expected="no existing outputs or force_recompute=True",
            observed="none" if not existing_outputs else ",".join(str(path) for path in existing_outputs),
        )
    )
    if not missing_required and (force_recompute or not existing_outputs):
        output_by_name = {Path(spec.path).name: Path(spec.path) for spec in output_specs}
        for filename in STAGED_REGISTRATION_CSVS:
            source_path = source_paths[filename]
            output_path = output_by_name[filename]
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_bytes(source_path.read_bytes())
        checks.extend(_build_staged_comparison_checks(paths, "export-canonical-tables"))

    outputs = _stage_output_records(paths, "export-canonical-tables")
    failed_checks = tuple(f"{check.label}: {check.observed}" for check in checks if check.status == "fail")
    warn_checks = tuple(f"{check.label}: {check.observed}" for check in checks if check.status == "warn")
    errors = missing_required + failed_checks
    warnings = warn_checks
    status = "fail" if errors else ("warn" if warnings else "pass")
    return StageManifest(
        manifest_version=PIPELINE_MANIFEST_VERSION,
        stage_name="export-canonical-tables",
        fish_id=config.fish_id,
        status=status,
        dry_run=False,
        generated_at=datetime.now(timezone.utc).isoformat(),
        inputs=inputs,
        outputs=outputs,
        checks=tuple(checks),
        parameters={
            "local_root": str(config.local_root),
            "owner": config.owner,
            "data_mode": config.data_mode,
            "strict": config.strict,
            "write_manifest": config.write_manifest,
            "pipeline_root": str(paths.pipeline_root),
            "score_input_root": str(score_root),
            "hcr_input_root": str(hcr_root),
            "score_input_root_is_explicit": score_input_root not in (None, "", False),
            "hcr_input_root_is_explicit": hcr_input_root not in (None, "", False),
            "force_recompute": bool(force_recompute),
            "source_policy": "score CSVs from staged score root; HCR/activity CSVs from staged assign root unless explicit roots are supplied",
        },
        warnings=warnings,
        errors=errors,
    )


def _make_qa_report_summary(paths: PipelinePaths, *, canonical_root: Path) -> dict[str, Any]:
    canonical_tables = []
    for filename in STAGED_REGISTRATION_CSVS:
        path = canonical_root / filename
        canonical_tables.append(
            {
                "filename": filename,
                "path": str(path),
                "exists": path.exists(),
                "rows": _csv_row_count(path),
            }
        )
    stage_outputs = []
    for stage_name in ("assign-hcr-identity", "score-activity-bpi", "export-canonical-tables", "make-figures"):
        specs = _stage_output_specs(paths, stage_name)
        existing = sum(1 for spec in specs if Path(spec.path).exists())
        stage_outputs.append(
            {
                "stage_name": stage_name,
                "declared_outputs": len(specs),
                "existing_outputs": existing,
                "status": "complete" if existing == len(specs) else ("not_started" if existing == 0 else "partial"),
            }
        )
    return {
        "fish_id": paths.fish_dir.name,
        "canonical_root": str(canonical_root),
        "canonical_tables": canonical_tables,
        "stage_outputs": stage_outputs,
    }


def _make_qa_report_markdown(summary: dict[str, Any]) -> str:
    lines = [
        f"# Single-Fish QA Report: {summary['fish_id']}",
        "",
        f"Canonical table root: `{summary['canonical_root']}`",
        "",
        "## Staged Output Status",
        "",
        "| Stage | Existing / Declared | Status |",
        "| --- | ---: | --- |",
    ]
    for stage in summary["stage_outputs"]:
        lines.append(
            f"| `{stage['stage_name']}` | {stage['existing_outputs']} / {stage['declared_outputs']} | {stage['status']} |"
        )
    lines.extend(
        [
            "",
            "## Canonical Tables",
            "",
            "| Table | Rows | Present |",
            "| --- | ---: | --- |",
        ]
    )
    for table in summary["canonical_tables"]:
        rows = "missing" if table["rows"] is None else str(table["rows"])
        lines.append(f"| `{table['filename']}` | {rows} | {table['exists']} |")
    lines.extend(
        [
            "",
            "## Notes",
            "",
            "- This report summarizes staged pipeline artifacts for review; it does not recompute matching, identity, response, BPI, or figures.",
            "- Use `compare-staged` for detailed table and figure parity checks.",
            "",
        ]
    )
    return "\n".join(lines)


def run_single_fish_make_qa_report_stage(
    config: SingleFishPipelineConfig,
    *,
    canonical_input_root: str | Path | None = None,
    force_recompute: bool = False,
) -> StageManifest:
    paths = resolve_pipeline_paths(config)
    canonical_root = (
        Path(canonical_input_root)
        if canonical_input_root not in (None, "", False)
        else _stage_root(paths, "export-canonical-tables") / "registration"
    )
    output_specs = _stage_output_specs(paths, "make-qa-report")
    output_paths = tuple(Path(spec.path) for spec in output_specs)
    inputs = (
        describe_glob(canonical_root, "*.csv", label="make-qa-report staged canonical CSV inputs"),
    )
    checks: list[StageCheckRecord] = []
    missing_required = tuple(record.path for record in inputs if record.required and not record.exists)
    existing_outputs = tuple(path for path in output_paths if path.exists())
    checks.append(
        StageCheckRecord(
            label="make-qa-report overwrite gate",
            status="pass" if force_recompute or not existing_outputs else "fail",
            detail="Existing staged QA report outputs are not overwritten unless --force-recompute is set.",
            expected="no existing outputs or force_recompute=True",
            observed="none" if not existing_outputs else ",".join(str(path) for path in existing_outputs),
        )
    )
    if not missing_required and (force_recompute or not existing_outputs):
        report_dir = _stage_root(paths, "make-qa-report")
        report_dir.mkdir(parents=True, exist_ok=True)
        summary = _make_qa_report_summary(paths, canonical_root=canonical_root)
        (report_dir / "qa_report_summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
        (report_dir / "qa_report.md").write_text(_make_qa_report_markdown(summary))
        checks.extend(_build_staged_comparison_checks(paths, "make-qa-report"))

    outputs = _stage_output_records(paths, "make-qa-report")
    failed_checks = tuple(f"{check.label}: {check.observed}" for check in checks if check.status == "fail")
    warn_checks = tuple(f"{check.label}: {check.observed}" for check in checks if check.status == "warn")
    errors = missing_required + failed_checks
    warnings = warn_checks
    status = "fail" if errors else ("warn" if warnings else "pass")
    return StageManifest(
        manifest_version=PIPELINE_MANIFEST_VERSION,
        stage_name="make-qa-report",
        fish_id=config.fish_id,
        status=status,
        dry_run=False,
        generated_at=datetime.now(timezone.utc).isoformat(),
        inputs=inputs,
        outputs=outputs,
        checks=tuple(checks),
        parameters={
            "local_root": str(config.local_root),
            "owner": config.owner,
            "data_mode": config.data_mode,
            "strict": config.strict,
            "write_manifest": config.write_manifest,
            "pipeline_root": str(paths.pipeline_root),
            "canonical_input_root": str(canonical_root),
            "canonical_input_root_is_explicit": canonical_input_root not in (None, "", False),
            "force_recompute": bool(force_recompute),
            "source_policy": "generated QA report summarizes staged canonical tables and declared post-preprocessing outputs",
        },
        warnings=warnings,
        errors=errors,
    )


def _make_figures_source_roots(
    paths: PipelinePaths,
    *,
    canonical_input_root: str | Path | None = None,
    figure_input_root: str | Path | None = None,
) -> tuple[Path, Path]:
    canonical_root = (
        Path(canonical_input_root)
        if canonical_input_root not in (None, "", False)
        else _stage_root(paths, "export-canonical-tables") / "registration"
    )
    figure_root = Path(figure_input_root) if figure_input_root not in (None, "", False) else paths.plots_dir
    return canonical_root, figure_root


def _render_package_owned_single_fish_figures(
    *,
    fish_id: str,
    canonical_root: Path,
    output_dir: Path,
) -> None:
    from codeants_2pf_hcr.plots.analysis import render_single_fish_50l_responsive_identity_donut
    from codeants_2pf_hcr.plots.hcr import render_single_fish_hcr_anatomy_coexpression_summary

    master_csv = canonical_root / "functional_roi_activity_identity.csv"
    conf_func_csv = canonical_root / "conf_to_func_pairs.csv"
    status_csv = canonical_root / "hcr_activity_status.csv"
    output_dir.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory(prefix="codeants-make-figures-") as tmpdir:
        temp_reg = Path(tmpdir) / fish_id / "03_analysis" / "functional" / "registration"
        temp_reg.mkdir(parents=True, exist_ok=True)
        for source in (master_csv, conf_func_csv):
            (temp_reg / source.name).write_bytes(source.read_bytes())
        render_single_fish_50l_responsive_identity_donut(
            fish_id=fish_id,
            master_csv=temp_reg / master_csv.name,
            conf_func_csv=temp_reg / conf_func_csv.name,
            outdir=output_dir,
        )

    render_single_fish_hcr_anatomy_coexpression_summary(
        fish_id=fish_id,
        status_csv=status_csv,
        outdir=output_dir,
    )

    try:
        import matplotlib.pyplot as plt
    except Exception:
        return
    plt.close("all")


def run_single_fish_make_figures_stage(
    config: SingleFishPipelineConfig,
    *,
    canonical_input_root: str | Path | None = None,
    figure_input_root: str | Path | None = None,
    force_recompute: bool = False,
) -> StageManifest:
    paths = resolve_pipeline_paths(config)
    canonical_root, figure_root = _make_figures_source_roots(
        paths,
        canonical_input_root=canonical_input_root,
        figure_input_root=figure_input_root,
    )
    output_specs = _stage_output_specs(paths, "make-figures")
    output_paths = tuple(Path(spec.path) for spec in output_specs)
    figure_sources = {filename: figure_root / filename for filename in LEGACY_COPIED_FIGURE_FILES}
    inputs = (
        describe_glob(canonical_root, "*.csv", label="make-figures staged canonical CSV inputs"),
        *(
            describe_manifest_path(canonical_root / filename, label=f"make-figures canonical render input: {filename}")
            for filename in (
                "functional_roi_activity_identity.csv",
                "conf_to_func_pairs.csv",
                "hcr_activity_status.csv",
            )
        ),
        *(
            describe_manifest_path(figure_sources[filename], label=f"make-figures legacy source figure: {filename}")
            for filename in LEGACY_COPIED_FIGURE_FILES
        ),
    )
    checks: list[StageCheckRecord] = []
    missing_required = tuple(record.path for record in inputs if record.required and not record.exists)
    existing_outputs = tuple(path for path in output_paths if path.exists())
    checks.append(
        StageCheckRecord(
            label="make-figures overwrite gate",
            status="pass" if force_recompute or not existing_outputs else "fail",
            detail="Existing staged figure outputs are not overwritten unless --force-recompute is set.",
            expected="no existing outputs or force_recompute=True",
            observed="none" if not existing_outputs else ",".join(str(path) for path in existing_outputs),
        )
    )
    if not missing_required and (force_recompute or not existing_outputs):
        output_by_name = {Path(spec.path).name: Path(spec.path) for spec in output_specs}
        for filename in LEGACY_COPIED_FIGURE_FILES:
            source_path = figure_sources[filename]
            output_path = output_by_name[filename]
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_bytes(source_path.read_bytes())
        _render_package_owned_single_fish_figures(
            fish_id=config.fish_id,
            canonical_root=canonical_root,
            output_dir=output_by_name[RENDERED_FIGURE_FILES[0]].parent,
        )
        checks.extend(_build_staged_comparison_checks(paths, "make-figures"))

    outputs = _stage_output_records(paths, "make-figures")
    failed_checks = tuple(f"{check.label}: {check.observed}" for check in checks if check.status == "fail")
    warn_checks = tuple(f"{check.label}: {check.observed}" for check in checks if check.status == "warn")
    errors = missing_required + failed_checks
    warnings = warn_checks
    status = "fail" if errors else ("warn" if warnings else "pass")
    return StageManifest(
        manifest_version=PIPELINE_MANIFEST_VERSION,
        stage_name="make-figures",
        fish_id=config.fish_id,
        status=status,
        dry_run=False,
        generated_at=datetime.now(timezone.utc).isoformat(),
        inputs=inputs,
        outputs=outputs,
        checks=tuple(checks),
        parameters={
            "local_root": str(config.local_root),
            "owner": config.owner,
            "data_mode": config.data_mode,
            "strict": config.strict,
            "write_manifest": config.write_manifest,
            "pipeline_root": str(paths.pipeline_root),
            "canonical_input_root": str(canonical_root),
            "figure_input_root": str(figure_root),
            "canonical_input_root_is_explicit": canonical_input_root not in (None, "", False),
            "figure_input_root_is_explicit": figure_input_root not in (None, "", False),
            "force_recompute": bool(force_recompute),
            "rendered_figures": RENDERED_FIGURE_FILES,
            "legacy_copied_figures": LEGACY_COPIED_FIGURE_FILES,
            "source_policy": "package-owned renderers produce the responsive identity donut and HCR anatomy coexpression summary from staged canonical CSVs; remaining declared figures are copied from the legacy figure input root until their full render inputs are staged",
        },
        warnings=warnings,
        errors=errors,
    )


def build_single_fish_downstream_stage_manifest(
    config: SingleFishPipelineConfig,
    stage_name: str,
) -> StageManifest:
    if stage_name not in POST_PREPROCESSING_STAGE_NAMES:
        raise ValueError(
            f"unsupported downstream stage {stage_name!r}; expected one of {', '.join(POST_PREPROCESSING_STAGE_NAMES)}"
        )
    paths = resolve_pipeline_paths(config)
    inputs = _stage_input_records(paths, stage_name)
    outputs = _stage_output_records(paths, stage_name)
    checks = _build_downstream_stage_checks(paths, stage_name)
    missing_required = tuple(record.path for record in outputs if record.required and not record.exists)
    failed_checks = tuple(f"{check.label}: {check.observed}" for check in checks if check.status == "fail")
    warnings = ()
    errors = missing_required + failed_checks if config.strict else ()
    if (missing_required or failed_checks) and not config.strict:
        warnings = tuple(f"missing required output: {path}" for path in missing_required) + tuple(
            f"failed check: {check}" for check in failed_checks
        )
    status = "fail" if errors else ("warn" if warnings else "pass")
    return StageManifest(
        manifest_version=PIPELINE_MANIFEST_VERSION,
        stage_name=stage_name,
        fish_id=config.fish_id,
        status=status,
        dry_run=True,
        generated_at=datetime.now(timezone.utc).isoformat(),
        inputs=inputs,
        outputs=outputs,
        checks=checks,
        parameters={
            "local_root": str(config.local_root),
            "owner": config.owner,
            "data_mode": config.data_mode,
            "strict": config.strict,
            "write_manifest": config.write_manifest,
            "pipeline_root": str(paths.pipeline_root),
            "read_only_inventory": True,
        },
        warnings=warnings,
        errors=errors,
    )


def build_single_fish_downstream_stage_manifests(
    config: SingleFishPipelineConfig,
) -> tuple[StageManifest, ...]:
    return tuple(
        build_single_fish_downstream_stage_manifest(config, stage_name)
        for stage_name in POST_PREPROCESSING_STAGE_NAMES
    )


def _summarize_downstream_stage_manifest(manifest: StageManifest, persisted: PersistedManifestStatus) -> dict[str, Any]:
    missing_required_outputs = [
        record.label
        for record in manifest.outputs
        if record.required and not record.exists
    ]
    failed_checks = [check.label for check in manifest.checks if check.status == "fail"]
    return {
        "stage_name": manifest.stage_name,
        "status": manifest.status,
        "input_records": len(manifest.inputs),
        "output_records": len(manifest.outputs),
        "check_records": len(manifest.checks),
        "missing_required_outputs": missing_required_outputs,
        "failed_checks": failed_checks,
        "persisted_manifest": persisted.to_dict(),
    }


def _stage_has_existing_outputs(paths: PipelinePaths, stage_name: str) -> bool:
    stage_root = _stage_root(paths, stage_name)
    return stage_root.exists() and any(_is_real_match(path) for path in stage_root.rglob("*"))


def build_single_fish_stage_status(config: SingleFishPipelineConfig, stage_name: str) -> dict[str, Any]:
    paths = resolve_pipeline_paths(config)
    manifest = build_single_fish_downstream_stage_manifest(config, stage_name)
    persisted = compare_persisted_manifest(stage_manifest_path(paths, stage_name), manifest)
    payload = _summarize_downstream_stage_manifest(manifest, persisted)
    if not _stage_has_existing_outputs(paths, stage_name):
        payload["status"] = "not_started"
    elif manifest.status == "pass" and persisted.status == "stale":
        payload["status"] = "warn"
    elif persisted.status == "fail":
        payload["status"] = "fail"
    return payload


def _build_audit_checks(paths: PipelinePaths) -> tuple[StageCheckRecord, ...]:
    checks: list[StageCheckRecord] = []
    plane_dirs = _suite2p_plane_dirs(paths)
    checks.append(
        StageCheckRecord(
            label="Suite2p plane directories",
            status="pass" if plane_dirs else "fail",
            detail="Discovered Suite2p plane directories.",
            expected=">=1",
            observed=str(len(plane_dirs)),
        )
    )
    suffixes = ("ops.npy", "F.npy", "Fneu.npy", "iscell.npy", "stat.npy")
    for plane_dir in plane_dirs:
        missing = [suffix for suffix in suffixes if not _real_files_matching(plane_dir, f"*{suffix}")]
        checks.append(
            StageCheckRecord(
                label=f"Suite2p core files: {plane_dir.name}",
                status="fail" if missing else "pass",
                detail="Each Suite2p plane should have core trace/ROI arrays.",
                expected="ops/F/Fneu/iscell/stat",
                observed="missing " + ",".join(missing) if missing else "complete",
            )
        )

    tforms_path = paths.functional_registration_dir / "tforms_by_plane.csv"
    tforms_rows = _csv_row_count(tforms_path)
    if tforms_rows is None:
        checks.append(
            StageCheckRecord(
                label="tforms rows vs Suite2p planes",
                status="fail",
                detail="Functional transform table is missing.",
                expected=str(len(plane_dirs)),
                observed="missing",
            )
        )
    else:
        checks.append(
            StageCheckRecord(
                label="tforms rows vs Suite2p planes",
                status="pass" if tforms_rows == len(plane_dirs) else "fail",
                detail="Transform rows should match discovered Suite2p plane directories.",
                expected=str(len(plane_dirs)),
                observed=str(tforms_rows),
            )
        )

    required_csvs = (
        ("ROI identity master rows", paths.functional_registration_dir / "functional_roi_activity_identity.csv"),
        ("HCR activity status rows", paths.functional_registration_dir / "hcr_activity_status.csv"),
        ("responsive HCR/function pair rows", paths.functional_registration_dir / "conf_to_func_pairs.csv"),
        ("ROI activity/BPI cell rows", paths.functional_registration_dir / "functional_roi_activity_bpi_cells.csv"),
    )
    for label, path in required_csvs:
        row_count = _csv_row_count(path)
        checks.append(
            StageCheckRecord(
                label=label,
                status="pass" if row_count is not None and row_count > 0 else "fail",
                detail="Required canonical/control CSV should contain data rows.",
                expected=">0",
                observed="missing" if row_count is None else str(row_count),
            )
        )
    for filename, required_columns in REQUIRED_CSV_COLUMNS.items():
        path = paths.functional_registration_dir / filename
        header = _csv_header(path)
        if header is None:
            checks.append(
                StageCheckRecord(
                    label=f"CSV schema: {filename}",
                    status="fail",
                    detail="Required CSV is missing.",
                    expected=",".join(required_columns),
                    observed="missing",
                )
            )
            continue
        missing_columns = tuple(column for column in required_columns if column not in header)
        checks.append(
            StageCheckRecord(
                label=f"CSV schema: {filename}",
                status="fail" if missing_columns else "pass",
                detail="Required columns should be present.",
                expected=",".join(required_columns),
                observed="missing " + ",".join(missing_columns) if missing_columns else "complete",
            )
        )
    tforms_rows_data = _csv_dict_rows(paths.functional_registration_dir / "tforms_by_plane.csv")
    roi_rows = _csv_dict_rows(paths.functional_registration_dir / "functional_roi_activity_identity.csv")
    bpi_rows = _csv_dict_rows(paths.functional_registration_dir / "functional_roi_activity_bpi_cells.csv")
    candidate_rows = _csv_dict_rows(paths.functional_registration_dir / "hcr_func_candidates.csv")
    pair_rows = _csv_dict_rows(paths.functional_registration_dir / "conf_to_func_pairs.csv")
    suite2p_planes = _plane_dir_indices(plane_dirs)
    tforms_planes = _column_set(tforms_rows_data, "plane_index")
    roi_planes = _column_set(roi_rows, "plane_idx")
    bpi_planes = _column_set(bpi_rows, "plane_idx")
    candidate_planes = _column_set(candidate_rows, "plane_idx")
    pair_planes = _column_set(pair_rows, "plane")
    _append_set_equality_check(
        checks,
        label="Suite2p planes vs tforms planes",
        left=suite2p_planes,
        right=tforms_planes,
        left_name="Suite2p",
        right_name="tforms",
    )
    _append_set_equality_check(
        checks,
        label="ROI master planes vs tforms planes",
        left=roi_planes,
        right=tforms_planes,
        left_name="ROI master",
        right_name="tforms",
    )
    _append_set_equality_check(
        checks,
        label="BPI cell planes vs ROI master planes",
        left=bpi_planes,
        right=roi_planes,
        left_name="BPI cells",
        right_name="ROI master",
    )
    checks.append(
        StageCheckRecord(
            label="HCR candidate planes subset ROI master planes",
            status="pass" if candidate_planes is not None and roi_planes is not None and candidate_planes <= roi_planes else "fail",
            detail="HCR candidate planes should be represented in the ROI master.",
            expected=f"subset of {_format_set(roi_planes)}",
            observed=_format_set(candidate_planes),
        )
    )
    checks.append(
        StageCheckRecord(
            label="responsive pair planes subset ROI master planes",
            status="pass" if pair_planes is not None and roi_planes is not None and pair_planes <= roi_planes else "fail",
            detail="Responsive HCR/function pair planes should be represented in the ROI master.",
            expected=f"subset of {_format_set(roi_planes)}",
            observed=_format_set(pair_planes),
        )
    )
    roi_key_duplicates = _duplicate_key_count(roi_rows, ("plane_idx", "func_label"))
    checks.append(
        StageCheckRecord(
            label="ROI master unique plane/func keys",
            status="pass" if roi_key_duplicates == 0 else "fail",
            detail="ROI-centric master table should have one row per functional ROI key.",
            expected="0 duplicates",
            observed="missing" if roi_key_duplicates is None else str(roi_key_duplicates),
        )
    )
    bpi_key_duplicates = _duplicate_key_count(bpi_rows, ("plane_idx", "func_label"))
    checks.append(
        StageCheckRecord(
            label="BPI cells unique plane/func keys",
            status="pass" if bpi_key_duplicates == 0 else "fail",
            detail="BPI cells table should have one row per functional ROI key.",
            expected="0 duplicates",
            observed="missing" if bpi_key_duplicates is None else str(bpi_key_duplicates),
        )
    )
    roi_keys = _key_set(roi_rows, ("plane_idx", "func_label"))
    bpi_keys = _key_set(bpi_rows, ("plane_idx", "func_label"))
    _append_key_subset_check(
        checks,
        label="BPI cell keys match ROI master keys",
        subset=bpi_keys,
        superset=roi_keys,
        subset_name="BPI cell",
        superset_name="ROI master",
    )
    _append_key_subset_check(
        checks,
        label="ROI master keys represented in BPI cells",
        subset=roi_keys,
        superset=bpi_keys,
        subset_name="ROI master",
        superset_name="BPI cell",
    )
    _append_key_subset_check(
        checks,
        label="HCR candidate keys subset ROI master keys",
        subset=_key_set(candidate_rows, ("plane_idx", "func_label")),
        superset=roi_keys,
        subset_name="HCR candidate",
        superset_name="ROI master",
    )
    _append_key_subset_check(
        checks,
        label="responsive pair keys subset ROI master keys",
        subset=_key_set(pair_rows, ("plane", "func_label")),
        superset=roi_keys,
        subset_name="responsive pair",
        superset_name="ROI master",
    )
    hcr_status_rows = _csv_dict_rows(paths.functional_registration_dir / "hcr_activity_status.csv")
    selected_trace_count = _count_value(hcr_status_rows, "selected_for_trace_export", "True")
    pair_count = None if pair_rows is None else len(pair_rows)
    checks.append(
        StageCheckRecord(
            label="HCR trace export selections match responsive pairs",
            status="pass" if selected_trace_count is not None and pair_count is not None and selected_trace_count == pair_count else "fail",
            detail="Selected HCR status rows should match exported responsive pair rows.",
            expected="conf_to_func_pairs row count",
            observed="missing" if selected_trace_count is None or pair_count is None else f"{selected_trace_count}/{pair_count}",
        )
    )
    active_pair_count = _count_value(pair_rows, "response_is_active", "True")
    selected_pair_count = _count_value(pair_rows, "is_selected_for_analysis", "True")
    checks.append(
        StageCheckRecord(
            label="responsive pairs are active selected rows",
            status="pass" if pair_count is not None and active_pair_count == pair_count and selected_pair_count == pair_count else "fail",
            detail="Rows in conf_to_func_pairs should be selected responsive exports.",
            expected="all rows active and selected",
            observed="missing" if pair_count is None else f"active={active_pair_count}/{pair_count}; selected={selected_pair_count}/{pair_count}",
        )
    )
    for column in ("has_unique_anat_match", "has_identity_assigned", "response_is_active", "suite2p_is_cell"):
        _append_domain_check(
            checks,
            label=f"ROI master boolean domain: {column}",
            rows=roi_rows,
            column=column,
            allowed_values=BOOLEAN_DOMAIN,
        )
    _append_domain_check(
        checks,
        label="BPI cells boolean domain: response_is_active",
        rows=bpi_rows,
        column="response_is_active",
        allowed_values=BOOLEAN_DOMAIN,
    )
    _append_domain_check(
        checks,
        label="HCR status boolean domain: selected_for_trace_export",
        rows=hcr_status_rows,
        column="selected_for_trace_export",
        allowed_values=BOOLEAN_DOMAIN,
    )
    _append_domain_check(
        checks,
        label="responsive pairs boolean domain: is_selected_for_analysis",
        rows=pair_rows,
        column="is_selected_for_analysis",
        allowed_values=BOOLEAN_DOMAIN,
    )
    _append_domain_check(
        checks,
        label="ROI master response class domain",
        rows=roi_rows,
        column="response_class",
        allowed_values=RESPONSE_CLASS_DOMAIN,
    )
    _append_domain_check(
        checks,
        label="ROI master response summary domain",
        rows=roi_rows,
        column="response_summary_class",
        allowed_values=RESPONSE_SUMMARY_DOMAIN,
    )
    _append_domain_check(
        checks,
        label="BPI cells response class domain",
        rows=bpi_rows,
        column="response_class",
        allowed_values=RESPONSE_CLASS_DOMAIN,
    )
    _append_domain_check(
        checks,
        label="ROI master BPI category domain",
        rows=roi_rows,
        column="bpi_category",
        allowed_values=BPI_CATEGORY_DOMAIN,
    )
    _append_domain_check(
        checks,
        label="BPI cells BPI category domain",
        rows=bpi_rows,
        column="bpi_category",
        allowed_values=BPI_CATEGORY_DOMAIN,
    )
    _append_numeric_check(checks, label="ROI master BPI numeric values", rows=roi_rows, column="bpi")
    _append_numeric_check(checks, label="BPI cells BPI numeric values", rows=bpi_rows, column="bpi")
    _append_zero_violation_check(
        checks,
        label="ROI geometry match flag matches selected anatomy label",
        rows=roi_rows,
        detail="has_unique_anat_match should match whether selected_anat_label is populated.",
        predicate=lambda row: _bool_value(row.get("has_unique_anat_match", "")) == (str(row.get("selected_anat_label", "")).strip() != ""),
    )
    _append_zero_violation_check(
        checks,
        label="ROI identity flag matches identity label",
        rows=roi_rows,
        detail="has_identity_assigned should match whether identity_label is populated.",
        predicate=lambda row: _bool_value(row.get("has_identity_assigned", "")) == (str(row.get("identity_label", "")).strip() != ""),
    )
    _append_zero_violation_check(
        checks,
        label="ROI identity implies unique anatomy match",
        rows=roi_rows,
        detail="Identity assignment should only happen after a unique anatomy match.",
        predicate=lambda row: (not _bool_value(row.get("has_identity_assigned", ""))) or bool(_bool_value(row.get("has_unique_anat_match", ""))),
    )
    for label, rows in (
        ("ROI master", roi_rows),
        ("BPI cells", bpi_rows),
        ("HCR status", hcr_status_rows),
        ("responsive pairs", pair_rows),
    ):
        _append_zero_violation_check(
            checks,
            label=f"{label} response active matches summary class",
            rows=rows,
            detail="response_is_active should match Responsive neurons summary membership.",
            predicate=lambda row: _bool_value(row.get("response_is_active", "")) == (row.get("response_summary_class", "") == "Responsive neurons"),
        )
        _append_zero_violation_check(
            checks,
            label=f"{label} response active matches response class",
            rows=rows,
            detail="response_is_active should be false for low activity or response unavailable classes and true otherwise.",
            predicate=lambda row: _bool_value(row.get("response_is_active", "")) == (row.get("response_class", "") not in {"low activity", "response unavailable", ""}),
        )
    _append_zero_violation_check(
        checks,
        label="HCR status trace export matches response active",
        rows=hcr_status_rows,
        detail="selected_for_trace_export should match response_is_active.",
        predicate=lambda row: _bool_value(row.get("selected_for_trace_export", "")) == _bool_value(row.get("response_is_active", "")),
    )
    _append_zero_violation_check(
        checks,
        label="responsive pair selected matches response active",
        rows=pair_rows,
        detail="is_selected_for_analysis should match response_is_active in trace-ready pairs.",
        predicate=lambda row: _bool_value(row.get("is_selected_for_analysis", "")) == _bool_value(row.get("response_is_active", "")),
    )
    staged_canonical_dir = _stage_root(paths, "export-canonical-tables") / "registration"
    for filename in (
        "functional_roi_activity_identity.csv",
        "functional_roi_activity_bpi_cells.csv",
        "functional_roi_activity_bpi_summary.csv",
        "hcr_activity_status.csv",
        "hcr_activity_status_summary.csv",
        "conf_to_func_pairs.csv",
        "conf_to_func_pairs_raw.csv",
        "hcr_func_candidates.csv",
    ):
        _append_optional_csv_parity_check(
            checks,
            label=f"staged canonical CSV parity: {filename}",
            control_path=paths.functional_registration_dir / filename,
            staged_path=staged_canonical_dir / filename,
        )
    staged_figures_dir = _stage_root(paths, "make-figures") / "04_plots"
    for filename in (
        "compound_50j_56i_unified.png",
        "bpi_all_pairs.png",
        "per_gene_stimulus_trace_with_hcr_status_56h.png",
        "single_fish_50l_responsive_identity_donut.png",
        "single_fish_hcr_anatomy_coexpression_summary.png",
    ):
        _append_optional_file_parity_check(
            checks,
            label=f"staged figure presence: {filename}",
            control_path=paths.plots_dir / filename,
            staged_path=staged_figures_dir / filename,
        )
    return tuple(checks)


def run_single_fish_audit_inputs_stage(config: SingleFishPipelineConfig) -> StageManifest:
    paths = resolve_pipeline_paths(config)
    inputs = (
        describe_manifest_path(paths.fish_dir, label="fish root"),
        describe_manifest_path(paths.raw_2p_dir, label="raw 2p root"),
        describe_manifest_path(paths.raw_2p_metadata_dir, label="raw 2p metadata directory"),
        describe_glob(paths.raw_2p_metadata_dir, "*metadata*.csv", label="raw 2p metadata CSVs"),
        describe_glob(paths.raw_2p_metadata_dir, "*experiment*.csv", label="raw 2p experiment log CSVs"),
        describe_glob(paths.raw_2p_metadata_dir, "*trial_sequence*.csv", required=False, label="raw 2p trial sequence CSVs"),
        describe_glob(paths.raw_2p_metadata_dir, "*planned_schedule*.csv", required=False, label="raw 2p planned schedule CSVs"),
        describe_manifest_path(paths.raw_2p_functional_dir, label="raw 2p functional directory"),
        describe_glob(paths.raw_2p_functional_dir, "**/*", required=False, label="raw 2p functional files"),
        describe_manifest_path(paths.raw_2p_anatomy_dir, required=False, label="raw 2p anatomy directory"),
        describe_glob(paths.raw_2p_anatomy_dir, "**/*", required=False, label="raw 2p anatomy files"),
        describe_manifest_path(paths.preproc_dir, required=False, label="preprocessing root"),
        describe_manifest_path(paths.anatomy_preproc_dir, required=False, label="2p anatomy preprocessing directory"),
        describe_glob(
            paths.anatomy_preproc_dir,
            "*_anatomy_2P_GCaMP.nrrd",
            label="2p anatomy registration NRRD",
        ),
        describe_glob(paths.anatomy_preproc_dir, "*.json", required=False, label="2p anatomy metadata sidecars"),
        describe_glob(paths.anatomy_preproc_dir, "*_anatomy_2P_GCaMP_uint8.tif*", required=False, label="legacy anatomy uint8 TIFF cache"),
        describe_manifest_path(paths.functional_preproc_dir, required=False, label="2p functional preprocessing directory"),
        describe_glob(paths.functional_preproc_dir, "**/*", required=False, label="2p functional preprocessing files"),
        describe_manifest_path(paths.functional_suite2p_dir, required=False, label="Suite2p analysis directory"),
        describe_glob(paths.functional_suite2p_dir, "plane*/*ops.npy", label="Suite2p ops files"),
        describe_glob(paths.functional_suite2p_dir, "plane*/*F.npy", label="Suite2p fluorescence files"),
        describe_glob(paths.functional_suite2p_dir, "plane*/*Fneu.npy", label="Suite2p neuropil files"),
        describe_glob(paths.functional_suite2p_dir, "plane*/*iscell.npy", label="Suite2p iscell files"),
        describe_glob(paths.functional_suite2p_dir, "plane*/*stat.npy", label="Suite2p stat files"),
        describe_manifest_path(paths.functional_registration_dir, required=False, label="functional registration directory"),
        describe_manifest_path(
            paths.functional_registration_dir / "tforms_by_plane.csv",
            label="functional transform table",
        ),
        describe_manifest_path(paths.functional_registration_dir / "plane_links.csv", label="functional plane links"),
        describe_manifest_path(paths.functional_registration_dir / "functional_roi_activity_identity.csv", label="ROI identity master table"),
        describe_manifest_path(paths.functional_registration_dir / "functional_roi_activity_identity_summary.csv", label="ROI identity summary"),
        describe_manifest_path(paths.functional_registration_dir / "functional_roi_activity_identity_by_plane.csv", label="ROI identity by-plane summary"),
        describe_manifest_path(paths.functional_registration_dir / "functional_roi_activity_bpi_cells.csv", label="ROI activity/BPI cells table"),
        describe_manifest_path(paths.functional_registration_dir / "functional_roi_activity_bpi_summary.csv", label="ROI activity/BPI summary"),
        describe_manifest_path(paths.functional_registration_dir / "hcr_activity_status.csv", label="HCR activity status table"),
        describe_manifest_path(paths.functional_registration_dir / "hcr_activity_status_summary.csv", label="HCR activity status summary"),
        describe_manifest_path(paths.functional_registration_dir / "conf_to_func_pairs_raw.csv", label="raw HCR/function pairs"),
        describe_manifest_path(paths.functional_registration_dir / "conf_to_func_pairs.csv", label="responsive HCR/function pairs"),
        describe_manifest_path(paths.functional_registration_dir / "hcr_func_candidates.csv", label="HCR/function candidates"),
        describe_glob(paths.functional_registration_dir, "*.csv", required=False, label="functional registration CSVs"),
        describe_glob(
            paths.functional_registration_dir / "reference_planes",
            "**/*",
            required=False,
            label="functional reference plane files",
        ),
        describe_manifest_path(paths.functional_ncc_dir / "ncc_bestz_by_plane.json", label="NCC best-z by plane"),
        describe_manifest_path(paths.functional_ncc_dir / "ncc_scale_by_fish.json", required=False, label="NCC scale by fish"),
        describe_manifest_path(
            paths.functional_ncc_dir / "inplane_registration_comparison" / "inplane_registration_comparison.csv",
            label="in-plane registration comparison",
        ),
        describe_manifest_path(
            paths.functional_ncc_dir / "inplane_registration_comparison" / "inplane_registration_recommendation.csv",
            label="in-plane registration recommendation",
        ),
        describe_glob(
            paths.functional_ncc_dir / "inplane_registration_comparison" / "transforms",
            "**/*",
            required=False,
            label="in-plane registration transform files",
        ),
        describe_manifest_path(
            paths.functional_qa_dir / "suite2p_native_labels" / "suite2p_native_label_export_manifest.csv",
            required=False,
            label="Suite2p native label export manifest",
        ),
        describe_manifest_path(paths.confocal_dir, required=False, label="confocal analysis directory"),
        describe_glob(paths.confocal_raw_cp_masks_dir, "*_cp_masks.tif", label="HCR Cellpose masks"),
        describe_manifest_path(paths.confocal_aligned_dir, required=False, label="aligned confocal directory"),
        describe_glob(paths.confocal_aligned_dir, "**/*", label="aligned confocal inventory"),
        describe_glob(paths.confocal_aligned_dir, "*.csv", required=False, label="aligned confocal CSVs"),
        describe_glob(paths.preproc_dir / "rbest", "*_channel*.nrrd", label="rbest HCR/channel NRRDs"),
        describe_glob(paths.preproc_dir / "rn", "*_channel*.nrrd", required=False, label="rn HCR/channel NRRDs"),
        describe_manifest_path(paths.plots_dir, required=False, label="plots directory"),
        describe_manifest_path(paths.plots_dir / "compound_50j_56i_unified.png", label="compound 50j/56i plot PNG"),
        describe_manifest_path(paths.plots_dir / "bpi_all_pairs.png", label="BPI all-pairs plot PNG"),
        describe_manifest_path(paths.plots_dir / "per_gene_stimulus_trace_with_hcr_status_56h.png", label="per-gene 56h trace plot PNG"),
        describe_manifest_path(paths.plots_dir / "single_fish_50l_responsive_identity_donut.png", label="responsive identity donut PNG"),
        describe_manifest_path(paths.plots_dir / "single_fish_hcr_anatomy_coexpression_summary.png", label="HCR anatomy coexpression PNG"),
        describe_glob(paths.plots_dir, "*.png", required=False, label="plot PNGs"),
        describe_glob(_stage_root(paths, "assign-hcr-identity") / "registration", "*.csv", required=False, label="staged assign-hcr-identity CSVs"),
        describe_glob(_stage_root(paths, "score-activity-bpi") / "registration", "*.csv", required=False, label="staged score-activity-bpi CSVs"),
        describe_glob(_stage_root(paths, "export-canonical-tables") / "registration", "*.csv", required=False, label="staged canonical export CSVs"),
        describe_glob(_stage_root(paths, "make-figures") / "04_plots", "*", required=False, label="staged figure outputs"),
        describe_manifest_path(paths.matching_metadata_csv, required=False, label="matching metadata CSV"),
    )
    checks = _build_audit_checks(paths)
    missing_required = tuple(record.path for record in inputs if record.required and not record.exists)
    failed_checks = tuple(f"{check.label}: {check.observed}" for check in checks if check.status == "fail")
    warnings = ()
    errors = missing_required + failed_checks if config.strict else ()
    if (missing_required or failed_checks) and not config.strict:
        warnings = tuple(f"missing required input: {path}" for path in missing_required) + tuple(
            f"failed check: {check}" for check in failed_checks
        )
    status = "fail" if errors else ("warn" if warnings else "pass")
    return StageManifest(
        manifest_version=PIPELINE_MANIFEST_VERSION,
        stage_name="audit-inputs",
        fish_id=config.fish_id,
        status=status,
        dry_run=config.dry_run,
        generated_at=datetime.now(timezone.utc).isoformat(),
        inputs=inputs,
        checks=checks,
        parameters={
            "local_root": str(config.local_root),
            "owner": config.owner,
            "data_mode": config.data_mode,
            "strict": config.strict,
            "write_manifest": config.write_manifest,
            "pipeline_root": str(paths.pipeline_root),
        },
        warnings=warnings,
        errors=errors,
    )


def discover_ex_vivo_anatomy_stack(paths: PipelinePaths) -> Path:
    patterns = (
        "*ex*vivo*.tif",
        "*ex*vivo*.tiff",
        "*exvivo*.tif",
        "*exvivo*.tiff",
        "*ex*vivo*.nrrd",
        "*exvivo*.nrrd",
    )
    matches: list[Path] = []
    for pattern in patterns:
        matches.extend(path for path in sorted(paths.raw_2p_anatomy_dir.glob(pattern)) if _is_real_match(path))
    unique = tuple(dict.fromkeys(matches))
    if not unique:
        raise FileNotFoundError(f"No ex vivo anatomy stack found under {paths.raw_2p_anatomy_dir}")
    if len(unique) > 1:
        names = ", ".join(str(path) for path in unique)
        raise RuntimeError(f"Multiple ex vivo anatomy stacks found; pass --ex-vivo-stack-path explicitly: {names}")
    return unique[0]


def prepared_in_vivo_anatomy_path(paths: PipelinePaths) -> Path:
    return paths.preproc_dir / "2p_anatomy" / f"{paths.fish_dir.name}_anatomy_2P_GCaMP.nrrd"


def functional_reference_output_dir(paths: PipelinePaths) -> Path:
    return _stage_root(paths, "prepare-functional-reference-stacks") / "functional" / "raw"


def functional_to_anatomy_registration_root(paths: PipelinePaths) -> Path:
    return _stage_root(paths, "register-functional-to-anatomy")


def hcr_to_anatomy_registration_root(paths: PipelinePaths) -> Path:
    return _stage_root(paths, "register-hcr-to-anatomy")


def roi_to_anatomy_match_root(paths: PipelinePaths) -> Path:
    return _stage_root(paths, "match-roi-to-anatomy")


def discover_functional_motion_corrected_stacks(paths: PipelinePaths) -> tuple[Path, ...]:
    motion_dir = paths.functional_preproc_dir / "02_motionCorrected"
    matches = tuple(path for path in sorted(motion_dir.glob("*mcorrected*.tif")) if _is_real_match(path))
    if not matches:
        raise FileNotFoundError(f"No motion-corrected functional stacks found under {motion_dir}")
    return matches


def _functional_reference_plane_label(raw_path: Path) -> str | None:
    stem = raw_path.stem
    for suffix in ("_ref_raw", "_raw"):
        if stem.endswith(suffix):
            return stem[: -len(suffix)]
    return None


def _functional_reference_norm_path(raw_path: Path, label: str) -> Path:
    if raw_path.stem.endswith("_ref_raw"):
        return raw_path.with_name(f"{label}_ref_norm.tif")
    return raw_path.with_name(f"{label}_norm.tif")


def discover_functional_reference_pairs(reference_dir: Path | str) -> tuple[tuple[str, Path, Path], ...]:
    ref_dir = Path(reference_dir)
    raw_paths = tuple(
        path
        for pattern in ("*_ref_raw.tif", "*_raw.tif")
        for path in sorted(ref_dir.glob(pattern))
        if _is_real_match(path)
    )
    pairs: list[tuple[str, Path, Path]] = []
    seen: set[Path] = set()
    for raw_path in raw_paths:
        if raw_path in seen:
            continue
        seen.add(raw_path)
        label = _functional_reference_plane_label(raw_path)
        if not label:
            continue
        norm_path = _functional_reference_norm_path(raw_path, label)
        if not norm_path.exists():
            continue
        pairs.append((label, raw_path, norm_path))
    if not pairs:
        raise FileNotFoundError(f"No functional reference raw/norm TIFF pairs found under {ref_dir}")
    return tuple(pairs)


def _parse_plane_index(label: str, default: int) -> int:
    match = re.search(r"plane(\d+)", str(label))
    if not match:
        return int(default)
    return int(match.group(1))


def _plane_refs_summary(plane_refs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for default_idx, plane_ref in enumerate(plane_refs):
        ncc_xy_record = plane_ref.get("ncc_xy", {}) if isinstance(plane_ref.get("ncc_xy"), dict) else {}
        scores = plane_ref.get("ncc_scores")
        scores_count = len(scores) if hasattr(scores, "__len__") else 0
        row = {
            "label": str(plane_ref.get("label", f"plane{default_idx}")),
            "index": int(plane_ref.get("index", default_idx)),
            "reference_raw_path": plane_ref.get("reference_raw_path"),
            "reference_norm_path": plane_ref.get("reference_norm_path"),
            "ref_shape": list(plane_ref.get("ref_shape", ())),
            "ref_scaled_shape": list(plane_ref.get("ref_scaled_shape", ())),
            "scale": plane_ref.get("scale"),
            "best_z": plane_ref.get("best_z"),
            "ncc_scores_count": int(scores_count),
            "tform_src": plane_ref.get("tform_src"),
            "ncc_xy": ncc_xy_record,
            "inplane_requested_active_method": plane_ref.get("inplane_requested_active_method"),
            "inplane_active_method": plane_ref.get("inplane_active_method"),
            "inplane_fallback_reason": plane_ref.get("inplane_fallback_reason"),
            "anat_label_z_mode": plane_ref.get("anat_label_z_mode", plane_ref.get("anat_labels_z_mode", "direct")),
        }
        if plane_ref.get("ants_transformlist"):
            row["ants_transformlist"] = list(plane_ref.get("ants_transformlist", ()))
        rows.append(row)
    return rows


def load_plane_refs_summary(path: str | Path) -> list[dict[str, Any]]:
    summary_path = Path(path)
    if not summary_path.exists():
        raise FileNotFoundError(f"Plane refs summary not found: {summary_path}")
    data = json.loads(summary_path.read_text())
    if not isinstance(data, list):
        raise RuntimeError(f"Plane refs summary should be a list: {summary_path}")
    plane_refs: list[dict[str, Any]] = []
    for idx, row in enumerate(data):
        if not isinstance(row, dict):
            continue
        plane_ref = {
            "label": str(row.get("label", f"plane{idx}")),
            "index": int(row.get("index", idx)),
            "best_z": int(row.get("best_z", 0)),
            "scale": row.get("scale"),
            "tform_src": row.get("tform_src"),
            "ncc_xy": row.get("ncc_xy") if isinstance(row.get("ncc_xy"), dict) else None,
            "ref_shape": tuple(row.get("ref_shape", ())),
            "ref_scaled_shape": tuple(row.get("ref_scaled_shape", ())),
            "reference_raw_path": row.get("reference_raw_path"),
            "reference_norm_path": row.get("reference_norm_path"),
            "anat_label_z_mode": row.get("anat_label_z_mode", row.get("anat_labels_z_mode", "direct")),
        }
        if row.get("ants_transformlist"):
            plane_ref["ants_transformlist"] = list(row.get("ants_transformlist", ()))
            plane_ref["tform_src"] = "ants_rigid_affine"
        plane_refs.append(plane_ref)
    if not plane_refs:
        raise RuntimeError(f"Plane refs summary has no plane records: {summary_path}")
    return plane_refs


def _parse_tuple_cell(value: Any) -> tuple[int, ...]:
    if value in (None, "", False):
        return ()
    vals = re.findall(r"-?\d+", str(value))
    return tuple(int(v) for v in vals)


def _split_transformlist_cell(value: Any) -> list[str]:
    if value in (None, "", False):
        return []
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return []
    if text.startswith("[") and text.endswith("]"):
        return [item.strip().strip("'\"") for item in text.strip("[]").split(",") if item.strip()]
    return [text]


def _selected_ants_inplane_comparison_path(paths: PipelinePaths) -> Path:
    return paths.functional_ncc_dir / "inplane_registration_comparison" / "inplane_registration_comparison.csv"


def _anatomy_xy_spacing_from_voxel_cache(paths: PipelinePaths) -> tuple[float, float]:
    cache_path = paths.analysis_dir / "voxel_sizes.json"
    if not cache_path.exists():
        return (1.0, 1.0)
    try:
        data = json.loads(cache_path.read_text())
    except Exception:
        return (1.0, 1.0)
    records = tuple((str(path), values) for path, values in (data.get("by_path") or {}).items())
    preferred_records = tuple(
        item
        for item in records
        if "anatomy" in item[0].lower() or "2p_anatomy" in item[0].lower()
    )
    for _path, values in preferred_records + records:
        if not isinstance(values, dict):
            continue
        if "X" in values and "Y" in values and "Z" in values and values.get("Z") not in (None, "", False):
            try:
                return (float(values["X"]), float(values["Y"]))
            except Exception:
                continue
    return (1.0, 1.0)


def _overlay_selected_ants_transformlists(
    plane_refs: list[dict[str, Any]],
    comparison_path: Path,
    *,
    xy_spacing: tuple[float, float] = (1.0, 1.0),
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    selected_by_plane: dict[int, dict[str, Any]] = {}
    if comparison_path.exists():
        with comparison_path.open(newline="") as handle:
            for row in csv.DictReader(handle):
                if str(row.get("method", "")) != "ants_rigid_affine":
                    continue
                if str(row.get("selected", "")).strip().lower() not in {"true", "1", "yes"}:
                    continue
                transformlist = _split_transformlist_cell(row.get("transformlist"))
                if not transformlist:
                    continue
                plane_idx = int(float(row.get("plane_idx", 0)))
                selected_by_plane[plane_idx] = {
                    "label": row.get("plane"),
                    "best_z": int(float(row.get("best_z", 0))),
                    "scale": float(row.get("scale", 1.0)),
                    "ref_shape": _parse_tuple_cell(row.get("ref_shape")),
                    "ref_scaled_shape": _parse_tuple_cell(row.get("ref_scaled_shape")),
                    "transformlist": transformlist,
                }

    out: list[dict[str, Any]] = []
    overlay_count = 0
    missing_transform_files = 0
    for default_idx, plane_ref in enumerate(plane_refs):
        ref = dict(plane_ref)
        plane_idx = int(ref.get("index", default_idx))
        selected = selected_by_plane.get(plane_idx)
        if selected is not None:
            spacing = (float(xy_spacing[0]), float(xy_spacing[1]))
            ref["tform_src"] = "ants_rigid_affine"
            ref["ants_transformlist"] = list(selected["transformlist"])
            ref["ants_transform"] = {
                "type": "ants_transformlist",
                "method": "ants_rigid_affine",
                "transformlist": list(selected["transformlist"]),
                "fixed_spacing": spacing,
                "moving_spacing": spacing,
                "fixed_origin": (0.0, 0.0),
                "moving_origin": (0.0, 0.0),
                "fixed_direction": [[1.0, 0.0], [0.0, 1.0]],
                "moving_direction": [[1.0, 0.0], [0.0, 1.0]],
                "moving_shape": tuple(selected.get("ref_scaled_shape") or ref.get("ref_scaled_shape", ())),
                "fixed_shape": tuple(selected.get("ref_scaled_shape") or ref.get("ref_scaled_shape", ())),
            }
            if selected.get("best_z") is not None:
                ref["best_z"] = selected["best_z"]
            if selected.get("scale") is not None:
                ref["scale"] = selected["scale"]
            if selected.get("ref_shape"):
                ref["ref_shape"] = tuple(selected["ref_shape"])
            if selected.get("ref_scaled_shape"):
                ref["ref_scaled_shape"] = tuple(selected["ref_scaled_shape"])
            overlay_count += 1
            missing_transform_files += sum(1 for path in selected["transformlist"] if not Path(path).exists())
        out.append(ref)

    backend_counts: dict[str, int] = {}
    for ref in out:
        key = str(ref.get("tform_src") or "unknown")
        backend_counts[key] = int(backend_counts.get(key, 0)) + 1
    return out, {
        "comparison_path": str(comparison_path),
        "comparison_exists": comparison_path.exists(),
        "selected_ants_rows": int(len(selected_by_plane)),
        "overlay_applied_planes": int(overlay_count),
        "plane_count": int(len(out)),
        "backend_counts": backend_counts,
        "missing_transform_files": int(missing_transform_files),
        "ants_xy_spacing": [float(xy_spacing[0]), float(xy_spacing[1])],
    }


def _selected_inplane_best_z_by_plane(comparison_path: Path) -> dict[int, int]:
    out: dict[int, int] = {}
    for plane_idx, row in _selected_inplane_metadata_by_plane(comparison_path).items():
        if row.get("best_z") is not None:
            out[int(plane_idx)] = int(row["best_z"])
    return out


def _selected_inplane_metadata_by_plane(comparison_path: Path) -> dict[int, dict[str, Any]]:
    out: dict[int, dict[str, Any]] = {}
    if not comparison_path.exists():
        return out
    with comparison_path.open(newline="") as handle:
        for row in csv.DictReader(handle):
            if str(row.get("selected", "")).strip().lower() not in {"true", "1", "yes"}:
                continue
            try:
                plane_idx = int(float(row.get("plane_idx", 0)))
                out[plane_idx] = {
                    "best_z": int(float(row.get("best_z", 0))),
                    "scale": float(row.get("scale", 1.0)),
                    "ref_shape": _parse_tuple_cell(row.get("ref_shape")),
                    "ref_scaled_shape": _parse_tuple_cell(row.get("ref_scaled_shape")),
                    "method": str(row.get("method", "")),
                }
            except Exception:
                continue
    return out


def _plane_refs_from_tforms_csv(
    tforms_path: Path,
    base_plane_refs: list[dict[str, Any]],
    *,
    best_z_by_plane: dict[int, int] | None = None,
    selected_metadata_by_plane: dict[int, dict[str, Any]] | None = None,
    offsets_by_plane: dict[int, tuple[float, float]] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    best_z_lookup = best_z_by_plane or {}
    metadata_lookup = selected_metadata_by_plane or {}
    offsets = offsets_by_plane or {}
    refs_by_index = {int(ref.get("index", idx)): dict(ref) for idx, ref in enumerate(base_plane_refs)}
    rows_loaded = 0
    missing_matrix_rows = 0
    out_by_index: dict[int, dict[str, Any]] = {}
    if tforms_path.exists():
        with tforms_path.open(newline="") as handle:
            for row in csv.DictReader(handle):
                try:
                    plane_idx = int(float(row.get("plane_index", row.get("plane_idx", rows_loaded))))
                    matrix = [
                        [float(row["m00"]), float(row["m01"]), float(row["m02"])],
                        [float(row["m10"]), float(row["m11"]), float(row["m12"])],
                        [0.0, 0.0, 1.0],
                    ]
                except Exception:
                    missing_matrix_rows += 1
                    continue
                dx, dy = offsets.get(plane_idx, (0.0, 0.0))
                matrix[0][2] += float(dx)
                matrix[1][2] += float(dy)
                selected_meta = metadata_lookup.get(plane_idx, {})
                ref = dict(refs_by_index.get(plane_idx, {}))
                ref["index"] = plane_idx
                ref["label"] = str(row.get("label") or ref.get("label", f"plane{plane_idx}"))
                ref["best_z"] = int(
                    best_z_lookup.get(
                        plane_idx,
                        selected_meta.get("best_z", int(float(row.get("best_z", ref.get("best_z", 0))))),
                    )
                )
                if selected_meta.get("scale") is not None:
                    ref["scale"] = float(selected_meta["scale"])
                if selected_meta.get("ref_shape"):
                    ref["ref_shape"] = tuple(selected_meta["ref_shape"])
                if selected_meta.get("ref_scaled_shape"):
                    ref["ref_scaled_shape"] = tuple(selected_meta["ref_scaled_shape"])
                ref["tform_src"] = "affine_tform"
                ref["affine_transform"] = {
                    "type": "skimage_affine",
                    "matrix": matrix,
                    "moving_shape": tuple(ref.get("ref_scaled_shape", ())),
                }
                ref["tform"] = ref["affine_transform"]
                ref["affine_matrix"] = matrix
                ref["affine_tforms_path"] = str(tforms_path)
                ref.pop("ants_transform", None)
                ref.pop("ants_transformlist", None)
                out_by_index[plane_idx] = ref
                rows_loaded += 1

    out: list[dict[str, Any]] = []
    for idx, base in enumerate(base_plane_refs):
        plane_idx = int(base.get("index", idx))
        out.append(out_by_index.get(plane_idx, dict(base)))
    backend_counts: dict[str, int] = {}
    for ref in out:
        key = str(ref.get("tform_src") or "unknown")
        backend_counts[key] = int(backend_counts.get(key, 0)) + 1
    return out, {
        "tforms_path": str(tforms_path),
        "tforms_exists": tforms_path.exists(),
        "rows_loaded": int(rows_loaded),
        "missing_matrix_rows": int(missing_matrix_rows),
        "best_z_overrides": int(len(best_z_lookup)),
        "selected_metadata_planes": int(len(metadata_lookup)),
        "offset_planes": {str(k): [float(v[0]), float(v[1])] for k, v in offsets.items()},
        "backend_counts": backend_counts,
    }


def _hcr_replay_plane_ref_variants(
    paths: PipelinePaths,
    base_plane_refs: list[dict[str, Any]],
    selected_ants_plane_refs: list[dict[str, Any]],
    selected_ants_report: dict[str, Any],
) -> list[tuple[str, list[dict[str, Any]], dict[str, Any]]]:
    variants: list[tuple[str, list[dict[str, Any]], dict[str, Any]]] = [
        ("selected_inplane_registration_ants_transformlist", selected_ants_plane_refs, dict(selected_ants_report))
    ]
    comparison_path = _selected_ants_inplane_comparison_path(paths)
    selected_best_z = _selected_inplane_best_z_by_plane(comparison_path)
    selected_metadata = _selected_inplane_metadata_by_plane(comparison_path)
    for label, tforms_path in (
        ("accepted_registration_tforms_by_plane_csv", paths.functional_registration_dir / "tforms_by_plane.csv"),
        ("accepted_ncc_tforms_by_plane_csv", paths.functional_ncc_dir / "tforms_by_plane.csv"),
    ):
        refs, report = _plane_refs_from_tforms_csv(tforms_path, base_plane_refs)
        if report["tforms_exists"] and report["rows_loaded"]:
            variants.append((label, refs, report))
        if selected_best_z:
            refs_best_z, report_best_z = _plane_refs_from_tforms_csv(
                tforms_path,
                base_plane_refs,
                best_z_by_plane=selected_best_z,
                selected_metadata_by_plane=selected_metadata,
            )
            if report_best_z["tforms_exists"] and report_best_z["rows_loaded"]:
                variants.append((f"{label}_selected_inplane_best_z", refs_best_z, report_best_z))
            refs_offset, report_offset = _plane_refs_from_tforms_csv(
                tforms_path,
                base_plane_refs,
                best_z_by_plane=selected_best_z,
                selected_metadata_by_plane=selected_metadata,
                offsets_by_plane={4: (-1.0, 1.0)},
            )
            if report_offset["tforms_exists"] and report_offset["rows_loaded"]:
                variants.append((f"{label}_selected_inplane_best_z_single_plane_dx-1_dy+1_p4", refs_offset, report_offset))
    seen: set[str] = set()
    deduped: list[tuple[str, list[dict[str, Any]], dict[str, Any]]] = []
    for name, refs, report in variants:
        if name in seen:
            continue
        seen.add(name)
        deduped.append((name, refs, report))
    return deduped


def ex_vivo_structural_root(paths: PipelinePaths) -> Path:
    return paths.analysis_dir / "structural" / "ex_vivo"


def prepared_ex_vivo_anatomy_path(paths: PipelinePaths) -> Path:
    return ex_vivo_structural_root(paths) / "prepared" / f"{paths.fish_dir.name}_exvivo_anatomy_2P_GCaMP_uint8.nrrd"


def _tiff_label_count(path: Path) -> int | None:
    if not path.exists() or path.suffix.lower() not in {".tif", ".tiff"}:
        return None
    try:
        import numpy as np
        import tifffile

        arr = np.asarray(tifffile.imread(path))
        values = np.unique(arr)
        return int(len(values) - (1 if np.any(values == 0) else 0))
    except Exception:
        return None


def _optional_manifest_path(path: Path | None, *, label: str, required: bool = True) -> ManifestPathRecord:
    if path is None:
        return ManifestPathRecord(
            path="",
            exists=False,
            kind="missing",
            required=required,
            label=label,
        )
    return describe_manifest_path(path, required=required, label=label)


def run_prepare_in_vivo_anatomy_stack_stage(
    config: SingleFishPipelineConfig,
    *,
    anatomy_stack_path: str | Path | None = None,
    output_path: str | Path | None = None,
    force_recompute: bool = False,
) -> StageManifest:
    from .context import (
        AnatomyUint8PreprocessingConfig,
        infer_anatomy_stack_path,
        preprocess_anatomy_uint8_stage,
        resolve_func_polarity,
    )

    paths = resolve_pipeline_paths(config)
    out_path = Path(output_path) if output_path not in (None, "", False) else prepared_in_vivo_anatomy_path(paths)
    try:
        source_path = (
            Path(anatomy_stack_path)
            if anatomy_stack_path not in (None, "", False)
            else infer_anatomy_stack_path(paths.fish_dir, paths.fish_dir.name)
        )
        if source_path is None:
            raise FileNotFoundError(f"No in vivo anatomy stack found under {paths.fish_dir}")
        polarity, polarity_source = resolve_func_polarity(
            config.fish_id,
            paths.matching_metadata_csv,
            fish_dir=paths.fish_dir,
        )
        result = preprocess_anatomy_uint8_stage(
            anat_stack_path=source_path,
            anat_stack_path_orig=source_path,
            preproc_dir=paths.preproc_dir,
            output_path=out_path,
            polarity=polarity,
            polarity_source=polarity_source,
            config=AnatomyUint8PreprocessingConfig(force_recompute_anat_uint8=force_recompute),
        )
        artifacts = result.get("artifacts", {})
        output_shape = artifacts.get("output_shape")
        output_yx = tuple(output_shape[-2:]) if output_shape else None
        status = "pass"
        errors: tuple[str, ...] = ()
        warnings: tuple[str, ...] = ()
        checks = (
            StageCheckRecord(
                label="prepared in vivo anatomy stack",
                status="pass" if out_path.exists() else "fail",
                detail="registration-ready in vivo anatomy NRRD exists",
                observed=str(out_path),
            ),
            StageCheckRecord(
                label="in vivo anatomy output dtype",
                status="pass" if str(artifacts.get("output_dtype", "")) == "uint8" else "fail",
                detail="prepared anatomy stack is uint8",
                observed=str(artifacts.get("output_dtype", "")),
                expected="uint8",
            ),
            StageCheckRecord(
                label="in vivo anatomy output Y/X",
                status="pass" if output_yx == (750, 750) else "fail",
                detail="prepared anatomy stack uses the standard 750x750 Y/X grid",
                observed=str(output_yx),
                expected="(750, 750)",
            ),
        )
        if any(check.status == "fail" for check in checks):
            status = "fail"
            errors = tuple(f"{check.label}: {check.observed}" for check in checks if check.status == "fail")
    except Exception as exc:
        source_path = Path(anatomy_stack_path) if anatomy_stack_path not in (None, "", False) else paths.raw_2p_anatomy_dir
        result = {"artifacts": {}, "log_lines": []}
        status = "fail"
        errors = (str(exc),)
        warnings = ()
        checks = ()
    return StageManifest(
        manifest_version=PIPELINE_MANIFEST_VERSION,
        stage_name="prepare-in-vivo-anatomy-stack",
        fish_id=config.fish_id,
        status=status,
        dry_run=False,
        generated_at=datetime.now(timezone.utc).isoformat(),
        inputs=(describe_manifest_path(source_path, label="raw in vivo anatomy stack"),),
        outputs=(
            describe_manifest_path(out_path, label="prepared in vivo anatomy NRRD"),
            describe_manifest_path(Path(str(out_path) + ".json"), label="prepared in vivo anatomy metadata"),
        ),
        checks=checks,
        parameters={
            "local_root": str(config.local_root),
            "force_recompute": bool(force_recompute),
            "output_root": str(out_path.parent),
            "log_lines": tuple(result.get("log_lines", ())),
        },
        warnings=warnings,
        errors=errors,
    )


def run_prepare_functional_reference_stacks_stage(
    config: SingleFishPipelineConfig,
    *,
    functional_stack_paths: tuple[str | Path, ...] | list[str | Path] | None = None,
    output_dir: str | Path | None = None,
    force_recompute: bool = False,
) -> StageManifest:
    from .context import resolve_func_polarity
    from .spatial import FunctionalReferenceConfig, build_functional_references_stage

    paths = resolve_pipeline_paths(config)
    out_dir = Path(output_dir) if output_dir not in (None, "", False) else functional_reference_output_dir(paths)
    try:
        if functional_stack_paths:
            source_paths = tuple(Path(path) for path in functional_stack_paths)
        else:
            source_paths = discover_functional_motion_corrected_stacks(paths)
        flipped_paths = tuple(out_dir / f"{path.stem}_flipX.tif" for path in source_paths)
        polarity, polarity_source = resolve_func_polarity(
            config.fish_id,
            paths.matching_metadata_csv,
            fish_dir=paths.fish_dir,
        )
        result = build_functional_references_stage(
            flipped_list=list(flipped_paths),
            func_nonflipped_list=list(source_paths),
            out_raw=out_dir,
            polarity=polarity,
            polarity_source=polarity_source,
            config=FunctionalReferenceConfig(force_recompute_refs=force_recompute),
        )
        plane_refs = tuple(result.get("plane_refs", ()))
        raw_outputs: list[Path] = []
        norm_outputs: list[Path] = []
        for plane_ref in plane_refs:
            label = str(plane_ref.get("label", ""))
            if not label:
                continue
            raw_candidates = (out_dir / f"{label}_raw.tif", out_dir / f"{label}_ref_raw.tif")
            norm_candidates = (out_dir / f"{label}_norm.tif", out_dir / f"{label}_ref_norm.tif")
            raw_outputs.append(next((path for path in raw_candidates if path.exists()), raw_candidates[0]))
            norm_outputs.append(next((path for path in norm_candidates if path.exists()), norm_candidates[0]))
        checks = (
            StageCheckRecord(
                label="functional reference planes",
                status="pass" if plane_refs else "fail",
                detail="at least one functional reference plane was built or reused",
                observed=str(len(plane_refs)),
                expected=">=1",
            ),
            StageCheckRecord(
                label="functional reference raw TIFFs",
                status="pass" if raw_outputs and all(path.exists() for path in raw_outputs) else "fail",
                detail="raw functional reference TIFFs exist",
                observed=str(sum(1 for path in raw_outputs if path.exists())),
                expected=str(len(raw_outputs)),
            ),
            StageCheckRecord(
                label="functional reference normalized TIFFs",
                status="pass" if norm_outputs and all(path.exists() for path in norm_outputs) else "fail",
                detail="normalized functional reference TIFFs exist",
                observed=str(sum(1 for path in norm_outputs if path.exists())),
                expected=str(len(norm_outputs)),
            ),
        )
        status = "pass"
        errors: tuple[str, ...] = ()
        warnings: tuple[str, ...] = ()
        if any(check.status == "fail" for check in checks):
            status = "fail"
            errors = tuple(f"{check.label}: {check.observed}" for check in checks if check.status == "fail")
    except Exception as exc:
        source_paths = tuple(Path(path) for path in (functional_stack_paths or ())) or (paths.functional_preproc_dir / "02_motionCorrected",)
        raw_outputs = []
        norm_outputs = []
        result = {"log_lines": []}
        polarity = None
        polarity_source = None
        status = "fail"
        errors = (str(exc),)
        warnings = ()
        checks = ()
    outputs = tuple(
        describe_manifest_path(path, label="functional reference raw TIFF")
        for path in raw_outputs
    ) + tuple(
        describe_manifest_path(path, label="functional reference normalized TIFF")
        for path in norm_outputs
    )
    return StageManifest(
        manifest_version=PIPELINE_MANIFEST_VERSION,
        stage_name="prepare-functional-reference-stacks",
        fish_id=config.fish_id,
        status=status,
        dry_run=False,
        generated_at=datetime.now(timezone.utc).isoformat(),
        inputs=tuple(describe_manifest_path(path, label="motion-corrected functional stack") for path in source_paths),
        outputs=outputs,
        checks=checks,
        parameters={
            "local_root": str(config.local_root),
            "force_recompute": bool(force_recompute),
            "output_dir": str(out_dir),
            "polarity": polarity,
            "polarity_source": polarity_source,
            "log_lines": tuple(result.get("log_lines", ())),
        },
        warnings=warnings,
        errors=errors,
    )


def _write_tforms_by_plane_csv(path: Path, plane_refs: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = (
        "plane_index",
        "label",
        "best_z",
        "scale",
        "tform_src",
        "ncc_xy_x0",
        "ncc_xy_y0",
        "ncc_xy_score",
    )
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for plane_idx, plane_ref in enumerate(plane_refs):
            ncc_xy_record = plane_ref.get("ncc_xy", {}) if isinstance(plane_ref.get("ncc_xy"), dict) else {}
            writer.writerow(
                {
                    "plane_index": int(plane_idx),
                    "label": str(plane_ref.get("label", f"plane{plane_idx}")),
                    "best_z": int(plane_ref.get("best_z", 0)),
                    "scale": plane_ref.get("scale"),
                    "tform_src": plane_ref.get("tform_src", "ncc_xy"),
                    "ncc_xy_x0": ncc_xy_record.get("x0"),
                    "ncc_xy_y0": ncc_xy_record.get("y0"),
                    "ncc_xy_score": ncc_xy_record.get("score"),
                }
            )


def run_register_functional_to_anatomy_stage(
    config: SingleFishPipelineConfig,
    *,
    reference_dir: str | Path | None = None,
    anatomy_stack_path: str | Path | None = None,
    anatomy_labels_path: str | Path | None = None,
    functional_labels_anatomy_dir: str | Path | None = None,
    output_root: str | Path | None = None,
    force_recompute: bool = False,
    run_inplane_comparison: bool = True,
    inplane_methods: tuple[str, ...] = ("ncc_xy",),
    active_inplane_method: str = "ncc_xy",
    use_cv2: bool = False,
    emit_visual_qa: bool = True,
    visual_qa_crop_size_px: int = 200,
) -> StageManifest:
    import numpy as np

    from .context import infer_anat_labels_path
    from .plots.qa import render_functional_anatomy_center_overlay_qc_png
    from .spatial import (
        InPlaneRegistrationComparisonConfig,
        RegistrationSearchConfig,
        imread_any,
        norm01,
        run_in_plane_registration_comparison_stage,
        run_registration_search_stage,
    )

    paths = resolve_pipeline_paths(config)
    stage_root = Path(output_root) if output_root not in (None, "", False) else functional_to_anatomy_registration_root(paths)
    ref_dir = Path(reference_dir) if reference_dir not in (None, "", False) else functional_reference_output_dir(paths)
    anat_path = Path(anatomy_stack_path) if anatomy_stack_path not in (None, "", False) else prepared_in_vivo_anatomy_path(paths)
    out_ncc = stage_root / "ncc"
    registration_dir = stage_root / "registration"
    qa_dir = stage_root / "qa"
    tforms_path = registration_dir / "tforms_by_plane.csv"
    summary_path = stage_root / "plane_refs_summary.json"
    qa_overlay_path = qa_dir / f"functional_anatomy_center_overlay_{int(visual_qa_crop_size_px)}px.png"
    qa_overlay_csv_path = qa_overlay_path.with_suffix(".csv")
    comparison_path = out_ncc / "inplane_registration_comparison" / "inplane_registration_comparison.csv"
    recommendation_path = out_ncc / "inplane_registration_comparison" / "inplane_registration_recommendation.csv"
    required_output_paths = (
        out_ncc / "ncc_scale_by_fish.json",
        out_ncc / "ncc_bestz_by_plane.json",
        summary_path,
        tforms_path,
    )
    if run_inplane_comparison:
        required_output_paths = required_output_paths + (comparison_path, recommendation_path)
    try:
        existing_outputs = tuple(path for path in required_output_paths if path.exists())
        if existing_outputs and not force_recompute:
            raise FileExistsError(
                "register-functional-to-anatomy outputs already exist; pass --force-recompute to overwrite: "
                + ", ".join(str(path) for path in existing_outputs)
            )
        reference_pairs = discover_functional_reference_pairs(ref_dir)
        plane_refs = [
            {
                "label": label,
                "index": _parse_plane_index(label, idx),
                "ref2d_raw": np.asarray(imread_any(raw_path), dtype=np.float32),
                "ref2d": norm01(imread_any(norm_path)),
                "reference_raw_path": str(raw_path),
                "reference_norm_path": str(norm_path),
            }
            for idx, (label, raw_path, norm_path) in enumerate(reference_pairs)
        ]
        search_result = run_registration_search_stage(
            anat_stack_path=anat_path,
            plane_refs=plane_refs,
            fish_id=config.fish_id,
            out_ncc=out_ncc,
            config=RegistrationSearchConfig(
                force_recompute=force_recompute,
                scale_coarse=(0.9, 1.0, 0.1),
                scale_fine=(0.0, 1.0),
                scale_xfine=(0.0, 1.0),
                scale_ufine=(0.0, 1.0),
                scale_workers=1,
                use_cv2=use_cv2,
            ),
        )
        if run_inplane_comparison:
            comparison_result = run_in_plane_registration_comparison_stage(
                plane_refs=search_result["plane_refs"],
                anat_f=search_result["anat_f"],
                fish_id=config.fish_id,
                out_ncc=out_ncc,
                best_z=int(search_result.get("best_z", 0)),
                config=InPlaneRegistrationComparisonConfig(
                    methods=tuple(inplane_methods),
                    active_method=str(active_inplane_method),
                    fallback_method=None,
                    fail_on_active_method_error=True,
                    use_cv2=use_cv2,
                ),
            )
            final_plane_refs = comparison_result["plane_refs"]
        else:
            comparison_result = {"log_lines": (), "comparison_path": comparison_path, "recommendation_path": recommendation_path}
            final_plane_refs = search_result["plane_refs"]
        _write_tforms_by_plane_csv(tforms_path, final_plane_refs)
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text(json.dumps(_plane_refs_summary(final_plane_refs), indent=2, sort_keys=True))
        qa_checks: tuple[StageCheckRecord, ...] = ()
        warnings_list: list[str] = []
        if emit_visual_qa:
            anat_labels_path = (
                Path(anatomy_labels_path)
                if anatomy_labels_path not in (None, "", False)
                else infer_anat_labels_path(paths.fish_dir, config.fish_id)
            )
            func_labels_dir = (
                Path(functional_labels_anatomy_dir)
                if functional_labels_anatomy_dir not in (None, "", False)
                else paths.functional_dir / "derived"
            )
            if anat_labels_path is None:
                warnings_list.append("functional/anatomy visual QA skipped: no anatomy label stack found")
                qa_checks = (
                    StageCheckRecord(
                        label="functional/anatomy center overlay QA",
                        status="warn",
                        detail="visual QA skipped because no anatomy label stack was found",
                        expected="anatomy labels",
                        observed="missing",
                    ),
                )
            else:
                try:
                    render_functional_anatomy_center_overlay_qc_png(
                        fish_id=config.fish_id,
                        plane_refs_summary_path=summary_path,
                        anatomy_stack_path=anat_path,
                        anatomy_labels_path=anat_labels_path,
                        functional_labels_anatomy_dir=func_labels_dir,
                        out_path=qa_overlay_path,
                        crop_size_px=int(visual_qa_crop_size_px),
                    )
                    qa_checks = (
                        StageCheckRecord(
                            label="functional/anatomy center overlay QA",
                            status="pass" if qa_overlay_path.exists() and qa_overlay_csv_path.exists() else "warn",
                            detail="center-crop anatomy-space functional/anatomy overlay was rendered",
                            observed=str(qa_overlay_path),
                        ),
                    )
                except Exception as qa_exc:
                    warnings_list.append(f"functional/anatomy visual QA skipped: {qa_exc}")
                    qa_checks = (
                        StageCheckRecord(
                            label="functional/anatomy center overlay QA",
                            status="warn",
                            detail="visual QA render did not complete",
                            observed=str(qa_exc),
                        ),
                    )

        checks = (
            StageCheckRecord(
                label="functional reference inputs",
                status="pass" if reference_pairs else "fail",
                detail="functional reference raw/norm pairs were loaded",
                observed=str(len(reference_pairs)),
                expected=">=1",
            ),
            StageCheckRecord(
                label="NCC best-z cache",
                status="pass" if Path(search_result["bestz_cache_path"]).exists() else "fail",
                detail="NCC best-z cache exists",
                observed=str(search_result["bestz_cache_path"]),
            ),
            StageCheckRecord(
                label="in-plane comparison CSV",
                status="pass" if (not run_inplane_comparison or comparison_path.exists()) else "fail",
                detail="in-plane comparison CSV exists",
                observed="skipped" if not run_inplane_comparison else str(comparison_path),
            ),
            StageCheckRecord(
                label="functional transform table",
                status="pass" if tforms_path.exists() and _csv_row_count(tforms_path) == len(reference_pairs) else "fail",
                detail="staged transform table has one row per functional reference",
                observed="missing" if not tforms_path.exists() else str(_csv_row_count(tforms_path)),
                expected=str(len(reference_pairs)),
            ),
            StageCheckRecord(
                label="plane refs summary",
                status="pass" if summary_path.exists() else "fail",
                detail="lightweight plane reference summary exists",
                observed=str(summary_path),
            ),
        ) + qa_checks
        status = "pass"
        errors: tuple[str, ...] = ()
        warnings: tuple[str, ...] = tuple(warnings_list)
        if any(check.status == "fail" for check in checks):
            status = "fail"
            errors = tuple(f"{check.label}: {check.observed}" for check in checks if check.status == "fail")
        log_lines = tuple(search_result.get("log_lines", ())) + tuple(comparison_result.get("log_lines", ()))
    except Exception as exc:
        reference_pairs = ()
        checks = ()
        status = "fail"
        errors = (str(exc),)
        warnings = ()
        log_lines = ()
    outputs = (
        describe_manifest_path(out_ncc / "ncc_scale_by_fish.json", label="staged NCC scale cache"),
        describe_manifest_path(out_ncc / "ncc_bestz_by_plane.json", label="staged NCC best-z cache"),
        describe_manifest_path(
            comparison_path,
            required=run_inplane_comparison,
            label="staged in-plane registration comparison",
        ),
        describe_manifest_path(
            recommendation_path,
            required=run_inplane_comparison,
            label="staged in-plane registration recommendation",
        ),
        describe_manifest_path(summary_path, label="staged plane refs summary"),
        describe_manifest_path(tforms_path, label="staged functional transform table"),
        describe_manifest_path(
            qa_overlay_path,
            required=False,
            label="functional/anatomy center overlay QA PNG",
        ),
        describe_manifest_path(
            qa_overlay_csv_path,
            required=False,
            label="functional/anatomy center overlay QA CSV",
        ),
        describe_glob(
            out_ncc / "inplane_registration_comparison",
            "*_ncc_xy_warped.tif",
            required=False,
            label="staged NCC warped functional references",
        ),
    )
    return StageManifest(
        manifest_version=PIPELINE_MANIFEST_VERSION,
        stage_name="register-functional-to-anatomy",
        fish_id=config.fish_id,
        status=status,
        dry_run=False,
        generated_at=datetime.now(timezone.utc).isoformat(),
        inputs=(
            describe_manifest_path(ref_dir, label="functional reference directory"),
            describe_manifest_path(anat_path, label="prepared in vivo anatomy stack"),
        )
        + tuple(
            describe_manifest_path(raw_path, label="functional reference raw TIFF")
            for _, raw_path, _ in reference_pairs
        ),
        outputs=outputs,
        checks=checks,
        parameters={
            "local_root": str(config.local_root),
            "force_recompute": bool(force_recompute),
            "functional_reference_dir": str(ref_dir),
            "anatomy_stack_path": str(anat_path),
            "output_root": str(stage_root),
            "registration_backend": str(active_inplane_method),
            "run_inplane_comparison": bool(run_inplane_comparison),
            "inplane_methods": tuple(inplane_methods),
            "use_cv2": bool(use_cv2),
            "emit_visual_qa": bool(emit_visual_qa),
            "visual_qa_crop_size_px": int(visual_qa_crop_size_px),
            "functional_anatomy_center_overlay_path": str(qa_overlay_path),
            "log_lines": log_lines,
        },
        warnings=warnings,
        errors=errors,
    )


def _discover_hcr_aligned_stage_files(source_root: Path) -> dict[str, tuple[Path, ...]]:
    files = tuple(path for path in sorted(source_root.glob("*")) if path.is_file() and _is_real_match(path))
    return {
        "label_tiffs": tuple(path for path in files if path.name.endswith("_cp_masks_in_2p_labels_uint16.tif")),
        "qc_tiffs": tuple(path for path in files if path.suffix.lower() in {".tif", ".tiff"} and "_cp_masks_in_2p_" in path.name),
        "match_csvs": tuple(path for path in files if path.name.endswith("_cp_masks_in_2p_matches.csv")),
        "review_csvs": tuple(path for path in files if path.name.endswith("_cp_masks_in_2p_review.csv")),
        "final_pair_csvs": tuple(path for path in files if path.name.endswith("_cp_masks_in_2p_final_pairs.csv")),
        "warp_meta_jsons": tuple(path for path in files if path.name.endswith("_warp_meta.json")),
        "aligned_nrrds": tuple(path for path in files if path.name.endswith("_in_2p.nrrd")),
    }


def _copy_hcr_aligned_stage_files(
    source_root: Path,
    output_root: Path,
    *,
    skip_label_tiffs: bool = False,
    skip_warp_meta_jsons: bool = False,
    skip_match_csvs: bool = False,
) -> tuple[Path, ...]:
    groups = _discover_hcr_aligned_stage_files(source_root)
    qc_tiffs = groups["qc_tiffs"]
    if skip_label_tiffs:
        qc_tiffs = tuple(path for path in qc_tiffs if path not in set(groups["label_tiffs"]))
    warp_meta_jsons = () if skip_warp_meta_jsons else groups["warp_meta_jsons"]
    match_csvs = () if skip_match_csvs else groups["match_csvs"]
    review_csvs = () if skip_match_csvs else groups["review_csvs"]
    final_pair_csvs = () if skip_match_csvs else groups["final_pair_csvs"]
    copied: list[Path] = []
    stage_inputs = (
        *qc_tiffs,
        *match_csvs,
        *review_csvs,
        *final_pair_csvs,
        *warp_meta_jsons,
    )
    for source_path in dict.fromkeys(stage_inputs):
        target_path = output_root / source_path.name
        target_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source_path, target_path)
        copied.append(target_path)
    return tuple(copied)


def _hcr_final_pair_schema_status(paths: tuple[Path, ...]) -> tuple[str, str]:
    missing: list[str] = []
    for path in paths:
        header = _csv_header(path) or ()
        missing_columns = [column for column in HCR_ALIGNED_PAIR_COLUMNS if column not in header]
        if missing_columns:
            missing.append(f"{path.name}: missing {','.join(missing_columns)}")
    if missing:
        return "fail", "; ".join(missing)
    return "pass", "complete"


def _hcr_final_pair_acceptance_status(paths: tuple[Path, ...]) -> tuple[str, str]:
    bad: list[str] = []
    n_rows = 0
    for path in paths:
        rows = _csv_dict_rows(path) or ()
        n_rows += len(rows)
        for index, row in enumerate(rows, start=2):
            if str(row.get("quality", "")).strip().lower() != "good":
                bad.append(f"{path.name}:{index}: quality={row.get('quality', '')}")
            if str(row.get("pair_type", "")).strip() != "1-1":
                bad.append(f"{path.name}:{index}: pair_type={row.get('pair_type', '')}")
            if str(row.get("within_gate", "")).strip().lower() not in {"true", "1", "yes"}:
                bad.append(f"{path.name}:{index}: within_gate={row.get('within_gate', '')}")
    if bad:
        return "fail", "; ".join(bad[:10])
    return "pass" if n_rows > 0 else "warn", str(n_rows)


def _hcr_match_results_from_staged_final_pairs(hcr_anatomy_root: Path) -> list[dict[str, Any]]:
    import pandas as pd

    results: list[dict[str, Any]] = []
    for final_pairs_path in sorted(hcr_anatomy_root.glob("*_cp_masks_in_2p_final_pairs.csv")):
        if not _is_real_match(final_pairs_path):
            continue
        mask_name = final_pairs_path.name.replace("_final_pairs.csv", "_labels_uint16.tif")
        mask_path = hcr_anatomy_root / mask_name
        final_pairs = pd.read_csv(final_pairs_path)
        results.append(
            {
                "mask_path": str(mask_path),
                "final_pairs": final_pairs,
                "final_pairs_path": str(final_pairs_path),
            }
        )
    return results


def _matching_metadata_row(path: Path, fish_id: str) -> dict[str, str]:
    if not path.exists() or not path.is_file():
        return {}
    with path.open(newline="") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            return {}
        normalized = {str(field).lstrip("\ufeff").strip().lower(): field for field in reader.fieldnames}
        fish_col = next((normalized[key] for key in ("fish_id", "fish", "fishid") if key in normalized), None)
        if fish_col is None:
            return {}
        for row in reader:
            if str(row.get(fish_col, "")).strip() == str(fish_id):
                return {str(key).lstrip("\ufeff"): str(value).strip() for key, value in row.items()}
    return {}


def _matching_metadata_value(row: dict[str, str], *names: str) -> str | None:
    normalized = {key.strip().lower(): value for key, value in row.items()}
    for name in names:
        value = normalized.get(name.strip().lower())
        if value not in (None, ""):
            return value
    return None


def _parse_bool_text(value: str | None) -> bool | None:
    if value is None:
        return None
    text = str(value).strip().lower()
    if text in {"true", "1", "yes", "y"}:
        return True
    if text in {"false", "0", "no", "n"}:
        return False
    return None


def _hcr_channel_nrrds(directory: Path) -> tuple[Path, ...]:
    if not directory.exists():
        return ()
    return tuple(
        path
        for path in sorted(directory.glob("*.nrrd"))
        if path.is_file() and _is_real_match(path) and "gcamp" not in path.name.lower()
    )


def _transform_file_counts(directory: Path) -> dict[str, int]:
    if not directory.exists():
        return {"affine": 0, "warp": 0, "inverse_warp": 0}
    files = tuple(path for path in directory.glob("*") if path.is_file() and _is_real_match(path))
    return {
        "affine": sum(1 for path in files if path.name.endswith("0GenericAffine.mat")),
        "warp": sum(1 for path in files if path.name.endswith("1Warp.nii.gz")),
        "inverse_warp": sum(1 for path in files if path.name.endswith("1InverseWarp.nii.gz")),
    }


def _count_real_files(directory: Path, pattern: str) -> int:
    return len(tuple(path for path in directory.glob(pattern) if path.is_file() and _is_real_match(path)))


def _anatomy_zyx_spacing_from_voxel_cache(paths: PipelinePaths) -> dict[str, float]:
    cache_path = paths.analysis_dir / "voxel_sizes.json"
    if not cache_path.exists():
        return {"dz": 1.0, "dy": 1.0, "dx": 1.0}
    try:
        data = json.loads(cache_path.read_text())
    except Exception:
        return {"dz": 1.0, "dy": 1.0, "dx": 1.0}
    records = tuple((str(path), values) for path, values in (data.get("by_path") or {}).items())
    preferred_records = tuple(
        item
        for item in records
        if "anatomy" in item[0].lower() or "2p_anatomy" in item[0].lower()
    )
    for _path, values in preferred_records + records:
        if not isinstance(values, dict):
            continue
        if all(key in values and values.get(key) not in (None, "", False) for key in ("X", "Y", "Z")):
            try:
                return {"dz": float(values["Z"]), "dy": float(values["Y"]), "dx": float(values["X"])}
            except Exception:
                continue
    return {"dz": 1.0, "dy": 1.0, "dx": 1.0}


def _hcr_raw_mask_paths(paths: PipelinePaths) -> tuple[Path, ...]:
    if not paths.confocal_raw_cp_masks_dir.exists():
        return ()
    return tuple(
        path
        for path in sorted(
            [*paths.confocal_raw_cp_masks_dir.glob("*round*_cp_masks*.tif"), *paths.confocal_raw_cp_masks_dir.glob("*round*_cp_masks*.tiff")]
        )
        if path.is_file() and _is_real_match(path)
    )


def _hcr_direct_filter_stats_recompute_count(direct_warp_results: tuple[Any, ...]) -> int:
    count = 0
    for result in direct_warp_results:
        metadata_path = Path(result.output_metadata_path)
        if not metadata_path.exists():
            continue
        try:
            payload = json.loads(metadata_path.read_text())
        except Exception:
            continue
        filter_stats = payload.get("filter_stats")
        if not isinstance(filter_stats, dict):
            continue
        if str(filter_stats.get("filter_policy_version", "")) != "label_voxel_floor_v3":
            continue
        if "n_dropped_small_components_abs" not in filter_stats:
            continue
        count += 1
    return count


def _hcr_final_pair_key_set(path: Path) -> set[tuple[str, str]]:
    rows = _csv_dict_rows(path) or ()
    return {
        (str(row.get("conf_label", "")).strip(), str(row.get("twoP_label", "")).strip())
        for row in rows
        if str(row.get("conf_label", "")).strip() and str(row.get("twoP_label", "")).strip()
    }


def _hcr_final_pair_key_parity_checks(source_root: Path, output_root: Path) -> tuple[StageCheckRecord, ...]:
    checks: list[StageCheckRecord] = []
    for output_path in sorted(output_root.glob("*_cp_masks_in_2p_final_pairs.csv")):
        if not _is_real_match(output_path):
            continue
        source_path = source_root / output_path.name
        if not source_path.exists():
            checks.append(
                StageCheckRecord(
                    label=f"HCR recomputed final-pair key parity: {output_path.name}",
                    status="warn",
                    detail="accepted final-pair CSV control is missing for recomputed HCR/anatomy pairs",
                    observed="accepted missing",
                    expected="accepted control exists",
                )
            )
            continue
        left = _hcr_final_pair_key_set(output_path)
        right = _hcr_final_pair_key_set(source_path)
        missing = right - left
        extra = left - right
        checks.append(
            StageCheckRecord(
                label=f"HCR recomputed final-pair key parity: {output_path.name}",
                status="pass" if not missing and not extra else "fail",
                detail="recomputed HCR/anatomy final-pair conf/twoP keys match accepted control",
                observed=f"missing={len(missing)},extra={len(extra)},both={len(left & right)}",
                expected="missing=0,extra=0",
            )
        )
    return tuple(checks)


def _write_recomputed_hcr_anatomy_match_csvs(
    *,
    direct_warp_results: tuple[Any, ...],
    anatomy_labels_path: Path,
    output_root: Path,
    vox_anat_um: dict[str, float],
) -> tuple[Path, ...]:
    from .matching import build_hcr_anatomy_match_tables
    from .spatial import imread_any

    anatomy_labels = imread_any(anatomy_labels_path)
    written: list[Path] = []
    for result in direct_warp_results:
        label_path = Path(result.output_label_path)
        if not label_path.exists():
            continue
        hcr_labels = imread_any(label_path)
        matches, final_pairs, review, _qc = build_hcr_anatomy_match_tables(
            hcr_labels,
            anatomy_labels,
            vox_anat_um=vox_anat_um,
        )
        save_base = output_root / label_path.name.replace("_labels_uint16.tif", "")
        paths_and_frames = (
            (Path(f"{save_base}_matches.csv"), matches),
            (Path(f"{save_base}_final_pairs.csv"), final_pairs),
            (Path(f"{save_base}_review.csv"), review),
        )
        for path, frame in paths_and_frames:
            path.parent.mkdir(parents=True, exist_ok=True)
            frame.to_csv(path, index=False)
            written.append(path)
    return tuple(written)


def _hcr_direct_ants_recompute_provenance(
    paths: PipelinePaths,
) -> tuple[dict[str, Any], tuple[StageCheckRecord, ...]]:
    metadata_row = _matching_metadata_row(paths.matching_metadata_csv, paths.fish_dir.name)
    best_round = _matching_metadata_value(metadata_row, "best_round", "best round")
    num_rounds = _matching_metadata_value(metadata_row, "num_rounds", "num rounds", "n_rounds")
    bigwarp = _parse_bool_text(_matching_metadata_value(metadata_row, "bigwarp", "big_warp", "use_bigwarp"))
    rbest_dir = paths.preproc_dir / "rbest"
    rn_dir = paths.preproc_dir / "rn"
    rbest_to_2p_dir = paths.preproc_dir.parent / "01_rbest-2p" / "transMatrices"
    rn_to_rbest_dir = paths.preproc_dir.parent / "02_rn-rbest" / "transMatrices"
    raw_cp_mask_count = _count_real_files(paths.confocal_raw_cp_masks_dir, "*_cp_masks.tif")
    rbest_hcr_nrrds = _hcr_channel_nrrds(rbest_dir)
    rn_hcr_nrrds = _hcr_channel_nrrds(rn_dir)
    rbest_to_2p_counts = _transform_file_counts(rbest_to_2p_dir)
    rn_to_rbest_counts = _transform_file_counts(rn_to_rbest_dir)
    num_rounds_value: float | None = None
    try:
        num_rounds_value = float(num_rounds) if num_rounds not in (None, "") else None
    except ValueError:
        num_rounds_value = None
    needs_rn_to_rbest = bool(rn_hcr_nrrds) or (num_rounds_value is not None and num_rounds_value > 1)
    direct_ants_expected = bigwarp is False
    provenance: dict[str, Any] = {
        "matching_metadata_row_found": bool(metadata_row),
        "matching_metadata_best_round": best_round,
        "matching_metadata_num_rounds": num_rounds,
        "matching_metadata_bigwarp": bigwarp,
        "hcr_raw_cp_mask_count": raw_cp_mask_count,
        "hcr_rbest_nrrd_count": len(rbest_hcr_nrrds),
        "hcr_rn_nrrd_count": len(rn_hcr_nrrds),
        "hcr_rbest_to_2p_affine_count": rbest_to_2p_counts["affine"],
        "hcr_rbest_to_2p_warp_count": rbest_to_2p_counts["warp"],
        "hcr_rbest_to_2p_inverse_warp_count": rbest_to_2p_counts["inverse_warp"],
        "hcr_rn_to_rbest_affine_count": rn_to_rbest_counts["affine"],
        "hcr_rn_to_rbest_warp_count": rn_to_rbest_counts["warp"],
        "hcr_rn_to_rbest_inverse_warp_count": rn_to_rbest_counts["inverse_warp"],
        "hcr_direct_ants_expected": direct_ants_expected,
        "hcr_rn_to_rbest_required": needs_rn_to_rbest,
    }
    checks = [
        StageCheckRecord(
            label="HCR direct recompute matching metadata",
            status="pass" if metadata_row else "warn",
            detail="matchingMetadata.csv row is available to select the HCR warp route",
            observed="found" if metadata_row else "missing",
            expected=paths.fish_dir.name,
        ),
        StageCheckRecord(
            label="HCR direct recompute route",
            status="pass" if direct_ants_expected else "warn",
            detail="matching metadata selects the direct ANTs HCR warp route rather than external BigWarp",
            observed=f"bigwarp={bigwarp}",
            expected="bigwarp=False",
        ),
        StageCheckRecord(
            label="HCR direct recompute raw masks",
            status="pass" if raw_cp_mask_count > 0 else "warn",
            detail="raw HCR Cellpose masks are available as warp inputs",
            observed=str(raw_cp_mask_count),
            expected=">=1",
        ),
        StageCheckRecord(
            label="HCR direct recompute rbest NRRDs",
            status="pass" if len(rbest_hcr_nrrds) > 0 else "warn",
            detail="rbest HCR intensity NRRDs are available for mask/intensity pairing",
            observed=str(len(rbest_hcr_nrrds)),
            expected=">=1",
        ),
        StageCheckRecord(
            label="HCR direct recompute rbest-to-2p transforms",
            status="pass" if rbest_to_2p_counts["affine"] > 0 and rbest_to_2p_counts["warp"] > 0 else "warn",
            detail="current fish rbest-to-2p ANTs affine and warp files are available",
            observed=f"affine={rbest_to_2p_counts['affine']},warp={rbest_to_2p_counts['warp']}",
            expected="affine>=1,warp>=1",
        ),
        StageCheckRecord(
            label="HCR direct recompute rn-to-rbest transforms",
            status=(
                "pass"
                if not needs_rn_to_rbest or (rn_to_rbest_counts["affine"] > 0 and rn_to_rbest_counts["warp"] > 0)
                else "warn"
            ),
            detail="current fish rn-to-rbest ANTs affine and warp files are available when non-rbest rounds are present",
            observed=(
                f"required={needs_rn_to_rbest},affine={rn_to_rbest_counts['affine']},"
                f"warp={rn_to_rbest_counts['warp']}"
            ),
            expected="not required or affine>=1,warp>=1",
        ),
    ]
    return provenance, tuple(checks)


def run_register_hcr_to_anatomy_stage(
    config: SingleFishPipelineConfig,
    *,
    source_root: str | Path | None = None,
    output_root: str | Path | None = None,
    force_recompute: bool = False,
    recompute_direct_ants: bool = False,
) -> StageManifest:
    from .context import infer_anat_labels_path

    paths = resolve_pipeline_paths(config)
    source_root_path = Path(source_root) if source_root not in (None, "", False) else paths.confocal_aligned_dir
    stage_root = Path(output_root) if output_root not in (None, "", False) else hcr_to_anatomy_registration_root(paths)
    aligned_output_root = stage_root / "confocal" / "aligned"
    try:
        source_groups = _discover_hcr_aligned_stage_files(source_root_path)
        existing_outputs = tuple(path for path in aligned_output_root.glob("*") if path.is_file() and _is_real_match(path))
        if existing_outputs and not force_recompute:
            raise FileExistsError(
                "register-hcr-to-anatomy outputs already exist; pass --force-recompute to overwrite: "
                + ", ".join(str(path) for path in existing_outputs[:10])
            )
        direct_ants_provenance, direct_ants_checks = _hcr_direct_ants_recompute_provenance(paths)
        direct_warp_results: tuple[Any, ...] = ()
        recomputed_match_outputs: tuple[Path, ...] = ()
        direct_filter_stats_recompute_count = 0
        anat_labels_path = infer_anat_labels_path(paths.fish_dir, config.fish_id)
        if recompute_direct_ants:
            if not direct_ants_provenance.get("hcr_direct_ants_expected"):
                raise RuntimeError("Direct ANTs HCR recompute requested, but matching metadata does not select bigwarp=False.")
            best_round_raw = direct_ants_provenance.get("matching_metadata_best_round")
            if best_round_raw in (None, ""):
                raise RuntimeError("Direct ANTs HCR recompute requested, but best_round is missing from matching metadata.")
            if anat_labels_path is None or not Path(anat_labels_path).exists():
                raise FileNotFoundError(f"Direct HCR/anatomy match recompute requires anatomy labels for {config.fish_id}")
            best_round_idx = int(str(best_round_raw).lower().lstrip("r"))
            from .hcr_warp import run_direct_ants_hcr_label_warp

            direct_warp_results = run_direct_ants_hcr_label_warp(
                fish_id=config.fish_id,
                mask_paths=_hcr_raw_mask_paths(paths),
                preproc_dir=paths.preproc_dir,
                anatomy_intensity_path=paths.anatomy_preproc_dir / f"{config.fish_id}_anatomy_2P_GCaMP.nrrd",
                output_dir=aligned_output_root,
                best_round_idx=best_round_idx,
                rbest_to_2p_transform_dir=paths.preproc_dir.parent / "01_rbest-2p" / "transMatrices",
                rn_to_rbest_transform_dir=paths.preproc_dir.parent / "02_rn-rbest" / "transMatrices",
            )
            accepted_filter_stats_overlay_count = 0
            direct_filter_stats_recompute_count = _hcr_direct_filter_stats_recompute_count(direct_warp_results)
            recomputed_match_outputs = _write_recomputed_hcr_anatomy_match_csvs(
                direct_warp_results=direct_warp_results,
                anatomy_labels_path=Path(anat_labels_path),
                output_root=aligned_output_root,
                vox_anat_um=_anatomy_zyx_spacing_from_voxel_cache(paths),
            )
        else:
            accepted_filter_stats_overlay_count = 0
            direct_filter_stats_recompute_count = 0
        copied_outputs = _copy_hcr_aligned_stage_files(
            source_root_path,
            aligned_output_root,
            skip_label_tiffs=bool(recompute_direct_ants),
            skip_warp_meta_jsons=bool(recompute_direct_ants),
            skip_match_csvs=bool(recompute_direct_ants),
        )
        recomputed_label_outputs = tuple(Path(result.output_label_path) for result in direct_warp_results) + tuple(
            Path(result.output_metadata_path) for result in direct_warp_results
        )
        recomputed_outputs = recomputed_label_outputs + recomputed_match_outputs
        staged_outputs = (*copied_outputs, *recomputed_outputs)
        copied_names = {path.name for path in copied_outputs}
        staged_label_count = sum(1 for path in staged_outputs if path.name.endswith("_cp_masks_in_2p_labels_uint16.tif"))
        copied_match_count = sum(1 for path in staged_outputs if path.name.endswith("_cp_masks_in_2p_matches.csv"))
        copied_final_pair_count = sum(1 for path in staged_outputs if path.name.endswith("_cp_masks_in_2p_final_pairs.csv"))
        staged_meta_count = sum(1 for path in staged_outputs if path.name.endswith("_warp_meta.json"))
        final_pair_paths = tuple(path for path in staged_outputs if path.name.endswith("_cp_masks_in_2p_final_pairs.csv"))
        final_pair_rows = sum((_csv_row_count(path) or 0) for path in final_pair_paths)
        final_pair_schema_status, final_pair_schema_observed = _hcr_final_pair_schema_status(final_pair_paths)
        final_pair_acceptance_status, final_pair_acceptance_observed = _hcr_final_pair_acceptance_status(final_pair_paths)
        checks = (
            StageCheckRecord(
                label="HCR aligned label TIFFs",
                status="pass" if staged_label_count > 0 else "fail",
                detail="staged aligned HCR label TIFFs exist",
                observed=str(staged_label_count),
                expected=">=1",
            ),
            StageCheckRecord(
                label="HCR aligned match CSVs",
                status="pass" if copied_match_count > 0 else "fail",
                detail="staged HCR/anatomy match CSVs exist",
                observed=str(copied_match_count),
                expected=">=1",
            ),
            StageCheckRecord(
                label="HCR aligned final-pair CSVs",
                status="pass" if copied_final_pair_count > 0 else "warn",
                detail="manual/accepted final-pair CSVs exist when review has been finalized",
                observed=str(copied_final_pair_count),
                expected=">=1",
            ),
            StageCheckRecord(
                label="HCR aligned final-pair rows",
                status="pass" if final_pair_rows > 0 else "warn",
                detail="accepted final-pair CSVs contain rows",
                observed=str(final_pair_rows),
                expected=">0",
            ),
            StageCheckRecord(
                label="HCR aligned final-pair schema",
                status=final_pair_schema_status,
                detail="accepted final-pair CSVs use the expected HCR/anatomy pair schema",
                observed=final_pair_schema_observed,
                expected=",".join(HCR_ALIGNED_PAIR_COLUMNS),
            ),
            StageCheckRecord(
                label="HCR aligned final-pair acceptance",
                status=final_pair_acceptance_status,
                detail="accepted final-pair rows are good 1-1 within-gate pairs",
                observed=final_pair_acceptance_observed,
                expected="quality=good,pair_type=1-1,within_gate=True",
            ),
            StageCheckRecord(
                label="HCR warp metadata",
                status="pass" if staged_meta_count > 0 else "warn",
                detail="warp metadata JSON sidecars were staged",
                observed=str(staged_meta_count),
                expected=">=1",
            ),
            StageCheckRecord(
                label="large aligned intensity volumes not copied",
                status="pass" if not any(name.endswith("_in_2p.nrrd") for name in copied_names) else "fail",
                detail="multi-GB aligned intensity NRRDs are recorded as inputs only for this baseline writer",
                observed=str(len(source_groups["aligned_nrrds"])),
            ),
            StageCheckRecord(
                label="HCR direct ANTs label warp recompute",
                status="pass" if (not recompute_direct_ants or len(direct_warp_results) > 0) else "fail",
                detail="raw HCR Cellpose masks were warped into 2P anatomy space with current direct ANTs transforms when requested",
                observed=str(len(direct_warp_results)),
                expected=">0 when recompute_direct_ants=True",
            ),
            StageCheckRecord(
                label="HCR direct ANTs filter stats recompute",
                status=(
                    "pass"
                    if not recompute_direct_ants or direct_filter_stats_recompute_count == len(direct_warp_results)
                    else "fail"
                ),
                detail="HCR prewarp label-filter statistics were recomputed into direct warp metadata",
                observed=f"{direct_filter_stats_recompute_count}/{len(direct_warp_results)}",
                expected="all recomputed masks",
            ),
            StageCheckRecord(
                label="HCR direct ANTs match table recompute",
                status="pass" if (not recompute_direct_ants or len(recomputed_match_outputs) >= 3 * len(direct_warp_results)) else "fail",
                detail="HCR/anatomy match, review, and final-pair CSVs were recomputed from warped labels when requested",
                observed=f"{len(recomputed_match_outputs)}/{3 * len(direct_warp_results)}",
                expected="3 CSVs per recomputed mask",
            ),
        ) + direct_ants_checks + (
            _hcr_final_pair_key_parity_checks(source_root_path, aligned_output_root) if recompute_direct_ants else ()
        )
        status = "pass" if not any(check.status == "fail" for check in checks) else "fail"
        errors: tuple[str, ...] = tuple(f"{check.label}: {check.observed}" for check in checks if check.status == "fail")
        warnings: tuple[str, ...] = tuple(f"{check.label}: {check.observed}" for check in checks if check.status == "warn")
    except Exception as exc:
        direct_ants_provenance = {}
        direct_warp_results = ()
        recomputed_label_outputs = ()
        recomputed_outputs = ()
        recomputed_match_outputs = ()
        accepted_filter_stats_overlay_count = 0
        direct_filter_stats_recompute_count = 0
        anat_labels_path = None
        source_groups = {
            "label_tiffs": (),
            "qc_tiffs": (),
            "match_csvs": (),
            "review_csvs": (),
            "final_pair_csvs": (),
            "warp_meta_jsons": (),
            "aligned_nrrds": (),
        }
        copied_outputs = ()
        checks = ()
        status = "fail"
        errors = (str(exc),)
        warnings = ()
    return StageManifest(
        manifest_version=PIPELINE_MANIFEST_VERSION,
        stage_name="register-hcr-to-anatomy",
        fish_id=config.fish_id,
        status=status,
        dry_run=False,
        generated_at=datetime.now(timezone.utc).isoformat(),
        inputs=(
            describe_manifest_path(source_root_path, label="accepted HCR aligned source root"),
            describe_glob(source_root_path, "*_cp_masks_in_2p_labels_uint16.tif", label="accepted HCR aligned label TIFFs"),
            describe_glob(source_root_path, "*_cp_masks_in_2p_matches.csv", label="accepted HCR/anatomy match CSVs"),
            describe_glob(source_root_path, "*_cp_masks_in_2p_final_pairs.csv", required=False, label="accepted HCR final-pair CSVs"),
            describe_glob(source_root_path, "*_warp_meta.json", required=False, label="accepted HCR warp metadata"),
            describe_glob(source_root_path, "*_in_2p.nrrd", required=False, label="accepted aligned intensity NRRDs"),
            describe_glob(paths.preproc_dir / "rbest", "*", required=False, label="rbest transform/input provenance"),
            describe_glob(paths.preproc_dir / "rn", "*", required=False, label="rn transform/input provenance"),
            describe_glob(paths.preproc_dir.parent / "01_rbest-2p" / "transMatrices", "*", required=False, label="rbest-to-2p transform provenance"),
            describe_glob(paths.preproc_dir.parent / "02_rn-rbest" / "transMatrices", "*", required=False, label="rn-to-rbest transform provenance"),
        ),
        outputs=(
            describe_manifest_path(aligned_output_root, label="staged HCR aligned artifact root"),
            describe_glob(aligned_output_root, "*_cp_masks_in_2p_labels_uint16.tif", label="staged HCR aligned label TIFFs"),
            describe_glob(aligned_output_root, "*_cp_masks_in_2p_matches.csv", label="staged HCR/anatomy match CSVs"),
            describe_glob(aligned_output_root, "*_cp_masks_in_2p_final_pairs.csv", required=False, label="staged HCR final-pair CSVs"),
            describe_glob(aligned_output_root, "*_warp_meta.json", required=False, label="staged HCR warp metadata"),
        )
        + tuple(describe_manifest_path(path, label=f"staged HCR aligned artifact: {path.name}") for path in copied_outputs)
        + tuple(describe_manifest_path(path, label=f"recomputed HCR direct ANTs artifact: {path.name}") for path in recomputed_outputs),
        checks=checks,
        parameters={
            "local_root": str(config.local_root),
            "source_root": str(source_root_path),
            "output_root": str(stage_root),
            "force_recompute": bool(force_recompute),
            "recompute_direct_ants": bool(recompute_direct_ants),
            "copied_artifact_count": len(copied_outputs),
            "recomputed_label_artifact_count": len(recomputed_label_outputs),
            "recomputed_artifact_count": len(recomputed_outputs),
            "direct_ants_warp_result_count": len(direct_warp_results),
            "direct_ants_filter_stats_overlay_count": accepted_filter_stats_overlay_count,
            "direct_ants_filter_stats_recompute_count": direct_filter_stats_recompute_count,
            "direct_ants_match_table_artifact_count": len(recomputed_match_outputs),
            "anatomy_labels_path": str(anat_labels_path) if anat_labels_path is not None else None,
            "anatomy_zyx_spacing_um": _anatomy_zyx_spacing_from_voxel_cache(paths),
            "input_aligned_nrrd_count": len(source_groups["aligned_nrrds"]),
            "copy_policy": "csv_json_tif_only",
            "hcr_recompute_mode": (
                "direct_ants_label_warp_and_match_tables"
                if recompute_direct_ants
                else "accepted_artifact_staging_with_direct_ants_readiness"
            ),
            **direct_ants_provenance,
        },
        warnings=warnings,
        errors=errors,
    )


def run_match_roi_to_anatomy_stage(
    config: SingleFishPipelineConfig,
    *,
    source_root: str | Path | None = None,
    plane_refs_summary_path: str | Path | None = None,
    anatomy_labels_path: str | Path | None = None,
    output_root: str | Path | None = None,
    force_recompute: bool = False,
) -> StageManifest:
    import pandas as pd

    from .context import infer_anat_labels_path, resolve_func_polarity
    from .matching import FunctionalRoiIdentityConfig, build_functional_roi_master_df, summarize_functional_anatomy_geometry_metrics
    from .spatial import imread_any
    from .suite2p import Suite2pStageConfig, load_suite2p_stage

    paths = resolve_pipeline_paths(config)
    stage_root = Path(output_root) if output_root not in (None, "", False) else roi_to_anatomy_match_root(paths)
    registration_dir = stage_root / "registration"
    detail_path = registration_dir / "functional_roi_anatomy_matches.csv"
    summary_path = registration_dir / "functional_roi_anatomy_match_by_plane.csv"
    meta_path = registration_dir / "functional_roi_anatomy_match_plane_meta.csv"
    source_root_path = Path(source_root) if source_root not in (None, "", False) else None
    plane_summary_path = (
        Path(plane_refs_summary_path)
        if plane_refs_summary_path not in (None, "", False)
        else functional_to_anatomy_registration_root(paths) / "plane_refs_summary.json"
    )
    anat_labels_path = (
        Path(anatomy_labels_path)
        if anatomy_labels_path not in (None, "", False)
        else infer_anat_labels_path(paths.fish_dir, config.fish_id)
    )
    try:
        existing_outputs = tuple(path for path in (detail_path, summary_path, meta_path) if path.exists())
        if existing_outputs and not force_recompute:
            raise FileExistsError(
                "match-roi-to-anatomy outputs already exist; pass --force-recompute to overwrite: "
                + ", ".join(str(path) for path in existing_outputs)
            )
        if source_root_path is not None:
            source_detail = source_root_path / "functional_roi_activity_identity.csv"
            if not source_detail.exists():
                raise FileNotFoundError(f"Control ROI identity table not found: {source_detail}")
            source_df = pd.read_csv(source_detail)
            missing_required = [
                column
                for column in ("plane_idx", "func_label", "selected_anat_label", "has_unique_anat_match", "anat_label")
                if column not in source_df.columns
            ]
            if missing_required:
                raise RuntimeError(
                    f"Control ROI identity table is missing geometry columns: {', '.join(missing_required)}"
                )
            geometry_columns = [column for column in ROI_ANATOMY_GEOMETRY_COLUMNS if column in source_df.columns]
            detail_df = source_df.loc[:, geometry_columns].copy()
            summary_df = summarize_functional_anatomy_geometry_metrics(
                detail_df,
                method="accepted_control_geometry",
                fish_id=config.fish_id,
            )
            source_meta = source_root_path / "functional_roi_activity_identity_by_plane.csv"
            if source_meta.exists():
                plane_meta_df = pd.read_csv(source_meta)
                plane_meta_df = plane_meta_df.loc[:, [column for column in plane_meta_df.columns if column not in {"identity_label", "response_class", "response_summary_class", "bpi", "bpi_category"}]]
            else:
                plane_meta_df = summary_df.copy()
            suite2p_plane_count = int(detail_df["plane_idx"].nunique()) if "plane_idx" in detail_df.columns else 0
            mode = "control_geometry"
            polarity = None
            polarity_source = None
            anatomy_xy_spacing = (1.0, 1.0)
            transform_report = {"overlay_applied_planes": 0}
            accepted_parity_checks: tuple[StageCheckRecord, ...] = ()
        else:
            if anat_labels_path is None:
                raise FileNotFoundError(f"No anatomy labels found under {paths.fish_dir}")
            plane_refs = load_plane_refs_summary(plane_summary_path)
            anatomy_xy_spacing = _anatomy_xy_spacing_from_voxel_cache(paths)
            plane_refs, transform_report = _overlay_selected_ants_transformlists(
                plane_refs,
                _selected_ants_inplane_comparison_path(paths),
                xy_spacing=anatomy_xy_spacing,
            )
            polarity, polarity_source = resolve_func_polarity(
                config.fish_id,
                paths.matching_metadata_csv,
                fish_dir=paths.fish_dir,
            )

            def _apply_match_func_orientation(arr):
                from .spatial import apply_func_orientation

                return apply_func_orientation(arr, polarity=polarity, flip_x=True)

            suite2p_result = load_suite2p_stage(
                plane_refs=plane_refs,
                suite2p_root=paths.functional_suite2p_dir,
                fish_id=config.fish_id,
                polarity=polarity,
                polarity_source=polarity_source,
                config=Suite2pStageConfig(verbose=False),
            )
            anat_labels = imread_any(anat_labels_path)
            cfg = FunctionalRoiIdentityConfig()
            detail_df, plane_meta_df = build_functional_roi_master_df(
                suite2p_result["suite2p_by_ref_idx"],
                plane_refs,
                anat_labels,
                dx_um=float(anatomy_xy_spacing[0]),
                dy_um=float(anatomy_xy_spacing[1]),
                fish_id=config.fish_id,
                active_class=cfg.active_class,
                inactive_class=cfg.inactive_class,
                require_overlap=cfg.require_overlap_func_anat,
                min_overlap=cfg.min_overlap_func_anat,
                max_dist_um=cfg.max_dist_func_anat,
                plane_unavailable=cfg.plane_unavailable,
                func_match_ok=cfg.func_match_ok,
                func_no_slot=cfg.func_no_slot,
                func_no_overlap=cfg.func_no_overlap,
                func_lost_overlap=cfg.func_lost_overlap,
                func_too_far=cfg.func_too_far,
                func_no_anat=cfg.func_no_anat,
                claim_matched=cfg.claim_matched,
                claim_duplicate=cfg.claim_duplicate,
                claim_unmatched=cfg.claim_unmatched,
                apply_func_orientation_func=_apply_match_func_orientation,
            )
            geometry_columns = [column for column in ROI_ANATOMY_GEOMETRY_COLUMNS if column in detail_df.columns]
            detail_df = detail_df.loc[:, geometry_columns].copy()
            summary_df = summarize_functional_anatomy_geometry_metrics(
                detail_df,
                method="staged_register_functional_to_anatomy",
                fish_id=config.fish_id,
            )
            accepted_parity_checks = ()
            accepted_detail_path = paths.functional_registration_dir / "functional_roi_activity_identity.csv"
            if accepted_detail_path.exists():
                accepted_df = pd.read_csv(accepted_detail_path)
                parity_columns = ("plane_idx", "func_label", "anat_label", "selected_anat_label", "has_unique_anat_match")
                if all(column in accepted_df.columns for column in parity_columns) and all(column in detail_df.columns for column in parity_columns):
                    merged = detail_df.loc[:, list(parity_columns)].merge(
                        accepted_df.loc[:, list(parity_columns)],
                        on=["plane_idx", "func_label"],
                        how="outer",
                        suffixes=("_staged", "_accepted"),
                        indicator=True,
                    )
                    left_only = int((merged["_merge"] == "left_only").sum())
                    right_only = int((merged["_merge"] == "right_only").sum())

                    def _numeric_parity_count(column: str) -> int:
                        staged_values = pd.to_numeric(merged[f"{column}_staged"], errors="coerce")
                        accepted_values = pd.to_numeric(merged[f"{column}_accepted"], errors="coerce")
                        equal = (staged_values.isna() & accepted_values.isna()) | (staged_values == accepted_values)
                        return int(equal.sum())

                    anat_label_equal = _numeric_parity_count("anat_label")
                    selected_label_equal = _numeric_parity_count("selected_anat_label")
                    staged_unique = merged["has_unique_anat_match_staged"].astype("boolean")
                    accepted_unique = merged["has_unique_anat_match_accepted"].astype("boolean")
                    unique_equal = int(((staged_unique.isna() & accepted_unique.isna()) | (staged_unique == accepted_unique)).sum())
                    accepted_parity_checks = (
                        StageCheckRecord(
                            label="accepted ROI/anatomy key parity",
                            status="pass" if left_only == 0 and right_only == 0 else "fail",
                            detail="recomputed ROI/anatomy geometry keys match accepted control when present",
                            observed=f"both={int((merged['_merge'] == 'both').sum())},left_only={left_only},right_only={right_only}",
                            expected=f"both={len(merged)},left_only=0,right_only=0",
                        ),
                        StageCheckRecord(
                            label="accepted ROI/anatomy label parity",
                            status="pass" if anat_label_equal == len(merged) and selected_label_equal == len(merged) else "fail",
                            detail="recomputed anatomy labels match accepted control when present",
                            observed=f"anat_label={anat_label_equal}/{len(merged)},selected_anat_label={selected_label_equal}/{len(merged)}",
                            expected=f"{len(merged)}/{len(merged)}",
                        ),
                        StageCheckRecord(
                            label="accepted ROI/anatomy unique-match parity",
                            status="pass" if unique_equal == len(merged) else "fail",
                            detail="recomputed unique-match flags match accepted control when present",
                            observed=f"{unique_equal}/{len(merged)}",
                            expected=f"{len(merged)}/{len(merged)}",
                        ),
                    )
                else:
                    missing = [
                        column
                        for column in parity_columns
                        if column not in accepted_df.columns or column not in detail_df.columns
                    ]
                    accepted_parity_checks = (
                        StageCheckRecord(
                            label="accepted ROI/anatomy parity inputs",
                            status="warn",
                            detail="accepted-control parity skipped because required columns are missing",
                            observed=",".join(missing),
                        ),
                    )
            suite2p_plane_count = len(suite2p_result["suite2p_by_ref_idx"])
            mode = "recompute_from_staged_registration"
        registration_dir.mkdir(parents=True, exist_ok=True)
        detail_df.to_csv(detail_path, index=False)
        summary_df.to_csv(summary_path, index=False)
        plane_meta_df.to_csv(meta_path, index=False)
        checks = (
            StageCheckRecord(
                label="ROI/anatomy planes loaded" if source_root_path is not None else "Suite2p planes loaded",
                status="pass" if suite2p_plane_count > 0 else "fail",
                detail="control geometry planes were loaded" if source_root_path is not None else "Suite2p labels were loaded for ROI/anatomy matching",
                observed=str(suite2p_plane_count),
                expected=">=1",
            ),
            StageCheckRecord(
                label="ROI/anatomy match rows",
                status="pass" if len(detail_df) > 0 else "fail",
                detail="geometry match table has ROI rows",
                observed=str(len(detail_df)),
                expected=">0",
            ),
            StageCheckRecord(
                label="ROI/anatomy unique matches",
                status="pass" if ("has_unique_anat_match" in detail_df.columns and bool(detail_df["has_unique_anat_match"].astype(bool).any())) else "warn",
                detail="at least one ROI has a unique anatomy match",
                observed=str(int(detail_df["has_unique_anat_match"].astype(bool).sum())) if "has_unique_anat_match" in detail_df.columns else "missing",
                expected=">=1",
            ),
            StageCheckRecord(
                label="geometry-only columns",
                status="pass" if not any(column in detail_df.columns for column in ("identity_label", "has_identity_assigned", "response_class", "response_summary_class", "bpi", "bpi_category", "gene")) else "fail",
                detail="match-roi-to-anatomy output excludes identity, HCR, response, and BPI columns",
                observed=",".join(detail_df.columns),
            ),
            StageCheckRecord(
                label="ROI/anatomy summary",
                status="pass" if summary_path.exists() else "fail",
                detail="geometry summary CSV exists",
                observed=str(summary_path),
            ),
        ) + accepted_parity_checks
        status = "pass" if not any(check.status == "fail" for check in checks) else "fail"
        errors: tuple[str, ...] = tuple(f"{check.label}: {check.observed}" for check in checks if check.status == "fail")
        warnings: tuple[str, ...] = tuple(f"{check.label}: {check.observed}" for check in checks if check.status == "warn")
    except Exception as exc:
        checks = ()
        status = "fail"
        errors = (str(exc),)
        warnings = ()
    return StageManifest(
        manifest_version=PIPELINE_MANIFEST_VERSION,
        stage_name="match-roi-to-anatomy",
        fish_id=config.fish_id,
        status=status,
        dry_run=False,
        generated_at=datetime.now(timezone.utc).isoformat(),
        inputs=(
            describe_manifest_path(source_root_path, label="control geometry source root", required=False) if source_root_path is not None else _optional_manifest_path(None, label="control geometry source root", required=False),
            describe_manifest_path(plane_summary_path, label="staged plane refs summary", required=source_root_path is None),
            describe_manifest_path(anat_labels_path, label="anatomy label stack", required=source_root_path is None) if anat_labels_path is not None else _optional_manifest_path(None, label="anatomy label stack", required=source_root_path is None),
            describe_manifest_path(paths.functional_suite2p_dir, label="Suite2p root", required=source_root_path is None),
        ),
        outputs=(
            describe_manifest_path(detail_path, label="staged ROI/anatomy geometry matches"),
            describe_manifest_path(summary_path, label="staged ROI/anatomy geometry summary"),
            describe_manifest_path(meta_path, label="staged ROI/anatomy plane metadata"),
        ),
        checks=checks,
        parameters={
            "local_root": str(config.local_root),
            "force_recompute": bool(force_recompute),
            "source_root": str(source_root_path) if source_root_path is not None else None,
            "plane_refs_summary_path": str(plane_summary_path),
            "anatomy_labels_path": str(anat_labels_path) if anat_labels_path is not None else None,
            "output_root": str(stage_root),
            "match_policy_version": FunctionalRoiIdentityConfig().match_policy_version,
            "geometry_only": True,
            "mode": mode if "mode" in locals() else None,
            "functional_polarity": polarity if "polarity" in locals() else None,
            "functional_polarity_source": polarity_source if "polarity_source" in locals() else None,
            "anatomy_xy_spacing_um": tuple(float(v) for v in anatomy_xy_spacing) if "anatomy_xy_spacing" in locals() else None,
            "selected_ants_overlay_count": int((transform_report or {}).get("overlay_applied_planes", 0)) if "transform_report" in locals() else 0,
            "selected_ants_missing_transform_files": int((transform_report or {}).get("missing_transform_files", 0)) if "transform_report" in locals() else 0,
        },
        warnings=warnings,
        errors=errors,
    )


def run_prepare_ex_vivo_anatomy_stack_stage(
    config: SingleFishPipelineConfig,
    *,
    ex_vivo_stack_path: str | Path | None = None,
    output_path: str | Path | None = None,
    force_recompute: bool = False,
) -> StageManifest:
    from .context import ExVivoAnatomyPreprocessingConfig, preprocess_ex_vivo_anatomy_stage

    paths = resolve_pipeline_paths(config)
    out_path = Path(output_path) if output_path not in (None, "", False) else prepared_ex_vivo_anatomy_path(paths)
    try:
        source_path = Path(ex_vivo_stack_path) if ex_vivo_stack_path not in (None, "", False) else discover_ex_vivo_anatomy_stack(paths)
        result = preprocess_ex_vivo_anatomy_stage(
            fish_id=config.fish_id,
            ex_vivo_stack_path=source_path,
            preproc_dir=paths.preproc_dir,
            output_path=out_path,
            config=ExVivoAnatomyPreprocessingConfig(force_recompute=force_recompute),
        )
        artifacts = result.get("artifacts", {})
        status = "pass"
        errors: tuple[str, ...] = ()
        warnings: tuple[str, ...] = ()
        checks = (
            StageCheckRecord(
                label="prepared ex vivo anatomy stack",
                status="pass" if Path(artifacts.get("registration_nrrd_path", out_path)).exists() else "fail",
                detail="registration-ready ex vivo anatomy NRRD exists",
                observed=str(artifacts.get("registration_nrrd_path", out_path)),
            ),
        )
    except Exception as exc:
        source_path = Path(ex_vivo_stack_path) if ex_vivo_stack_path not in (None, "", False) else paths.raw_2p_anatomy_dir
        result = {"artifacts": {}, "log_lines": []}
        status = "fail"
        errors = (str(exc),)
        warnings = ()
        checks = ()
    outputs = (
        describe_manifest_path(out_path, label="prepared ex vivo anatomy NRRD"),
        describe_manifest_path(Path(str(out_path) + ".json"), label="prepared ex vivo anatomy metadata"),
    )
    return StageManifest(
        manifest_version=PIPELINE_MANIFEST_VERSION,
        stage_name="prepare-ex-vivo-anatomy-stack",
        fish_id=config.fish_id,
        status=status,
        dry_run=False,
        generated_at=datetime.now(timezone.utc).isoformat(),
        inputs=(describe_manifest_path(source_path, label="raw ex vivo anatomy stack"),),
        outputs=outputs,
        checks=checks,
        parameters={
            "local_root": str(config.local_root),
            "force_recompute": bool(force_recompute),
            "output_root": str(ex_vivo_structural_root(paths)),
            "log_lines": tuple(result.get("log_lines", ())),
        },
        warnings=warnings,
        errors=errors,
    )


def run_segment_ex_vivo_anatomy_cellpose_stage(
    config: SingleFishPipelineConfig,
    *,
    anatomy_stack_path: str | Path | None = None,
    anat_cp_model_path: str | Path | None = None,
    use_gpu: bool | None = None,
    compute_device: str | None = None,
    force_recompute: bool = False,
) -> StageManifest:
    paths = resolve_pipeline_paths(config)
    structural_root = ex_vivo_structural_root(paths)
    source_path = Path(anatomy_stack_path) if anatomy_stack_path not in (None, "", False) else prepared_ex_vivo_anatomy_path(paths)
    model_path = Path(anat_cp_model_path) if anat_cp_model_path not in (None, "", False) else None
    early_errors: list[str] = []
    if not source_path.exists():
        early_errors.append(f"missing prepared ex vivo anatomy stack: {source_path}")
    if model_path is None or not model_path.exists():
        early_errors.append(f"missing anatomy Cellpose model: {model_path or ''}")
    if early_errors:
        return StageManifest(
            manifest_version=PIPELINE_MANIFEST_VERSION,
            stage_name="segment-ex-vivo-anatomy-cellpose",
            fish_id=config.fish_id,
            status="fail",
            dry_run=False,
            generated_at=datetime.now(timezone.utc).isoformat(),
            inputs=(
                describe_manifest_path(source_path, label="prepared ex vivo anatomy stack"),
                _optional_manifest_path(model_path, label="anatomy Cellpose model"),
            ),
            outputs=(describe_manifest_path(structural_root / "cp_masks", required=False, label="ex vivo anatomy Cellpose mask directory"),),
            checks=(),
            parameters={
                "local_root": str(config.local_root),
                "force_recompute": bool(force_recompute),
                "use_gpu": use_gpu,
                "compute_device": compute_device,
                "output_root": str(structural_root),
                "log_lines": (),
            },
            warnings=(),
            errors=tuple(early_errors),
        )
    from .segmentation import AnatomyCellposeConfig, run_anatomy_cellpose_stage

    try:
        result = run_anatomy_cellpose_stage(
            anat_seg_source_path=source_path,
            analysis_dir=paths.analysis_dir,
            output_root=structural_root,
            anat_cp_model_path=model_path,
            fish_id=config.fish_id,
            config=AnatomyCellposeConfig(
                force_recompute=force_recompute,
                skip_if_exists=True,
                use_gpu=use_gpu,
                compute_device=compute_device,
            ),
        )
        label_path = Path(result["bindings"]["ANAT_LABELS_PATH"])
        n_labels = _tiff_label_count(label_path)
        checks = (
            StageCheckRecord(
                label="ex vivo anatomy Cellpose labels",
                status="pass" if label_path.exists() else "fail",
                detail="ex vivo anatomy masks exist under structural/ex_vivo",
                observed=str(label_path),
            ),
            StageCheckRecord(
                label="ex vivo anatomy label count",
                status="pass" if n_labels is None or n_labels >= 0 else "fail",
                detail="non-background label count from mask TIFF",
                observed=None if n_labels is None else str(n_labels),
            ),
        )
        status = "pass" if all(check.status != "fail" for check in checks) else "fail"
        errors: tuple[str, ...] = ()
        warnings: tuple[str, ...] = ()
        outputs = (
            describe_manifest_path(label_path, label="ex vivo anatomy Cellpose masks"),
            describe_glob(structural_root / "raw" / "converted_nrrd_to_tif", "*_8bit.tif", required=False, label="ex vivo anatomy converted uint8 TIFFs"),
        )
    except Exception as exc:
        result = {"bindings": {}, "log_lines": []}
        status = "fail"
        errors = (str(exc),)
        warnings = ()
        checks = ()
        outputs = (describe_manifest_path(structural_root / "cp_masks", required=False, label="ex vivo anatomy Cellpose mask directory"),)
    return StageManifest(
        manifest_version=PIPELINE_MANIFEST_VERSION,
        stage_name="segment-ex-vivo-anatomy-cellpose",
        fish_id=config.fish_id,
        status=status,
        dry_run=False,
        generated_at=datetime.now(timezone.utc).isoformat(),
        inputs=(
            describe_manifest_path(source_path, label="prepared ex vivo anatomy stack"),
            _optional_manifest_path(model_path, label="anatomy Cellpose model"),
        ),
        outputs=outputs,
        checks=checks,
        parameters={
            "local_root": str(config.local_root),
            "force_recompute": bool(force_recompute),
            "use_gpu": use_gpu,
            "compute_device": compute_device,
            "output_root": str(structural_root),
            "log_lines": tuple(result.get("log_lines", ())),
        },
        warnings=warnings,
        errors=errors,
    )


def run_segment_hcr_cellpose_stage(
    config: SingleFishPipelineConfig,
    *,
    hcr_source: str = "rbest",
    cp_hcr_model_path: str | Path | None = None,
    use_gpu: bool = True,
    force_recompute: bool = False,
) -> StageManifest:
    paths = resolve_pipeline_paths(config)
    source_key = str(hcr_source).strip().lower()
    source_dir = paths.preproc_dir / source_key
    model_path = Path(cp_hcr_model_path) if cp_hcr_model_path not in (None, "", False) else None
    if not source_dir.exists():
        return StageManifest(
            manifest_version=PIPELINE_MANIFEST_VERSION,
            stage_name="segment-hcr-cellpose",
            fish_id=config.fish_id,
            status="fail",
            dry_run=False,
            generated_at=datetime.now(timezone.utc).isoformat(),
            inputs=(
                describe_manifest_path(source_dir, label=f"HCR {source_key} intensity directory"),
                _optional_manifest_path(model_path, label="HCR Cellpose model"),
            ),
            outputs=(describe_manifest_path(paths.confocal_raw_cp_masks_dir, required=False, label="HCR Cellpose mask directory"),),
            checks=(),
            parameters={
                "local_root": str(config.local_root),
                "hcr_source": source_key,
                "force_recompute": bool(force_recompute),
                "use_gpu": bool(use_gpu),
                "log_lines": (),
            },
            warnings=(),
            errors=(f"missing HCR {source_key} intensity directory: {source_dir}",),
        )
    from .segmentation import HcrCellposeConfig, run_hcr_cellpose_stage

    try:
        result = run_hcr_cellpose_stage(
            fish_dir=paths.fish_dir,
            preproc_dir=paths.preproc_dir,
            cp_hcr_model_path=model_path,
            data_mode=config.data_mode,
            data_root=paths.data_root,
            local_root=paths.data_root,
            source=source_key,
            config=HcrCellposeConfig(
                skip_if_exists=not force_recompute,
                use_gpu=use_gpu,
                data_mode=config.data_mode,
            ),
        )
        resolved_model_path = Path(result["bindings"]["CP_MODEL_PATH"])
        candidate_pairs = tuple(result.get("candidate_pairs", ()))
        mask_paths = tuple(mask_path for _input_path, mask_path in candidate_pairs)
        checks = (
            StageCheckRecord(
                label="HCR Cellpose candidate stacks",
                status="pass" if candidate_pairs else "fail",
                detail=f"discovered HCR intensity stacks from {source_key}",
                observed=str(len(candidate_pairs)),
            ),
            StageCheckRecord(
                label="HCR Cellpose mask outputs",
                status="pass" if mask_paths and all(path.exists() for path in mask_paths) else "fail",
                detail="expected mask TIFFs exist",
                observed=str(sum(1 for path in mask_paths if path.exists())),
                expected=str(len(mask_paths)),
            ),
        )
        status = "pass" if all(check.status != "fail" for check in checks) else "fail"
        errors: tuple[str, ...] = ()
        warnings: tuple[str, ...] = ()
        outputs = tuple(describe_manifest_path(path, label=f"HCR Cellpose mask: {path.name}") for path in mask_paths)
        if not outputs:
            outputs = (describe_manifest_path(paths.confocal_raw_cp_masks_dir, required=False, label="HCR Cellpose mask directory"),)
    except Exception as exc:
        result = {"candidate_pairs": (), "log_lines": []}
        resolved_model_path = model_path
        status = "fail"
        errors = (str(exc),)
        warnings = ()
        checks = ()
        outputs = (describe_manifest_path(paths.confocal_raw_cp_masks_dir, required=False, label="HCR Cellpose mask directory"),)
    return StageManifest(
        manifest_version=PIPELINE_MANIFEST_VERSION,
        stage_name="segment-hcr-cellpose",
        fish_id=config.fish_id,
        status=status,
        dry_run=False,
        generated_at=datetime.now(timezone.utc).isoformat(),
        inputs=(
            describe_manifest_path(paths.preproc_dir / source_key, label=f"HCR {source_key} intensity directory"),
            _optional_manifest_path(resolved_model_path, label="HCR Cellpose model"),
        ),
        outputs=outputs,
        checks=checks,
        parameters={
            "local_root": str(config.local_root),
            "hcr_source": source_key,
            "force_recompute": bool(force_recompute),
            "use_gpu": bool(use_gpu),
            "log_lines": tuple(result.get("log_lines", ())),
        },
        warnings=warnings,
        errors=errors,
    )


__all__ = [
    "PIPELINE_MANIFEST_VERSION",
    "PIPELINE_STAGE_ORDER",
    "POST_PREPROCESSING_STAGE_NAMES",
    "GRANULAR_PREPROCESSING_STAGE_NAMES",
    "ManifestPathRecord",
    "PipelinePaths",
    "SingleFishPipelineConfig",
    "StageContract",
    "StageCheckRecord",
    "StageManifest",
    "StageOutputSpec",
    "PersistedManifestStatus",
    "build_single_fish_compare_staged_manifest",
    "build_single_fish_compare_legacy_baseline_manifest",
    "build_single_fish_downstream_stage_manifest",
    "build_single_fish_downstream_stage_manifests",
    "build_single_fish_hcr_activity_replay_manifest",
    "build_single_fish_score_activity_bpi_recompute_manifest",
    "build_single_fish_stage_status",
    "build_single_fish_status",
    "compare_persisted_manifest",
    "compare_single_fish_legacy_baseline",
    "compare_single_fish_staged_outputs",
    "cellpose_stage_manifest_path",
    "describe_glob",
    "describe_manifest_path",
    "discover_functional_reference_pairs",
    "discover_functional_motion_corrected_stacks",
    "discover_ex_vivo_anatomy_stack",
    "downstream_stage_names",
    "ex_vivo_structural_root",
    "functional_to_anatomy_registration_root",
    "functional_reference_output_dir",
    "hcr_to_anatomy_registration_root",
    "load_plane_refs_summary",
    "legacy_baseline_root",
    "pipeline_contracts",
    "prepared_in_vivo_anatomy_path",
    "prepared_ex_vivo_anatomy_path",
    "roi_to_anatomy_match_root",
    "run_prepare_functional_reference_stacks_stage",
    "run_prepare_in_vivo_anatomy_stack_stage",
    "run_prepare_ex_vivo_anatomy_stack_stage",
    "run_register_functional_to_anatomy_stage",
    "run_register_hcr_to_anatomy_stage",
    "run_match_roi_to_anatomy_stage",
    "run_segment_ex_vivo_anatomy_cellpose_stage",
    "run_segment_hcr_cellpose_stage",
    "run_single_fish_assign_hcr_identity_stage",
    "run_single_fish_export_canonical_tables_stage",
    "run_single_fish_freeze_legacy_baseline_stage",
    "run_single_fish_make_figures_stage",
    "run_single_fish_make_qa_report_stage",
    "resolve_pipeline_paths",
    "run_single_fish_audit_inputs_stage",
    "run_single_fish_score_activity_bpi_stage",
    "stage_manifest_to_json",
    "stage_manifest_path",
    "write_cellpose_stage_manifest",
    "write_stage_manifest",
]
