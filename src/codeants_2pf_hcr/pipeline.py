"""Read-only staged single-fish pipeline contracts and input audits."""

from __future__ import annotations

import csv
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
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
    "make-figures",
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

STAGED_FIGURE_FILES: tuple[str, ...] = (
    "compound_50j_56i_unified.png",
    "bpi_all_pairs.png",
    "per_gene_stimulus_trace_with_hcr_status_56h.png",
    "single_fish_50l_responsive_identity_donut.png",
    "single_fish_hcr_anatomy_coexpression_summary.png",
)


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


def pipeline_contracts() -> tuple[StageContract, ...]:
    purposes = {
        "audit-inputs": "Inspect required fish-scoped inputs without writing pipeline outputs.",
        "preprocess-functional": "Prepare or inventory functional preprocessing products.",
        "preprocess-anatomy": "Prepare registration-ready 2P anatomy products.",
        "preprocess-hcr": "Prepare or inventory HCR intensity and mask products.",
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


def _stage_output_specs(paths: PipelinePaths, stage_name: str) -> tuple[StageOutputSpec, ...]:
    stage_root = paths.functional_pipeline_outputs_dir / stage_name
    if stage_name == "assign-hcr-identity":
        registration_dir = stage_root / "registration"
        return (
            StageOutputSpec(
                "staged ROI identity master",
                str(registration_dir / "functional_roi_activity_identity.csv"),
                control_path=str(paths.functional_registration_dir / "functional_roi_activity_identity.csv"),
                parity="csv_shape",
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
            ),
            StageOutputSpec(
                "staged HCR activity status summary",
                str(registration_dir / "hcr_activity_status_summary.csv"),
                control_path=str(paths.functional_registration_dir / "hcr_activity_status_summary.csv"),
                parity="csv_shape",
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
            ),
            StageOutputSpec(
                "staged HCR/function candidates",
                str(registration_dir / "hcr_func_candidates.csv"),
                control_path=str(paths.functional_registration_dir / "hcr_func_candidates.csv"),
                parity="csv_shape",
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
            ),
            StageOutputSpec(
                "staged ROI activity/BPI cells",
                str(registration_dir / "functional_roi_activity_bpi_cells.csv"),
                control_path=str(paths.functional_registration_dir / "functional_roi_activity_bpi_cells.csv"),
                parity="csv_shape",
            ),
            StageOutputSpec(
                "staged ROI activity/BPI summary",
                str(registration_dir / "functional_roi_activity_bpi_summary.csv"),
                control_path=str(paths.functional_registration_dir / "functional_roi_activity_bpi_summary.csv"),
                parity="csv_shape",
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
            )
            for filename in STAGED_REGISTRATION_CSVS
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
                    paths.functional_pipeline_outputs_dir / "assign-hcr-identity" / "registration" / "functional_roi_activity_identity.csv",
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
                    paths.functional_pipeline_outputs_dir / "score-activity-bpi" / "registration",
                    "*.csv",
                    required=False,
                    label="staged score-activity-bpi CSV inputs",
                ),
                describe_glob(
                    paths.functional_pipeline_outputs_dir / "assign-hcr-identity" / "registration",
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
                    paths.functional_pipeline_outputs_dir / "export-canonical-tables" / "registration",
                    "*.csv",
                    label="staged canonical export CSV inputs",
                ),
                describe_glob(paths.plots_dir, "*.png", label="control plot PNG inputs"),
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
            "csv_contract": "header_and_row_count_required; byte_parity_warn_only",
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
    stage_root = paths.functional_pipeline_outputs_dir / stage_name
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
    staged_canonical_dir = paths.functional_pipeline_outputs_dir / "export-canonical-tables" / "registration"
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
    staged_figures_dir = paths.functional_pipeline_outputs_dir / "make-figures" / "04_plots"
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
        describe_glob(paths.functional_pipeline_outputs_dir / "assign-hcr-identity" / "registration", "*.csv", required=False, label="staged assign-hcr-identity CSVs"),
        describe_glob(paths.functional_pipeline_outputs_dir / "score-activity-bpi" / "registration", "*.csv", required=False, label="staged score-activity-bpi CSVs"),
        describe_glob(paths.functional_pipeline_outputs_dir / "export-canonical-tables" / "registration", "*.csv", required=False, label="staged canonical export CSVs"),
        describe_glob(paths.functional_pipeline_outputs_dir / "make-figures" / "04_plots", "*", required=False, label="staged figure outputs"),
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


__all__ = [
    "PIPELINE_MANIFEST_VERSION",
    "PIPELINE_STAGE_ORDER",
    "POST_PREPROCESSING_STAGE_NAMES",
    "ManifestPathRecord",
    "PipelinePaths",
    "SingleFishPipelineConfig",
    "StageContract",
    "StageCheckRecord",
    "StageManifest",
    "StageOutputSpec",
    "PersistedManifestStatus",
    "build_single_fish_compare_staged_manifest",
    "build_single_fish_downstream_stage_manifest",
    "build_single_fish_downstream_stage_manifests",
    "build_single_fish_stage_status",
    "build_single_fish_status",
    "compare_persisted_manifest",
    "compare_single_fish_staged_outputs",
    "describe_glob",
    "describe_manifest_path",
    "downstream_stage_names",
    "pipeline_contracts",
    "resolve_pipeline_paths",
    "run_single_fish_audit_inputs_stage",
    "stage_manifest_to_json",
    "stage_manifest_path",
    "write_stage_manifest",
]
