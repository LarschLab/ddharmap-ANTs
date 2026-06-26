import json
import subprocess
import sys
from pathlib import Path

from codeants_2pf_hcr.pipeline import (
    GRANULAR_PREPROCESSING_STAGE_NAMES,
    PIPELINE_STAGE_ORDER,
    REQUIRED_CSV_COLUMNS,
    SingleFishPipelineConfig,
    build_single_fish_compare_staged_manifest,
    build_single_fish_downstream_stage_manifest,
    build_single_fish_downstream_stage_manifests,
    build_single_fish_stage_status,
    build_single_fish_status,
    cellpose_stage_manifest_path,
    compare_single_fish_staged_outputs,
    discover_ex_vivo_anatomy_stack,
    downstream_stage_names,
    ex_vivo_structural_root,
    prepared_ex_vivo_anatomy_path,
    pipeline_contracts,
    resolve_pipeline_paths,
    run_single_fish_audit_inputs_stage,
    stage_manifest_path,
    write_stage_manifest,
)


REPO_ROOT = Path(__file__).resolve().parents[1]


def _make_minimal_fish(root: Path, fish_id: str = "L000_f00") -> Path:
    fish_dir = root / fish_id
    metadata_dir = fish_dir / "01_raw" / "2p" / "metadata"
    functional_raw_dir = fish_dir / "01_raw" / "2p" / "functional"
    anatomy_raw_dir = fish_dir / "01_raw" / "2p" / "anatomy"
    anatomy_preproc_dir = fish_dir / "02_reg" / "00_preprocessing" / "2p_anatomy"
    rbest_dir = fish_dir / "02_reg" / "00_preprocessing" / "rbest"
    suite2p_plane_dir = fish_dir / "03_analysis" / "functional" / "suite2P" / "plane0"
    registration_dir = fish_dir / "03_analysis" / "functional" / "registration"
    ncc_compare_dir = fish_dir / "03_analysis" / "functional" / "ncc" / "inplane_registration_comparison"
    cp_masks_dir = fish_dir / "03_analysis" / "confocal" / "raw" / "cp_masks"
    confocal_aligned_dir = fish_dir / "03_analysis" / "confocal" / "aligned"
    plots_dir = fish_dir / "04_plots"
    for directory in (
        metadata_dir,
        functional_raw_dir,
        anatomy_raw_dir,
        anatomy_preproc_dir,
        rbest_dir,
        suite2p_plane_dir,
        registration_dir / "reference_planes",
        ncc_compare_dir,
        cp_masks_dir,
        confocal_aligned_dir,
        plots_dir,
    ):
        directory.mkdir(parents=True)
    (metadata_dir / f"{fish_id}_metadata.csv").write_text("key,value\n")
    (metadata_dir / f"{fish_id}_experiment_log.csv").write_text("time,event\n")
    (metadata_dir / ".DS_Store").write_text("ignored\n")
    (functional_raw_dir / ".DS_Store").write_text("ignored\n")
    (anatomy_preproc_dir / f"{fish_id}_anatomy_2P_GCaMP.nrrd").write_text("NRRD0005\n")
    (rbest_dir / f"{fish_id}_round1_channel2_gene.nrrd").write_text("NRRD0005\n")
    for name in ("F.npy", "Fneu.npy", "iscell.npy", "ops.npy", "stat.npy"):
        (suite2p_plane_dir / f"{fish_id}_plane0_{name}").write_bytes(b"npy")
    required_registration_csvs = (
        "tforms_by_plane.csv",
        "plane_links.csv",
        "functional_roi_activity_identity.csv",
        "functional_roi_activity_identity_summary.csv",
        "functional_roi_activity_identity_by_plane.csv",
        "functional_roi_activity_bpi_cells.csv",
        "functional_roi_activity_bpi_summary.csv",
        "hcr_activity_status.csv",
        "hcr_activity_status_summary.csv",
        "conf_to_func_pairs_raw.csv",
        "conf_to_func_pairs.csv",
        "hcr_func_candidates.csv",
    )
    for name in required_registration_csvs:
        header = REQUIRED_CSV_COLUMNS.get(name, ("col",))
        row_values = {
            "plane_index": "0",
            "plane_idx": "0",
            "plane": "0",
            "func_label": "1",
            "has_unique_anat_match": "True",
            "has_identity_assigned": "True",
            "suite2p_is_cell": "True",
            "response_is_active": "True",
            "selected_for_trace_export": "True",
            "is_selected_for_analysis": "True",
            "response_class": "bout-responsive",
            "response_summary_class": "Responsive neurons",
            "bpi_category": "bout-responsive",
            "bpi": "1.0",
        }
        row = tuple(row_values.get(column, "1") for column in header)
        (registration_dir / name).write_text(",".join(header) + "\n" + ",".join(row) + "\n")
    (registration_dir / "reference_planes" / f"{fish_id}_plane0_ref.tif").write_bytes(b"tif")
    (fish_dir / "03_analysis" / "functional" / "ncc" / "ncc_bestz_by_plane.json").write_text("{}\n")
    (ncc_compare_dir / "inplane_registration_comparison.csv").write_text("col\n")
    (ncc_compare_dir / "inplane_registration_recommendation.csv").write_text("col\n")
    (cp_masks_dir / f"{fish_id}_round1_channel2_gene_cp_masks.tif").write_bytes(b"tif")
    (confocal_aligned_dir / "aligned.csv").write_text("col\n")
    for name in (
        "compound_50j_56i_unified.png",
        "bpi_all_pairs.png",
        "per_gene_stimulus_trace_with_hcr_status_56h.png",
        "single_fish_50l_responsive_identity_donut.png",
        "single_fish_hcr_anatomy_coexpression_summary.png",
    ):
        (plots_dir / name).write_bytes(b"png")
    return fish_dir


def _copy_file(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    dst.write_bytes(src.read_bytes())


def _make_minimal_staged_outputs(fish_dir: Path) -> None:
    registration_dir = fish_dir / "03_analysis" / "functional" / "registration"
    plots_dir = fish_dir / "04_plots"
    pipeline_outputs = fish_dir / "03_analysis" / "functional" / "pipeline_outputs"
    assign_dir = pipeline_outputs / "assign-hcr-identity" / "registration"
    for filename in (
        "functional_roi_activity_identity.csv",
        "hcr_activity_status.csv",
        "hcr_activity_status_summary.csv",
        "conf_to_func_pairs_raw.csv",
        "conf_to_func_pairs.csv",
        "hcr_func_candidates.csv",
    ):
        _copy_file(registration_dir / filename, assign_dir / filename)
    (assign_dir / "anatomy_identity_lookup.csv").write_text("gene,anat_label\nmarker,1\n")

    score_dir = pipeline_outputs / "score-activity-bpi" / "registration"
    for filename in (
        "functional_roi_activity_identity.csv",
        "functional_roi_activity_bpi_cells.csv",
        "functional_roi_activity_bpi_summary.csv",
    ):
        _copy_file(registration_dir / filename, score_dir / filename)

    canonical_dir = pipeline_outputs / "export-canonical-tables" / "registration"
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
        _copy_file(registration_dir / filename, canonical_dir / filename)

    figure_dir = pipeline_outputs / "make-figures" / "04_plots"
    for filename in (
        "compound_50j_56i_unified.png",
        "bpi_all_pairs.png",
        "per_gene_stimulus_trace_with_hcr_status_56h.png",
        "single_fish_50l_responsive_identity_donut.png",
        "single_fish_hcr_anatomy_coexpression_summary.png",
    ):
        _copy_file(plots_dir / filename, figure_dir / filename)


def test_pipeline_contracts_follow_declared_stage_order() -> None:
    contracts = pipeline_contracts()
    assert tuple(contract.name for contract in contracts) == PIPELINE_STAGE_ORDER
    assert contracts[0].name == "audit-inputs"
    assert contracts[0].depends_on == ()
    assert contracts[-1].name == "make-figures"
    assert "prepare-ex-vivo-anatomy-stack" in GRANULAR_PREPROCESSING_STAGE_NAMES
    assert "segment-hcr-cellpose" in GRANULAR_PREPROCESSING_STAGE_NAMES
    assert "segment-ex-vivo-anatomy-cellpose" in GRANULAR_PREPROCESSING_STAGE_NAMES


def test_ex_vivo_stage_paths_are_structural_ex_vivo_scoped(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    paths = resolve_pipeline_paths(SingleFishPipelineConfig(fish_id=fish_dir.name, local_root=tmp_path))
    assert ex_vivo_structural_root(paths) == fish_dir / "03_analysis" / "structural" / "ex_vivo"
    assert prepared_ex_vivo_anatomy_path(paths) == (
        fish_dir / "03_analysis" / "structural" / "ex_vivo" / "prepared" / f"{fish_dir.name}_exvivo_anatomy_2P_GCaMP_uint8.nrrd"
    )
    assert cellpose_stage_manifest_path(paths, "segment-hcr-cellpose") == (
        fish_dir / "03_analysis" / "confocal" / "raw" / "manifests" / "segment-hcr-cellpose_manifest.json"
    )
    assert cellpose_stage_manifest_path(paths, "segment-ex-vivo-anatomy-cellpose") == (
        fish_dir / "03_analysis" / "structural" / "ex_vivo" / "manifests" / "segment-ex-vivo-anatomy-cellpose_manifest.json"
    )


def test_discover_ex_vivo_anatomy_stack_requires_explicit_choice_when_ambiguous(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    anatomy_dir = fish_dir / "01_raw" / "2p" / "anatomy"
    ex_vivo = anatomy_dir / f"{fish_dir.name}_anatomy_ex_vivo_00001.tif"
    ex_vivo.write_bytes(b"tif")
    paths = resolve_pipeline_paths(SingleFishPipelineConfig(fish_id=fish_dir.name, local_root=tmp_path))
    assert discover_ex_vivo_anatomy_stack(paths) == ex_vivo
    (anatomy_dir / f"{fish_dir.name}_second_ex_vivo_00001.tif").write_bytes(b"tif")
    try:
        discover_ex_vivo_anatomy_stack(paths)
    except RuntimeError as exc:
        assert "--ex-vivo-stack-path" in str(exc)
    else:
        raise AssertionError("ambiguous ex vivo stacks should require an explicit path")


def test_audit_inputs_is_read_only_and_passes_on_minimal_fish(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    pipeline_root = tmp_path / "dry-run-output-root"
    before = sorted(path.relative_to(tmp_path) for path in tmp_path.rglob("*"))
    manifest = run_single_fish_audit_inputs_stage(
        SingleFishPipelineConfig(
            fish_id=fish_dir.name,
            local_root=tmp_path,
            strict=True,
            pipeline_root=pipeline_root,
        )
    )
    after = sorted(path.relative_to(tmp_path) for path in tmp_path.rglob("*"))
    assert before == after
    assert manifest.stage_name == "audit-inputs"
    assert manifest.status == "pass"
    assert manifest.dry_run is True
    assert manifest.errors == ()
    assert any(check.label == "Suite2p plane directories" and check.status == "pass" for check in manifest.checks)
    assert any(check.label == "tforms rows vs Suite2p planes" and check.observed == "1" for check in manifest.checks)
    assert any(
        check.label == "CSV schema: functional_roi_activity_identity.csv" and check.status == "pass"
        for check in manifest.checks
    )
    assert any(check.label == "BPI cell keys match ROI master keys" and check.status == "pass" for check in manifest.checks)
    assert any(
        check.label == "responsive pair keys subset ROI master keys" and check.status == "pass"
        for check in manifest.checks
    )
    assert any(
        check.label == "HCR trace export selections match responsive pairs" and check.status == "pass"
        for check in manifest.checks
    )
    assert any(check.label == "responsive pairs are active selected rows" and check.status == "pass" for check in manifest.checks)
    assert manifest.parameters is not None
    assert manifest.parameters["pipeline_root"] == str(pipeline_root)
    assert not pipeline_root.exists()
    metadata_record = next(record for record in manifest.inputs if record.label == "raw 2p metadata CSVs")
    assert metadata_record.count == 1
    assert metadata_record.mtime is not None
    raw_functional_record = next(record for record in manifest.inputs if record.label == "raw 2p functional files")
    assert raw_functional_record.count == 0


def test_audit_inputs_strict_fails_missing_required_inputs(tmp_path: Path) -> None:
    manifest = run_single_fish_audit_inputs_stage(
        SingleFishPipelineConfig(fish_id="missing_fish", local_root=tmp_path, strict=True)
    )
    assert manifest.status == "fail"
    assert any("missing_fish" in path for path in manifest.errors)


def test_status_summarizes_dry_run_audit(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    status = build_single_fish_status(
        SingleFishPipelineConfig(fish_id=fish_dir.name, local_root=tmp_path, strict=True)
    )
    assert status["status"] == "pass"
    assert status["dry_run"] is True
    assert status["input_records"] > 0
    assert status["check_records"] > 0
    assert status["missing_required_inputs"] == []
    assert status["failed_checks"] == []
    assert status["persisted_manifests"][0]["status"] == "missing"
    assert status["stale_records"] == []


def test_manifest_write_is_explicit_and_status_reports_current(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    config = SingleFishPipelineConfig(
        fish_id=fish_dir.name,
        local_root=tmp_path,
        strict=True,
        dry_run=True,
        write_manifest=True,
    )
    manifest = run_single_fish_audit_inputs_stage(config)
    manifest_path = write_stage_manifest(manifest, resolve_pipeline_paths(config))
    assert manifest_path == stage_manifest_path(resolve_pipeline_paths(config), "audit-inputs")
    assert manifest_path.exists()
    assert manifest.dry_run is True
    assert manifest.parameters is not None
    assert manifest.parameters["write_manifest"] is True
    status = build_single_fish_status(
        SingleFishPipelineConfig(fish_id=fish_dir.name, local_root=tmp_path, strict=True)
    )
    assert status["status"] == "pass"
    assert status["persisted_manifests"][0]["status"] == "current"
    assert status["stale_records"] == []


def test_status_reports_stale_manifest_when_inputs_change(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    write_config = SingleFishPipelineConfig(
        fish_id=fish_dir.name,
        local_root=tmp_path,
        strict=True,
        dry_run=True,
        write_manifest=True,
    )
    write_stage_manifest(run_single_fish_audit_inputs_stage(write_config), resolve_pipeline_paths(write_config))
    metadata_path = fish_dir / "01_raw" / "2p" / "metadata" / f"{fish_dir.name}_metadata.csv"
    metadata_path.write_text("key,value\nnew,row\n")
    status = build_single_fish_status(
        SingleFishPipelineConfig(fish_id=fish_dir.name, local_root=tmp_path, strict=True)
    )
    assert status["status"] == "warn"
    assert status["persisted_manifests"][0]["status"] == "stale"
    assert "inputs.raw 2p metadata CSVs" in status["stale_records"]


def test_status_fails_on_invalid_persisted_manifest_json(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    config = SingleFishPipelineConfig(fish_id=fish_dir.name, local_root=tmp_path, strict=True)
    manifest_path = stage_manifest_path(resolve_pipeline_paths(config), "audit-inputs")
    manifest_path.parent.mkdir(parents=True)
    manifest_path.write_text("{not json\n")
    status = build_single_fish_status(config)
    assert status["status"] == "fail"
    assert status["persisted_manifests"][0]["status"] == "fail"
    assert "invalid JSON" in status["stale_records"][0]


def test_audit_inputs_flags_staged_canonical_csv_parity_mismatch_when_baseline_present(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    staged_dir = (
        fish_dir
        / "03_analysis"
        / "functional"
        / "pipeline_outputs"
        / "export-canonical-tables"
        / "registration"
    )
    staged_dir.mkdir(parents=True)
    (staged_dir / "functional_roi_activity_identity.csv").write_text("unexpected\n1\n")
    manifest = run_single_fish_audit_inputs_stage(
        SingleFishPipelineConfig(fish_id=fish_dir.name, local_root=tmp_path, strict=True)
    )
    check = next(
        item
        for item in manifest.checks
        if item.label == "staged canonical CSV parity: functional_roi_activity_identity.csv"
    )
    assert check.status == "fail"
    assert manifest.status == "fail"


def test_downstream_stage_manifests_are_read_only_and_pass_when_outputs_exist(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    _make_minimal_staged_outputs(fish_dir)
    before = sorted(path.relative_to(tmp_path) for path in tmp_path.rglob("*"))
    manifests = build_single_fish_downstream_stage_manifests(
        SingleFishPipelineConfig(fish_id=fish_dir.name, local_root=tmp_path, strict=True)
    )
    after = sorted(path.relative_to(tmp_path) for path in tmp_path.rglob("*"))
    assert before == after
    assert tuple(manifest.stage_name for manifest in manifests) == downstream_stage_names()
    assert all(manifest.status == "pass" for manifest in manifests)
    assert all(manifest.outputs for manifest in manifests)
    assert all(any(check.label.startswith("required output exists") for check in manifest.checks) for manifest in manifests)


def test_downstream_stage_manifest_fails_missing_required_stage_outputs(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    _make_minimal_staged_outputs(fish_dir)
    missing_path = (
        fish_dir
        / "03_analysis"
        / "functional"
        / "pipeline_outputs"
        / "export-canonical-tables"
        / "registration"
        / "hcr_func_candidates.csv"
    )
    missing_path.unlink()
    manifest = build_single_fish_downstream_stage_manifest(
        SingleFishPipelineConfig(fish_id=fish_dir.name, local_root=tmp_path, strict=True),
        "export-canonical-tables",
    )
    assert manifest.status == "fail"
    assert any(record.label == "staged canonical export: hcr_func_candidates.csv" for record in manifest.outputs)
    assert any("hcr_func_candidates.csv" in error for error in manifest.errors)


def test_status_includes_downstream_stage_summaries(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    _make_minimal_staged_outputs(fish_dir)
    status = build_single_fish_status(
        SingleFishPipelineConfig(fish_id=fish_dir.name, local_root=tmp_path, strict=True)
    )
    assert status["status"] == "pass"
    stage_summaries = {stage["stage_name"]: stage for stage in status["downstream_stages"]}
    assert set(stage_summaries) == set(downstream_stage_names())
    assert stage_summaries["export-canonical-tables"]["status"] == "pass"
    assert stage_summaries["export-canonical-tables"]["output_records"] == 8


def test_status_fails_when_existing_downstream_stage_is_incomplete(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    incomplete_stage_dir = (
        fish_dir
        / "03_analysis"
        / "functional"
        / "pipeline_outputs"
        / "export-canonical-tables"
        / "registration"
    )
    incomplete_stage_dir.mkdir(parents=True)
    (incomplete_stage_dir / "functional_roi_activity_identity.csv").write_text("unexpected\n1\n")
    status = build_single_fish_status(
        SingleFishPipelineConfig(fish_id=fish_dir.name, local_root=tmp_path, strict=True)
    )
    assert status["status"] == "fail"
    stage_summary = next(stage for stage in status["downstream_stages"] if stage["stage_name"] == "export-canonical-tables")
    assert stage_summary["status"] == "fail"
    assert "staged canonical export: hcr_func_candidates.csv" in stage_summary["missing_required_outputs"]


def test_stage_status_reports_not_started_without_affecting_global_status(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    status = build_single_fish_status(
        SingleFishPipelineConfig(fish_id=fish_dir.name, local_root=tmp_path, strict=True)
    )
    assert status["status"] == "pass"
    assert {stage["status"] for stage in status["downstream_stages"]} == {"not_started"}


def test_single_fish_stage_status_reports_current_after_manifest_write(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    _make_minimal_staged_outputs(fish_dir)
    config = SingleFishPipelineConfig(fish_id=fish_dir.name, local_root=tmp_path, strict=True)
    manifest = build_single_fish_downstream_stage_manifest(config, "make-figures")
    write_stage_manifest(manifest, resolve_pipeline_paths(config))
    stage_status = build_single_fish_stage_status(config, "make-figures")
    assert stage_status["status"] == "pass"
    assert stage_status["persisted_manifest"]["status"] == "current"


def test_compare_staged_is_read_only_and_passes_on_minimal_staged_outputs(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    _make_minimal_staged_outputs(fish_dir)
    before = sorted(path.relative_to(tmp_path) for path in tmp_path.rglob("*"))
    payload = compare_single_fish_staged_outputs(
        SingleFishPipelineConfig(fish_id=fish_dir.name, local_root=tmp_path, strict=True)
    )
    after = sorted(path.relative_to(tmp_path) for path in tmp_path.rglob("*"))
    assert before == after
    assert payload["status"] == "pass"
    assert [item["stage_name"] for item in payload["comparisons"]] == list(downstream_stage_names())
    assert all(item["failed_checks"] == [] for item in payload["comparisons"])


def test_compare_staged_reports_csv_shape_mismatch(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    _make_minimal_staged_outputs(fish_dir)
    staged_path = (
        fish_dir
        / "03_analysis"
        / "functional"
        / "pipeline_outputs"
        / "export-canonical-tables"
        / "registration"
        / "functional_roi_activity_identity.csv"
    )
    staged_path.write_text("unexpected\n1\n")
    payload = compare_single_fish_staged_outputs(
        SingleFishPipelineConfig(fish_id=fish_dir.name, local_root=tmp_path, strict=True),
        "export-canonical-tables",
    )
    assert payload["status"] == "fail"
    comparison = payload["comparisons"][0]
    assert comparison["stage_name"] == "export-canonical-tables"
    assert any("comparison CSV shape" in label for label in comparison["failed_checks"])


def test_compare_staged_specific_stage_can_fail_strict(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    _make_minimal_staged_outputs(fish_dir)
    staged_path = (
        fish_dir
        / "03_analysis"
        / "functional"
        / "pipeline_outputs"
        / "make-figures"
        / "04_plots"
        / "bpi_all_pairs.png"
    )
    staged_path.write_bytes(b"")
    manifest = build_single_fish_compare_staged_manifest(
        SingleFishPipelineConfig(fish_id=fish_dir.name, local_root=tmp_path, strict=True),
        "make-figures",
    )
    assert manifest.status == "fail"
    assert any("bpi_all_pairs.png" in error for error in manifest.errors)


def test_single_fish_pipeline_cli_contracts_outputs_json() -> None:
    result = subprocess.run(
        [sys.executable, "tools/single_fish_pipeline.py", "contracts"],
        cwd=REPO_ROOT,
        env={"PYTHONPATH": "src"},
        text=True,
        capture_output=True,
        check=True,
    )
    payload = json.loads(result.stdout)
    assert [stage["name"] for stage in payload] == list(PIPELINE_STAGE_ORDER)


def test_single_fish_pipeline_cli_help_exposes_current_read_only_and_granular_writer_commands() -> None:
    result = subprocess.run(
        [sys.executable, "tools/single_fish_pipeline.py", "--help"],
        cwd=REPO_ROOT,
        env={"PYTHONPATH": "src"},
        text=True,
        capture_output=True,
        check=True,
    )
    assert "contracts" in result.stdout
    assert "compare-staged" in result.stdout
    assert "prepare-ex-vivo-anatomy-stack" in result.stdout
    assert "segment-ex-vivo-anatomy-cellpose" in result.stdout
    assert "segment-hcr-cellpose" in result.stdout
    for command in (
        "preprocess-functional",
        "assign-hcr-identity",
        "score-activity-bpi",
        "export-canonical-tables",
        "make-figures",
    ):
        assert command not in result.stdout


def test_target_scaffold_commands_are_not_runnable_yet() -> None:
    for command in (
        "assign-hcr-identity",
        "score-activity-bpi",
        "export-canonical-tables",
        "make-figures",
    ):
        result = subprocess.run(
            [sys.executable, "tools/single_fish_pipeline.py", command],
            cwd=REPO_ROOT,
            env={"PYTHONPATH": "src"},
            text=True,
            capture_output=True,
            check=False,
        )
        assert result.returncode == 2
        assert "invalid choice" in result.stderr


def test_single_fish_pipeline_cli_audit_inputs_dry_run(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    result = subprocess.run(
        [
            sys.executable,
            "tools/single_fish_pipeline.py",
            "audit-inputs",
            "--fish-id",
            fish_dir.name,
            "--local-root",
            str(tmp_path),
            "--strict",
            "--pipeline-root",
            str(tmp_path / "reported-output-root"),
        ],
        cwd=REPO_ROOT,
        env={"PYTHONPATH": "src"},
        text=True,
        capture_output=True,
        check=True,
    )
    payload = json.loads(result.stdout)
    assert payload["stage_name"] == "audit-inputs"
    assert payload["status"] == "pass"
    assert payload["dry_run"] is True
    assert payload["parameters"]["pipeline_root"] == str(tmp_path / "reported-output-root")
    assert not (fish_dir / "03_analysis" / "functional" / "pipeline_manifests").exists()


def test_single_fish_pipeline_cli_audit_inputs_writes_manifest_with_opt_in(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    result = subprocess.run(
        [
            sys.executable,
            "tools/single_fish_pipeline.py",
            "audit-inputs",
            "--fish-id",
            fish_dir.name,
            "--local-root",
            str(tmp_path),
            "--strict",
            "--write-manifest",
        ],
        cwd=REPO_ROOT,
        env={"PYTHONPATH": "src"},
        text=True,
        capture_output=True,
        check=True,
    )
    payload = json.loads(result.stdout)
    assert payload["stage_name"] == "audit-inputs"
    assert payload["dry_run"] is True
    assert payload["parameters"]["write_manifest"] is True
    manifest_path = fish_dir / "03_analysis" / "functional" / "pipeline_manifests" / "audit-inputs_manifest.json"
    assert manifest_path.exists()
    assert json.loads(manifest_path.read_text())["stage_name"] == "audit-inputs"


def test_single_fish_pipeline_cli_status_outputs_summary(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    result = subprocess.run(
        [
            sys.executable,
            "tools/single_fish_pipeline.py",
            "status",
            "--fish-id",
            fish_dir.name,
            "--local-root",
            str(tmp_path),
            "--strict",
        ],
        cwd=REPO_ROOT,
        env={"PYTHONPATH": "src"},
        text=True,
        capture_output=True,
        check=True,
    )
    payload = json.loads(result.stdout)
    assert payload["status"] == "pass"
    assert payload["dry_run"] is True
    assert payload["failed_checks"] == []


def test_single_fish_pipeline_cli_stage_status_outputs_manifest(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    _make_minimal_staged_outputs(fish_dir)
    result = subprocess.run(
        [
            sys.executable,
            "tools/single_fish_pipeline.py",
            "stage-status",
            "--fish-id",
            fish_dir.name,
            "--local-root",
            str(tmp_path),
            "--strict",
            "--stage-name",
            "make-figures",
        ],
        cwd=REPO_ROOT,
        env={"PYTHONPATH": "src"},
        text=True,
        capture_output=True,
        check=True,
    )
    payload = json.loads(result.stdout)
    assert payload["stage_name"] == "make-figures"
    assert payload["status"] == "pass"
    assert len(payload["outputs"]) == 5


def test_single_fish_pipeline_cli_compare_staged_outputs_json(tmp_path: Path) -> None:
    fish_dir = _make_minimal_fish(tmp_path)
    _make_minimal_staged_outputs(fish_dir)
    result = subprocess.run(
        [
            sys.executable,
            "tools/single_fish_pipeline.py",
            "compare-staged",
            "--fish-id",
            fish_dir.name,
            "--local-root",
            str(tmp_path),
            "--strict",
            "--stage-name",
            "export-canonical-tables",
        ],
        cwd=REPO_ROOT,
        env={"PYTHONPATH": "src"},
        text=True,
        capture_output=True,
        check=True,
    )
    payload = json.loads(result.stdout)
    assert payload["status"] == "pass"
    assert payload["stage_name"] == "export-canonical-tables"
    assert len(payload["comparisons"]) == 1
    assert payload["comparisons"][0]["stage_name"] == "export-canonical-tables"
