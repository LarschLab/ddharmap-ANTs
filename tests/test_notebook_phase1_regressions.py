import json
import unittest
from pathlib import Path

from codeants_2pf_hcr import cohort_cache_paths
from codeants_2pf_hcr.notebook_contract import find_required_cell_contract_violations


REPO_ROOT = Path(__file__).resolve().parents[1]
NOTEBOOK_PATH = REPO_ROOT / "notebooks" / "2PF_to_HCR.ipynb"
COHORT_NOTEBOOK_PATH = REPO_ROOT / "notebooks" / "multi_fish_56h_56g.ipynb"
GENERATOR_PATH = REPO_ROOT / "tools" / "refactor_notebook_phase1.py"
PHASE2_GENERATOR_PATH = REPO_ROOT / "tools" / "refactor_notebook_phase2.py"
PHASE4_GENERATOR_PATH = REPO_ROOT / "tools" / "refactor_notebook_phase4.py"
PHASE5_GENERATOR_PATH = REPO_ROOT / "tools" / "refactor_notebook_phase5.py"
PHASE6_GENERATOR_PATH = REPO_ROOT / "tools" / "refactor_notebook_phase6.py"
PHASE7_GENERATOR_PATH = REPO_ROOT / "tools" / "refactor_notebook_phase7.py"
PHASE8_GENERATOR_PATH = REPO_ROOT / "tools" / "refactor_notebook_phase8.py"
PHASE9_GENERATOR_PATH = REPO_ROOT / "tools" / "refactor_notebook_phase9.py"
PHASE10_GENERATOR_PATH = REPO_ROOT / "tools" / "refactor_notebook_phase10.py"
PHASE12_GENERATOR_PATH = REPO_ROOT / "tools" / "refactor_notebook_phase12.py"
PHASE13_GENERATOR_PATH = REPO_ROOT / "tools" / "refactor_notebook_phase13.py"
COHORT_PHASE1_GENERATOR_PATH = REPO_ROOT / "tools" / "refactor_notebook_cohort_phase1.py"
COHORT_PHASE2_GENERATOR_PATH = REPO_ROOT / "tools" / "refactor_notebook_cohort_phase2.py"


def _code_cell_by_tag(tag: str) -> str:
    notebook = json.loads(NOTEBOOK_PATH.read_text())
    for cell in notebook["cells"]:
        if cell.get("cell_type") != "code":
            continue
        source = "".join(cell.get("source", []))
        if source.startswith(f"# [{tag}]"):
            return source
    raise AssertionError(f"Notebook cell [{tag}] not found")


def _code_cell_index_by_tag(tag: str) -> int:
    notebook = json.loads(NOTEBOOK_PATH.read_text())
    for idx, cell in enumerate(notebook["cells"]):
        if cell.get("cell_type") != "code":
            continue
        source = "".join(cell.get("source", []))
        if source.startswith(f"# [{tag}]"):
            return idx
    raise AssertionError(f"Notebook cell [{tag}] not found")


def _code_cell_by_prefix(prefix: str) -> str:
    notebook = json.loads(NOTEBOOK_PATH.read_text())
    for cell in notebook["cells"]:
        if cell.get("cell_type") != "code":
            continue
        source = "".join(cell.get("source", []))
        if source.startswith(prefix):
            return source
    raise AssertionError(f"Notebook cell starting with {prefix!r} not found")


def _cohort_cell_by_tag(tag: str) -> str:
    notebook = json.loads(COHORT_NOTEBOOK_PATH.read_text())
    for cell in notebook["cells"]:
        if cell.get("cell_type") != "code":
            continue
        source = "".join(cell.get("source", []))
        if source.startswith(f"# [{tag}]"):
            return source
    raise AssertionError(f"Cohort notebook cell [{tag}] not found")


def _cohort_cfg_cell() -> str:
    notebook = json.loads(COHORT_NOTEBOOK_PATH.read_text())
    for cell in notebook["cells"]:
        if cell.get("cell_type") != "code":
            continue
        source = "".join(cell.get("source", []))
        if source.startswith("# [cfg] Imports + cohort configuration"):
            return source
    raise AssertionError("Cohort cfg cell not found")


class NotebookPhase1RegressionTests(unittest.TestCase):
    def test_single_fish_required_package_owner_contract_tracks_50l_and_57a_slice(self) -> None:
        violations = find_required_cell_contract_violations(NOTEBOOK_PATH)

        def _details(tag: str, kind: str) -> set[str]:
            return {violation.detail for violation in violations if violation.tag == tag and violation.kind == kind}

        self.assertNotIn(
            "codeants_2pf_hcr.plots.analysis:render_single_fish_50l_composite",
            _details("50l", "required-import"),
        )
        self.assertNotIn(
            "render_single_fish_50l_composite",
            _details("50l", "required-call"),
        )
        self.assertNotIn(
            "codeants_2pf_hcr.plots.analysis:render_single_fish_50l_responsive_identity_donut",
            _details("57a-responsive-identity-donut", "required-import"),
        )
        self.assertNotIn(
            "render_single_fish_50l_responsive_identity_donut",
            _details("57a-responsive-identity-donut", "required-call"),
        )

    def test_cell_4_binds_legacy_compatibility_surface(self) -> None:
        cell = _code_cell_by_tag("4")
        self.assertIn("resolve_notebook_context_stage", cell)
        self.assertIn("ContextStageConfig", cell)
        self.assertIn("RUN_CONFIG = dict(context_result['run_config'])", cell)
        self.assertIn("FUNC_STACK_PATH = FUNC_RAW_STACK_PATH", cell)
        self.assertNotIn("def _resolve_func_polarity(", cell)

    def test_cell_8_uses_local_snapshots_for_voxel_resolution(self) -> None:
        cell = _code_cell_by_tag("8")
        self.assertIn("resolve_voxel_context_stage", cell)
        self.assertIn("VoxelStageConfig", cell)
        self.assertNotIn("def _vox_complete(", cell)
        self.assertNotIn("def _path_from_data_root(", cell)

    def test_metadata_cell_records_raw_functional_source_path(self) -> None:
        cell = _code_cell_by_tag("40")
        self.assertIn("FUNC_RAW_STACK_PATH_LOCAL = FUNC_RAW_STACK_PATH", cell)
        self.assertIn("'FUNC_STACK_PATH': str(FUNC_RAW_STACK_PATH_LOCAL) if FUNC_RAW_STACK_PATH_LOCAL else None", cell)

    def test_generator_contains_phase1_binding_fixes(self) -> None:
        source = PHASE5_GENERATOR_PATH.read_text()
        self.assertIn("resolve_notebook_context_stage", source)
        self.assertIn("build_run_config_stage", source)
        self.assertIn("build_context_audit_stage", source)
        self.assertIn("build_final_fish_audit_stage", source)

    def test_phase6_generator_tracks_voxel_and_orientation_extraction(self) -> None:
        source = PHASE6_GENERATOR_PATH.read_text()
        self.assertIn("resolve_voxel_context_stage", source)
        self.assertIn("build_voxel_debug_stage", source)
        self.assertIn("orient_functional_stacks_stage", source)
        self.assertIn("resolve_plane_transform", source)

    def test_phase7_generator_tracks_registration_extraction(self) -> None:
        source = PHASE7_GENERATOR_PATH.read_text()
        self.assertIn("RegistrationSearchConfig", source)
        self.assertIn("run_registration_search_stage", source)
        self.assertIn("show_registration_overlay_stage", source)

    def test_phase8_generator_tracks_segmentation_extraction(self) -> None:
        source = PHASE8_GENERATOR_PATH.read_text()
        self.assertIn("HcrCellposeConfig", source)
        self.assertIn("run_hcr_cellpose_stage", source)
        self.assertIn("show_functional_label_overlay_stage", source)
        self.assertIn("export_suite2p_native_labels_stage", source)

    def test_phase9_generator_tracks_early_spatial_normalization_extraction(self) -> None:
        source = PHASE9_GENERATOR_PATH.read_text()
        self.assertIn("FunctionalReferenceConfig", source)
        self.assertIn("build_functional_references_stage", source)
        self.assertIn("AnatomyNormalizationStageConfig", source)
        self.assertIn("normalize_anatomy_stack_stage", source)
        self.assertIn("InPlaneRegistrationComparisonConfig", source)
        self.assertIn("run_in_plane_registration_comparison_stage", source)

    def test_cells_12_14_and_20_are_stage_wrappers_only(self) -> None:
        cell_12 = _code_cell_by_tag("12")
        cell_14 = _code_cell_by_tag("14")
        cell_20 = _code_cell_by_tag("20")

        self.assertIn("FunctionalReferenceConfig", cell_12)
        self.assertIn("build_functional_references_stage", cell_12)
        self.assertNotIn("def _quickshow_12(", cell_12)

        self.assertIn("AnatomyNormalizationStageConfig", cell_14)
        self.assertIn("normalize_anatomy_stack_stage", cell_14)
        self.assertNotIn("import nrrd", cell_14)
        self.assertNotIn("SimpleITK", cell_14)

        self.assertIn("InPlaneRegistrationComparisonConfig", cell_20)
        self.assertIn("run_in_plane_registration_comparison_stage", cell_20)
        self.assertNotIn("def _ncc_xy(", cell_20)

    def test_phase10_generator_tracks_small_notebook_only_cleanup(self) -> None:
        source = PHASE10_GENERATOR_PATH.read_text()
        self.assertIn("_response_active_truthy", source)
        self.assertIn("effective_motion_window(", source)

    def test_cells_56g_and_57_no_longer_define_local_helper_functions(self) -> None:
        cell_56g = _code_cell_by_tag("56g")
        cell_57 = _code_cell_by_tag("57")

        self.assertNotIn("def _bool_from_any_local(", cell_56g)
        self.assertIn("SingleFishBpiDiagnosticsConfig", cell_56g)
        self.assertIn("prepare_single_fish_bpi_diagnostics_stage", cell_56g)
        self.assertIn("run_single_fish_cell_56g_stage", cell_56g)

        self.assertNotIn("def _effective_motion_span_57(", cell_57)
        self.assertIn("effective_motion_window", cell_57)
        self.assertIn("run_single_fish_cell_57_stage", cell_57)

    def test_phase12_generator_tracks_56g_stage_rewrite(self) -> None:
        source = PHASE12_GENERATOR_PATH.read_text()
        self.assertIn("SingleFishBpiDiagnosticsConfig", source)
        self.assertIn("prepare_single_fish_bpi_diagnostics_stage", source)

    def test_phase13_generator_tracks_34a_stage_rewrite(self) -> None:
        source = PHASE13_GENERATOR_PATH.read_text()
        self.assertIn("FunctionalAnatomyDebugConfig", source)
        self.assertIn("build_functional_anatomy_debug_stage", source)

    def test_cell_34a_is_stage_wrapper_only(self) -> None:
        cell_34a = _code_cell_by_tag("34a")
        self.assertIn("FunctionalAnatomyDebugConfig", cell_34a)
        self.assertIn("build_functional_anatomy_debug_stage", cell_34a)
        self.assertNotIn("anat_labels_all = _ensure_uint_labels(imread_any(", cell_34a)

    def test_cell_55_publishes_df_stim_fish_id(self) -> None:
        cell = _code_cell_by_tag("55")
        self.assertIn("DF_STIM_FISH_ID = FISH_ID", cell)

    def test_matching_cells_use_package_builders(self) -> None:
        cell_23a = _code_cell_by_tag("23a")
        cell_50i = _code_cell_by_tag("50i")
        cell_50 = _code_cell_by_tag("50")
        cell_50ia = _code_cell_by_tag("50ia")
        self.assertIn("Suite2pStageConfig", cell_23a)
        self.assertIn("load_suite2p_stage", cell_23a)
        self.assertNotIn("def _build_labels_from_stat(", cell_23a)
        self.assertNotIn("def _find_suite2p_file(", cell_23a)
        self.assertIn("from codeants_2pf_hcr import build_anat_identity_lookup_df, build_functional_roi_master_df, gene_from_mask", cell_50i)
        self.assertIn("from codeants_2pf_hcr import build_hcr_activity_tables, gene_from_mask", cell_50)
        self.assertIn("from codeants_2pf_hcr import ActivityConfig, build_response_bpi_tables", cell_50ia)
        self.assertNotIn("def _build_prestim_baseline_windows_local", cell_50ia)
        self.assertNotIn("def _load_suite2p_dff_map_from_disk_local", cell_50ia)

    def test_downstream_cells_import_shared_helpers(self) -> None:
        for tag in ("56", "56h", "57"):
            cell = _code_cell_by_tag(tag)
            self.assertIn("prepare_pairs_for_unique_cells", cell)
        cell_56 = _code_cell_by_tag("56")
        cell_56h = _code_cell_by_tag("56h")
        cell_57 = _code_cell_by_tag("57")
        self.assertNotIn("def _prepare_pairs_for_unique_cells(", cell_56)
        self.assertNotIn("def _build_prestim_baseline_windows(", cell_56)
        self.assertNotIn("def _prepare_pairs_for_unique_cells_local(", cell_56h)
        self.assertNotIn("def _build_prestim_baseline_windows_local(", cell_56h)
        self.assertIn("combine_segments", cell_56h)
        self.assertIn("_combine_segments = combine_segments", cell_56h)
        self.assertNotIn("globals().get('hcr_match_summary_table', None)", cell_56h)
        self.assertIn("effective_motion_window", cell_57)

    def test_cell_57a_responsive_identity_donut_uses_package_renderer_only(self) -> None:
        cell = _code_cell_by_tag("57a-responsive-identity-donut")
        self.assertIn(
            "from codeants_2pf_hcr.plots.analysis import render_single_fish_50l_responsive_identity_donut",
            cell,
        )
        self.assertIn("render_single_fish_50l_responsive_identity_donut(", cell)
        self.assertNotIn("def ", cell)

    def test_cell_57b_anatomy_coexpression_summary_uses_package_renderer_only(self) -> None:
        cell = _code_cell_by_tag("57b-anatomy-coexpression-summary")
        self.assertIn(
            "from codeants_2pf_hcr.plots.qa import render_single_fish_hcr_anatomy_coexpression_summary",
            cell,
        )
        self.assertIn("render_single_fish_hcr_anatomy_coexpression_summary(", cell)
        self.assertNotIn("def ", cell)

    def test_cell_50l_supports_bottom_only_extra_height(self) -> None:
        cell = _code_cell_by_tag("50l")
        self.assertIn("render_single_fish_50l_composite(", cell)
        self.assertIn("run_config=RUN_CONFIG_LOCAL", cell)

    def test_cell_50l_uses_package_global_auc_renderer(self) -> None:
        cell = _code_cell_by_tag("50l")
        self.assertIn("from codeants_2pf_hcr.plots.analysis import render_single_fish_50l_composite", cell)
        self.assertIn("render_single_fish_50l_composite(", cell)
        self.assertIn("FIG_50L_COMPOSITE = _result_50l['FIG_50L_COMPOSITE']", cell)
        self.assertIn("FIG_50L_COMPOSITE_RGBA = _result_50l['FIG_50L_COMPOSITE_RGBA']", cell)
        self.assertIn("FIG_50L_COMPOSITE_PATH = _result_50l['FIG_50L_COMPOSITE_PATH']", cell)
        self.assertIn("globals()[f\"ax_{_axis_name_50l}\"] = _axis_50l", cell)
        self.assertNotIn("Response strength stays separable across stimulus directions and modes", cell)
        self.assertNotIn("motion_auc_by_gene_ipsi_contra.png", cell)
        self.assertNotIn("_single_fish_50l_auc_cache_stale_reasons", cell)
        self.assertNotIn("build_single_fish_motion_auc_plot_tables(", cell)
        self.assertNotIn("render_single_fish_50l_gene_auc_panel(", cell)
        self.assertNotIn("render_single_fish_50l_global_auc_panel(", cell)
        self.assertNotIn("def _plot_auc_block(", cell)
        self.assertNotIn("def _plot_auc_violin_all_neurons(", cell)

    def test_cells_50f_and_50g_rebuild_mask_fate_from_matching_stage(self) -> None:
        cell_50f = _code_cell_by_tag("50f")
        cell_50g = _code_cell_by_tag("50g")
        self.assertIn("from codeants_2pf_hcr.plots.qa import run_single_fish_cell_50f_stage", cell_50f)
        self.assertIn("from codeants_2pf_hcr.plots.qa import run_single_fish_cell_50g_stage", cell_50g)
        self.assertIn("run_single_fish_cell_50f_stage(globals())", cell_50f)
        self.assertIn("run_single_fish_cell_50g_stage(globals())", cell_50g)
        self.assertNotIn("def ", cell_50f)
        self.assertNotIn("def ", cell_50g)
        self.assertNotIn("Need HCR_MASK_FATE_DF from [50e]", cell_50f)
        self.assertNotIn("Need HCR_MASK_FATE_DF from [50e]", cell_50g)

    def test_phase2_generator_tracks_extraction(self) -> None:
        source = PHASE2_GENERATOR_PATH.read_text()
        self.assertIn("DF_STIM_FISH_ID = FISH_ID", source)
        self.assertIn("build_response_bpi_tables", source)
        self.assertIn("prepare_pairs_for_unique_cells", source)
        self.assertIn("combine_segments", source)

    def test_phase4_generator_tracks_suite2p_stage_extraction(self) -> None:
        source = PHASE4_GENERATOR_PATH.read_text()
        self.assertIn("Suite2pStageConfig", source)
        self.assertIn("load_suite2p_stage", source)
        self.assertIn("SUITE2P_FISH_ID = suite2p_result[\"suite2p_fish_id\"]", source)

    def test_context_and_debug_cells_use_package_stage_wrappers(self) -> None:
        cell_4a = _code_cell_by_tag("4a")
        cell_4b = _code_cell_by_tag("4b")
        cell_4c = _code_cell_by_tag("4c")
        cell_6 = _code_cell_by_tag("6")
        cell_8a = _code_cell_by_tag("8a")
        cell_10 = _code_cell_by_tag("10")
        cell_99 = _code_cell_by_tag("99-debug-fish-audit")
        self.assertIn("resolve_fish_state_stage", cell_4a)
        self.assertNotIn("def reset_fish_state(", cell_4a)
        self.assertIn("build_run_config_stage", cell_4b)
        self.assertIn("build_context_audit_stage", cell_4c)
        self.assertNotIn("def _apply_func_orientation(", cell_6)
        self.assertIn("build_registration_helper_stage", cell_6)
        self.assertIn("build_voxel_debug_stage", cell_8a)
        self.assertNotIn("def _read_nrrd_header(", cell_8a)
        self.assertIn("orient_functional_stacks_stage", cell_10)
        self.assertIn("FunctionalOrientationStageConfig", cell_10)
        self.assertNotIn("def _resolve_mode_local(", cell_10)
        self.assertIn("build_final_fish_audit_stage", cell_99)
        self.assertNotIn("def _maybe(", cell_99)

    def test_hidden_matching_helper_cell_uses_package_imports(self) -> None:
        cell = _code_cell_by_prefix("# HCR matching/QC helpers (antsQC-style)")
        self.assertIn("resolve_plane_transform", cell)
        self.assertIn("build_hcr_activity_tables", cell)
        self.assertIn("build_functional_anatomy_debug_df", cell)
        self.assertIn("build_plane_centroid_matches", cell)
        self.assertIn("resample_labels_nn", cell)
        self.assertIn("resolve_functional_labels_for_plane", cell)
        self.assertNotIn("def _tform_for_plane(", cell)
        self.assertNotIn("def compute_centroids(", cell)

    def test_centroid_qa_cells_use_package_matching_builders(self) -> None:
        cell_22d = _code_cell_by_tag("22d")
        cell_34 = _code_cell_by_tag("34")
        cell_34a = _code_cell_by_tag("34a")
        self.assertIn("show_region_shift_square_selector_stage", cell_22d)
        self.assertNotIn("def _render(", cell_22d)
        self.assertNotIn("def _bundle(", cell_22d)
        self.assertIn("show_centroid_match_qa_stage", cell_34)
        self.assertIn("centroid_qa_result", cell_34)
        self.assertIn("use_suite2p_labels=bool(globals().get('USE_SUITE2P_LABELS', False))", cell_34)
        self.assertIn("_prepare_plane_data = centroid_qa_result['helpers']['prepare_plane_data']", cell_34)
        self.assertNotIn("def _render(", cell_34)
        self.assertIn("build_functional_anatomy_debug_stage", cell_34a)
        self.assertIn("FunctionalAnatomyDebugConfig", cell_34a)
        self.assertIn("resample_labels_nn", cell_34a)
        self.assertNotIn("resample_failed (name 'resample_labels_nn' is not defined)", cell_34a)

    def test_registration_cells_use_package_stage_wrappers(self) -> None:
        cell_16 = _code_cell_by_tag("16")
        cell_19a = _code_cell_by_tag("19a")
        cell_20 = _code_cell_by_tag("20")
        cell_22 = _code_cell_by_tag("22")
        cell_22e = _code_cell_by_tag("22e")
        self.assertIn("RegistrationSearchConfig", cell_16)
        self.assertIn("run_registration_search_stage", cell_16)
        self.assertNotIn("def _search_scale_for_ref(", cell_16)
        self.assertNotIn("def _solve_plane(", cell_16)
        self.assertIn("show_ants_registration_region_selector_stage", cell_19a)
        self.assertIn("ANTS_REGISTRATION_REGION_MARGIN_FRACTION = 0.10", cell_19a)
        self.assertIn("anat_stack=globals().get('anat_f', globals().get('anat'))", cell_19a)
        self.assertNotIn("def _render(", cell_19a)
        self.assertLess(_code_cell_index_by_tag("19a"), _code_cell_index_by_tag("20"))
        self.assertIn("InPlaneRegistrationComparisonConfig", cell_20)
        self.assertIn("run_in_plane_registration_comparison_stage", cell_20)
        self.assertIn("INPLANE_ACTIVE_METHOD = 'ncc_xy'", cell_20)
        self.assertIn("ants_fixed_mask_json=INPLANE_ANTS_REGION_MASK_JSON", cell_20)
        self.assertIn("show_registration_overlay_stage", cell_22)
        self.assertNotIn("def _render(", cell_22)
        self.assertNotIn("def _apply_color(", cell_22)
        self.assertIn("show_inplane_registration_method_comparison_stage", cell_22e)
        self.assertIn("methods=('ncc_xy', 'ants_rigid_affine')", cell_22e)
        self.assertIn("anat_labels_path=globals().get('ANAT_LABELS_PATH')", cell_22e)
        self.assertIn("use_suite2p_labels=bool(INPLANE_METHOD_REVIEW_USE_SUITE2P_LABELS)", cell_22e)
        self.assertNotIn("def _render(", cell_22e)
        self.assertGreater(_code_cell_index_by_tag("22e"), _code_cell_index_by_tag("23a"))

    def test_segmentation_cells_use_package_stage_wrappers(self) -> None:
        cell_24 = _code_cell_by_tag("24")
        cell_26 = _code_cell_by_tag("26")
        cell_26a = _code_cell_by_tag("26a")
        self.assertIn("HcrCellposeConfig", cell_24)
        self.assertIn("run_hcr_cellpose_stage", cell_24)
        self.assertNotIn("def _src_priority(", cell_24)
        self.assertIn("show_functional_label_overlay_stage", cell_26)
        self.assertNotIn("def _get_labels_for_plane(", cell_26)
        self.assertNotIn("def _rescale_labels_to_ref(", cell_26)
        self.assertIn("export_suite2p_native_labels_stage", cell_26a)
        self.assertNotIn("def _native_suite2p_ref_paths(", cell_26a)
        self.assertNotIn("def _get_native_suite2p_labels_for_plane(", cell_26a)

    def test_cohort_53a_cell_uses_package_renderer_only(self) -> None:
        cell = _cohort_cell_by_tag("53a-cohort")
        self.assertIn("render_cohort_53a_summary(", cell)
        self.assertIn("cohort_53a_summary.png", cell)
        self.assertIn("cohort_53a_summary.pdf", cell)
        self.assertNotIn("def ", cell)

    def test_cohort_build_uses_package_collector(self) -> None:
        cell = _cohort_cell_by_tag("cohort-build")
        self.assertIn("build_cohort_outputs_stage(", cell)
        self.assertNotIn("collect_cohort_53a_tables(", cell)

    def test_cohort_cfg_wires_53a_cache_paths(self) -> None:
        cell = _cohort_cfg_cell()
        self.assertIn("CohortBuildConfig(", cell)
        self.assertIn("cohort_cache_paths", cell)
        self.assertIn("resolve_cohort_context_stage", cell)
        self.assertNotIn("def cohort_cache_paths(", cell)
        self.assertNotIn("def save_cohort_outputs_to_disk(", cell)
        self.assertNotIn("def load_cohort_outputs_from_disk(", cell)

    def test_cohort_late_cells_use_package_renderers_only(self) -> None:
        expected_imports = {
            "56h-cohort": "from codeants_2pf_hcr.plots.analysis import render_cohort_56h_by_fish",
            "56g-cohort": "from codeants_2pf_hcr.plots.analysis import render_cohort_56g_diagnostics",
            "cohort-auc": "from codeants_2pf_hcr.plots.analysis import render_cohort_motion_auc",
            "cohort-56h-donut-grid": "from codeants_2pf_hcr.plots.analysis import render_cohort_56h_status_donut_grid",
            "cohort-50l-donut-row": "from codeants_2pf_hcr.plots.analysis import render_cohort_50l_donut_row",
            "cohort-50l-responsive-identity-donut-row": "from codeants_2pf_hcr.plots.analysis import render_cohort_50l_responsive_identity_donut_row",
        }
        for tag, expected_import in expected_imports.items():
            cell = _cohort_cell_by_tag(tag)
            self.assertIn(expected_import, cell)
            self.assertIn("load_cohort_analysis_state", cell)
            self.assertNotIn("def ", cell)

    def test_cohort_responsive_identity_donut_cell_uses_package_renderer(self) -> None:
        cell = _cohort_cell_by_tag("cohort-50l-responsive-identity-donut-row")
        self.assertIn(
            "from codeants_2pf_hcr.plots.analysis import render_cohort_50l_responsive_identity_donut_row",
            cell,
        )
        self.assertIn("load_cohort_analysis_state", cell)
        self.assertNotIn("def ", cell)

    def test_cohort_phase2_generator_tracks_late_slice_refactor(self) -> None:
        source = COHORT_PHASE2_GENERATOR_PATH.read_text()
        self.assertIn("render_cohort_56h_by_fish", source)
        self.assertIn("render_cohort_56g_diagnostics", source)
        self.assertIn("render_cohort_motion_auc", source)
        self.assertIn("render_cohort_56h_status_donut_grid", source)
        self.assertIn("render_cohort_50l_donut_row", source)
        self.assertIn("render_cohort_50l_responsive_identity_donut_row", source)
        self.assertIn("load_cohort_analysis_state", source)

    def test_cohort_cache_keys_remain_stable(self) -> None:
        keys = set(cohort_cache_paths(Path("/tmp/cohort-test-keys")).keys())
        assert keys == {
            "fish_summary_csv",
            "stim_summary_csv",
            "bpi_trials_csv",
            "bpi_cells_csv",
            "cohort_53a_ncc_curves_csv",
            "cohort_53a_diameters_csv",
            "cohort_53a_diameter_filter_summary_csv",
            "cohort_53a_func_anat_offsets_csv",
            "cohort_53a_hcr_offsets_csv",
            "cohort_53a_thresholds_csv",
            "trace_cache_pkl",
        }

    def test_cohort_phase1_generator_tracks_build_cache_extraction(self) -> None:
        source = COHORT_PHASE1_GENERATOR_PATH.read_text()
        self.assertIn("CohortBuildConfig", source)
        self.assertIn("build_cohort_outputs_stage", source)
        self.assertIn("cohort_cache_paths", source)


if __name__ == "__main__":
    unittest.main()
