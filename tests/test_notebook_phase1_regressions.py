import json
import unittest
from pathlib import Path

from codeants_2pf_hcr import cohort_cache_paths


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

    def test_cell_50l_supports_bottom_only_extra_height(self) -> None:
        cell = _code_cell_by_tag("50l")
        self.assertIn(
            "COMPOSITE_50L_EXTRA_BOTTOM_HEIGHT_IN = float(RUN_CONFIG_LOCAL.get('COMPOSITE_50L_EXTRA_BOTTOM_HEIGHT_IN', 0.0))",
            cell,
        )
        self.assertIn("COMPOSITE_50L_TOTAL_HEIGHT_IN = COMPOSITE_50L_FIG_HEIGHT_IN + COMPOSITE_50L_EXTRA_BOTTOM_HEIGHT_IN", cell)
        self.assertIn("figsize=(COMPOSITE_50L_FIG_WIDTH_IN, COMPOSITE_50L_TOTAL_HEIGHT_IN)", cell)
        self.assertIn("if COMPOSITE_50L_EXTRA_BOTTOM_HEIGHT_IN > 0:", cell)
        self.assertIn("_top_anchor_scale_50l = COMPOSITE_50L_FIG_HEIGHT_IN / COMPOSITE_50L_TOTAL_HEIGHT_IN", cell)
        self.assertIn("for _ax in [ax for ax in (globals().get('ax_bpi', None), ax_donut) if ax is not None]:", cell)

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
        self.assertIn("_prepare_plane_data = centroid_qa_result['helpers']['prepare_plane_data']", cell_34)
        self.assertNotIn("def _render(", cell_34)
        self.assertIn("build_functional_anatomy_debug_df", cell_34a)
        self.assertIn("resample_labels_nn", cell_34a)
        self.assertNotIn("resample_failed (name 'resample_labels_nn' is not defined)", cell_34a)

    def test_registration_cells_use_package_stage_wrappers(self) -> None:
        cell_16 = _code_cell_by_tag("16")
        cell_22 = _code_cell_by_tag("22")
        self.assertIn("RegistrationSearchConfig", cell_16)
        self.assertIn("run_registration_search_stage", cell_16)
        self.assertNotIn("def _search_scale_for_ref(", cell_16)
        self.assertNotIn("def _solve_plane(", cell_16)
        self.assertIn("show_registration_overlay_stage", cell_22)
        self.assertNotIn("def _render(", cell_22)
        self.assertNotIn("def _apply_color(", cell_22)

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
