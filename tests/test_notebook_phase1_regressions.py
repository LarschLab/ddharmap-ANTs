import json
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
NOTEBOOK_PATH = REPO_ROOT / "notebooks" / "2PF_to_HCR.ipynb"
GENERATOR_PATH = REPO_ROOT / "tools" / "refactor_notebook_phase1.py"
PHASE2_GENERATOR_PATH = REPO_ROOT / "tools" / "refactor_notebook_phase2.py"


def _code_cell_by_tag(tag: str) -> str:
    notebook = json.loads(NOTEBOOK_PATH.read_text())
    for cell in notebook["cells"]:
        if cell.get("cell_type") != "code":
            continue
        source = "".join(cell.get("source", []))
        if source.startswith(f"# [{tag}]"):
            return source
    raise AssertionError(f"Notebook cell [{tag}] not found")


class NotebookPhase1RegressionTests(unittest.TestCase):
    def test_cell_4_binds_legacy_compatibility_surface(self) -> None:
        cell = _code_cell_by_tag("4")
        self.assertIn("notebook_bindings_from_context", cell)
        self.assertIn("RUN_CONFIG = dict(CTX.run_config)", cell)
        self.assertIn("FUNC_STACK_PATH = FUNC_RAW_STACK_PATH", cell)
        self.assertIn("def _resolve_func_polarity(force_refresh=False):", cell)

    def test_cell_8_uses_local_snapshots_for_voxel_resolution(self) -> None:
        cell = _code_cell_by_tag("8")
        self.assertIn("func_scale = _norm_vox(VOX_FUNC_AUTO_LOCAL)", cell)
        self.assertIn("Path(FUNC_RAW_STACK_PATH_LOCAL).name", cell)
        self.assertIn("v = VOX_FUNC_MANUAL_LOCAL.get(ax)", cell)
        self.assertNotIn("func_scale = _norm_vox(VOX_FUNC_AUTO)\n", cell)
        self.assertNotIn("Path(FUNC_RAW_STACK_PATH).name", cell)
        self.assertNotIn("v = VOX_FUNC_MANUAL.get(ax)", cell)

    def test_metadata_cell_records_raw_functional_source_path(self) -> None:
        cell = _code_cell_by_tag("40")
        self.assertIn("FUNC_RAW_STACK_PATH_LOCAL = FUNC_RAW_STACK_PATH", cell)
        self.assertIn("'FUNC_STACK_PATH': str(FUNC_RAW_STACK_PATH_LOCAL) if FUNC_RAW_STACK_PATH_LOCAL else None", cell)

    def test_generator_contains_phase1_binding_fixes(self) -> None:
        source = GENERATOR_PATH.read_text()
        self.assertIn("globals().update(notebook_bindings_from_context(CTX))", source)
        self.assertIn("RUN_CONFIG = dict(CTX.run_config)", source)
        self.assertIn("FUNC_STACK_PATH = FUNC_RAW_STACK_PATH", source)
        self.assertIn("def _resolve_func_polarity(force_refresh=False):", source)

    def test_cell_55_publishes_df_stim_fish_id(self) -> None:
        cell = _code_cell_by_tag("55")
        self.assertIn("DF_STIM_FISH_ID = FISH_ID", cell)

    def test_matching_cells_use_package_builders(self) -> None:
        cell_50i = _code_cell_by_tag("50i")
        cell_50 = _code_cell_by_tag("50")
        cell_50ia = _code_cell_by_tag("50ia")
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
        self.assertNotIn("globals().get('hcr_match_summary_table', None)", cell_56h)
        self.assertIn("effective_motion_window", cell_57)

    def test_phase2_generator_tracks_extraction(self) -> None:
        source = PHASE2_GENERATOR_PATH.read_text()
        self.assertIn("DF_STIM_FISH_ID = FISH_ID", source)
        self.assertIn("build_response_bpi_tables", source)
        self.assertIn("prepare_pairs_for_unique_cells", source)


if __name__ == "__main__":
    unittest.main()
