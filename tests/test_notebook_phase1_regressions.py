import json
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
NOTEBOOK_PATH = REPO_ROOT / "notebooks" / "2PF_to_HCR.ipynb"
GENERATOR_PATH = REPO_ROOT / "tools" / "refactor_notebook_phase1.py"


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


if __name__ == "__main__":
    unittest.main()
