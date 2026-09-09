from __future__ import annotations

import ast
from pathlib import Path

import nbformat

import codeants_2pf_hcr as codeants


REPO_ROOT = Path(__file__).resolve().parents[1]
QC_NOTEBOOKS = tuple(
    REPO_ROOT / "notebooks" / "qc" / name
    for name in (
        "00_single_fish_pipeline_overview.ipynb",
        "01_functional_reference_and_drift_qc.ipynb",
        "02_functional_registration_qc.ipynb",
        "03_roi_anatomy_geometry_qc.ipynb",
        "04_molecular_geometry_qc.ipynb",
        "05_molecular_identity_qc.ipynb",
        "06_activity_and_export_qc.ipynb",
    )
)

FORBIDDEN_WRITER_CALLS = {
    "run_prepare_functional_reference_stacks_stage",
    "run_prepare_in_vivo_anatomy_stack_stage",
    "run_prepare_ex_vivo_anatomy_stack_stage",
    "run_register_functional_to_anatomy_stage",
    "run_transform_functional_rois_to_anatomy_stage",
    "run_make_functional_registration_qc_stage",
    "run_register_hcr_to_anatomy_stage",
    "run_match_roi_to_anatomy_stage",
    "run_segment_ex_vivo_anatomy_cellpose_stage",
    "run_segment_hcr_cellpose_stage",
    "run_single_fish_assign_hcr_identity_stage",
    "run_single_fish_score_activity_bpi_stage",
    "run_single_fish_export_canonical_tables_stage",
    "run_single_fish_make_qa_report_stage",
    "run_single_fish_make_figures_stage",
}


def _code_source(path: Path) -> str:
    notebook = nbformat.read(path, as_version=4)
    return "\n".join(cell.source for cell in notebook.cells if cell.cell_type == "code")


def test_qc_notebook_suite_is_valid_and_orchestration_thin() -> None:
    assert all(path.is_file() for path in QC_NOTEBOOKS)
    for path in QC_NOTEBOOKS:
        notebook = nbformat.read(path, as_version=4)
        nbformat.validate(notebook)
        source = _code_source(path)
        tree = ast.parse(source)
        assert not [node for node in ast.walk(tree) if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))], path
        assert "QCNotebookConfig" in source, path
        assert "resolve_qc_notebook_context" in source, path
        assert not (FORBIDDEN_WRITER_CALLS & {node.func.id for node in ast.walk(tree) if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)}), path
        assert "force_recompute" not in source


def test_qc_notebook_public_apis_are_exported() -> None:
    for name in (
        "QCNotebookConfig",
        "resolve_qc_notebook_context",
        "build_qc_stage_inventory",
        "write_qc_review_record",
        "load_geometry_review_bundle",
        "load_functional_reference_drift_qc",
        "load_functional_registration_qc",
        "inspect_molecular_geometry_qc",
        "inspect_molecular_identity_qc",
        "inspect_activity_export_qc",
    ):
        assert callable(getattr(codeants, name)), name

def test_qc_notebook_titles_match_their_declared_review_gates() -> None:
    expected = {
        "00_single_fish_pipeline_overview.ipynb": "Pipeline overview",
        "01_functional_reference_and_drift_qc.ipynb": "Functional reference and depth-stability review",
        "02_functional_registration_qc.ipynb": "Functional-to-anatomy registration review",
        "03_roi_anatomy_geometry_qc.ipynb": "ROI-to-anatomy geometry review",
        "04_molecular_geometry_qc.ipynb": "Molecular geometry review",
        "05_molecular_identity_qc.ipynb": "Molecular identity review",
        "06_activity_and_export_qc.ipynb": "Activity and BPI review",
    }
    for notebook_path in QC_NOTEBOOKS:
        notebook = nbformat.read(notebook_path, as_version=4)
        first_markdown = next(cell.source for cell in notebook.cells if cell.cell_type == "markdown")
        assert expected[notebook_path.name] in first_markdown, notebook_path


def test_functional_registration_notebook_uses_registration_review_owners() -> None:
    notebook_path = REPO_ROOT / "notebooks" / "qc" / "02_functional_registration_qc.ipynb"
    source = _code_source(notebook_path)
    assert "load_functional_registration_qc" in source
    assert "render_functional_registration_all_planes" in source
    assert "render_functional_registration_artifacts" in source
    assert "build_early_response_qc_from_raw" not in source
    assert "score-activity-bpi" not in source
