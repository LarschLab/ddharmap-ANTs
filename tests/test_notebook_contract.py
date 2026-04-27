import json
from pathlib import Path

from codeants_2pf_hcr import check_notebook_contract
from codeants_2pf_hcr.notebook_contract import find_required_cell_contract_violations


REPO_ROOT = Path(__file__).resolve().parents[1]
NOTEBOOK_PATH = REPO_ROOT / "notebooks" / "2PF_to_HCR.ipynb"
COHORT_NOTEBOOK_PATH = REPO_ROOT / "notebooks" / "multi_fish_56h_56g.ipynb"


def _write_notebook(path: Path, cells: list[str]) -> None:
    payload = {
        "cells": [
            {
                "cell_type": "code",
                "metadata": {},
                "source": source.splitlines(keepends=True),
                "outputs": [],
                "execution_count": None,
            }
            for source in cells
        ],
        "metadata": {},
        "nbformat": 4,
        "nbformat_minor": 5,
    }
    path.write_text(json.dumps(payload))


def test_notebook_contract_audit_returns_counts() -> None:
    result = check_notebook_contract(NOTEBOOK_PATH)
    assert result["notebook_path"].endswith("notebooks/2PF_to_HCR.ipynb")
    assert result["profile"] == "single-fish"
    assert isinstance(result["n_top_level_defs"], int)
    assert isinstance(result["n_figure_violations"], int)
    assert isinstance(result["n_required_cell_violations"], int)
    assert isinstance(result["top_level_defs"], list)
    assert isinstance(result["figure_violations"], list)
    assert isinstance(result["required_cell_violations"], list)


def test_cohort_notebook_contract_audit_returns_counts() -> None:
    result = check_notebook_contract(COHORT_NOTEBOOK_PATH)
    assert result["notebook_path"].endswith("notebooks/multi_fish_56h_56g.ipynb")
    assert result["profile"] == "cohort"
    assert isinstance(result["n_top_level_defs"], int)
    assert isinstance(result["n_figure_violations"], int)
    assert isinstance(result["n_required_cell_violations"], int)
    assert isinstance(result["top_level_defs"], list)
    assert isinstance(result["figure_violations"], list)
    assert isinstance(result["required_cell_violations"], list)


def test_cohort_notebook_contract_has_zero_violations() -> None:
    result = check_notebook_contract(COHORT_NOTEBOOK_PATH)
    assert result["n_top_level_defs"] == 0
    assert result["n_figure_violations"] == 0
    assert result["n_required_cell_violations"] == 0


def test_required_cell_contract_detects_missing_single_fish_owner_imports_and_calls(tmp_path: Path) -> None:
    notebook_path = tmp_path / "2PF_to_HCR.ipynb"
    _write_notebook(
        notebook_path,
        [
            "# [50l]\n"
            "from codeants_2pf_hcr.plots.analysis import render_single_fish_50l_bpi_panel\n"
            "def _plot_auc_block():\n"
            "    return None\n"
            "render_single_fish_50l_bpi_panel(ax_bpi, bpi_plot_df)\n",
            "# [57a-responsive-identity-donut]\nprint('placeholder')\n",
        ],
    )

    violations = find_required_cell_contract_violations(notebook_path)
    pairs = {(violation.tag, violation.kind, violation.detail) for violation in violations}
    assert ("50l", "required-import", "codeants_2pf_hcr.plots.analysis:render_single_fish_50l_composite") in pairs
    assert ("50l", "required-call", "render_single_fish_50l_composite") in pairs
    assert ("50l", "local-helper-def", "FunctionDef:_plot_auc_block") in pairs
    assert (
        "57a-responsive-identity-donut",
        "required-import",
        "codeants_2pf_hcr.plots.analysis:render_single_fish_50l_responsive_identity_donut",
    ) in pairs
    assert ("57a-responsive-identity-donut", "required-call", "render_single_fish_50l_responsive_identity_donut") in pairs


def test_required_cell_contract_accepts_package_owned_single_fish_cells(tmp_path: Path) -> None:
    notebook_path = tmp_path / "2PF_to_HCR.ipynb"
    _write_notebook(
        notebook_path,
        [
            "# [50l]\n"
            "from codeants_2pf_hcr.plots.analysis import render_single_fish_50l_composite\n"
            "render_single_fish_50l_composite(out_reg=OUT_REG, outdir=FISH_PLOTS_DIR_50L)\n",
            "# [57a-responsive-identity-donut]\n"
            "from codeants_2pf_hcr.plots.analysis import render_single_fish_50l_responsive_identity_donut\n"
            "render_single_fish_50l_responsive_identity_donut(fish_id=FISH_ID, master_csv=master_csv, conf_func_csv=conf_csv, outdir=outdir)\n",
        ],
    )

    violations = find_required_cell_contract_violations(notebook_path)
    assert violations == []
