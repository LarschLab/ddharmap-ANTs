from pathlib import Path

from codeants_2pf_hcr import check_notebook_contract


REPO_ROOT = Path(__file__).resolve().parents[1]
NOTEBOOK_PATH = REPO_ROOT / "notebooks" / "2PF_to_HCR.ipynb"
COHORT_NOTEBOOK_PATH = REPO_ROOT / "notebooks" / "multi_fish_56h_56g.ipynb"


def test_notebook_contract_audit_returns_counts() -> None:
    result = check_notebook_contract(NOTEBOOK_PATH)
    assert result["notebook_path"].endswith("notebooks/2PF_to_HCR.ipynb")
    assert result["profile"] == "single-fish"
    assert isinstance(result["n_top_level_defs"], int)
    assert isinstance(result["n_figure_violations"], int)
    assert isinstance(result["top_level_defs"], list)
    assert isinstance(result["figure_violations"], list)


def test_cohort_notebook_contract_audit_returns_counts() -> None:
    result = check_notebook_contract(COHORT_NOTEBOOK_PATH)
    assert result["notebook_path"].endswith("notebooks/multi_fish_56h_56g.ipynb")
    assert result["profile"] == "cohort"
    assert isinstance(result["n_top_level_defs"], int)
    assert isinstance(result["n_figure_violations"], int)
    assert isinstance(result["top_level_defs"], list)
    assert isinstance(result["figure_violations"], list)


def test_cohort_notebook_contract_has_zero_violations() -> None:
    result = check_notebook_contract(COHORT_NOTEBOOK_PATH)
    assert result["n_top_level_defs"] == 0
    assert result["n_figure_violations"] == 0
