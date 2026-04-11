from pathlib import Path

from codeants_2pf_hcr import check_notebook_contract


REPO_ROOT = Path(__file__).resolve().parents[1]
NOTEBOOK_PATH = REPO_ROOT / "notebooks" / "2PF_to_HCR.ipynb"


def test_notebook_contract_audit_returns_counts() -> None:
    result = check_notebook_contract(NOTEBOOK_PATH)
    assert result["notebook_path"].endswith("notebooks/2PF_to_HCR.ipynb")
    assert isinstance(result["n_top_level_defs"], int)
    assert isinstance(result["n_figure_violations"], int)
    assert isinstance(result["top_level_defs"], list)
    assert isinstance(result["figure_violations"], list)
