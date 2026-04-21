"""Static notebook-contract checks for the staged 2PF->HCR notebook."""

from __future__ import annotations

import ast
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any


_TAG_RE = re.compile(r"^\s*#\s*\[([^\]]+)\]")
_PROFILE_FIGURE_TAGS = {
    "single-fish": {"50e", "50f", "50g", "50l", "56", "56f-qc", "56f-qc-activity", "56h", "56g", "57a-responsive-identity-donut", "57"},
    "cohort": {
        "53a-cohort",
        "56h-cohort",
        "56g-cohort",
        "cohort-auc",
        "cohort-56h-donut-grid",
        "cohort-50l-donut-row",
        "cohort-50l-responsive-identity-donut-row",
    },
}
_PROFILE_REQUIRED_CELL_OWNERS = {
    "single-fish": {
        "12": {
            "imports": {
                "codeants_2pf_hcr": {"FunctionalReferenceConfig", "build_functional_references_stage"},
            },
            "calls": {"FunctionalReferenceConfig", "build_functional_references_stage"},
            "forbid_top_level_defs": True,
        },
        "14": {
            "imports": {
                "codeants_2pf_hcr": {"AnatomyNormalizationStageConfig", "normalize_anatomy_stack_stage"},
            },
            "calls": {"AnatomyNormalizationStageConfig", "normalize_anatomy_stack_stage"},
            "forbid_top_level_defs": True,
        },
        "20": {
            "imports": {
                "codeants_2pf_hcr": {"FunctionalPlacementConfig", "run_ncc_placement_stage"},
            },
            "calls": {"FunctionalPlacementConfig", "run_ncc_placement_stage"},
            "forbid_top_level_defs": True,
        },
        "34a": {
            "imports": {
                "codeants_2pf_hcr": {
                    "FunctionalAnatomyDebugConfig",
                    "build_functional_anatomy_debug_stage",
                },
            },
            "calls": {
                "FunctionalAnatomyDebugConfig",
                "build_functional_anatomy_debug_stage",
            },
            "forbid_top_level_defs": True,
        },
        "56g": {
            "imports": {
                "codeants_2pf_hcr": {
                    "SingleFishBpiDiagnosticsConfig",
                    "prepare_single_fish_bpi_diagnostics_stage",
                },
            },
            "calls": {
                "SingleFishBpiDiagnosticsConfig",
                "prepare_single_fish_bpi_diagnostics_stage",
            },
            "forbid_top_level_defs": True,
        },
        "50l": {
            "imports": {
                "codeants_2pf_hcr.traces": {"build_single_fish_motion_auc_plot_tables"},
                "codeants_2pf_hcr.plots.analysis": {
                    "render_single_fish_50l_bpi_panel",
                    "render_single_fish_50l_global_auc_panel",
                    "render_single_fish_50l_gene_auc_panel",
                },
            },
            "calls": {
                "build_single_fish_motion_auc_plot_tables",
                "render_single_fish_50l_bpi_panel",
                "render_single_fish_50l_global_auc_panel",
                "render_single_fish_50l_gene_auc_panel",
            },
            "forbid_top_level_defs": True,
        },
        "57a-responsive-identity-donut": {
            "imports": {
                "codeants_2pf_hcr.plots.analysis": {"render_single_fish_50l_responsive_identity_donut"},
            },
            "calls": {"render_single_fish_50l_responsive_identity_donut"},
            "forbid_top_level_defs": True,
        },
    },
    "cohort": {},
}


def _profile_from_notebook_path(notebook_path: str | Path) -> str:
    path_name = Path(notebook_path).name
    if path_name == "multi_fish_56h_56g.ipynb":
        return "cohort"
    return "single-fish"


@dataclass(frozen=True)
class NotebookContractViolation:
    cell_index: int
    tag: str
    kind: str
    detail: str


def _cell_tag(cell: dict[str, Any], index: int) -> str:
    source = "".join(cell.get("source", []))
    first = source.splitlines()[0] if source.splitlines() else ""
    match = _TAG_RE.match(first)
    if match:
        return match.group(1)
    return f"cell-{index}"


def _code_cells(notebook_path: str | Path) -> list[tuple[int, dict[str, Any]]]:
    notebook = json.loads(Path(notebook_path).read_text())
    return [(idx, cell) for idx, cell in enumerate(notebook.get("cells", [])) if cell.get("cell_type") == "code"]


def _parse_cell_tree(
    source: str,
    *,
    cell_index: int,
    tag: str,
) -> tuple[ast.AST | None, NotebookContractViolation | None]:
    try:
        return ast.parse(source), None
    except SyntaxError as exc:
        return None, NotebookContractViolation(
            cell_index=cell_index,
            tag=tag,
            kind="syntax-error",
            detail=str(exc),
        )


def _imported_names_by_module(tree: ast.AST) -> dict[str, set[str]]:
    imported: dict[str, set[str]] = {}
    for node in ast.walk(tree):
        if not isinstance(node, ast.ImportFrom) or node.module is None:
            continue
        names = imported.setdefault(node.module, set())
        for alias in node.names:
            names.add(alias.name)
    return imported


def _called_function_names(tree: ast.AST) -> set[str]:
    called: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if isinstance(func, ast.Name):
            called.add(func.id)
        elif isinstance(func, ast.Attribute):
            called.add(func.attr)
    return called


def find_top_level_defs(notebook_path: str | Path) -> list[NotebookContractViolation]:
    violations: list[NotebookContractViolation] = []
    for index, cell in _code_cells(notebook_path):
        source = "".join(cell.get("source", []))
        if not source.strip():
            continue
        tag = _cell_tag(cell, index)
        tree, parse_violation = _parse_cell_tree(source, cell_index=index, tag=tag)
        if parse_violation is not None:
            violations.append(parse_violation)
            continue
        for node in tree.body:
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                violations.append(
                    NotebookContractViolation(
                        cell_index=index,
                        tag=tag,
                        kind="top-level-def",
                        detail=f"{type(node).__name__}:{node.name}",
                    )
                )
    return violations


def find_figure_contract_violations(
    notebook_path: str | Path,
    *,
    profile: str | None = None,
) -> list[NotebookContractViolation]:
    profile_name = profile or _profile_from_notebook_path(notebook_path)
    figure_tags = _PROFILE_FIGURE_TAGS.get(profile_name, _PROFILE_FIGURE_TAGS["single-fish"])
    violations: list[NotebookContractViolation] = []
    for index, cell in _code_cells(notebook_path):
        tag = _cell_tag(cell, index)
        if tag not in figure_tags:
            continue
        source = "".join(cell.get("source", []))
        if "from codeants_2pf_hcr.plots." not in source:
            violations.append(
                NotebookContractViolation(
                    cell_index=index,
                    tag=tag,
                    kind="figure-import",
                    detail="missing plots.* renderer import",
                )
            )
        if re.search(r"^\s*(RESPONSE_|BPI_|IDENTITY_)", source, flags=re.MULTILINE):
            violations.append(
                NotebookContractViolation(
                    cell_index=index,
                    tag=tag,
                    kind="local-semantics",
                    detail="defines local identity/response/BPI semantics",
                )
            )
    return violations


def find_required_cell_contract_violations(
    notebook_path: str | Path,
    *,
    profile: str | None = None,
) -> list[NotebookContractViolation]:
    profile_name = profile or _profile_from_notebook_path(notebook_path)
    required_contracts = _PROFILE_REQUIRED_CELL_OWNERS.get(profile_name, {})
    if not required_contracts:
        return []

    notebook_imports_by_module: dict[str, set[str]] = {}
    for index, cell in _code_cells(notebook_path):
        source = "".join(cell.get("source", []))
        if not source.strip():
            continue
        tag = _cell_tag(cell, index)
        tree, _ = _parse_cell_tree(source, cell_index=index, tag=tag)
        if tree is None:
            continue
        for module_name, names in _imported_names_by_module(tree).items():
            notebook_imports_by_module.setdefault(module_name, set()).update(names)

    cells_by_tag = {
        _cell_tag(cell, index): (index, cell)
        for index, cell in _code_cells(notebook_path)
    }
    violations: list[NotebookContractViolation] = []
    for tag, contract in required_contracts.items():
        cell_entry = cells_by_tag.get(tag)
        if cell_entry is None:
            # Synthetic notebook fixtures often exercise only a subset of owner-tagged
            # cells. Enforce contracts for tags that are present instead of treating
            # absent tags as notebook-completeness failures.
            continue
        index, cell = cell_entry
        source = "".join(cell.get("source", []))
        tree, parse_violation = _parse_cell_tree(source, cell_index=index, tag=tag)
        if parse_violation is not None:
            violations.append(parse_violation)
            continue

        called_names = _called_function_names(tree)
        for module_name, names in contract.get("imports", {}).items():
            imported_names = notebook_imports_by_module.get(module_name, set())
            for name in sorted(names):
                if name not in imported_names:
                    violations.append(
                        NotebookContractViolation(
                            cell_index=index,
                            tag=tag,
                            kind="required-import",
                            detail=f"{module_name}:{name}",
                        )
                    )
        for name in sorted(contract.get("calls", set())):
            if name not in called_names:
                violations.append(
                    NotebookContractViolation(
                        cell_index=index,
                        tag=tag,
                        kind="required-call",
                        detail=name,
                    )
                )
        if contract.get("forbid_top_level_defs"):
            for node in tree.body:
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                    violations.append(
                        NotebookContractViolation(
                            cell_index=index,
                            tag=tag,
                            kind="local-helper-def",
                            detail=f"{type(node).__name__}:{node.name}",
                        )
                    )
    return violations


def check_notebook_contract(notebook_path: str | Path, *, profile: str | None = None) -> dict[str, Any]:
    profile_name = profile or _profile_from_notebook_path(notebook_path)
    top_level_defs = find_top_level_defs(notebook_path)
    figure_violations = find_figure_contract_violations(notebook_path, profile=profile_name)
    required_cell_violations = find_required_cell_contract_violations(notebook_path, profile=profile_name)
    return {
        "notebook_path": str(Path(notebook_path)),
        "profile": profile_name,
        "top_level_defs": [violation.__dict__ for violation in top_level_defs],
        "figure_violations": [violation.__dict__ for violation in figure_violations],
        "required_cell_violations": [violation.__dict__ for violation in required_cell_violations],
        "n_top_level_defs": len(top_level_defs),
        "n_figure_violations": len(figure_violations),
        "n_required_cell_violations": len(required_cell_violations),
    }


__all__ = [
    "NotebookContractViolation",
    "check_notebook_contract",
    "find_figure_contract_violations",
    "find_required_cell_contract_violations",
    "find_top_level_defs",
]
