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
    "single-fish": {"50e", "50f", "50g", "50l", "56", "56f-qc", "56f-qc-activity", "56h", "56g", "57"},
    "cohort": {"53a-cohort", "56h-cohort", "56g-cohort", "cohort-auc", "cohort-56h-donut-grid", "cohort-50l-donut-row"},
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


def find_top_level_defs(notebook_path: str | Path) -> list[NotebookContractViolation]:
    violations: list[NotebookContractViolation] = []
    for index, cell in _code_cells(notebook_path):
        source = "".join(cell.get("source", []))
        if not source.strip():
            continue
        try:
            tree = ast.parse(source)
        except SyntaxError as exc:
            violations.append(
                NotebookContractViolation(
                    cell_index=index,
                    tag=_cell_tag(cell, index),
                    kind="syntax-error",
                    detail=str(exc),
                )
            )
            continue
        for node in tree.body:
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                violations.append(
                    NotebookContractViolation(
                        cell_index=index,
                        tag=_cell_tag(cell, index),
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


def check_notebook_contract(notebook_path: str | Path, *, profile: str | None = None) -> dict[str, Any]:
    profile_name = profile or _profile_from_notebook_path(notebook_path)
    top_level_defs = find_top_level_defs(notebook_path)
    figure_violations = find_figure_contract_violations(notebook_path, profile=profile_name)
    return {
        "notebook_path": str(Path(notebook_path)),
        "profile": profile_name,
        "top_level_defs": [violation.__dict__ for violation in top_level_defs],
        "figure_violations": [violation.__dict__ for violation in figure_violations],
        "n_top_level_defs": len(top_level_defs),
        "n_figure_violations": len(figure_violations),
    }


__all__ = [
    "NotebookContractViolation",
    "check_notebook_contract",
    "find_figure_contract_violations",
    "find_top_level_defs",
]
