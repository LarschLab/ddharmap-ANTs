"""Notebook-facing HCR warp stage wrappers for single-fish cells [43] and [43b]."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from ._hcr_warp_stage_cells import CELL_43_SOURCE, CELL_43B_SOURCE


def _exec_stage_source(*, source: str, stage_tag: str, env: dict[str, Any] | None = None) -> dict[str, Any]:
    namespace = dict(env or {})
    namespace.setdefault("__name__", f"codeants_2pf_hcr.hcr_warp.{stage_tag}")
    code = compile(source, f"<{stage_tag}>", "exec")
    exec(code, namespace, namespace)
    bindings = {key: value for key, value in namespace.items() if not key.startswith("__")}
    return {
        "status": "ok",
        "bindings": bindings,
        "log_lines": [f"{stage_tag} executed via codeants_2pf_hcr.hcr_warp"],
    }


def run_hcr_external_bigwarp_label_stage(*, env: dict[str, Any] | None = None) -> dict[str, Any]:
    return _exec_stage_source(source=CELL_43_SOURCE, stage_tag="[43]", env=env)


def run_hcr_external_bigwarp_intensity_stage(*, env: dict[str, Any] | None = None) -> dict[str, Any]:
    return _exec_stage_source(source=CELL_43B_SOURCE, stage_tag="[43b]", env=env)


__all__ = [
    "run_hcr_external_bigwarp_intensity_stage",
    "run_hcr_external_bigwarp_label_stage",
]
