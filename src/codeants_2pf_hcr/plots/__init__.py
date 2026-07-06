"""Deterministic figure builders for notebook QA and CLI wrappers."""

from __future__ import annotations

from importlib import import_module
from typing import Any

_EXPORT_MODULES = {
    "build_best_plane_modality_merge_grid": "codeants_2pf_hcr.plots.qa",
    "build_round_channel_mip_grid": "codeants_2pf_hcr.plots.qa",
    "collect_cohort_53a_tables": "codeants_2pf_hcr.plots.qa",
    "place_labels_no_overlap": "codeants_2pf_hcr.plots.annotations",
    "plot_single_roi_57style": "codeants_2pf_hcr.plots.analysis",
    "render_single_fish_hcr_anatomy_coexpression_summary": "codeants_2pf_hcr.plots.hcr",
    "render_single_fish_50l_composite": "codeants_2pf_hcr.plots.analysis",
    "render_single_fish_50l_population_response_donut_poster": "codeants_2pf_hcr.plots.analysis",
    "render_single_fish_50l_bpi_panel": "codeants_2pf_hcr.plots.analysis",
    "render_single_fish_50l_gene_auc_panel": "codeants_2pf_hcr.plots.analysis",
    "render_single_fish_50l_global_auc_panel": "codeants_2pf_hcr.plots.analysis",
    "render_single_fish_56h_per_gene_stimulus_trace_with_hcr_status": "codeants_2pf_hcr.plots.analysis",
    "render_cohort_50l_donut_row": "codeants_2pf_hcr.plots.analysis",
    "render_cohort_50l_responsive_identity_donut_row": "codeants_2pf_hcr.plots.analysis",
    "render_single_fish_50l_responsive_identity_donut": "codeants_2pf_hcr.plots.analysis",
    "render_cohort_53a_summary": "codeants_2pf_hcr.plots.qa",
    "render_cohort_56g_diagnostics": "codeants_2pf_hcr.plots.analysis",
    "render_cohort_56h_by_fish": "codeants_2pf_hcr.plots.analysis",
    "render_cohort_56h_fish_average_poster_traces": "codeants_2pf_hcr.plots.analysis",
    "render_cohort_56h_status_donut_grid": "codeants_2pf_hcr.plots.analysis",
    "render_cohort_motion_auc": "codeants_2pf_hcr.plots.analysis",
}

__all__ = list(_EXPORT_MODULES)


def __getattr__(name: str) -> Any:
    if name not in _EXPORT_MODULES:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module = import_module(_EXPORT_MODULES[name])
    value = getattr(module, name)
    globals()[name] = value
    return value
