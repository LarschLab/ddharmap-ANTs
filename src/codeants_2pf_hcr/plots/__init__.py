"""Deterministic figure builders for notebook QA and CLI wrappers."""

from __future__ import annotations

from importlib import import_module
from typing import Any

_EXPORT_MODULES = {
    "geometry_review_queue": "codeants_2pf_hcr.plots.qc_geometry",
    "load_geometry_review_bundle": "codeants_2pf_hcr.plots.qc_geometry",
    "render_geometry_review_summary": "codeants_2pf_hcr.plots.qc_geometry",
    "show_geometry_review_dashboard": "codeants_2pf_hcr.plots.qc_geometry",
    "summarize_geometry_review": "codeants_2pf_hcr.plots.qc_geometry",
    "functional_registration_plane_review": "codeants_2pf_hcr.plots.qc_functional",
    "load_functional_reference_drift_qc": "codeants_2pf_hcr.plots.qc_functional",
    "load_early_response_timing_qc": "codeants_2pf_hcr.plots.qc_functional",
    "build_early_response_qc_from_raw": "codeants_2pf_hcr.plots.qc_functional",
    "load_functional_registration_qc": "codeants_2pf_hcr.plots.qc_functional",
    "render_functional_reference_artifacts": "codeants_2pf_hcr.plots.qc_functional",
    "render_functional_reference_drift_summary": "codeants_2pf_hcr.plots.qc_functional",
    "render_early_response_timing_audit": "codeants_2pf_hcr.plots.qc_functional",
    "render_early_response_full_session_heatmap": "codeants_2pf_hcr.plots.qc_functional",
    "render_stimulus_type_mean_dff": "codeants_2pf_hcr.plots.qc_functional",
    "render_stimulus_type_mean_dff_frame": "codeants_2pf_hcr.plots.qc_functional",
    "render_functional_registration_artifacts": "codeants_2pf_hcr.plots.qc_functional",
    "render_functional_registration_plane": "codeants_2pf_hcr.plots.qc_functional",
    "MidlineAnnotationSession": "codeants_2pf_hcr.plots.qc_midline",
    "build_automatic_anatomy_midline_qc": "codeants_2pf_hcr.plots.qc_midline",
    "render_automatic_anatomy_midline_qc": "codeants_2pf_hcr.plots.qc_midline",
    "build_automatic_native_functional_midline_qc": "codeants_2pf_hcr.plots.qc_midline",
    "render_automatic_native_functional_midline_qc": "codeants_2pf_hcr.plots.qc_midline",
    "load_legacy_midline_annotation_session": "codeants_2pf_hcr.plots.qc_midline",
    "load_motion_corrected_mean_midline_session": "codeants_2pf_hcr.plots.qc_midline",
    "load_native_functional_midline_annotation_session": "codeants_2pf_hcr.plots.qc_midline",
    "load_suite2p_meanimg_midline_session": "codeants_2pf_hcr.plots.qc_midline",
    "show_midline_annotation_gui": "codeants_2pf_hcr.plots.qc_midline",
    "load_accepted_anatomy_midline_context": "codeants_2pf_hcr.plots.qc_midline",
    "build_midline_laterality_qc_tables": "codeants_2pf_hcr.plots.qc_midline",
    "render_midline_laterality_plane_grid": "codeants_2pf_hcr.plots.qc_midline",
    "render_midline_activity_plane_grid": "codeants_2pf_hcr.plots.qc_midline",
    "inspect_activity_export_qc": "codeants_2pf_hcr.plots.qc_molecular",
    "build_hcr_anatomy_centroid_offset_table": "codeants_2pf_hcr.plots.qc_molecular",
    "inspect_molecular_geometry_qc": "codeants_2pf_hcr.plots.qc_molecular",
    "inspect_molecular_identity_qc": "codeants_2pf_hcr.plots.qc_molecular",
    "plot_activity_export_qc": "codeants_2pf_hcr.plots.qc_molecular",
    "plot_hcr_anatomy_centroid_offsets": "codeants_2pf_hcr.plots.qc_molecular",
    "plot_molecular_geometry_qc": "codeants_2pf_hcr.plots.qc_molecular",
    "plot_molecular_identity_qc": "codeants_2pf_hcr.plots.qc_molecular",
    "build_best_plane_modality_merge_grid": "codeants_2pf_hcr.plots.qa",
    "build_round_channel_mip_grid": "codeants_2pf_hcr.plots.qa",
    "collect_cohort_53a_tables": "codeants_2pf_hcr.plots.qa",
    "place_labels_no_overlap": "codeants_2pf_hcr.plots.annotations",
    "plot_single_roi_57style": "codeants_2pf_hcr.plots.analysis",
    "render_single_fish_hcr_anatomy_coexpression_summary": "codeants_2pf_hcr.plots.hcr",
    "render_single_fish_50l_composite": "codeants_2pf_hcr.plots.analysis",
    "render_accepted_global_laterality_auc_qc": "codeants_2pf_hcr.plots.analysis",
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
