"""Deterministic figure builders for notebook QA and CLI wrappers."""

from .analysis import (
    render_cohort_50l_responsive_identity_donut_row,
    render_single_fish_50l_responsive_identity_donut,
    plot_single_roi_57style,
    render_cohort_50l_donut_row,
    render_cohort_56g_diagnostics,
    render_cohort_56h_by_fish,
    render_cohort_56h_status_donut_grid,
    render_cohort_motion_auc,
)
from .annotations import place_labels_no_overlap
from .qa import (
    build_best_plane_modality_merge_grid,
    build_round_channel_mip_grid,
    collect_cohort_53a_tables,
    render_cohort_53a_summary,
)

__all__ = [
    "build_best_plane_modality_merge_grid",
    "build_round_channel_mip_grid",
    "collect_cohort_53a_tables",
    "place_labels_no_overlap",
    "plot_single_roi_57style",
    "render_cohort_50l_donut_row",
    "render_cohort_50l_responsive_identity_donut_row",
    "render_single_fish_50l_responsive_identity_donut",
    "render_cohort_53a_summary",
    "render_cohort_56g_diagnostics",
    "render_cohort_56h_by_fish",
    "render_cohort_56h_status_donut_grid",
    "render_cohort_motion_auc",
]
