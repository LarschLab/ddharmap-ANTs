"""Deterministic figure builders for notebook QA and CLI wrappers."""

from .analysis import plot_single_roi_57style
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
    "plot_single_roi_57style",
    "render_cohort_53a_summary",
]
