from codeants_2pf_hcr import (
    ActivityConfig,
    AnatomyNormalizationStageConfig,
    ContextStageConfig,
    CohortBuildConfig,
    FinalFishAuditConfig,
    FunctionalAnatomyDebugConfig,
    FunctionalPlacementConfig,
    FunctionalOrientationStageConfig,
    FunctionalRoiIdentityConfig,
    FunctionalReferenceConfig,
    FishStateStageConfig,
    HcrActivityExportConfig,
    HcrCellposeConfig,
    InPlaneRegistrationComparisonConfig,
    SingleFishBpiDiagnosticsConfig,
    NotebookContractViolation,
    RegistrationSearchConfig,
    SmokeValidationError,
    Suite2pStageConfig,
    VoxelStageConfig,
    _ensure_uint_labels,
    _regionprops_centroids_2d,
    _find_embedded_nrrd_header,
    _infer_voxels_from_open_tiff,
    _infer_voxels_nrrd,
    _parse_nrrd_header_text,
    _res_to_um_per_px,
    _to_um,
    apply_square_region_mask,
    apply_func_orientation,
    best_z_by_ncc,
    build_context_audit_stage,
    build_cohort_outputs_stage,
    build_final_fish_audit_stage,
    build_hcr_mask_fate_df,
    build_functional_anatomy_debug_df,
    build_functional_anatomy_debug_stage,
    build_functional_references_stage,
    build_functional_roi_master_df,
    build_hcr_activity_tables,
    build_single_fish_motion_auc_plot_tables,
    build_plane_centroid_matches,
    build_voxel_debug_stage,
    build_null_window_start_map,
    build_prestim_baseline_windows,
    build_prestim_trial_windows,
    build_registration_helper_stage,
    build_response_bpi_tables,
    build_run_config_stage,
    check_notebook_contract,
    collect_cohort_53a_tables,
    cohort_cache_paths,
    count_trace_genes,
    collect_hcr_intensity_stack_paths,
    combine_segments,
    compute_centroids,
    corrcoef_img,
    compute_zscore_stats,
    classify_stim_type,
    deduplicate_hcr_intensity_targets,
    export_suite2p_native_labels_stage,
    infer_frame_rate_from_detail,
    imread_any,
    infer_voxels_tiff,
    idx_to_um,
    local_unsharp,
    load_suite2p_stage,
    load_suite2p_dff_map,
    load_or_cache_voxels,
    load_cohort_outputs_from_disk,
    load_cohort_analysis_state,
    load_midline_context,
    ncc_xy,
    norm01,
    normalize_anatomy_stack_stage,
    orient_functional_stacks_stage,
    MotionAucPlotConfig,
    annotate_midline_side,
    extract_window_with_padding,
    filter_high_confidence_pairs,
    prepare_single_fish_bpi_diagnostics_stage,
    prepare_pairs_for_unique_cells,
    nearest_neighbor_match,
    registration_metric_from_scores,
    render_cohort_53a_summary,
    render_cohort_50l_donut_row,
    render_cohort_50l_responsive_identity_donut_row,
    render_single_fish_hcr_anatomy_coexpression_summary,
    render_single_fish_50l_composite,
    render_single_fish_50l_bpi_panel,
    render_single_fish_50l_gene_auc_panel,
    render_single_fish_50l_global_auc_panel,
    render_single_fish_50l_responsive_identity_donut,
    render_cohort_56g_diagnostics,
    render_cohort_56h_by_fish,
    render_cohort_56h_status_donut_grid,
    render_cohort_motion_auc,
    hungarian_match,
    harmonize_functional_labels_to_anatomy,
    resolve_functional_labels_for_plane,
    resolve_conf_func_csv_analysis,
    resolve_fish_state_stage,
    resolve_hcr_cellpose_model_path,
    resolve_native_suite2p_labels_for_plane,
    resolve_notebook_context_stage,
    resolve_plane_transform,
    resample_labels_nn,
    resolve_voxel_context_stage,
    resolve_cohort_context_stage,
    run_hcr_cellpose_stage,
    run_in_plane_registration_comparison_stage,
    run_ncc_placement_stage,
    run_registration_search_stage,
    run_single_fish_cell_22c_stage,
    run_single_fish_cell_50_stage,
    run_single_fish_cell_50e_stage,
    run_single_fish_cell_50f_stage,
    run_single_fish_cell_50g_stage,
    run_single_fish_cell_53_stage,
    run_single_fish_cell_53a_stage,
    run_single_fish_cell_56_stage,
    run_single_fish_cell_56f_qc_activity_stage,
    run_single_fish_cell_56f_qc_stage,
    run_single_fish_cell_56g_stage,
    run_single_fish_cell_56h_stage,
    run_single_fish_cell_57_stage,
    organize,
    run_smoke_tier,
    scale_image,
    save_cohort_outputs_to_disk,
    show_functional_label_overlay_stage,
    show_centroid_match_qa_stage,
    show_ants_registration_region_selector_stage,
    show_inplane_registration_method_comparison_stage,
    show_region_shift_square_selector_stage,
    compute_anatomy_median_xy_radius_um,
    show_registration_overlay_stage,
    summarize_distances,
    summarize_functional_anatomy_geometry_metrics,
    top_correlated_mean,
    zproject_mean,
)


def test_notebook_spatial_exports_are_available() -> None:
    assert callable(ActivityConfig)
    assert callable(AnatomyNormalizationStageConfig)
    assert callable(ContextStageConfig)
    assert callable(CohortBuildConfig)
    assert callable(FinalFishAuditConfig)
    assert callable(FunctionalAnatomyDebugConfig)
    assert callable(FunctionalPlacementConfig)
    assert callable(FunctionalOrientationStageConfig)
    assert callable(FunctionalRoiIdentityConfig)
    assert callable(FunctionalReferenceConfig)
    assert callable(FishStateStageConfig)
    assert callable(HcrActivityExportConfig)
    assert callable(HcrCellposeConfig)
    assert callable(InPlaneRegistrationComparisonConfig)
    assert callable(SingleFishBpiDiagnosticsConfig)
    assert callable(NotebookContractViolation)
    assert callable(RegistrationSearchConfig)
    assert callable(SmokeValidationError)
    assert callable(Suite2pStageConfig)
    assert callable(VoxelStageConfig)
    assert callable(_ensure_uint_labels)
    assert callable(_regionprops_centroids_2d)
    assert callable(_find_embedded_nrrd_header)
    assert callable(_infer_voxels_from_open_tiff)
    assert callable(_infer_voxels_nrrd)
    assert callable(_parse_nrrd_header_text)
    assert callable(_res_to_um_per_px)
    assert callable(_to_um)
    assert callable(apply_square_region_mask)
    assert callable(apply_func_orientation)
    assert callable(best_z_by_ncc)
    assert callable(build_context_audit_stage)
    assert callable(build_cohort_outputs_stage)
    assert callable(build_final_fish_audit_stage)
    assert callable(build_hcr_mask_fate_df)
    assert callable(build_functional_anatomy_debug_df)
    assert callable(build_functional_anatomy_debug_stage)
    assert callable(build_functional_references_stage)
    assert callable(build_functional_roi_master_df)
    assert callable(build_hcr_activity_tables)
    assert callable(build_single_fish_motion_auc_plot_tables)
    assert callable(build_plane_centroid_matches)
    assert callable(build_voxel_debug_stage)
    assert callable(build_null_window_start_map)
    assert callable(build_prestim_baseline_windows)
    assert callable(build_prestim_trial_windows)
    assert callable(build_registration_helper_stage)
    assert callable(build_response_bpi_tables)
    assert callable(build_run_config_stage)
    assert callable(check_notebook_contract)
    assert callable(classify_stim_type)
    assert callable(collect_cohort_53a_tables)
    assert callable(cohort_cache_paths)
    assert callable(count_trace_genes)
    assert callable(collect_hcr_intensity_stack_paths)
    assert callable(combine_segments)
    assert callable(compute_centroids)
    assert callable(compute_zscore_stats)
    assert callable(corrcoef_img)
    assert callable(deduplicate_hcr_intensity_targets)
    assert callable(export_suite2p_native_labels_stage)
    assert callable(harmonize_functional_labels_to_anatomy)
    assert callable(hungarian_match)
    assert callable(infer_frame_rate_from_detail)
    assert callable(imread_any)
    assert callable(infer_voxels_tiff)
    assert callable(idx_to_um)
    assert callable(local_unsharp)
    assert callable(load_suite2p_stage)
    assert callable(load_suite2p_dff_map)
    assert callable(load_or_cache_voxels)
    assert callable(load_cohort_outputs_from_disk)
    assert callable(load_cohort_analysis_state)
    assert callable(load_midline_context)
    assert callable(ncc_xy)
    assert callable(nearest_neighbor_match)
    assert callable(normalize_anatomy_stack_stage)
    assert callable(norm01)
    assert callable(orient_functional_stacks_stage)
    assert callable(MotionAucPlotConfig)
    assert callable(annotate_midline_side)
    assert callable(extract_window_with_padding)
    assert callable(filter_high_confidence_pairs)
    assert callable(prepare_single_fish_bpi_diagnostics_stage)
    assert callable(prepare_pairs_for_unique_cells)
    assert callable(resolve_functional_labels_for_plane)
    assert callable(resolve_plane_transform)
    assert callable(resample_labels_nn)
    assert callable(registration_metric_from_scores)
    assert callable(render_cohort_53a_summary)
    assert callable(render_cohort_50l_donut_row)
    assert callable(render_cohort_50l_responsive_identity_donut_row)
    assert callable(render_single_fish_hcr_anatomy_coexpression_summary)
    assert callable(render_single_fish_50l_composite)
    assert callable(render_single_fish_50l_bpi_panel)
    assert callable(render_single_fish_50l_gene_auc_panel)
    assert callable(render_single_fish_50l_global_auc_panel)
    assert callable(render_single_fish_50l_responsive_identity_donut)
    assert callable(render_cohort_56g_diagnostics)
    assert callable(render_cohort_56h_by_fish)
    assert callable(render_cohort_56h_status_donut_grid)
    assert callable(render_cohort_motion_auc)
    assert callable(resolve_conf_func_csv_analysis)
    assert callable(resolve_fish_state_stage)
    assert callable(resolve_hcr_cellpose_model_path)
    assert callable(resolve_native_suite2p_labels_for_plane)
    assert callable(resolve_notebook_context_stage)
    assert callable(resolve_voxel_context_stage)
    assert callable(resolve_cohort_context_stage)
    assert callable(run_hcr_cellpose_stage)
    assert callable(run_in_plane_registration_comparison_stage)
    assert callable(run_ncc_placement_stage)
    assert callable(run_registration_search_stage)
    assert callable(run_single_fish_cell_22c_stage)
    assert callable(run_single_fish_cell_50_stage)
    assert callable(run_single_fish_cell_50e_stage)
    assert callable(run_single_fish_cell_50f_stage)
    assert callable(run_single_fish_cell_50g_stage)
    assert callable(run_single_fish_cell_53_stage)
    assert callable(run_single_fish_cell_53a_stage)
    assert callable(run_single_fish_cell_56_stage)
    assert callable(run_single_fish_cell_56f_qc_activity_stage)
    assert callable(run_single_fish_cell_56f_qc_stage)
    assert callable(run_single_fish_cell_56g_stage)
    assert callable(run_single_fish_cell_56h_stage)
    assert callable(run_single_fish_cell_57_stage)
    assert callable(organize)
    assert callable(run_smoke_tier)
    assert callable(scale_image)
    assert callable(save_cohort_outputs_to_disk)
    assert callable(show_centroid_match_qa_stage)
    assert callable(show_ants_registration_region_selector_stage)
    assert callable(show_inplane_registration_method_comparison_stage)
    assert callable(show_region_shift_square_selector_stage)
    assert callable(compute_anatomy_median_xy_radius_um)
    assert callable(show_functional_label_overlay_stage)
    assert callable(show_registration_overlay_stage)
    assert callable(summarize_distances)
    assert callable(summarize_functional_anatomy_geometry_metrics)
    assert callable(top_correlated_mean)
    assert callable(zproject_mean)
