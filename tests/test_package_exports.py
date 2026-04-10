from codeants_2pf_hcr import (
    ActivityConfig,
    SmokeValidationError,
    _ensure_uint_labels,
    _find_embedded_nrrd_header,
    _infer_voxels_from_open_tiff,
    _infer_voxels_nrrd,
    _parse_nrrd_header_text,
    _res_to_um_per_px,
    _to_um,
    apply_func_orientation,
    best_z_by_ncc,
    build_functional_roi_master_df,
    build_hcr_activity_tables,
    build_null_window_start_map,
    build_prestim_baseline_windows,
    build_prestim_trial_windows,
    build_response_bpi_tables,
    corrcoef_img,
    compute_zscore_stats,
    classify_stim_type,
    infer_frame_rate_from_detail,
    imread_any,
    infer_voxels_tiff,
    local_unsharp,
    load_suite2p_dff_map,
    load_or_cache_voxels,
    norm01,
    prepare_pairs_for_unique_cells,
    run_smoke_tier,
    top_correlated_mean,
    zproject_mean,
)


def test_notebook_spatial_exports_are_available() -> None:
    assert callable(ActivityConfig)
    assert callable(SmokeValidationError)
    assert callable(_ensure_uint_labels)
    assert callable(_find_embedded_nrrd_header)
    assert callable(_infer_voxels_from_open_tiff)
    assert callable(_infer_voxels_nrrd)
    assert callable(_parse_nrrd_header_text)
    assert callable(_res_to_um_per_px)
    assert callable(_to_um)
    assert callable(apply_func_orientation)
    assert callable(best_z_by_ncc)
    assert callable(build_functional_roi_master_df)
    assert callable(build_hcr_activity_tables)
    assert callable(build_null_window_start_map)
    assert callable(build_prestim_baseline_windows)
    assert callable(build_prestim_trial_windows)
    assert callable(build_response_bpi_tables)
    assert callable(classify_stim_type)
    assert callable(compute_zscore_stats)
    assert callable(corrcoef_img)
    assert callable(infer_frame_rate_from_detail)
    assert callable(imread_any)
    assert callable(infer_voxels_tiff)
    assert callable(local_unsharp)
    assert callable(load_suite2p_dff_map)
    assert callable(load_or_cache_voxels)
    assert callable(norm01)
    assert callable(prepare_pairs_for_unique_cells)
    assert callable(run_smoke_tier)
    assert callable(top_correlated_mean)
    assert callable(zproject_mean)
